from __future__ import annotations

import asyncio
from dataclasses import replace
from pathlib import Path

import pytest

from fabric_jupyter.broker import BrokerClient, BrokerServer
from fabric_jupyter.config import default_profiles
from fabric_jupyter.models import FabricLanguage, Profile, TransportKind
from fabric_jupyter.transport import FakeFabricTransport


def test_requests_are_bound_to_server_policy(local_state: Path) -> None:
    async def run() -> None:
        profile = default_profiles()["fabric-pyspark"]
        target = profile.target
        assert target is not None
        transport = FakeFabricTransport()
        server = BrokerServer(profile=profile, transport=transport)
        endpoint = await server.start()
        client = BrokerClient(endpoint)
        try:
            for changed in (
                replace(target, workspace_id="11111111-1111-1111-1111-111111111111"),
                replace(target, notebook_id="22222222-2222-2222-2222-222222222222"),
                replace(target, language=FabricLanguage.PYTHON),
            ):
                with pytest.raises(RuntimeError, match="not authorized"):
                    await client.execute(
                        request_id="wrong-target", target=changed, code="not evaluated",
                        silent=False, transport=TransportKind.FAKE,
                    )
                with pytest.raises(RuntimeError, match="not authorized"):
                    await client.interrupt(changed)
                with pytest.raises(RuntimeError, match="not authorized"):
                    await client.shutdown(changed)
            with pytest.raises(RuntimeError, match="not authorized"):
                await client.execute(
                    request_id="wrong-transport", target=target, code="not evaluated",
                    silent=False, transport=TransportKind.EXPERIMENTAL,
                )
            assert await client.status() == []
            assert transport.executions == []
            assert transport.interrupted == []
            assert transport.shutdown_targets == []
            for method in ("execute", "interrupt", "shutdown"):
                params = {"target": target.to_dict()}
                if method == "execute":
                    params.update(requestId="override", code="not evaluated")
                for field in ("profile", "lakehouseId", "environmentId"):
                    with pytest.raises(RuntimeError):
                        await client._request(method, {**params, field: "not-allowed"})
            with pytest.raises(RuntimeError, match="no parameters"):
                await client._request("status", {"target": target.to_dict()})
            events = await client.execute(
                request_id="allowed", target=target, code="not evaluated",
                silent=False, transport=TransportKind.FAKE,
            )
            assert any(event.kind.value == "result" for event in events)
            assert all(session["remoteSession"] is False for session in await client.status())
            result = next(event for event in events if event.kind.value == "result")
            assert "no Fabric session was created" in result.content["data"]["text/plain"]
        finally:
            await server.close()

    asyncio.run(run())


def test_unavailable_transport_is_rejected_before_listener_creation(local_state: Path) -> None:
    profile = replace(default_profiles()["fabric-pyspark"], transport=TransportKind.EXPERIMENTAL)
    with pytest.raises(ValueError, match="broker will not start"):
        BrokerServer(profile=profile)


def test_empty_target_policy_does_not_accept_client_selected_target(local_state: Path) -> None:
    with pytest.raises(ValueError):
        BrokerServer(profile=Profile(name="unbound", language=FabricLanguage.PYSPARK))


def test_private_kernel_brokers_do_not_replace_each_other(local_state: Path) -> None:
    async def run() -> None:
        first, second = BrokerServer(), BrokerServer()
        try:
            one, two = await first.start(), await second.start()
            assert one.address != two.address
            assert await BrokerClient(one).status() == []
            assert await BrokerClient(two).status() == []
        finally:
            await first.close()
            await second.close()

    asyncio.run(run())


def test_truncated_broker_response_is_not_success() -> None:
    from fabric_jupyter.broker import BrokerEndpoint

    async def run() -> None:
        async def truncate(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            await reader.readline()
            writer.close()
            await writer.wait_closed()

        server = await asyncio.start_server(truncate, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        try:
            client = BrokerClient(BrokerEndpoint("tcp", f"127.0.0.1:{port}", "offline-test"))
            with pytest.raises(RuntimeError, match="before completing"):
                await client.status()
        finally:
            server.close()
            await server.wait_closed()

    asyncio.run(run())


def test_broker_does_not_bind_when_private_state_cannot_be_verified(
    local_state: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import fabric_jupyter.broker as broker

    def deny(_: Path) -> Path:
        raise PermissionError("private runtime state cannot be verified")

    monkeypatch.setattr(broker, "ensure_private", deny)

    async def run() -> None:
        server = BrokerServer()
        with pytest.raises(PermissionError, match="cannot be verified"):
            await server.start()
        assert server._server is None
        assert server._endpoint is None

    asyncio.run(run())


def test_broker_never_binds_external_tcp_address(local_state: Path) -> None:
    from fabric_jupyter.broker import BrokerEndpoint

    async def run() -> None:
        server = BrokerServer(endpoint=BrokerEndpoint("tcp", "0.0.0.0:0", "offline-test"))
        with pytest.raises(ValueError, match="127.0.0.1"):
            await server.start()
        assert server._server is None

    asyncio.run(run())


def test_owned_descriptor_is_removed_on_orderly_shutdown(local_state: Path) -> None:
    from fabric_jupyter.paths import broker_endpoint_path

    async def run() -> None:
        server = BrokerServer()
        await server.start(persist_endpoint=True)
        descriptor = broker_endpoint_path()
        assert descriptor.exists()
        second = BrokerServer()
        with pytest.raises(FileExistsError, match="owning broker"):
            await second.start(persist_endpoint=True)
        assert second._server is None
        await server.close()
        assert not descriptor.exists()

    asyncio.run(run())
