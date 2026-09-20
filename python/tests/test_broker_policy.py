from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import replace
from pathlib import Path

import pytest

from fabric_jupyter.broker import BrokerClient, BrokerServer
from fabric_jupyter.models import (
    EventKind,
    ExecutionEvent,
    ExecutionRequest,
    FabricLanguage,
    FabricTarget,
    Profile,
    TransportKind,
)
from fabric_jupyter.transport import FabricTransport

WORKSPACE = "11111111-1111-1111-1111-111111111111"
NOTEBOOK = "22222222-2222-2222-2222-222222222222"


class PolicyTransport(FabricTransport):
    def __init__(self) -> None:
        self.executions: list[ExecutionRequest] = []
        self.interrupted: list[FabricTarget] = []
        self.shutdown_targets: list[FabricTarget] = []

    async def start(self) -> dict[str, object]:
        return {"remoteSession": True}

    async def execute(self, request: ExecutionRequest) -> AsyncIterator[ExecutionEvent]:
        self.executions.append(request)
        yield ExecutionEvent(
            EventKind.RESULT,
            {"data": {"text/plain": "executed"}, "metadata": {}, "execution_count": 1},
        )

    async def interrupt(self, target: FabricTarget) -> None:
        self.interrupted.append(target)

    async def shutdown(self, target: FabricTarget) -> None:
        self.shutdown_targets.append(target)

    def status(self) -> dict[str, object]:
        return {"remoteSession": True}


def profile() -> Profile:
    target = FabricTarget(WORKSPACE, NOTEBOOK, FabricLanguage.PYSPARK)
    return Profile(
        name="test",
        language=FabricLanguage.PYSPARK,
        transport=TransportKind.FABRIC,
        target=target,
    )


def test_requests_are_bound_to_server_policy(local_state: Path) -> None:
    async def run() -> None:
        active_profile = profile()
        target = active_profile.target
        assert target is not None
        transport = PolicyTransport()
        server = BrokerServer(profile=active_profile, transport=transport)
        endpoint = await server.start()
        client = BrokerClient(endpoint)
        try:
            for changed in (
            replace(target, workspace_id="33333333-3333-3333-3333-333333333333"),
            replace(target, notebook_id="44444444-4444-4444-4444-444444444444"),
                replace(target, language=FabricLanguage.SPARK),
            ):
                with pytest.raises(RuntimeError, match="not authorized"):
                    await client.execute(
                        request_id="wrong-target", target=changed, code="not evaluated",
                        silent=False, transport=TransportKind.FABRIC,
                    )
                with pytest.raises(RuntimeError, match="not authorized"):
                    await client.interrupt(changed)
                with pytest.raises(RuntimeError, match="not authorized"):
                    await client.shutdown(changed)
            with pytest.raises(RuntimeError, match="not a valid TransportKind"):
                await client._request(
                    "execute",
                    {
                        "requestId": "wrong-transport",
                        "target": target.to_dict(),
                        "code": "not evaluated",
                        "transport": "experimental",
                    },
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
            await client.connect(target, TransportKind.FABRIC)
            events = await client.execute(
                request_id="allowed", target=target, code="not evaluated",
                silent=False, transport=TransportKind.FABRIC,
            )
            assert any(event.kind.value == "result" for event in events)
            assert all(session["remoteSession"] is True for session in await client.status())
            result = next(event for event in events if event.kind.value == "result")
            assert result.content["data"]["text/plain"] == "executed"
        finally:
            await server.close()

    asyncio.run(run())


def test_empty_target_policy_does_not_accept_client_selected_target(local_state: Path) -> None:
    with pytest.raises(ValueError):
        BrokerServer(
            profile=Profile(
                name="unbound",
                language=FabricLanguage.PYSPARK,
                transport=TransportKind.FABRIC,
            )
        )


def test_private_kernel_brokers_do_not_replace_each_other(local_state: Path) -> None:
    async def run() -> None:
        first, second = BrokerServer(profile=profile()), BrokerServer(profile=profile())
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
        server = BrokerServer(profile=profile())
        with pytest.raises(PermissionError, match="cannot be verified"):
            await server.start()
        assert server._server is None
        assert server._endpoint is None

    asyncio.run(run())


def test_broker_never_binds_external_tcp_address(local_state: Path) -> None:
    from fabric_jupyter.broker import BrokerEndpoint

    async def run() -> None:
        server = BrokerServer(
            profile=profile(),
            endpoint=BrokerEndpoint("tcp", "0.0.0.0:0", "offline-test"),
        )
        with pytest.raises(ValueError, match="127.0.0.1"):
            await server.start()
        assert server._server is None

    asyncio.run(run())


def test_owned_descriptor_is_removed_on_orderly_shutdown(local_state: Path) -> None:
    from fabric_jupyter.paths import broker_endpoint_path

    async def run() -> None:
        server = BrokerServer(profile=profile())
        await server.start(persist_endpoint=True)
        descriptor = broker_endpoint_path()
        assert descriptor.exists()
        second = BrokerServer(profile=profile())
        with pytest.raises(FileExistsError, match="owning broker"):
            await second.start(persist_endpoint=True)
        assert second._server is None
        await server.close()
        assert not descriptor.exists()

    asyncio.run(run())
