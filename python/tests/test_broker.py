from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from pathlib import Path

import pytest

from fabric_jupyter.broker import BrokerClient, BrokerEndpoint, BrokerServer, load_endpoint
from fabric_jupyter.models import (
    EventKind,
    ExecutionEvent,
    ExecutionRequest,
    FabricLanguage,
    FabricTarget,
    Profile,
    TransportKind,
)
from fabric_jupyter.transport import FabricTransport, FakeFabricTransport

WORKSPACE = "11111111-1111-1111-1111-111111111111"
NOTEBOOK = "22222222-2222-2222-2222-222222222222"


class MockRuntimeTransport(FabricTransport):
    def __init__(self) -> None:
        self.started = 0
        self.interrupted = 0
        self.stopped = 0

    async def start(self) -> dict[str, object]:
        self.started += 1
        return {"ready": True, "accessToken": "must-not-escape"}

    async def execute(self, request: ExecutionRequest) -> AsyncIterator[ExecutionEvent]:
        del request
        yield ExecutionEvent(EventKind.STREAM, {"name": "stdout", "text": "first\n"})
        await asyncio.sleep(0)
        yield ExecutionEvent(EventKind.DISPLAY_DATA, {"data": {"text/plain": "second"}})
        yield ExecutionEvent(
            EventKind.UPDATE_DISPLAY_DATA,
            {"data": {"text/plain": "updated"}, "transient": {"display_id": "statement"}},
        )
        yield ExecutionEvent(EventKind.CLEAR_OUTPUT, {"wait": True})

    async def interrupt(self, target: FabricTarget) -> None:
        del target
        self.interrupted += 1

    async def shutdown(self, target: FabricTarget) -> None:
        del target
        self.stopped += 1

    def status(self) -> dict[str, object]:
        return {"ready": True, "authorization": "must-not-escape"}


class UnverifiedShutdownTransport(MockRuntimeTransport):
    async def shutdown(self, target: FabricTarget) -> None:
        del target
        raise RuntimeError("remote stop was not verified")


def test_broker_auth_execute_and_cleanup(local_state: Path) -> None:
    async def run() -> None:
        transport = FakeFabricTransport()
        target = FabricTarget(WORKSPACE, NOTEBOOK, FabricLanguage.PYSPARK)
        server = BrokerServer(
            transport=transport, idle_timeout_seconds=60,
            profile=Profile(name="test", language=FabricLanguage.PYSPARK, target=target),
        )
        endpoint = await server.start(persist_endpoint=True)
        assert load_endpoint().public_dict() == endpoint.public_dict()
        events = await BrokerClient(endpoint).execute(
            request_id="request-1",
            target=target,
            code="print('safe')",
            silent=False,
            transport=TransportKind.FAKE,
        )
        assert [event.kind.value for event in events] == ["status", "stream", "result", "status"]
        assert len(await BrokerClient(endpoint).status()) == 1
        await BrokerClient(endpoint).shutdown(target)
        assert transport.shutdown_targets == [target]
        await server.close()

    asyncio.run(run())


def test_fabric_broker_requires_connect_streams_and_redacts_status(
    local_state: Path,
) -> None:
    async def run() -> None:
        target = FabricTarget(WORKSPACE, NOTEBOOK, FabricLanguage.PYSPARK)
        profile = Profile(
            name="real",
            language=FabricLanguage.PYSPARK,
            transport=TransportKind.FABRIC,
            target=target,
        )
        transport = MockRuntimeTransport()
        server = BrokerServer(profile=profile, transport=transport)
        endpoint = await server.start()
        client = BrokerClient(endpoint)
        try:
            with pytest.raises(RuntimeError, match="connect first"):
                await client.execute(
                    request_id="before-connect",
                    target=target,
                    code="ignored",
                    silent=False,
                    transport=TransportKind.FABRIC,
                )
            connected = await client.connect(target, TransportKind.FABRIC)
            assert connected["remoteSession"] is True
            assert connected["remoteStatus"]["accessToken"] == "<redacted>"
            kinds = [
                event.kind
                async for event in client.stream_execute(
                    request_id="stream",
                    target=target,
                    code="ignored",
                    silent=False,
                    transport=TransportKind.FABRIC,
                )
            ]
            assert kinds == [
                EventKind.STATUS,
                EventKind.STREAM,
                EventKind.DISPLAY_DATA,
                EventKind.UPDATE_DISPLAY_DATA,
                EventKind.CLEAR_OUTPUT,
                EventKind.STATUS,
            ]
            status = (await client.status())[0]
            assert status["remoteStatus"]["authorization"] == "<redacted>"
            await client.interrupt(target)
            await client.shutdown(target)
            assert (transport.started, transport.interrupted, transport.stopped) == (1, 1, 1)
        finally:
            await server.close()

    asyncio.run(run())


def test_fabric_broker_close_surfaces_unverified_remote_shutdown(
    local_state: Path,
) -> None:
    async def run() -> None:
        target = FabricTarget(WORKSPACE, NOTEBOOK, FabricLanguage.PYSPARK)
        profile = Profile(
            name="real",
            language=FabricLanguage.PYSPARK,
            transport=TransportKind.FABRIC,
            target=target,
        )
        server = BrokerServer(profile=profile, transport=UnverifiedShutdownTransport())
        endpoint = await server.start()
        await BrokerClient(endpoint).connect(target, TransportKind.FABRIC)
        with pytest.raises(RuntimeError, match="not verified"):
            await server.close()
        assert server._server is None
        assert server._sessions == {}

    asyncio.run(run())


def test_fabric_broker_recreates_terminal_transport_after_shutdown(
    local_state: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from fabric_jupyter import broker as broker_module

    async def run() -> None:
        target = FabricTarget(WORKSPACE, NOTEBOOK, FabricLanguage.PYSPARK)
        profile = Profile(
            name="real",
            language=FabricLanguage.PYSPARK,
            transport=TransportKind.FABRIC,
            target=target,
        )
        transports = [MockRuntimeTransport(), MockRuntimeTransport()]

        def create_transport(*args, **kwargs):
            del args, kwargs
            return transports.pop(0)

        monkeypatch.setattr(broker_module, "make_transport", create_transport)
        server = BrokerServer(profile=profile)
        first = server._transport
        endpoint = await server.start()
        client = BrokerClient(endpoint)
        await client.connect(target, TransportKind.FABRIC)
        await client.shutdown(target)
        assert isinstance(first, MockRuntimeTransport)
        assert first.stopped == 1
        assert server._transport is None
        await client.connect(target, TransportKind.FABRIC)
        second = server._transport
        assert isinstance(second, MockRuntimeTransport) and second is not first
        assert second.started == 1
        await server.close()

    asyncio.run(run())


@pytest.mark.parametrize("invalid_auth", ["wrong", "\u2603"])
def test_broker_rejects_wrong_auth(local_state: Path, invalid_auth: str) -> None:
    async def run() -> None:
        server = BrokerServer()
        endpoint = await server.start()
        attacker = BrokerClient(BrokerEndpoint(endpoint.transport, endpoint.address, invalid_auth))
        with pytest.raises(RuntimeError, match="unauthorized"):
            await attacker.status()
        await server.close()

    asyncio.run(run())
