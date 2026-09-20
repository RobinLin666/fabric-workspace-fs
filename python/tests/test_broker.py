from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from fabric_jupyter.broker import BrokerClient, BrokerEndpoint, BrokerServer, load_endpoint
from fabric_jupyter.models import FabricLanguage, FabricTarget, Profile, TransportKind
from fabric_jupyter.transport import FakeFabricTransport

WORKSPACE = "11111111-1111-1111-1111-111111111111"
NOTEBOOK = "22222222-2222-2222-2222-222222222222"


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
