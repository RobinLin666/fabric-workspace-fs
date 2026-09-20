from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

from fabric_jupyter.kernel import FabricKernel


def test_control_interrupt_uses_broker_not_local_process_signals():
    async def run():
        session = SimpleNamespace(send=Mock())
        kernel = SimpleNamespace(session=session, do_interrupt=AsyncMock(return_value={"status": "ok"}))
        parent = {"header": {"msg_id": "control-test"}}
        await FabricKernel.interrupt_request(kernel, "stream", "identity", parent)
        kernel.do_interrupt.assert_awaited_once()
        session.send.assert_called_once_with(
            "stream", "interrupt_reply", {"status": "ok"}, parent, ident="identity",
        )

    asyncio.run(run())
