from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock

import pytest

from fabric_jupyter.fabric_runtime import NotebookRuntimeTransport, parse_frame
from fabric_jupyter.models import EventKind, ExecutionRequest, FabricLanguage, FabricTarget
from fabric_jupyter.runtime_auth import RuntimeFailure

TARGET = FabricTarget(
    "11111111-1111-1111-1111-111111111111",
    "22222222-2222-2222-2222-222222222222",
    FabricLanguage.PYSPARK,
)


def test_unverified_pure_python_runtime_is_rejected_before_auth():
    with pytest.raises(RuntimeFailure, match="real Python runtime is not supported"):
        NotebookRuntimeTransport(FabricTarget(TARGET.workspace_id, TARGET.notebook_id, FabricLanguage.PYTHON))


def frame(kind, content, parent=None):
    return {
        "channel": "iopub", "header": {"msg_type": kind},
        "parent_header": {"msg_id": parent} if parent else None,
        "content": content, "metadata": {}, "buffers": [],
    }


def test_unsolicited_null_parent_is_not_a_protocol_failure():
    parsed = parse_frame(json.dumps(frame("comm_msg", {"data": {}})))
    assert parsed["parent_header"] == {}


@pytest.mark.parametrize("value", ["not json", "[]", "{}", json.dumps({
    **frame("stream", {}), "buffers": ["unsupported"],
})])
def test_malformed_and_binary_protocol_rejected(value):
    with pytest.raises(RuntimeFailure):
        parse_frame(value)


def test_execute_streams_and_waits_for_both_reply_and_idle():
    async def run():
        runtime = NotebookRuntimeTransport(TARGET)
        runtime._ready = True

        class Socket:
            async def send(self, raw):
                request = json.loads(raw)
                assert request["header"]["msg_type"] == "execute_request"
                assert request["content"]["store_history"] is True
                assert request["content"]["allow_stdin"] is False
                queue = runtime._pending[request["header"]["msg_id"]]
                queue.put_nowait(frame("stream", {"name": "stdout", "text": "marker\n"}))
                queue.put_nowait(frame("execute_reply", {"status": "ok"}))
                queue.put_nowait(frame("execute_result", {"data": {"text/plain": "42"}}))
                queue.put_nowait(frame("status", {"execution_state": "idle"}))

        runtime._ws = Socket()
        events = [event async for event in runtime.execute(ExecutionRequest("test", TARGET, "6 * 7"))]
        assert [event.kind for event in events] == [EventKind.STREAM, EventKind.RESULT]
        assert not runtime._pending
        await runtime._access.close()

    asyncio.run(run())


def test_missing_idle_channel_failure_cannot_report_success():
    async def run():
        runtime = NotebookRuntimeTransport(TARGET)
        runtime._ready = True

        class Socket:
            async def send(self, raw):
                request = json.loads(raw)
                queue = runtime._pending[request["header"]["msg_id"]]
                queue.put_nowait(frame("execute_reply", {"status": "ok"}))
                queue.put_nowait({"failure": "closed"})

        runtime._ws = Socket()
        with pytest.raises(RuntimeFailure, match="before execute_reply and idle"):
            async for _ in runtime.execute(ExecutionRequest("test", TARGET, "6 * 7")):
                pass
        await runtime._access.close()

    asyncio.run(run())


def test_shutdown_stops_only_allocated_session_and_verifies_404():
    async def run():
        requests = []

        class Access:
            async def runtime_request(self, method, path, **kwargs):
                requests.append((method, path, kwargs))
                return (404, None) if method == "GET" else (204, None)

            async def close(self):
                pass

        runtime = NotebookRuntimeTransport(TARGET, access=Access())
        runtime._session_id = "33333333-3333-3333-3333-333333333333"
        await runtime.shutdown(TARGET)
        await runtime.shutdown(TARGET)
        assert [(method, path) for method, path, _ in requests] == [
            ("DELETE", "/api/sessions/33333333-3333-3333-3333-333333333333"),
            ("GET", "/api/sessions/33333333-3333-3333-3333-333333333333"),
        ]
        assert requests[-1][2]["expected"] == (404,)

    asyncio.run(run())


def test_foreign_target_cannot_interrupt_or_shutdown():
    async def run():
        other = FabricTarget(TARGET.workspace_id, "33333333-3333-3333-3333-333333333333", TARGET.language)
        runtime = NotebookRuntimeTransport(TARGET)
        try:
            for operation in (runtime.interrupt, runtime.shutdown):
                with pytest.raises(RuntimeFailure, match="authorized startup"):
                    await operation(other)
        finally:
            await runtime._access.close()

    asyncio.run(run())


def test_startup_failure_deletes_exact_owned_allocation_without_retrying_create():
    async def run():
        calls = []

        class Access:
            async def runtime_request(self, method, path, **kwargs):
                calls.append((method, path))
                if method == "POST":
                    return 201, {
                        "id": "33333333-3333-3333-3333-333333333333",
                        "kernel": {"id": "44444444-4444-4444-4444-444444444444", "name": "synapse_pyspark"},
                    }
                return (404, None) if method == "GET" else (204, None)

            async def close(self):
                pass

        runtime = NotebookRuntimeTransport(TARGET, access=Access())
        runtime._open_channel = AsyncMock()
        runtime._kernel_info = AsyncMock(side_effect=RuntimeFailure("remote info failed"))
        with pytest.raises(RuntimeFailure, match="owned session cleanup completed"):
            await runtime.start()
        assert calls == [
            ("POST", "/api/sessions"),
            ("DELETE", "/api/sessions/33333333-3333-3333-3333-333333333333"),
            ("GET", "/api/sessions/33333333-3333-3333-3333-333333333333"),
        ]
        assert runtime._session.session == "33333333-3333-3333-3333-333333333333"

    asyncio.run(run())


def test_control_configuration_has_bounded_timeout_and_no_data_attachments():
    async def run():
        runtime = NotebookRuntimeTransport(TARGET, idle_timeout_seconds=120)
        runtime._send = AsyncMock(return_value="message")
        runtime._control = AsyncMock()
        await runtime._configure()
        commands = [(call.args[0], call.args[1] if len(call.args) > 1 else None)
                    for call in runtime._control.await_args_list]
        assert commands[0][0] == "set_livy_session_options"
        options = commands[0][1]
        assert options["conf"]["spark.synapse.nbs.session.timeout"] == "120000"
        assert "lakehouseDetails" not in options and "environmentDetails" not in options
        assert commands[-1] == ("start_livy_session", {})
        await runtime._access.close()

    asyncio.run(run())
