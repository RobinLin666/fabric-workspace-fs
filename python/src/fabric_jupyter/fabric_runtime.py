"""Opt-in Notebook runtime protocol, independently implemented from wire behavior.

The Notebook workload protocol is private and version-sensitive. Never fall back
to local evaluation or retry a possibly executed cell after a channel failure.
"""

from __future__ import annotations

import asyncio
import json
import logging
import secrets
import time
from collections.abc import AsyncIterator
from contextlib import suppress
from typing import Any
from uuid import uuid4

from jupyter_client.session import Session
from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import WebSocketException
from websockets.typing import Subprotocol

from .models import EventKind, ExecutionEvent, ExecutionRequest, FabricLanguage, FabricTarget
from .runtime_auth import RuntimeAccess, RuntimeFailure, uuid_text
from .transport import FabricTransport

START_TIMEOUT = 600
EXECUTE_TIMEOUT = 300
MAX_FRAME = 1024 * 1024


class _NoRedirectConnect(connect):
    def process_redirect(self, exc: Exception) -> Exception:
        return RuntimeFailure("runtime WebSocket redirect rejected")


def _quiet_logger() -> logging.Logger:
    # websockets DEBUG includes handshake headers, so never use its global logger.
    logger = logging.Logger("fabric-jupyter-private-channel", level=logging.CRITICAL + 1)
    logger.addHandler(logging.NullHandler())
    logger.propagate = False
    return logger


def parse_frame(raw: str | bytes) -> dict[str, Any]:
    if len(raw) > MAX_FRAME:
        raise RuntimeFailure("runtime channel frame exceeds 1 MiB limit")
    try:
        value = json.loads(raw)
    except (ValueError, UnicodeDecodeError):
        raise RuntimeFailure("runtime channel sent unsupported non-JSON framing") from None
    if isinstance(value, dict) and value.get("parent_header") is None:
        value["parent_header"] = {}
    if (
        not isinstance(value, dict) or not isinstance(value.get("header"), dict)
        or not isinstance(value["header"].get("msg_type"), str)
        or not isinstance(value.get("parent_header"), dict)
        or not isinstance(value.get("content"), dict)
        or value.get("channel") not in ("shell", "iopub", "control", "stdin")
        or value.get("buffers")
    ):
        raise RuntimeFailure("runtime channel response has an unsupported schema or binary buffers")
    return value


class NotebookRuntimeTransport(FabricTransport):
    def __init__(
        self, target: FabricTarget, idle_timeout_seconds: int = 900, *,
        access: RuntimeAccess | None = None,
    ) -> None:
        if target.language is not FabricLanguage.PYSPARK:
            raise RuntimeFailure("real Python runtime is not supported; use PySpark or offline fake")
        self.target = target
        self.idle_timeout_seconds = idle_timeout_seconds
        self._access = access or RuntimeAccess(target)
        # Session builds canonical dictionaries only; WebSocket authentication
        # replaces ZMQ signing. A nonempty key avoids disabling the local app's
        # shared Session signing configuration when embedded in ipykernel.
        self._session = Session(key=secrets.token_bytes(32), username="fabric-jupyter")
        self._ws: ClientConnection | None = None
        self._listener: asyncio.Task[None] | None = None
        self._refresh: asyncio.Task[None] | None = None
        self._pending: dict[str, asyncio.Queue[dict[str, Any]]] = {}
        self._comms: dict[str, str] = {}
        self._start_lock = asyncio.Lock()
        self._execute_lock = asyncio.Lock()
        self._failure: str | None = None
        self._session_id: str | None = None
        self._kernel_id: str | None = None
        self._livy_id: str | int | None = None
        self._state = "not_started"
        self._ready = False
        self._stopped = False
        self._shutdown_failure: str | None = None
        self._session_deleted = False
        self._stop_verified = False
        self._channel_expiry = 0.0
        self._created_name = "fabric-jupyter-" + uuid4().hex

    def status(self) -> dict[str, Any]:
        return {
            "remoteSession": self._session_id is not None and not self._session_deleted,
            "sessionId": self._session_id, "kernelId": self._kernel_id,
            "livySessionId": self._livy_id, "runtimeState": self._state,
            "ready": self._ready, "stopVerified": self._stop_verified,
            "sessionDeleted": self._session_deleted,
        }

    def _check_target(self, target: FabricTarget) -> None:
        if (target.workspace_id, target.notebook_id, target.language) != (
            self.target.workspace_id, self.target.notebook_id, self.target.language,
        ):
            raise RuntimeFailure("runtime target differs from authorized startup policy")

    async def start(self) -> dict[str, Any]:
        async with self._start_lock:
            if self._ready:
                self._check_health()
                return self.status()
            if self._stopped:
                raise RuntimeFailure("runtime was shut down; start a new kernel explicitly")
            try:
                async with asyncio.timeout(START_TIMEOUT):
                    await self._allocate()
                    await self._open_channel()
                    await self._kernel_info()
                    await self._configure()
                    await self._wait_state({"idle"}, START_TIMEOUT - 30)
                    self._ready = True
                    self._refresh = asyncio.create_task(self._refresh_loop())
                    return self.status()
            except (RuntimeFailure, TimeoutError, WebSocketException, OSError, asyncio.CancelledError) as exc:
                reason = str(exc) if isinstance(exc, RuntimeFailure) else type(exc).__name__
                try:
                    await self.shutdown(self.target)
                except RuntimeFailure:
                    if isinstance(exc, asyncio.CancelledError):
                        raise
                    raise RuntimeFailure(
                        "runtime startup failed and cleanup could not be verified; "
                        "inspect broker-status and stop the owned session in Fabric"
                    ) from None
                raise RuntimeFailure(
                    f"runtime startup failed: {reason}; owned session cleanup completed"
                ) from None

    async def _allocate(self) -> None:
        kernel = "synapse_pyspark"
        try:
            _, model = await self._access.runtime_request(
                "POST", "/api/sessions", expected=(201,),
                body={
                    "name": self._created_name, "path": self._created_name + ".ipynb",
                    "type": "notebook", "kernel": {"name": kernel},
                },
            )
        except RuntimeFailure:
            # POST is never retried: recover only an exact uniquely-owned allocation.
            _, sessions = await self._access.runtime_request("GET", "/api/sessions")
            if not isinstance(sessions, list):
                raise RuntimeFailure("cannot reconcile uncertain runtime allocation") from None
            matches = [item for item in sessions if isinstance(item, dict) and item.get("name") == self._created_name]
            if len(matches) != 1:
                raise RuntimeFailure(
                    "runtime allocation failed; no unique owned allocation could be recovered"
                ) from None
            model = matches[0]
        if not isinstance(model, dict):
            raise RuntimeFailure("session allocation response is not an object")
        self._session_id = uuid_text(model.get("id"))
        self._session.session = self._session_id
        kernel_model = model.get("kernel")
        if not isinstance(kernel_model, dict):
            raise RuntimeFailure("session allocation response has no kernel")
        self._kernel_id = uuid_text(kernel_model.get("id"))
        if kernel_model.get("name") != kernel:
            raise RuntimeFailure("allocated kernel language does not match the profile")

    async def _open_channel(self) -> None:
        grant = await self._access.grant()
        self._channel_expiry = grant.expires
        url = (
            self._access.base(grant).replace("https://", "wss://", 1)
            + f"/api/kernels/{self._kernel_id}/channels"
        )
        # Subprotocol authentication is the desktop Notebook client's convention.
        # Credentials are never in URLs, logs, profiles or persisted state.
        try:
            self._ws = await _NoRedirectConnect(
                url, subprotocols=[Subprotocol("synapse"), Subprotocol("MwcToken%20" + grant.token)],
                open_timeout=30, close_timeout=10, max_size=MAX_FRAME,
                max_queue=32, proxy=None, logger=_quiet_logger(),
            )
        except (WebSocketException, OSError):
            raise RuntimeFailure(
                "runtime WebSocket handshake failed; no redirect or alternate auth attempted"
            ) from None
        self._listener = asyncio.create_task(self._listen())

    def _check_health(self) -> None:
        if self._failure:
            raise RuntimeFailure(self._failure)
        if self._ws is None:
            raise RuntimeFailure("runtime channel is not connected")

    async def _send(self, kind: str, content: dict[str, Any], *, channel: str = "shell") -> str:
        self._check_health()
        assert self._ws is not None
        message = self._session.msg(kind, content)
        message["channel"] = channel
        raw = json.dumps(message, default=lambda value: value.isoformat(), separators=(",", ":"))
        if len(raw.encode("utf-8")) > MAX_FRAME:
            raise RuntimeFailure("runtime request exceeds 1 MiB limit")
        try:
            await self._ws.send(raw)
        except (WebSocketException, OSError):
            raise RuntimeFailure("runtime channel send failed; request will not be replayed") from None
        return str(message["header"]["msg_id"])

    async def _request(self, kind: str, content: dict[str, Any]) -> tuple[str, asyncio.Queue[dict[str, Any]]]:
        # Sending and registration cannot interleave with listener before send returns
        # reliably on all connectors, so register from a pre-generated msg_id.
        self._check_health()
        assert self._ws is not None
        message = self._session.msg(kind, content)
        message["channel"] = "shell"
        raw = json.dumps(message, default=lambda value: value.isoformat())
        if len(raw.encode("utf-8")) > MAX_FRAME:
            raise RuntimeFailure("runtime request exceeds 1 MiB limit")
        message_id = str(message["header"]["msg_id"])
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=32)
        self._pending[message_id] = queue
        try:
            await self._ws.send(raw)
        except (WebSocketException, OSError):
            self._pending.pop(message_id, None)
            raise RuntimeFailure("runtime request send failed; request will not be replayed") from None
        return message_id, queue

    async def _listen(self) -> None:
        assert self._ws is not None
        try:
            async for raw in self._ws:
                message = parse_frame(raw)
                kind = message["header"]["msg_type"]
                if kind == "comm_msg":
                    data = message["content"].get("data", {})
                    if isinstance(data, dict) and data.get("status_type") == "user_token_expired":
                        await self._refresh_authorization()
                    if isinstance(data, dict) and data.get("status_type") == "livy_session_status":
                        content = data.get("content", {})
                        if isinstance(content, dict):
                            state = content.get("session_state")
                            if isinstance(state, str):
                                self._state = state.lower()
                            identity = content.get("session_id")
                            if isinstance(identity, (str, int)):
                                self._livy_id = identity
                parent = message["parent_header"].get("msg_id")
                if isinstance(parent, str) and parent in self._pending:
                    self._pending[parent].put_nowait(message)
            if not self._stopped:
                self._failure = "runtime channel closed before shutdown"
        except (RuntimeFailure, WebSocketException, OSError, asyncio.QueueFull):
            self._failure = "runtime channel failed or returned unsupported/oversized protocol data"
        finally:
            for queue in self._pending.values():
                if not queue.full():
                    queue.put_nowait({"failure": self._failure or "runtime channel closed"})

    async def _kernel_info(self) -> None:
        key, queue = await self._request("kernel_info_request", {})
        try:
            async with asyncio.timeout(30):
                while True:
                    frame = await queue.get()
                    if "failure" in frame:
                        raise RuntimeFailure("runtime kernel_info channel failed")
                    if frame["header"]["msg_type"] == "kernel_info_reply":
                        if frame["content"].get("status", "ok") != "ok":
                            raise RuntimeFailure("remote kernel_info reported an error")
                        return
        finally:
            self._pending.pop(key, None)

    async def _control(self, kind: str, content: Any = None) -> None:
        data = {"type": kind}
        if content is not None:
            data["content"] = content
        await self._send("comm_msg", {"comm_id": self._comms["control"], "data": data})

    async def _configure(self) -> None:
        for name in ("control", "status", "query"):
            self._comms[name] = str(uuid4())
            await self._send("comm_open", {
                "comm_id": self._comms[name], "target_name": "synapse:" + name, "data": {},
            })
        await self._control("set_livy_session_options", {
            "isQueueable": False,
            "conf": {
                "spark.synapse.context.notebookname": self._created_name,
                "spark.synapse.nbs.session.timeout": str(self.idle_timeout_seconds * 1000),
            },
        })
        await self._control("set_kernel_options", {
            "enableDebugMode": False, "enableSparkJob": False, "enableSparkAdvice": False,
            "deleteKernelOnComputeSessionEnd": False,
        })
        await self._control("set_language", "pyspark")
        await self._control("start_livy_session", {})

    async def _wait_state(self, states: set[str], timeout: float) -> None:
        async with asyncio.timeout(timeout):
            while self._state not in states:
                self._check_health()
                if self._state in {"error", "failed", "dead", "killed"} and "dead" not in states:
                    raise RuntimeFailure("Fabric compute startup reported a terminal failure")
                await self._send("comm_msg", {
                    "comm_id": self._comms["status"], "data": {"status_type": "livy_session_status"},
                })
                await asyncio.sleep(2)

    async def _refresh_authorization(self) -> None:
        grant = await self._access.grant(force=True)
        await self._control("set_authorization_header", "MwcToken " + grant.token)
        self._channel_expiry = grant.expires

    async def _refresh_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(60)
                if time.time() + 180 >= self._channel_expiry:
                    await self._refresh_authorization()
        except (RuntimeFailure, OSError):
            self._failure = "runtime authorization refresh failed; reconnect explicitly"

    async def execute(self, request: ExecutionRequest) -> AsyncIterator[ExecutionEvent]:
        self._check_target(request.target)
        await self.start()
        async with self._execute_lock:
            key, queue = await self._request("execute_request", {
                "code": request.code, "silent": request.silent, "store_history": not request.silent,
                "user_expressions": {}, "allow_stdin": False, "stop_on_error": True,
            })
            reply = idle = False
            try:
                async with asyncio.timeout(EXECUTE_TIMEOUT):
                    while not (reply and idle):
                        frame = await queue.get()
                        if "failure" in frame:
                            raise RuntimeFailure("runtime channel closed before execute_reply and idle")
                        kind, content = frame["header"]["msg_type"], frame["content"]
                        if kind == "execute_reply":
                            if content.get("status") not in {"ok", "error", "abort", "aborted"}:
                                raise RuntimeFailure("runtime execute_reply has an unknown status")
                            reply = True
                            if content["status"] != "ok":
                                yield ExecutionEvent(EventKind.ERROR, {
                                    "ename": str(content.get("ename", "ExecutionAborted")),
                                    "evalue": str(content.get("evalue", "remote execution aborted")),
                                    "traceback": content.get("traceback", []),
                                })
                        elif kind == "status":
                            idle = content.get("execution_state") == "idle"
                        elif kind == "stream":
                            yield ExecutionEvent(EventKind.STREAM, content)
                        elif kind == "execute_result":
                            yield ExecutionEvent(EventKind.RESULT, content)
                        elif kind == "error":
                            yield ExecutionEvent(EventKind.ERROR, content)
                        elif kind in ("display_data", "update_display_data", "clear_output"):
                            yield ExecutionEvent(EventKind(kind), content)
                        elif kind in ("comm_open", "comm_msg", "comm_close"):
                            yield ExecutionEvent(EventKind(kind), content)
                        elif kind == "input_request":
                            raise RuntimeFailure("runtime requested unsupported interactive input")
            except (TimeoutError, asyncio.CancelledError) as exc:
                await self.interrupt(self.target)
                if isinstance(exc, asyncio.CancelledError):
                    raise
                raise RuntimeFailure(
                    "runtime execution timed out and interrupt was requested; code was not retried"
                ) from None
            finally:
                self._pending.pop(key, None)

    async def interrupt(self, target: FabricTarget) -> None:
        self._check_target(target)
        if self._kernel_id is None:
            raise RuntimeFailure("no owned runtime kernel exists to interrupt")
        await self._access.runtime_request(
            "POST", f"/api/kernels/{self._kernel_id}/interrupt", expected=(200, 204),
        )

    async def shutdown(self, target: FabricTarget) -> None:
        self._check_target(target)
        if self._stopped:
            if self._shutdown_failure:
                raise RuntimeFailure(self._shutdown_failure)
            return
        stop_error: RuntimeFailure | None = None
        try:
            if self._ws is not None and "control" in self._comms and not self._failure:
                try:
                    await self._control("stop_livy_session")
                    await self._wait_state({"dead", "killed", "stopped", "not_started"}, 60)
                    self._stop_verified = True
                except (RuntimeFailure, TimeoutError):
                    stop_error = RuntimeFailure("remote compute stop status could not be verified")
            if self._session_id is not None:
                await self._access.runtime_request(
                    "DELETE", f"/api/sessions/{self._session_id}", expected=(200, 204, 404),
                )
                code, _ = await self._access.runtime_request(
                    "GET", f"/api/sessions/{self._session_id}", expected=(404,),
                )
                if code != 404:
                    raise RuntimeFailure("owned Jupyter session deletion could not be verified")
                self._session_deleted = True
            if stop_error is not None:
                raise stop_error
        except (RuntimeFailure, TimeoutError, asyncio.CancelledError):
            self._shutdown_failure = "remote shutdown could not be fully verified; inspect the owned Fabric session"
            raise
        finally:
            self._stopped = True
            self._ready = False
            for task in (self._refresh, self._listener):
                if task is not None:
                    task.cancel()
                    with suppress(asyncio.CancelledError):
                        await task
            if self._ws is not None:
                await self._ws.close()
            self._ws = None
            await self._access.close()
