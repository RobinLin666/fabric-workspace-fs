"""Single-user authenticated local broker with Unix socket / loopback endpoints."""

from __future__ import annotations

import asyncio
import hmac
import json
import os
import secrets
import time
from collections.abc import Awaitable, Mapping
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from .models import (
    BrokerSession,
    EventKind,
    ExecutionEvent,
    FabricTarget,
    SessionState,
    TransportKind,
)
from .paths import broker_endpoint_path, broker_socket_path, ensure_private
from .protocol import (
    PROTOCOL_VERSION,
    decode_message,
    encode_message,
    event_from_message,
    event_message,
    parse_execute,
    request_from_message,
)
from .transport import FabricTransport, make_transport


@dataclass(frozen=True, slots=True)
class BrokerEndpoint:
    """Private broker descriptor. Its auth value must never be printed or logged."""

    transport: str
    address: str
    auth: str

    def public_dict(self) -> dict[str, str]:
        return {"transport": self.transport, "address": self.address, "auth": "<redacted>"}

    def to_private_dict(self) -> dict[str, Any]:
        return {
            "version": PROTOCOL_VERSION,
            "transport": self.transport,
            "address": self.address,
            "auth": self.auth,
        }

    @classmethod
    def from_private_dict(cls, value: Mapping[str, Any]) -> BrokerEndpoint:
        if value.get("version") != PROTOCOL_VERSION:
            raise ValueError("unsupported broker endpoint version")
        transport, address, auth = value.get("transport"), value.get("address"), value.get("auth")
        if transport not in {"unix", "tcp"} or not all(
            isinstance(item, str) and item for item in (address, auth)
        ):
            raise ValueError("invalid broker endpoint")
        return cls(transport=transport, address=str(address), auth=str(auth))


class BrokerServer:
    """An in-process broker server. It never opens an external listener."""

    def __init__(
        self,
        *,
        transport: FabricTransport | None = None,
        idle_timeout_seconds: int = 900,
        endpoint: BrokerEndpoint | None = None,
    ) -> None:
        self._transport = transport or make_transport(TransportKind.FAKE)
        self._idle_timeout_seconds = idle_timeout_seconds
        self._endpoint = endpoint
        self._server: asyncio.AbstractServer | None = None
        self._sessions: dict[tuple[str, str, str], BrokerSession] = {}
        self._cleanup_task: asyncio.Task[None] | None = None

    @property
    def endpoint(self) -> BrokerEndpoint:
        if self._endpoint is None:
            raise RuntimeError("broker has not started")
        return self._endpoint

    async def start(self, *, persist_endpoint: bool = False) -> BrokerEndpoint:
        if self._server is not None:
            return self.endpoint
        if self._endpoint is None:
            self._endpoint = self._new_endpoint()
        if self._endpoint.transport == "unix":
            path = Path(self._endpoint.address)
            ensure_private(path.parent)
            with suppress(FileNotFoundError):
                path.unlink()
            start_unix_server = getattr(asyncio, "start_unix_server", None)
            if start_unix_server is None:
                raise RuntimeError("Unix socket broker endpoints are unavailable on this platform")
            self._server = await cast(
                Awaitable[asyncio.AbstractServer],
                start_unix_server(self._handle, path=str(path)),
            )
            if os.name != "nt":
                path.chmod(0o600)
        else:
            host, port = self._endpoint.address.rsplit(":", 1)
            self._server = await asyncio.start_server(self._handle, host=host, port=int(port))
            actual_port = self._server.sockets[0].getsockname()[1]
            self._endpoint = BrokerEndpoint("tcp", f"127.0.0.1:{actual_port}", self._endpoint.auth)
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())
        if persist_endpoint:
            self.write_endpoint()
        return self.endpoint

    def _new_endpoint(self) -> BrokerEndpoint:
        auth = secrets.token_urlsafe(32)
        if os.name == "nt":
            return BrokerEndpoint("tcp", "127.0.0.1:0", auth)
        return BrokerEndpoint("unix", str(broker_socket_path()), auth)

    def write_endpoint(self) -> Path:
        location = broker_endpoint_path()
        temporary = location.with_suffix(".tmp")
        with open(temporary, "x", encoding="utf-8") as stream:
            if os.name != "nt":
                os.chmod(temporary, 0o600)
            json.dump(self.endpoint.to_private_dict(), stream, separators=(",", ":"))
        temporary.replace(location)
        if os.name != "nt":
            location.chmod(0o600)
        return location

    async def close(self) -> None:
        if self._cleanup_task is not None:
            self._cleanup_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._cleanup_task
            self._cleanup_task = None
        for session in list(self._sessions.values()):
            with suppress(RuntimeError):
                await self._transport.shutdown(session.target)
            session.state = SessionState.STOPPED
        self._sessions.clear()
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
        if self._endpoint is not None and self._endpoint.transport == "unix":
            with suppress(FileNotFoundError):
                Path(self._endpoint.address).unlink()

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            raw = await reader.readline()
            message = decode_message(raw)
            if not hmac.compare_digest(str(message.get("auth", "")), self.endpoint.auth):
                await self._send_error(writer, "unauthorized")
                return
            method, params = request_from_message(message)
            if method == "execute":
                await self._execute(params, writer)
            elif method == "interrupt":
                await self._interrupt(params, writer)
            elif method == "shutdown":
                await self._shutdown(params, writer)
            elif method == "status":
                await self._status(writer)
            else:
                await self._send_error(writer, "unknown method")
        except (ValueError, RuntimeError) as exc:
            await self._send_error(writer, str(exc))
        finally:
            writer.close()
            with suppress(Exception):
                await writer.wait_closed()

    async def _execute(self, params: Mapping[str, Any], writer: asyncio.StreamWriter) -> None:
        request = parse_execute(params)
        transport_kind = TransportKind(str(params.get("transport", "fake")))
        key = (
            request.target.workspace_id,
            request.target.notebook_id,
            request.target.language.value,
        )
        session = self._sessions.get(key)
        if session is None:
            session = BrokerSession(target=request.target, transport=transport_kind)
            self._sessions[key] = session
        if session.transport is not transport_kind:
            raise ValueError("target already has a session with a different transport")
        session.state = SessionState.BUSY
        session.last_used_monotonic = time.monotonic()
        await self._send(
            writer, event_message(ExecutionEvent(EventKind.STATUS, {"execution_state": "busy"}))
        )
        try:
            if transport_kind is not TransportKind.FAKE:
                transport = make_transport(transport_kind)
            else:
                transport = self._transport
            async for event in transport.execute(request):
                await self._send(writer, event_message(event))
        except RuntimeError as exc:
            await self._send(
                writer,
                event_message(
                    ExecutionEvent(
                        EventKind.ERROR,
                        {
                            "ename": "FabricTransportError",
                            "evalue": str(exc),
                            "traceback": [str(exc)],
                        },
                    )
                ),
            )
        finally:
            session.state = SessionState.IDLE
            session.last_used_monotonic = time.monotonic()
            await self._send(
                writer, event_message(ExecutionEvent(EventKind.STATUS, {"execution_state": "idle"}))
            )
            await self._send(writer, {"done": True})

    async def _interrupt(self, params: Mapping[str, Any], writer: asyncio.StreamWriter) -> None:
        target = _target_from_params(params)
        await self._transport.interrupt(target)
        await self._send(writer, {"ok": True})

    async def _shutdown(self, params: Mapping[str, Any], writer: asyncio.StreamWriter) -> None:
        target = _target_from_params(params)
        key = (target.workspace_id, target.notebook_id, target.language.value)
        session = self._sessions.pop(key, None)
        if session is not None:
            await self._transport.shutdown(target)
            session.state = SessionState.STOPPED
        await self._send(writer, {"ok": True})

    async def _status(self, writer: asyncio.StreamWriter) -> None:
        await self._send(
            writer, {"sessions": [session.public_dict() for session in self._sessions.values()]}
        )

    async def _cleanup_loop(self) -> None:
        while True:
            await asyncio.sleep(min(30, self._idle_timeout_seconds))
            deadline = time.monotonic() - self._idle_timeout_seconds
            for key, session in list(self._sessions.items()):
                if session.state is SessionState.IDLE and session.last_used_monotonic < deadline:
                    await self._transport.shutdown(session.target)
                    session.state = SessionState.STOPPED
                    del self._sessions[key]

    async def _send(self, writer: asyncio.StreamWriter, value: Mapping[str, Any]) -> None:
        writer.write(encode_message(value))
        await writer.drain()

    async def _send_error(self, writer: asyncio.StreamWriter, message: str) -> None:
        await self._send(writer, {"error": {"message": message}})


def _target_from_params(params: Mapping[str, Any]) -> FabricTarget:
    target = params.get("target")
    if not isinstance(target, Mapping):
        raise ValueError("request requires target")
    return FabricTarget.from_dict(target)


def load_endpoint(path: Path | None = None) -> BrokerEndpoint:
    location = path or broker_endpoint_path()
    if location.is_symlink():
        raise ValueError("broker endpoint file must not be a symlink")
    value = json.loads(location.read_text(encoding="utf-8"))
    if not isinstance(value, Mapping):
        raise ValueError("broker endpoint must be an object")
    return BrokerEndpoint.from_private_dict(value)


class BrokerClient:
    """Authenticated client for the local broker. No endpoint token is logged."""

    def __init__(self, endpoint: BrokerEndpoint) -> None:
        self.endpoint = endpoint

    async def execute(
        self,
        *,
        request_id: str,
        target: FabricTarget,
        code: str,
        silent: bool,
        transport: TransportKind,
    ) -> list[ExecutionEvent]:
        values = await self._request(
            "execute",
            {
                "requestId": request_id,
                "target": target.to_dict(),
                "code": code,
                "silent": silent,
                "transport": transport.value,
            },
            streaming=True,
        )
        return [event_from_message(value) for value in values if "event" in value]

    async def interrupt(self, target: FabricTarget) -> None:
        await self._request("interrupt", {"target": target.to_dict()})

    async def shutdown(self, target: FabricTarget) -> None:
        await self._request("shutdown", {"target": target.to_dict()})

    async def status(self) -> list[Mapping[str, Any]]:
        values = await self._request("status", {})
        sessions = values[0].get("sessions", [])
        if not isinstance(sessions, list):
            raise RuntimeError("invalid broker status response")
        return sessions

    async def _request(
        self, method: str, params: Mapping[str, Any], *, streaming: bool = False
    ) -> list[dict[str, Any]]:
        reader, writer = await self._connect()
        try:
            writer.write(
                encode_message(
                    {
                        "version": PROTOCOL_VERSION,
                        "auth": self.endpoint.auth,
                        "method": method,
                        "params": params,
                    }
                )
            )
            await writer.drain()
            values: list[dict[str, Any]] = []
            while True:
                raw = await reader.readline()
                if not raw:
                    break
                value = decode_message(raw)
                if "error" in value:
                    raise RuntimeError(str(value["error"]))
                if value.get("done") is True:
                    break
                values.append(value)
                if not streaming:
                    break
            return values
        finally:
            writer.close()
            with suppress(Exception):
                await writer.wait_closed()

    async def _connect(self) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        if self.endpoint.transport == "unix":
            open_unix_connection = getattr(asyncio, "open_unix_connection", None)
            if open_unix_connection is None:
                raise RuntimeError("Unix socket broker endpoints are unavailable on this platform")
            return await cast(
                Awaitable[tuple[asyncio.StreamReader, asyncio.StreamWriter]],
                open_unix_connection(self.endpoint.address),
            )
        host, port = self.endpoint.address.rsplit(":", 1)
        if host != "127.0.0.1":
            raise ValueError("broker TCP endpoint must be loopback")
        return await asyncio.open_connection(host, int(port))
