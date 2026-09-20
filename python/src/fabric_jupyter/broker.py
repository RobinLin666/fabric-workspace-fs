"""Single-user authenticated local broker with Unix socket / loopback endpoints."""

from __future__ import annotations

import asyncio
import hmac
import json
import os
import secrets
import time
from collections.abc import AsyncIterator, Awaitable, Mapping
from contextlib import suppress
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, cast

from .models import (
    BrokerSession,
    EventKind,
    ExecutionEvent,
    FabricTarget,
    Profile,
    SessionState,
    TransportKind,
    redact_mapping,
)
from .paths import (
    broker_endpoint_path,
    broker_socket_path,
    ensure_private,
    secure_private_file,
    state_dir,
    verify_private_directory,
    verify_private_file,
    verify_private_socket,
)
from .protocol import (
    MAX_MESSAGE_BYTES,
    PROTOCOL_VERSION,
    decode_message,
    encode_message,
    event_from_message,
    event_message,
    parse_execute,
    request_from_message,
)
from .targets import resolve_target
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
        profile: Profile | None = None,
    ) -> None:
        if profile is None:
            raise ValueError("broker requires an explicit Fabric profile")
        self._profile = profile
        self._target = resolve_target(self._profile)
        self._transport_override = transport
        self._transport: FabricTransport | None = transport
        if self._transport is None:
            self._transport = self._new_transport()
        self._idle_timeout_seconds = idle_timeout_seconds
        self._endpoint = endpoint
        self._server: asyncio.AbstractServer | None = None
        self._sessions: dict[tuple[str, str, str], BrokerSession] = {}
        self._cleanup_task: asyncio.Task[None] | None = None
        self._endpoint_file: Path | None = None

    def _new_transport(self) -> FabricTransport:
        return make_transport(
            self._profile.transport,
            self._target,
            self._profile.idle_timeout_seconds,
        )

    def _active_transport(self) -> FabricTransport:
        if self._transport is None:
            self._transport = self._new_transport()
        return self._transport

    def _discard_terminal_transport(self) -> None:
        if (
            self._profile.transport is TransportKind.FABRIC
            and self._transport_override is None
        ):
            self._transport = None

    def _authorize_target(self, target: FabricTarget) -> FabricTarget:
        if (
            target.workspace_id.lower() != self._target.workspace_id.lower()
            or target.notebook_id.lower() != self._target.notebook_id.lower()
            or target.language is not self._target.language
        ):
            raise ValueError("request target is not authorized by this broker's startup profile")
        return self._target

    @property
    def endpoint(self) -> BrokerEndpoint:
        if self._endpoint is None:
            raise RuntimeError("broker has not started")
        return self._endpoint

    async def start(self, *, persist_endpoint: bool = False) -> BrokerEndpoint:
        if self._server is not None:
            return self.endpoint
        ensure_private(state_dir())
        if persist_endpoint and broker_endpoint_path().exists():
            raise FileExistsError("broker descriptor already exists; stop its owning broker first")
        if self._endpoint is None:
            self._endpoint = self._new_endpoint(persist_endpoint=persist_endpoint)
        if self._endpoint.transport == "unix":
            path = Path(self._endpoint.address)
            ensure_private(path.parent)
            if path.exists():
                raise FileExistsError("broker socket already exists; do not replace a running broker")
            start_unix_server = getattr(asyncio, "start_unix_server", None)
            if start_unix_server is None:
                raise RuntimeError("Unix socket broker endpoints are unavailable on this platform")
            self._server = await cast(
                Awaitable[asyncio.AbstractServer],
                start_unix_server(self._handle, path=str(path), limit=MAX_MESSAGE_BYTES + 1),
            )
            try:
                path.chmod(0o600)
                verify_private_socket(path)
            except (OSError, RuntimeError, ValueError):
                await self.close()
                raise
        else:
            host, port = self._endpoint.address.rsplit(":", 1)
            if host != "127.0.0.1" or not port.isdecimal() or not 0 <= int(port) <= 65535:
                raise ValueError("broker TCP listener must use 127.0.0.1 and a valid port")
            self._server = await asyncio.start_server(
                self._handle, host=host, port=int(port), limit=MAX_MESSAGE_BYTES + 1,
            )
            actual_port = self._server.sockets[0].getsockname()[1]
            self._endpoint = BrokerEndpoint("tcp", f"127.0.0.1:{actual_port}", self._endpoint.auth)
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())
        if persist_endpoint:
            try:
                self.write_endpoint()
            except (OSError, RuntimeError, ValueError):
                await self.close()
                raise
        return self.endpoint

    def _new_endpoint(self, *, persist_endpoint: bool) -> BrokerEndpoint:
        auth = secrets.token_urlsafe(32)
        if os.name == "nt":
            return BrokerEndpoint("tcp", "127.0.0.1:0", auth)
        path = broker_socket_path()
        if not persist_endpoint:
            path = path.with_name(f"kernel-{secrets.token_hex(6)}.sock")
        return BrokerEndpoint("unix", str(path), auth)

    def write_endpoint(self) -> Path:
        location = broker_endpoint_path()
        if location.exists():
            raise FileExistsError("refusing to replace an existing broker descriptor")
        temporary = location.with_suffix(".tmp")
        with secure_private_file(temporary) as stream:
            json.dump(self.endpoint.to_private_dict(), stream, separators=(",", ":"))
        temporary.replace(location)
        verify_private_file(location)
        self._endpoint_file = location
        return location

    async def close(self) -> None:
        shutdown_failure: BaseException | None = None
        if self._cleanup_task is not None:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
            except (RuntimeError, TimeoutError) as exc:
                if self._profile.transport is TransportKind.FABRIC:
                    shutdown_failure = exc
            self._cleanup_task = None
        for session in list(self._sessions.values()):
            try:
                async with asyncio.timeout(180):
                    await self._active_transport().shutdown(session.target)
            except (RuntimeError, TimeoutError) as exc:
                if (
                    self._profile.transport is TransportKind.FABRIC
                    and shutdown_failure is None
                ):
                    shutdown_failure = exc
            else:
                session.state = SessionState.STOPPED
        self._sessions.clear()
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
        if self._endpoint is not None and self._endpoint.transport == "unix":
            with suppress(FileNotFoundError):
                Path(self._endpoint.address).unlink()
        if self._endpoint_file is not None:
            with suppress(FileNotFoundError):
                recorded = load_endpoint(self._endpoint_file)
                if hmac.compare_digest(recorded.auth, self.endpoint.auth):
                    self._endpoint_file.unlink()
            self._endpoint_file = None
        if shutdown_failure is not None:
            raise shutdown_failure

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            raw = await asyncio.wait_for(
                reader.readline(), timeout=self._profile.request_timeout_seconds
            )
            message = decode_message(raw)
            provided_auth = message.get("auth")
            if not isinstance(provided_auth, str) or not hmac.compare_digest(
                provided_auth.encode("utf-8"), self.endpoint.auth.encode("utf-8"),
            ):
                await self._send_error(writer, "unauthorized")
                return
            method, params = request_from_message(message)
            if method == "connect":
                await self._connect_transport(params, writer)
            elif method == "execute":
                await self._execute(params, writer)
            elif method == "interrupt":
                await self._interrupt(params, writer)
            elif method == "shutdown":
                await self._shutdown(params, writer)
            elif method == "status":
                if params:
                    raise ValueError("status takes no parameters")
                await self._status(writer)
            else:
                await self._send_error(writer, "unknown method")
        except TimeoutError:
            await self._send_error(writer, "broker request timed out")
        except (ValueError, RuntimeError) as exc:
            await self._send_error(writer, str(exc))
        finally:
            writer.close()
            with suppress(Exception):
                await writer.wait_closed()

    async def _connect_transport(
        self, params: Mapping[str, Any], writer: asyncio.StreamWriter
    ) -> None:
        target, transport_kind = self._bound_request(params, method="connect")
        key = self._session_key(target)
        session = self._sessions.get(key)
        if session is None:
            session = BrokerSession(target=target, transport=transport_kind)
            self._sessions[key] = session
        if session.transport is not transport_kind:
            raise ValueError("target already has a session with a different transport")
        if session.state is SessionState.STOPPED:
            raise RuntimeError(
                "previous Fabric shutdown was not verified; restart the kernel or broker"
            )
        if session.state is SessionState.STARTING:
            try:
                async with asyncio.timeout(self._profile.startup_timeout_seconds):
                    status = await self._active_transport().start()
            except TimeoutError as exc:
                session.state = SessionState.STOPPED
                raise RuntimeError("Fabric transport startup timed out") from exc
            except (OSError, RuntimeError, ValueError):
                session.state = SessionState.STOPPED
                raise
            if not isinstance(status, Mapping):
                self._sessions.pop(key, None)
                raise RuntimeError("Fabric transport returned invalid startup status")
            session.remote_status = redact_mapping(status)
            session.state = SessionState.IDLE
        session.last_used_monotonic = time.monotonic()
        await self._send(writer, {"session": session.public_dict()})

    async def _execute(self, params: Mapping[str, Any], writer: asyncio.StreamWriter) -> None:
        request = parse_execute(params)
        transport_kind = TransportKind(str(params.get("transport")))
        if transport_kind is not self._profile.transport:
            raise ValueError("request transport is not authorized by this broker's startup profile")
        request = replace(request, target=self._authorize_target(request.target))
        key = (
            request.target.workspace_id,
            request.target.notebook_id,
            request.target.language.value,
        )
        session = self._sessions.get(key)
        if session is None:
            if transport_kind is TransportKind.FABRIC:
                raise RuntimeError("fabric session is not connected; call connect first")
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
            async with asyncio.timeout(self._profile.execution_timeout_seconds):
                async for event in self._active_transport().execute(request):
                    await self._send(writer, event_message(event))
        except (RuntimeError, TimeoutError) as exc:
            message = "Fabric execution timed out" if isinstance(exc, TimeoutError) else str(exc)
            await self._send(
                writer,
                event_message(
                    ExecutionEvent(
                        EventKind.ERROR,
                        {
                            "ename": "FabricTransportError",
                            "evalue": message,
                            "traceback": [message],
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
        target = self._authorize_target(_target_from_params(params))
        async with asyncio.timeout(180):
            await self._active_transport().interrupt(target)
        await self._send(writer, {"ok": True})

    async def _shutdown(self, params: Mapping[str, Any], writer: asyncio.StreamWriter) -> None:
        target = self._authorize_target(_target_from_params(params))
        key = (target.workspace_id, target.notebook_id, target.language.value)
        session = self._sessions.get(key)
        if session is not None:
            try:
                async with asyncio.timeout(180):
                    await self._active_transport().shutdown(target)
            except (RuntimeError, TimeoutError):
                session.state = SessionState.STOPPED
                raise
            else:
                session.state = SessionState.STOPPED
                del self._sessions[key]
                self._discard_terminal_transport()
        await self._send(writer, {"ok": True})

    async def _status(self, writer: asyncio.StreamWriter) -> None:
        for session in self._sessions.values():
            if session.transport is TransportKind.FABRIC:
                status = self._active_transport().status()
                if not isinstance(status, Mapping):
                    raise RuntimeError("Fabric transport returned invalid status")
                session.remote_status = redact_mapping(status)
        await self._send(
            writer, {"sessions": [session.public_dict() for session in self._sessions.values()]}
        )

    async def _cleanup_loop(self) -> None:
        while True:
            await asyncio.sleep(min(30, self._idle_timeout_seconds))
            deadline = time.monotonic() - self._idle_timeout_seconds
            for key, session in list(self._sessions.items()):
                if session.state is SessionState.IDLE and session.last_used_monotonic < deadline:
                    try:
                        async with asyncio.timeout(60):
                            await self._active_transport().shutdown(session.target)
                    except (RuntimeError, TimeoutError):
                        session.state = SessionState.STOPPED
                        raise
                    else:
                        session.state = SessionState.STOPPED
                        del self._sessions[key]
                        self._discard_terminal_transport()

    def _bound_request(
        self, params: Mapping[str, Any], *, method: str
    ) -> tuple[FabricTarget, TransportKind]:
        if set(params) != {"target", "transport"}:
            raise ValueError(f"{method} requires only target and transport parameters")
        target_value = params.get("target")
        if not isinstance(target_value, Mapping):
            raise ValueError(f"{method} requires target")
        target = self._authorize_target(FabricTarget.from_dict(target_value))
        transport_kind = TransportKind(str(params.get("transport")))
        if transport_kind is not self._profile.transport:
            raise ValueError("request transport is not authorized by this broker's startup profile")
        return target, transport_kind

    @staticmethod
    def _session_key(target: FabricTarget) -> tuple[str, str, str]:
        return (target.workspace_id, target.notebook_id, target.language.value)

    async def _send(self, writer: asyncio.StreamWriter, value: Mapping[str, Any]) -> None:
        writer.write(encode_message(value))
        await writer.drain()

    async def _send_error(self, writer: asyncio.StreamWriter, message: str) -> None:
        await self._send(writer, {"error": {"message": message}})


def _target_from_params(params: Mapping[str, Any]) -> FabricTarget:
    if set(params) != {"target"}:
        raise ValueError("interrupt and shutdown require only a target parameter")
    target = params.get("target")
    if not isinstance(target, Mapping):
        raise ValueError("request requires target")
    return FabricTarget.from_dict(target)


def load_endpoint(path: Path | None = None) -> BrokerEndpoint:
    location = path or broker_endpoint_path()
    verify_private_directory(location.parent)
    verify_private_file(location)
    value = json.loads(location.read_text(encoding="utf-8"))
    if not isinstance(value, Mapping):
        raise ValueError("broker endpoint must be an object")
    return BrokerEndpoint.from_private_dict(value)


class BrokerClient:
    """Authenticated client for the local broker. No endpoint token is logged."""

    def __init__(self, endpoint: BrokerEndpoint) -> None:
        self.endpoint = endpoint

    async def connect(
        self, target: FabricTarget, transport: TransportKind
    ) -> Mapping[str, Any]:
        values = await self._request(
            "connect",
            {"target": target.to_dict(), "transport": transport.value},
            timeout=660,
        )
        session = values[0].get("session")
        if not isinstance(session, Mapping):
            raise RuntimeError("invalid broker connect response")
        return session

    async def stream_execute(
        self,
        *,
        request_id: str,
        target: FabricTarget,
        code: str,
        silent: bool,
        transport: TransportKind,
    ) -> AsyncIterator[ExecutionEvent]:
        async for value in self._request_stream(
            "execute",
            {
                "requestId": request_id,
                "target": target.to_dict(),
                "code": code,
                "silent": silent,
                "transport": transport.value,
            },
            timeout=660,
        ):
            if "event" in value:
                yield event_from_message(value)

    async def execute(
        self,
        *,
        request_id: str,
        target: FabricTarget,
        code: str,
        silent: bool,
        transport: TransportKind,
    ) -> list[ExecutionEvent]:
        return [
            event
            async for event in self.stream_execute(
                request_id=request_id,
                target=target,
                code=code,
                silent=silent,
                transport=transport,
            )
        ]

    async def interrupt(self, target: FabricTarget) -> None:
        await self._request("interrupt", {"target": target.to_dict()}, timeout=60)

    async def shutdown(self, target: FabricTarget) -> None:
        await self._request("shutdown", {"target": target.to_dict()}, timeout=210)

    async def status(self) -> list[Mapping[str, Any]]:
        values = await self._request("status", {}, timeout=60)
        sessions = values[0].get("sessions", [])
        if not isinstance(sessions, list):
            raise RuntimeError("invalid broker status response")
        return sessions

    async def _request(
        self,
        method: str,
        params: Mapping[str, Any],
        *,
        timeout: float = 30,
    ) -> list[dict[str, Any]]:
        return [
            value
            async for value in self._request_stream(method, params, timeout=timeout)
        ]

    async def _request_stream(
        self,
        method: str,
        params: Mapping[str, Any],
        *,
        timeout: float,
    ) -> AsyncIterator[dict[str, Any]]:
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
            while True:
                raw = await asyncio.wait_for(reader.readline(), timeout=timeout)
                if not raw:
                    raise RuntimeError("broker closed the connection before completing its response")
                value = decode_message(raw)
                if "error" in value:
                    error = value["error"]
                    if isinstance(error, Mapping) and isinstance(error.get("message"), str):
                        raise RuntimeError(error["message"])
                    raise RuntimeError("invalid broker error response")
                if value.get("done") is True:
                    break
                yield value
                if method != "execute":
                    break
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
                open_unix_connection(self.endpoint.address, limit=MAX_MESSAGE_BYTES + 1),
            )
        host, port = self.endpoint.address.rsplit(":", 1)
        if host != "127.0.0.1":
            raise ValueError("broker TCP endpoint must be loopback")
        return await asyncio.open_connection(host, int(port), limit=MAX_MESSAGE_BYTES + 1)
