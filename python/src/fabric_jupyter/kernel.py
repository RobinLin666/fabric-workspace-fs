"""Jupyter ZeroMQ adapter backed by the authenticated local broker."""

from __future__ import annotations

import asyncio
import os
import sys
from typing import Any, cast
from uuid import uuid4

from ipykernel.ipkernel import IPythonKernel
from ipykernel.kernelapp import IPKernelApp

from .broker import BrokerClient, BrokerServer, load_endpoint
from .config import load_profiles
from .models import EventKind, FabricTarget, Profile, TransportKind
from .targets import resolve_target


class FabricKernel(IPythonKernel):
    """A local adapter: Jupyter speaks only to this process, never to Fabric."""

    implementation = "fabric-jupyter"
    implementation_version = "0.1.0"
    language_info = {
        "name": "python",
        "mimetype": "text/x-python",
        "file_extension": ".py",
        "pygments_lexer": "python",
    }
    banner = "fabric-jupyter local broker kernel"

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        name = os.environ.get("FABRIC_JUPYTER_PROFILE", "fabric-pyspark")
        self._profile: Profile | None = None
        self._target: FabricTarget | None = None
        self._client: BrokerClient | None = None
        self._embedded_broker: BrokerServer | None = None
        self._embedded_broker_loop: asyncio.AbstractEventLoop | None = None
        self._startup_error: str | None = None
        try:
            profile = load_profiles().get(name)
            if profile is None:
                raise RuntimeError(f"unknown fabric-jupyter profile: {name}")
            self._profile = profile
            self._target = resolve_target(profile)
        except (OSError, RuntimeError, ValueError) as exc:
            self._startup_error = _diagnostic("startup configuration failed", exc)

    async def _broker_client(self) -> BrokerClient:
        if self._startup_error is not None:
            raise RuntimeError(self._startup_error)
        if self._profile is None or self._target is None:
            raise RuntimeError("fabric-jupyter startup did not produce a runnable profile")
        if self._client is not None:
            return self._client
        try:
            self._client = BrokerClient(load_endpoint())
            return self._client
        except FileNotFoundError:
            if self._profile.transport is not TransportKind.FAKE:
                raise RuntimeError(
                    "broker endpoint was not found; start `fabric-jupyter broker` for "
                    f"transport '{self._profile.transport.value}' or use the default fake "
                    "transport for local protocol tests"
                ) from None
            self._embedded_broker = BrokerServer(
                idle_timeout_seconds=self._profile.idle_timeout_seconds
            )
            endpoint = await self._embedded_broker.start(persist_endpoint=False)
            self._embedded_broker_loop = asyncio.get_running_loop()
            self._client = BrokerClient(endpoint)
            return self._client
        except (OSError, RuntimeError, ValueError) as exc:
            raise RuntimeError(_diagnostic("broker endpoint is not usable", exc)) from exc

    async def do_execute(
        self,
        code: str,
        silent: bool,
        store_history: bool = True,
        user_expressions: dict[str, str] | None = None,
        allow_stdin: bool = False,
        *,
        cell_meta: dict[str, Any] | None = None,
        cell_id: str | None = None,
    ) -> dict[str, Any]:
        del store_history, user_expressions, allow_stdin, cell_meta, cell_id
        try:
            client = await self._broker_client()
            if self._profile is None or self._target is None:
                raise RuntimeError("fabric-jupyter startup did not produce a runnable profile")
            events = await client.execute(
                request_id=str(uuid4()),
                target=self._target,
                code=code,
                silent=silent,
                transport=self._profile.transport,
            )
        except (OSError, RuntimeError, ValueError) as exc:
            self.send_response(
                self.iopub_socket,
                "error",
                {
                    "ename": type(exc).__name__,
                    "evalue": str(exc),
                    "traceback": [str(exc)],
                },
            )
            return {
                "status": "error",
                "ename": type(exc).__name__,
                "evalue": str(exc),
                "traceback": [str(exc)],
            }
        for event in events:
            if event.kind is EventKind.STREAM and not silent:
                stream_content = cast(dict[str, Any], dict(event.content))
                self.send_response(self.iopub_socket, "stream", stream_content)
            elif event.kind is EventKind.RESULT and not silent:
                result_content = cast(dict[str, Any], dict(event.content))
                self.send_response(self.iopub_socket, "execute_result", result_content)
            elif event.kind is EventKind.ERROR:
                content = cast(dict[str, Any], dict(event.content))
                self.send_response(self.iopub_socket, "error", content)
                error_payload: dict[str, object] = {"status": "error"}
                error_payload.update(content)
                return error_payload
        return {
            "status": "ok",
            "execution_count": self.execution_count,
            "payload": [],
            "user_expressions": {},
        }

    async def do_interrupt(self) -> dict[str, Any]:
        try:
            client = await self._broker_client()
            if self._target is None:
                raise RuntimeError("fabric-jupyter startup did not produce a runnable target")
            await client.interrupt(self._target)
            return {"status": "ok"}
        except (OSError, RuntimeError, ValueError) as exc:
            return {"status": "error", "ename": type(exc).__name__, "evalue": str(exc)}

    async def do_shutdown(self, restart: bool) -> dict[str, Any]:
        if not restart:
            try:
                if self._client is not None and self._target is not None:
                    await self._client.shutdown(self._target)
            finally:
                await self._close_embedded_broker()
        return {"status": "ok", "restart": restart}

    async def _close_embedded_broker(self) -> None:
        broker = self._embedded_broker
        if broker is None:
            return
        self._embedded_broker = None
        self._client = None
        broker_loop = self._embedded_broker_loop
        self._embedded_broker_loop = None
        try:
            running_loop = asyncio.get_running_loop()
        except RuntimeError:
            running_loop = None
        if broker_loop is None or broker_loop is running_loop:
            await broker.close()
            return
        if broker_loop.is_closed():
            return
        broker_loop.call_soon_threadsafe(lambda: broker_loop.create_task(broker.close()))


def _diagnostic(context: str, exc: BaseException) -> str:
    return f"{context}: {type(exc).__name__}: {exc}"


def launch_kernel(connection_file: str, profile: str) -> None:
    """Start a standard ipykernel process with the requested Fabric profile."""

    os.environ["FABRIC_JUPYTER_PROFILE"] = profile
    try:
        IPKernelApp.launch_instance(argv=["-f", connection_file], kernel_class=FabricKernel)
    except (OSError, RuntimeError, ValueError) as exc:
        print(f"fabric-jupyter: {_diagnostic('kernel launch failed', exc)}", file=sys.stderr)
        raise
