"""Jupyter ZeroMQ adapter backed by the authenticated local broker."""

from __future__ import annotations

import os
from typing import Any, cast
from uuid import uuid4

from ipykernel.ipkernel import IPythonKernel
from ipykernel.kernelapp import IPKernelApp

from .broker import BrokerClient, load_endpoint
from .config import load_profiles
from .models import EventKind
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
        profile = load_profiles().get(name)
        if profile is None:
            raise RuntimeError(f"unknown fabric-jupyter profile: {name}")
        self._profile = profile
        self._target = resolve_target(profile)
        self._client = BrokerClient(load_endpoint())

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
            events = await self._client.execute(
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

    async def do_interrupt(self) -> dict[str, str]:
        await self._client.interrupt(self._target)
        return {"status": "ok"}

    async def do_shutdown(self, restart: bool) -> dict[str, Any]:
        if not restart:
            await self._client.shutdown(self._target)
        return {"status": "ok", "restart": restart}


def launch_kernel(connection_file: str, profile: str) -> None:
    """Start a standard ipykernel process with the requested Fabric profile."""

    os.environ["FABRIC_JUPYTER_PROFILE"] = profile
    IPKernelApp.launch_instance(argv=[f"--f={connection_file}"], kernel_class=FabricKernel)
