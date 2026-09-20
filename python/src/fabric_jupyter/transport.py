"""Transport boundary for deterministic simulation and opt-in Fabric sessions."""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator

from .models import EventKind, ExecutionEvent, ExecutionRequest, FabricTarget, TransportKind


class FabricTransport(ABC):
    """A target-bound remote execution transport."""

    @abstractmethod
    async def start(self) -> dict[str, object]:
        """Start the target-bound session and return credential-free status."""

    @abstractmethod
    def execute(self, request: ExecutionRequest) -> AsyncIterator[ExecutionEvent]:
        """Submit code and yield normalized events."""

    @abstractmethod
    async def interrupt(self, target: FabricTarget) -> None:
        """Interrupt target execution."""

    @abstractmethod
    async def shutdown(self, target: FabricTarget) -> None:
        """Release target session resources."""

    @abstractmethod
    def status(self) -> dict[str, object]:
        """Return credential-free target session status."""


class FakeFabricTransport(FabricTransport):
    """Deterministic test transport; it never evaluates user Python code."""

    def __init__(self) -> None:
        self.executions: list[ExecutionRequest] = []
        self.interrupted: list[FabricTarget] = []
        self.shutdown_targets: list[FabricTarget] = []

    async def start(self) -> dict[str, object]:
        return {"state": "idle", "remoteSession": False}

    async def execute(self, request: ExecutionRequest) -> AsyncIterator[ExecutionEvent]:
        self.executions.append(request)
        yield ExecutionEvent(
            EventKind.STREAM,
            {"name": "stdout", "text": "[offline fake] request accepted; no Fabric connection\n"},
        )
        await asyncio.sleep(0)
        if request.code.strip().startswith("raise"):
            yield ExecutionEvent(
                EventKind.ERROR,
                {
                    "ename": "FakeFabricError",
                    "evalue": "fake transport requested an error",
                    "traceback": ["FakeFabricError: fake transport requested an error"],
                },
            )
            return
        if not request.silent:
            yield ExecutionEvent(
                EventKind.RESULT,
                {
                    "data": {
                        "text/plain": (
                            "Offline simulation complete; no code was evaluated "
                            "and no Fabric session was created."
                        )
                    },
                    "metadata": {},
                    "execution_count": 1,
                },
            )

    async def interrupt(self, target: FabricTarget) -> None:
        self.interrupted.append(target)

    async def shutdown(self, target: FabricTarget) -> None:
        self.shutdown_targets.append(target)

    def status(self) -> dict[str, object]:
        return {"state": "idle", "remoteSession": False}


class ExperimentalFabricTransport(FabricTransport):
    """An intentionally inert placeholder for a separately reviewed real transport."""

    async def start(self) -> dict[str, object]:
        raise RuntimeError("the experimental Fabric transport is not configured")

    async def execute(self, request: ExecutionRequest) -> AsyncIterator[ExecutionEvent]:
        del request
        raise RuntimeError(
            "the experimental Fabric transport is not configured in this release; "
            "no undocumented endpoint or credential flow will be attempted. "
            "Run `fabric-jupyter runtime-status --require-fabric` for readiness blockers"
        )
        yield  # pragma: no cover

    async def interrupt(self, target: FabricTarget) -> None:
        del target
        raise RuntimeError("the experimental Fabric transport is not configured")

    async def shutdown(self, target: FabricTarget) -> None:
        del target
        raise RuntimeError("the experimental Fabric transport is not configured")

    def status(self) -> dict[str, object]:
        raise RuntimeError("the experimental Fabric transport is not configured")


def make_transport(
    kind: TransportKind,
    target: FabricTarget | None = None,
    idle_timeout_seconds: int | None = None,
) -> FabricTransport:
    if kind is TransportKind.FAKE:
        return FakeFabricTransport()
    if kind is TransportKind.FABRIC:
        if target is None:
            raise ValueError("fabric transport requires an explicit target")
        from .fabric_runtime import NotebookRuntimeTransport

        if idle_timeout_seconds is None:
            return NotebookRuntimeTransport(target)
        return NotebookRuntimeTransport(target, idle_timeout_seconds=idle_timeout_seconds)
    if kind is TransportKind.EXPERIMENTAL:
        return ExperimentalFabricTransport()
    raise ValueError("unsupported transport")
