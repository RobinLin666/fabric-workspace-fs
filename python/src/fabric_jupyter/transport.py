"""Transport boundary for deterministic simulation and opt-in Fabric sessions."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator

from .models import ExecutionEvent, ExecutionRequest, FabricTarget, TransportKind


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


def make_transport(
    kind: TransportKind,
    target: FabricTarget | None = None,
    idle_timeout_seconds: int | None = None,
) -> FabricTransport:
    if kind is TransportKind.FABRIC:
        if target is None:
            raise ValueError("fabric transport requires an explicit target")
        from .fabric_runtime import NotebookRuntimeTransport

        if idle_timeout_seconds is None:
            return NotebookRuntimeTransport(target)
        return NotebookRuntimeTransport(target, idle_timeout_seconds=idle_timeout_seconds)
    raise ValueError("unsupported transport")
