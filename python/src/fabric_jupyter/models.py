"""Strict messages shared by the CLI, kernel, broker, and transports."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any
from uuid import UUID


class FabricLanguage(StrEnum):
    PYSPARK = "pyspark"
    PYTHON = "python"


class TransportKind(StrEnum):
    FAKE = "fake"
    FABRIC = "fabric"
    EXPERIMENTAL = "experimental"


class SessionState(StrEnum):
    STARTING = "starting"
    IDLE = "idle"
    BUSY = "busy"
    STOPPED = "stopped"


class EventKind(StrEnum):
    STREAM = "stream"
    RESULT = "result"
    DISPLAY_DATA = "display_data"
    UPDATE_DISPLAY_DATA = "update_display_data"
    CLEAR_OUTPUT = "clear_output"
    COMM_OPEN = "comm_open"
    COMM_MSG = "comm_msg"
    COMM_CLOSE = "comm_close"
    ERROR = "error"
    STATUS = "status"


@dataclass(frozen=True, slots=True)
class FabricTarget:
    """A Fabric execution target, never a credential container."""

    workspace_id: str
    notebook_id: str
    language: FabricLanguage
    display_name: str | None = None

    def __post_init__(self) -> None:
        for name, value in (("workspace_id", self.workspace_id), ("notebook_id", self.notebook_id)):
            try:
                canonical = str(UUID(value))
            except (TypeError, ValueError) as exc:
                raise ValueError(f"{name} must be a UUID") from exc
            object.__setattr__(self, name, canonical)
        if self.display_name is not None and (
            not self.display_name or len(self.display_name) > 256
        ):
            raise ValueError("display_name must be non-empty and at most 256 characters")

    def to_dict(self) -> dict[str, str]:
        result = {
            "workspaceId": self.workspace_id,
            "notebookId": self.notebook_id,
            "language": self.language.value,
        }
        if self.display_name is not None:
            result["displayName"] = self.display_name
        return result

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> FabricTarget:
        allowed = {"workspaceId", "notebookId", "language", "displayName"}
        unknown = set(value).difference(allowed)
        if unknown:
            raise ValueError(f"unknown target field(s): {', '.join(sorted(unknown))}")
        try:
            return cls(
                workspace_id=str(value["workspaceId"]),
                notebook_id=str(value["notebookId"]),
                language=FabricLanguage(str(value["language"])),
                display_name=str(value["displayName"]) if "displayName" in value else None,
            )
        except KeyError as exc:
            raise ValueError(f"missing target field: {exc.args[0]}") from exc


@dataclass(frozen=True, slots=True)
class Profile:
    """A serializable, credential-free local profile."""

    name: str
    language: FabricLanguage
    transport: TransportKind = TransportKind.FAKE
    target: FabricTarget | None = None
    fuse_notebook_path: str | None = None
    idle_timeout_seconds: int = 900
    startup_timeout_seconds: int = 600
    execution_timeout_seconds: int = 300
    request_timeout_seconds: int = 30

    def __post_init__(self) -> None:
        allowed_name_characters = "-_abcdefghijklmnopqrstuvwxyz0123456789"
        if (
            not self.name
            or len(self.name) > 64
            or any(c not in allowed_name_characters for c in self.name)
        ):
            raise ValueError("profile name must contain only lowercase letters, digits, '-' or '_'")
        if self.idle_timeout_seconds < 60 or self.idle_timeout_seconds > 86_400:
            raise ValueError("idle_timeout_seconds must be between 60 and 86400")
        if not 1 <= self.startup_timeout_seconds <= 600:
            raise ValueError("startup_timeout_seconds must be between 1 and 600")
        if not 1 <= self.execution_timeout_seconds <= 300:
            raise ValueError("execution_timeout_seconds must be between 1 and 300")
        if not 1 <= self.request_timeout_seconds <= 30:
            raise ValueError("request_timeout_seconds must be between 1 and 30")
        if self.target is not None and self.target.language is not self.language:
            raise ValueError("profile target language must match profile language")
        if self.fuse_notebook_path is not None and not self.fuse_notebook_path:
            raise ValueError("fuse_notebook_path cannot be empty")
        if (
            self.transport is TransportKind.FABRIC
            and self.language is FabricLanguage.PYTHON
        ):
            raise ValueError(
                "real Python runtime is not supported; use PySpark or offline fake"
            )
        if (
            self.transport is TransportKind.FABRIC
            and self.target is None
            and self.fuse_notebook_path is None
        ):
            raise ValueError(
                "fabric transport requires an explicit target or fuseNotebookPath"
            )

    def public_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "name": self.name,
            "language": self.language.value,
            "transport": self.transport.value,
            "idleTimeoutSeconds": self.idle_timeout_seconds,
            "startupTimeoutSeconds": self.startup_timeout_seconds,
            "executionTimeoutSeconds": self.execution_timeout_seconds,
            "requestTimeoutSeconds": self.request_timeout_seconds,
        }
        if self.target is not None:
            result["target"] = self.target.to_dict()
        if self.fuse_notebook_path is not None:
            result["fuseNotebookPath"] = self.fuse_notebook_path
        return result

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> Profile:
        allowed = {
            "name",
            "language",
            "transport",
            "target",
            "fuseNotebookPath",
            "idleTimeoutSeconds",
            "startupTimeoutSeconds",
            "executionTimeoutSeconds",
            "requestTimeoutSeconds",
        }
        unknown = set(value).difference(allowed)
        if unknown:
            raise ValueError(f"unknown profile field(s): {', '.join(sorted(unknown))}")
        try:
            target = FabricTarget.from_dict(value["target"]) if "target" in value else None
            return cls(
                name=str(value["name"]),
                language=FabricLanguage(str(value["language"])),
                transport=TransportKind(str(value.get("transport", "fake"))),
                target=target,
                fuse_notebook_path=str(value["fuseNotebookPath"])
                if "fuseNotebookPath" in value
                else None,
                idle_timeout_seconds=int(value.get("idleTimeoutSeconds", 900)),
                startup_timeout_seconds=int(value.get("startupTimeoutSeconds", 600)),
                execution_timeout_seconds=int(value.get("executionTimeoutSeconds", 300)),
                request_timeout_seconds=int(value.get("requestTimeoutSeconds", 30)),
            )
        except KeyError as exc:
            raise ValueError(f"missing profile field: {exc.args[0]}") from exc


@dataclass(frozen=True, slots=True)
class ExecutionRequest:
    request_id: str
    target: FabricTarget
    code: str
    silent: bool = False

    def __post_init__(self) -> None:
        if not self.request_id or len(self.request_id) > 128:
            raise ValueError("request_id must be non-empty and at most 128 characters")
        if len(self.code.encode("utf-8")) > 1_048_576:
            raise ValueError("code exceeds the 1 MiB broker request limit")


@dataclass(frozen=True, slots=True)
class ExecutionEvent:
    kind: EventKind
    content: Mapping[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {"kind": self.kind.value, "content": dict(self.content)}

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> ExecutionEvent:
        try:
            content = value["content"]
            if not isinstance(content, Mapping):
                raise ValueError("event content must be an object")
            return cls(kind=EventKind(str(value["kind"])), content=dict(content))
        except KeyError as exc:
            raise ValueError(f"missing event field: {exc.args[0]}") from exc


@dataclass(slots=True)
class BrokerSession:
    target: FabricTarget
    transport: TransportKind
    state: SessionState = SessionState.STARTING
    last_used_monotonic: float = field(default=0.0)
    remote_status: Mapping[str, Any] = field(default_factory=dict)

    def public_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "target": self.target.to_dict(),
            "transport": self.transport.value,
            "state": self.state.value,
            "remoteSession": self.transport is TransportKind.FABRIC,
        }
        if self.remote_status:
            result["remoteStatus"] = redact_mapping(self.remote_status)
        return result


def redact_mapping(value: Mapping[str, Any]) -> dict[str, Any]:
    """Return a safe view for inspection; secrets are never emitted by the CLI."""

    forbidden = ("token", "secret", "password", "authorization", "cookie", "credential")
    result: dict[str, Any] = {}
    for key, item in value.items():
        if any(fragment in key.lower() for fragment in forbidden):
            result[key] = "<redacted>"
        elif isinstance(item, Mapping):
            result[key] = redact_mapping(item)
        else:
            result[key] = item
    return result
