"""Length-bounded authenticated local broker request protocol."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from .models import ExecutionEvent, ExecutionRequest, FabricTarget

PROTOCOL_VERSION = 1
MAX_MESSAGE_BYTES = 1_048_576


def encode_message(value: Mapping[str, Any]) -> bytes:
    raw = json.dumps(value, separators=(",", ":"), sort_keys=True).encode("utf-8")
    if len(raw) > MAX_MESSAGE_BYTES:
        raise ValueError("broker message exceeds 1 MiB")
    return raw + b"\n"


def decode_message(raw: bytes) -> dict[str, Any]:
    if len(raw) > MAX_MESSAGE_BYTES:
        raise ValueError("broker message exceeds 1 MiB")
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError("broker message is not JSON") from exc
    if not isinstance(value, dict):
        raise ValueError("broker message must be an object")
    return value


def request_from_message(value: Mapping[str, Any]) -> tuple[str, dict[str, Any]]:
    allowed = {"version", "auth", "method", "params"}
    unknown = set(value).difference(allowed)
    if unknown:
        raise ValueError(f"unknown broker request fields: {', '.join(sorted(unknown))}")
    if value.get("version") != PROTOCOL_VERSION:
        raise ValueError("unsupported broker protocol version")
    method = value.get("method")
    params = value.get("params", {})
    if not isinstance(method, str) or not isinstance(params, dict):
        raise ValueError("broker method and params must be an object")
    return method, params


def parse_execute(params: Mapping[str, Any]) -> ExecutionRequest:
    allowed = {"requestId", "target", "code", "silent", "transport"}
    unknown = set(params).difference(allowed)
    if unknown:
        raise ValueError(f"unknown execute parameter(s): {', '.join(sorted(unknown))}")
    target = params.get("target")
    if not isinstance(target, Mapping) or not isinstance(params.get("code"), str):
        raise ValueError("execute requires target object and code string")
    return ExecutionRequest(
        request_id=str(params.get("requestId", "")),
        target=FabricTarget.from_dict(target),
        code=params["code"],
        silent=bool(params.get("silent", False)),
    )


def event_message(event: ExecutionEvent) -> dict[str, Any]:
    return {"event": event.to_dict()}


def event_from_message(value: Mapping[str, Any]) -> ExecutionEvent:
    event = value.get("event")
    if not isinstance(event, Mapping):
        raise ValueError("broker response lacks event")
    return ExecutionEvent.from_dict(event)
