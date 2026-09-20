"""Strict credential-free profile configuration."""

from __future__ import annotations

import json
import secrets
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from .models import Profile, redact_mapping
from .paths import profiles_path, secure_private_file, verify_private_file

_MAX_CONFIG_BYTES = 1_048_576


def _read_json(path: Path) -> Mapping[str, Any]:
    if path.is_symlink():
        raise ValueError("profile configuration must not be a symlink")
    verify_private_file(path)
    data = path.read_bytes()
    if len(data) > _MAX_CONFIG_BYTES:
        raise ValueError("profile configuration exceeds 1 MiB")
    try:
        value = json.loads(data)
    except json.JSONDecodeError as exc:
        raise ValueError("profile configuration is not valid JSON") from exc
    if not isinstance(value, Mapping):
        raise ValueError("profile configuration root must be an object")
    return value


def load_profiles(path: Path | None = None) -> dict[str, Profile]:
    location = path or profiles_path()
    if not location.exists():
        return default_profiles()
    value = _read_json(location)
    if set(value).difference({"profiles"}):
        raise ValueError("profile configuration has unknown root fields")
    raw_profiles = value.get("profiles")
    if not isinstance(raw_profiles, list):
        raise ValueError("profile configuration requires a profiles array")
    profiles: dict[str, Profile] = {}
    for raw in raw_profiles:
        if not isinstance(raw, Mapping):
            raise ValueError("each profile must be an object")
        profile = Profile.from_dict(raw)
        if profile.name in profiles:
            raise ValueError(f"duplicate profile: {profile.name}")
        profiles[profile.name] = profile
    return profiles


def default_profiles() -> dict[str, Profile]:
    return {}


def inspect_profiles(path: Path | None = None) -> dict[str, Any]:
    location = path or profiles_path()
    profiles = load_profiles(location)
    return {
        "path": str(location),
        "exists": location.exists(),
        "profiles": [redact_mapping(profile.public_dict()) for profile in profiles.values()],
    }


def write_profiles(profiles: Mapping[str, Profile], path: Path | None = None) -> Path:
    location = path or profiles_path()
    if location.exists():
        verify_private_file(location)
    payload = json.dumps(
        {"profiles": [profile.public_dict() for profile in profiles.values()]},
        indent=2,
        sort_keys=True,
    )
    temporary = location.with_name(f".{location.name}.{secrets.token_hex(6)}.tmp")
    with secure_private_file(temporary) as stream:
        stream.write(payload)
    temporary.replace(location)
    verify_private_file(location)
    return location
