"""Owner-private local state paths. No state is stored in notebook mounts."""

from __future__ import annotations

import os
from pathlib import Path


def state_dir() -> Path:
    base = os.environ.get("LOCALAPPDATA") if os.name == "nt" else os.environ.get("XDG_STATE_HOME")
    root = (
        Path(base)
        if base
        else Path.home() / (".fabric-jupyter" if os.name == "nt" else ".local/state")
    )
    return root / "fabric-jupyter" if os.name != "nt" else root


def config_dir() -> Path:
    base = os.environ.get("APPDATA") if os.name == "nt" else os.environ.get("XDG_CONFIG_HOME")
    root = (
        Path(base) if base else Path.home() / (".fabric-jupyter" if os.name == "nt" else ".config")
    )
    return root / "fabric-jupyter" if os.name != "nt" else root


def ensure_private(directory: Path) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    if os.name != "nt":
        directory.chmod(0o700)
    return directory


def profiles_path() -> Path:
    return ensure_private(config_dir()) / "profiles.json"


def broker_endpoint_path() -> Path:
    return ensure_private(state_dir()) / "broker.json"


def broker_socket_path() -> Path:
    return ensure_private(state_dir()) / "broker.sock"
