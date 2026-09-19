"""Resolve targets from explicit data, profiles, then optional FUSE metadata."""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from .models import FabricLanguage, FabricTarget, Profile


def _identity_target(path: Path, language: FabricLanguage) -> FabricTarget:
    identity = path / ".fabric.json"
    if identity.is_symlink() or not identity.is_file():
        raise ValueError("FUSE notebook directory must contain a regular .fabric.json file")
    try:
        value: Mapping[str, Any] = json.loads(identity.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError("FUSE .fabric.json is not readable JSON") from exc
    if value.get("type") != "Notebook":
        raise ValueError("FUSE metadata does not describe a Notebook")
    workspace = value.get("workspaceId")
    item = value.get("id")
    if not isinstance(workspace, str) or not isinstance(item, str):
        raise ValueError("FUSE metadata lacks Notebook identity fields")
    display = value.get("displayName")
    return FabricTarget(
        workspace_id=workspace,
        notebook_id=item,
        language=language,
        display_name=display if isinstance(display, str) else None,
    )


def resolve_target(
    profile: Profile,
    *,
    workspace_id: str | None = None,
    notebook_id: str | None = None,
    fuse_notebook_path: Path | None = None,
) -> FabricTarget:
    """Resolve explicit IDs first, then configured target, then FUSE identity."""

    if (workspace_id is None) != (notebook_id is None):
        raise ValueError("workspace_id and notebook_id must be supplied together")
    if workspace_id is not None and notebook_id is not None:
        return FabricTarget(
            workspace_id=workspace_id, notebook_id=notebook_id, language=profile.language
        )
    if profile.target is not None:
        return profile.target
    mount_path = fuse_notebook_path or (
        Path(profile.fuse_notebook_path) if profile.fuse_notebook_path is not None else None
    )
    if mount_path is not None:
        return _identity_target(mount_path, profile.language)
    raise ValueError(
        "profile has no target: supply explicit workspace/notebook IDs, configure target, "
        "or configure an optional FUSE notebook directory"
    )
