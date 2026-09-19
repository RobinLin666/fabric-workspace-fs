from __future__ import annotations

import json
from pathlib import Path

import pytest

from fabric_jupyter.config import inspect_profiles, load_profiles, write_profiles
from fabric_jupyter.models import FabricLanguage, FabricTarget, Profile
from fabric_jupyter.targets import resolve_target

WORKSPACE = "11111111-1111-1111-1111-111111111111"
NOTEBOOK = "22222222-2222-2222-2222-222222222222"


def test_target_precedence_explicit_then_profile_then_fuse(tmp_path: Path) -> None:
    profile = Profile(
        name="example",
        language=FabricLanguage.PYSPARK,
        target=FabricTarget(WORKSPACE, NOTEBOOK, FabricLanguage.PYSPARK),
    )
    explicit = resolve_target(
        profile,
        workspace_id="33333333-3333-3333-3333-333333333333",
        notebook_id="44444444-4444-4444-4444-444444444444",
    )
    assert explicit.workspace_id.startswith("3333")
    assert resolve_target(profile) == profile.target

    mount = tmp_path / "Notebook.Notebook"
    mount.mkdir()
    (mount / ".fabric.json").write_text(
        json.dumps(
            {"id": NOTEBOOK, "workspaceId": WORKSPACE, "type": "Notebook", "displayName": "Example"}
        )
    )
    from_fuse = resolve_target(
        Profile(name="from-fuse", language=FabricLanguage.PYSPARK, fuse_notebook_path=str(mount))
    )
    assert from_fuse.notebook_id == NOTEBOOK


def test_profile_config_is_strict_and_never_serializes_secret(local_state: Path) -> None:
    profile = Profile(name="fabric-python", language=FabricLanguage.PYTHON)
    path = write_profiles({profile.name: profile})
    assert load_profiles(path)[profile.name] == profile
    view = inspect_profiles(path)
    assert "secret" not in json.dumps(view).lower()
    path.write_text('{"profiles":[{"name":"bad","language":"python","token":"no"}]}')
    with pytest.raises(ValueError, match="unknown"):
        load_profiles(path)


def test_invalid_fuse_identity_is_rejected(tmp_path: Path) -> None:
    mount = tmp_path / "notebook"
    mount.mkdir()
    (mount / ".fabric.json").write_text("{}")
    profile = Profile(
        name="from-fuse", language=FabricLanguage.PYTHON, fuse_notebook_path=str(mount)
    )
    with pytest.raises(ValueError):
        resolve_target(profile)
