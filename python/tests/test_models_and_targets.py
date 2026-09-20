from __future__ import annotations

import json
from pathlib import Path

import pytest

from fabric_jupyter.cli import main
from fabric_jupyter.config import inspect_profiles, load_profiles, write_profiles
from fabric_jupyter.models import FabricLanguage, FabricTarget, Profile, TransportKind
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


def test_target_ids_are_canonicalized_before_policy_and_path_use() -> None:
    target = FabricTarget(
        "{11111111-1111-1111-1111-111111111111}",
        "urn:uuid:22222222-2222-2222-2222-222222222222",
        FabricLanguage.PYSPARK,
    )
    assert target.workspace_id == WORKSPACE
    assert target.notebook_id == NOTEBOOK
    assert target.to_dict()["workspaceId"] == WORKSPACE
    assert target.to_dict()["notebookId"] == NOTEBOOK


def test_profile_config_is_strict_and_never_serializes_secret(local_state: Path) -> None:
    profile = Profile(
        name="fabric-python-3.12",
        language=FabricLanguage.PYTHON312,
        target=FabricTarget(WORKSPACE, NOTEBOOK, FabricLanguage.PYTHON312),
    )
    path = write_profiles({profile.name: profile})
    assert load_profiles(path)[profile.name] == profile
    view = inspect_profiles(path)
    assert "secret" not in json.dumps(view).lower()
    path.write_text('{"profiles":[{"name":"bad","language":"python","token":"no"}]}')
    with pytest.raises(ValueError, match="unknown"):
        load_profiles(path)


def test_fabric_profile_requires_target_and_serializes_bounded_timeouts() -> None:
    python_profile = Profile(
        name="fabric-python-3.12",
        language=FabricLanguage.PYTHON312,
        transport=TransportKind.FABRIC,
        target=FabricTarget(WORKSPACE, NOTEBOOK, FabricLanguage.PYTHON312),
    )
    assert Profile.from_dict(python_profile.public_dict()) == python_profile
    with pytest.raises(ValueError, match="explicit target"):
        Profile(
            name="real",
            language=FabricLanguage.PYSPARK,
            transport=TransportKind.FABRIC,
        )
    profile = Profile(
        name="real",
        language=FabricLanguage.PYSPARK,
        transport=TransportKind.FABRIC,
        target=FabricTarget(WORKSPACE, NOTEBOOK, FabricLanguage.PYSPARK),
        startup_timeout_seconds=600,
        execution_timeout_seconds=300,
        request_timeout_seconds=30,
    )
    assert Profile.from_dict(profile.public_dict()) == profile
    with pytest.raises(ValueError, match="startup_timeout_seconds"):
        Profile(
            name="real",
            language=FabricLanguage.PYSPARK,
            transport=TransportKind.FABRIC,
            target=profile.target,
            startup_timeout_seconds=601,
        )


def test_invalid_fuse_identity_is_rejected(tmp_path: Path) -> None:
    mount = tmp_path / "notebook"
    mount.mkdir()
    (mount / ".fabric.json").write_text("{}")
    profile = Profile(
        name="from-fuse", language=FabricLanguage.PYTHON312, fuse_notebook_path=str(mount)
    )
    with pytest.raises(ValueError):
        resolve_target(profile)


def test_cli_configures_owner_private_fabric_profile(
    local_state: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert (
        main(
            [
                "profile",
                "configure",
                "--name",
                "fabric-pyspark",
                "--transport",
                "fabric",
                "--workspace",
                WORKSPACE,
                "--notebook",
                NOTEBOOK,
            ]
        )
        == 0
    )
    output = json.loads(capsys.readouterr().out)
    assert output["profile"]["transport"] == "fabric"
    profile = load_profiles()["fabric-pyspark"]
    assert profile.transport is TransportKind.FABRIC
    assert profile.target == FabricTarget(WORKSPACE, NOTEBOOK, FabricLanguage.PYSPARK)


def test_cli_does_not_promote_synthetic_default_target_to_fabric(
    local_state: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert (
        main(
            [
                "profile",
                "configure",
                "--name",
                "fabric-pyspark",
                "--transport",
                "fabric",
            ]
        )
        == 2
    )
    assert "explicit target" in capsys.readouterr().err


def test_cli_configures_python_311_runtime(
    local_state: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert (
        main(
            [
                "profile",
                "configure",
                "--name",
                "fabric-python-3.11",
                "--transport",
                "fabric",
                "--workspace",
                WORKSPACE,
                "--notebook",
                NOTEBOOK,
            ]
        )
        == 0
    )
    assert load_profiles()["fabric-python-3.11"].language is FabricLanguage.PYTHON311
