from __future__ import annotations

import os
import stat
from pathlib import Path

import pytest

from fabric_jupyter.paths import (
    broker_endpoint_path,
    ensure_private,
    protect_private_file,
    secure_private_file,
    state_dir,
    verify_private_file,
)


def test_state_dir_uses_dedicated_app_directory(local_state: Path) -> None:
    expected = local_state / "state" / "fabric-jupyter"
    if os.name == "nt":
        expected /= "runtime"
    assert state_dir() == expected


@pytest.mark.skipif(os.name == "nt", reason="POSIX permissions only")
def test_unix_ensure_private_rejects_existing_shared_directory(tmp_path: Path) -> None:
    directory = tmp_path / "state" / "fabric-jupyter"
    directory.mkdir(parents=True)
    directory.chmod(0o755)

    with pytest.raises(PermissionError, match="0700"):
        ensure_private(directory)


@pytest.mark.skipif(os.name == "nt", reason="POSIX permissions only")
def test_unix_broker_endpoint_rejects_symlink(local_state: Path) -> None:
    directory = ensure_private(state_dir())
    target = local_state / "attacker.json"
    target.write_text("{}", encoding="utf-8")
    (directory / "broker.json").symlink_to(target)

    with pytest.raises(PermissionError, match="symbolic link"):
        broker_endpoint_path()


@pytest.mark.skipif(os.name == "nt", reason="POSIX permissions only")
def test_unix_broker_endpoint_rejects_shared_file(local_state: Path) -> None:
    endpoint = ensure_private(state_dir()) / "broker.json"
    endpoint.write_text("{}", encoding="utf-8")
    endpoint.chmod(0o644)

    with pytest.raises(PermissionError, match="group or other"):
        broker_endpoint_path()


@pytest.mark.skipif(os.name != "nt", reason="Windows ACLs only")
def test_windows_ensure_private_creates_owner_only_acl(local_state: Path) -> None:
    from fabric_jupyter import _win_acl

    directory = ensure_private(state_dir())

    _win_acl.validate_owner_only_dacl(directory)


@pytest.mark.skipif(os.name != "nt", reason="Windows ACLs only")
def test_windows_state_dir_accepts_normal_local_app_root(local_state: Path) -> None:
    from fabric_jupyter import _win_acl

    app_root = local_state / "state" / "fabric-jupyter"
    app_root.mkdir(parents=True)

    runtime = ensure_private(state_dir())

    assert runtime == app_root / "runtime"
    _win_acl.validate_owner_only_dacl(runtime)


@pytest.mark.skipif(os.name != "nt", reason="Windows ACLs only")
def test_windows_ensure_private_rejects_existing_shared_acl(local_state: Path) -> None:
    from fabric_jupyter import _win_acl

    directory = state_dir()
    directory.mkdir(parents=True)
    _win_acl._set_dacl_from_sddl(
        directory,
        f"D:P(A;;FA;;;{_win_acl.current_user_sid_string()})(A;;FA;;;WD)",
    )

    with pytest.raises(PermissionError, match="Repair only if you own and trust this path"):
        ensure_private(directory)


@pytest.mark.skipif(os.name != "nt", reason="Windows ACLs only")
def test_windows_state_dir_rejects_shared_writable_app_root(local_state: Path) -> None:
    from fabric_jupyter import _win_acl

    app_root = local_state / "state" / "fabric-jupyter"
    app_root.mkdir(parents=True)
    _win_acl._set_dacl_from_sddl(
        app_root,
        f"D:P(A;;FA;;;{_win_acl.current_user_sid_string()})(A;;0x1301bf;;;WD)",
    )

    with pytest.raises(PermissionError, match="write, create, or delete access"):
        ensure_private(state_dir())


@pytest.mark.skipif(os.name != "nt", reason="Windows ACLs only")
def test_windows_state_dir_rejects_shared_writable_localappdata(local_state: Path) -> None:
    from fabric_jupyter import _win_acl

    local_app_data = local_state / "state"
    local_app_data.mkdir()
    _win_acl._set_dacl_from_sddl(
        local_app_data,
        f"D:P(A;;FA;;;{_win_acl.current_user_sid_string()})(A;;0x1301bf;;;WD)",
    )

    with pytest.raises(PermissionError, match="write, create, or delete access"):
        ensure_private(state_dir())


@pytest.mark.skipif(os.name != "nt", reason="Windows ACLs only")
def test_windows_verify_runtime_rejects_app_parent_acl_tampering(local_state: Path) -> None:
    from fabric_jupyter import _win_acl
    from fabric_jupyter.paths import verify_private_directory

    runtime = ensure_private(state_dir())
    app_root = runtime.parent
    _win_acl._set_dacl_from_sddl(
        app_root,
        f"D:P(A;;FA;;;{_win_acl.current_user_sid_string()})(A;;0x1301bf;;;WD)",
    )

    with pytest.raises(PermissionError, match="write, create, or delete access"):
        verify_private_directory(runtime)


@pytest.mark.skipif(os.name != "nt", reason="Windows paths only")
def test_windows_parent_chain_rejects_unc_path() -> None:
    from fabric_jupyter import _win_acl

    share = Path(r"\\server\share")
    with pytest.raises(PermissionError, match="local filesystem"):
        _win_acl.validate_secure_parent_chain(share / "fabric-jupyter", stop_at=share)


@pytest.mark.skipif(os.name != "nt", reason="Windows ACLs only")
def test_windows_broker_endpoint_rejects_shared_acl(local_state: Path) -> None:
    from fabric_jupyter import _win_acl

    endpoint = ensure_private(state_dir()) / "broker.json"
    endpoint.write_text("{}", encoding="utf-8")
    protect_private_file(endpoint)
    _win_acl._set_dacl_from_sddl(
        endpoint,
        f"D:P(A;;FA;;;{_win_acl.current_user_sid_string()})(A;;FA;;;WD)",
    )

    with pytest.raises(PermissionError) as exc_info:
        verify_private_file(endpoint)
    message = str(exc_info.value)
    assert "exactly one ACE" in message
    assert "icacls" in message


@pytest.mark.skipif(os.name != "nt", reason="Windows ACLs only")
def test_windows_broker_endpoint_rejects_restricted_owner_acl(local_state: Path) -> None:
    from fabric_jupyter import _win_acl

    endpoint = ensure_private(state_dir()) / "broker.json"
    endpoint.write_text("{}", encoding="utf-8")
    protect_private_file(endpoint)
    _win_acl._set_dacl_from_sddl(
        endpoint,
        f"D:P(A;;FR;;;{_win_acl.current_user_sid_string()})",
    )

    with pytest.raises(PermissionError, match="full access.*icacls"):
        verify_private_file(endpoint)


def test_secure_private_file_creates_verifiable_private_file(local_state: Path) -> None:
    endpoint = ensure_private(state_dir()) / "created.json"

    with secure_private_file(endpoint) as stream:
        stream.write("{}")

    assert verify_private_file(endpoint) == endpoint


def test_secure_private_file_rejects_existing_file(local_state: Path) -> None:
    endpoint = ensure_private(state_dir()) / "created.json"
    with secure_private_file(endpoint) as stream:
        stream.write("{}")

    with pytest.raises(FileExistsError):
        with secure_private_file(endpoint):
            pass


@pytest.mark.skipif(os.name != "nt", reason="Windows ACLs only")
def test_windows_protect_private_file_repairs_new_file_acl(local_state: Path) -> None:
    from fabric_jupyter import _win_acl

    endpoint = ensure_private(state_dir()) / "broker.json"
    endpoint.write_text("{}", encoding="utf-8")
    _win_acl._set_dacl_from_sddl(
        endpoint,
        f"D:P(A;;FA;;;{_win_acl.current_user_sid_string()})(A;;FA;;;WD)",
    )

    protect_private_file(endpoint)
    _win_acl.validate_owner_only_dacl(endpoint)


@pytest.mark.skipif(os.name != "nt", reason="Windows reparse points only")
def test_windows_broker_endpoint_rejects_reparse_point(local_state: Path) -> None:
    directory = ensure_private(state_dir())
    target = local_state / "attacker.json"
    target.write_text("{}", encoding="utf-8")
    endpoint = directory / "broker.json"
    try:
        endpoint.symlink_to(target)
    except OSError as exc:
        pytest.skip(f"symlink creation is unavailable: {exc}")

    with pytest.raises(PermissionError, match="reparse point"):
        broker_endpoint_path()


@pytest.mark.skipif(os.name == "nt", reason="POSIX mode check only")
def test_unix_ensure_private_creates_0700(local_state: Path) -> None:
    directory = ensure_private(state_dir())

    assert stat.S_IMODE(directory.stat().st_mode) == 0o700
