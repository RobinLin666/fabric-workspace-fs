"""Owner-private local state paths. No state is stored in notebook mounts."""

from __future__ import annotations

import os
import stat
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import TextIO

APP_DIRECTORY = "fabric-jupyter"
RUNTIME_DIRECTORY = "runtime"


def state_dir() -> Path:
    base = os.environ.get("LOCALAPPDATA") if os.name == "nt" else os.environ.get("XDG_STATE_HOME")
    if base:
        root = Path(base) / APP_DIRECTORY
        return root / RUNTIME_DIRECTORY if os.name == "nt" else root
    if os.name == "nt":
        return Path.home() / ".fabric-jupyter" / RUNTIME_DIRECTORY
    return Path.home() / ".local/state" / APP_DIRECTORY


def config_dir() -> Path:
    base = os.environ.get("APPDATA") if os.name == "nt" else os.environ.get("XDG_CONFIG_HOME")
    if base:
        return Path(base) / APP_DIRECTORY
    if os.name == "nt":
        return Path.home() / ".fabric-jupyter"
    return Path.home() / ".config" / APP_DIRECTORY


def ensure_private(directory: Path) -> Path:
    directory = directory.absolute()
    _reject_link_or_reparse_traversal(directory)
    if os.name == "nt":
        _ensure_private_windows(directory)
    else:
        _ensure_private_unix(directory)
    return directory


def verify_private_directory(directory: Path) -> Path:
    directory = directory.absolute()
    _reject_link_or_reparse_traversal(directory)
    if os.name == "nt":
        from . import _win_acl

        _verify_windows_parent_chain(directory.parent)
        if not directory.is_dir():
            raise NotADirectoryError(str(directory))
        _win_acl.reject_reparse_point(directory)
        try:
            _win_acl.validate_owner_only_dacl(directory)
        except PermissionError as exc:
            raise PermissionError(_windows_private_repair_message(directory, directory=True, reason=str(exc))) from exc
    else:
        _validate_unix_private_directory(directory)
    return directory


def profiles_path() -> Path:
    return ensure_private(config_dir()) / "profiles.json"


def broker_endpoint_path() -> Path:
    path = ensure_private(state_dir()) / "broker.json"
    _verify_private_endpoint_if_present(path, regular_file=True)
    return path


def broker_socket_path() -> Path:
    path = ensure_private(state_dir()) / "broker.sock"
    _verify_private_endpoint_if_present(path, regular_file=False)
    return path


def verify_private_file(path: Path) -> Path:
    path = path.absolute()
    verify_private_directory(path.parent)
    if os.name == "nt":
        _verify_private_file_windows(path)
    else:
        _verify_private_file_unix(path)
    return path


def verify_private_socket(path: Path) -> Path:
    path = path.absolute()
    verify_private_directory(path.parent)
    if os.name == "nt":
        raise RuntimeError("Unix socket broker endpoints are unavailable on this platform")
    _verify_private_socket_unix(path)
    return path


@contextmanager
def secure_private_file(path: Path, *, encoding: str = "utf-8") -> Iterator[TextIO]:
    path = path.absolute()
    ensure_private(path.parent)
    if os.name == "nt":
        import msvcrt

        from . import _win_acl

        handle = _win_acl.create_owner_only_file_handle(path)
        try:
            fd = int(msvcrt.__dict__["open_osfhandle"](handle, os.O_WRONLY))
        except Exception:
            _win_acl.kernel32.CloseHandle(handle)
            raise
    else:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)

    stream = os.fdopen(fd, "w", encoding=encoding)
    try:
        yield stream
    finally:
        stream.close()
        verify_private_file(path)


def _reject_link_or_reparse_traversal(path: Path) -> None:
    current = Path(path.anchor)
    for part in path.parts[1:] if path.anchor else path.parts:
        current /= part
        try:
            if os.name == "nt":
                if current.exists():
                    from . import _win_acl

                    _win_acl.reject_reparse_point(current)
            else:
                stat_result = current.lstat()
                if stat.S_ISLNK(stat_result.st_mode):
                    raise PermissionError(f"{current} must not be a symbolic link")
        except FileNotFoundError:
            return


def _ensure_private_windows(directory: Path) -> None:
    from . import _win_acl

    _verify_windows_parent_chain(directory.parent)
    if directory.exists():
        if not directory.is_dir():
            raise NotADirectoryError(str(directory))
        _win_acl.reject_reparse_point(directory)
        verify_private_directory(directory)
        return

    _reject_link_or_reparse_traversal(directory.parent)
    directory.parent.mkdir(parents=True, exist_ok=True)
    _reject_link_or_reparse_traversal(directory.parent)
    _verify_windows_parent_chain(directory.parent)
    _win_acl.create_owner_only_directory(directory)


def _ensure_private_unix(directory: Path) -> None:
    if directory.exists():
        _validate_unix_private_directory(directory)
        return

    _reject_link_or_reparse_traversal(directory.parent)
    directory.parent.mkdir(parents=True, exist_ok=True)
    _reject_link_or_reparse_traversal(directory.parent)
    directory.mkdir(mode=0o700)
    directory.chmod(0o700)
    _validate_unix_private_directory(directory)


def _validate_unix_private_directory(directory: Path) -> None:
    stat_result = directory.lstat()
    if not stat.S_ISDIR(stat_result.st_mode):
        raise NotADirectoryError(str(directory))
    if stat.S_ISLNK(stat_result.st_mode):
        raise PermissionError(f"{directory} must not be a symbolic link")
    if stat_result.st_uid != _current_uid():
        raise PermissionError(f"{directory} is not owned by the current user")
    if stat.S_IMODE(stat_result.st_mode) != 0o700:
        raise PermissionError(f"{directory} permissions must be 0700")


def _verify_private_endpoint_if_present(path: Path, *, regular_file: bool) -> None:
    try:
        if os.name == "nt":
            if path.exists() or path.is_symlink():
                if regular_file:
                    _verify_private_file_windows(path)
                else:
                    raise RuntimeError("Unix socket broker endpoints are unavailable on this platform")
        else:
            if regular_file:
                _verify_private_file_unix(path)
            else:
                _verify_private_socket_unix(path)
    except FileNotFoundError:
        return


def _verify_private_file_windows(path: Path) -> None:
    from . import _win_acl

    _win_acl.reject_reparse_point(path)
    if not path.is_file():
        raise PermissionError(f"{path} must be a regular file")
    try:
        _win_acl.validate_owner_only_dacl(path)
    except PermissionError as exc:
        raise PermissionError(_windows_private_repair_message(path, directory=False, reason=str(exc))) from exc


def _verify_private_file_unix(path: Path) -> None:
    stat_result = path.lstat()
    mode = stat_result.st_mode
    if stat.S_ISLNK(mode):
        raise PermissionError(f"{path} must not be a symbolic link")
    if not stat.S_ISREG(mode):
        raise PermissionError(f"{path} must be a regular file")
    _verify_unix_owner_only_endpoint(path, stat_result)


def _verify_private_socket_unix(path: Path) -> None:
    stat_result = path.lstat()
    mode = stat_result.st_mode
    if stat.S_ISLNK(mode):
        raise PermissionError(f"{path} must not be a symbolic link")
    if not stat.S_ISSOCK(mode):
        raise PermissionError(f"{path} must be a socket")
    _verify_unix_owner_only_endpoint(path, stat_result)


def _verify_unix_owner_only_endpoint(path: Path, stat_result: os.stat_result) -> None:
    if stat_result.st_uid != _current_uid():
        raise PermissionError(f"{path} is not owned by the current user")
    if stat.S_IMODE(stat_result.st_mode) & 0o077:
        raise PermissionError(f"{path} permissions must not grant group or other access")


def protect_private_file(path: Path) -> Path:
    if os.name == "nt":
        from . import _win_acl

        _win_acl.reject_reparse_point(path)
        if not path.is_file():
            raise PermissionError(f"{path} must be a regular file")
        _win_acl.set_owner_only_dacl(path)
    else:
        stat_result = path.lstat()
        if not stat.S_ISREG(stat_result.st_mode):
            raise PermissionError(f"{path} must be a regular file")
        if stat_result.st_uid != _current_uid():
            raise PermissionError(f"{path} is not owned by the current user")
        path.chmod(0o600)
        _verify_private_file_unix(path)
    return path


def _windows_private_repair_message(path: Path, *, directory: bool, reason: str) -> str:
    inheritance = "(OI)(CI)F" if directory else "F"
    return (
        f"{reason}. Repair only if you own and trust this path by running in PowerShell: "
        f"$p = {str(path)!r}; "
        f"icacls $p /inheritance:r /remove:g *S-1-1-0 *S-1-5-11 *S-1-5-18 *S-1-5-32-544; "
        f"icacls $p /grant:r \"*{_windows_current_user_sid()}:{inheritance}\""
    )


def _verify_windows_parent_chain(parent: Path) -> None:
    from . import _win_acl

    absolute = parent.absolute()
    boundaries = [
        Path(value).absolute()
        for name in ("LOCALAPPDATA", "APPDATA")
        if (value := os.environ.get(name))
    ]
    boundary = next(
        (candidate for candidate in boundaries if absolute.is_relative_to(candidate)),
        absolute,
    )
    _win_acl.validate_secure_parent_chain(absolute, stop_at=boundary)


def _windows_current_user_sid() -> str:
    if os.name != "nt":
        raise RuntimeError("Windows SID checks are unavailable on this platform")
    from . import _win_acl

    return _win_acl.current_user_sid_string()


def _current_uid() -> int:
    getuid = getattr(os, "getuid", None)
    if getuid is None:
        raise RuntimeError("Unix uid checks are unavailable on this platform")
    return int(getuid())
