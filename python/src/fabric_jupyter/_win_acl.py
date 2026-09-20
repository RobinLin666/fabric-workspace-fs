"""Windows owner-only ACL helpers for local IPC state."""

from __future__ import annotations

import ctypes
import os
from pathlib import Path
from typing import Final

if os.name != "nt":  # pragma: no cover - imported conditionally by paths.py
    raise ImportError("Windows ACL helpers are only available on Windows")

from ctypes import wintypes

ERROR_ALREADY_EXISTS: Final = 183
ERROR_FILE_EXISTS: Final = 80
ERROR_INSUFFICIENT_BUFFER: Final = 122

TOKEN_QUERY: Final = 0x0008
TOKEN_USER_CLASS: Final = 1

SE_FILE_OBJECT: Final = 1
DACL_SECURITY_INFORMATION: Final = 0x00000004
PROTECTED_DACL_SECURITY_INFORMATION: Final = 0x80000000
OWNER_SECURITY_INFORMATION: Final = 0x00000001

SDDL_REVISION_1: Final = 1
SE_DACL_PROTECTED: Final = 0x1000

ACL_SIZE_INFORMATION_CLASS: Final = 2
ACCESS_ALLOWED_ACE_TYPE: Final = 0
ACCESS_DENIED_ACE_TYPE: Final = 1
INHERIT_ONLY_ACE: Final = 0x08
FILE_ATTRIBUTE_REPARSE_POINT: Final = 0x00000400
INVALID_FILE_ATTRIBUTES: Final = 0xFFFFFFFF
FILE_ALL_ACCESS: Final = 0x001F01FF
FILE_WRITE_DATA: Final = 0x00000002
FILE_APPEND_DATA: Final = 0x00000004
FILE_DELETE_CHILD: Final = 0x00000040
DELETE: Final = 0x00010000
WRITE_DAC: Final = 0x00040000
WRITE_OWNER: Final = 0x00080000
GENERIC_ALL: Final = 0x10000000
GENERIC_WRITE: Final = 0x40000000
DANGEROUS_PARENT_ACCESS: Final = (
    FILE_WRITE_DATA
    | FILE_APPEND_DATA
    | FILE_DELETE_CHILD
    | DELETE
    | WRITE_DAC
    | WRITE_OWNER
    | GENERIC_WRITE
    | GENERIC_ALL
)

DRIVE_REMOTE: Final = 4
SYSTEM_SID: Final = "S-1-5-18"
ADMINISTRATORS_SID: Final = "S-1-5-32-544"
CREATOR_OWNER_SID: Final = "S-1-3-0"
OWNER_RIGHTS_SID: Final = "S-1-3-4"

CREATE_NEW: Final = 1
FILE_ATTRIBUTE_NORMAL: Final = 0x00000080
INVALID_HANDLE_VALUE: Final = ctypes.c_void_p(-1).value


class SECURITY_ATTRIBUTES(ctypes.Structure):
    _fields_ = [
        ("nLength", wintypes.DWORD),
        ("lpSecurityDescriptor", wintypes.LPVOID),
        ("bInheritHandle", wintypes.BOOL),
    ]


class SID_AND_ATTRIBUTES(ctypes.Structure):
    _fields_ = [
        ("Sid", wintypes.LPVOID),
        ("Attributes", wintypes.DWORD),
    ]


class TOKEN_USER(ctypes.Structure):
    _fields_ = [("User", SID_AND_ATTRIBUTES)]


class ACL_SIZE_INFORMATION(ctypes.Structure):
    _fields_ = [
        ("AceCount", wintypes.DWORD),
        ("AclBytesInUse", wintypes.DWORD),
        ("AclBytesFree", wintypes.DWORD),
    ]


class ACE_HEADER(ctypes.Structure):
    _fields_ = [
        ("AceType", wintypes.BYTE),
        ("AceFlags", wintypes.BYTE),
        ("AceSize", wintypes.WORD),
    ]


class ACCESS_ALLOWED_ACE(ctypes.Structure):
    _fields_ = [
        ("Header", ACE_HEADER),
        ("Mask", wintypes.DWORD),
        ("SidStart", wintypes.DWORD),
    ]


_win_dll = ctypes.__dict__["WinDLL"]
advapi32 = _win_dll("advapi32", use_last_error=True)
kernel32 = _win_dll("kernel32", use_last_error=True)

advapi32.OpenProcessToken.argtypes = [wintypes.HANDLE, wintypes.DWORD, ctypes.POINTER(wintypes.HANDLE)]
advapi32.OpenProcessToken.restype = wintypes.BOOL
advapi32.GetTokenInformation.argtypes = [
    wintypes.HANDLE,
    wintypes.DWORD,
    wintypes.LPVOID,
    wintypes.DWORD,
    ctypes.POINTER(wintypes.DWORD),
]
advapi32.GetTokenInformation.restype = wintypes.BOOL
advapi32.ConvertSidToStringSidW.argtypes = [wintypes.LPVOID, ctypes.POINTER(wintypes.LPWSTR)]
advapi32.ConvertSidToStringSidW.restype = wintypes.BOOL
advapi32.ConvertStringSecurityDescriptorToSecurityDescriptorW.argtypes = [
    wintypes.LPCWSTR,
    wintypes.DWORD,
    ctypes.POINTER(wintypes.LPVOID),
    ctypes.POINTER(wintypes.ULONG),
]
advapi32.ConvertStringSecurityDescriptorToSecurityDescriptorW.restype = wintypes.BOOL
advapi32.GetSecurityDescriptorDacl.argtypes = [
    wintypes.LPVOID,
    ctypes.POINTER(wintypes.BOOL),
    ctypes.POINTER(wintypes.LPVOID),
    ctypes.POINTER(wintypes.BOOL),
]
advapi32.GetSecurityDescriptorDacl.restype = wintypes.BOOL
advapi32.GetNamedSecurityInfoW.argtypes = [
    wintypes.LPWSTR,
    wintypes.DWORD,
    wintypes.DWORD,
    ctypes.POINTER(wintypes.LPVOID),
    ctypes.POINTER(wintypes.LPVOID),
    ctypes.POINTER(wintypes.LPVOID),
    ctypes.POINTER(wintypes.LPVOID),
    ctypes.POINTER(wintypes.LPVOID),
]
advapi32.GetNamedSecurityInfoW.restype = wintypes.DWORD
advapi32.SetNamedSecurityInfoW.argtypes = [
    wintypes.LPWSTR,
    wintypes.DWORD,
    wintypes.DWORD,
    wintypes.LPVOID,
    wintypes.LPVOID,
    wintypes.LPVOID,
    wintypes.LPVOID,
]
advapi32.SetNamedSecurityInfoW.restype = wintypes.DWORD
advapi32.GetSecurityDescriptorControl.argtypes = [
    wintypes.LPVOID,
    ctypes.POINTER(wintypes.WORD),
    ctypes.POINTER(wintypes.DWORD),
]
advapi32.GetSecurityDescriptorControl.restype = wintypes.BOOL
advapi32.GetAclInformation.argtypes = [
    wintypes.LPVOID,
    wintypes.LPVOID,
    wintypes.DWORD,
    wintypes.DWORD,
]
advapi32.GetAclInformation.restype = wintypes.BOOL
advapi32.GetAce.argtypes = [wintypes.LPVOID, wintypes.DWORD, ctypes.POINTER(wintypes.LPVOID)]
advapi32.GetAce.restype = wintypes.BOOL

kernel32.GetCurrentProcess.argtypes = []
kernel32.GetCurrentProcess.restype = wintypes.HANDLE
kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
kernel32.CloseHandle.restype = wintypes.BOOL
kernel32.LocalFree.argtypes = [wintypes.HLOCAL]
kernel32.LocalFree.restype = wintypes.HLOCAL
kernel32.CreateDirectoryW.argtypes = [wintypes.LPCWSTR, ctypes.POINTER(SECURITY_ATTRIBUTES)]
kernel32.CreateDirectoryW.restype = wintypes.BOOL
kernel32.CreateFileW.argtypes = [
    wintypes.LPCWSTR,
    wintypes.DWORD,
    wintypes.DWORD,
    ctypes.POINTER(SECURITY_ATTRIBUTES),
    wintypes.DWORD,
    wintypes.DWORD,
    wintypes.HANDLE,
]
kernel32.CreateFileW.restype = wintypes.HANDLE
kernel32.GetFileAttributesW.argtypes = [wintypes.LPCWSTR]
kernel32.GetFileAttributesW.restype = wintypes.DWORD
kernel32.GetDriveTypeW.argtypes = [wintypes.LPCWSTR]
kernel32.GetDriveTypeW.restype = wintypes.UINT


def _raise_last_error(message: str) -> None:
    error = _get_last_error()
    raise OSError(error, f"{message}: {_format_error(error)}")


def _check_win32_error(code: int, message: str) -> None:
    if code:
        raise OSError(code, f"{message}: {_format_error(code)}")


def _get_last_error() -> int:
    return int(ctypes.__dict__["get_last_error"]())


def _format_error(error: int) -> str:
    return str(ctypes.__dict__["FormatError"](error))


def _sid_to_string(sid: wintypes.LPVOID) -> str:
    sid_string = wintypes.LPWSTR()
    if not advapi32.ConvertSidToStringSidW(sid, ctypes.byref(sid_string)):
        _raise_last_error("ConvertSidToStringSidW failed")
    try:
        return str(sid_string.value)
    finally:
        kernel32.LocalFree(ctypes.cast(sid_string, wintypes.HLOCAL))


def current_user_sid_string() -> str:
    """Return the current process user's SID string."""

    token = wintypes.HANDLE()
    if not advapi32.OpenProcessToken(kernel32.GetCurrentProcess(), TOKEN_QUERY, ctypes.byref(token)):
        _raise_last_error("OpenProcessToken failed")
    try:
        needed = wintypes.DWORD()
        advapi32.GetTokenInformation(token, TOKEN_USER_CLASS, None, 0, ctypes.byref(needed))
        if _get_last_error() != ERROR_INSUFFICIENT_BUFFER:
            _raise_last_error("GetTokenInformation sizing failed")
        buffer = ctypes.create_string_buffer(needed.value)
        if not advapi32.GetTokenInformation(
            token,
            TOKEN_USER_CLASS,
            ctypes.byref(buffer),
            needed,
            ctypes.byref(needed),
        ):
            _raise_last_error("GetTokenInformation failed")
        user = ctypes.cast(buffer, ctypes.POINTER(TOKEN_USER)).contents
        return _sid_to_string(user.User.Sid)
    finally:
        kernel32.CloseHandle(token)


def _security_descriptor_from_sddl(sddl: str) -> wintypes.LPVOID:
    security_descriptor = wintypes.LPVOID()
    if not advapi32.ConvertStringSecurityDescriptorToSecurityDescriptorW(
        sddl,
        SDDL_REVISION_1,
        ctypes.byref(security_descriptor),
        None,
    ):
        _raise_last_error("ConvertStringSecurityDescriptorToSecurityDescriptorW failed")
    return security_descriptor


def _set_dacl_from_sddl(path: Path, sddl: str) -> None:
    security_descriptor = _security_descriptor_from_sddl(sddl)
    dacl_present = wintypes.BOOL()
    dacl_defaulted = wintypes.BOOL()
    dacl = wintypes.LPVOID()
    try:
        if not advapi32.GetSecurityDescriptorDacl(
            security_descriptor,
            ctypes.byref(dacl_present),
            ctypes.byref(dacl),
            ctypes.byref(dacl_defaulted),
        ):
            _raise_last_error("GetSecurityDescriptorDacl failed")
        if not dacl_present or not dacl:
            raise PermissionError(f"{path} security descriptor does not contain a DACL")
        _check_win32_error(
            advapi32.SetNamedSecurityInfoW(
                str(path),
                SE_FILE_OBJECT,
                DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION,
                None,
                None,
                dacl,
                None,
            ),
            f"SetNamedSecurityInfoW failed for {path}",
        )
    finally:
        kernel32.LocalFree(security_descriptor)


def owner_only_sddl() -> str:
    """Return a protected DACL SDDL string that grants only the current user full access."""

    return f"D:P(A;;FA;;;{current_user_sid_string()})"


def set_owner_only_dacl(path: Path) -> None:
    """Replace an existing filesystem object's DACL with current-user-only full access."""

    _set_dacl_from_sddl(path, owner_only_sddl())
    validate_owner_only_dacl(path)


def create_owner_only_directory(path: Path) -> None:
    """Create a directory with a current-user-only protected DACL."""

    security_descriptor = _security_descriptor_from_sddl(owner_only_sddl())
    attributes = SECURITY_ATTRIBUTES(
        ctypes.sizeof(SECURITY_ATTRIBUTES),
        security_descriptor,
        False,
    )
    try:
        if not kernel32.CreateDirectoryW(str(path), ctypes.byref(attributes)):
            error = _get_last_error()
            if error == ERROR_ALREADY_EXISTS:
                raise FileExistsError(error, f"{path} already exists", str(path))
            raise OSError(error, f"CreateDirectoryW failed for {path}: {_format_error(error)}")
    finally:
        kernel32.LocalFree(security_descriptor)
    validate_owner_only_dacl(path)


def create_owner_only_file_handle(path: Path) -> int:
    """Create a new file with a current-user-only protected DACL and return its handle."""

    security_descriptor = _security_descriptor_from_sddl(owner_only_sddl())
    attributes = SECURITY_ATTRIBUTES(
        ctypes.sizeof(SECURITY_ATTRIBUTES),
        security_descriptor,
        False,
    )
    handle = wintypes.HANDLE()
    try:
        handle = kernel32.CreateFileW(
            str(path),
            GENERIC_WRITE,
            0,
            ctypes.byref(attributes),
            CREATE_NEW,
            FILE_ATTRIBUTE_NORMAL,
            None,
        )
        if handle == INVALID_HANDLE_VALUE:
            error = _get_last_error()
            if error == ERROR_FILE_EXISTS:
                raise FileExistsError(error, f"{path} already exists", str(path))
            _raise_last_error(f"CreateFileW failed for {path}")
        try:
            validate_owner_only_dacl(path)
        except Exception:
            kernel32.CloseHandle(handle)
            raise
        return int(handle)
    finally:
        kernel32.LocalFree(security_descriptor)


def reject_reparse_point(path: Path) -> None:
    attributes = kernel32.GetFileAttributesW(str(path))
    if attributes == INVALID_FILE_ATTRIBUTES:
        _raise_last_error(f"GetFileAttributesW failed for {path}")
    if attributes & FILE_ATTRIBUTE_REPARSE_POINT:
        raise PermissionError(f"{path} must not be a reparse point")


def validate_secure_parent_chain(path: Path, *, stop_at: Path) -> None:
    """Reject remote paths or ancestors writable by untrusted Windows principals."""

    absolute = path.absolute()
    boundary = stop_at.absolute()
    if not absolute.is_relative_to(boundary):
        raise ValueError(f"{boundary} is not an ancestor of {absolute}")
    if str(absolute).startswith("\\\\") or kernel32.GetDriveTypeW(absolute.anchor) == DRIVE_REMOTE:
        raise PermissionError(f"{absolute} must be on a local filesystem")

    trusted_sids = {
        current_user_sid_string(),
        SYSTEM_SID,
        ADMINISTRATORS_SID,
        CREATOR_OWNER_SID,
        OWNER_RIGHTS_SID,
    }
    current = absolute
    while True:
        if current.exists():
            reject_reparse_point(current)
            _validate_parent_acl(current, trusted_sids)
        if current == boundary:
            break
        current = current.parent


def _validate_parent_acl(path: Path, trusted_sids: set[str]) -> None:
    owner = wintypes.LPVOID()
    dacl = wintypes.LPVOID()
    security_descriptor = wintypes.LPVOID()
    _check_win32_error(
        advapi32.GetNamedSecurityInfoW(
            str(path),
            SE_FILE_OBJECT,
            OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION,
            ctypes.byref(owner),
            None,
            ctypes.byref(dacl),
            None,
            ctypes.byref(security_descriptor),
        ),
        f"GetNamedSecurityInfoW failed for {path}",
    )
    try:
        owner_sid = _sid_to_string(owner)
        if owner_sid not in trusted_sids:
            raise PermissionError(f"{path} is owned by an untrusted SID {owner_sid}")
        if not dacl:
            raise PermissionError(f"{path} does not have a DACL")

        acl_information = ACL_SIZE_INFORMATION()
        if not advapi32.GetAclInformation(
            dacl,
            ctypes.byref(acl_information),
            ctypes.sizeof(acl_information),
            ACL_SIZE_INFORMATION_CLASS,
        ):
            _raise_last_error("GetAclInformation failed")
        for index in range(acl_information.AceCount):
            ace = wintypes.LPVOID()
            if not advapi32.GetAce(dacl, index, ctypes.byref(ace)):
                _raise_last_error("GetAce failed")
            allowed = ctypes.cast(ace, ctypes.POINTER(ACCESS_ALLOWED_ACE)).contents
            if allowed.Header.AceType == ACCESS_DENIED_ACE_TYPE:
                continue
            if allowed.Header.AceType != ACCESS_ALLOWED_ACE_TYPE:
                continue
            if allowed.Header.AceFlags & INHERIT_ONLY_ACE:
                continue
            if not allowed.Mask & DANGEROUS_PARENT_ACCESS:
                continue
            sid_address = ctypes.addressof(allowed) + ACCESS_ALLOWED_ACE.SidStart.offset
            ace_sid = _sid_to_string(ctypes.cast(sid_address, wintypes.LPVOID))
            if ace_sid not in trusted_sids:
                raise PermissionError(
                    f"{path} grants write, create, or delete access to untrusted SID {ace_sid}"
                )
    finally:
        kernel32.LocalFree(security_descriptor)


def validate_owner_only_dacl(path: Path) -> None:
    """Fail unless path is owned by and grants access only to the current user."""

    owner = wintypes.LPVOID()
    dacl = wintypes.LPVOID()
    security_descriptor = wintypes.LPVOID()
    _check_win32_error(
        advapi32.GetNamedSecurityInfoW(
            str(path),
            SE_FILE_OBJECT,
            OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION,
            ctypes.byref(owner),
            None,
            ctypes.byref(dacl),
            None,
            ctypes.byref(security_descriptor),
        ),
        f"GetNamedSecurityInfoW failed for {path}",
    )
    try:
        current_user_sid = current_user_sid_string()
        if _sid_to_string(owner) != current_user_sid:
            raise PermissionError(f"{path} is not owned by the current user")
        if not dacl:
            raise PermissionError(f"{path} does not have a DACL")

        control = wintypes.WORD()
        revision = wintypes.DWORD()
        if not advapi32.GetSecurityDescriptorControl(
            security_descriptor,
            ctypes.byref(control),
            ctypes.byref(revision),
        ):
            _raise_last_error("GetSecurityDescriptorControl failed")
        if not control.value & SE_DACL_PROTECTED:
            raise PermissionError(f"{path} DACL inherits permissions")

        acl_information = ACL_SIZE_INFORMATION()
        if not advapi32.GetAclInformation(
            dacl,
            ctypes.byref(acl_information),
            ctypes.sizeof(acl_information),
            ACL_SIZE_INFORMATION_CLASS,
        ):
            _raise_last_error("GetAclInformation failed")
        if acl_information.AceCount != 1:
            raise PermissionError(f"{path} DACL must contain exactly one ACE")

        ace = wintypes.LPVOID()
        if not advapi32.GetAce(dacl, 0, ctypes.byref(ace)):
            _raise_last_error("GetAce failed")
        allowed = ctypes.cast(ace, ctypes.POINTER(ACCESS_ALLOWED_ACE)).contents
        if allowed.Header.AceType != ACCESS_ALLOWED_ACE_TYPE:
            raise PermissionError(f"{path} DACL ACE must be an allow ACE")
        if allowed.Mask & FILE_ALL_ACCESS != FILE_ALL_ACCESS:
            raise PermissionError(f"{path} DACL must grant the owner full access")
        sid_address = ctypes.addressof(allowed) + ACCESS_ALLOWED_ACE.SidStart.offset
        ace_sid = ctypes.cast(sid_address, wintypes.LPVOID)
        if _sid_to_string(ace_sid) != current_user_sid:
            raise PermissionError(f"{path} DACL grants access to a non-owner SID")
    finally:
        kernel32.LocalFree(security_descriptor)
