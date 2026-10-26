"""Windows DPAPI protected files and exclusive process locks."""

from __future__ import annotations

import ctypes
import os
import re
import secrets
import subprocess
import time
from ctypes import wintypes
from pathlib import Path
from typing import BinaryIO


class ProtectedStorageError(RuntimeError):
    """Protected persistence is unavailable or a record cannot be read safely."""


class _DataBlob(ctypes.Structure):
    _fields_ = [("cbData", wintypes.DWORD), ("pbData", ctypes.POINTER(ctypes.c_byte))]


def _blob(data: bytes) -> tuple[_DataBlob, ctypes.Array]:
    backing = ctypes.create_string_buffer(data, max(1, len(data)))
    return _DataBlob(len(data), ctypes.cast(backing, ctypes.POINTER(ctypes.c_byte))), backing


def _dpapi_apis():
    """Load DPAPI functions with pointer-sized signatures and thread-local last error."""
    crypt32 = ctypes.WinDLL("crypt32", use_last_error=True)
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)

    crypt32.CryptProtectData.argtypes = [
        ctypes.POINTER(_DataBlob), wintypes.LPCWSTR, ctypes.POINTER(_DataBlob),
        wintypes.LPVOID, wintypes.LPVOID, wintypes.DWORD, ctypes.POINTER(_DataBlob),
    ]
    crypt32.CryptProtectData.restype = wintypes.BOOL
    crypt32.CryptUnprotectData.argtypes = [
        ctypes.POINTER(_DataBlob), ctypes.POINTER(wintypes.LPWSTR), ctypes.POINTER(_DataBlob),
        wintypes.LPVOID, wintypes.LPVOID, wintypes.DWORD, ctypes.POINTER(_DataBlob),
    ]
    crypt32.CryptUnprotectData.restype = wintypes.BOOL
    kernel32.LocalFree.argtypes = [ctypes.c_void_p]
    kernel32.LocalFree.restype = ctypes.c_void_p
    return crypt32, kernel32


def _crypt(data: bytes, *, protect: bool) -> bytes:
    if os.name != "nt":
        raise ProtectedStorageError("Windows DPAPI is required for recovery storage")
    source, source_memory = _blob(data)
    target = _DataBlob()
    try:
        crypt32, kernel32 = _dpapi_apis()
    except Exception as exc:
        raise ProtectedStorageError("Windows DPAPI is unavailable") from exc
    failure: Exception | None = None
    result: bytes | None = None
    try:
        if protect:
            ok = crypt32.CryptProtectData(
                ctypes.byref(source), "Deposit Python recovery", None, None, None, 0, ctypes.byref(target)
            )
        else:
            ok = crypt32.CryptUnprotectData(
                ctypes.byref(source), None, None, None, None, 0, ctypes.byref(target)
            )
        if not ok:
            error_code = ctypes.get_last_error() if hasattr(ctypes, "get_last_error") else 0
            failure = OSError(error_code, "Windows DPAPI operation failed")
        else:
            result = ctypes.string_at(target.pbData, target.cbData)
    except Exception as exc:
        failure = exc
    finally:
        free_failure: Exception | None = None
        if target.pbData:
            try:
                remaining = kernel32.LocalFree(ctypes.cast(target.pbData, ctypes.c_void_p))
                if remaining:
                    free_failure = OSError("Windows LocalFree failed")
            except Exception as exc:
                free_failure = exc
        if free_failure is not None and failure is None:
            failure = free_failure
    if failure is not None:
        raise ProtectedStorageError("Windows DPAPI operation failed") from failure
    if result is None:
        raise ProtectedStorageError("Windows DPAPI returned no protected data")
    return result


def _secure_acl(path: Path, *, directory: bool) -> None:
    if os.name != "nt":
        raise ProtectedStorageError("Windows ACLs are required for recovery storage")
    try:
        import csv

        identity = subprocess.run(
            ["whoami.exe", "/user", "/fo", "csv", "/nh"], capture_output=True, text=True, check=True
        )
        fields = next(csv.reader(identity.stdout.splitlines()))
        sid = next((field for field in fields if field.startswith("S-1-")), None)
        account = next((field for field in fields if "\\" in field), None)
        if not sid or not account:
            raise ValueError("Current Windows SID was not returned")
        inheritance = subprocess.run(
            ["icacls.exe", str(path), "/inheritance:r"], capture_output=True, text=True
        )
        if inheritance.returncode:
            raise OSError(inheritance.stderr.strip() or "ACL inheritance could not be removed")
        rights = "(OI)(CI)F" if directory else "F"
        grant = subprocess.run(
            ["icacls.exe", str(path), "/grant:r", f"*{sid}:{rights}"], capture_output=True, text=True
        )
        if grant.returncode:
            raise OSError(grant.stderr.strip() or "Current-user ACL could not be applied")
        verify = subprocess.run(["icacls.exe", str(path)], capture_output=True, text=True, check=True)
        principals = []
        for line in verify.stdout.splitlines():
            match = re.search(r"\s([^:]+):\(", line)
            if match:
                principals.append(match.group(1))
        if not principals or any(principal.lower() not in {account.lower(), f"*{sid}".lower()} for principal in principals):
            raise PermissionError("Recovery ACL grants access to an unexpected identity")
    except Exception as exc:
        raise ProtectedStorageError("Could not apply the current-user-only recovery ACL") from exc


class FileLock:
    """A byte-range lock held for the lifetime of this object."""

    def __init__(self, path: Path, timeout: float = 0.0):
        if not isinstance(timeout, (int, float)) or isinstance(timeout, bool) or not 0 <= timeout <= 3600:
            raise ValueError("Lock timeout must be a finite number from 0 through 3600 seconds")
        self.path = path
        self.timeout = timeout
        self._file: BinaryIO | None = None

    def acquire(self) -> "FileLock":
        if os.name != "nt":
            raise ProtectedStorageError("Windows process locks are required for recovery storage")
        import msvcrt

        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self.path.exists():
            try:
                with self.path.open("xb") as created:
                    created.write(b"\0")
                    created.flush()
            except FileExistsError:
                pass
        _secure_acl(self.path, directory=False)
        deadline = time.monotonic() + self.timeout
        while True:
            try:
                self._file = self.path.open("r+b")
                self._file.seek(0)
                msvcrt.locking(self._file.fileno(), msvcrt.LK_NBLCK, 1)
                if self.path.stat().st_size == 0:
                    self._file.write(b"\0")
                    self._file.flush()
                return self
            except OSError as exc:
                if self._file is not None:
                    self._file.close()
                    self._file = None
                if time.monotonic() >= deadline:
                    raise ProtectedStorageError("Recovery storage is locked by another process") from exc
                time.sleep(min(0.05, max(0.001, deadline - time.monotonic())))

    def release(self) -> None:
        if self._file is None:
            return
        import msvcrt

        try:
            self._file.seek(0)
            msvcrt.locking(self._file.fileno(), msvcrt.LK_UNLCK, 1)
        finally:
            self._file.close()
            self._file = None

    def __enter__(self) -> "FileLock":
        return self.acquire()

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.release()


class ProtectedStore:
    """Atomic, DPAPI-protected named byte records in a current-user directory."""

    _MAGIC = b"DEPOSIT-DPAPI-1\0"

    def __init__(self, root: str | Path):
        if os.name != "nt":
            raise ProtectedStorageError("Protected recovery storage is supported only on Windows")
        self.root = Path(root).expanduser().resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        _secure_acl(self.root, directory=True)

    def _path(self, name: str) -> Path:
        if not name or Path(name).name != name or name in {".", ".."}:
            raise ValueError("Record name must be a single path component")
        return self.root / f"{name}.dpapi"

    def write(self, name: str, plaintext: bytes) -> Path:
        if not isinstance(plaintext, bytes):
            raise TypeError("ProtectedStore writes bytes")
        path = self._path(name)
        payload = self._MAGIC + _crypt(plaintext, protect=True)
        temporary = self.root / f".{secrets.token_hex(16)}.tmp"
        try:
            with temporary.open("xb") as output:
                output.write(payload)
                output.flush()
                os.fsync(output.fileno())
            _secure_acl(temporary, directory=False)
            os.replace(temporary, path)
        except Exception:
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                pass
            raise
        return path

    def read(self, name: str) -> bytes:
        payload = self._path(name).read_bytes()
        if not payload.startswith(self._MAGIC):
            raise ProtectedStorageError("Recovery record format is invalid")
        return _crypt(payload[len(self._MAGIC):], protect=False)

    def lock(self, name: str = "journal", *, timeout: float = 0.0) -> FileLock:
        if not isinstance(name, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}", name):
            raise ValueError("Lock name must be a safe 1-64 character identifier")
        return FileLock(self.root / f"{name}.lock", timeout)
