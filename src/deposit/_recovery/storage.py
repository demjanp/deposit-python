"""Windows DPAPI protected files and exclusive process locks."""

from __future__ import annotations

import ctypes
from contextlib import contextmanager
import os
import re
import secrets
import stat
import subprocess
import threading
import time
from ctypes import wintypes
from pathlib import Path
from typing import BinaryIO


class ProtectedStorageError(RuntimeError):
    """Protected persistence is unavailable or a record cannot be read safely."""


_quota_lock_state = threading.local()
_quota_thread_lock = threading.Lock()


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
    MAX_RECORD_BYTES = 16 * 1024 * 1024
    MAX_PROTECTED_BYTES = MAX_RECORD_BYTES + 64 * 1024
    MAX_TOTAL_BYTES = 1024 * 1024 * 1024

    def __init__(self, root: str | Path):
        if os.name != "nt":
            raise ProtectedStorageError("Protected recovery storage is supported only on Windows")
        self.root = Path(root).expanduser().resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        _secure_acl(self.root, directory=True)
        self._register_root()

    @staticmethod
    def _coordinator_root() -> Path:
        local = os.environ.get("LOCALAPPDATA")
        if not local:
            raise ProtectedStorageError("Windows current-user recovery directory is unavailable")
        root = (Path(local) / "Deposit" / "recovery").resolve()
        try:
            root.mkdir(parents=True, exist_ok=True)
            _secure_acl(root, directory=True)
        except Exception as exc:
            raise ProtectedStorageError("Per-user recovery quota coordinator is unavailable") from exc
        return root

    @contextmanager
    def user_quota_lock(self):
        """Serialize quota and root-index operations across every recovery root."""
        depth = getattr(_quota_lock_state, "depth", 0)
        if depth:
            _quota_lock_state.depth = depth + 1
            try:
                yield
            finally:
                _quota_lock_state.depth -= 1
            return
        if not _quota_thread_lock.acquire(blocking=False):
            raise ProtectedStorageError("Per-user recovery storage is locked by another process")
        try:
            lock = FileLock(self._coordinator_root() / "user-quota.lock", timeout=0.0)
            with lock:
                _quota_lock_state.depth = 1
                try:
                    yield
                finally:
                    _quota_lock_state.depth = 0
        finally:
            _quota_thread_lock.release()

    def _registry_path(self) -> Path:
        return self._coordinator_root() / "quota-roots.dpapi"

    def _read_roots_locked(self) -> tuple[Path, ...]:
        path = self._registry_path()
        try:
            try:
                metadata = path.lstat()
            except FileNotFoundError:
                return ()
            if not stat.S_ISREG(metadata.st_mode) or path.is_symlink():
                raise ProtectedStorageError("Per-user recovery root index is not a regular file")
            payload = path.read_bytes()
            if not payload.startswith(self._MAGIC):
                raise ProtectedStorageError("Per-user recovery root index is corrupt")
            raw = _crypt(payload[len(self._MAGIC):], protect=False)
            import json
            values = json.loads(raw.decode("utf-8"))
            if not isinstance(values, list) or any(not isinstance(value, str) or not value for value in values):
                raise ValueError("Invalid root list")
            roots = tuple(Path(value).resolve(strict=True) for value in values)
            if len(set(roots)) != len(roots):
                raise ValueError("Duplicate roots")
            return roots
        except ProtectedStorageError:
            raise
        except Exception as exc:
            raise ProtectedStorageError("Per-user recovery root index cannot be assessed") from exc

    def _write_roots_locked(self, roots: tuple[Path, ...]) -> None:
        import json
        path = self._registry_path()
        plaintext = json.dumps(sorted(str(root) for root in roots), separators=(",", ":")).encode("utf-8")
        if len(plaintext) > self.MAX_RECORD_BYTES:
            raise ProtectedStorageError("Per-user recovery root index exceeds the 16 MiB limit")
        payload = self._MAGIC + _crypt(plaintext, protect=True)
        if len(payload) > self.MAX_PROTECTED_BYTES:
            raise ProtectedStorageError("Protected recovery root index exceeds the 16 MiB limit")
        if self._usage_bytes_for_roots_locked(roots) + len(payload) > self.MAX_TOTAL_BYTES:
            raise ProtectedStorageError("Per-user recovery storage quota (1 GiB) is exhausted")
        temporary = path.with_name(f".{secrets.token_hex(16)}.tmp")
        try:
            with temporary.open("xb") as output:
                output.write(payload)
                output.flush()
                os.fsync(output.fileno())
            _secure_acl(temporary, directory=False)
            os.replace(temporary, path)
        except Exception as exc:
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                pass
            raise ProtectedStorageError("Per-user recovery root index cannot be updated") from exc

    def _register_root(self) -> None:
        with self.user_quota_lock():
            roots = self._read_roots_locked()
            coordinator = self._coordinator_root().resolve(strict=True)
            registered = tuple(dict.fromkeys((*roots, coordinator, self.root)))
            if len(registered) != len(roots):
                self._write_roots_locked(registered)

    def registered_roots(self) -> tuple[Path, ...]:
        with self.user_quota_lock():
            roots = self._read_roots_locked()
            if self.root not in roots:
                raise ProtectedStorageError("Current recovery root is missing from the per-user index")
            return roots

    def _path(self, name: str) -> Path:
        if not name or Path(name).name != name or name in {".", ".."}:
            raise ValueError("Record name must be a single path component")
        return self.root / f"{name}.dpapi"

    def write(self, name: str, plaintext: bytes) -> Path:
        if not isinstance(plaintext, bytes):
            raise TypeError("ProtectedStore writes bytes")
        if len(plaintext) > self.MAX_RECORD_BYTES:
            raise ProtectedStorageError("Recovery record exceeds the 16 MiB limit")
        path = self._path(name)
        payload = self._MAGIC + _crypt(plaintext, protect=True)
        temporary = self.root / f".{secrets.token_hex(16)}.tmp"
        if len(payload) > self.MAX_PROTECTED_BYTES:
            raise ProtectedStorageError("Protected recovery record exceeds the 16 MiB limit")
        # A shared per-user coordinator counts all registered custom roots and
        # serializes the complete write, including its encrypted temporary copy.
        with self.user_quota_lock():
            projected = self._usage_bytes_locked() + len(payload)
            if projected > self.MAX_TOTAL_BYTES:
                raise ProtectedStorageError("Per-user recovery storage quota (1 GiB) is exhausted")
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
        path = self._path(name)
        try:
            size = path.stat().st_size
        except OSError as exc:
            raise ProtectedStorageError("Recovery record is unavailable") from exc
        if size > self.MAX_PROTECTED_BYTES:
            raise ProtectedStorageError("Recovery record exceeds the 16 MiB limit")
        payload = path.read_bytes()
        if not payload.startswith(self._MAGIC):
            raise ProtectedStorageError("Recovery record format is invalid")
        return _crypt(payload[len(self._MAGIC):], protect=False)

    def list_records(self) -> tuple[str, ...]:
        """List protected record names without decrypting their contents."""
        names = []
        try:
            with os.scandir(self.root) as entries:
                for entry in entries:
                    if not entry.name.endswith(".dpapi"):
                        continue
                    details = entry.stat(follow_symlinks=False)
                    if not stat.S_ISREG(details.st_mode):
                        raise ProtectedStorageError("Unexpected link in recovery storage")
                    names.append(entry.name[:-len(".dpapi")])
        except ProtectedStorageError:
            raise
        except OSError as exc:
            raise ProtectedStorageError("Recovery journal records cannot be listed safely") from exc
        return tuple(sorted(names))

    def delete(self, name: str) -> None:
        """Delete one protected record after its retention policy permits cleanup."""
        path = self._path(name)
        with self.user_quota_lock():
            try:
                path.unlink(missing_ok=True)
            except OSError as exc:
                raise ProtectedStorageError("Recovery record cleanup failed") from exc

    def usage_bytes(self) -> int:
        """Return per-user physical bytes across all registered roots."""
        with self.user_quota_lock():
            return self._usage_bytes_locked()

    def _usage_bytes_locked(self) -> int:
        roots = self._read_roots_locked()
        if self.root not in roots:
            raise ProtectedStorageError("Current recovery root is missing from the per-user index")
        return self._usage_bytes_for_roots_locked(roots)

    def _usage_bytes_for_roots_locked(self, roots: tuple[Path, ...]) -> int:
        total = 0
        for root in roots:
            try:
                if not root.is_dir() or root.resolve(strict=True) != root:
                    raise OSError("Registered recovery root is unavailable")
                with os.scandir(root) as entries:
                    for entry in entries:
                        name = entry.name
                        if not (name.endswith(".dpapi") or (name.startswith(".") and name.endswith(".tmp"))):
                            continue
                        details = entry.stat(follow_symlinks=False)
                        if not stat.S_ISREG(details.st_mode):
                            raise OSError("Unexpected link in recovery storage")
                        total += details.st_size
            except OSError as exc:
                raise ProtectedStorageError("Recovery storage usage cannot be determined") from exc
        return total

    def lock(self, name: str = "journal", *, timeout: float = 0.0) -> FileLock:
        if not isinstance(name, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}", name):
            raise ValueError("Lock name must be a safe 1-64 character identifier")
        return FileLock(self.root / f"{name}.lock", timeout)
