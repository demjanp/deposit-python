"""Windows DPAPI protected files and exclusive process locks."""

from __future__ import annotations

import ctypes
from contextlib import ExitStack, contextmanager
import hashlib
import json
import os
import re
import secrets
import stat
import threading
import time
from ctypes import wintypes
from pathlib import Path
from typing import BinaryIO
import shutil


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


class _SecurityAttributes(ctypes.Structure):
    _fields_ = [("length", wintypes.DWORD), ("descriptor", wintypes.LPVOID),
                ("inherit", wintypes.BOOL)]


class _FileInformation(ctypes.Structure):
    _fields_ = [("attributes", wintypes.DWORD), ("created", wintypes.FILETIME),
                ("accessed", wintypes.FILETIME), ("written", wintypes.FILETIME),
                ("volume", wintypes.DWORD), ("size_high", wintypes.DWORD),
                ("size_low", wintypes.DWORD), ("links", wintypes.DWORD),
                ("index_high", wintypes.DWORD), ("index_low", wintypes.DWORD)]


class _AclHeader(ctypes.Structure):
    _fields_ = [("revision", wintypes.BYTE), ("reserved", wintypes.BYTE),
                ("size", wintypes.WORD), ("count", wintypes.WORD),
                ("reserved2", wintypes.WORD)]


class _AllowedAce(ctypes.Structure):
    _fields_ = [("kind", wintypes.BYTE), ("flags", wintypes.BYTE),
                ("size", wintypes.WORD), ("mask", wintypes.DWORD),
                ("sid", wintypes.DWORD)]


class _WindowsAcl:
    """Native SID and handle based protection; never modify owner or SACL."""

    def __init__(self):
        self.advapi = ctypes.WinDLL("advapi32", use_last_error=True)
        self.kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        pointer = ctypes.c_void_p
        out_pointer = ctypes.POINTER(pointer)
        signatures = {
            "OpenProcessToken": ([wintypes.HANDLE, wintypes.DWORD, out_pointer], wintypes.BOOL),
            "OpenThreadToken": ([wintypes.HANDLE, wintypes.DWORD, wintypes.BOOL, out_pointer], wintypes.BOOL),
            "GetTokenInformation": ([wintypes.HANDLE, ctypes.c_int, pointer, wintypes.DWORD,
                                     ctypes.POINTER(wintypes.DWORD)], wintypes.BOOL),
            "ConvertSidToStringSidW": ([pointer, ctypes.POINTER(wintypes.LPWSTR)], wintypes.BOOL),
            "ConvertStringSecurityDescriptorToSecurityDescriptorW":
                ([wintypes.LPCWSTR, wintypes.DWORD, out_pointer, pointer], wintypes.BOOL),
            "GetSecurityDescriptorDacl": ([pointer, ctypes.POINTER(wintypes.BOOL), out_pointer,
                                            ctypes.POINTER(wintypes.BOOL)], wintypes.BOOL),
            "GetSecurityDescriptorControl": ([pointer, ctypes.POINTER(wintypes.WORD),
                                               ctypes.POINTER(wintypes.DWORD)], wintypes.BOOL),
            "GetSecurityInfo": ([wintypes.HANDLE, ctypes.c_int, wintypes.DWORD, out_pointer,
                                 pointer, out_pointer, pointer, out_pointer], wintypes.DWORD),
            "SetSecurityInfo": ([wintypes.HANDLE, ctypes.c_int, wintypes.DWORD, pointer,
                                 pointer, pointer, pointer], wintypes.DWORD),
            "EqualSid": ([pointer, pointer], wintypes.BOOL),
            "GetAce": ([pointer, wintypes.DWORD, out_pointer], wintypes.BOOL),
        }
        for name, (arguments, result) in signatures.items():
            function = getattr(self.advapi, name)
            function.argtypes, function.restype = arguments, result
        signatures = {
            "CreateFileW": ([wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD, pointer,
                             wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE], wintypes.HANDLE),
            "CreateDirectoryW": ([wintypes.LPCWSTR, pointer], wintypes.BOOL),
            "GetFileInformationByHandle": ([wintypes.HANDLE, ctypes.POINTER(_FileInformation)], wintypes.BOOL),
            "CloseHandle": ([wintypes.HANDLE], wintypes.BOOL),
            "LocalFree": ([pointer], pointer),
            "GetCurrentProcess": ([], wintypes.HANDLE),
            "GetCurrentThread": ([], wintypes.HANDLE),
        }
        for name, (arguments, result) in signatures.items():
            function = getattr(self.kernel, name)
            function.argtypes, function.restype = arguments, result
        token = pointer()
        if not self.advapi.OpenThreadToken(self.kernel.GetCurrentThread(), 8, True, ctypes.byref(token)):
            if ctypes.get_last_error() != 1008:  # ERROR_NO_TOKEN
                raise ctypes.WinError(ctypes.get_last_error())
            self.check(self.advapi.OpenProcessToken(self.kernel.GetCurrentProcess(), 8, ctypes.byref(token)))
        try:
            size = wintypes.DWORD()
            self.advapi.GetTokenInformation(token, 1, None, 0, ctypes.byref(size))
            self.token_user = ctypes.create_string_buffer(size.value)
            self.check(self.advapi.GetTokenInformation(token, 1, self.token_user, size, ctypes.byref(size)))
            self.sid = ctypes.cast(self.token_user, ctypes.POINTER(pointer))[0]
        finally:
            self.kernel.CloseHandle(token)
        sid_text = wintypes.LPWSTR()
        self.check(self.advapi.ConvertSidToStringSidW(self.sid, ctypes.byref(sid_text)))
        try:
            self.sid_text = sid_text.value
        finally:
            self.kernel.LocalFree(ctypes.cast(sid_text, pointer))

    @staticmethod
    def check(result):
        if not result:
            raise ctypes.WinError(ctypes.get_last_error())
        return result

    @contextmanager
    def descriptor(self, directory):
        descriptor = ctypes.c_void_p()
        flags = "OICI" if directory else ""
        # Explicit owner also avoids an elevated token's default Administrators owner.
        sddl = f"O:{self.sid_text}D:P(A;{flags};FA;;;{self.sid_text})"
        self.check(self.advapi.ConvertStringSecurityDescriptorToSecurityDescriptorW(
            sddl, 1, ctypes.byref(descriptor), None))
        try:
            yield descriptor
        finally:
            self.kernel.LocalFree(descriptor)

    def dacl(self, descriptor):
        present, defaulted = wintypes.BOOL(), wintypes.BOOL()
        acl = ctypes.c_void_p()
        self.check(self.advapi.GetSecurityDescriptorDacl(
            descriptor, ctypes.byref(present), ctypes.byref(acl), ctypes.byref(defaulted)))
        if not present.value or not acl.value:
            raise PermissionError("Recovery DACL must be present and non-null")
        return acl

    @contextmanager
    def security(self, handle):
        owner, acl, descriptor = ctypes.c_void_p(), ctypes.c_void_p(), ctypes.c_void_p()
        error = self.advapi.GetSecurityInfo(handle, 1, 5, ctypes.byref(owner), None,
                                          ctypes.byref(acl), None, ctypes.byref(descriptor))
        if error:
            raise ctypes.WinError(error)
        try:
            yield owner, acl, descriptor
        finally:
            self.kernel.LocalFree(descriptor)

    def validate_target(self, handle, directory):
        info = _FileInformation()
        self.check(self.kernel.GetFileInformationByHandle(handle, ctypes.byref(info)))
        if info.attributes & 0x400 or bool(info.attributes & 0x10) != directory:
            raise PermissionError("Recovery target is a reparse point or has the wrong type")
        if not directory and info.links != 1:
            raise PermissionError("Recovery files must not have additional hard links")
        with self.security(handle) as (owner, _acl, _descriptor):
            if not owner.value or not self.advapi.EqualSid(owner, self.sid):
                raise PermissionError("Recovery target has an unexpected owner")

    def verify(self, handle, directory):
        self.validate_target(handle, directory)
        with self.security(handle) as (_owner, acl, descriptor):
            control, revision = wintypes.WORD(), wintypes.DWORD()
            self.check(self.advapi.GetSecurityDescriptorControl(descriptor, ctypes.byref(control),
                                                               ctypes.byref(revision)))
            if not control.value & 0x1000 or not acl.value:
                raise PermissionError("Recovery DACL is not protected and non-null")
            self.dacl(descriptor)
            if ctypes.cast(acl, ctypes.POINTER(_AclHeader)).contents.count != 1:
                raise PermissionError("Recovery DACL contains unexpected grants")
            ace = ctypes.c_void_p()
            self.check(self.advapi.GetAce(acl, 0, ctypes.byref(ace)))
            allowed = ctypes.cast(ace, ctypes.POINTER(_AllowedAce)).contents
            sid = ace.value + _AllowedAce.sid.offset
            if (allowed.kind != 0 or allowed.flags != (3 if directory else 0)
                    or allowed.mask != 0x1F01FF or not self.advapi.EqualSid(sid, self.sid)):
                raise PermissionError("Recovery DACL is not exactly current-user full control")

    def apply(self, handle, directory):
        self.validate_target(handle, directory)
        with self.descriptor(directory) as descriptor:
            error = self.advapi.SetSecurityInfo(handle, 1, 0x80000004, None, None,
                                               self.dacl(descriptor), None)
            if error:
                raise ctypes.WinError(error)
        self.verify(handle, directory)

    def open(self, path, *, directory, access=0x60080, share=None, create=False, descriptor=None):
        attributes = None
        if descriptor is not None:
            attributes = _SecurityAttributes(ctypes.sizeof(_SecurityAttributes), descriptor, False)
        handle = self.kernel.CreateFileW(str(path), access, (0 if directory else 3) if share is None else share,
                                        ctypes.byref(attributes) if attributes is not None else None,
                                        1 if create else 3, 0x02200000, None)
        if handle == ctypes.c_void_p(-1).value:
            raise ctypes.WinError(ctypes.get_last_error())
        return handle

    @contextmanager
    def parents(self, path, *, create=False):
        # FILE_LIST_DIRECTORY participates in Windows sharing checks; metadata
        # access alone does not prevent rename even without FILE_SHARE_DELETE.
        # Retain this validated chain through use, replacement and temp cleanup.
        handles = []
        try:
            for parent in reversed(path.parents):
                try:
                    handle = self.open(parent, directory=True, access=0x20081, share=3)
                except FileNotFoundError:
                    if not create:
                        raise
                    self.mkdir(parent)
                    handle = self.open(parent, directory=True, access=0x20081, share=3)
                    try:
                        self.verify(handle, True)
                    except Exception:
                        self.kernel.CloseHandle(handle)
                        raise
                handles.append(handle)
                info = _FileInformation()
                self.check(self.kernel.GetFileInformationByHandle(handle, ctypes.byref(info)))
                if info.attributes & 0x400 or not info.attributes & 0x10:
                    raise PermissionError("Recovery path traverses a reparse point")
            yield
        finally:
            for handle in reversed(handles):
                self.kernel.CloseHandle(handle)

    def mkdir(self, path):
        with self.descriptor(True) as descriptor:
            attributes = _SecurityAttributes(ctypes.sizeof(_SecurityAttributes), descriptor, False)
            if not self.kernel.CreateDirectoryW(str(path), ctypes.byref(attributes)):
                if ctypes.get_last_error() != 183:  # concurrent creator; checked on open
                    raise ctypes.WinError(ctypes.get_last_error())


def _acl_apis():
    if os.name != "nt":
        raise ProtectedStorageError("Windows ACLs are required for recovery storage")
    return _WindowsAcl()


def _absolute_path(path: Path) -> Path:
    # Do not resolve(): resolving first would hide a junction or symbolic link.
    return Path(os.path.abspath(path.expanduser()))


def _secure_acl(path: Path, *, directory: bool) -> None:
    try:
        api = _acl_apis()
        path = _absolute_path(path)
        with api.parents(path):
            handle = api.open(path, directory=directory,
                              access=0x02000000 if directory else 0x60080)
            try:
                api.apply(handle, directory)
            finally:
                api.kernel.CloseHandle(handle)
    except Exception as exc:
        raise ProtectedStorageError("Could not apply the current-user-only recovery ACL") from exc


def _private_directory(path: Path) -> None:
    try:
        api = _acl_apis()
        with api.parents(path, create=True):
            api.mkdir(path)
            # Already-correct roots can be used while legitimate child/ancestor
            # handles exist. Only repairs need an exclusive directory handle.
            handle = api.open(path, directory=True, access=0x20080, share=3)
            try:
                try:
                    api.verify(handle, True)
                except PermissionError:
                    pass
                else:
                    return
            finally:
                api.kernel.CloseHandle(handle)
            # SetSecurityInfo explicitly does not propagate to existing children
            # for a MAXIMUM_ALLOWED handle. Share mode alone cannot ensure this.
            handle = api.open(path, directory=True, access=0x02000000)
            try:
                api.apply(handle, True)
            finally:
                api.kernel.CloseHandle(handle)
    except Exception as exc:
        raise ProtectedStorageError("Could not apply the current-user-only recovery ACL") from exc


@contextmanager
def _private_file(path: Path, *, create: bool):
    """Retain the validated ancestor chain through file use and failed cleanup."""
    import msvcrt

    with ExitStack() as stack:
        handle = None
        created = False
        stream = None
        try:
            api = _acl_apis()
            stack.enter_context(api.parents(path))
            with api.descriptor(False) as descriptor:
                handle = api.open(path, directory=False, access=0xC0060080,
                                  create=create, descriptor=descriptor if create else None)
                created = create
            try:
                api.verify(handle, False)
                descriptor_fd = msvcrt.open_osfhandle(handle, os.O_BINARY | os.O_RDWR)
                handle = None  # descriptor now owns the native handle
                stream = stack.enter_context(os.fdopen(descriptor_fd, "w+b" if create else "r+b"))
            finally:
                if handle is not None:
                    api.kernel.CloseHandle(handle)
        except Exception as exc:
            if created:
                try:
                    path.unlink(missing_ok=True)
                except OSError:
                    pass
            if isinstance(exc, FileExistsError):
                raise
            raise ProtectedStorageError("Protected recovery file is unavailable") from exc
        try:
            # Preserve errors from writes/flushes and lock operations after setup.
            yield stream
        except BaseException:
            if created:
                stream.close()
                try:
                    path.unlink(missing_ok=True)
                except OSError:
                    pass
            raise


class FileLock:
    """A byte-range lock held for the lifetime of this object."""

    def __init__(self, path: Path, timeout: float = 0.0):
        if not isinstance(timeout, (int, float)) or isinstance(timeout, bool) or not 0 <= timeout <= 3600:
            raise ValueError("Lock timeout must be a finite number from 0 through 3600 seconds")
        self.path = path
        self.timeout = timeout
        self._file: BinaryIO | None = None
        self._file_context = None

    def acquire(self) -> "FileLock":
        if os.name != "nt":
            raise ProtectedStorageError("Windows process locks are required for recovery storage")
        import msvcrt

        self.path = _absolute_path(self.path)
        _private_directory(self.path.parent)
        if not self.path.exists():
            try:
                with _private_file(self.path, create=True) as created:
                    created.write(b"\0")
                    created.flush()
            except FileExistsError:
                pass
        _secure_acl(self.path, directory=False)
        deadline = time.monotonic() + self.timeout
        while True:
            try:
                self._file_context = _private_file(self.path, create=False)
                self._file = self._file_context.__enter__()
                self._file.seek(0)
                msvcrt.locking(self._file.fileno(), msvcrt.LK_NBLCK, 1)
                if self.path.stat().st_size == 0:
                    self._file.write(b"\0")
                    self._file.flush()
                return self
            except OSError as exc:
                if self._file is not None:
                    self._file_context.__exit__(None, None, None)
                    self._file_context = None
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
            self._file_context.__exit__(None, None, None)
            self._file_context = None
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

    def __init__(self, root: str | Path):
        if os.name != "nt":
            raise ProtectedStorageError("Protected recovery storage is supported only on Windows")
        self.root = _absolute_path(Path(root))
        _private_directory(self.root)
        self._register_root()

    @staticmethod
    def _coordinator_root() -> Path:
        local = os.environ.get("LOCALAPPDATA")
        if not local:
            raise ProtectedStorageError("Windows current-user recovery directory is unavailable")
        root = _absolute_path(Path(local) / "Deposit" / "recovery")
        try:
            _private_directory(root)
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
            _secure_acl(path, directory=False)
            with _private_file(path, create=False) as source:
                payload = source.read()
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
        temporary = path.with_name(f".{secrets.token_hex(16)}.tmp")
        try:
            with _private_file(temporary, create=True) as output:
                _secure_acl(temporary, directory=False)
                output.write(payload)
                output.flush()
                os.fsync(output.fileno())
                output.close()
                os.replace(temporary, path)
        except Exception as exc:
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
        # Serialize writes across registered roots while replacing the root
        # registry. Capacity is determined by the filesystem, not a byte quota.
        with self.user_quota_lock():
            with _private_file(temporary, create=True) as output:
                _secure_acl(temporary, directory=False)
                output.write(payload)
                output.flush()
                os.fsync(output.fileno())
                output.close()
                os.replace(temporary, path)
        return path

    def read(self, name: str) -> bytes:
        path = self._path(name)
        try:
            size = path.stat().st_size
        except OSError as exc:
            raise ProtectedStorageError("Recovery record is unavailable") from exc
        if size > self.MAX_PROTECTED_BYTES:
            raise ProtectedStorageError("Recovery record exceeds the 16 MiB limit")
        _secure_acl(path, directory=False)
        with _private_file(path, create=False) as source:
            payload = source.read()
        if not payload.startswith(self._MAGIC):
            raise ProtectedStorageError("Recovery record format is invalid")
        return _crypt(payload[len(self._MAGIC):], protect=False)

    def record_size(self, name: str) -> int:
        """Return the physical size of one existing protected record."""
        try:
            return self._path(name).stat().st_size
        except OSError as exc:
            raise ProtectedStorageError("Protected record size is unavailable") from exc

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

    def has_record(self, name: str) -> bool:
        """Check one protected record name without enumerating the root."""
        path = self._path(name)
        try:
            details = path.lstat()
        except FileNotFoundError:
            return False
        except OSError as exc:
            raise ProtectedStorageError("Recovery record presence cannot be determined") from exc
        if not stat.S_ISREG(details.st_mode):
            raise ProtectedStorageError("Unexpected link in recovery storage")
        return True

    def delete(self, name: str) -> None:
        """Delete one protected record after its retention policy permits cleanup."""
        path = self._path(name)
        with self.user_quota_lock():
            try:
                path.unlink(missing_ok=True)
            except OSError as exc:
                raise ProtectedStorageError("Recovery record cleanup failed") from exc

    def usage_bytes(self) -> int:
        """Return measured physical bytes across all registered roots."""
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


class StorageAdmissionCoordinator:
    """Durable disk-space commitments shared by protected recovery roots.

    Reservations are protected records written before callers create artifacts.
    Materialized bytes are removed from future commitments because the
    filesystem free-space reading already accounts for them.
    """

    def __init__(self, store: ProtectedStore):
        self.store = store

    @staticmethod
    def _volume(path: str | Path) -> tuple[str, Path, str | None, str | None]:
        target = _absolute_path(Path(path))
        probe = target if target.exists() else target.parent
        while not probe.exists() and probe != probe.parent:
            probe = probe.parent
        if not probe.exists():
            raise ProtectedStorageError("Storage volume cannot be identified")
        try:
            if os.name == "nt":
                kernel = ctypes.WinDLL("kernel32", use_last_error=True)
                create_file = kernel.CreateFileW
                create_file.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD,
                                        ctypes.c_void_p, wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE]
                create_file.restype = wintypes.HANDLE
                handle = create_file(str(probe), 0x80, 0x7, None, 3, 0x02000000, None)
                if handle == ctypes.c_void_p(-1).value:
                    raise ctypes.WinError(ctypes.get_last_error())
                try:
                    resolve = kernel.GetFinalPathNameByHandleW
                    resolve.argtypes = [wintypes.HANDLE, wintypes.LPWSTR, wintypes.DWORD, wintypes.DWORD]
                    resolve.restype = wintypes.DWORD

                    def final_name(kind):
                        needed = resolve(handle, None, 0, kind)
                        if not needed:
                            raise ctypes.WinError(ctypes.get_last_error())
                        target = ctypes.create_unicode_buffer(needed + 1)
                        written = resolve(handle, target, len(target), kind)
                        if not written or written >= len(target):
                            raise ctypes.WinError(ctypes.get_last_error())
                        return target.value

                    guid_path = final_name(1)  # VOLUME_NAME_GUID
                    nt_path = final_name(2)  # VOLUME_NAME_NT
                    guid_match = re.match(r"^(\\\\\?\\Volume\{[0-9A-Fa-f-]+\}\\)", guid_path)
                    nt_match = re.match(r"^(\\Device\\[^\\]+)", nt_path, re.IGNORECASE)
                    if guid_match is None or nt_match is None:
                        raise OSError("Windows did not return stable volume names")
                    guid_root = guid_match.group(1).casefold()
                    nt_root = nt_match.group(1).casefold()
                    return f"nt:{nt_root}", probe, guid_root, nt_root
                finally:
                    kernel.CloseHandle.argtypes = [wintypes.HANDLE]
                    kernel.CloseHandle.restype = wintypes.BOOL
                    kernel.CloseHandle(handle)
            # This branch supports isolated filesystem tests. Production
            # ProtectedStore refuses non-Windows hosts.
            identity = f"device:{probe.stat().st_dev}"
            return identity, probe, None, identity
        except OSError as exc:
            raise ProtectedStorageError("Storage volume cannot be identified") from exc

    def acquire(self, owner_id: str, purpose: str,
                commitments_by_volume: dict[str, int]) -> dict:
        if not isinstance(owner_id, str) or not owner_id or not isinstance(purpose, str) or not purpose:
            raise ValueError("Admission owner and purpose are required")
        volumes, locations = self._normalize(commitments_by_volume)
        reservation_id = secrets.token_hex(16)
        record = {"version": 1, "id": reservation_id, "owner_id": owner_id,
                  "purpose": purpose, "writer_pid": os.getpid(), "commitments": volumes,
                  "locations": locations, "materialized": {}, "ownership_resolved": False}
        with self.store.user_quota_lock():
            self._check_capacity(volumes, locations)
            self._save(record, creating=True)
        return record

    def grow(self, reservation: dict, additional_commitments_by_volume: dict[str, int]) -> dict:
        with self.store.user_quota_lock():
            record = self._load(reservation)
            additional, locations = self._normalize(additional_commitments_by_volume)
            proposed = dict(record["commitments"])
            for volume, amount in additional.items():
                proposed[volume] = proposed.get(volume, 0) + amount
                record["locations"].setdefault(volume, locations[volume])
            self._check_capacity(proposed, record["locations"], excluding=record["id"])
            record["commitments"] = proposed
            self._save(record)
        reservation.update(record)
        return record

    def materialized(self, reservation: dict, volume: str | Path, actual_bytes: int) -> dict:
        if type(actual_bytes) is not int or actual_bytes < 0:
            raise ValueError("Materialized byte count must be a nonnegative integer")
        volume_id, _, _, _ = self._volume(volume)
        with self.store.user_quota_lock():
            record = self._load(reservation)
            committed = record["commitments"].get(volume_id, 0)
            if actual_bytes > committed:
                raise ProtectedStorageError("Materialized storage exceeded its admitted commitment")
            record["commitments"][volume_id] = committed - actual_bytes
            record["materialized"][volume_id] = record["materialized"].get(volume_id, 0) + actual_bytes
            self._save(record)
        reservation.update(record)
        return record

    def release(self, reservation: dict, unused_commitments: dict[str, int]) -> dict:
        with self.store.user_quota_lock():
            record = self._load(reservation)
            unused, _ = self._normalize(unused_commitments)
            for volume, amount in unused.items():
                if amount > record["commitments"].get(volume, 0):
                    raise ProtectedStorageError("Cannot release more than the unused commitment")
                record["commitments"][volume] -= amount
            self._save(record)
        reservation.update(record)
        return record

    def close(self, reservation: dict) -> None:
        with self.store.user_quota_lock():
            record = self._load(reservation)
            if any(record["commitments"].values()):
                raise ProtectedStorageError("Unused commitments must be reconciled before closing")
            record["ownership_resolved"] = True
            self._save(record)

    def reconcile(self) -> tuple[dict, ...]:
        """Inspect writer ownership and artifacts without releasing uncertainty."""
        observed = []
        with self.store.user_quota_lock():
            for root in self.store.registered_roots():
                owner_store = self.store if root == self.store.root else object.__new__(ProtectedStore)
                owner_store.root = root
                coordinator = self if owner_store is self.store else StorageAdmissionCoordinator(owner_store)
                for name in owner_store.list_records():
                    if not name.startswith("admission-"):
                        continue
                    try:
                        record = json.loads(owner_store.read(name))
                        self._validate_reservation(record)
                    except Exception as exc:
                        raise ProtectedStorageError("Admission ownership cannot be reconciled safely") from exc
                    if record.get("ownership_resolved"):
                        continue
                    lock = coordinator.writer(record)
                    try:
                        lock.acquire()
                    except ProtectedStorageError:
                        observed.append({"id": record["id"], "writer_alive": True,
                                         "ownership_resolved": False, "commitments_retained": True})
                        continue
                    try:
                        artifacts = coordinator._reconcile_owner(record)
                        observed.append({"id": record["id"], "writer_alive": False,
                                         "ownership_resolved": artifacts.get("resolved", False),
                                         "artifacts": artifacts,
                                         "commitments_retained": not artifacts.get("resolved", False)})
                    finally:
                        lock.release()
        return tuple(observed)

    def _normalize(self, values: dict[str, int]) -> tuple[dict[str, int], dict[str, dict[str, str | None]]]:
        if not isinstance(values, dict):
            raise TypeError("Commitments must be a mapping of paths to bytes")
        totals: dict[str, int] = {}
        locations: dict[str, dict[str, str | None]] = {}
        for path, amount in values.items():
            if type(amount) is not int or amount < 0:
                raise ValueError("Commitments must be nonnegative integer byte counts")
            volume, probe, guid, nt = self._volume(path)
            totals[volume] = totals.get(volume, 0) + amount
            locations[volume] = {"path": str(probe), "guid": guid, "nt": nt}
        return totals, locations

    def _check_capacity(self, requested: dict[str, int], locations: dict,
                        *, excluding=None) -> None:
        committed: dict[str, int] = {}
        roots = self.store.registered_roots()
        for root in roots:
            probe_store = self.store if root == self.store.root else object.__new__(ProtectedStore)
            probe_store.root = root
            for name in probe_store.list_records():
                if not name.startswith("admission-") or name == f"admission-{excluding}":
                    continue
                try:
                    record = json.loads(probe_store.read(name))
                    self._validate_reservation(record)
                except Exception as exc:
                    raise ProtectedStorageError("Existing storage commitments cannot be assessed") from exc
                if record.get("ownership_resolved"):
                    continue
                for volume, amount in record.get("commitments", {}).items():
                    committed[volume] = committed.get(volume, 0) + amount
        for volume, amount in requested.items():
            location = locations[volume]["path"]
            try:
                free = shutil.disk_usage(location).free
            except OSError as exc:
                raise ProtectedStorageError("Available storage cannot be assessed") from exc
            if committed.get(volume, 0) + amount > free:
                raise ProtectedStorageError("Insufficient available storage for the requested commitment")

    def _save(self, record: dict, *, creating=False) -> None:
        self.store.write(f"admission-{record['id']}", json.dumps(
            record, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8"))

    def _load(self, reservation: dict) -> dict:
        if not isinstance(reservation, dict) or not isinstance(reservation.get("id"), str):
            raise ValueError("Admission reservation is invalid")
        try:
            record = json.loads(self.store.read(f"admission-{reservation['id']}"))
        except Exception as exc:
            raise ProtectedStorageError("Admission reservation is unavailable") from exc
        self._validate_reservation(record)
        if record["id"] != reservation["id"]:
            raise ProtectedStorageError("Admission reservation is corrupt")
        return record

    @staticmethod
    def _validate_reservation(record: dict) -> None:
        required = {"version", "id", "owner_id", "purpose", "writer_pid", "commitments",
                    "locations", "materialized", "ownership_resolved"}
        if (not isinstance(record, dict) or set(record) != required or record.get("version") != 1
                or not isinstance(record.get("id"), str) or not re.fullmatch(r"[0-9a-f]{32}", record["id"])
                or not isinstance(record.get("owner_id"), str) or not record["owner_id"]
                or not isinstance(record.get("purpose"), str) or not record["purpose"]
                or type(record.get("writer_pid")) is not int or record["writer_pid"] < 1
                or type(record.get("ownership_resolved")) is not bool
                or not isinstance(record.get("commitments"), dict)
                or not isinstance(record.get("materialized"), dict)
                or not isinstance(record.get("locations"), dict)):
            raise ProtectedStorageError("Admission reservation is corrupt")
        for values in (record["commitments"], record["materialized"]):
            if any(not isinstance(key, str) or type(value) is not int or value < 0
                   for key, value in values.items()):
                raise ProtectedStorageError("Admission reservation byte accounting is corrupt")
        if not set(record["commitments"]) <= set(record["locations"]):
            raise ProtectedStorageError("Admission reservation volume locators are incomplete")
        for location in record["locations"].values():
            if (not isinstance(location, dict) or set(location) != {"path", "guid", "nt"}
                    or not isinstance(location.get("path"), str)
                    or (location["guid"] is not None and not isinstance(location["guid"], str))
                    or not isinstance(location.get("nt"), str)):
                raise ProtectedStorageError("Admission reservation volume locator is corrupt")

    def writer(self, reservation: dict):
        """Return a process-lifetime lock retained while an owner writes files."""
        return self.store.lock(f"admission-owner-{reservation['id']}", timeout=0.0)

    def _reconcile_owner(self, record: dict) -> dict:
        if record.get("purpose") == "import-result-segment":
            name = record.get("owner_id")
            if not isinstance(name, str) or Path(name).name != name or not name:
                return {"state": "unknown", "reason": "invalid result segment owner"}
            try:
                if self._has_temporary_files():
                    return {"state": "uncertain", "reason": "protected write temporary files remain"}
                if name in self.store.list_records():
                    self.store.read(name)  # authenticate and validate the completed segment
                    segment_state = "materialized"
                else:
                    segment_state = "not_written"
                for volume in tuple(record["commitments"]):
                    record["commitments"][volume] = 0
                record["ownership_resolved"] = True
                self._save(record)
                return {"state": segment_state, "resolved": True}
            except Exception:
                return {"state": "uncertain", "reason": "result segment artifacts are uncertain"}
        owner = record.get("owner_id")
        if not isinstance(owner, str) or not re.fullmatch(r"[0-9a-f]{32}", owner):
            return {"state": "unknown", "reason": "unrecognized owner"}
        try:
            manifest = json.loads(self.store.read(f"transfer-{owner}-manifest"))
        except Exception:
            return {"state": "unknown", "reason": "owner manifest unavailable or corrupt"}
        try:
            if manifest.get("format_version") != 3 or manifest.get("transfer_id") != owner:
                return {"state": "unknown", "reason": "unsupported owner manifest"}
            count, total = manifest["chunk_count"], manifest["bytes"]
            for index in range(count):
                self._verify_chunk(owner, index, min(1024 * 1024, total - index * 1024 * 1024))
            pending_length = manifest["staging_pending_length"]
            if pending_length:
                index = count
                names = (f"transfer-{owner}-chunk-{index:08d}", f"transfer-{owner}-meta-{index:08d}")
                present = [name in self.store.list_records() for name in names]
                if present[0] != present[1]:
                    return {"state": "uncertain", "reason": "partial pending chunk artifacts"}
                if present[0]:
                    self._verify_chunk(owner, index, pending_length, manifest["staging_pending_sha256"])
                    actual = sum(self.store.record_size(name) for name in names)
                    volume = next(iter(record["commitments"]), None)
                    if volume is not None:
                        outstanding = record["commitments"][volume]
                        if actual > outstanding:
                            return {"state": "uncertain", "reason": "pending bytes exceed commitment"}
                        record["materialized"][volume] = record["materialized"].get(volume, 0) + actual
                        record["commitments"][volume] = outstanding - actual
                # No partial writer remains; unused reservation is now known.
            for volume in tuple(record["commitments"]):
                record["commitments"][volume] = 0
            record["ownership_resolved"] = True
            self._save(record)
            return {"state": manifest.get("state"), "bytes": total, "chunk_count": count,
                    "pending_length": pending_length, "resolved": True}
        except Exception:
            return {"state": "uncertain", "reason": "owned artifacts could not be verified"}

    def _verify_chunk(self, owner: str, index: int, expected_length: int, expected_hash=None) -> None:
        data_name = f"transfer-{owner}-chunk-{index:08d}"
        meta_name = f"transfer-{owner}-meta-{index:08d}"
        data = self.store.read(data_name)
        metadata = json.loads(self.store.read(meta_name))
        checksum = hashlib.sha256(data).hexdigest()
        if (len(data) != expected_length or metadata != {"index": index, "bytes": expected_length,
                "sha256": checksum} or (expected_hash is not None and checksum != expected_hash)):
            raise ProtectedStorageError("Owned transfer artifacts failed reconciliation")

    def _has_temporary_files(self) -> bool:
        try:
            with os.scandir(self.store.root) as entries:
                for entry in entries:
                    if not (entry.name.startswith(".") and entry.name.endswith(".tmp")):
                        continue
                    if not stat.S_ISREG(entry.stat(follow_symlinks=False).st_mode):
                        raise ProtectedStorageError("Unexpected temporary file in recovery storage")
                    return True
            return False
        except ProtectedStorageError:
            raise
        except OSError as exc:
            raise ProtectedStorageError("Protected temporary files cannot be reconciled") from exc
