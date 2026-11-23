"""Windows DPAPI protected files and exclusive process locks."""

from __future__ import annotations

import ctypes
from contextlib import ExitStack, closing, contextmanager
from functools import lru_cache
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


_RECORD_TEMPORARY = re.compile(r"\.([0-9a-f]{64})\.[0-9a-f]{32}\.tmp")


def _record_name_digest(name: str) -> str:
    return hashlib.sha256(name.encode("utf-8")).hexdigest()


class ProtectedStorageError(RuntimeError):
    """Protected persistence is unavailable or a record cannot be read safely."""


class InsufficientStorageError(ProtectedStorageError):
    """The requested commitment exceeds available space after existing commitments."""


_quota_lock_state = threading.local()
_quota_thread_lock = threading.Lock()


class _DataBlob(ctypes.Structure):
    _fields_ = [("cbData", wintypes.DWORD), ("pbData", ctypes.POINTER(ctypes.c_byte))]


def _blob(data: bytes) -> tuple[_DataBlob, ctypes.Array]:
    backing = ctypes.create_string_buffer(data, max(1, len(data)))
    # Casting the array itself installs a ctypes ownership cycle in _objects.
    # The caller explicitly retains backing for the synchronous native call.
    return _DataBlob(len(data), ctypes.cast(ctypes.addressof(backing), ctypes.POINTER(ctypes.c_byte))), backing


@lru_cache(maxsize=1)
def _dpapi_apis():
    """Load DPAPI functions with pointer-sized signatures and thread-local last error."""
    if os.name != "nt":
        raise ProtectedStorageError("Windows DPAPI is required for recovery storage")
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
        output_pointer = ctypes.c_void_p.from_address(
            ctypes.addressof(target) + _DataBlob.pbData.offset).value
        if output_pointer:
            try:
                remaining = kernel32.LocalFree(output_pointer)
                if remaining:
                    free_failure = OSError("Windows LocalFree failed")
            except Exception as exc:
                free_failure = exc
        if free_failure is not None and failure is None:
            failure = free_failure
        # Keep the source owner alive until all synchronous native work ends.
        del source_memory
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


@lru_cache(maxsize=1)
def _acl_native_apis():
    """Load immutable typed Windows API bindings, but never cache a user SID."""
    if os.name != "nt":
        raise ProtectedStorageError("Windows ACLs are required for recovery storage")
    advapi = ctypes.WinDLL("advapi32", use_last_error=True)
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
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
        function = getattr(advapi, name)
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
        function = getattr(kernel, name)
        function.argtypes, function.restype = arguments, result
    return advapi, kernel


class _WindowsAcl:
    """Native SID and handle based protection; never modify owner or SACL."""

    def __init__(self):
        self.advapi, self.kernel = _acl_native_apis()
        pointer = ctypes.c_void_p
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
            self.sid = ctypes.cast(ctypes.addressof(self.token_user), ctypes.POINTER(pointer))[0]
        finally:
            self.kernel.CloseHandle(token)
        sid_text = wintypes.LPWSTR()
        self.check(self.advapi.ConvertSidToStringSidW(self.sid, ctypes.byref(sid_text)))
        try:
            self.sid_text = sid_text.value
        finally:
            sid_pointer = ctypes.c_void_p.from_address(ctypes.addressof(sid_text)).value
            self.kernel.LocalFree(sid_pointer)

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
            self.kernel.LocalFree(descriptor.value)

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
            self.kernel.LocalFree(descriptor.value)

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
            if _AclHeader.from_address(acl.value).count != 1:
                raise PermissionError("Recovery DACL contains unexpected grants")
            ace = ctypes.c_void_p()
            self.check(self.advapi.GetAce(acl, 0, ctypes.byref(ace)))
            allowed = _AllowedAce.from_address(ace.value)
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

    _MAGIC = b"DEPOSIT-DPAPI\0"
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
        # Bind an interrupted rename to its destination without another file write.
        temporary = self.root / f".{_record_name_digest(name)}.{secrets.token_hex(16)}.tmp"
        if len(payload) > self.MAX_PROTECTED_BYTES:
            raise ProtectedStorageError("Protected recovery record exceeds the 16 MiB limit")
        # Serialize writes across registered roots while replacing the root
        # registry. Capacity is determined by the filesystem, not a byte quota.
        with self.user_quota_lock():
            try:
                with _private_file(temporary, create=True) as output:
                    _secure_acl(temporary, directory=False)
                    output.write(payload)
                    output.flush()
                    os.fsync(output.fileno())
                    output.close()
                    os.replace(temporary, path)
            except BaseException:
                # This invocation alone created this exact temporary path.
                # Failed cleanup remains visible to ownership reconciliation.
                try:
                    temporary.unlink(missing_ok=True)
                except OSError as cleanup:
                    raise ProtectedStorageError("Protected write temporary cleanup failed") from cleanup
                raise
        return path

    def read(self, name: str) -> bytes:
        return self._read_protected_path(self._path(name))

    @classmethod
    def _read_protected_path(cls, path: Path) -> bytes:
        """Authenticate a named record or an interrupted protected write."""
        try:
            size = path.stat().st_size
        except OSError as exc:
            raise ProtectedStorageError("Recovery record is unavailable") from exc
        if size > cls.MAX_PROTECTED_BYTES:
            raise ProtectedStorageError("Recovery record exceeds the 16 MiB limit")
        _secure_acl(path, directory=False)
        with _private_file(path, create=False) as source:
            payload = source.read()
        if not payload.startswith(cls._MAGIC):
            raise ProtectedStorageError("Recovery record format is invalid")
        return _crypt(payload[len(cls._MAGIC):], protect=False)

    def record_size(self, name: str) -> int:
        """Return the physical size of one existing protected record."""
        try:
            return self._path(name).stat().st_size
        except OSError as exc:
            raise ProtectedStorageError("Protected record size is unavailable") from exc

    def iter_records(self):
        """Stream validated protected record names without decrypting contents."""
        try:
            with os.scandir(self.root) as entries:
                for entry in entries:
                    if not entry.name.endswith(".dpapi"):
                        continue
                    details = entry.stat(follow_symlinks=False)
                    if not stat.S_ISREG(details.st_mode):
                        raise ProtectedStorageError("Unexpected link in recovery storage")
                    yield entry.name[:-len(".dpapi")]
        except ProtectedStorageError:
            raise
        except OSError as exc:
            raise ProtectedStorageError("Recovery journal records cannot be listed safely") from exc

    def list_records(self) -> tuple[str, ...]:
        """Return an ordered collection when a caller explicitly needs one."""
        return tuple(sorted(self.iter_records()))

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


@lru_cache(maxsize=1)
def _volume_apis():
    if os.name != "nt":
        raise ProtectedStorageError("Windows volume APIs are unavailable")
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    signatures = {
        "CreateFileW": ([wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD, ctypes.c_void_p,
                         wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE], wintypes.HANDLE),
        "GetFinalPathNameByHandleW": ([wintypes.HANDLE, wintypes.LPWSTR, wintypes.DWORD, wintypes.DWORD], wintypes.DWORD),
        "CloseHandle": ([wintypes.HANDLE], wintypes.BOOL),
    }
    for name, (arguments, result) in signatures.items():
        function = getattr(kernel, name)
        function.argtypes, function.restype = arguments, result
    return kernel


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
                kernel = _volume_apis()
                create_file = kernel.CreateFileW
                handle = create_file(str(probe), 0x80, 0x7, None, 3, 0x02000000, None)
                if handle == ctypes.c_void_p(-1).value:
                    raise ctypes.WinError(ctypes.get_last_error())
                try:
                    resolve = kernel.GetFinalPathNameByHandleW

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
                    kernel.CloseHandle(handle)
            # This branch supports isolated filesystem tests. Production
            # ProtectedStore refuses non-Windows hosts.
            identity = f"device:{probe.stat().st_dev}"
            return identity, probe, None, identity
        except OSError as exc:
            raise ProtectedStorageError("Storage volume cannot be identified") from exc

    def acquire(self, owner_id: str, purpose: str,
                commitments_by_volume: dict[str, int], *, operation: dict | None = None) -> dict:
        if not isinstance(owner_id, str) or not owner_id or not isinstance(purpose, str) or not purpose:
            raise ValueError("Admission owner and purpose are required")
        volumes, locations = self._normalize(commitments_by_volume)
        reservation_id = secrets.token_hex(16)
        record = {"id": reservation_id, "owner_id": owner_id,
                  "purpose": purpose, "writer_pid": os.getpid(), "commitments": volumes,
                  "locations": locations, "materialized": {}, "ownership_resolved": False}
        if operation is not None:
            record["operation"] = operation
        self._validate_reservation(record)
        with self.store.user_quota_lock():
            # A stop before the first ledger rename leaves neither a public
            # manifest nor an admission to select. Scan at every new admission
            # so Store startup and initial-journal same-key retries reach the
            # same reconciliation before writing more records.
            for root in self.store.registered_roots():
                owner_store = (self.store if root == self.store.root
                               else object.__new__(ProtectedStore))
                owner_store.root = root
                StorageAdmissionCoordinator(owner_store)._reconcile_uncommitted_ledger_temporaries()
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

    def materialized_release(self, reservation: dict, volume: str | Path, actual_bytes: int,
                             unused_commitments: dict[str, int]) -> dict:
        """Atomically account an admitted write and release its unused estimate.

        A single protected save is correct for this final, coherent pair of
        commit transitions; no recovery observation spans the quota lock.
        """
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
            unused, _ = self._normalize(unused_commitments)
            for volume, amount in unused.items():
                if amount > record["commitments"].get(volume, 0):
                    raise ProtectedStorageError("Cannot release more than the unused commitment")
                record["commitments"][volume] -= amount
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

    def close(self, reservation: dict, *, retire: bool = False) -> None:
        """Resolve a completed reservation under the existing quota lock.

        The default always writes a durable resolved marker because owners
        without their own close evidence, such as exchange staging, must
        survive a restart. ``retire=True`` intentionally reclaims the complete
        record instead of a durable resolved marker: only a caller that already
        retains its own durable close marker may use it, which spares a
        successful journal record a second encrypted/fsync rewrite. Load and
        outstanding-commitment checks always fail closed without deleting.
        """
        with self.store.user_quota_lock():
            record = self._load(reservation)
            if any(record["commitments"].values()):
                raise ProtectedStorageError("Unused commitments must be reconciled before closing")
            record["ownership_resolved"] = True
            if retire:
                self.store.delete(f"admission-{record['id']}")
                return
            self._save(record)

    def reconcile(self) -> tuple[dict, ...]:
        """Inspect writer ownership and artifacts without releasing uncertainty."""
        observed = []
        with self.store.user_quota_lock():
            for root in self.store.registered_roots():
                owner_store = self.store if root == self.store.root else object.__new__(ProtectedStore)
                owner_store.root = root
                coordinator = self if owner_store is self.store else StorageAdmissionCoordinator(owner_store)
                coordinator._reconcile_uncommitted_ledger_temporaries()
                for name in tuple(owner_store.iter_records()):
                    if not name.startswith("admission-"):
                        continue
                    try:
                        record = coordinator._read_reservation(name)
                    except Exception as exc:
                        raise ProtectedStorageError("Admission ownership cannot be reconciled safely") from exc
                    if record.get("ownership_resolved") and record.get("purpose") != "exchange-staging":
                        continue
                    lock = coordinator.writer(record)
                    try:
                        lock.acquire()
                    except ProtectedStorageError:
                        observed.append({"id": record["id"], "writer_alive": True,
                                         "ownership_resolved": False, "commitments_retained": True})
                        continue
                    artifacts = None
                    observation = None
                    try:
                        artifacts = coordinator._reconcile_owner(record)
                        observation = {"id": record["id"], "writer_alive": False,
                                       "ownership_resolved": artifacts.get("resolved", False),
                                       "artifacts": artifacts,
                                       "commitments_retained": not artifacts.get("resolved", False)}
                        observed.append(observation)
                    finally:
                        lock.release()
                        if (record.get("purpose") == "exchange-staging" and artifacts is not None
                                and artifacts.get("resolved")):
                            try:
                                lock.path.unlink(missing_ok=True)
                                owner_store.delete(f"admission-{record['id']}")
                            except Exception:
                                observation.update(ownership_resolved=False, commitments_retained=True,
                                                   artifacts={"state": "cleanup_pending", "resolved": False})
        return tuple(observed)

    def retire(self, reservation: dict, *, storage: ProtectedStore) -> None:
        """Reclaim a fully reconciled record; see ``close(retire=True)``."""
        if storage is not self.store:
            raise ProtectedStorageError("Retirement requires the coordinating protected store")
        self.close(reservation, retire=True)


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
            for name in probe_store.iter_records():
                if not name.startswith("admission-") or name == f"admission-{excluding}":
                    continue
                try:
                    record = StorageAdmissionCoordinator(probe_store)._read_reservation(name)
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
                raise InsufficientStorageError("Insufficient available storage for the requested commitment")

    def _save(self, record: dict, *, creating=False) -> None:
        self.store.write(f"admission-{record['id']}", json.dumps(
            record, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8"))

    def _load(self, reservation: dict) -> dict:
        if not isinstance(reservation, dict) or not isinstance(reservation.get("id"), str):
            raise ValueError("Admission reservation is invalid")
        try:
            return self._read_reservation(f"admission-{reservation['id']}")
        except Exception as exc:
            raise ProtectedStorageError("Admission reservation is unavailable") from exc

    def _read_reservation(self, name: str) -> dict:
        from .journal import _strict_json_loads

        record = _strict_json_loads(self.store.read(name))
        self._validate_reservation(record)
        if name != f"admission-{record['id']}":
            raise ProtectedStorageError("Admission reservation filename does not match its ID")
        return record

    @staticmethod
    def _validate_reservation(record: dict) -> None:
        required = {"id", "owner_id", "purpose", "writer_pid", "commitments",
                    "locations", "materialized", "ownership_resolved"}
        if isinstance(record, dict) and record.get("purpose") == "journal-initial-operation":
            required.add("operation")
        if (not isinstance(record, dict) or set(record) != required
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
        if record["purpose"] == "journal-initial-operation":
            from .journal import _is_uuid, _validate_payload_descriptor

            operation = record["operation"]
            owner = re.fullmatch(r"op-[0-9a-f]{16}-(.+)", record["owner_id"])
            if (owner is None or not _is_uuid(owner[1])
                    or not isinstance(operation, dict) or set(operation) != {"payload", "rootSha256"}
                    or not isinstance(operation["rootSha256"], str)
                    or re.fullmatch(r"[0-9a-f]{64}", operation["rootSha256"]) is None
                    or record["ownership_resolved"] or len(record["commitments"]) != 1
                    or set(record["commitments"]) != set(record["locations"])
                    or not set(record["materialized"]) <= set(record["commitments"])):
                raise ProtectedStorageError("Initial journal operation reservation is corrupt")
            try:
                _validate_payload_descriptor(operation["payload"], "operation")
            except ValueError as exc:
                raise ProtectedStorageError("Initial journal operation reservation is corrupt") from exc

    def _reservations(self, names):
        for name in names:
            if name.startswith("admission-"):
                yield self._read_reservation(name)

    def initial_operations(self, names):
        """Yield authenticated initial operation ownership under the quota lock."""
        for record in self._reservations(names):
            if record["purpose"] == "journal-initial-operation":
                yield record

    def journal_root_admissions(self, owner: str, names):
        """Yield unresolved admissions for this operation root under the quota lock."""
        for record in self._reservations(names):
            if (record["owner_id"] == owner and not record["ownership_resolved"]
                    and record["purpose"] in {"journal-record", "journal-initial-operation"}):
                yield record

    def reconcile_initial_operation(self, record: dict, *, published: bool) -> bool:
        """Settle one authenticated reservation under the caller's index and quota locks."""
        with self.writer(record):
            return self._reconcile_initial_operation(record, expected_published=published).get("resolved", False)

    def reconcile_journal_root(self, record: dict) -> bool:
        """Settle a previous root write under the caller's index and quota locks."""
        with self.writer(record):
            return self._reconcile_owner(record).get("resolved", False)

    def writer(self, reservation: dict):
        """Return a process-lifetime lock retained while an owner writes files."""
        return self.store.lock(f"admission-owner-{reservation['id']}", timeout=0.0)

    def _reconcile_owner(self, record: dict) -> dict:
        if record.get("purpose") == "exchange-staging":
            return ExchangeStagingOwner.reconcile(self, record)
        if record.get("purpose") == "journal-initial-operation":
            return self._reconcile_initial_operation(record)
        if record.get("purpose") in {"journal-record", "journal-payload"}:
            name = record.get("owner_id")
            if not isinstance(name, str) or Path(name).name != name or not name:
                return {"state": "unknown", "reason": "invalid journal record owner"}
            try:
                if self._has_temporary_files():
                    if (record["purpose"] == "journal-record" and name.startswith("op-")
                            and self._discard_interrupted_root_replacement(record)):
                        return {"state": "materialized", "resolved": True}
                    return {"state": "uncertain", "reason": "protected write temporary files remain"}
                if record["purpose"] == "journal-payload":
                    present = self._authenticate_journal_payload(name)
                else:
                    present = self.store.has_record(name)
                    if present:
                        self.store.read(name)
                for volume in tuple(record["commitments"]):
                    record["commitments"][volume] = 0
                record["ownership_resolved"] = True
                self._save(record)
                return {"state": "materialized" if present else "not_written", "resolved": True}
            except Exception:
                return {"state": "uncertain", "reason": "journal record artifacts are uncertain"}
        owner = record.get("owner_id")
        if not isinstance(owner, str) or not re.fullmatch(r"[0-9a-f]{32}", owner):
            return {"state": "unknown", "reason": "unrecognized owner"}
        try:
            from .transfers import decode_transfer_manifest

            manifest = decode_transfer_manifest(self.store.read(f"transfer-{owner}-manifest"), owner)
            count, total = manifest["chunk_count"], manifest["bytes"]
            for index in range(count):
                self._verify_chunk(owner, index, min(1024 * 1024, total - index * 1024 * 1024))
            pending_length = manifest["staging_pending_length"]
            if pending_length:
                index = count
                names = (f"transfer-{owner}-chunk-{index:08d}", f"transfer-{owner}-meta-{index:08d}")
                present = [self.store.has_record(name) for name in names]
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

    def _discard_interrupted_root_replacement(self, record: dict) -> bool:
        from .journal import _PROTECTED_WRITE_ALLOWANCE, ProtectedJournal

        owner = record["owner_id"]
        with closing(self._temporary_files()) as paths:
            temporary = next(paths, None)
            if temporary is None or next(paths, None) is not None:
                return False
        match = _RECORD_TEMPORARY.fullmatch(temporary.name)
        if match is None or match[1] != _record_name_digest(owner) or not self.store.has_record(owner):
            return False

        # An older ledger and another writer's temporary must not be paired
        # solely because they share a destination.
        admission = f"admission-{record['id']}"
        if any(other["id"] != record["id"]
               for other in self.journal_root_admissions(owner, self.store.iter_records())):
            return False

        pending = ProtectedStore._read_protected_path(temporary)
        volume, _, _, _ = self._volume(self.store.root)
        if (record["materialized"] or set(record["locations"]) != {volume}
                or record["commitments"] != {volume: len(pending) + _PROTECTED_WRITE_ALLOWANCE}):
            return False

        journal = ProtectedJournal(store=self.store)
        published = self.store.read(owner)
        current_root = journal._decode_operation_root(published, owner)
        pending_root = journal._decode_operation_root(pending, owner)
        if current_root["payload"] != pending_root["payload"]:
            return False
        # Validate both lifecycle records and the shared, authenticated payload
        # before rolling back the unpublished replacement. The published root
        # remains the durable record of what survived the crash.
        journal._decode_record(published, owner)
        journal._decode_record(pending, owner)
        temporary.unlink()
        self.store.delete(admission)
        return True

    def _reconcile_initial_operation(self, record: dict, *, expected_published: bool | None = None) -> dict:
        """Retire a verified root or remove an authenticated unpublished prefix."""
        from .journal import (
            OPERATION_CHUNK_BYTES, ProtectedJournal, _payload_owner, _segmented_payload_prefix,
            _strict_json_loads,
        )

        owner = record["owner_id"]
        operation = record["operation"]
        descriptor = operation["payload"]
        generation = descriptor["generation"]
        prefix = _segmented_payload_prefix(owner, "operation", generation)
        marker = prefix + "owner"
        admission = f"admission-{record['id']}"
        try:
            published = self.store.has_record(owner)
            if expected_published is not None and published != expected_published:
                raise ProtectedStorageError("Initial journal root presence changed")
            chunks = sorted((name for name in self.store.iter_records()
                             if name.startswith(prefix) and name != marker), key=lambda name: (len(name), name))
            count = descriptor["chunkCount"]
            if len(chunks) > count or any(name != f"{prefix}{index}" for index, name in enumerate(chunks)):
                raise ProtectedStorageError("Initial journal payload has unexpected chunks")
            has_marker = self.store.has_record(marker)
            if has_marker and _strict_json_loads(self.store.read(marker)) != _payload_owner(
                    owner, "operation", generation):
                raise ProtectedStorageError("Initial journal payload ownership is invalid")
            if not has_marker and (chunks or published):
                raise ProtectedStorageError("Initial journal payload ownership is missing")
            # Renames are ordered, so a dead writer can leave one temporary
            # for the next rename, including ledger replacement after publication.
            if published:
                next_name = admission
            elif not has_marker:
                next_name = marker
            elif len(chunks) < count:
                next_name = f"{prefix}{len(chunks)}"
            else:
                next_name = owner
            temporary = self._initial_operation_temporary(next_name, (owner, marker, admission, *chunks))
            if published:
                root = self.store.read(owner)
                journal = ProtectedJournal(store=self.store)
                if (hashlib.sha256(root).hexdigest() != operation["rootSha256"]
                        or journal._decode_operation_root(root, owner)["payload"] != descriptor
                        or len(chunks) != count):
                    raise ProtectedStorageError("Initial journal root does not match its reservation")
                action = journal._decode_record(root, owner)
                if action["procedure"] != "deposit.resource.beginUpload" or action["state"] != "intent":
                    raise ProtectedStorageError("Initial journal root is not an upload intent")
            else:
                checksum = hashlib.sha256()
                for index, name in enumerate(chunks):
                    chunk = self.store.read(name)
                    if len(chunk) != min(OPERATION_CHUNK_BYTES, descriptor["bytes"] - index * OPERATION_CHUNK_BYTES):
                        raise ProtectedStorageError("Initial journal payload chunk length is invalid")
                    checksum.update(chunk)
                if len(chunks) == count and checksum.hexdigest() != descriptor["sha256"]:
                    raise ProtectedStorageError("Initial journal payload checksum is invalid")
            if temporary is not None:
                temporary.unlink()
            if not published:
                # Delete from the end so another interrupted reconciliation
                # still leaves an authenticated, contiguous prefix.
                for name in reversed(chunks):
                    self.store.delete(name)
                if has_marker:
                    self.store.delete(marker)
            self.store.delete(admission)
            return {"state": "materialized" if published else "not_written", "resolved": True}
        except Exception:
            return {"state": "uncertain", "reason": "Initial journal operation artifacts are uncertain"}

    def _authenticate_journal_payload(self, marker: str) -> bool:
        from .journal import _is_uuid, _payload_owner, _segmented_payload_prefix, _strict_json_loads

        match = re.fullmatch(
            r"(?:action-payload-(op-[0-9a-f]{16}-[0-9a-f-]{36})"
            r"|workflow-payload-(import-workflow-[0-9a-f]{32}))-([0-9a-f]{32})-owner",
            marker,
        )
        if match is None:
            raise ProtectedStorageError("Invalid journal payload reservation owner")
        operation, workflow, generation = match.groups()
        if operation is not None and not _is_uuid(operation.split("-", 2)[2]):
            raise ProtectedStorageError("Invalid journal payload reservation owner")
        owner, role = (operation, "operation") if operation is not None else (workflow, "workflow")
        prefix = _segmented_payload_prefix(owner, role, generation)
        present = self.store.has_record(marker)
        if present and _strict_json_loads(self.store.read(marker)) != _payload_owner(owner, role, generation):
            raise ProtectedStorageError("Invalid journal payload ownership")
        chunks = sorted((name for name in self.store.iter_records()
                         if name.startswith(prefix) and name != marker), key=lambda name: (len(name), name))
        if (chunks and not present) or any(name != f"{prefix}{index}" for index, name in enumerate(chunks)):
            raise ProtectedStorageError("Invalid owned journal payload segment")
        for name in chunks:
            self.store.read(name)
        return present

    def _verify_chunk(self, owner: str, index: int, expected_length: int, expected_hash=None) -> None:
        data_name = f"transfer-{owner}-chunk-{index:08d}"
        meta_name = f"transfer-{owner}-meta-{index:08d}"
        data = self.store.read(data_name)
        metadata = json.loads(self.store.read(meta_name))
        checksum = hashlib.sha256(data).hexdigest()
        if (len(data) != expected_length or metadata != {"index": index, "bytes": expected_length,
                "sha256": checksum} or (expected_hash is not None and checksum != expected_hash)):
            raise ProtectedStorageError("Owned transfer artifacts failed reconciliation")

    def _initial_operation_temporary(self, next_name: str, owned_names: tuple[str, ...]) -> Path | None:
        expected = _record_name_digest(next_name)
        owned = {_record_name_digest(name) for name in owned_names}
        temporary = None
        with closing(self._temporary_files()) as paths:
            for path in paths:
                match = _RECORD_TEMPORARY.fullmatch(path.name)
                if match is None:
                    raise ProtectedStorageError("Unattributed protected write temporary remains")
                digest = match[1]
                if digest == expected and temporary is None:
                    temporary = path
                elif digest == expected or digest in owned:
                    raise ProtectedStorageError("Unexpected initial operation temporary remains")
        return temporary

    def _has_temporary_files(self) -> bool:
        with closing(self._temporary_files()) as paths:
            return next(paths, None) is not None

    def _reconcile_uncommitted_ledger_temporaries(self) -> None:
        """Reclaim only an authenticated reservation whose rename was interrupted.

        A first admission write has no destination ledger to identify its
        ownership. The DPAPI-protected record supplies it. Its id, the temp
        filename hash of that exact destination and the admission owner lock
        must all agree. Anonymous/unreadable artifacts stay fail-closed; a
        held writer lock also retains the temporary. This is only provisional
        cleanup because reservation preparation never had side effects yet.
        It does not establish or infer a public action/root and writes no
        completed-admission state.
        """
        from .journal import _strict_json_loads

        for path in tuple(self.store.root.glob(".*.tmp")):
            match = _RECORD_TEMPORARY.fullmatch(path.name)
            if match is None or path.is_symlink() or not path.is_file():
                continue  # The ordinary temporary guard retains unknown entries.
            try:
                # Temporaries use the same on-disk format and ACL checks as
                # published ProtectedStore records.
                record = _strict_json_loads(ProtectedStore._read_protected_path(path).decode("utf-8"))
                self._validate_reservation(record)
                ledger = f"admission-{record['id']}"
                if match[1] != _record_name_digest(ledger):
                    continue
                lock = self.writer(record)
                try:
                    lock.acquire()
                except ProtectedStorageError:
                    continue
                try:
                    if self.store.has_record(ledger):
                        # A created ledger is now authoritative; reconcile it ordinarily.
                        continue
                    path.unlink(missing_ok=True)
                finally:
                    lock.release()
            except (ProtectedStorageError, OSError, ValueError, TypeError, KeyError):
                # Anonymous/corrupt/reservation bodies can conceal arbitrary
                # evidence and remain for the normal guard to retain.
                continue

    def _temporary_files(self):
        try:
            with os.scandir(self.store.root) as entries:
                for entry in entries:
                    if not (entry.name.startswith(".") and entry.name.endswith(".tmp")):
                        continue
                    if not stat.S_ISREG(entry.stat(follow_symlinks=False).st_mode):
                        raise ProtectedStorageError("Unexpected temporary file in recovery storage")
                    yield Path(entry.path)
        except ProtectedStorageError:
            raise
        except OSError as exc:
            raise ProtectedStorageError("Protected temporary files cannot be reconciled") from exc


class ExchangeStagingOwner:
    """Durable admission and exact cleanup ownership for one temporary stream."""

    def __init__(self, store: ProtectedStore, owner_id: str, chunk_size: int):
        self.store = store
        self.coordinator = StorageAdmissionCoordinator(store)
        self.owner_id, self.chunk_size = owner_id, chunk_size
        self.prefix = f"exchange-{owner_id}"
        self.name = f"{self.prefix}-owner"
        self.reservation = None
        self.writer_lock = None
        self.extent = 0
        self.pending = None
        self.cleanup = False
        self.complete = False

    def _ensure(self):
        if self.reservation is None:
            with self.store.user_quota_lock():
                self.reservation = self.coordinator.acquire(
                    self.owner_id, "exchange-staging", {str(self.store.root): 0})
                lock = self.coordinator.writer(self.reservation)
                lock.acquire()
                self.writer_lock = lock
            self._save()

    def _metadata(self):
        return {"owner_id": self.owner_id, "admission_id": self.reservation["id"],
                "prefix": self.prefix, "chunk_size": self.chunk_size,
                "extent": self.extent, "pending_write": self.pending, "cleanup": self.cleanup}

    def _admitted_write(self, name, payload):
        # The old destination already reduces measured free space. Admit the
        # complete new encrypted temporary copy before replacing it.
        estimate = len(payload) + 65536
        root = str(self.store.root)
        self.coordinator.grow(self.reservation, {root: estimate})
        self.store.write(name, payload)
        actual = self.store.record_size(name)
        self.coordinator.materialized(self.reservation, self.store.root, actual)
        self.coordinator.release(self.reservation, {root: estimate - actual})

    def _save(self):
        self._admitted_write(self.name, json.dumps(
            self._metadata(), sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8"))

    def write(self, index: int, payload: bytes):
        if self.cleanup or self.complete:
            raise ProtectedStorageError("Exchange staging owner is closed for writes")
        self._ensure()
        self.pending = {"index": index, "bytes": len(payload),
                        "sha256": hashlib.sha256(payload).hexdigest()}
        self._save()
        self._admitted_write(f"{self.prefix}-{index:016x}", payload)
        self.extent = max(self.extent, index + 1)
        self.pending = None
        self._save()

    def close(self):
        if self.complete:
            return
        if self.reservation is None:
            self.complete = True
            return
        with self.store.user_quota_lock():
            self._close_locked()

    def _close_locked(self):
        extent = max(self.extent, 0 if self.pending is None else self.pending["index"] + 1)
        if not self.store.has_record(f"admission-{self.reservation['id']}"):
            if self.store.has_record(self.name) or any(
                    self.store.has_record(f"{self.prefix}-{index:016x}") for index in range(extent)):
                if self.writer_lock is not None:
                    self.writer_lock.release()
                    self.writer_lock = None
                raise ProtectedStorageError("Exchange admission ownership is missing")
            lock = self.writer_lock or self.coordinator.writer(self.reservation)
            lock.release()
            lock.path.unlink(missing_ok=True)
            self.writer_lock = None
            self.complete = True
            return
        if self.writer_lock is None:
            lock = self.coordinator.writer(self.reservation)
            lock.acquire()
            self.writer_lock = lock
        ready = False
        try:
            self.cleanup = True
            durable = self.coordinator._load(self.reservation)
            # Existing metadata still proves exact ownership if a disk-full
            # condition prevents recording the cleanup transition.
            if not durable["ownership_resolved"]:
                try:
                    self._save()
                except Exception:
                    pass
            failure = None
            for index in range(extent):
                try:
                    self.store.delete(f"{self.prefix}-{index:016x}")
                except Exception as exc:
                    if failure is None:
                        failure = exc
            if failure is not None:
                raise ProtectedStorageError("Exchange staging cleanup is incomplete") from failure
            if self.coordinator._has_temporary_files():
                raise ProtectedStorageError("Exchange staging write ownership remains uncertain")
            record = self.coordinator._load(self.reservation)
            outstanding = sum(record["commitments"].values())
            if outstanding:
                self.coordinator.release(self.reservation, {str(self.store.root): outstanding})
            if not record["ownership_resolved"]:
                self.coordinator.close(self.reservation)
            self.store.delete(self.name)
            ready = True
        finally:
            lock = self.writer_lock
            lock.release()
            self.writer_lock = None
            if ready:
                # The quota lock excludes a reconciler between retiring this
                # unique writer lock and removing its final durable marker.
                lock.path.unlink(missing_ok=True)
                self.store.delete(f"admission-{self.reservation['id']}")
                self.complete = True

    @staticmethod
    def _validate(metadata, record):
        owner = record["owner_id"]
        required = {"owner_id", "admission_id", "prefix", "chunk_size", "extent", "pending_write", "cleanup"}
        if (not isinstance(metadata, dict) or set(metadata) != required
                or not re.fullmatch(r"[0-9a-f]{32}", owner)
                or metadata["owner_id"] != owner or metadata["admission_id"] != record["id"]
                or metadata["prefix"] != f"exchange-{owner}"
                or type(metadata["chunk_size"]) is not int
                or not 1 <= metadata["chunk_size"] <= ProtectedStore.MAX_RECORD_BYTES
                or type(metadata["extent"]) is not int or metadata["extent"] < 0
                or type(metadata["cleanup"]) is not bool):
            raise ProtectedStorageError("Exchange staging owner is corrupt")
        pending = metadata["pending_write"]
        if pending is not None and (not isinstance(pending, dict)
                or set(pending) != {"index", "bytes", "sha256"}
                or type(pending["index"]) is not int or not 0 <= pending["index"] <= metadata["extent"]
                or type(pending["bytes"]) is not int or not 0 <= pending["bytes"] <= metadata["chunk_size"]
                or not isinstance(pending["sha256"], str)
                or not re.fullmatch(r"[0-9a-f]{64}", pending["sha256"])):
            raise ProtectedStorageError("Exchange pending write is corrupt")
        return metadata

    @staticmethod
    def reconcile(coordinator, record):
        owner = record.get("owner_id")
        if not isinstance(owner, str) or not re.fullmatch(r"[0-9a-f]{32}", owner):
            return {"state": "uncertain", "reason": "invalid Exchange owner"}
        store = coordinator.store
        name = f"exchange-{owner}-owner"
        try:
            if coordinator._has_temporary_files():
                return {"state": "uncertain", "reason": "protected write temporary files remain"}
            if not store.has_record(name):
                if record["ownership_resolved"]:
                    return {"state": "reclaimed", "resolved": True}
                # A writer can exit between reserving metadata and creating
                # the owner record. Resolve only proven absence of its exact
                # prefix; unexpected artifacts remain attributable evidence.
                if any(name.startswith(f"exchange-{owner}-") for name in store.iter_records()):
                    return {"state": "uncertain", "reason": "Exchange owner metadata is missing"}
                for volume in record["commitments"]:
                    record["commitments"][volume] = 0
                record["ownership_resolved"] = True
                coordinator._save(record)
                return {"state": "not_created", "resolved": True}
            metadata = ExchangeStagingOwner._validate(json.loads(store.read(name)), record)
            pending = metadata["pending_write"]
            extent = max(metadata["extent"], 0 if pending is None else pending["index"] + 1)
            for index in range(extent):
                child = f"exchange-{owner}-{index:016x}"
                if store.has_record(child):
                    if len(store.read(child)) > metadata["chunk_size"]:
                        raise ProtectedStorageError("Exchange staging block is corrupt")
                    store.delete(child)
            for volume in record["commitments"]:
                record["commitments"][volume] = 0
            record["ownership_resolved"] = True
            coordinator._save(record)
            store.delete(name)
            return {"state": "reclaimed", "resolved": True}
        except Exception:
            return {"state": "uncertain", "reason": "Exchange staging artifacts could not be reclaimed"}
