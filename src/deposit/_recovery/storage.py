"""Windows DPAPI protected files and exclusive process locks."""

from __future__ import annotations

import ctypes
from contextlib import ExitStack, contextmanager
import os
import re
import secrets
import stat
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
    MAX_TOTAL_BYTES = 1024 * 1024 * 1024

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
        if self._usage_bytes_for_roots_locked(roots) + len(payload) > self.MAX_TOTAL_BYTES:
            raise ProtectedStorageError("Per-user recovery storage quota (1 GiB) is exhausted")
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
        # A shared per-user coordinator counts all registered custom roots and
        # serializes the complete write, including its encrypted temporary copy.
        with self.user_quota_lock():
            projected = self._usage_bytes_locked() + len(payload)
            if projected > self.MAX_TOTAL_BYTES:
                raise ProtectedStorageError("Per-user recovery storage quota (1 GiB) is exhausted")
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
