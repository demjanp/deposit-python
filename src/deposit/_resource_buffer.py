"""Protected, delete-on-close storage for a single borrowed resource stream."""

from contextlib import contextmanager
import ctypes
from ctypes import wintypes
from functools import lru_cache
from hashlib import sha256
import os
from pathlib import Path
import shutil
import struct
import tempfile
from uuid import uuid4

from ._exchange import CHUNK_BYTES, _stream_blocks
from .errors import DepositError


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
        raise OSError("Resource stream buffering requires Windows DPAPI")
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


def _crypt(data: bytes, entropy: bytes, *, protect: bool) -> bytes:
    if os.name != "nt":
        raise OSError("Resource stream buffering requires Windows DPAPI")
    source, source_memory = _blob(data)
    secret, secret_memory = _blob(entropy)
    target = _DataBlob()
    try:
        crypt32, kernel32 = _dpapi_apis()
    except Exception as exc:
        raise OSError("Windows DPAPI is unavailable") from exc
    failure: Exception | None = None
    result: bytes | None = None
    try:
        if protect:
            ok = crypt32.CryptProtectData(
                ctypes.byref(source), "Deposit resource buffer", ctypes.byref(secret), None, None, 1, ctypes.byref(target)
            )
        else:
            ok = crypt32.CryptUnprotectData(
                ctypes.byref(source), None, ctypes.byref(secret), None, None, 1, ctypes.byref(target)
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
        del source_memory, secret_memory
    if failure is not None:
        raise OSError("Windows DPAPI operation failed") from failure
    if result is None:
        raise OSError("Windows DPAPI returned no protected data")
    return result


class _SecurityAttributes(ctypes.Structure):
    _fields_ = [("length", wintypes.DWORD), ("descriptor", wintypes.LPVOID),
                ("inherit", wintypes.BOOL)]



@contextmanager
def _private_tempfile(directory):
    """Create an exclusive Windows handle with a current-user-only ACL.

    The kernel deletes this file when the owning handle closes, including
    on process termination. No name is opened again.
    """
    import msvcrt

    pointer = ctypes.c_void_p
    advapi = ctypes.WinDLL("advapi32", use_last_error=True)
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    for library, name, arguments, result in (
        (advapi, "OpenProcessToken", (wintypes.HANDLE, wintypes.DWORD, ctypes.POINTER(pointer)), wintypes.BOOL),
        (advapi, "GetTokenInformation", (wintypes.HANDLE, ctypes.c_int, pointer, wintypes.DWORD,
                                         ctypes.POINTER(wintypes.DWORD)), wintypes.BOOL),
        (advapi, "ConvertSidToStringSidW", (pointer, ctypes.POINTER(wintypes.LPWSTR)), wintypes.BOOL),
        (advapi, "ConvertStringSecurityDescriptorToSecurityDescriptorW",
         (wintypes.LPCWSTR, wintypes.DWORD, ctypes.POINTER(pointer), pointer), wintypes.BOOL),
        (kernel, "GetCurrentProcess", (), wintypes.HANDLE),
        (kernel, "CloseHandle", (wintypes.HANDLE,), wintypes.BOOL),
        (kernel, "LocalFree", (pointer,), pointer),
        (kernel, "CreateFileW", (wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD,
                                ctypes.POINTER(_SecurityAttributes), wintypes.DWORD,
                                wintypes.DWORD, wintypes.HANDLE), wintypes.HANDLE),
    ):
        function = getattr(library, name)
        function.argtypes, function.restype = arguments, result

    def check(success):
        if not success:
            raise ctypes.WinError(ctypes.get_last_error())

    token, size, sid = pointer(), wintypes.DWORD(), wintypes.LPWSTR()
    check(advapi.OpenProcessToken(kernel.GetCurrentProcess(), 8, ctypes.byref(token)))
    try:
        advapi.GetTokenInformation(token, 1, None, 0, ctypes.byref(size))
        identity = ctypes.create_string_buffer(size.value)
        check(advapi.GetTokenInformation(token, 1, identity, size.value, ctypes.byref(size)))
        check(advapi.ConvertSidToStringSidW(ctypes.cast(identity, ctypes.POINTER(pointer))[0], ctypes.byref(sid)))
    finally:
        kernel.CloseHandle(token)
    descriptor = pointer()
    try:
        check(advapi.ConvertStringSecurityDescriptorToSecurityDescriptorW(
            f"O:{sid.value}D:P(A;;FA;;;{sid.value})", 1, ctypes.byref(descriptor), None,
        ))
    finally:
        kernel.LocalFree(ctypes.cast(sid, pointer))
    try:
        attributes = _SecurityAttributes(ctypes.sizeof(_SecurityAttributes), descriptor, False)
        path = directory / f".deposit-{uuid4().hex}.tmp"
        # GENERIC_READ | GENERIC_WRITE | DELETE, CREATE_NEW, DELETE_ON_CLOSE | TEMPORARY
        handle = kernel.CreateFileW(str(path), 0xC0010000, 0, ctypes.byref(attributes), 1, 0x04000100, None)
        if handle == pointer(-1).value:
            raise ctypes.WinError(ctypes.get_last_error())
    finally:
        kernel.LocalFree(descriptor)
    try:
        fd = msvcrt.open_osfhandle(handle, os.O_BINARY | os.O_RDWR)
    except BaseException:
        kernel.CloseHandle(handle)
        raise
    with os.fdopen(fd, "w+b") as stream:
        yield stream


@contextmanager
def freeze_resource(source, check_running):
    """Freeze a borrowed stream in bounded, process-owned DPAPI records."""
    if os.name != "nt":
        raise OSError("Resource stream buffering requires Windows DPAPI")
    entropy = os.urandom(32)
    directory = Path(tempfile.gettempdir()).resolve(strict=True)
    digest = sha256()
    length = 0
    with _private_tempfile(directory) as frozen:
        for block in _stream_blocks(source):
            check_running()
            encoded = _crypt(block, entropy, protect=True)
            if shutil.disk_usage(directory).free < 4 + len(encoded):
                raise DepositError("LIMIT_EXCEEDED", "Insufficient space for the temporary resource buffer")
            frozen.write(struct.pack("<I", len(encoded)))
            frozen.write(encoded)
            digest.update(block)
            length += len(block)
            if length >= (1 << 63):
                raise DepositError("LIMIT_EXCEEDED", "Resource size exceeds the supported integer range")
        frozen.seek(0)

        def chunks():
            while header := frozen.read(4):
                if len(header) != 4:
                    raise DepositError("RESOURCE_CORRUPT", "Incomplete temporary resource record")
                (size,) = struct.unpack("<I", header)
                if not 0 < size <= CHUNK_BYTES + 64 * 1024:
                    raise DepositError("RESOURCE_CORRUPT", "Invalid temporary resource record length")
                encoded = frozen.read(size)
                if len(encoded) != size:
                    raise DepositError("RESOURCE_CORRUPT", "Incomplete temporary resource record")
                yield _crypt(encoded, entropy, protect=False)

        yield length, digest.hexdigest(), chunks()
