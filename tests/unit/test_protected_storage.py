from __future__ import annotations

import multiprocessing
import os
import hashlib
import ctypes
from pathlib import Path
from types import SimpleNamespace
from ctypes import wintypes

import pytest

from deposit._recovery import storage
from deposit._recovery.storage import ProtectedStorageError, ProtectedStore


class _NativeFunction:
    def __init__(self, callback):
        self.callback = callback
        self.argtypes = None
        self.restype = None

    def __call__(self, *args):
        return self.callback(*args)


def _install_fake_dpapi(monkeypatch, *, protect, unprotect, local_free):
    protect_fn = _NativeFunction(protect)
    unprotect_fn = _NativeFunction(unprotect)
    local_free_fn = _NativeFunction(local_free)
    crypt32 = SimpleNamespace(CryptProtectData=protect_fn, CryptUnprotectData=unprotect_fn)
    kernel32 = SimpleNamespace(LocalFree=local_free_fn)
    loads = []

    def load_library(name, *, use_last_error):
        loads.append((name, use_last_error))
        return crypt32 if name == "crypt32" else kernel32

    monkeypatch.setattr(storage.ctypes, "WinDLL", load_library, raising=False)
    return crypt32, kernel32, loads


def _child_lock_attempt(path: str, timeout: float, output) -> None:
    try:
        with storage.FileLock(Path(path), timeout=timeout):
            output.put("acquired")
    except ProtectedStorageError:
        output.put("locked")


def _anonymous_dpapi_probe(root: str, output) -> None:
    """Probe real DPAPI under a thread anonymous token in an isolated process."""
    store = ProtectedStore(root)
    target = store.write("anonymous-probe", b"owner-only-probe-secret")
    protected = target.read_bytes()
    owner_value = store.read("anonymous-probe")
    if owner_value != b"owner-only-probe-secret":
        output.put(("failed", "owner decrypt before impersonation failed"))
        return
    entered = False
    denied = False
    impersonation_error = None
    revert_error = None
    try:
        import ctypes
        from ctypes import wintypes

        advapi = ctypes.windll.advapi32
        kernel = ctypes.windll.kernel32
        kernel.GetCurrentThread.argtypes = []
        kernel.GetCurrentThread.restype = wintypes.HANDLE
        advapi.ImpersonateAnonymousToken.argtypes = [wintypes.HANDLE]
        advapi.ImpersonateAnonymousToken.restype = wintypes.BOOL
        advapi.RevertToSelf.argtypes = []
        advapi.RevertToSelf.restype = wintypes.BOOL
        kernel.GetLastError.argtypes = []
        kernel.GetLastError.restype = wintypes.DWORD
        if not advapi.ImpersonateAnonymousToken(kernel.GetCurrentThread()):
            impersonation_error = int(kernel.GetLastError())
        else:
            entered = True
            try:
                storage._crypt(protected[len(store._MAGIC):], protect=False)
            except ProtectedStorageError:
                denied = True
    except Exception as exc:
        output.put(("unavailable", f"anonymous impersonation API unavailable: {type(exc).__name__}: {exc}"))
        return
    finally:
        if entered:
            import ctypes

            if not ctypes.windll.advapi32.RevertToSelf():
                revert_error = int(ctypes.windll.kernel32.GetLastError())
    if revert_error is not None:
        output.put(("failed", f"RevertToSelf failed: WinError {revert_error}"))
        return
    if impersonation_error is not None:
        output.put(("unavailable", f"ImpersonateAnonymousToken failed: WinError {impersonation_error}"))
        return
    after = target.read_bytes()
    owner_after = store.read("anonymous-probe")
    output.put(("denied" if denied else "allowed", hashlib.sha256(protected).hexdigest(),
                hashlib.sha256(after).hexdigest(), owner_after.decode("ascii")))


@pytest.mark.skipif(os.name != "nt", reason="DPAPI and Windows ACLs are required")
def test_round_trip_is_dpapi_protected_and_atomic(tmp_path, monkeypatch):
    store = ProtectedStore(tmp_path / "private")
    secret = b"credential-do-not-write-in-plaintext"
    target = store.write("inputs", secret)
    assert store.read("inputs") == secret
    assert secret not in target.read_bytes()
    assert list(target.parent.glob("*.tmp")) == []

    with pytest.raises(ValueError):
        store.write("..\\escape", secret)

    prior = target.read_bytes()
    monkeypatch.setattr(storage.os, "replace", lambda *_: (_ for _ in ()).throw(OSError("disk failure")))
    with pytest.raises(OSError):
        store.write("inputs", b"new-secret")
    assert target.read_bytes() == prior
    assert list(target.parent.glob("*.tmp")) == []


@pytest.mark.skipif(os.name != "nt", reason="DPAPI and Windows ACLs are required")
def test_write_flush_failure_preserves_prior_record(tmp_path, monkeypatch):
    store = ProtectedStore(tmp_path / "private")
    target = store.write("inputs", b"prior-secret")
    prior = target.read_bytes()

    def fail_flush(_fd):
        raise OSError("simulated disk flush failure")

    monkeypatch.setattr(storage.os, "fsync", fail_flush)
    with pytest.raises(OSError, match="simulated disk flush failure"):
        store.write("inputs", b"replacement-secret")

    assert target.read_bytes() == prior
    assert store.read("inputs") == b"prior-secret"
    assert list(target.parent.glob("*.tmp")) == []


@pytest.mark.skipif(os.name != "nt", reason="DPAPI and Windows ACLs are required")
def test_native_dpapi_denial_preserves_existing_ciphertext_and_never_writes_plaintext(tmp_path, monkeypatch):
    store = ProtectedStore(tmp_path / "private")
    secret = b"owner-only-secret"
    target = store.write("record", secret)
    before = target.read_bytes()

    retained = ctypes.create_string_buffer(b"native partial result")
    freed = []

    def denied_with_output(*args):
        output = args[-1]._obj
        output.cbData = len(retained.value)
        output.pbData = ctypes.cast(retained, ctypes.POINTER(ctypes.c_byte))
        return 0

    _install_fake_dpapi(
        monkeypatch,
        protect=denied_with_output,
        unprotect=denied_with_output,
        local_free=lambda pointer: freed.append(ctypes.cast(pointer, ctypes.c_void_p).value) or None,
    )
    with pytest.raises(ProtectedStorageError, match="DPAPI operation failed"):
        store.write("record", b"replacement-secret")
    assert target.read_bytes() == before
    assert secret not in before and b"replacement-secret" not in before
    with pytest.raises(ProtectedStorageError, match="DPAPI operation failed"):
        store.read("record")
    assert target.read_bytes() == before
    assert list(target.parent.glob("*.tmp")) == []
    assert len(freed) == 2  # partial ProtectData and UnprotectData output are both released


def test_dpapi_signatures_use_last_error_and_free_pointer_safely(monkeypatch):
    monkeypatch.setattr(storage.os, "name", "nt")
    encrypted = ctypes.create_string_buffer(b"ciphertext")
    freed = []

    def success(*args):
        output = args[-1]._obj
        output.cbData = len(encrypted.value)
        output.pbData = ctypes.cast(encrypted, ctypes.POINTER(ctypes.c_byte))
        return 1

    crypt32, kernel32, loads = _install_fake_dpapi(
        monkeypatch,
        protect=success,
        unprotect=success,
        local_free=lambda pointer: freed.append(ctypes.cast(pointer, ctypes.c_void_p).value) or None,
    )
    assert storage._crypt(b"plain", protect=True) == b"ciphertext"
    assert loads == [("crypt32", True), ("kernel32", True)]
    assert crypt32.CryptProtectData.argtypes == [
        ctypes.POINTER(storage._DataBlob), wintypes.LPCWSTR, ctypes.POINTER(storage._DataBlob),
        wintypes.LPVOID, wintypes.LPVOID, wintypes.DWORD, ctypes.POINTER(storage._DataBlob),
    ]
    assert crypt32.CryptProtectData.restype is wintypes.BOOL
    assert crypt32.CryptUnprotectData.argtypes == [
        ctypes.POINTER(storage._DataBlob), ctypes.POINTER(wintypes.LPWSTR), ctypes.POINTER(storage._DataBlob),
        wintypes.LPVOID, wintypes.LPVOID, wintypes.DWORD, ctypes.POINTER(storage._DataBlob),
    ]
    assert crypt32.CryptUnprotectData.restype is wintypes.BOOL
    assert kernel32.LocalFree.argtypes == [ctypes.c_void_p]
    assert kernel32.LocalFree.restype is ctypes.c_void_p
    assert freed == [ctypes.cast(encrypted, ctypes.c_void_p).value]

    # A failed free is surfaced instead of silently leaking a native output buffer.
    _, _, _ = _install_fake_dpapi(
        monkeypatch,
        protect=success,
        unprotect=success,
        local_free=lambda pointer: pointer,
    )
    with pytest.raises(ProtectedStorageError, match="DPAPI operation failed"):
        storage._crypt(b"plain", protect=True)


@pytest.mark.skipif(os.name != "nt", reason="Windows ACL backend is required")
def test_acl_backend_failure_preserves_existing_record(tmp_path, monkeypatch):
    store = ProtectedStore(tmp_path / "private")
    target = store.write("record", b"owner-only-secret")
    before = target.read_bytes()

    def unavailable(*_args, **_kwargs):
        raise OSError("simulated icacls backend unavailable")

    monkeypatch.setattr(storage.subprocess, "run", unavailable)
    with pytest.raises(ProtectedStorageError, match="current-user-only"):
        store.write("record", b"replacement-secret")
    assert target.read_bytes() == before
    assert b"owner-only-secret" not in before
    assert list(target.parent.glob("*.tmp")) == []


def test_lock_names_and_timeouts_reject_path_escape():
    for name in ("", "..", "../outside", "a/b", "a\\b", "x" * 65):
        with pytest.raises(ValueError):
            ProtectedStore.lock(object(), name)  # validation precedes any filesystem access
    for timeout in (-1, float("inf"), float("nan"), True, 3601):
        with pytest.raises(ValueError):
            storage.FileLock(Path("unused.lock"), timeout)


@pytest.mark.skipif(os.name != "nt", reason="Windows ACLs and byte-range locks are required")
def test_preexisting_lock_file_acl_is_revalidated_before_locking(tmp_path, monkeypatch):
    lock_path = tmp_path / "journal.lock"
    lock_path.write_bytes(b"\0")
    observed = []

    def reject_stale_acl(path, *, directory):
        observed.append((path, directory))
        raise ProtectedStorageError("Could not apply the current-user-only recovery ACL")

    monkeypatch.setattr(storage, "_secure_acl", reject_stale_acl)
    with pytest.raises(ProtectedStorageError, match="current-user-only"):
        storage.FileLock(lock_path).acquire()
    assert observed == [(lock_path, False)]
    assert lock_path.read_bytes() == b"\0"


def test_dpapi_native_failure_and_acl_failure_are_wrapped_and_fail_closed(tmp_path, monkeypatch):
    monkeypatch.setattr(storage.os, "name", "nt")
    _install_fake_dpapi(
        monkeypatch,
        protect=lambda *_args: (_ for _ in ()).throw(OSError("native backend unavailable")),
        unprotect=lambda *_args: 0,
        local_free=lambda _pointer: None,
    )
    with pytest.raises(ProtectedStorageError, match="DPAPI operation failed"):
        storage._crypt(b"secret", protect=True)

    def deny_acl(*_args, **_kwargs):
        raise OSError("ACL backend unavailable")

    monkeypatch.setattr(storage.subprocess, "run", deny_acl)
    with pytest.raises(ProtectedStorageError, match="current-user-only"):
        storage._secure_acl(tmp_path / "private", directory=True)


@pytest.mark.skipif(os.name != "nt", reason="Real DPAPI anonymous-token test requires Windows")
def test_anonymous_token_is_denied_and_owner_data_remains_decryptable(tmp_path):
    context = multiprocessing.get_context("spawn")
    output = context.Queue()
    child = context.Process(target=_anonymous_dpapi_probe, args=(str(tmp_path / "private"), output))
    child.start()
    result = output.get(timeout=20)
    child.join(timeout=20)
    assert child.exitcode == 0
    assert result[0] in {"denied", "unavailable"}
    if result[0] == "unavailable":
        pytest.skip(f"Anonymous DPAPI denial unavailable: {result[1]}")
    _, before, after, plaintext = result
    assert before == after
    assert plaintext == "owner-only-probe-secret"


@pytest.mark.skipif(os.name != "nt", reason="Windows byte-range locking is required")
def test_cross_process_lock_contention_and_release(tmp_path):
    store = ProtectedStore(tmp_path / "private")
    lock_path = store.root / "journal.lock"
    context = multiprocessing.get_context("spawn")
    output = context.Queue()
    with store.lock(timeout=0.0):
        child = context.Process(target=_child_lock_attempt, args=(str(lock_path), 0.2, output))
        child.start()
        assert output.get(timeout=5) == "locked"
        child.join(timeout=5)
        assert child.exitcode == 0
    child = context.Process(target=_child_lock_attempt, args=(str(lock_path), 2.0, output))
    child.start()
    assert output.get(timeout=5) == "acquired"
    child.join(timeout=5)
    assert child.exitcode == 0
