from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import json
import multiprocessing
import os
from pathlib import Path
import threading
import uuid

import pytest

from deposit._recovery.identity import DeploymentIdentity
from deposit._recovery import journal
from deposit._recovery.journal import (
    RESULT_FORMAT_NAMED_ROWS_V1,
    JournalError,
    ProtectedJournal,
)
from deposit._recovery.storage import ProtectedStorageError, ProtectedStore


class _MemoryStorage:
    def __init__(self):
        self.records = {}
        self.guard = threading.RLock()

    @contextmanager
    def lock(self, name="journal", *, timeout=0):
        with self.guard:
            yield self

    def write(self, name, plaintext):
        if len(plaintext) > self.MAX_RECORD_BYTES:
            raise ProtectedStorageError("too large")
        self.records[name] = bytes(plaintext)

    def read(self, name):
        return self.records[name]

    def list_records(self):
        return tuple(sorted(self.records))

    def delete(self, name):
        self.records.pop(name, None)


_MemoryStorage.MAX_RECORD_BYTES = 16 * 1024 * 1024


def _identity():
    return DeploymentIdentity(1, "db.example:7687", "neo4j", "db-id", "dbms-id", "actor-id", "ns-id")


def _action_lock_owner_exits(root: str, key: str, acquired) -> None:
    storage = object.__new__(ProtectedStore)
    storage.root = Path(root)
    journal_store = ProtectedJournal(store=storage)
    with journal_store.action_lock(key):
        acquired.set()
        os._exit(17)


def test_journal_persists_exact_inputs_and_requires_valid_transitions():
    storage = _MemoryStorage()
    journal_store = ProtectedJournal(store=storage)
    identity = _identity()
    args = ["Find", {"comment": "original", "count": 4}]
    record = journal_store.create(identity, "deposit.create.class", args, 12)

    assert journal_store.load(identity, record["key"]) == record
    assert record["arguments"] == args
    with pytest.raises(JournalError, match="Invalid recovery transition"):
        journal_store.update(identity, record["key"], state="released")

    record = journal_store.update(identity, record["key"], state="prepare_sent")
    record = journal_store.update(identity, record["key"], state="prepared", operation_id=str(uuid.uuid4()))
    record = journal_store.update(identity, record["key"], state="executing")
    record = journal_store.update(identity, record["key"], state="outcome_saved", outcome={"id": "entity"})
    record = journal_store.update(identity, record["key"], state="released")
    assert record["resolved_at"]
    assert journal_store.unresolved(identity) == []


def test_optional_result_format_keeps_legacy_v1_records_and_strictly_validates_advanced_rows():
    storage = _MemoryStorage()
    journal_store = ProtectedJournal(store=storage)
    identity = _identity()
    legacy = journal_store.create(identity, "deposit.create.class", ["Find", {}], 12)
    assert "result_format" not in legacy
    assert journal_store.load(identity, legacy["key"]) == legacy

    advanced = journal_store.create(
        identity,
        "deposit.apply.batch",
        [[]],
        12,
        result_format=RESULT_FORMAT_NAMED_ROWS_V1,
    )
    assert journal_store.load(identity, advanced["key"]) == advanced
    with pytest.raises(JournalError, match="immutable"):
        journal_store.update(identity, advanced["key"], result_format=None)
    advanced = journal_store.update(identity, advanced["key"], state="prepare_sent")
    empty_result = {"columns": ["data", "presence"], "rows": []}
    advanced = journal_store.update(identity, advanced["key"], state="outcome_saved", outcome=empty_result)
    advanced = journal_store.update(identity, advanced["key"], state="released")
    assert journal_store.load(identity, advanced["key"]) == advanced

    name = next(name for name, payload in storage.records.items() if json.loads(payload)["key"] == advanced["key"])
    unsupported = json.loads(storage.records[name])
    unsupported["result_format"] = "future_rows_v2"
    storage.records[name] = json.dumps(unsupported).encode()
    with pytest.raises(JournalError, match="unsupported format"):
        journal_store.load(identity, advanced["key"])

    unsupported.pop("result_format")
    unsupported["unreviewed"] = True
    storage.records[name] = json.dumps(unsupported).encode()
    with pytest.raises(JournalError, match="unsupported format"):
        journal_store.load(identity, advanced["key"])


def test_journal_identity_and_corrupt_versions_fail_closed():
    storage = _MemoryStorage()
    journal_store = ProtectedJournal(store=storage)
    identity = _identity()
    record = journal_store.create(identity, "deposit.create.object", ["Find", {}], 3)
    other = DeploymentIdentity(1, "other.example:7687", "neo4j", "db-id", "dbms-id", "actor-id", "ns-id")
    with pytest.raises(JournalError, match="identity"):
        journal_store.load(other, record["key"])

    name = next(iter(storage.records))
    corrupt = json.loads(storage.records[name])
    corrupt["format_version"] = 99
    storage.records[name] = json.dumps(corrupt).encode()
    with pytest.raises(JournalError, match="unsupported format"):
        journal_store.load(identity, record["key"])


def test_corrupt_journal_json_fails_closed():
    storage = _MemoryStorage()
    journal_store = ProtectedJournal(store=storage)
    identity = _identity()
    record = journal_store.create(identity, "deposit.create.class", ["Find", {}], 3)
    name = next(iter(storage.records))
    storage.records[name] = b"{truncated"
    with pytest.raises(JournalError, match="corrupt"):
        journal_store.load(identity, record["key"])


@pytest.mark.skipif(os.name != "nt", reason="Windows byte-range lock owner-death semantics")
def test_action_lock_is_released_when_process_owner_dies(tmp_path, monkeypatch):
    from deposit._recovery.storage import ProtectedStore

    monkeypatch.setattr(ProtectedStore, "_coordinator_root", staticmethod(lambda: tmp_path / "coordinator"))
    root = tmp_path / "recovery"
    key = str(uuid.uuid4())
    journal_store = ProtectedJournal(root)
    with journal_store.action_lock(key):
        pass  # Create and ACL the lock file before the child opens it.
    context = multiprocessing.get_context("spawn")
    acquired = context.Event()
    owner = context.Process(target=_action_lock_owner_exits, args=(str(root), key, acquired))
    owner.start()
    assert acquired.wait(10)
    owner.join(10)
    assert owner.exitcode == 17
    with journal_store.action_lock(key):
        assert True


def test_only_released_records_expire_after_retention(monkeypatch):
    storage = _MemoryStorage()
    journal_store = ProtectedJournal(store=storage)
    identity = _identity()
    pending = journal_store.create(identity, "deposit.create.class", ["Keep", {}], 1)
    journal_store.update(identity, pending["key"], state="prepare_sent")
    journal_store.update(identity, pending["key"], state="prepared")
    journal_store.update(identity, pending["key"], state="executing")
    journal_store.update(identity, pending["key"], state="conflicted")
    resolved = journal_store.create(identity, "deposit.create.class", ["Done", {}], 1)
    journal_store.update(identity, resolved["key"], state="prepare_sent")
    journal_store.update(identity, resolved["key"], state="outcome_saved", outcome={"id": "ok"})
    released = journal_store.update(identity, resolved["key"], state="released")
    old = datetime.fromisoformat(released["resolved_at"]) - timedelta(days=31)
    storage.records[next(name for name, payload in storage.records.items() if json.loads(payload)["key"] == released["key"])] = json.dumps({
        **released, "resolved_at": old.isoformat()
    }).encode()

    assert journal_store.cleanup_expired(identity) == 1
    assert journal_store.load(identity, pending["key"])["state"] == "conflicted"


def test_unresolved_operation_limit_and_record_limit(monkeypatch):
    storage = _MemoryStorage()
    journal_store = ProtectedJournal(store=storage)
    monkeypatch.setattr(journal, "MAX_UNRESOLVED", 1)
    journal_store.create(_identity(), "deposit.create.class", ["First", {}], 0)
    with pytest.raises(ProtectedStorageError, match="1,024 unresolved"):
        journal_store.create(_identity(), "deposit.create.class", ["Second", {}], 0)

    monkeypatch.setattr(journal, "MAX_RECORD_BYTES", 100)
    with pytest.raises(ProtectedStorageError, match="16 MiB"):
        journal_store.create(_identity(), "deposit.create.class", ["x" * 500, {}], 0)


@pytest.mark.skipif(os.name != "nt", reason="Protected storage requires Windows DPAPI and byte-range locks")
def test_unresolved_limit_is_shared_across_custom_recovery_roots(monkeypatch, tmp_path):
    from deposit._recovery import storage as storage_module

    monkeypatch.setattr(storage_module, "_secure_acl", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(storage_module, "_crypt", lambda data, *, protect: bytes(data))
    monkeypatch.setattr(ProtectedStore, "_coordinator_root", staticmethod(lambda: tmp_path / "coordinator"))
    first = ProtectedJournal(tmp_path / "first")
    second = ProtectedJournal(tmp_path / "second")
    monkeypatch.setattr(journal, "MAX_UNRESOLVED", 1)

    first.create(_identity(), "deposit.create.class", ["First", {}], 0)
    with pytest.raises(ProtectedStorageError, match="1,024 unresolved"):
        second.create(_identity(), "deposit.create.class", ["Second", {}], 0)
    assert len(first.unresolved(_identity())) == 1
    assert second.unresolved(_identity()) == []


@pytest.mark.skipif(os.name != "nt", reason="Protected storage requires Windows DPAPI and byte-range locks")
def test_byte_quota_is_shared_across_custom_roots_and_counts_replacement_peak(monkeypatch, tmp_path):
    from deposit._recovery import storage as storage_module

    monkeypatch.setattr(storage_module, "_secure_acl", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(storage_module, "_crypt", lambda data, *, protect: bytes(data))
    monkeypatch.setattr(ProtectedStore, "_coordinator_root", staticmethod(lambda: tmp_path / "coordinator"))
    first = ProtectedStore(tmp_path / "first")
    second = ProtectedStore(tmp_path / "second")
    baseline = first.usage_bytes()
    first_payload = first._MAGIC + b"12345"
    second_payload = second._MAGIC + b"abcde"
    monkeypatch.setattr(ProtectedStore, "MAX_TOTAL_BYTES", baseline + len(first_payload) + len(second_payload) - 1)

    first.write("first", b"12345")
    with pytest.raises(ProtectedStorageError, match="1 GiB"):
        second.write("second", b"abcde")
    assert first.read("first") == b"12345"
    assert "second" not in second.list_records()

    current = first.usage_bytes()
    replacement_payload = first._MAGIC + b"longer-value"
    monkeypatch.setattr(ProtectedStore, "MAX_TOTAL_BYTES", current + len(replacement_payload) - 1)
    with pytest.raises(ProtectedStorageError, match="1 GiB"):
        first.write("first", b"longer-value")
    assert first.read("first") == b"12345"


@pytest.mark.skipif(os.name != "nt", reason="Protected storage requires Windows DPAPI and byte-range locks")
def test_global_quota_serializes_cross_root_writers_and_fails_closed_on_bad_index(monkeypatch, tmp_path):
    from deposit._recovery import storage as storage_module

    monkeypatch.setattr(storage_module, "_secure_acl", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(storage_module, "_crypt", lambda data, *, protect: bytes(data))
    monkeypatch.setattr(ProtectedStore, "_coordinator_root", staticmethod(lambda: tmp_path / "coordinator"))
    first = ProtectedStore(tmp_path / "first")
    second = ProtectedStore(tmp_path / "second")
    baseline = first.usage_bytes()
    payload_size = len(first._MAGIC) + 8
    monkeypatch.setattr(ProtectedStore, "MAX_TOTAL_BYTES", baseline + payload_size * 2 - 1)
    barrier = threading.Barrier(3)
    results = []

    def write(store, name):
        barrier.wait()
        try:
            store.write(name, b"12345678")
            results.append("written")
        except ProtectedStorageError:
            results.append("quota")

    workers = [threading.Thread(target=write, args=(first, "one")), threading.Thread(target=write, args=(second, "two"))]
    for worker in workers:
        worker.start()
    barrier.wait()
    for worker in workers:
        worker.join(10)
    assert all(not worker.is_alive() for worker in workers)
    assert sorted(results) == ["quota", "written"]

    registry = first._registry_path()
    registry.write_bytes(b"corrupt")
    with pytest.raises(ProtectedStorageError, match="root index"):
        first.write("three", b"data")
    assert "three" not in first.list_records()


@pytest.mark.skipif(os.name != "nt", reason="Protected storage requires Windows DPAPI and byte-range locks")
def test_unreadable_quota_index_fails_closed_without_replacing_existing_record(monkeypatch, tmp_path):
    from deposit._recovery import storage as storage_module

    monkeypatch.setattr(storage_module, "_secure_acl", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(storage_module, "_crypt", lambda data, *, protect: bytes(data))
    monkeypatch.setattr(ProtectedStore, "_coordinator_root", staticmethod(lambda: tmp_path / "coordinator"))
    store = ProtectedStore(tmp_path / "recovery")
    store.write("keep", b"original")
    registry = store._registry_path()
    original_lstat = Path.lstat

    def denied(path):
        if path == registry:
            raise PermissionError("simulated inaccessible quota index")
        return original_lstat(path)

    monkeypatch.setattr(Path, "lstat", denied)
    with pytest.raises(ProtectedStorageError, match="cannot be assessed"):
        store.write("new", b"must-not-write")

    assert store.read("keep") == b"original"
    assert "new" not in store.list_records()


@pytest.mark.skipif(os.name != "nt", reason="Protected storage requires Windows DPAPI and byte-range locks")
def test_missing_registered_root_fails_closed_without_changing_existing_records(monkeypatch, tmp_path):
    from deposit._recovery import storage as storage_module

    monkeypatch.setattr(storage_module, "_secure_acl", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(storage_module, "_crypt", lambda data, *, protect: bytes(data))
    monkeypatch.setattr(ProtectedStore, "_coordinator_root", staticmethod(lambda: tmp_path / "coordinator"))
    first = ProtectedStore(tmp_path / "first")
    second = ProtectedStore(tmp_path / "second")
    first.write("keep", b"original")
    second.root.rename(second.root.with_name("second-unavailable"))

    with pytest.raises(ProtectedStorageError, match="cannot be assessed"):
        first.write("new", b"must-not-write")

    assert first.read("keep") == b"original"
    assert "new" not in first.list_records()


@pytest.mark.skipif(os.name != "nt", reason="Protected storage requires Windows DPAPI and byte-range locks")
def test_unresolved_count_scandir_error_on_registered_root_fails_closed(monkeypatch, tmp_path):
    from deposit._recovery import storage as storage_module

    monkeypatch.setattr(storage_module, "_secure_acl", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(storage_module, "_crypt", lambda data, *, protect: bytes(data))
    monkeypatch.setattr(ProtectedStore, "_coordinator_root", staticmethod(lambda: tmp_path / "coordinator"))
    first = ProtectedJournal(tmp_path / "first")
    second = ProtectedJournal(tmp_path / "second")
    first.create(_identity(), "deposit.create.class", ["Existing", {}], 0)
    real_scandir = os.scandir

    def denied(path):
        if Path(path) == second.storage.root:
            raise PermissionError("simulated inaccessible registered root")
        return real_scandir(path)

    monkeypatch.setattr(storage_module.os, "scandir", denied)
    with pytest.raises(JournalError, match="cannot be counted safely"):
        first.create(_identity(), "deposit.create.class", ["Must not be written", {}], 0)

    assert len(first.records(_identity())) == 1


def _seed_old_released_record(journal_store, storage):
    identity = _identity()
    record = journal_store.create(identity, "deposit.create.class", ["Expired", {}], 1)
    journal_store.update(identity, record["key"], state="prepare_sent")
    journal_store.update(identity, record["key"], state="outcome_saved", outcome={"id": "expired"})
    released = journal_store.update(identity, record["key"], state="released")
    name = next(name for name, payload in storage.records.items() if json.loads(payload)["key"] == record["key"])
    raw = json.loads(storage.records[name])
    raw["resolved_at"] = (datetime.now(timezone.utc) - timedelta(days=31)).isoformat()
    storage.records[name] = json.dumps(raw, separators=(",", ":")).encode("utf-8")
    return identity, released, name


@pytest.mark.parametrize(
    "corruption",
    [
        "unknown_version",
        "missing_outcome",
        "invalid_retention",
        "filename_key_mismatch",
        "identity_filename_mismatch",
        "non_uuid_key",
    ],
)
@pytest.mark.parametrize("access", ["create", "records_all", "cleanup", "accounting"])
def test_expired_released_record_is_validated_before_any_prune_or_count(access, corruption):
    storage = _MemoryStorage()
    journal_store = ProtectedJournal(store=storage)
    identity, released, name = _seed_old_released_record(journal_store, storage)
    raw = json.loads(storage.records[name])
    if corruption == "unknown_version":
        raw["format_version"] = 99
    else:
        if corruption == "missing_outcome":
            raw.pop("outcome")
        elif corruption == "invalid_retention":
            raw["resolved_at"] = "not-a-timezone-aware-timestamp"
        elif corruption == "filename_key_mismatch":
            raw["key"] = str(uuid.uuid4())
        elif corruption == "non_uuid_key":
            raw["key"] = "not-a-uuid"
        else:
            raw["identity"]["namespace_id"] = "different-namespace"
    storage.records[name] = json.dumps(raw, separators=(",", ":")).encode("utf-8")
    original = storage.records[name]

    with pytest.raises(JournalError):
        if access == "create":
            journal_store.create(identity, "deposit.create.class", ["New", {}], 2)
        elif access == "records_all":
            journal_store.records_all()
        elif access == "cleanup":
            journal_store.cleanup_expired(identity)
        else:
            journal_store._unresolved_count_all_roots_locked()

    assert storage.records[name] == original


@pytest.mark.parametrize(
    "malformed_json",
    [
        "duplicate_field",
        "NaN",
        "Infinity",
        "-Infinity",
    ],
)
@pytest.mark.parametrize("access", ["create", "records_all", "cleanup", "accounting"])
def test_nonstandard_or_duplicate_journal_json_fails_closed_and_is_retained(access, malformed_json):
    storage = _MemoryStorage()
    journal_store = ProtectedJournal(store=storage)
    identity, released, name = _seed_old_released_record(journal_store, storage)
    original_text = storage.records[name].decode("utf-8")
    if malformed_json == "duplicate_field":
        assert '"state":"released"' in original_text
        corrupt_text = original_text.replace('"state":"released"', '"state":"released","state":"released"', 1)
    else:
        assert '"outcome":{"id":"expired"}' in original_text
        corrupt_text = original_text.replace('"outcome":{"id":"expired"}', f'"outcome":{malformed_json}', 1)
    storage.records[name] = corrupt_text.encode("utf-8")
    original = storage.records[name]

    with pytest.raises(JournalError):
        if access == "create":
            journal_store.create(identity, "deposit.create.class", ["New", {}], 2)
        elif access == "records_all":
            journal_store.records_all()
        elif access == "cleanup":
            journal_store.cleanup_expired(identity)
        else:
            journal_store._unresolved_count_all_roots_locked()

    assert storage.records[name] == original
