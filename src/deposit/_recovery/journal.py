"""DPAPI-protected journal for prepared Deposit actions."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import json
import logging
import math
import os
import re
from pathlib import Path
import stat
import uuid
from contextlib import nullcontext
from typing import Any

from ..errors import DepositError
from .._paging import valid_import_result_envelope
from .identity import DeploymentIdentity
from .storage import InsufficientStorageError, ProtectedStorageError, ProtectedStore, StorageAdmissionCoordinator


RESULT_FORMAT_NAMED_ROWS = "named_rows"
RESULT_FORMAT_IMPORT = "import_result"
RESULT_FORMAT_CLONE = "clone_result"
_SERVER_SCOPED_OPERATIONS = frozenset({
    "deposit.import.cloneNamespace", "deposit.import.beginCloneUpload",
    "deposit.import.cloneUploadChunk", "deposit.import.finishCloneUpload",
    "deposit.import.abortCloneUpload",
})
MAX_UNRESOLVED = 1024
MAX_RECORD_BYTES = 16 * 1024 * 1024
OPERATION_CHUNK_BYTES = 1024 * 1024
_PAYLOAD_ADMISSION_BATCH_RECORDS = 4
_PROTECTED_WRITE_ALLOWANCE = 65536
RESULT_RETENTION = timedelta(days=30)
_MAX_SAFE_INTEGER = (1 << 53) - 1
UNRESOLVED_STATES = frozenset({"intent", "prepare_sent", "prepared", "executing",
                               "COMMITTED_RESULT_PENDING", "outcome_saved", "release_sent",
                               "conflicted", "failure_saved"})
PENDING_STATES = UNRESOLVED_STATES | {"failed"}
_TRANSITIONS = {
    "intent": {"prepare_sent", "prepared", "outcome_saved", "COMMITTED_RESULT_PENDING", "conflicted", "failure_saved"},
    "prepare_sent": {"prepared", "executing", "outcome_saved", "COMMITTED_RESULT_PENDING", "conflicted"},
    "prepared": {"executing", "outcome_saved", "COMMITTED_RESULT_PENDING", "conflicted"},
    "executing": {"prepared", "outcome_saved", "COMMITTED_RESULT_PENDING", "conflicted", "failure_saved"},
    "COMMITTED_RESULT_PENDING": {"COMMITTED_RESULT_PENDING", "outcome_saved"},
    "outcome_saved": {"release_sent", "released"},
    "release_sent": {"outcome_saved", "released"},
    "released": {"released"},
    "conflicted": {"conflicted"},
    "failure_saved": {"failure_saved", "failed", "outcome_saved"},
    "failed": {"failed"},
}


class JournalError(RuntimeError):
    """A recovery journal cannot be safely read or updated."""


def default_recovery_directory() -> Path:
    local = os.environ.get("LOCALAPPDATA")
    if not local:
        raise ProtectedStorageError("Windows current-user recovery directory is unavailable")
    return Path(local) / "Deposit" / "recovery"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _endpoint_scope(identity: DeploymentIdentity) -> str:
    payload = json.dumps(identity.to_dict(), sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()[:16]


def _record_name(identity: DeploymentIdentity, key: str) -> str:
    return f"op-{_endpoint_scope(identity)}-{key}"


def _strict_json_loads(data: bytes | str) -> Any:
    """Decode journal JSON without duplicate fields or nonstandard numbers."""
    if isinstance(data, bytes):
        data = data.decode("utf-8")

    def object_from_pairs(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"Duplicate journal field: {key}")
            result[key] = value
        return result

    def reject_constant(value):
        raise ValueError(f"Nonstandard JSON number: {value}")

    def finite_float(value):
        number = float(value)
        if not math.isfinite(number):
            raise ValueError("Non-finite JSON number")
        return number

    return json.loads(
        data,
        object_pairs_hook=object_from_pairs,
        parse_constant=reject_constant,
        parse_float=finite_float,
    )


def _aware_datetime(value: Any) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("Timestamp has no timezone")
    return parsed


class ProtectedJournal:
    """Atomic action records stored through the Stage 1 protected backend."""

    def __init__(self, root: str | Path | None = None, *, store: ProtectedStore | None = None):
        self.storage = store or ProtectedStore(root or default_recovery_directory())

    def create(
        self,
        identity: DeploymentIdentity,
        procedure: str,
        arguments: list[Any],
        snapshot_revision: int,
        *,
        result_format: str | None = None,
        key: str | None = None,
    ) -> dict[str, Any]:
        if result_format is not None and (
            type(result_format) is not str or result_format not in {RESULT_FORMAT_NAMED_ROWS, RESULT_FORMAT_IMPORT, RESULT_FORMAT_CLONE}
        ):
            raise JournalError("Recovery result format is unsupported")
        if (identity.namespace_id is None) != (procedure in _SERVER_SCOPED_OPERATIONS):
            raise JournalError("Recovery action scope does not match its procedure")
        if (procedure == "deposit.import.cloneNamespace") != (result_format == RESULT_FORMAT_CLONE):
            raise JournalError("Clone action result format does not match its procedure")
        key = str(uuid.uuid4()) if key is None else key
        if not _is_uuid(key):
            raise JournalError("Recovery action key is invalid")
        now = _utc_now()
        record = {
            "key": key,
            "public_id": str(uuid.uuid4()),
            "identity": identity.to_dict(),
            "procedure": procedure,
            "arguments": arguments,
            "snapshot_revision": snapshot_revision,
            "state": "intent",
            "created_at": now,
            "updated_at": now,
            "outcome": None,
            "operation_id": None,
            "resolved_at": None,
        }
        if result_format is not None:
            record["result_format"] = result_format
        self._write(record, creating=True)
        return record

    def load(self, identity: DeploymentIdentity, key: str) -> dict[str, Any]:
        # A root and its immutable payload remain one read snapshot while an
        # update or retention cleanup replaces/deletes their generation.
        with self.storage.lock("journal-index"):
            return self._load_locked(identity, key)

    def _load_locked(self, identity: DeploymentIdentity, key: str) -> dict[str, Any]:
        if not _is_uuid(key):
            raise JournalError("Recovery action key is invalid")
        name = _record_name(identity, key)
        try:
            record = self._decode_record(self.storage.read(name), name, identity=identity)
        except KeyError as exc:
            if any(candidate.startswith("op-") and candidate.endswith(f"-{key}") for candidate in self.storage.list_records()):
                raise JournalError("Recovery journal identity does not match this Store") from exc
            raise JournalError("Recovery action record is unavailable") from exc
        except (JournalError, ProtectedStorageError):
            raise
        except (UnicodeError, ValueError, TypeError) as exc:
            raise JournalError("Recovery journal record is corrupt") from exc
        return record

    def load_if_present(self, identity: DeploymentIdentity, key: str) -> dict[str, Any] | None:
        """Load an action only when its exact protected record is present.

        The index lock makes absence classification consistent with journal
        creation and pruning. Storage read failures for present records remain
        errors; only a successful enumeration proving absence returns None.
        """
        if not _is_uuid(key):
            raise JournalError("Recovery action key is invalid")
        name = _record_name(identity, key)
        with self.storage.lock("journal-index"):
            names = self.storage.list_records()
            if name not in names:
                if any(candidate.startswith("op-") and candidate.endswith(f"-{key}") for candidate in names):
                    raise JournalError("Recovery journal identity does not match this Store")
                return None
            return self._load_locked(identity, key)

    def update(self, identity: DeploymentIdentity, key: str, **changes: Any) -> dict[str, Any]:
        before = self.load(identity, key)
        record = self._validated_update(before, changes, identity=identity)
        # Carry this exact transition through serialized publication. _write()
        # rechecks current content/lifecycle after acquiring journal-index.
        return self._write(record, expected=before, changes=changes)

    def _validated_update(self, before: dict[str, Any], changes: dict[str, Any], *,
                          identity: DeploymentIdentity) -> dict[str, Any]:
        record = dict(before)
        if set(changes) - (set(record) | {"failure"}):
            raise JournalError("Recovery journal update contains unsupported fields")
        if "failure" in record and changes.get("failure", record["failure"]) not in (record["failure"], None):
            raise JournalError("Saved failure metadata is immutable")
        if "result_format" in changes:
            raise JournalError("Recovery result format metadata is immutable")
        next_state = changes.get("state", record["state"])
        if next_state != record["state"] and next_state not in _TRANSITIONS[record["state"]]:
            raise JournalError(f"Invalid recovery transition {record['state']} -> {next_state}")
        record.update(changes)
        record["updated_at"] = _utc_now()
        if next_state in {"released", "failed"} and record["resolved_at"] is None:
            record["resolved_at"] = record["updated_at"]
        self._validate(record, identity, record["key"])
        return record

    def _write_protected(self, name: str, payload: bytes) -> None:
        self._write_admitted_records(name, "journal-record", {name: payload})

    def _write_admitted_records(self, owner: str, purpose: str, records: dict[str, bytes], *,
                                optional_batch: bool = False, operation: dict | None = None) -> bool:
        """Admit every protected temporary copy before writing any file in the batch.

        Existing files already reduce measured free space, including the old
        copy during replacement. Failed writes retain their reservation until
        its writer/artifacts can be reconciled without guessing.
        """
        if any(len(payload) > MAX_RECORD_BYTES for payload in records.values()):
            raise ProtectedStorageError("Journal segment exceeds the 16 MiB protected-record limit")
        coordinator = StorageAdmissionCoordinator(self.storage)
        volume = str(self.storage.root)
        estimates = {name: len(payload) + _PROTECTED_WRITE_ALLOWANCE for name, payload in records.items()}
        estimate = sum(estimates.values())
        # No reconciler can observe a new reservation before its writer owns
        # the lifetime lock. Quota locks are reentrant; journal-index is always
        # acquired before the quota lock, never from inside admission code.
        with self.storage.user_quota_lock():
            try:
                if operation is None:
                    reservation = coordinator.acquire(owner, purpose, {volume: estimate})
                else:
                    reservation = coordinator.acquire(owner, purpose, {volume: estimate}, operation=operation)
            except InsufficientStorageError:
                if optional_batch:
                    return False
                raise
            writer = coordinator.writer(reservation)
            writer.acquire()
        try:
            actual = 0
            for name, payload in records.items():
                self.storage.write(name, payload)
                size = self.storage.record_size(name)
                if size > estimates[name]:
                    raise ProtectedStorageError("Journal record exceeded its admitted commitment")
                actual += size
            coordinator.materialized_release(reservation, volume, actual, {volume: estimate - actual})
            coordinator.close(reservation, retire=True)
        finally:
            writer.release()
        return True

    def records(self, identity: DeploymentIdentity) -> list[dict[str, Any]]:
        with self.storage.lock("journal-index"):
            return self._records_locked(identity)

    def _records_locked(self, identity: DeploymentIdentity) -> list[dict[str, Any]]:
        prefix = f"op-{_endpoint_scope(identity)}-"
        result: list[dict[str, Any]] = []
        for name in self.storage.list_records():
            if not name.startswith(prefix):
                continue
            key = name[len(prefix):]
            result.append(self._load_locked(identity, key))
        return sorted(result, key=lambda item: item["created_at"])

    def records_all(self) -> list[dict[str, Any]]:
        with self.storage.lock("journal-index"):
            self._prune_expired_locked(None)
            result: list[dict[str, Any]] = []
            for name in self.storage.list_records():
                if not name.startswith("op-"):
                    continue
                try:
                    record = self._decode_record(self.storage.read(name), name)
                except Exception as exc:
                    raise JournalError("A recovery journal record is corrupt or unsupported") from exc
                result.append(record)
            return sorted(result, key=lambda item: item["created_at"])

    def find_public_id(self, public_id: str) -> dict[str, Any]:
        if not _is_uuid(public_id):
            raise JournalError("Operation reference is invalid")
        matches = [record for record in self.records_all() if record["public_id"] == public_id]
        if len(matches) != 1:
            raise JournalError("Operation reference is unavailable")
        return matches[0]

    def unresolved(self, identity: DeploymentIdentity) -> list[dict[str, Any]]:
        return [record for record in self.records(identity) if record["state"] in UNRESOLVED_STATES]

    def action_lock(self, key: str):
        if not _is_uuid(key):
            raise JournalError("Recovery action key is invalid")
        return self.storage.lock(f"action-{key}", timeout=0.0)

    def cleanup_expired(self, identity: DeploymentIdentity, *, now: datetime | None = None) -> int:
        cutoff = (now or datetime.now(timezone.utc)) - RESULT_RETENTION
        removed = 0
        with self.storage.lock("journal-index"):
            self._prune_clone_workflows_locked(cutoff)
            dependencies = (self._transfer_dependencies_locked() | self._workflow_dependencies_locked()
                           | self._clone_workflow_dependencies_locked(cutoff))
            for record in self._records_locked(identity):
                if record["state"] not in {"released", "failed"}:
                    continue
                try:
                    resolved = datetime.fromisoformat(record["resolved_at"])
                except (TypeError, ValueError) as exc:
                    raise JournalError("Resolved journal record has invalid retention metadata") from exc
                if resolved.tzinfo is None:
                    raise JournalError("Resolved journal record has invalid retention metadata")
                if resolved <= cutoff and _record_name(identity, record["key"]) not in dependencies:
                    self.storage.delete(_record_name(identity, record["key"]))
                    removed += 1
            self._cleanup_operation_payloads_locked()
            self._cleanup_workflow_payloads_locked()
        return removed

    def _write(self, record: dict[str, Any], *, creating: bool = False,
               expected: dict[str, Any] | None = None,
               changes: dict[str, Any] | None = None) -> None:
        identity = DeploymentIdentity.from_dict(record["identity"])
        self._validate(record, identity, record["key"])
        name = _record_name(identity, record["key"])
        # Snapshot immutable content before rebasing a stale lifecycle update.
        # Otherwise the comparison would prove only that current matches itself.
        immutable = {field: value for field, value in expected.items()
                     if field not in {"updated_at", "resolved_at", "state"}} if expected is not None else None
        # The user quota coordinator makes the unresolved-count check global to
        # all registered recovery roots. The per-root index lock still serializes
        # updates/pruning in this journal; storage.write is reentrant under the
        # same per-user coordinator and includes the encrypted temporary copy.
        initial_upload = creating and record["procedure"] == "deposit.resource.beginUpload"
        with self.storage.lock("journal-index"):
            if expected is not None:
                current = self._load_locked(identity, record["key"])
                if current != expected:
                    if any(current[field] != value for field, value in immutable.items()):
                        raise JournalError("Recovery operation immutable content changed before update")
                    # Identical-content successors may advance lifecycle state,
                    # but validate every requested change from the actual state.
                    record = self._validated_update(current, dict(changes or {}), identity=identity)
            if immutable is None:
                immutable = {field: value for field, value in record.items()
                             if field not in {"updated_at", "resolved_at", "state"}}
            lifecycle_fields = {"state", "updated_at", "resolved_at"}
            if (record["procedure"] == "deposit.resource.uploadChunk"
                    and record["operation_id"] == immutable["operation_id"]):
                # A resume that confirms the same ID only advances lifecycle.
                # Keep the exact authenticated argument generation in that case.
                lifecycle_fields.add("operation_id")
            reuse_generation = (not creating and changes is not None and
                                set(changes) <= lifecycle_fields)
            if not initial_upload and not reuse_generation:
                self._prune_expired_locked(identity)
            quota_lock = getattr(self.storage, "user_quota_lock", None)
            with quota_lock() if quota_lock is not None else nullcontext():
                current_unresolved = self._unresolved_count_all_roots_locked()
                current_names = self.storage.list_records()
                exists = name in current_names
                if creating and exists:
                    raise JournalError("Recovery key collision")
                if creating and current_unresolved >= MAX_UNRESOLVED:
                    raise ProtectedStorageError("The 1,024 unresolved recovery operation limit is reached")
                coordinator = StorageAdmissionCoordinator(self.storage)
                admissions = tuple(coordinator.journal_root_admissions(name, current_names))
                pending = [entry for entry in admissions if entry["purpose"] == "journal-initial-operation"]
                if pending and (len(pending) != 1 or not (exists or initial_upload)
                                or not coordinator.reconcile_initial_operation(pending[0], published=exists)):
                    raise ProtectedStorageError("Initial upload admission requires reconciliation")
                if exists:
                    for entry in admissions:
                        if entry["purpose"] == "journal-record" and not coordinator.reconcile_journal_root(entry):
                            raise ProtectedStorageError("Previous journal root admission requires reconciliation")
                pending_records = {} if initial_upload else None
                if reuse_generation:
                    # The current record was authenticated above under this same
                    # journal-index lock. Reuse that verified snapshot instead of
                    # decoding its potentially large immutable payload again.
                    if expected is None:
                        current = self._load_locked(identity, record["key"])
                    if any(current[field] != value for field, value in immutable.items()):
                        raise JournalError("Recovery operation immutable content changed before payload reuse")
                    current_root = self._decode_operation_root(self.storage.read(name), name)
                    descriptor = current_root["payload"]
                else:
                    descriptor = self._write_segmented_payload(name, "operation", record, pending=pending_records)
                root = {"key": record["key"], "identity": record["identity"], "state": record["state"],
                        "updatedAt": record["updated_at"], "resolvedAt": record["resolved_at"],
                        "payload": descriptor}
                payload = _json_bytes(root)
                try:
                    if pending_records is None:
                        self._write_protected(name, payload)
                    else:
                        # Immutable bytes first. The authoritative root becomes
                        # visible only after the entire generation is durable.
                        pending_records[name] = payload
                        operation = {"payload": descriptor, "rootSha256": hashlib.sha256(payload).hexdigest()}
                        self._write_admitted_records(name, "journal-initial-operation", pending_records,
                                                     operation=operation)
                except Exception:
                    # Replacement may have completed before an interrupted
                    # acknowledgement/bookkeeping step; trust exact bytes only.
                    try:
                        published = self.storage.read(name) == payload
                    except Exception:
                        published = False
                    if not published:
                        raise
                if initial_upload:
                    try:
                        self._prune_expired_locked(identity)
                    except Exception as exc:
                        logging.getLogger(__name__).warning("Post-publication journal pruning deferred: %s", exc)
                elif not reuse_generation:
                    self._cleanup_operation_payloads_locked()
            return record

    def _write_segmented_payload(self, owner: str, role: str, value: Any, *,
                                 pending: dict[str, bytes] | None = None) -> dict[str, Any]:
        """Persist immutable content apart from the root's lifecycle state.

        Argument/result state changes still create a new sealed generation.
        Merely advancing prepared/resume/lifecycle state reuses this exact
        immutable record, with root fields carrying their authoritative dates.
        """
        immutable = dict(value) if role == "operation" else value
        if role == "operation":
            for field in ("state", "updated_at", "resolved_at"):
                immutable.pop(field, None)
        generation = uuid.uuid4().hex
        prefix = _segmented_payload_prefix(owner, role, generation)
        marker = prefix + "owner"
        batch = {marker: _json_bytes(_payload_owner(owner, role, generation))}
        checksum = hashlib.sha256()
        length = count = 0
        for count, chunk in enumerate(_operation_json_chunks(value if role != "operation" else immutable), 1):
            batch[f"{prefix}{count - 1}"] = chunk
            checksum.update(chunk)
            length += len(chunk)
            if pending is None and len(batch) == _PAYLOAD_ADMISSION_BATCH_RECORDS:
                self._write_payload_batch(marker, batch)
                batch.clear()
        if pending is not None:
            pending.update(batch)
        elif batch:
            self._write_payload_batch(marker, batch)
        return {"role": role, "generation": generation, "chunkCount": count,
                "bytes": length, "sha256": checksum.hexdigest()}


    def _write_payload_batch(self, marker: str, batch: dict[str, bytes]) -> None:
        if len(batch) > 1 and self._write_admitted_records(marker, "journal-payload", batch, optional_batch=True):
            return
        # Per-file admission can fit when only the combined encryption
        # headroom is too large. Keep the identical write order in that case.
        for name, payload in batch.items():
            self._write_protected(name, payload)

    def _read_segmented_payload(self, owner: str, descriptor: dict[str, Any]):
        _validate_payload_descriptor(descriptor, descriptor.get("role"))
        prefix = _segmented_payload_prefix(owner, descriptor["role"], descriptor["generation"])
        if _strict_json_loads(self.storage.read(prefix + "owner")) != _payload_owner(
                owner, descriptor["role"], descriptor["generation"]):
            raise JournalError("Segmented payload ownership is invalid")
        checksum = hashlib.sha256()
        payload = bytearray()
        for index in range(descriptor["chunkCount"]):
            chunk = self.storage.read(f"{prefix}{index}")
            if len(chunk) != min(OPERATION_CHUNK_BYTES, descriptor["bytes"] - index * OPERATION_CHUNK_BYTES):
                raise JournalError("Segmented payload chunk length is invalid")
            checksum.update(chunk)
            payload.extend(chunk)
        if checksum.hexdigest() != descriptor["sha256"]:
            raise JournalError("Segmented payload checksum is invalid")
        return _strict_json_loads(payload.decode("utf-8"))

    def _cleanup_operation_payloads_locked(self) -> None:
        self._cleanup_segmented_payloads_locked("operation", self._decode_operation_root)

    def _cleanup_workflow_payloads_locked(self) -> None:
        from .._import_workflows import ImportWorkflowStore
        self._cleanup_segmented_payloads_locked("workflow", ImportWorkflowStore.decode_root)

    def _cleanup_segmented_payloads_locked(self, role, decode_root) -> None:
        """Reclaim authenticated unpublished/replaced generations under journal-index."""
        pattern = (r"action-payload-(op-[0-9a-f]{16}-[0-9a-f-]{36})-([0-9a-f]{32})-owner"
                   if role == "operation" else r"workflow-payload-(import-workflow-[0-9a-f]{32})-([0-9a-f]{32})-owner")
        try:
            # Filename discovery already replaces list_records and sorts the whole
            # directory per obsolete owner. Validate candidate names before
            # decrypting them, and enumerate the record names exactly once.
            owners = tuple(filter(re.compile(pattern).fullmatch, self.storage.list_records()))
        except Exception as exc:
            logging.getLogger(__name__).warning("Payload cleanup enumeration deferred: %s", exc)
            return
        for name in owners:
            match = re.fullmatch(pattern, name)
            owner, generation = match.groups()
            try:
                if _strict_json_loads(self.storage.read(name)) != _payload_owner(owner, role, generation):
                    raise ValueError("Invalid payload ownership")
                prefix = _segmented_payload_prefix(owner, role, generation)
                # The current action urn may have many obsolete generations, while
                # its chunks in the active history belong to a different prefix.
                # Classifying genealogy by prefix does not require a full directory
                # scan per owner; only a genuinely stale generation is enumerated.
                if self.storage.has_record(owner):
                    root = decode_root(self.storage.read(owner), owner)
                    if root["payload"]["generation"] == generation:
                        continue
                owned = tuple(candidate for candidate in self.storage.iter_records()
                              if candidate.startswith(prefix) and candidate != name)
                if any(re.fullmatch(r"[0-9]+", candidate[len(prefix):]) is None for candidate in owned):
                    raise ValueError("Invalid owned payload name")
            except Exception as exc:
                logging.getLogger(__name__).warning("Payload cleanup ownership deferred (%s): %s", owner, exc)
                continue
            try:
                quota_lock = self.storage.user_quota_lock if role == "operation" else nullcontext
                with quota_lock():
                    if role == "operation" and any(
                            pending["owner_id"] == owner and pending["operation"]["payload"]["generation"] == generation
                            for pending in StorageAdmissionCoordinator(self.storage).initial_operations(
                                self.storage.iter_records())):
                        continue
                    for candidate in owned:
                        self.storage.delete(candidate)
                    self.storage.delete(name)
            except Exception as exc:
                # The authoritative root remains published. Retain the owner
                # until every obsolete segment is removed; cleanup is retryable.
                logging.getLogger(__name__).warning("Payload cleanup deferred (%s): %s", owner, exc)

    def _unresolved_count_all_roots_locked(self) -> int:
        count = 0
        registered_roots = getattr(self.storage, "registered_roots", None)
        if registered_roots is None:
            roots = (None,)
        else:
            roots = registered_roots()
        for root in roots:
            if root is None or root == self.storage.root:
                records = ((name, self.storage.read(name)) for name in self.storage.list_records() if name.startswith("op-"))
            else:
                other = object.__new__(ProtectedStore)
                other.root = root
                try:
                    entries = []
                    with os.scandir(root) as paths:
                        for entry in paths:
                            if not entry.name.startswith("op-") or not entry.name.endswith(".dpapi"):
                                continue
                            details = entry.stat(follow_symlinks=False)
                            if not stat.S_ISREG(details.st_mode):
                                raise OSError("Unexpected journal path")
                            name = entry.name[:-len(".dpapi")]
                            entries.append((name, other.read(name)))
                    records = iter(entries)
                except OSError as exc:
                    raise JournalError("An existing recovery journal record cannot be counted safely") from exc
            for name, contents in records:
                try:
                    validated = self._decode_operation_root(contents, f"{name}.dpapi")
                except Exception as exc:
                    raise JournalError("An existing recovery journal record cannot be counted safely") from exc
                if validated["state"] in UNRESOLVED_STATES:
                    count += 1
        return count

    def _workflow_dependencies_locked(self) -> set[str]:
        """Read identity-bound workflow pins, including independently stored sources.

        Only bounded authenticated roots are needed. Never acquire another
        root's journal/workflow locks while holding this journal-index lock.
        The shared quota lock serializes root publication and deletion.
        """
        from .._import_workflows import ImportWorkflowStore
        dependencies = set()
        quota = getattr(self.storage, "user_quota_lock", None)
        with quota() if quota else nullcontext():
            registered = getattr(self.storage, "registered_roots", None)
            roots = registered() if registered else (self.storage.root,)
            for root in roots:
                if root == self.storage.root:
                    storage = self.storage
                else:
                    storage = object.__new__(ProtectedStore)
                    storage.root = root
                try:
                    for name in storage.list_records():
                        if re.fullmatch(r"import-workflow-[0-9a-f]{32}", name) is None:
                            continue
                        header = ImportWorkflowStore.decode_root(storage.read(name), name)
                        if header["state"] in {"APPLIED", "ABANDONED", "FAILED", "CANCELLED", "EXPIRED"} and header["cleanup_status"] == "complete":
                            continue
                        for side in ("source", "target"):
                            if header["dependencies"][side]:
                                identity = DeploymentIdentity.from_dict(header[side + "_identity"])
                                dependencies.update(_record_name(identity, key) for key in header["dependencies"][side])
                except Exception as exc:
                    raise JournalError("Protected workflow dependencies cannot be assessed safely") from exc
        return dependencies

    def _clone_workflow_dependencies_locked(self, cutoff: datetime) -> set[str]:
        """Pin outstanding clone actions and committed clone results to their local root."""
        from .._clone_workflows import CloneWorkflowStore
        dependencies = set()
        for name in self.storage.iter_records():
            match = re.fullmatch(r"clone-workflow-([0-9a-f]{32})", name)
            if match is None:
                continue
            try:
                record = _strict_json_loads(self.storage.read(name))
                CloneWorkflowStore._validate(record, match.group(1))
                if record["outstanding_action_key"] is not None:
                    dependencies.add(_record_name(DeploymentIdentity.from_dict(record["identity"]),
                                                   record["outstanding_action_key"]))
                if record["clone_action_key"] is not None:
                    resolved = record["resolved_at"]
                    if (resolved is None or _aware_datetime(resolved) > cutoff
                            or record["cleanup_status"] != "complete"):
                        dependencies.add(_record_name(DeploymentIdentity.from_dict(record["identity"]),
                                                       record["clone_action_key"]))
            except Exception as exc:
                raise JournalError("A clone workflow reference cannot be assessed safely") from exc
        return dependencies

    def _prune_clone_workflows_locked(self, cutoff: datetime) -> None:
        """Remove terminal clone roots after the ordinary result retention window."""
        from .._clone_workflows import CloneWorkflowStore
        expired = []
        for name in self.storage.iter_records():
            match = re.fullmatch(r"clone-workflow-([0-9a-f]{32})", name)
            if match is None:
                continue
            try:
                record = _strict_json_loads(self.storage.read(name))
                CloneWorkflowStore._validate(record, match.group(1))
                if record["state"] not in {"COMMITTED", "FAILED"} or record["resolved_at"] is None:
                    continue
                if record["cleanup_status"] == "complete" and _aware_datetime(record["resolved_at"]) <= cutoff:
                    expired.append(name)
                    if len(expired) >= 64:
                        break
            except Exception as exc:
                raise JournalError("A clone workflow record cannot be pruned safely") from exc
        for name in expired:
            self.storage.delete(name)

    def _transfer_dependencies_locked(self) -> set[str]:
        """Snapshot validated transfer pins under journal-index, never transfer locks."""
        from .transfers import ProtectedTransferSpool

        dependencies = set()
        for name in self.storage.list_records():
            match = re.fullmatch(r"transfer-([0-9a-f]{32})-manifest", name)
            if match is None:
                continue
            try:
                raw = _strict_json_loads(self.storage.read(name))
                identity = DeploymentIdentity.from_dict(raw["identity"])
                manifest = ProtectedTransferSpool(self, identity).load(match.group(1))
                if manifest["state"] == "failed":
                    continue  # Self-contained failure; retain ordinary notice TTL.
                fields = ("finish_action_key",) if manifest["state"] == "finished" else (
                    "begin_action_key", "pending_action_key", "finish_action_key")
                dependencies.update(_record_name(identity, manifest[field]) for field in fields
                                    if manifest[field] is not None)
            except Exception as exc:
                raise JournalError("Protected transfer dependencies cannot be assessed safely") from exc
        return dependencies

    def _prune_expired_locked(self, identity: DeploymentIdentity) -> None:
        cutoff = datetime.now(timezone.utc) - RESULT_RETENTION
        self._prune_clone_workflows_locked(cutoff)
        dependencies = (self._transfer_dependencies_locked() | self._workflow_dependencies_locked()
                       | self._clone_workflow_dependencies_locked(cutoff))
        for name in self.storage.list_records():
            if not name.startswith("op-"):
                continue
            try:
                payload = self.storage.read(name)
                root = self._decode_operation_root(payload, name)
                if root["state"] not in {"released", "failed"}:
                    continue
                record = self._decode_record(payload, name)
                resolved = _aware_datetime(record["resolved_at"])
            except Exception as exc:
                raise JournalError("An existing recovery journal record cannot be pruned safely") from exc
            if resolved <= cutoff and name not in dependencies:
                self.storage.delete(name)
        self._cleanup_operation_payloads_locked()
        self._cleanup_workflow_payloads_locked()

    @staticmethod
    def _decode_operation_root(payload: bytes, name: str) -> dict[str, Any]:
        root = _strict_json_loads(payload)
        required = {"key", "identity", "state", "payload", "updatedAt", "resolvedAt"}
        if not isinstance(root, dict) or set(root) != required:
            raise JournalError("Recovery operation root has an unsupported format")
        try:
            identity = DeploymentIdentity.from_dict(root["identity"])
            expected = _record_name(identity, root["key"])
            if (not _is_uuid(root["key"]) or name not in {expected, f"{expected}.dpapi"}
                    or not isinstance(root["state"], str) or root["state"] not in _TRANSITIONS
                    or _aware_datetime(root["updatedAt"]) is None
                    or (root["resolvedAt"] is not None and not isinstance(root["resolvedAt"], str))):
                raise ValueError("Invalid operation root binding")
            _validate_payload_descriptor(root["payload"], "operation")
        except (ValueError, TypeError, KeyError) as exc:
            raise JournalError("Recovery operation root is corrupt") from exc
        return root

    def _decode_record(
        self,
        payload: bytes,
        name: str,
        *,
        identity: DeploymentIdentity | None = None,
    ) -> dict[str, Any]:
        """Strictly decode, validate, and bind one record to its protected filename."""
        root = self._decode_operation_root(payload, name)
        try:
            raw = self._read_segmented_payload(name.removesuffix(".dpapi"), root["payload"])
            raw.update(state=root["state"], updated_at=root["updatedAt"], resolved_at=root["resolvedAt"])

            if root["resolvedAt"] is not None and root["state"] not in {"released", "failed"}:
                raise ValueError("Unreleased root lifecycle timestamp is invalid")
            if any(raw[field] != root[field] for field in ("identity", "key")):
                raise ValueError("Operation root and payload disagree")
        except Exception as exc:
            raise JournalError("Recovery operation payload is corrupt or unavailable") from exc
        if not isinstance(raw, dict):
            raise JournalError("Recovery journal record is corrupt")
        try:
            identity_data = raw.get("identity")
            if not isinstance(identity_data, dict):
                raise ValueError("Invalid stored identity")
            stored_identity = DeploymentIdentity.from_dict(identity_data)
            key = raw.get("key")
            if not isinstance(key, str):
                raise ValueError("Invalid recovery key")
            record = self._validate(raw, stored_identity, key)
            expected_name = _record_name(stored_identity, key)
            if name not in {expected_name, f"{expected_name}.dpapi"}:
                raise JournalError("Recovery journal filename does not match its identity and key")
            if identity is not None and not stored_identity.matches(identity):
                raise JournalError("Recovery journal identity does not match this Store")
            return record
        except JournalError:
            raise
        except Exception as exc:
            raise JournalError("Recovery journal record is corrupt or unsupported") from exc

    @staticmethod
    def _validate(record: Any, identity: DeploymentIdentity, key: str) -> dict[str, Any]:
        required = {
            "key", "public_id", "identity", "procedure", "arguments", "snapshot_revision",
            "state", "created_at", "updated_at", "outcome", "operation_id", "resolved_at",
        }
        allowed = required | {"result_format", "failure"}
        if not isinstance(record, dict) or not required <= set(record) or set(record) - allowed:
            raise JournalError("Recovery journal record has an unsupported format")
        if "result_format" in record and (
            type(record["result_format"]) is not str
            or record["result_format"] not in {RESULT_FORMAT_NAMED_ROWS, RESULT_FORMAT_IMPORT, RESULT_FORMAT_CLONE}
        ):
            raise JournalError("Recovery journal record has an unsupported format")
        if (
            not isinstance(record["key"], str)
            or not _is_uuid(record["key"])
            or record["key"] != key
            or not _is_uuid(record["public_id"])
        ):
            raise JournalError("Recovery journal record has an unsupported format")
        stored_identity = DeploymentIdentity.from_dict(record["identity"])
        if not stored_identity.matches(identity):
            raise JournalError("Recovery journal identity does not match this Store")
        if not isinstance(record["procedure"], str) or not record["procedure"].startswith("deposit."):
            raise JournalError("Recovery journal procedure is invalid")
        if (identity.namespace_id is None) != (record["procedure"] in _SERVER_SCOPED_OPERATIONS):
            raise JournalError("Recovery journal action scope does not match its procedure")
        if (record["procedure"] == "deposit.import.cloneNamespace") != (record.get("result_format") == RESULT_FORMAT_CLONE):
            raise JournalError("Recovery journal result format does not match its procedure")
        if (
            not isinstance(record["arguments"], list)
            or type(record["snapshot_revision"]) is not int
            or record["snapshot_revision"] < 0
        ):
            raise JournalError("Recovery journal inputs are invalid")
        if not isinstance(record["state"], str) or record["state"] not in _TRANSITIONS:
            raise JournalError("Recovery journal state is invalid")
        for field in ("created_at", "updated_at"):
            try:
                _aware_datetime(record[field])
            except (TypeError, ValueError, OverflowError) as exc:
                raise JournalError("Recovery journal timestamp is invalid") from exc
        if record["operation_id"] is not None and not isinstance(record["operation_id"], str):
            raise JournalError("Recovery journal operation metadata is invalid")
        failure = record.get("failure")
        if record["state"] in {"failure_saved", "failed"}:
            if not _valid_failure(failure) or record["outcome"] is not None:
                raise JournalError("Saved recovery failure is invalid")
        elif failure is not None:
            raise JournalError("Recovery failure metadata is invalid for its state")
        if record["state"] in {"released", "failed"}:
            if not isinstance(record["outcome"], (dict, list, str, int, float, bool, type(None))):
                raise JournalError("Released recovery outcome is invalid")
            if not isinstance(record["resolved_at"], str):
                raise JournalError("Released recovery retention metadata is invalid")
            try:
                _aware_datetime(record["resolved_at"])
            except (TypeError, ValueError, OverflowError) as exc:
                raise JournalError("Released recovery retention metadata is invalid") from exc
            if isinstance(record["outcome"], float) and not math.isfinite(record["outcome"]):
                raise JournalError("Released recovery outcome is invalid")
        elif record["resolved_at"] is not None:
            raise JournalError("Unreleased recovery record has retention metadata")
        if record.get("result_format") == RESULT_FORMAT_CLONE and (
                record["state"] in {"COMMITTED_RESULT_PENDING", "outcome_saved", "release_sent", "released"}
                or record["outcome"] is not None):
            if record["outcome"] is None or record["outcome"].get("kind") != "clone" or (
                    record["outcome"]["operationId"] != record["operation_id"]):
                raise JournalError("Clone recovery result is incomplete or belongs to another action")
        if (record.get("result_format") == RESULT_FORMAT_IMPORT
                and record["outcome"] is not None
                and record["outcome"].get("kind") == "clone"):
            raise JournalError("Import recovery result cannot have server-scoped clone semantics")
        if record.get("result_format") == RESULT_FORMAT_NAMED_ROWS and record["outcome"] is not None:
            if not valid_named_rows_result(record["outcome"]):
                raise JournalError("Advanced recovery result is invalid")
        if record.get("result_format") in {RESULT_FORMAT_IMPORT, RESULT_FORMAT_CLONE} and record["outcome"] is not None:
            if not valid_import_result_envelope(record["outcome"], leased=False):
                raise JournalError("Recovery result envelope is invalid")
        if record["state"] == "COMMITTED_RESULT_PENDING" and (
                record.get("result_format") not in {RESULT_FORMAT_IMPORT, RESULT_FORMAT_CLONE}
                or not valid_import_result_envelope(record.get("outcome"), leased=False)
                or not isinstance(record.get("operation_id"), str)):
            raise JournalError("Committed result recovery state is invalid")
        return record



def _operation_payload_prefix(operation: str, generation: str) -> str:
    return f"action-payload-{operation}-{generation}-"


def _segmented_payload_prefix(owner: str, role: str, generation: str) -> str:
    if role == "operation":
        return _operation_payload_prefix(owner, generation)
    if role == "workflow":
        return f"workflow-payload-{owner}-{generation}-"
    raise JournalError("Segmented payload role is unsupported")


def _payload_owner(owner, role, generation):
    return {"operation" if role == "operation" else "workflow": owner,
            "generation": generation, "role": role}


def _validate_payload_descriptor(descriptor, role):
    if (role not in {"operation", "workflow"} or not isinstance(descriptor, dict)
            or set(descriptor) != {"role", "generation", "chunkCount", "bytes", "sha256"}
            or descriptor["role"] != role or not isinstance(descriptor["generation"], str)
            or re.fullmatch(r"[0-9a-f]{32}", descriptor["generation"]) is None
            or type(descriptor["chunkCount"]) is not int or descriptor["chunkCount"] < 1
            or type(descriptor["bytes"]) is not int or descriptor["bytes"] < 1
            or descriptor["chunkCount"] != (descriptor["bytes"] + OPERATION_CHUNK_BYTES - 1) // OPERATION_CHUNK_BYTES
            or not isinstance(descriptor["sha256"], str)
            or re.fullmatch(r"[0-9a-f]{64}", descriptor["sha256"]) is None):
        raise ValueError("Segmented payload descriptor is invalid")


def _operation_json_chunks(record: dict[str, Any]):
    """Encode exact JSON in bounded byte chunks, without an aggregate byte cap."""
    pending = bytearray()
    encoder = json.JSONEncoder(sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    try:
        for token in encoder.iterencode(record):
            # iterencode can yield one large string token; do not duplicate its
            # entire UTF-8 representation while assembling protected chunks.
            for offset in range(0, len(token), 65536):
                encoded = token[offset:offset + 65536].encode("utf-8")
                position = 0
                while position < len(encoded):
                    count = min(OPERATION_CHUNK_BYTES - len(pending), len(encoded) - position)
                    pending.extend(encoded[position:position + count])
                    position += count
                    if len(pending) == OPERATION_CHUNK_BYTES:
                        yield bytes(pending)
                        pending.clear()
        if pending:
            yield bytes(pending)
    except (TypeError, ValueError, UnicodeError) as exc:
        raise JournalError("Recovery arguments or result are not losslessly journalable") from exc


def _json_bytes(value: Any) -> bytes:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"),
                          ensure_ascii=False, allow_nan=False).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise JournalError("Import result contains a nonportable value") from exc


def _valid_failure(failure: Any) -> bool:
    if not isinstance(failure, dict) or set(failure) != {"code", "message", "details", "request_id", "retryable"}:
        return False
    if (not isinstance(failure["code"], str) or re.fullmatch(r"[A-Z][A-Z0-9_]{0,63}", failure["code"]) is None
            or not isinstance(failure["message"], str) or not 1 <= len(failure["message"]) <= 2048
            or failure["retryable"] is not False
            or (failure["details"] is not None and not isinstance(failure["details"], dict))):
        return False
    request_id = failure["request_id"]
    if request_id is not None:
        try:
            uuid.UUID(request_id)
        except (ValueError, TypeError, AttributeError):
            return False
    return True


def valid_named_rows_result(value: Any) -> bool:
    if not isinstance(value, dict) or set(value) != {"columns", "rows"}:
        return False
    columns, rows = value["columns"], value["rows"]
    if (
        not isinstance(columns, list)
        or not columns
        or any(not isinstance(column, str) or not column for column in columns)
        or len(set(columns)) != len(columns)
        or not isinstance(rows, list)
    ):
        return False
    return all(isinstance(row, dict) and set(row) == set(columns) for row in rows)




def _is_uuid(value: str) -> bool:
    try:
        return str(uuid.UUID(value)) == value
    except (TypeError, ValueError, AttributeError):
        return False
