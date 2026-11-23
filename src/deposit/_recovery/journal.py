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
IMPORT_RESULT_PAGE_LIMIT = 128
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

    def persist_import_result(self, identity: DeploymentIdentity, key: str, envelope: dict[str, Any], fetch_page):
        """Persist the exact immutable result before the prepared action is released."""
        if not _is_uuid(key) or not valid_import_result_envelope(envelope):
            raise JournalError("Import result envelope is invalid")
        clone_result = envelope["kind"] == "clone"
        action = next((item for item in self.records(identity)
                       if item["key"] == key and item["procedure"] == "deposit.import.cloneNamespace"), None)
        if clone_result:
            if (identity.namespace_id is not None or envelope["operationId"] is None
                    or action is None or action.get("result_format") != RESULT_FORMAT_CLONE):
                raise JournalError("Clone result is not bound to a server-scoped action")
        elif (identity.namespace_id is None or envelope["namespaceId"] != identity.namespace_id
                or envelope["kind"] not in {"analysis", "apply", "diagnostic", "plan_inspection"}):
            raise JournalError("Import result belongs to a different Namespace")
        index_name = _import_result_index_name(key)
        prior = None
        if self.storage.has_record(index_name):
            try:
                prior = _strict_json_loads(self.storage.read(index_name))
            except Exception as exc:
                raise JournalError("Protected import result index is corrupt") from exc
        if prior is not None and prior.get("envelope") != envelope:
            raise JournalError("Prepared action is bound to a different committed result")

        for collection, descriptor in envelope["collections"].items():
            self._ensure_import_collection_meta(key, envelope["resultId"], collection,
                                                descriptor["kind"], descriptor["count"], None)
        prefix = f"import-result-{key}-collection-"
        from collections import deque
        pending = deque()
        for name in self.storage.list_records():
            if name.startswith(prefix) and name.endswith("-meta"):
                meta = _strict_json_loads(self.storage.read(name))
                if meta.get("complete") is not True:
                    pending.append(meta["collection"])
        queued = set(pending)
        while pending:
            collection = pending.popleft()
            queued.discard(collection)
            meta_name = _import_collection_meta_name(key, collection)
            meta = _strict_json_loads(self.storage.read(meta_name))
            if meta.get("complete") is True:
                continue
            nested = self._persist_import_collection_page(key, envelope, meta, fetch_page)
            if meta.get("complete") is not True and collection not in queued:
                pending.append(collection)
                queued.add(collection)
            for child in reversed(nested):
                if child not in queued:
                    pending.appendleft(child)
                    queued.add(child)

        self._validate_import_mapping_keys(key)

        root_index = {"key": key, "resultId": envelope["resultId"],
                      "envelope": envelope, "collections": envelope["collections"]}
        self._write_import_segments(key, {_import_result_index_name(key): _json_bytes(root_index)})
        return root_index

    def get_or_create_local_import_result(self, identity: DeploymentIdentity, result_id: str) -> str:
        """Return a durable local key for read-only plan inspection, without an action record."""
        if not isinstance(result_id, str) or not result_id or len(result_id) > 512:
            raise JournalError("Import result reference is invalid")
        payload = _json_bytes(identity.to_dict()) + b"\0" + result_id.encode("utf-8")
        reference_name = _import_result_ref_name(payload)
        with self.storage.lock("journal-index"):
            if self.storage.has_record(reference_name):
                reference = self._read_local_import_result_ref(reference_name)
                if reference["identity"] != identity.to_dict() or reference["resultId"] != result_id:
                    raise JournalError("Local import result reference is bound to different data")
                if self._local_import_result_expired(reference, datetime.now(timezone.utc)):
                    if not self._cleanup_import_result_locked(reference["key"], identity=DeploymentIdentity.from_dict(reference["identity"])):
                        return reference["key"]
                    self.storage.delete(reference_name)
                else:
                    return reference["key"]
            key = str(uuid.uuid4())
            reference = {"key": key, "resultId": result_id,
                         "identity": identity.to_dict(), "state": "pending",
                         "createdAt": _utc_now(), "completedAt": None,
                         "planObservation": None}
            self._write_protected(reference_name, _json_bytes(reference))
            return key

    def complete_local_import_result(self, identity: DeploymentIdentity, key: str,
                                     envelope: dict[str, Any], plan_observation: dict[str, Any]) -> dict[str, Any]:
        if not _is_uuid(key) or not valid_import_result_envelope(envelope):
            raise JournalError("Local import result is invalid")
        if envelope["namespaceId"] != identity.namespace_id:
            raise JournalError("Local import result belongs to another Namespace")
        reference_name = _import_result_ref_name(
            _json_bytes(identity.to_dict()) + b"\0" + envelope["resultId"].encode("utf-8"))
        with self.storage.lock("journal-index"):
            reference = self._read_local_import_result_ref(reference_name)
            if (reference["key"] != key or reference["identity"] != identity.to_dict()
                    or reference["resultId"] != envelope["resultId"]):
                raise JournalError("Local import result reference does not match the saved envelope")
            if not _valid_plan_observation(plan_observation, envelope):
                raise JournalError("Current import plan observation is invalid")
            index = self.read_import_index(key)
            if index["envelope"] != envelope:
                raise JournalError("Local import result index does not match its envelope")
            reference["planObservation"] = plan_observation
            if reference["state"] != "complete":
                reference["state"] = "complete"
                reference["completedAt"] = _utc_now()
            self._write_protected(reference_name, _json_bytes(reference))
            return reference

    def _read_local_import_result_ref(self, name: str) -> dict[str, Any]:
        try:
            reference = _strict_json_loads(self.storage.read(name))
            if (not isinstance(reference, dict)
                    or set(reference) != {"key", "resultId", "identity", "state", "createdAt", "completedAt", "planObservation"}
                    or not _is_uuid(reference.get("key"))
                    or not isinstance(reference.get("resultId"), str)
                    or not isinstance(reference.get("identity"), dict)
                    or reference.get("state") not in {"pending", "complete"}
                    or (reference["state"] == "pending") != (reference.get("completedAt") is None)
                    or (reference.get("planObservation") is not None
                        and not _valid_plan_observation(reference["planObservation"]))):
                raise ValueError("invalid local result reference")
            DeploymentIdentity.from_dict(reference["identity"])
            _aware_datetime(reference["createdAt"])
            if reference["completedAt"] is not None:
                _aware_datetime(reference["completedAt"])
            return reference
        except Exception as exc:
            raise JournalError("Protected local import result reference is corrupt") from exc

    @staticmethod
    def _local_import_result_expired(reference: dict[str, Any], now: datetime) -> bool:
        anchor = reference["completedAt"] if reference["completedAt"] is not None else reference["createdAt"]
        return _aware_datetime(anchor) <= now - RESULT_RETENTION

    def _prune_local_import_results_locked(self, cutoff: datetime) -> int:
        removed = 0
        for name in self.storage.list_records():
            if not name.startswith("import-result-ref-"):
                continue
            reference = self._read_local_import_result_ref(name)
            anchor = reference["completedAt"] if reference["completedAt"] is not None else reference["createdAt"]
            if _aware_datetime(anchor) <= cutoff:
                if not self._cleanup_import_result_locked(reference["key"], identity=DeploymentIdentity.from_dict(reference["identity"])):
                    continue
                self.storage.delete(name)
                removed += 1
        return removed

    def _ensure_import_collection_meta(self, key, result_id, collection, kind, count, text_bytes):
        if not isinstance(collection, str) or not collection or len(collection) > 512:
            raise JournalError("Import result collection identifier is invalid")
        meta_name = _import_collection_meta_name(key, collection)
        created = not self.storage.has_record(meta_name)
        if not created:
            try:
                meta = _strict_json_loads(self.storage.read(meta_name))
                if (not isinstance(meta, dict) or meta.get("resultId") != result_id or meta.get("collection") != collection
                        or meta.get("kind") != kind or meta.get("count") != count
                        or meta.get("textBytes") != text_bytes):
                    raise JournalError("Saved import collection metadata changed")
            except JournalError:
                raise
            except Exception as exc:
                raise JournalError("Protected import collection metadata is corrupt") from exc
        else:
            meta = {"resultId": result_id, "collection": collection,
                    "kind": kind, "count": count, "textBytes": text_bytes,
                    "pageCount": 0, "entryCount": 0, "nextCursor": None, "complete": False}
            self._write_import_segments(key, {meta_name: _json_bytes(meta)})
        return meta, created

    def _persist_import_collection_page(self, key, envelope, meta, fetch_page):
        collection, kind, count, text_bytes = (meta["collection"], meta["kind"], meta["count"], meta["textBytes"])
        nested_collections = []
        try:
            request_cursor = meta["nextCursor"]
            response = fetch_page(collection, request_cursor, IMPORT_RESULT_PAGE_LIMIT)
            _validate_import_page(response, envelope["resultId"], collection, kind,
                                  request_cursor, meta["entryCount"], count, text_bytes)
            page_index = meta["pageCount"]
            page_payload = {"requestCursor": request_cursor, "response": response}
            page_name = _import_result_page_name(key, collection, page_index)
            encoded_page = _json_bytes(page_payload)
            if self.storage.has_record(page_name):
                try:
                    saved_page = _strict_json_loads(self.storage.read(page_name))
                except Exception as exc:
                    raise JournalError("Saved import page is corrupt") from exc
                if saved_page != page_payload:
                    raise JournalError("Immutable import result page changed at a saved cursor")
            else:
                self._write_import_segments(key, {page_name: encoded_page})
            for value in _collection_values(response["entries"], kind):
                nested_collections.extend(_ensure_nested_import_collections(
                    self, key, envelope["resultId"], value))
            if response["nextCursor"] is None and text_bytes is not None:
                actual_bytes = sum(
                    len(chunk["value"].encode("utf-8"))
                    for index in range(meta["pageCount"])
                    for chunk in self._read_import_page_with_meta(key, collection, index, meta)["response"]["entries"]
                )
                actual_bytes += sum(len(chunk["value"].encode("utf-8")) for chunk in response["entries"])
                if actual_bytes != text_bytes:
                    raise JournalError("Import text collection byte count is invalid")
            meta["pageCount"] += 1
            meta["entryCount"] += len(response["entries"])
            meta["nextCursor"] = response["nextCursor"]
            meta["complete"] = response["nextCursor"] is None
            self._write_import_segments(key, {_import_collection_meta_name(key, collection): _json_bytes(meta)})
            if meta["complete"] and count is not None and meta["entryCount"] != count:
                raise JournalError("Import result collection count does not match its envelope")
            return nested_collections
        except (JournalError, DepositError):
            raise
        except Exception as exc:
            raise JournalError("Import result retrieval did not complete") from exc

    def _validate_import_mapping_keys(self, key: str) -> None:
        prefix = f"import-result-{key}-collection-"
        for name in self.storage.list_records():
            if not name.startswith(prefix) or not name.endswith("-meta"):
                continue
            meta = _strict_json_loads(self.storage.read(name))
            collection = meta["collection"]
            if meta["kind"] != "mapping":
                continue
            meta = self.read_import_collection_meta(key, collection)
            for page_index in range(meta["pageCount"]):
                entries = self.read_import_page(key, collection, page_index)["response"]["entries"]
                for entry_index, pair in enumerate(entries):
                    key_descriptor = pair["key"]
                    signature = self._mapping_key_signature(key, key_descriptor)
                    signature_hash = hashlib.sha256(_json_bytes([
                        item.hex() if isinstance(item, bytes) else item for item in signature
                    ])).hexdigest()
                    collection_hash = hashlib.sha256(collection.encode("utf-8")).hexdigest()
                    marker = f"import-result-{key}-mapkey-{collection_hash}-{signature_hash}"
                    location = {"collection": collection, "page": page_index, "entry": entry_index}
                    if self.storage.has_record(marker):
                        prior = _strict_json_loads(self.storage.read(marker))
                        if prior != location:
                            raise JournalError("Import result mapping contains duplicate keys")
                    else:
                        self._write_import_segments(key, {marker: _json_bytes(location)})

    def _mapping_key_signature(self, action_key: str, descriptor: dict[str, Any]):
        _validate_import_value(descriptor)
        if descriptor["type"] == "scalar":
            scalar = descriptor["value"]
            if isinstance(scalar, str):
                data = scalar.encode("utf-8")
                return ("text", len(data), hashlib.sha256(data).hexdigest())
            raise JournalError("Import mapping scalar keys must be strings")
        if descriptor["type"] != "text":
            raise JournalError("Import result mapping key must be scalar or text")
        collection = descriptor["collection"]
        meta = self.read_import_collection_meta(action_key, collection)
        if meta["kind"] != "text" or meta["textBytes"] != descriptor["bytes"]:
            raise JournalError("Import result mapping text key descriptor is inconsistent")
        digest = hashlib.sha256()
        byte_count = 0
        for page_index in range(meta["pageCount"]):
            page = self.read_import_page(action_key, collection, page_index)["response"]
            for chunk in page["entries"]:
                encoded = chunk["value"].encode("utf-8")
                digest.update(encoded)
                byte_count += len(encoded)
        return ("text", byte_count, digest.hexdigest())

    def read_import_index(self, key: str) -> dict[str, Any]:
        if not _is_uuid(key):
            raise JournalError("Recovery action key is invalid")
        try:
            index = _strict_json_loads(self.storage.read(_import_result_index_name(key)))
        except Exception as exc:
            raise JournalError("Protected import result index is unavailable") from exc
        if (not isinstance(index, dict) or set(index) != {"key", "resultId", "envelope", "collections"}
                or index["key"] != key
                or not valid_import_result_envelope(index["envelope"])
                or index["resultId"] != index["envelope"]["resultId"]
                or index["collections"] != index["envelope"]["collections"]):
            raise JournalError("Protected import result index is corrupt")
        return index

    def pin_import_result(self, key: str):
        """Hold a process lock and durable marker while a lazy reader is live."""
        if not _is_uuid(key):
            raise JournalError("Recovery action key is invalid")
        pin_id = uuid.uuid4().hex
        pin_name = f"import-result-{key}-pin-{pin_id}"
        owner_lock = self.storage.lock(f"import-result-pin-{pin_id}")
        with self.storage.lock("journal-index"):
            # Do not publish a pin for a missing, corrupt, or different result.
            self.read_import_index(key)
            owner_lock.acquire()
            try:
                self._write_protected(pin_name, _json_bytes({"key": key, "pinId": pin_id}))
            except BaseException:
                owner_lock.release()
                raise
        return pin_name, owner_lock

    def release_import_result_pin(self, pin_name: str, owner_lock) -> None:
        try:
            with self.storage.lock("journal-index"):
                self.storage.delete(pin_name)
        finally:
            owner_lock.release()

    def _cleanup_import_result_locked(self, key: str, *, identity: DeploymentIdentity) -> bool:
        """Delete expired result segments only when readers/workflows release them."""
        if (_record_name(identity, key) in self._workflow_dependencies_locked()
                or _record_name(identity, key) in self._clone_workflow_dependencies_locked(
                    datetime.now(timezone.utc) - RESULT_RETENTION)):
            return False
        prefix = f"import-result-{key}-"
        for name in self.storage.list_records():
            if not name.startswith(prefix) or "-pin-" not in name:
                continue
            try:
                pin = _strict_json_loads(self.storage.read(name))
                if (not isinstance(pin, dict) or set(pin) != {"key", "pinId"}
                        or pin["key"] != key
                        or not isinstance(pin["pinId"], str) or not re.fullmatch(r"[0-9a-f]{32}", pin["pinId"])):
                    raise ValueError("invalid result pin")
                lock = self.storage.lock(f"import-result-pin-{pin['pinId']}")
                try:
                    lock.acquire()
                except ProtectedStorageError:
                    return False
                else:
                    lock.release()
                    self.storage.delete(name)
            except ProtectedStorageError:
                raise
            except Exception as exc:
                raise JournalError("Import result reader pins cannot be assessed safely") from exc
        for name in self.storage.list_records():
            if name.startswith(prefix):
                self.storage.delete(name)
        for name in self.storage.list_records():
            if not name.startswith("import-result-ref-"):
                continue
            reference = self._read_local_import_result_ref(name)
            if reference["key"] == key:
                self.storage.delete(name)
        return True

    def read_import_collection_meta(self, key: str, collection: str) -> dict[str, Any]:
        try:
            meta = _strict_json_loads(self.storage.read(_import_collection_meta_name(key, collection)))
        except Exception as exc:
            raise JournalError("Protected import collection index is unavailable") from exc
        if (not isinstance(meta, dict)
                or set(meta) != {"resultId", "collection", "kind", "count", "textBytes",
                                 "pageCount", "entryCount", "nextCursor", "complete"}
                or not isinstance(meta.get("resultId"), str)
                or meta.get("collection") != collection or not isinstance(meta.get("kind"), str)
                or meta.get("kind") not in {"sequence", "mapping", "text"}
                or meta.get("complete") is not True
                or type(meta.get("pageCount")) is not int or not 0 <= meta["pageCount"] <= _MAX_SAFE_INTEGER
                or type(meta.get("entryCount")) is not int or not 0 <= meta["entryCount"] <= _MAX_SAFE_INTEGER
                or (meta["count"] is not None and (type(meta["count"]) is not int
                    or not 0 <= meta["count"] <= _MAX_SAFE_INTEGER))
                or (meta["textBytes"] is not None and (type(meta["textBytes"]) is not int
                    or not 0 <= meta["textBytes"] <= _MAX_SAFE_INTEGER))
                or (meta["kind"] == "text") != (meta["textBytes"] is not None)
                or (meta["kind"] != "text" and meta["count"] != meta["entryCount"])
                or (meta["kind"] == "text" and meta["count"] is not None)):
            raise JournalError("Protected import collection index is corrupt or incomplete")
        return meta

    def read_import_page(self, key: str, collection: str, page_index: int) -> dict[str, Any]:
        meta = self.read_import_collection_meta(key, collection)
        return self._read_import_page_with_meta(key, collection, page_index, meta)

    def _read_import_page_with_meta(self, key: str, collection: str, page_index: int,
                                    meta: dict[str, Any]) -> dict[str, Any]:
        if type(page_index) is not int or not 0 <= page_index < meta["pageCount"]:
            raise IndexError("Import result page index is out of range")
        try:
            payload = _strict_json_loads(self.storage.read(_import_result_page_name(key, collection, page_index)))
        except Exception as exc:
            raise JournalError("Protected import result page is unavailable") from exc
        if not isinstance(payload, dict) or set(payload) != {"requestCursor", "response"}:
            raise JournalError("Protected import result page is corrupt")
        response = payload["response"]
        request_cursor = payload["requestCursor"]
        if request_cursor is not None and not isinstance(request_cursor, dict):
            raise JournalError("Protected import page cursor is corrupt")
        start = 0 if request_cursor is None else request_cursor.get("position")
        if type(start) is not int or (page_index == 0 and payload["requestCursor"] is not None):
            raise JournalError("Protected import page cursor is corrupt")
        if page_index > 0:
            prior = _strict_json_loads(self.storage.read(_import_result_page_name(key, collection, page_index - 1)))
            prior_response = prior.get("response") if isinstance(prior, dict) else None
            if (not isinstance(prior_response, dict)
                    or prior_response.get("nextCursor") != payload["requestCursor"]):
                raise JournalError("Protected import page cursor chain is corrupt")
        _validate_import_page(response, meta["resultId"], collection, meta["kind"],
                              payload["requestCursor"], start, meta["count"], meta["textBytes"])
        if not isinstance(response, dict) or response.get("resultId") != meta["resultId"] or response.get("collection") != collection:
            raise JournalError("Protected import result page is corrupt")
        return payload

    def _write_import_segments(self, action_key: str, segments: dict[str, bytes]) -> None:
        for name, payload in segments.items():
            self._write_protected(name, payload)

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
                    if record.get("result_format") in {RESULT_FORMAT_IMPORT, RESULT_FORMAT_CLONE}:
                        if not self._cleanup_import_result_locked(record["key"], identity=DeploymentIdentity.from_dict(record["identity"])):
                            continue
                    self.storage.delete(_record_name(identity, record["key"]))
                    removed += 1
            self._cleanup_operation_payloads_locked()
            self._cleanup_workflow_payloads_locked()
            removed += self._prune_local_import_results_locked(cutoff)
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
                if record.get("result_format") in {RESULT_FORMAT_IMPORT, RESULT_FORMAT_CLONE}:
                    if not self._cleanup_import_result_locked(record["key"], identity=DeploymentIdentity.from_dict(record["identity"])):
                        continue
                self.storage.delete(name)
        self._prune_local_import_results_locked(cutoff)
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
            if not valid_import_result_envelope(record["outcome"]):
                raise JournalError("Recovery result envelope is invalid")
        if record["state"] == "COMMITTED_RESULT_PENDING" and (
                record.get("result_format") not in {RESULT_FORMAT_IMPORT, RESULT_FORMAT_CLONE}
                or not valid_import_result_envelope(record.get("outcome"))
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


def _import_result_index_name(key: str) -> str:
    return f"import-result-{key}-index"


def _import_result_ref_name(payload: bytes) -> str:
    return f"import-result-ref-{hashlib.sha256(payload).hexdigest()}"


def _import_collection_meta_name(key: str, collection: str) -> str:
    digest = hashlib.sha256(collection.encode("utf-8")).hexdigest()
    return f"import-result-{key}-collection-{digest}-meta"


def _import_result_page_name(key: str, collection: str, page_index: int) -> str:
    digest = hashlib.sha256(collection.encode("utf-8")).hexdigest()
    return f"import-result-{key}-collection-{digest}-page-{page_index:012d}"


def _validate_import_page(response: Any, result_id: str, collection: str, kind: str,
                          request_cursor: Any, entry_count: int, expected_count: int | None,
                          expected_text_bytes: int | None) -> None:
    required = {"resultId", "collection", "entries", "nextCursor"}
    if (not isinstance(response, dict) or set(response) != required
            or response.get("resultId") != result_id or response.get("collection") != collection
            or not isinstance(response.get("entries"), list)
            or len(response["entries"]) > IMPORT_RESULT_PAGE_LIMIT):
        raise JournalError("Import result page has an invalid V1 envelope")
    if request_cursor is not None and (
            not isinstance(request_cursor, dict)
            or set(request_cursor) != {"resultId", "collection", "position", "integrity"}
            or request_cursor.get("resultId") != result_id
            or request_cursor.get("collection") != collection
            or type(request_cursor.get("position")) is not int
            or not 0 <= request_cursor["position"] <= _MAX_SAFE_INTEGER
            or not isinstance(request_cursor.get("integrity"), str)
            or not request_cursor["integrity"]):
        raise JournalError("Import result request cursor is invalid")
    start = 0 if request_cursor is None else request_cursor["position"]
    if type(start) is not int or start != entry_count:
        raise JournalError("Import result cursor does not match the next unread position")
    next_cursor = response["nextCursor"]
    if next_cursor is not None:
        if (not isinstance(next_cursor, dict)
                or set(next_cursor) != {"resultId", "collection", "position", "integrity"}
                or next_cursor.get("resultId") != result_id
                or next_cursor.get("collection") != collection
                or type(next_cursor.get("position")) is not int
                or not 0 <= next_cursor["position"] <= _MAX_SAFE_INTEGER
                or next_cursor["position"] != start + len(response["entries"])
                or not isinstance(next_cursor.get("integrity"), str) or not next_cursor["integrity"]
                or not response["entries"]):
            raise JournalError("Import result cursor is invalid or made no progress")
    if expected_count is not None:
        next_count = entry_count + len(response["entries"])
        if next_count > expected_count or (next_cursor is None and next_count != expected_count):
            raise JournalError("Import result page count disagrees with its descriptor")
    if kind == "text":
        if any(not isinstance(chunk, dict) or set(chunk) != {"type", "value"}
               or chunk.get("type") != "scalar" or not isinstance(chunk.get("value"), str)
               for chunk in response["entries"]):
            raise JournalError("Import text page contains a non-scalar-string chunk descriptor")
        if any(len(chunk["value"].encode("utf-8")) > MAX_RECORD_BYTES for chunk in response["entries"]):
            raise JournalError("Import text chunk exceeds its bounded record unit")
    else:
        for value in _collection_values(response["entries"], kind):
            _validate_import_value(value)


def _collection_values(entries: list[Any], kind: str):
    if kind == "sequence":
        yield from entries
        return
    if kind == "text":
        return
    if kind != "mapping":
        raise JournalError("Import result collection kind is invalid")
    for entry in entries:
        if isinstance(entry, dict) and set(entry) == {"key", "value"}:
            key, value = entry["key"], entry["value"]
        else:
            raise JournalError("Import mapping page entry is invalid")
        if (not isinstance(key, dict) or not isinstance(key.get("type"), str)
                or key["type"] not in {"scalar", "text"}):
            raise JournalError("Import mapping key must be a scalar or lazy text descriptor")
        yield key
        yield value


def _validate_import_value(value: Any) -> None:
    if not isinstance(value, dict) or not isinstance(value.get("type"), str):
        raise JournalError("Import result value is missing its tagged descriptor")
    kind = value["type"]
    if kind == "scalar":
        if set(value) != {"type", "value"}:
            raise JournalError("Import scalar descriptor has unsupported fields")
        scalar = value["value"]
        if (not isinstance(scalar, (str, int, float, bool, type(None)))
                or isinstance(scalar, float) and not math.isfinite(scalar)
                or type(scalar) is int and not -_MAX_SAFE_INTEGER <= scalar <= _MAX_SAFE_INTEGER):
            raise JournalError("Import scalar descriptor is invalid")
        if len(_json_bytes(scalar)) > MAX_RECORD_BYTES:
            raise JournalError("Import scalar exceeds its bounded record unit")
    elif kind in {"mapping", "sequence"}:
        if (set(value) != {"type", "collection", "count"}
                or not isinstance(value["collection"], str) or not value["collection"]
                or len(value["collection"]) > 512 or type(value["count"]) is not int
                or not 0 <= value["count"] <= _MAX_SAFE_INTEGER):
            raise JournalError("Import collection descriptor is invalid")
    elif kind == "text":
        if (set(value) != {"type", "collection", "bytes"}
                or not isinstance(value["collection"], str) or not value["collection"]
                or len(value["collection"]) > 512 or type(value["bytes"]) is not int
                or not 0 <= value["bytes"] <= _MAX_SAFE_INTEGER):
            raise JournalError("Import text descriptor is invalid")
    else:
        raise JournalError("Import result value descriptor type is unsupported")


def _ensure_nested_import_collections(journal: ProtectedJournal, key: str, result_id: str,
                                     value: dict[str, Any]) -> list[str]:
    _validate_import_value(value)
    if value["type"] in {"mapping", "sequence"}:
        journal._ensure_import_collection_meta(key, result_id, value["collection"],
                                               value["type"], value["count"], None)
        return [value["collection"]]
    elif value["type"] == "text":
        journal._ensure_import_collection_meta(key, result_id, value["collection"],
                                               "text", None, value["bytes"])
        return [value["collection"]]
    return []

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


def valid_import_result_envelope(value: Any) -> bool:
    if not isinstance(value, dict) or set(value) != {
        "format", "resultId", "operationId", "kind", "state", "namespaceId",
        "planId", "revision", "expiresAt", "collections",
    }:
        return False
    if (value["format"] != "deposit.import-result"
            or not isinstance(value["resultId"], str) or not value["resultId"]
            or (value["operationId"] is not None
                and (not isinstance(value["operationId"], str) or not value["operationId"]))
            or not isinstance(value["kind"], str) or value["kind"] not in {"analysis", "apply", "diagnostic", "plan_inspection", "clone"}
            or not isinstance(value["state"], str) or not value["state"]
            or not isinstance(value["namespaceId"], str) or not value["namespaceId"]
            or (value["planId"] is not None and not isinstance(value["planId"], str))
            or type(value["revision"]) is not int or not 0 <= value["revision"] <= _MAX_SAFE_INTEGER
            or type(value["expiresAt"]) is not int or not 0 <= value["expiresAt"] <= _MAX_SAFE_INTEGER
            or not isinstance(value["collections"], dict)):
        return False
    if value["kind"] == "apply" and value["state"] != "APPLIED":
        return False
    if value["kind"] == "clone" and value["operationId"] is None:
        return False
    if value["kind"] == "clone" and (
        value["state"] != "committed" or value["planId"] is not None
        or set(value["collections"]) != {"mapping", "identityMappings", "resourceMapping", "metadata"}
        or any(not isinstance(value["collections"].get(key), dict)
               for key in value["collections"])
        or any(value["collections"][key].get("kind") != "mapping"
               for key in value["collections"])
    ):
        return False
    for name, descriptor in value["collections"].items():
        if (not isinstance(name, str) or not name or len(name) > 256
                or name in {"format", "resultId", "operationId", "kind", "state", "namespaceId", "planId", "revision", "expiresAt"}
                or not isinstance(descriptor, dict) or set(descriptor) != {"kind", "count"}
                or not isinstance(descriptor["kind"], str) or descriptor["kind"] not in {"sequence", "mapping"}
                or type(descriptor["count"]) is not int
                or not 0 <= descriptor["count"] <= _MAX_SAFE_INTEGER):
            return False
    return True


def _valid_plan_observation(value: Any, envelope: dict[str, Any] | None = None) -> bool:
    if (not isinstance(value, dict)
            or set(value) != {"planId", "namespaceId", "state", "targetRevision", "expiresAt"}
            or not isinstance(value["planId"], str) or not value["planId"]
            or not isinstance(value["namespaceId"], str) or not value["namespaceId"]
            or not isinstance(value["state"], str) or value["state"] not in {"ready", "applied"}
            or type(value["targetRevision"]) is not int
            or not 0 <= value["targetRevision"] <= _MAX_SAFE_INTEGER
            or type(value["expiresAt"]) is not int or not 0 <= value["expiresAt"] <= _MAX_SAFE_INTEGER):
        return False
    return (envelope is None or (value["planId"] == envelope["planId"]
                                 and value["namespaceId"] == envelope["namespaceId"]
                                 and value["targetRevision"] == envelope["revision"]))


def valid_import_plan_wrapper(value: Any, *, plan_id: str | None = None,
                              namespace_id: str | None = None) -> bool:
    if (not isinstance(value, dict) or set(value) != {"plan", "analysisResult"}
            or not valid_import_result_envelope(value.get("analysisResult"))
            or not _valid_plan_observation(value.get("plan"), value["analysisResult"])):
        return False
    envelope, plan = value["analysisResult"], value["plan"]
    if (envelope["kind"] != "analysis" or envelope["state"] != "PLAN_READY"
            or envelope["revision"] != plan["targetRevision"]):
        return False
    if plan_id is not None and plan["planId"] != plan_id:
        return False
    if namespace_id is not None and plan["namespaceId"] != namespace_id:
        return False
    return True


def _is_uuid(value: str) -> bool:
    try:
        return str(uuid.UUID(value)) == value
    except (TypeError, ValueError, AttributeError):
        return False
