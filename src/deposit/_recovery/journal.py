"""Versioned, DPAPI-protected journal for prepared Deposit actions."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
import os
import re
from pathlib import Path
import stat
import uuid
from contextlib import nullcontext
from typing import Any

from .identity import DeploymentIdentity
from .storage import ProtectedStorageError, ProtectedStore


FORMAT_VERSION = 1
RESULT_FORMAT_NAMED_ROWS_V1 = "named_rows_v1"
MAX_UNRESOLVED = 1024
MAX_RECORD_BYTES = 16 * 1024 * 1024
MAX_USER_BYTES = 1024 * 1024 * 1024
RESULT_RETENTION = timedelta(days=30)
UNRESOLVED_STATES = frozenset({"intent", "prepare_sent", "prepared", "executing", "outcome_saved", "release_sent", "conflicted", "failure_saved"})
PENDING_STATES = UNRESOLVED_STATES | {"failed"}
_TRANSITIONS = {
    "intent": {"prepare_sent", "prepared", "outcome_saved", "conflicted"},
    "prepare_sent": {"prepared", "executing", "outcome_saved", "conflicted"},
    "prepared": {"executing", "outcome_saved", "conflicted"},
    "executing": {"prepared", "outcome_saved", "conflicted", "failure_saved"},
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
    ) -> dict[str, Any]:
        if result_format is not None and (
            type(result_format) is not str or result_format != RESULT_FORMAT_NAMED_ROWS_V1
        ):
            raise JournalError("Recovery result format is unsupported")
        key = str(uuid.uuid4())
        now = _utc_now()
        record = {
            "format_version": FORMAT_VERSION,
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

    def update(self, identity: DeploymentIdentity, key: str, **changes: Any) -> dict[str, Any]:
        record = self.load(identity, key)
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
        self._validate(record, identity, key)
        self._write(record)
        return record

    def records(self, identity: DeploymentIdentity) -> list[dict[str, Any]]:
        prefix = f"op-{_endpoint_scope(identity)}-"
        result: list[dict[str, Any]] = []
        for name in self.storage.list_records():
            if not name.startswith(prefix):
                continue
            key = name[len(prefix):]
            result.append(self.load(identity, key))
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
            for record in self.records(identity):
                if record["state"] not in {"released", "failed"}:
                    continue
                try:
                    resolved = datetime.fromisoformat(record["resolved_at"])
                except (TypeError, ValueError) as exc:
                    raise JournalError("Resolved journal record has invalid retention metadata") from exc
                if resolved.tzinfo is None:
                    raise JournalError("Resolved journal record has invalid retention metadata")
                if resolved <= cutoff:
                    self.storage.delete(_record_name(identity, record["key"]))
                    removed += 1
        return removed

    def _write(self, record: dict[str, Any], *, creating: bool = False) -> None:
        try:
            payload = json.dumps(record, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise JournalError("Recovery arguments or result are not losslessly journalable") from exc
        if len(payload) > MAX_RECORD_BYTES:
            raise ProtectedStorageError("Recovery operation exceeds the 16 MiB record limit")
        identity = DeploymentIdentity.from_dict(record["identity"])
        name = _record_name(identity, record["key"])
        # The user quota coordinator makes the unresolved-count check global to
        # all registered recovery roots. The per-root index lock still serializes
        # updates/pruning in this journal; storage.write is reentrant under the
        # same per-user coordinator and includes the encrypted temporary copy.
        with self.storage.lock("journal-index"):
            self._prune_expired_locked(identity)
            quota_lock = getattr(self.storage, "user_quota_lock", None)
            with quota_lock() if quota_lock is not None else nullcontext():
                current_unresolved = self._unresolved_count_all_roots_locked()
                current_names = [n for n in self.storage.list_records() if n.startswith("op-")]
                exists = name in current_names
                if creating and exists:
                    raise JournalError("Recovery key collision")
                if creating and current_unresolved >= MAX_UNRESOLVED:
                    raise ProtectedStorageError("The 1,024 unresolved recovery operation limit is reached")
                self.storage.write(name, payload)

    def _unresolved_count_all_roots_locked(self) -> int:
        count = 0
        registered_roots = getattr(self.storage, "registered_roots", None)
        if registered_roots is None:
            roots = (None,)
        else:
            roots = registered_roots()
        for root in roots:
            if root is None:
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
                    validated = self._decode_record(contents, f"{name}.dpapi")
                except Exception as exc:
                    raise JournalError("An existing recovery journal record cannot be counted safely") from exc
                if validated["state"] in UNRESOLVED_STATES:
                    count += 1
        return count

    def _prune_expired_locked(self, identity: DeploymentIdentity) -> None:
        cutoff = datetime.now(timezone.utc) - RESULT_RETENTION
        for name in self.storage.list_records():
            if not name.startswith("op-"):
                continue
            try:
                record = self._decode_record(self.storage.read(name), name)
                if record["state"] not in {"released", "failed"}:
                    continue
                resolved = _aware_datetime(record["resolved_at"])
            except Exception as exc:
                raise JournalError("An existing recovery journal record cannot be pruned safely") from exc
            if resolved <= cutoff:
                self.storage.delete(name)

    def _decode_record(
        self,
        payload: bytes,
        name: str,
        *,
        identity: DeploymentIdentity | None = None,
    ) -> dict[str, Any]:
        """Strictly decode, validate, and bind one record to its protected filename."""
        raw = _strict_json_loads(payload)
        if not isinstance(raw, dict):
            raise JournalError("Recovery journal record is corrupt")
        try:
            identity_data = raw.get("identity")
            if not isinstance(identity_data, dict) or type(identity_data.get("schema_version")) is not int:
                raise ValueError("Invalid stored identity version")
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
            "format_version", "key", "public_id", "identity", "procedure", "arguments", "snapshot_revision",
            "state", "created_at", "updated_at", "outcome", "operation_id", "resolved_at",
        }
        allowed = required | {"result_format", "failure"}
        if not isinstance(record, dict) or not required <= set(record) or set(record) - allowed:
            raise JournalError("Recovery journal record has an unsupported format")
        if "result_format" in record and (
            type(record["result_format"]) is not str
            or record["result_format"] != RESULT_FORMAT_NAMED_ROWS_V1
        ):
            raise JournalError("Recovery journal record has an unsupported format")
        if (
            type(record["format_version"]) is not int
            or record["format_version"] != FORMAT_VERSION
            or not isinstance(record["key"], str)
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
        if "result_format" in record and record["outcome"] is not None:
            if not valid_named_rows_result(record["outcome"]):
                raise JournalError("Advanced recovery result is invalid")
        return record


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
