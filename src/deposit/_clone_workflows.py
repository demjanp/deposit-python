"""Protected ownership records for resumable server-scoped Namespace clones."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import re
import uuid

from ._recovery.identity import DeploymentIdentity
from ._recovery.storage import ProtectedStorageError


_ID = re.compile(r"[0-9a-f]{32}\Z")
_UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_STATES = frozenset({"SOURCE_REQUIRED", "FROZEN", "UPLOADING", "READY", "CLONE_PENDING", "COMMITTED", "FAILED"})
_ROLES = frozenset({"begin", "chunk", "finish", "clone"})


@dataclass(frozen=True, slots=True)
class CloneWorkflow:
    """Safe local reference for a resumable Namespace clone."""

    id: str
    state: str
    created_at: str


@dataclass(frozen=True, slots=True)
class CloneWorkflowStatus:
    """Side-effect-free local clone observation."""

    workflow: CloneWorkflow
    phase: str
    verified_offset: int
    total_bytes: int | None
    result_id: str | None
    cleanup_status: str
    result: object | None = None


class CloneWorkflowStore:
    """Small DPAPI-protected roots linking transfer spools to action journals."""

    def __init__(self, storage):
        self.storage = storage

    @staticmethod
    def _name(workflow_id):
        if not isinstance(workflow_id, str) or not _ID.fullmatch(workflow_id):
            raise ValueError("Clone workflow reference is invalid")
        return f"clone-workflow-{workflow_id}"

    def create(self, identity: DeploymentIdentity):
        if identity.namespace_id is not None:
            raise ValueError("Clone workflow identity must be server-scoped")
        workflow_id = uuid.uuid4().hex
        record = {
            "workflow_id": workflow_id,
            "created_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "identity": identity.to_dict(),
            "state": "SOURCE_REQUIRED",
            "phase": "source_required",
            "source_resource_id": None,
            "transfer_id": None,
            "sha256": None,
            "total_bytes": None,
            "verified_offset": 0,
            "upload_id": None,
            "outstanding_role": None,
            "outstanding_action_key": None,
            "clone_action_key": None,
            "result_id": None,
            "resolved_at": None,
            "failure": None,
            "cleanup_status": "owned",
        }
        self._save(record, creating=True)
        return self.reference(record)

    def get(self, workflow_id):
        from ._recovery.journal import _strict_json_loads
        try:
            with self.storage.lock("journal-index"):
                name = self._name(workflow_id)
                record = _strict_json_loads(self.storage.read(name))
                self._validate(record, workflow_id)
                return record
        except (KeyError, UnicodeError, ValueError, TypeError, ProtectedStorageError) as exc:
            raise ValueError("Clone workflow record is unavailable or corrupt") from exc

    def update(self, workflow_id, **changes):
        record = self.get(workflow_id)
        record.update(changes)
        self._save(record)
        return record

    def pending(self):
        from ._recovery.journal import _strict_json_loads
        records = []
        with self.storage.lock("journal-index"):
            for name in self.storage.iter_records():
                match = re.fullmatch(r"clone-workflow-([0-9a-f]{32})", name)
                if match:
                    record = _strict_json_loads(self.storage.read(name))
                    self._validate(record, match.group(1))
                    records.append(record)
        return tuple(self.reference(record) for record in sorted(records, key=lambda item: item["created_at"]))

    def lock(self, workflow_id):
        self._name(workflow_id)
        return self.storage.lock(f"clone-workflow-lock-{workflow_id}", timeout=0.0)

    def status(self, workflow_id, *, result=None):
        record = self.get(workflow_id)
        return CloneWorkflowStatus(
            self.reference(record), record["phase"], record["verified_offset"],
            record["total_bytes"], record["result_id"], record["cleanup_status"], result,
        )

    @staticmethod
    def reference(record):
        return CloneWorkflow(record["workflow_id"], record["state"], record["created_at"])

    def _save(self, record, *, creating=False):
        from ._recovery.journal import ProtectedJournal, _json_bytes
        self._validate(record, record.get("workflow_id"))
        with self.storage.lock("journal-index"):
            name = self._name(record["workflow_id"])
            if creating and self.storage.has_record(name):
                raise ProtectedStorageError("Clone workflow ID collision")
            payload = _json_bytes(record)
            try:
                ProtectedJournal(store=self.storage)._write_protected(name, payload)
            except Exception:
                try:
                    published = self.storage.read(name) == payload
                except Exception:
                    published = False
                if not published:
                    raise

    @staticmethod
    def _validate(record, workflow_id):
        required = {
            "workflow_id", "created_at", "identity", "state", "phase", "source_resource_id", "transfer_id", "sha256",
            "total_bytes", "verified_offset", "upload_id", "outstanding_role", "outstanding_action_key",
            "clone_action_key", "result_id", "resolved_at", "failure", "cleanup_status",
        }
        if (not isinstance(record, dict) or set(record) != required
                or record["workflow_id"] != workflow_id or not isinstance(workflow_id, str)
                or not _ID.fullmatch(workflow_id) or record["state"] not in _STATES
                or not isinstance(record["phase"], str) or not record["phase"]
                or record["cleanup_status"] not in {"owned", "pending", "complete"}
                or type(record["verified_offset"]) is not int or record["verified_offset"] < 0):
            raise ValueError("Clone workflow has an unsupported structure")
        created = datetime.fromisoformat(record["created_at"].replace("Z", "+00:00"))
        if created.tzinfo is None:
            raise ValueError("Clone workflow timestamp is invalid")
        identity = DeploymentIdentity.from_dict(record["identity"])
        if identity.namespace_id is not None:
            raise ValueError("Clone workflow identity must be server-scoped")
        for field in ("transfer_id",):
            if record[field] is not None and (not isinstance(record[field], str) or not _ID.fullmatch(record[field])):
                raise ValueError("Clone workflow transfer identity is invalid")
        for field in ("upload_id", "outstanding_action_key", "clone_action_key", "result_id"):
            if record[field] is not None and (not isinstance(record[field], str) or not _UUID.fullmatch(record[field])):
                raise ValueError("Clone workflow action or result identity is invalid")
        if record["source_resource_id"] is not None and (
                not isinstance(record["source_resource_id"], str) or not _UUID.fullmatch(record["source_resource_id"])):
            raise ValueError("Clone workflow resource identity is invalid")
        if record["sha256"] is not None and (not isinstance(record["sha256"], str) or not _SHA256.fullmatch(record["sha256"])):
            raise ValueError("Clone workflow digest is invalid")
        if record["total_bytes"] is not None and (type(record["total_bytes"]) is not int or record["total_bytes"] < 0):
            raise ValueError("Clone workflow size is invalid")
        if ((record["outstanding_role"] is None) != (record["outstanding_action_key"] is None)
                or record["outstanding_role"] is not None and record["outstanding_role"] not in _ROLES):
            raise ValueError("Clone workflow outstanding action is invalid")
        if record["transfer_id"] is None:
            if record["total_bytes"] is not None or record["upload_id"] is not None:
                raise ValueError("Clone workflow has detached transfer metadata")
            if record["state"] != "SOURCE_REQUIRED" and record["source_resource_id"] is None:
                raise ValueError("Clone workflow has no frozen package source")
        elif (record["state"] != "SOURCE_REQUIRED"
                and (record["sha256"] is None or record["total_bytes"] is None
                     or record["verified_offset"] > record["total_bytes"])):
            raise ValueError("Clone workflow frozen transfer metadata is incomplete")
        if (record["state"] == "SOURCE_REQUIRED" and record["sha256"] is not None
                or record["transfer_id"] is None and record["sha256"] is not None):
            raise ValueError("Clone workflow has an unbound source digest")
        if record["source_resource_id"] is not None and record["transfer_id"] is not None:
            raise ValueError("Clone workflow cannot bind two package sources")
        if record["state"] == "COMMITTED":
            if record["clone_action_key"] is None or record["result_id"] is None or record["resolved_at"] is None:
                raise ValueError("Committed clone workflow has no result binding")
        if record["resolved_at"] is not None:
            resolved = datetime.fromisoformat(record["resolved_at"].replace("Z", "+00:00"))
            if resolved.tzinfo is None:
                raise ValueError("Clone workflow resolution time is invalid")
        if record["failure"] is not None and not isinstance(record["failure"], dict):
            raise ValueError("Clone workflow failure metadata is invalid")
