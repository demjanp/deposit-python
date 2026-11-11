"""Durable client-side ownership records for composed Exchange imports.

The workflow record is deliberately metadata-only. Prepared actions, upload
spools, results and protected package bytes remain owned by their existing
subsystems and are linked here by opaque references.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import re
import uuid

from ._recovery.storage import ProtectedStorageError


_ID = re.compile(r"[0-9a-f]{32}\Z")
_STATES = frozenset({
    "CREATED", "SOURCE_EXPORT_REQUESTED", "SOURCE_REFERENCE_READY",
    "SOURCE_STAGING", "SOURCE_STAGED", "ROUTING_DECIDED", "TARGET_UPLOAD",
    "TARGET_PACKAGE_READY", "ANALYSIS_REQUESTED", "PLAN_READY",
    "APPLY_REQUESTED", "APPLYING", "APPLIED", "OUTCOME_UNCERTAIN",
    "ABANDON_REQUESTED", "ABANDON_PENDING", "ABANDONED", "FAILED", "EXPIRED", "CANCELLED",
})


@dataclass(frozen=True)
class ImportWorkflow:
    """Safe public reference to durable import work."""
    id: str
    state: str
    created_at: str
    source_namespace_id: str | None
    target_namespace_id: str | None
    required_endpoints: tuple[str, ...]


@dataclass(frozen=True)
class ImportWorkflowStatus:
    """Side-effect-free local observation of an import workflow."""
    workflow: ImportWorkflow
    phase: str
    result: object | None = None
    cleanup_status: str = "owned"
    action_status: str = "none"


class ImportWorkflowStore:
    """Small durable journal using the protected Store already owned by Client."""

    def __init__(self, storage):
        self.storage = storage

    @staticmethod
    def _name(workflow_id):
        if not isinstance(workflow_id, str) or not _ID.fullmatch(workflow_id):
            raise ValueError("Import workflow reference is invalid")
        return f"import-workflow-{workflow_id}"

    def create(self, *, source=None, target=None):
        workflow_id = uuid.uuid4().hex
        endpoints = tuple(sorted({x for x in (_endpoint(source), _endpoint(target)) if x}))
        record = {
            "workflow_id": workflow_id,
            "created_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "state": "CREATED", "phase": "created",
            "source_namespace_id": getattr(source, "namespace_id", None),
            "target_namespace_id": getattr(target, "namespace_id", None),
            "source_identity": _identity(source), "target_identity": _identity(target),
            "required_endpoints": list(endpoints),
            "action_keys": {}, "upload_id": None, "package_resource_id": None,
            "plan_id": None, "result_action_key": None, "apply_requested": False,
            "failure": None, "source_export_arguments": None, "analysis_arguments": None,
            "package_stage_id": None, "source_stage_id": None, "routing_decision": None,
            "plan_expires_at": None, "frozen_rules": {}, "source_export_result": None, "resume_state": None, "cleanup_status": "owned",
        }
        self._save(record, creating=True)
        return self.reference(record)

    def get(self, workflow_id):
        from ._recovery.journal import ProtectedJournal, JournalError
        try:
            with self.storage.lock("journal-index"):
                name = self._name(workflow_id)
                root = self.decode_root(self.storage.read(name), name)
                record = ProtectedJournal(store=self.storage)._read_segmented_payload(name, root["payload"])
                self._validate(record, workflow_id)
                if self._header(record) != {key: value for key, value in root.items() if key != "payload"}:
                    raise ValueError("Workflow root and payload disagree")
                return record
        except (KeyError, UnicodeError, ValueError, TypeError, ProtectedStorageError, JournalError) as exc:
            raise ValueError("Import workflow record is unavailable or corrupt") from exc

    def update(self, workflow_id, **changes):
        record = self.get(workflow_id)
        record.update(changes)
        self._save(record)
        return record

    def pending(self):
        records = []
        for name in self.storage.list_records():
            match = re.fullmatch(r"import-workflow-([0-9a-f]{32})", name)
            if match:
                records.append(self.decode_root(self.storage.read(name), name))
        # Retained terminal references remain discoverable after restart without
        # authenticating an endpoint. The public state distinguishes pending work.
        return tuple(self.reference(r) for r in sorted(records, key=lambda r: r["created_at"]))

    def matching_plan(self, plan_id):
        records = []
        for name in self.storage.list_records():
            match = re.fullmatch(r"import-workflow-([0-9a-f]{32})", name)
            if match:
                root = self.decode_root(self.storage.read(name), name)
                if root["plan_id"] == plan_id:
                    records.append(self.get(match.group(1)))
        return tuple(sorted(records, key=lambda record: record["created_at"]))

    def lock(self, workflow_id):
        return self.storage.lock(f"import-workflow-lock-{workflow_id}", timeout=0.0)

    def status(self, workflow_id, *, result=None):
        record = self.get(workflow_id)
        action = "pending" if record["state"] in {"SOURCE_EXPORT_REQUESTED", "TARGET_UPLOAD", "ANALYSIS_REQUESTED", "APPLY_REQUESTED",
                                                                  "OUTCOME_UNCERTAIN", "APPLYING", "ABANDON_PENDING"} else "none"
        return ImportWorkflowStatus(self.reference(record), record["phase"], result,
                                    record["cleanup_status"], action)

    @staticmethod
    def reference(record):
        return ImportWorkflow(record["workflow_id"], record["state"], record["created_at"],
                              record["source_namespace_id"], record["target_namespace_id"],
                              tuple(record["required_endpoints"]))

    @staticmethod
    def _header(record):
        fields = ("workflow_id", "created_at", "state", "source_namespace_id", "target_namespace_id",
                  "source_identity", "target_identity", "required_endpoints", "cleanup_status", "plan_id")
        header = {name: record[name] for name in fields}
        # Fixed internal action roles, not caller input cardinality. Selector and
        # rule values live only in the segmented immutable payload.
        source = [record["action_keys"]["export"]] if "export" in record["action_keys"] else []
        target = {key for role, key in record["action_keys"].items() if role not in {"export", "upload_transfer"}}
        if record["result_action_key"] is not None:
            target.add(record["result_action_key"])
        header["dependencies"] = {"source": source, "target": sorted(target)}
        return header

    @staticmethod
    def decode_root(payload, name):
        from ._recovery.journal import _strict_json_loads, _validate_payload_descriptor, _is_uuid
        from ._recovery.identity import DeploymentIdentity
        root = _strict_json_loads(payload)
        expected = {"workflow_id", "created_at", "state", "source_namespace_id", "target_namespace_id",
                    "source_identity", "target_identity", "required_endpoints", "cleanup_status", "plan_id",
                    "dependencies", "payload"}
        if (not isinstance(root, dict) or set(root) != expected
                or name != ImportWorkflowStore._name(root.get("workflow_id"))
                or root["state"] not in _STATES
                or root["cleanup_status"] not in {"owned", "pending", "complete"}
                or not isinstance(root["dependencies"], dict) or set(root["dependencies"]) != {"source", "target"}
                or not isinstance(root["required_endpoints"], list)):
            raise ValueError("Import workflow root is invalid")
        created = datetime.fromisoformat(root["created_at"])
        if created.tzinfo is None:
            raise ValueError("Import workflow root timestamp is invalid")
        for side in ("source", "target"):
            identity = root[side + "_identity"]
            keys = root["dependencies"][side]
            if not isinstance(keys, list) or any(not _is_uuid(key) for key in keys) or len(set(keys)) != len(keys):
                raise ValueError("Import workflow root dependencies are invalid")
            if identity is not None:
                binding = DeploymentIdentity.from_dict(identity)
                if binding.namespace_id != root[side + "_namespace_id"]:
                    raise ValueError("Import workflow root Namespace binding is invalid")
            elif keys or root[side + "_namespace_id"] is not None:
                raise ValueError("Import workflow dependency identity is unavailable")
        endpoints = sorted({binding["endpoint"] for binding in (root["source_identity"], root["target_identity"]) if binding})
        if root["required_endpoints"] != endpoints:
            raise ValueError("Import workflow endpoint binding is invalid")
        _validate_payload_descriptor(root["payload"], "workflow")
        return root

    def _save(self, record, *, creating=False):
        from ._recovery.journal import ProtectedJournal, _json_bytes
        self._validate(record, record.get("workflow_id"))
        journal = ProtectedJournal(store=self.storage)
        with self.storage.lock("journal-index"):
            name = self._name(record["workflow_id"])
            if creating and self.storage.has_record(name):
                raise ProtectedStorageError("Import workflow ID collision")
            descriptor = journal._write_segmented_payload(name, "workflow", record)
            payload = _json_bytes(dict(self._header(record), payload=descriptor))
            self.decode_root(payload, name)
            try:
                journal._write_protected(name, payload)
            except Exception:
                # Root replacement can commit before acknowledgement/accounting.
                # Only the exact authenticated intended root proves publication.
                try:
                    published = self.storage.read(name) == payload
                except Exception:
                    published = False
                if not published:
                    raise
            journal._cleanup_workflow_payloads_locked()

    def cleanup_payloads(self):
        from ._recovery.journal import ProtectedJournal
        with self.storage.lock("journal-index"):
            ProtectedJournal(store=self.storage)._cleanup_workflow_payloads_locked()

    @staticmethod
    def _validate(record, workflow_id):
        required = {"workflow_id", "created_at", "state", "phase", "source_namespace_id",
                    "target_namespace_id", "source_identity", "target_identity", "required_endpoints",
                    "action_keys", "upload_id", "package_resource_id", "plan_id",
                    "result_action_key", "apply_requested", "failure", "source_export_arguments",
                    "analysis_arguments", "package_stage_id", "source_stage_id", "routing_decision",
                    "plan_expires_at", "frozen_rules", "source_export_result", "resume_state", "cleanup_status"}
        if (not isinstance(record, dict) or set(record) != required or record["workflow_id"] != workflow_id
                or not isinstance(workflow_id, str) or not _ID.fullmatch(workflow_id)
                or record["state"] not in _STATES or not isinstance(record["phase"], str)
                or not isinstance(record["required_endpoints"], list)
                or not isinstance(record["action_keys"], dict)
                or type(record["apply_requested"]) is not bool):
            raise ValueError("Import workflow record has an unsupported structure")
        from ._recovery.journal import _is_uuid
        roles = {"export", "analysis", "direct_analysis", "apply", "upload_begin", "upload_finish", "upload_abort", "upload_chunk", "upload_transfer"}
        if set(record["action_keys"]) - roles or any(
                not _is_uuid(key) for role, key in record["action_keys"].items() if role != "upload_transfer"):
            raise ValueError("Import workflow action references are invalid")
        if record["state"] == "PLAN_READY" and record["apply_requested"]:
            raise ValueError("PLAN_READY cannot contain apply intent")


def _identity(store):
    identity = getattr(store, "_identity", None)
    if identity is None:
        return None
    return identity.to_dict()


def _endpoint(store):
    identity = getattr(store, "_identity", None)
    if identity is None:
        return None
    # Do not infer endpoint identity from native database identifiers.
    endpoint = getattr(identity, "endpoint", None)
    return endpoint if isinstance(endpoint, str) and endpoint else None
