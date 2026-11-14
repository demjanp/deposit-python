"""Protected, chunked local input for restartable resource uploads."""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from contextlib import nullcontext
from typing import Any, BinaryIO

from .identity import DeploymentIdentity
from .journal import JournalError, MAX_RECORD_BYTES, ProtectedJournal, _is_uuid, _valid_failure, _strict_json_loads
from .storage import ProtectedStorageError, StorageAdmissionCoordinator


TRANSFER_CHUNK_BYTES = 1024 * 1024
_TRANSFER_ID = re.compile(r"^[0-9a-f]{32}$")
_MANIFEST_FIELDS = {
    "transfer_id", "identity", "filename", "media_type", "bytes", "sha256",
    "chunk_count", "admission_id", "staging_pending_length", "staging_pending_sha256",
    "state", "offset", "upload_id", "begin_action_key", "pending_action_key",
    "pending_offset", "pending_length", "finish_action_key", "failed_action_key", "failure",
}


class TransferError(RuntimeError):
    """A protected upload spool cannot safely be used or reconciled."""


class ProtectedTransferSpool:
    """Store upload bytes as protected records beside a Store's action journal.

    Every chunk remains below the existing 16 MiB protected-record cap. Chunk
    metadata is implicit in fixed-size content records, so operation size is
    not bounded by a single manifest record.
    """

    def __init__(self, journal: ProtectedJournal, identity: DeploymentIdentity):
        self._storage = journal.storage
        self._identity = identity

    def freeze(
        self,
        source: BinaryIO,
        *,
        filename: str,
        media_type: str,
        transfer_id: str | None = None,
    ) -> dict[str, Any]:
        """Consume a binary source once and durably freeze it before any action."""
        if not isinstance(filename, str) or not filename or len(filename) > 255:
            raise ValueError("resource filename must be a nonempty string of at most 255 characters")
        if not isinstance(media_type, str) or not media_type:
            raise ValueError("resource media type must be a nonempty string")
        if not callable(getattr(source, "read", None)):
            raise TypeError("resource source must be a readable binary stream")

        transfer_id = uuid.uuid4().hex if transfer_id is None else transfer_id
        if not isinstance(transfer_id, str) or not _TRANSFER_ID.fullmatch(transfer_id):
            raise ValueError("Transfer reference is invalid")
        admission = StorageAdmissionCoordinator(self._storage)
        reservation = admission.acquire(transfer_id, "protected-upload-spool", {str(self._storage.root): 0})
        digest = hashlib.sha256()
        manifest = {
            "transfer_id": transfer_id,
            "identity": self._identity.to_dict(), "filename": filename, "media_type": media_type,
            "bytes": 0, "sha256": digest.hexdigest(), "chunk_count": 0,
            "admission_id": reservation["id"], "staging_pending_length": 0,
            "staging_pending_sha256": None,
            "state": "staging", "offset": 0, "upload_id": None,
            "begin_action_key": None, "pending_action_key": None,
            "pending_offset": None, "pending_length": None, "finish_action_key": None,
            "failed_action_key": None, "failure": None,
        }
        with admission.writer(reservation):
            self._save(manifest, creating=True)
            total = 0
            pending = bytearray()
            try:
                while True:
                    request_size = TRANSFER_CHUNK_BYTES - len(pending)
                    part = source.read(request_size)
                    if part is None:
                        raise TypeError("resource stream returned None instead of bytes")
                    if not isinstance(part, bytes):
                        raise TypeError("resource stream must return bytes")
                    if len(part) > request_size:
                        raise ValueError("resource stream returned more bytes than requested")
                    if not part:
                        if pending:
                            block = bytes(pending)
                            self._write_staging_chunk(admission, reservation, manifest, block, digest)
                            total += len(block)
                        break
                    pending.extend(part)
                    if len(pending) == TRANSFER_CHUNK_BYTES:
                        block = bytes(pending)
                        self._write_staging_chunk(admission, reservation, manifest, block, digest)
                        total += len(block)
                        pending.clear()
                if total != manifest["bytes"]:
                    raise TransferError("Protected transfer checkpoint disagrees with consumed input")
                manifest.update(state="staged", staging_pending_length=0, staging_pending_sha256=None)
                self._save(manifest)
                admission.close(reservation)
                return manifest
            except Exception:
                # The manifest records any write intent. Retain owned chunks
                # and commitments for restart diagnosis; never retry from a new
                # caller stream under the old transfer identity.
                raise

    @staticmethod
    def _clone_intent_name(workflow_id: str) -> str:
        if not isinstance(workflow_id, str) or not _TRANSFER_ID.fullmatch(workflow_id):
            raise ValueError("Clone workflow reference is invalid")
        return f"clone-source-{workflow_id}-transfer"

    def reserve_clone_source(self, workflow_id: str) -> dict[str, Any]:
        """Own one clone input before the caller's first read."""
        from .journal import _strict_json_loads
        with self._storage.lock(self._clone_intent_name(workflow_id)):
            if self._storage.has_record(self._clone_intent_name(workflow_id)):
                return self.read_clone_intent(workflow_id)
            record = {
                "workflow_id": workflow_id,
                "transfer_id": uuid.uuid4().hex,
                "identity": self._identity.to_dict(),
            }
            payload = json.dumps(record, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
            ProtectedJournal(store=self._storage)._write_protected(
                self._clone_intent_name(workflow_id), payload)
            return record

    def read_clone_intent(self, workflow_id: str) -> dict[str, Any]:
        """Decode only the exact preallocated clone owner, not arbitrary spools."""
        try:
            record = _strict_json_loads(self._storage.read(self._clone_intent_name(workflow_id)))
            if (not isinstance(record, dict) or set(record) != {"workflow_id", "transfer_id", "identity"}
                    or record["workflow_id"] != workflow_id
                    or not isinstance(record["transfer_id"], str)
                    or not _TRANSFER_ID.fullmatch(record["transfer_id"])
                    or DeploymentIdentity.from_dict(record["identity"]) != self._identity):
                raise TransferError("Clone source reservation does not belong to this deployment")
            return record
        except TransferError:
            raise
        except Exception as exc:
            raise TransferError("Clone source reservation is unavailable or corrupt") from exc

    def _write_staging_chunk(self, admission, reservation, manifest, block: bytes, digest) -> None:
        index = manifest["chunk_count"]
        checksum = hashlib.sha256(block).hexdigest()
        data_name = self._chunk_name(manifest["transfer_id"], index)
        meta_name = self._chunk_meta_name(manifest["transfer_id"], index)
        estimate = len(block) + 2 * 65536
        admission.grow(reservation, {str(self._storage.root): estimate})
        manifest["staging_pending_length"] = len(block)
        manifest["staging_pending_sha256"] = checksum
        self._save(manifest)
        self._storage.write(data_name, block)
        meta = json.dumps({"index": index, "bytes": len(block), "sha256": checksum},
                          sort_keys=True, separators=(",", ":")).encode("utf-8")
        self._storage.write(meta_name, meta)
        actual = self._storage.record_size(data_name) + self._storage.record_size(meta_name)
        admission.materialized(reservation, self._storage.root, actual)
        admission.release(reservation, {str(self._storage.root): estimate - actual})
        digest.update(block)
        manifest.update(bytes=manifest["bytes"] + len(block), sha256=digest.hexdigest(),
                        chunk_count=index + 1, staging_pending_length=0,
                        staging_pending_sha256=None)
        self._save(manifest)

    def load(self, transfer_id: str) -> dict[str, Any]:
        if not isinstance(transfer_id, str) or not _TRANSFER_ID.fullmatch(transfer_id):
            raise TransferError("Transfer reference is invalid")
        try:
            raw = self._storage.read(self._manifest_name(transfer_id))
            manifest = _strict_json_loads(raw)
            self._validate(manifest, transfer_id)
            return manifest
        except TransferError:
            raise
        except (KeyError, UnicodeError, ValueError, TypeError, ProtectedStorageError) as exc:
            raise TransferError("Protected transfer manifest is unavailable or corrupt") from exc

    def update(self, transfer_id: str, *, _locked: bool = False, **changes: Any) -> dict[str, Any]:
        if set(changes) - _MANIFEST_FIELDS:
            raise TransferError("Transfer manifest update contains unsupported fields")
        lock = self._storage.lock(self._lock_name(transfer_id), timeout=0.0)
        with (nullcontext() if _locked else lock):
            manifest = self.load(transfer_id)
            if manifest["state"] == "failed":
                raise TransferError("Failed transfer manifest is immutable")
            manifest.update(changes)
            self._save(manifest)
            return manifest

    def read_chunk(self, transfer_id: str, index: int) -> bytes:
        manifest = self.load(transfer_id)
        if type(index) is not int or not 0 <= index < self.chunk_count(manifest):
            raise TransferError("Transfer chunk index is invalid")
        entry = self.chunk_metadata(manifest, index)
        try:
            block = self._storage.read(entry["name"])
        except (KeyError, ProtectedStorageError) as exc:
            raise TransferError("Protected upload spool is unavailable") from exc
        if (len(block) != entry["bytes"]
                or hashlib.sha256(block).hexdigest() != entry["sha256"]):
            raise TransferError("Protected upload spool failed its checksum")
        return block

    @staticmethod
    def chunk_count(manifest: dict[str, Any]) -> int:
        return manifest["chunk_count"]

    @staticmethod
    def chunk_length(manifest: dict[str, Any], index: int) -> int:
        remaining = manifest["bytes"] - index * TRANSFER_CHUNK_BYTES
        return min(TRANSFER_CHUNK_BYTES, remaining)

    def chunk_metadata(self, manifest: dict[str, Any], index: int) -> dict[str, Any]:
        if type(index) is not int or not 0 <= index < manifest["chunk_count"]:
            raise TransferError("Protected transfer chunk index is invalid")
        try:
            metadata = _strict_json_loads(self._storage.read(self._chunk_meta_name(manifest["transfer_id"], index)))
        except Exception as exc:
            raise TransferError("Protected upload chunk metadata is unavailable") from exc
        expected_size = self.chunk_length(manifest, index)
        if (not isinstance(metadata, dict) or set(metadata) != {"index", "bytes", "sha256"}
                or metadata["index"] != index or metadata["bytes"] != expected_size
                or not isinstance(metadata["sha256"], str)
                or not re.fullmatch(r"[0-9a-f]{64}", metadata["sha256"])):
            raise TransferError("Protected upload chunk metadata is invalid")
        return {"name": self._chunk_name(manifest["transfer_id"], index), **metadata}

    def verify_content(self, manifest: dict[str, Any]) -> None:
        """Verify every protected chunk against the immutable whole-file digest."""
        digest = hashlib.sha256()
        total = 0
        for index in range(self.chunk_count(manifest)):
            block = self.read_chunk(manifest["transfer_id"], index)
            digest.update(block)
            total += len(block)
        if total != manifest["bytes"] or digest.hexdigest() != manifest["sha256"]:
            raise TransferError("Protected upload spool failed its whole-file checksum")

    def ensure_action_capacity(self, manifest: dict[str, Any]) -> None:
        """Check only per-action representability; disk admission is incremental."""
        for index in range(self.chunk_count(manifest)):
            encoded_bytes = 4 * ((self.chunk_metadata(manifest, index)["bytes"] + 2) // 3)
            if encoded_bytes + 8192 > MAX_RECORD_BYTES:
                raise ProtectedStorageError("Upload chunk action exceeds the 16 MiB recovery record limit")

    def pending(self) -> tuple[dict[str, Any], ...]:
        """Find this deployment's unfinished manifests without exposing keys."""
        result = []
        for name in self._storage.list_records():
            match = re.fullmatch(r"transfer-([0-9a-f]{32})-manifest", name)
            if match is None:
                continue
            try:
                raw = self._storage.read(name)
                manifest = _strict_json_loads(raw)
                identity_data = manifest.get("identity") if isinstance(manifest, dict) else None
                if not isinstance(identity_data, dict):
                    raise TransferError("Protected transfer manifest has an invalid identity")
                identity = DeploymentIdentity.from_dict(identity_data)
                if not identity.matches(self._identity):
                    continue
                self._validate(manifest, match.group(1))
                if manifest["state"] not in {"failed", "staging"}:
                    result.append(manifest)
            except TransferError:
                raise
            except Exception as exc:
                raise TransferError("Protected transfer manifest is unavailable or corrupt") from exc
        return tuple(sorted(result, key=lambda item: item["transfer_id"]))

    def incomplete_staging(self) -> tuple[dict[str, Any], ...]:
        """List frozen-input intents requiring explicit inspection.

        Interrupted caller streams are never replaced with new stream bytes
        during automatic upload recovery.
        """
        result = []
        for name in self._storage.list_records():
            match = re.fullmatch(r"transfer-([0-9a-f]{32})-manifest", name)
            if match is None:
                continue
            try:
                raw = _strict_json_loads(self._storage.read(name))
                identity = DeploymentIdentity.from_dict(raw.get("identity", {}))
                if identity.matches(self._identity):
                    self._validate(raw, match.group(1))
                    if raw["state"] == "staging":
                        result.append(raw)
            except Exception as exc:
                raise TransferError("Protected transfer manifest is unavailable or corrupt") from exc
        return tuple(sorted(result, key=lambda item: item["transfer_id"]))

    def cleanup_resolved(self, transfer_id: str, *, _locked: bool = False) -> None:
        """Remove spool records only after the manifest is durably resolved."""
        lock = self._storage.lock(self._lock_name(transfer_id), timeout=0.0)
        with (nullcontext() if _locked else lock):
            manifest = self.load(transfer_id)
            if manifest["state"] != "finished":
                raise TransferError("Unresolved upload spool cannot be removed")
            with self._storage.lock("journal-index"):
                # Keep both admission and manifest until every payload chunk is gone.
                # If a delete fails, retry still owns the disk bytes and has its index.
                for index in range(self.chunk_count(manifest)):
                    self._storage.delete(self._chunk_name(transfer_id, index))
                if manifest.get("admission_id"):
                    self._storage.delete(f"admission-{manifest['admission_id']}")
                self._storage.delete(self._manifest_name(transfer_id))

    def _save(self, manifest: dict[str, Any], *, creating: bool = False) -> None:
        self._validate(manifest, manifest.get("transfer_id"))
        payload = json.dumps(manifest, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
        if len(payload) > MAX_RECORD_BYTES:
            raise ProtectedStorageError("Transfer manifest exceeds the 16 MiB record limit")
        name = self._manifest_name(manifest["transfer_id"])
        # Share the journal pruning boundary: dependency changes and snapshot /
        # deletion are atomic, without journal-index -> transfer-lock inversion.
        with self._storage.lock("journal-index"):
            if creating and name in self._storage.list_records():
                raise JournalError("Transfer reference collision")
            self._storage.write(name, payload)

    def _validate(self, manifest: Any, transfer_id: Any) -> None:
        validate_transfer_manifest(manifest, transfer_id, expected_identity=self._identity)

    @staticmethod
    def validate_manifest(manifest: Any, transfer_id: Any) -> dict[str, Any]:
        return validate_transfer_manifest(manifest, transfer_id)

    @staticmethod
    def _unique_pairs(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("Duplicate transfer manifest field")
            result[key] = value
        return result

    @staticmethod
    def _manifest_name(transfer_id: str) -> str:
        return f"transfer-{transfer_id}-manifest"

    @staticmethod
    def _chunk_name(transfer_id: str, index: int) -> str:
        return f"transfer-{transfer_id}-chunk-{index:08d}"

    @staticmethod
    def _chunk_meta_name(transfer_id: str, index: int) -> str:
        return f"transfer-{transfer_id}-meta-{index:08d}"

    @staticmethod
    def _lock_name(transfer_id: str) -> str:
        if not isinstance(transfer_id, str) or not _TRANSFER_ID.fullmatch(transfer_id):
            raise TransferError("Transfer reference is invalid")
        return f"transfer-{transfer_id}"


def validate_transfer_manifest(manifest: Any, transfer_id: Any,
                               *, expected_identity: DeploymentIdentity | None = None) -> dict[str, Any]:
    if (not isinstance(manifest, dict) or set(manifest) != _MANIFEST_FIELDS
            or manifest.get("transfer_id") != transfer_id
            or not isinstance(transfer_id, str) or not _TRANSFER_ID.fullmatch(transfer_id)
            or not isinstance(manifest.get("identity"), dict)
            or not isinstance(manifest.get("filename"), str) or not manifest["filename"]
            or not isinstance(manifest.get("media_type"), str) or not manifest["media_type"]
            or type(manifest.get("bytes")) is not int or manifest["bytes"] < 0
            or not isinstance(manifest.get("sha256"), str)
            or not re.fullmatch(r"[0-9a-f]{64}", manifest["sha256"])
            or manifest.get("state") not in {"staging", "staged", "uploading", "finished", "failed"}
            or type(manifest.get("offset")) is not int
            or not 0 <= manifest["offset"] <= manifest["bytes"]):
        raise TransferError("Protected transfer manifest has an unsupported structure")
    identity = DeploymentIdentity.from_dict(manifest["identity"])
    if expected_identity is not None and not identity.matches(expected_identity):
        raise TransferError("Protected transfer manifest identity does not match this Store")
    failed_key, failure = manifest["failed_action_key"], manifest["failure"]
    if manifest["state"] == "failed":
        references = {manifest["begin_action_key"], manifest["pending_action_key"], manifest["finish_action_key"]}
        if not _is_uuid(failed_key) or failed_key not in references or not _valid_failure(failure):
            raise TransferError("Failed transfer manifest has invalid failure metadata")
    elif failed_key is not None or failure is not None:
        raise TransferError("Active transfer manifest has terminal failure metadata")
    count = manifest["chunk_count"]
    pending_length = manifest["staging_pending_length"]
    pending_digest = manifest["staging_pending_sha256"]
    if (type(count) is not int or count < 0
            or count != (manifest["bytes"] + TRANSFER_CHUNK_BYTES - 1) // TRANSFER_CHUNK_BYTES
            or not isinstance(manifest["admission_id"], str)
            or not re.fullmatch(r"[0-9a-f]{32}", manifest["admission_id"])
            or type(pending_length) is not int or not 0 <= pending_length <= TRANSFER_CHUNK_BYTES
            or ((pending_length == 0) != (pending_digest is None))
            or (pending_digest is not None and (not isinstance(pending_digest, str)
                or not re.fullmatch(r"[0-9a-f]{64}", pending_digest)))
            or (manifest["state"] in {"staged", "uploading", "finished"} and pending_length != 0)
            or (manifest["state"] == "staging" and any(manifest[field] is not None for field in
                ("upload_id", "begin_action_key", "pending_action_key", "pending_offset", "finish_action_key")))):
        raise TransferError("Protected transfer manifest has invalid segmented chunk metadata")
    for field in ("upload_id", "begin_action_key", "pending_action_key"):
        value = manifest.get(field)
        if value is not None and (not isinstance(value, str) or not value):
            raise TransferError("Protected transfer manifest has an invalid action reference")
    finish_key = manifest.get("finish_action_key")
    if finish_key is not None and (not isinstance(finish_key, str) or not finish_key):
        raise TransferError("Protected transfer manifest has an invalid finish action reference")
    if manifest["state"] == "finished" and finish_key is None:
        raise TransferError("Finished transfer manifest has no finish action reference")
    for field in ("pending_offset", "pending_length"):
        value = manifest.get(field)
        if value is not None and (type(value) is not int or value < 0):
            raise TransferError("Protected transfer manifest has invalid pending chunk metadata")
    pending = (manifest.get("pending_action_key"), manifest.get("pending_offset"), manifest.get("pending_length"))
    if any(value is not None for value in pending) and not all(value is not None for value in pending):
        raise TransferError("Protected transfer manifest has incomplete pending chunk metadata")
    if pending[1] is not None and (pending[2] == 0 or pending[1] + pending[2] > manifest["bytes"]):
        raise TransferError("Protected transfer manifest pending chunk is outside the frozen content")
    if manifest["state"] == "uploading" and (manifest["upload_id"] is None or manifest["begin_action_key"] is None):
        raise TransferError("Uploading transfer manifest has no server identity or begin action")
    if manifest["state"] == "finished" and manifest["offset"] != manifest["bytes"]:
        raise TransferError("Finished transfer manifest has an incomplete offset")
    return manifest


def decode_transfer_manifest(payload: bytes | str, transfer_id: Any) -> dict[str, Any]:
    manifest = _strict_json_loads(payload)
    return validate_transfer_manifest(manifest, transfer_id)


class ResourceReader:
    """Small checked binary reader over the server's chunk-download route."""

    def __init__(self, store, resource_id: str, metadata: dict[str, Any]):
        self._store = store
        self._resource_id = resource_id
        self._size = metadata.get("bytes")
        self._expected = metadata.get("checksum")
        self._chunk_size = TRANSFER_CHUNK_BYTES
        self._offset = 0
        self._buffer = bytearray()
        self._digest = hashlib.sha256()
        self._closed = False
        self._complete = False
        if type(self._size) is not int or self._size < 0 or not isinstance(self._expected, str) or not re.fullmatch(r"[0-9a-f]{64}", self._expected):
            raise TransferError("Resource metadata has invalid size or checksum")
        if self._size == 0:
            self._verify_complete()

    @property
    def closed(self) -> bool:
        return self._closed

    def readable(self) -> bool:
        return not self._closed

    def read(self, size: int = -1) -> bytes:
        if self._closed:
            raise ValueError("I/O operation on closed resource reader")
        if type(size) is not int:
            raise TypeError("read size must be an integer")
        if size == 0:
            return b""
        while not self._complete and (size < 0 or len(self._buffer) < size):
            self._read_chunk()
        if size < 0:
            result = bytes(self._buffer)
            self._buffer.clear()
            return result
        result = bytes(self._buffer[:size])
        del self._buffer[:size]
        return result

    def readinto(self, buffer) -> int:
        view = memoryview(buffer).cast("B")
        data = self.read(len(view))
        view[:len(data)] = data
        return len(data)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if not self._complete:
            raise TransferError("Resource download was closed before checksum verification completed")

    def __enter__(self):
        if self._closed:
            raise ValueError("I/O operation on closed resource reader")
        return self

    def __exit__(self, exc_type, exc, traceback):
        if exc_type is None:
            self.close()
            return False
        try:
            self.close()
        except TransferError as close_error:
            if str(close_error) != "Resource download was closed before checksum verification completed":
                raise
        return False

    def _read_chunk(self) -> None:
        remaining = self._size - self._offset
        if remaining <= 0:
            self._verify_complete()
            return
        requested = min(self._chunk_size, remaining)
        data = self._store._call_one("deposit.resource.downloadChunk", self._resource_id, self._offset, requested)
        if not isinstance(data, dict):
            raise TransferError("Resource download returned invalid chunk metadata")
        try:
            block = __import__("base64").b64decode(data["bytesBase64"], validate=True)
        except Exception as exc:
            raise TransferError("Resource download returned invalid Base64") from exc
        next_offset = self._offset + len(block)
        if (
            data.get("offset") != self._offset
            or data.get("nextOffset") != next_offset
            or not block
            or len(block) > requested
            or data.get("eof") is not (next_offset == self._size)
            or hashlib.sha256(block).hexdigest() != data.get("chunkSha256")
        ):
            raise TransferError("Resource download chunk failed offset or checksum validation")
        self._digest.update(block)
        self._buffer.extend(block)
        self._offset = next_offset
        if self._offset == self._size:
            self._verify_complete()

    def _verify_complete(self) -> None:
        if self._digest.hexdigest() != self._expected:
            raise TransferError("Resource download failed its whole-file checksum")
        self._complete = True
