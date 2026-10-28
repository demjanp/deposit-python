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
from .storage import ProtectedStorageError


TRANSFER_CHUNK_BYTES = 1024 * 1024
_TRANSFER_ID = re.compile(r"^[0-9a-f]{32}$")
_MANIFEST_FIELDS = {
    "format_version", "transfer_id", "identity", "filename", "media_type", "bytes",
    "sha256", "chunks", "state", "offset", "upload_id", "begin_action_key",
    "pending_action_key", "pending_offset", "pending_length", "finish_action_key",
}

_MANIFEST_V2_FIELDS = _MANIFEST_FIELDS | {"failed_action_key", "failure"}


class TransferError(RuntimeError):
    """A protected upload spool cannot safely be used or reconciled."""


class ProtectedTransferSpool:
    """Store upload bytes as protected records beside a Store's action journal.

    Every chunk remains below the existing 16 MiB protected-record cap. Writes
    use the same ProtectedStore, per-user quota index, ACL and DPAPI backend as
    prepared actions, so the existing 1 GiB aggregate quota applies unchanged.
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
    ) -> dict[str, Any]:
        """Consume a binary source once and durably freeze it before any action."""
        if not isinstance(filename, str) or not filename or len(filename) > 255:
            raise ValueError("resource filename must be a nonempty string of at most 255 characters")
        if not isinstance(media_type, str) or not media_type:
            raise ValueError("resource media type must be a nonempty string")
        if not callable(getattr(source, "read", None)):
            raise TypeError("resource source must be a readable binary stream")

        transfer_id = uuid.uuid4().hex
        chunks: list[dict[str, Any]] = []
        digest = hashlib.sha256()
        total = 0
        try:
            while True:
                block = source.read(TRANSFER_CHUNK_BYTES)
                if block in (b"", None):
                    if block is None:
                        raise TypeError("resource stream returned None instead of bytes")
                    break
                if not isinstance(block, bytes):
                    raise TypeError("resource stream must return bytes")
                if len(block) > TRANSFER_CHUNK_BYTES:
                    raise ValueError("resource stream returned more bytes than requested")
                name = self._chunk_name(transfer_id, len(chunks))
                self._storage.write(name, block)
                chunks.append({"name": name, "bytes": len(block), "sha256": hashlib.sha256(block).hexdigest()})
                digest.update(block)
                total += len(block)
            manifest = {
                "format_version": 2,
                "transfer_id": transfer_id,
                "identity": self._identity.to_dict(),
                "filename": filename,
                "media_type": media_type,
                "bytes": total,
                "sha256": digest.hexdigest(),
                "chunks": chunks,
                "state": "staged",
                "offset": 0,
                "upload_id": None,
                "begin_action_key": None,
                "pending_action_key": None,
                "pending_offset": None,
                "pending_length": None,
                "finish_action_key": None,
                "failed_action_key": None,
                "failure": None,
            }
            self._save(manifest, creating=True)
            return manifest
        except Exception:
            # Staging has not prepared a server action yet, so these partial
            # records cannot represent unresolved remote work.
            for item in chunks:
                try:
                    self._storage.delete(item["name"])
                except Exception:
                    pass
            raise

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
        if set(changes) - _MANIFEST_V2_FIELDS:
            raise TransferError("Transfer manifest update contains unsupported fields")
        lock = self._storage.lock(self._lock_name(transfer_id), timeout=0.0)
        with (nullcontext() if _locked else lock):
            manifest = self.load(transfer_id)
            if manifest["state"] == "failed":
                raise TransferError("Failed transfer manifest is immutable")
            if manifest["format_version"] == 1:
                manifest = {**manifest, "format_version": 2, "failed_action_key": None, "failure": None}
            manifest.update(changes)
            self._save(manifest)
            return manifest

    def read_chunk(self, transfer_id: str, index: int) -> bytes:
        manifest = self.load(transfer_id)
        if type(index) is not int or not 0 <= index < len(manifest["chunks"]):
            raise TransferError("Transfer chunk index is invalid")
        entry = manifest["chunks"][index]
        try:
            block = self._storage.read(entry["name"])
        except (KeyError, ProtectedStorageError) as exc:
            raise TransferError("Protected upload spool is unavailable") from exc
        if len(block) != entry["bytes"] or hashlib.sha256(block).hexdigest() != entry["sha256"]:
            raise TransferError("Protected upload spool failed its checksum")
        return block

    def verify_content(self, manifest: dict[str, Any]) -> None:
        """Verify every protected chunk against the immutable whole-file digest."""
        digest = hashlib.sha256()
        total = 0
        for index in range(len(manifest["chunks"])):
            block = self.read_chunk(manifest["transfer_id"], index)
            digest.update(block)
            total += len(block)
        if total != manifest["bytes"] or digest.hexdigest() != manifest["sha256"]:
            raise TransferError("Protected upload spool failed its whole-file checksum")

    def ensure_action_capacity(self, manifest: dict[str, Any]) -> None:
        """Reserve quota headroom for the immutable chunk action records."""
        limit = getattr(self._storage, "MAX_TOTAL_BYTES", None)
        usage = getattr(self._storage, "usage_bytes", None)
        if type(limit) is not int or not callable(usage):
            raise ProtectedStorageError("Per-user recovery quota cannot be checked safely")
        actions = 2  # begin and finish
        required = actions * 8192
        for chunk in manifest["chunks"]:
            encoded_bytes = 4 * ((chunk["bytes"] + 2) // 3)
            if encoded_bytes + 8192 > MAX_RECORD_BYTES:
                raise ProtectedStorageError("Upload chunk action exceeds the 16 MiB recovery record limit")
            required += encoded_bytes + 8192
        if usage() + required > limit:
            raise ProtectedStorageError("Protected upload plus recovery actions exceeds the 1 GiB per-user quota")

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
                if manifest["state"] != "failed":
                    result.append(manifest)
            except TransferError:
                raise
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
                for chunk in manifest["chunks"]:
                    self._storage.delete(chunk["name"])
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
        if (
            not isinstance(manifest, dict)
            or set(manifest) != (_MANIFEST_FIELDS if manifest.get("format_version") == 1 else _MANIFEST_V2_FIELDS)
            or manifest.get("format_version") not in {1, 2}
            or type(manifest.get("format_version")) is not int
            or manifest.get("transfer_id") != transfer_id
            or not isinstance(transfer_id, str)
            or not _TRANSFER_ID.fullmatch(transfer_id)
            or not DeploymentIdentity.from_dict(manifest.get("identity", {})).matches(self._identity)
            or not isinstance(manifest.get("filename"), str)
            or not manifest["filename"]
            or not isinstance(manifest.get("media_type"), str)
            or not manifest["media_type"]
            or type(manifest.get("bytes")) is not int
            or manifest["bytes"] < 0
            or not isinstance(manifest.get("sha256"), str)
            or not re.fullmatch(r"[0-9a-f]{64}", manifest["sha256"])
            or manifest.get("state") not in ({"staged", "uploading", "finished"} if manifest.get("format_version") == 1
                                             else {"staged", "uploading", "finished", "failed"})
            or type(manifest.get("offset")) is not int
            or not 0 <= manifest["offset"] <= manifest["bytes"]
            or not isinstance(manifest.get("chunks"), list)
        ):
            raise TransferError("Protected transfer manifest has an unsupported format")
        if manifest["format_version"] == 2:
            failed_key, failure = manifest["failed_action_key"], manifest["failure"]
            if manifest["state"] == "failed":
                references = {manifest["begin_action_key"], manifest["pending_action_key"], manifest["finish_action_key"]}
                if not _is_uuid(failed_key) or failed_key not in references or not _valid_failure(failure):
                    raise TransferError("Failed transfer manifest has invalid failure metadata")
            elif failed_key is not None or failure is not None:
                raise TransferError("Active transfer manifest has terminal failure metadata")
        total = 0
        for index, entry in enumerate(manifest["chunks"]):
            if (
                not isinstance(entry, dict)
                or set(entry) != {"name", "bytes", "sha256"}
                or entry.get("name") != self._chunk_name(transfer_id, index)
                or type(entry.get("bytes")) is not int
                or not 0 <= entry["bytes"] <= TRANSFER_CHUNK_BYTES
                or not isinstance(entry.get("sha256"), str)
                or not re.fullmatch(r"[0-9a-f]{64}", entry["sha256"])
            ):
                raise TransferError("Protected transfer manifest has invalid chunk metadata")
            total += entry["bytes"]
        if total != manifest["bytes"] or (manifest["bytes"] and not manifest["chunks"]):
            raise TransferError("Protected transfer manifest length does not match its chunks")
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
    def _lock_name(transfer_id: str) -> str:
        if not isinstance(transfer_id, str) or not _TRANSFER_ID.fullmatch(transfer_id):
            raise TransferError("Transfer reference is invalid")
        return f"transfer-{transfer_id}"


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
