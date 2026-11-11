"""Streaming Exchange ZIP helpers.

The manifest's producer/provenance fields are owned by the package producer.
These helpers preserve those fields and only derive the ``files`` integrity
index from bytes actually written or read.
"""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Mapping
import bisect
from contextlib import nullcontext
import codecs
import hashlib
import heapq
import json
import math
import re
import stat
import struct
import sys
import uuid
import zlib

from ._imports import MANIFEST_FIELDS
from ._recovery.storage import ExchangeStagingOwner, ProtectedStore


_RESOURCE_PATH = re.compile(r"resources/([0-9a-f]{64})\Z")
_CHUNK_SIZE = 1024 * 1024
_MANIFEST_ALLOWED = frozenset(MANIFEST_FIELDS) | {
    "files", "maxHops", "rootClassBoundary", "boundaryReferences", "omittedEdgeCount",
}


class _RetryCleanup:
    """Retain each cleanup owner until its cleanup actually succeeds."""

    def __init__(self):
        self.resources = {}

    def register(self, resource):
        self.resources[id(resource)] = resource
        return resource

    def discard(self, resource):
        self.resources.pop(id(resource), None)

    def close(self):
        failure = None
        for resource in reversed(tuple(self.resources.values())):
            try:
                resource.close()
                self.discard(resource)
            except Exception as exc:
                if failure is None:
                    failure = exc
        if failure is not None:
            raise OSError("Protected Exchange cleanup is incomplete") from failure


class ProtectedChunkFile:
    """Seekable, chunk-backed protected staging over a ProtectedStore.

    The file is owned by its context. Every block is an independently protected
    record and is deleted on close, including when writing or validation fails.
    The class is intended for one synchronous workflow owner.
    """

    def __init__(self, storage, *, chunk_size: int = _CHUNK_SIZE, registry=None):
        if type(chunk_size) is not int or not 1 <= chunk_size <= storage.MAX_RECORD_BYTES:
            raise ValueError("Protected staging chunk size is outside the record boundary")
        self._storage = storage
        self._chunk_size = chunk_size
        owner_id = uuid.uuid4().hex
        self._prefix = "exchange-" + owner_id
        self._admission = ExchangeStagingOwner(storage, owner_id, chunk_size) if isinstance(storage, ProtectedStore) else None
        self._registry = registry
        self._size = 0
        self._committed_extent = 0
        self._attempted_extent = 0
        self._position = 0
        self._closed = False
        self._cleanup_complete = False
        self._cached_index: int | None = None
        self._cached_data = bytearray()
        self._cached_dirty = False
        if registry is not None:
            registry.register(self)

    def __enter__(self):
        if self._closed:
            raise ValueError("Protected staging stream is closed")
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.close()
        return False

    @property
    def closed(self) -> bool:
        return self._closed

    def readable(self) -> bool:
        return not self._closed

    def writable(self) -> bool:
        return not self._closed

    def seekable(self) -> bool:
        return not self._closed

    def tell(self) -> int:
        self._check_open()
        return self._position

    def seek(self, offset: int, whence: int = 0) -> int:
        self._check_open()
        if type(offset) is not int or type(whence) is not int or whence not in (0, 1, 2):
            raise ValueError("Invalid protected staging seek")
        base = 0 if whence == 0 else self._position if whence == 1 else self._size
        position = base + offset
        if position < 0:
            raise ValueError("Negative protected staging seek")
        selected = position // self._chunk_size if position < self._size else None
        if selected != self._cached_index:
            self._flush_cached()
        self._position = position
        return position

    def truncate(self, size: int | None = None) -> int:
        self._check_open()
        selected = self._position if size is None else size
        if type(selected) is not int or selected < 0:
            raise ValueError("Invalid protected staging length")
        self._flush_cached()
        if selected > self._size:
            original = self._position
            self._position = self._size
            while self._size < selected:
                self.write(b"\0" * min(self._chunk_size, selected - self._size))
            self._flush_cached()
            self._position = min(original, selected)
        if selected < self._size:
            keep = (selected + self._chunk_size - 1) // self._chunk_size
            for index in range((self._size + self._chunk_size - 1) // self._chunk_size):
                if index >= keep and self._storage.has_record(self._block_name(index)):
                    self._storage.delete(self._block_name(index))
            if selected and selected % self._chunk_size:
                index = selected // self._chunk_size
                self._write_block(index, self._read_block(index)[:selected % self._chunk_size])
            self._committed_extent = min(self._committed_extent, keep)
        self._cached_index = None
        self._cached_data = bytearray()
        self._cached_dirty = False
        self._size = selected
        if self._position > selected:
            self._position = selected
        return selected

    def write(self, data: bytes) -> int:
        self._check_open()
        if not isinstance(data, (bytes, bytearray, memoryview)):
            raise TypeError("Protected staging accepts bytes-like values")
        view = memoryview(data).cast("B")
        total = len(view)
        if total and self._position > self._size:
            target = self._position
            self._position = self._size
            while self._size < target:
                self.write(b"\0" * min(self._chunk_size, target - self._size))
            self._position = target
        consumed = 0
        while consumed < total:
            block_index, inner = divmod(self._position, self._chunk_size)
            count = min(total - consumed, self._chunk_size - inner)
            block = self._block(block_index)
            if len(block) < inner + count:
                block.extend(b"\0" * (inner + count - len(block)))
            block[inner:inner + count] = view[consumed:consumed + count]
            self._cached_dirty = True
            self._position += count
            self._size = max(self._size, self._position)
            consumed += count
        return total

    def read(self, size: int = -1) -> bytes:
        self._check_open()
        if type(size) is not int:
            raise TypeError("Protected staging read size must be an integer")
        remaining = max(0, self._size - self._position)
        wanted = remaining if size < 0 else min(size, remaining)
        result = bytearray()
        while len(result) < wanted:
            block_index, inner = divmod(self._position, self._chunk_size)
            block = self._block(block_index)
            count = min(wanted - len(result), self._chunk_size - inner)
            if count <= 0:
                break
            piece = block[inner:inner + count]
            if len(piece) != count:
                raise OSError("Protected Exchange block is truncated")
            result.extend(piece)
            self._position += count
        return bytes(result)

    def flush(self) -> None:
        self._check_open()
        self._flush_cached()

    def close(self) -> None:
        if self._cleanup_complete:
            return
        self._closed = True
        # Closing destroys this workflow-owned stream. Flushing here would
        # create needless records and can turn cancellation into a disk-full
        # failure while we are already trying to remove the staging data.
        failure = None
        if self._admission is not None:
            try:
                self._admission.close()
            except Exception as exc:
                failure = exc
        else:
            for index in range(self._attempted_extent):
                try:
                    name = self._block_name(index)
                    if self._storage.has_record(name):
                        self._storage.delete(name)
                except Exception as exc:
                    if failure is None:
                        failure = exc
        self._cached_index = None
        self._cached_data = bytearray()
        self._cached_dirty = False
        if failure is not None:
            raise OSError("Could not remove protected Exchange staging records") from failure
        self._cleanup_complete = True
        if self._registry is not None:
            self._registry.discard(self)

    def _check_open(self):
        if self._closed:
            raise ValueError("I/O operation on closed protected staging stream")

    def _block_name(self, index: int) -> str:
        return f"{self._prefix}-{index:016x}"

    def _read_block(self, index: int) -> bytes:
        name = self._block_name(index)
        if self._storage.has_record(name):
            return self._storage.read(name)
        if index < self._committed_extent:
            raise OSError("Committed protected Exchange block is missing")
        return b""

    def _block(self, index: int) -> bytearray:
        if self._cached_index != index:
            self._flush_cached()
            data = bytearray(self._read_block(index))
            self._cached_index = index
            self._cached_data = data
            self._cached_dirty = False
        return self._cached_data

    def _flush_cached(self) -> None:
        if self._cached_index is not None and self._cached_dirty:
            self._write_block(self._cached_index, bytes(self._cached_data))
            self._cached_dirty = False

    def _write_block(self, index: int, data: bytes) -> None:
        self._attempted_extent = max(self._attempted_extent, index + 1)
        try:
            if self._admission is None:
                self._storage.write(self._block_name(index), data)
            else:
                self._admission.write(index, data)
            self._committed_extent = max(self._committed_extent, index + 1)
        except Exception:
            self._closed = True
            raise


class DurableExchangeFile:
    """Reopenable immutable chunk checkpoints; short appends never replace a checkpoint."""

    CHUNK_SIZE = _CHUNK_SIZE

    def __init__(self, storage, owner_id, *, identity=None, create=False, verify=True):
        if not isinstance(owner_id, str) or not re.fullmatch(r"[0-9a-f]{32}", owner_id):
            raise ValueError("Durable Exchange owner reference is invalid")
        self.storage, self.owner_id, self.identity = storage, owner_id, identity
        self.prefix = f"import-stage-{owner_id}"
        self.manifest_name = self.prefix + "-manifest"
        self.position, self.closed = 0, False
        if create and not storage.has_record(self.manifest_name):
            self.manifest = {"owner_id": owner_id, "identity": identity, "bytes": 0,
                             "chunk_count": 0, "complete": False}
            self._save_manifest()
        else:
            self.manifest = self._load_manifest()
        if identity is not None and self.manifest["identity"] != identity:
            raise ValueError("Durable Exchange staging identity changed")
        if verify:
            self._verify_chunks()
        self.position = self.manifest["bytes"]

    def _write(self, name, data):
        # Reuse the admitted protected-record writer and its restart reconciler.
        from ._recovery.journal import ProtectedJournal
        ProtectedJournal(store=self.storage)._write_protected(name, data)

    def _load_manifest(self):
        try:
            value = json.loads(self.storage.read(self.manifest_name).decode("utf-8"))
        except Exception as exc:
            raise OSError("Durable Exchange checkpoint is unavailable") from exc
        if (not isinstance(value, dict) or set(value) != {"owner_id", "identity", "bytes", "chunk_count", "complete"}
                or value.get("owner_id") != self.owner_id or type(value.get("bytes")) is not int
                or value["bytes"] < 0 or type(value.get("chunk_count")) is not int
                or value["chunk_count"] != (value["bytes"] + self.CHUNK_SIZE - 1) // self.CHUNK_SIZE
                or type(value.get("complete")) is not bool):
            raise OSError("Durable Exchange checkpoint is corrupt")
        return value

    def _save_manifest(self):
        self._write(self.manifest_name, json.dumps(self.manifest, sort_keys=True,
                    separators=(",", ":"), allow_nan=False).encode())

    def _chunk_name(self, index, *, length=None):
        length = min(self.CHUNK_SIZE, self.manifest["bytes"] - index * self.CHUNK_SIZE) if length is None else length
        # Partial chunks use immutable length-qualified generations. A crash
        # before publishing a larger checkpoint leaves the old tail intact.
        return f"{self.prefix}-chunk-{index:08x}-{length:08x}"

    def _meta_name(self, index, *, length=None):
        return self._chunk_name(index, length=length).replace("-chunk-", "-meta-")

    def _verify_chunks(self):
        for index in range(self.manifest["chunk_count"]):
            expected = min(self.CHUNK_SIZE, self.manifest["bytes"] - index * self.CHUNK_SIZE)
            try:
                data = self.storage.read(self._chunk_name(index))
                meta = json.loads(self.storage.read(self._meta_name(index)))
            except Exception as exc:
                raise OSError("A committed Exchange chunk is missing") from exc
            if (meta != {"index": index, "bytes": expected, "sha256": hashlib.sha256(data).hexdigest()}
                    or len(data) != expected):
                raise OSError("A committed Exchange chunk failed checkpoint verification")

    def write(self, data):
        self._check()
        if self.manifest["complete"]:
            raise OSError("Completed Exchange bytes are immutable")
        if self.position != self.manifest["bytes"]:
            raise OSError("Durable Exchange writes must append at the verified checkpoint")
        view = memoryview(data).cast("B")
        consumed = 0
        while consumed < len(view):
            index, inner = divmod(self.position, self.CHUNK_SIZE)
            previous = self._chunk_name(index) if inner else None
            prior = self.storage.read(previous) if inner else b""
            count = min(self.CHUNK_SIZE - inner, len(view) - consumed)
            piece = prior + bytes(view[consumed:consumed + count])
            name = self._chunk_name(index, length=len(piece))
            meta = self._meta_name(index, length=len(piece))
            self._write(name, piece)
            self._write(meta, json.dumps({"index": index, "bytes": len(piece),
                        "sha256": hashlib.sha256(piece).hexdigest()}, separators=(",", ":")).encode())
            old_manifest = dict(self.manifest)
            self.manifest["bytes"] += count
            self.manifest["chunk_count"] = index + 1
            try:
                self._save_manifest()
            except BaseException:
                self.manifest = old_manifest
                raise
            self.position += count
            consumed += count
            if previous:
                self.storage.delete(previous)
                self.storage.delete(previous.replace("-chunk-", "-meta-"))
        return consumed

    def read(self, size=-1):
        self._check()
        remaining = max(0, self.manifest["bytes"] - self.position)
        wanted = remaining if size is None or size < 0 else min(size, remaining)
        result = bytearray()
        while len(result) < wanted:
            index, inner = divmod(self.position, self.CHUNK_SIZE)
            data = self.storage.read(self._chunk_name(index))
            count = min(wanted - len(result), len(data) - inner)
            if count <= 0:
                raise OSError("Durable Exchange chunk is truncated")
            result.extend(data[inner:inner + count])
            self.position += count
        return bytes(result)

    def seek(self, offset, whence=0):
        self._check()
        base = 0 if whence == 0 else self.position if whence == 1 else self.manifest["bytes"] if whence == 2 else None
        if type(offset) is not int or base is None or base + offset < 0:
            raise ValueError("Invalid durable Exchange seek")
        self.position = base + offset
        return self.position

    def tell(self):
        self._check()
        return self.position

    def flush(self):
        self._check()

    def complete(self):
        self._check()
        self._verify_chunks()
        self.manifest["complete"] = True
        self._save_manifest()

    def close(self):
        self.closed = True

    @classmethod
    def release_owned(cls, storage, owner_id, *, workflow_id=None):
        if not isinstance(owner_id, str) or not re.fullmatch(r"[0-9a-f]{32}", owner_id):
            raise ValueError("Durable Exchange owner reference is invalid")
        # Cleanup authority comes from the authenticated workflow record, not
        # successful reads of content that may have failed during disk pressure.
        stage = object.__new__(cls)
        stage.storage, stage.owner_id = storage, owner_id
        stage.prefix = f"import-stage-{owner_id}"
        return stage.cleanup(workflow_id=workflow_id)

    def cleanup(self, *, workflow_id=None):
        from ._recovery.storage import ProtectedStorageError, StorageAdmissionCoordinator
        with self.storage.lock(self.prefix + "-ownership"):
            if workflow_id is not None:
                self.storage.delete(self.prefix + "-workflow-" + workflow_id)
            if any(name.startswith(self.prefix + "-workflow-") for name in self.storage.list_records()):
                return True
            for name in self.storage.list_records():
                if name.startswith(self.prefix + "-reader-"):
                    try:
                        with self.storage.lock("ix-reader-" + hashlib.sha256(name.encode()).hexdigest()[:40]):
                            self.storage.delete(name)
                    except ProtectedStorageError:
                        self._write(self.prefix + "-cleanup", b"true")
                        return False
            # No write is needed to reclaim an unpinned stage under disk pressure.
            for name in self.storage.list_records():
                if name.startswith(self.prefix + "-"):
                    self.storage.delete(name)
            coordinator = StorageAdmissionCoordinator(self.storage)
            complete = True
            for name in self.storage.list_records():
                if name.startswith("admission-"):
                    reservation = json.loads(self.storage.read(name))
                    if (reservation.get("purpose") == "journal-record"
                            and str(reservation.get("owner_id", "")).startswith(self.prefix + "-")):
                        try:
                            with coordinator.writer(reservation):
                                observation = coordinator._reconcile_owner(reservation)
                                if observation.get("resolved"):
                                    self.storage.delete(name)
                                else:
                                    complete = False
                        except ProtectedStorageError:
                            complete = False
            return complete

    def reader(self, *, borrower=None):
        return _ExchangeReader(self, borrower=borrower)

    def _check(self):
        if self.closed:
            raise ValueError("Durable Exchange staging file is closed")


class _ExchangeReader:
    """An independently positioned process-pinned immutable archive reader."""

    def __init__(self, stage, *, borrower=None):
        self._stage = stage
        self.closed = False
        self._name = stage.prefix + "-reader-" + uuid.uuid4().hex
        with stage.storage.lock(stage.prefix + "-ownership"):
            if stage.storage.has_record(stage.prefix + "-cleanup") and (borrower is None or borrower.closed):
                raise ValueError("Exchange staging cleanup has been requested")
            self._lock = stage.storage.lock("ix-reader-" + hashlib.sha256(self._name.encode()).hexdigest()[:40])
            self._lock.acquire()
            try:
                stage._write(self._name, b"true")
                self._file = DurableExchangeFile(stage.storage, stage.owner_id, identity=stage.identity, verify=False)
                if not self._file.manifest["complete"]:
                    raise ValueError("Exchange staging is incomplete")
                self._file.seek(0)
            except BaseException:
                self._lock.release()
                stage.storage.delete(self._name)
                raise

    def read(self, size=-1):
        return self._file.read(size)

    def seek(self, offset, whence=0):
        return self._file.seek(offset, whence)

    def tell(self):
        return self._file.tell()

    def close(self):
        if not self.closed:
            self.closed = True
            self._file.close()
            self._lock.release()
            self._stage.storage.delete(self._name)
            if self._stage.storage.has_record(self._stage.prefix + "-cleanup"):
                self._stage.cleanup()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


class _ExchangeContentReader:
    def __init__(self, reader, info):
        self._reader = reader
        # Reuse the already checked bounded ZIP entry metadata with an
        # independent cursor. No JSON/index lifetime is borrowed from the bundle.
        archive = object.__new__(_Archive)
        archive.source, archive.check = reader, lambda: None
        self._chunks = iter(archive.chunks(info))
        self._buffer = b""
        self.closed = False

    def read(self, size=-1):
        if self.closed:
            raise ValueError("Exchange content reader is closed")
        result = bytearray()
        while size < 0 or len(result) < size:
            if not self._buffer:
                self._buffer = next(self._chunks, b"")
                if not self._buffer:
                    break
            count = len(self._buffer) if size < 0 else min(len(self._buffer), size - len(result))
            result.extend(self._buffer[:count])
            self._buffer = self._buffer[count:]
        return bytes(result)

    def close(self):
        if not self.closed:
            self._reader.close()
            self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


class ExchangeBundle:
    """Lazy validated frozen archive; Stores and caller input streams are borrowed."""

    def __init__(self, source_store, export_result, *, workflow_id=None):
        import copy
        self.source_store = source_store
        self._source_binding = copy.deepcopy(source_store._identity.to_dict())
        self._export_result = copy.deepcopy(export_result)
        self._closed = False
        self._workflow_id = workflow_id
        self._stage_id = uuid.uuid4().hex
        self._stage = None
        self._package_view = None
        self._owner_reader = None

    def pin(self, workflow_id):
        if not isinstance(workflow_id, str) or not re.fullmatch(r"[0-9a-f]{32}", workflow_id):
            raise ValueError("Import workflow reference is invalid")
        self._workflow_id = workflow_id
        if self._stage is not None:
            self._stage._write(self._stage.prefix + "-workflow-" + workflow_id, b"true")
        return self

    @property
    def source_binding(self):
        import copy
        return copy.deepcopy(self._source_binding)

    @property
    def package_reference(self):
        result = self._export_result
        return (result.get("packageResourceId") or result.get("resourceId")) if isinstance(result, Mapping) else None

    def _assert_source(self):
        if self.source_store._identity.to_dict() != self._source_binding or getattr(self.source_store, "closed", False):
            raise ValueError("Authenticated source Store identity changed")

    def materialize(self):
        if self._closed:
            raise ValueError("Exchange bundle is closed")
        if self._package_view is None:
            self._materialize_remote()
        return self._package_view

    def _materialize_remote(self):
        from ._recovery.transfers import TRANSFER_CHUNK_BYTES
        self._assert_source()
        result = self._export_result
        resource_id = self.package_reference
        if not isinstance(resource_id, str) or not resource_id:
            raise ValueError("Export returned no package resource reference")
        storage = self.source_store._journal.storage
        self._stage = DurableExchangeFile(storage, self._stage_id, identity=self._source_binding, create=True)
        if self._workflow_id is not None:
            self._stage._write(self._stage.prefix + "-workflow-" + self._workflow_id, b"true")
        if self._stage.manifest["complete"]:
            self._attach_view()
            return
        metadata = self.source_store._call_one("deposit.resource.info", resource_id)
        if (not isinstance(metadata, dict) or metadata.get("resourceId") != resource_id
                or type(metadata.get("bytes")) is not int or metadata["bytes"] < 0
                or not isinstance(metadata.get("checksum"), str)
                or not re.fullmatch(r"[0-9a-f]{64}", metadata["checksum"])):
            raise ValueError("Export package metadata is invalid")
        expected_size, expected_digest = metadata["bytes"], metadata["checksum"]
        while self._stage.manifest["bytes"] < expected_size:
            self._assert_source()
            offset = self._stage.manifest["bytes"]
            requested = min(TRANSFER_CHUNK_BYTES, expected_size - offset)
            chunk = self.source_store._call_one("deposit.resource.downloadChunk", resource_id, offset, requested)
            try:
                block = __import__("base64").b64decode(chunk["bytesBase64"], validate=True)
            except Exception as exc:
                raise ValueError("Export package download returned invalid Base64") from exc
            next_offset = offset + len(block)
            if (chunk.get("offset") != offset or chunk.get("nextOffset") != next_offset or not block
                    or len(block) > requested or chunk.get("eof") is not (next_offset == expected_size)
                    or hashlib.sha256(block).hexdigest() != chunk.get("chunkSha256")):
                raise ValueError("Export package chunk failed offset or digest verification")
            self._stage.write(block)
        digest = hashlib.sha256()
        self._stage.seek(0)
        while block := self._stage.read(_CHUNK_SIZE):
            digest.update(block)
        if self._stage.manifest["bytes"] != expected_size or digest.hexdigest() != expected_digest:
            raise ValueError("Export package failed its whole-file checksum")
        for claim, actual in ((result.get("bytes"), expected_size),
                              (result.get("checksum") or result.get("sha256"), expected_digest)):
            if claim is not None and claim != actual:
                raise ValueError("Export package receipt contradicts its resource metadata")
        self._stage.complete()
        self._attach_view()

    def _attach_view(self):
        self._owner_reader = self._stage.reader()
        try:
            self._package_view = validate_exchange_archive(self._owner_reader, storage=self._stage.storage)
        except BaseException:
            self._owner_reader.close()
            self._owner_reader = None
            raise

    @property
    def manifest(self):
        return self.materialize().manifest

    @property
    def graph(self):
        return self.materialize().graph

    def open_resource(self, entry):
        package = self.materialize()
        if not isinstance(entry, str) or not _RESOURCE_PATH.fullmatch(entry):
            raise ValueError("Exchange resource entry is unsafe")
        info = package._table.get(("entry", entry))
        if info is None:
            raise KeyError(entry)
        return _ExchangeContentReader(self._stage.reader(borrower=self._owner_reader), list(info))

    def open_package_stream(self):
        self.materialize()
        return self._stage.reader(borrower=self._owner_reader)

    def save(self, path):
        from pathlib import Path
        with self.open_package_stream() as source, Path(path).open("wb") as output:
            while block := source.read(_CHUNK_SIZE):
                output.write(block)

    def close(self):
        self._closed = True
        if self._package_view is not None:
            self._package_view.close()
            self._package_view = None
        if self._owner_reader is not None:
            self._owner_reader.close()
            self._owner_reader = None
        if self._stage is not None:
            self._stage.close()
            self._stage.cleanup()

    def __enter__(self):
        if self._closed:
            raise ValueError("Exchange bundle is closed")
        return self

    def __exit__(self, *args):
        self.close()


class _ZipWriter:
    """ZIP64 writer with a protected spooled central directory."""

    def __init__(self, destination, directory, check):
        self.destination, self.directory, self.check = destination, directory, check
        self.count = 0

    @staticmethod
    def write_all(stream, data):
        view = memoryview(data)
        while view:
            count = stream.write(view)
            if not isinstance(count, int) or count <= 0 or count > len(view):
                raise OSError("Exchange destination did not accept a complete write")
            view = view[count:]

    def add(self, name, blocks):
        name_bytes = name.encode("ascii")
        offset = self.destination.tell()
        header = struct.pack("<4s5H3I2H", b"PK\x03\x04", 45, 0, 8, 0, 33, 0,
                             0xFFFFFFFF, 0xFFFFFFFF, len(name_bytes), 20)
        self.write_all(self.destination, header + name_bytes + struct.pack("<HHQQ", 1, 16, 0, 0))
        compressor = zlib.compressobj(wbits=-15)
        digest, crc, length, compressed = hashlib.sha256(), 0, 0, 0
        for block in blocks:
            self.check()
            digest.update(block)
            crc = zlib.crc32(block, crc)
            length += len(block)
            if length > 2 ** 53 - 1:
                raise ValueError("Exchange byte length exceeds its exact metadata representation")
            output = compressor.compress(block)
            self.write_all(self.destination, output)
            compressed += len(output)
        output = compressor.flush()
        self.write_all(self.destination, output)
        compressed += len(output)
        end = self.destination.tell()
        self.destination.seek(offset + 14)
        self.write_all(self.destination, struct.pack("<I", crc))
        self.destination.seek(offset + 30 + len(name_bytes) + 4)
        self.write_all(self.destination, struct.pack("<QQ", length, compressed))
        self.destination.seek(end)
        central = struct.pack("<4s6H3I5H2I", b"PK\x01\x02", 45, 45, 0, 8, 0, 33, crc,
                              0xFFFFFFFF, 0xFFFFFFFF, len(name_bytes), 28, 0, 0, 0,
                              (stat.S_IFREG | 0o600) << 16, 0xFFFFFFFF)
        self.write_all(self.directory, central + name_bytes + struct.pack("<HHQQQ", 1, 24, length, compressed, offset))
        self.count += 1
        return {"bytes": length, "sha256": digest.hexdigest()}

    def finish(self):
        offset = self.destination.tell()
        size = self.directory.tell()
        self.directory.seek(0)
        while True:
            self.check()
            block = self.directory.read(_CHUNK_SIZE)
            if not block:
                break
            self.write_all(self.destination, block)
        zip64_offset = self.destination.tell()
        self.write_all(self.destination, struct.pack("<4sQ2H2I4Q", b"PK\x06\x06", 44, 45, 45,
                                                    0, 0, self.count, self.count, size, offset))
        self.write_all(self.destination, struct.pack("<4sIQI", b"PK\x06\x07", 0, zip64_offset, 1))
        self.write_all(self.destination, struct.pack("<4s4H2IH", b"PK\x05\x06", 0, 0,
                                                    min(self.count, 65535), min(self.count, 65535),
                                                    min(size, 0xFFFFFFFF), min(offset, 0xFFFFFFFF), 0))


def write_exchange_package(destination, snapshot, resources, *, storage=None, checkpoint=None,
                           materialize_manifest=False):
    """Write ZIP64 with bounded buffers and protected central/file-index spools.

    Portable destinations remain caller-owned. Protected staging supplies its
    store; other destinations require explicit protected ``storage``. The full
    manifest is returned only when ``materialize_manifest=True`` is requested.
    Producer/provenance fields are preserved without interpretation.
    """
    if not isinstance(snapshot, Mapping) or set(snapshot) - {"manifest", "graph", "history"}:
        raise TypeError("Exchange snapshot must contain manifest, graph, and optional history")
    manifest, graph, history = snapshot.get("manifest"), snapshot.get("graph"), snapshot.get("history")
    if not isinstance(manifest, Mapping) or not isinstance(graph, Mapping):
        raise TypeError("Exchange snapshot requires manifest and graph mappings")
    if any(key not in _MANIFEST_ALLOWED for key in manifest):
        raise ValueError("Exchange manifest contains an unknown field")
    version = manifest.get("depositVersion")
    if version is not None and (not isinstance(version, str) or not version):
        raise ValueError("Exchange depositVersion must be a nonempty Server version String or null")
    if history is not None and not isinstance(history, Mapping):
        raise TypeError("Exchange history must be a mapping")
    if not isinstance(resources, Mapping):
        raise TypeError("Exchange resources must map content-addressed paths to binary streams")
    storage = storage if storage is not None else getattr(destination, "_storage", None)
    if storage is None:
        raise ValueError("Exchange writing requires protected recovery storage")
    check = checkpoint or (lambda: None)
    for path in resources:
        check()
        if not isinstance(path, str) or not _RESOURCE_PATH.fullmatch(path):
            raise ValueError("Exchange resource path must be resources/<lowercase sha256>")
    with ProtectedChunkFile(storage) as directory, ProtectedChunkFile(storage) as files:
        archive = _ZipWriter(destination, directory, check)
        count = 0

        def index(name, info):
            nonlocal count
            if count:
                files.write(b",")
            for block in _iter_json_bytes(name, check):
                files.write(block)
            files.write(b":")
            for block in _iter_json_bytes(info, check):
                files.write(block)
            count += 1

        for name, value in (("graph.json", graph), ("history.json", history)):
            if value is not None:
                index(name, archive.add(name, _iter_json_bytes(value, check)))
        for path, provider in resources.items():
            manager = provider() if callable(provider) else nullcontext(provider)
            with manager as reader:
                if not callable(getattr(reader, "read", None)):
                    raise TypeError(f"Exchange source for {path} is not a readable binary stream")

                def blocks():
                    while True:
                        check()
                        block = reader.read(_CHUNK_SIZE)
                        if not isinstance(block, bytes):
                            raise TypeError(f"Exchange source for {path} must return bytes")
                        if not block:
                            return
                        yield block

                info = archive.add(path, blocks())
            if info["sha256"] != path[10:]:
                raise ValueError(f"Exchange resource checksum does not match its path: {path}")
            index(path, info)

        def manifest_blocks():
            yield b"{"
            for key, value in manifest.items():
                if key == "files":
                    continue
                yield from _iter_json_bytes(key, check)
                yield b":"
                yield from _iter_json_bytes(value, check)
                yield b","
            if "depositVersion" not in manifest:
                yield b'"depositVersion":null,'
            yield b'"files":{'
            files.seek(0)
            while True:
                check()
                block = files.read(_CHUNK_SIZE)
                if not block:
                    break
                yield block
            yield b"}}"

        archive.add("manifest.json", manifest_blocks())
        archive.finish()
        if materialize_manifest:
            files.seek(0)
            result = dict(manifest)
            result.setdefault("depositVersion", None)
            result["files"] = json.loads(b"{" + files.read() + b"}")
            return result
    return None


def _object_size(value):
    # Run pages hold bounded internal tuples/lists of primitive metadata.
    size = sys.getsizeof(value)
    if isinstance(value, (list, tuple)):
        size += sum(_object_size(item) for item in value)
    return size


class _IndexCache:
    """One aggregate bounded cache shared by all live sorted runs."""

    def __init__(self):
        self.pages = OrderedDict()
        self.size = 0

    def get(self, key, load, weight=len):
        entry = self.pages.pop(key, None)
        if entry is None:
            value = load()
            size = weight(value)
            if size > 512 * 1024:
                return value
            while self.pages and self.size + size > 512 * 1024:
                _, old = self.pages.popitem(last=False)
                self.size -= old[1]
            self.size += size
        else:
            value, size = entry
        self.pages[key] = (value, size)
        return value

    def remove(self, run):
        for key in tuple(self.pages):  # At most the aggregate cache capacity.
            if key[0] == run:
                self.size -= self.pages.pop(key)[1]


class _SortedRun:
    """Protected sorted pages with a protected fixed-width sparse directory."""

    _POINTER = struct.Struct("<QI")

    def __init__(self, storage, rows, check, cache, row_bound, registry=None):
        self.cache, self.identity = cache, uuid.uuid4().hex
        self.registry = registry
        self.data = ProtectedChunkFile(storage, chunk_size=64 * 1024)
        self.directory = ProtectedChunkFile(storage, chunk_size=64 * 1024)
        self.filter = ProtectedChunkFile(storage, chunk_size=64 * 1024)
        if registry is not None:
            registry.register(self)
        self.filter_size = max(64, (row_bound * 12 + 7) // 8)
        self.filter.truncate(self.filter_size)
        self.pages = 0
        self.row_count = 0
        try:
            page = bytearray()
            for row in rows:
                check()
                for bit in self._filter_bits(row[0]):
                    self.filter.seek(bit // 8)
                    previous = self.filter.read(1)[0]
                    self.filter.seek(bit // 8)
                    self.filter.write(bytes((previous | 1 << (bit % 8),)))
                self.row_count += 1
                encoded = json.dumps(row, ensure_ascii=True, separators=(",", ":")).encode("ascii")
                if page and len(page) + len(encoded) + 4 > 32 * 1024:
                    self._page(page)
                    page.clear()
                page.extend(struct.pack("<I", len(encoded)))
                page.extend(encoded)
            if page:
                self._page(page)
            self.release_cache()
        except BaseException:
            self.close()
            raise

    def _page(self, page):
        self.directory.write(self._POINTER.pack(self.data.tell(), len(page)))
        self.data.write(page)
        self.pages += 1

    def _rows(self, page):
        block = page // 512

        def pointer_block():
            self.directory.seek(block * 512 * self._POINTER.size)
            return self.directory.read(min(512, self.pages - block * 512) * self._POINTER.size)

        pointers = self.cache.get((self.identity, "directory", block), pointer_block)
        offset, length = self._POINTER.unpack_from(pointers, (page % 512) * self._POINTER.size)

        def data_page():
            self.data.seek(offset)
            return self.data.read(length)

        raw = self.cache.get((self.identity, "data", page), data_page)
        position = 0
        while position < len(raw):
            count = int.from_bytes(raw[position:position + 4], "little")
            position += 4
            yield json.loads(raw[position:position + count])
            position += count

    def get(self, key):
        try:
            for bit in self._filter_bits(key):
                byte = bit // 8
                block = byte // 4096

                def load():
                    self.filter.seek(block * 4096)
                    return self.filter.read(min(4096, self.filter_size - block * 4096))

                raw = self.cache.get((self.identity, "filter", block), load)
                if not raw[byte % 4096] & (1 << (bit % 8)):
                    return None
            low, high = 0, self.pages
            while low < high:
                middle = (low + high) // 2
                first = next(self._rows(middle))[0]
                if first <= key:
                    low = middle + 1
                else:
                    high = middle
            if not low:
                return None
            def decoded():
                rows = list(self._rows(low - 1))
                return [row[0] for row in rows], rows

            keys, rows = self.cache.get((self.identity, "decoded", low - 1), decoded, _object_size)
            at = bisect.bisect_left(keys, key)
            if at < len(rows) and keys[at] == key:
                return rows[at]
            return None
        finally:
            self.release_cache()

    def __iter__(self):
        try:
            for page in range(self.pages):
                yield from self._rows(page)
        finally:
            self.release_cache()

    def _filter_bits(self, key):
        digest = hashlib.sha256(key.encode("ascii")).digest()
        for offset in (0, 8, 16, 24):
            yield int.from_bytes(digest[offset:offset + 8], "little") % (self.filter_size * 8)

    def release_cache(self):
        # Idle runs retain no decrypted pages: cache use is bounded by merge
        # fan-in, rather than the number or size of input runs.
        for stream in (self.data, self.directory, self.filter):
            stream._flush_cached()
            stream._cached_index = None
            stream._cached_data = bytearray()

    def close(self):
        self.cache.remove(self.identity)
        try:
            self.data.close()
        finally:
            try:
                self.directory.close()
            finally:
                self.filter.close()
        if self.registry is not None:
            self.registry.discard(self)


class _ProtectedIndex:
    """Bounded batches and four-way protected sort/merge, with latest-row wins.

    Sorting and sparse-page lookup compare complete internal keys. Runs are
    compacted geometrically; neither lookup chains nor a bucket population can
    grow linearly with the input. No plaintext database/journal/temp file exists.
    """

    def __init__(self, owner):
        self.owner = owner
        self.batch = {}
        self.batch_bytes = 0
        self.sequence = 0
        self.levels = {}
        self.cache = _IndexCache()
        owner._owned.register(self)

    @staticmethod
    def _key(key):
        return json.dumps(key, ensure_ascii=True, separators=(",", ":"))

    def get(self, key):
        key = self._key(key)
        newest = self.batch.get(key)
        if newest is not None:
            return newest[2]
        for runs in self.levels.values():
            for run in runs:
                self.owner._check()
                row = run.get(key)
                if row is not None and (newest is None or row[1] > newest[1]):
                    newest = row
        return None if newest is None else newest[2]

    def put(self, key, value):
        self.owner._check()
        key = self._key(key)
        self.sequence += 1
        row = [key, self.sequence, value]
        encoded_size = len(json.dumps(row, separators=(",", ":")))
        previous = self.batch.get(key)
        if previous is not None:
            self.batch_bytes -= len(json.dumps(previous, separators=(",", ":")))
        self.batch[key] = row
        self.batch_bytes += encoded_size
        if self.batch_bytes >= 256 * 1024:
            rows = self.batch
            self.batch = {}
            self.batch_bytes = 0
            run = _SortedRun(self.owner._storage, (rows[key] for key in sorted(rows)), self.owner._check,
                             self.cache, len(rows), self.owner._owned)
            try:
                self._install(run, 0)
            except BaseException:
                run.close()
                raise

    def _install(self, run, level):
        runs = self.levels.setdefault(level, [])
        runs.append(run)
        if len(runs) < 4:
            return

        def merged():
            pending = None
            for row in heapq.merge(*(iter(run) for run in runs), key=lambda row: row[0]):
                self.owner._check()
                if pending is not None and row[0] != pending[0]:
                    yield pending
                    pending = None
                if pending is None or row[1] > pending[1]:
                    pending = row
            if pending is not None:
                yield pending

        combined = _SortedRun(self.owner._storage, merged(), self.owner._check, self.cache,
                              sum(run.row_count for run in runs), self.owner._owned)
        # Retirement may fail after deleting only some children. Keep both
        # source and combined owners registered until cleanup succeeds.
        for old in runs:
            old.close()
        self.levels[level] = []
        self._install(combined, level + 1)

    def close(self):
        failure = None
        for level, runs in self.levels.items():
            remaining = []
            for run in runs:
                try:
                    run.close()
                except Exception as exc:
                    remaining.append(run)
                    if failure is None:
                        failure = exc
            self.levels[level] = remaining
        self.batch.clear()
        if failure is not None:
            raise OSError("Could not remove protected Exchange indexes") from failure
        self.levels.clear()
        self.owner._owned.discard(self)


class _JsonInput:
    """Bounded UTF-8 JSON lexer; Strings and numbers are never accumulated."""

    def __init__(self, source, check):
        self.source, self.check = source, check
        self.block = b""
        self.position = 0
        self.offset = source.tell()

    def peek(self):
        if self.position == len(self.block):
            self.check()
            self.source.seek(self.offset)
            self.block = self.source.read(64 * 1024)
            self.position = 0
        return self.block[self.position] if self.block else None

    def take(self):
        value = self.peek()
        if value is None:
            raise ValueError("Exchange JSON is incomplete")
        self.position += 1
        self.offset += 1
        return value

    def token(self):
        while self.peek() in (9, 10, 13, 32):
            self.take()
        start, value = self.offset, self.peek()
        if value is None:
            return None
        if value in b"{}[]:,":
            return chr(self.take()), start, self.offset, None
        if value == 34:
            self.take()
            digest = hashlib.sha256()
            decoder = codecs.getincrementaldecoder("utf-8")("strict")
            while True:
                self.peek()
                begin = self.position
                # Process ordinary text in bounded runs, not one Python object
                # per character. Escapes are decoded before hashing keys.
                while self.position < len(self.block) and self.block[self.position] not in (34, 92):
                    if self.block[self.position] < 32:
                        raise ValueError("Exchange JSON String contains a control character")
                    self.position += 1
                count = self.position - begin
                self.offset += count
                digest.update(decoder.decode(self.block[begin:self.position]).encode("utf-8"))
                if self.position == len(self.block):
                    if not self.block:
                        raise ValueError("Exchange JSON String is incomplete")
                    continue
                digest.update(decoder.decode(b"", final=True).encode("utf-8"))
                special = self.take()
                if special == 34:
                    return "s", start, self.offset, digest.hexdigest()
                escape = self.take()
                simple = {34: '"', 92: "\\", 47: "/", 98: "\b", 102: "\f", 110: "\n", 114: "\r", 116: "\t"}
                if escape in simple:
                    text = simple[escape]
                elif escape == 117:
                    code = self._hex_code()
                    if 0xD800 <= code <= 0xDBFF:
                        if self.take() != 92 or self.take() != 117:
                            raise ValueError("Exchange JSON String has an unpaired surrogate")
                        low = self._hex_code()
                        if not 0xDC00 <= low <= 0xDFFF:
                            raise ValueError("Exchange JSON String has an unpaired surrogate")
                        code = 0x10000 + ((code - 0xD800) << 10) + low - 0xDC00
                    elif 0xDC00 <= code <= 0xDFFF:
                        raise ValueError("Exchange JSON String has an unpaired surrogate")
                    text = chr(code)
                else:
                    raise ValueError("Exchange JSON String escape is invalid")
                digest.update(text.encode("utf-8"))
                decoder = codecs.getincrementaldecoder("utf-8")("strict")
        for literal, kind in ((b"true", "t"), (b"false", "f"), (b"null", "z")):
            if value == literal[0]:
                for char in literal:
                    if self.take() != char:
                        raise ValueError("Exchange JSON literal is invalid")
                return kind, start, self.offset, None
        if value == 45 or 48 <= value <= 57:
            if value == 45:
                self.take()
            first = self.take()
            if not 48 <= first <= 57:
                raise ValueError("Exchange JSON number is invalid")
            if first != 48:
                while self.peek() is not None and 48 <= self.peek() <= 57:
                    self.take()
            if self.peek() == 46:
                self.take()
                self._digits()
            if self.peek() in (69, 101):
                self.take()
                if self.peek() in (43, 45):
                    self.take()
                self._digits()
            return "n", start, self.offset, None
        raise ValueError("Exchange JSON contains an invalid token or non-finite number")

    def _digits(self):
        if self.peek() is None or not 48 <= self.peek() <= 57:
            raise ValueError("Exchange JSON number is invalid")
        while self.peek() is not None and 48 <= self.peek() <= 57:
            self.take()

    def _hex_code(self):
        digits = bytes(self.take() for _ in range(4))
        if any(char not in b"0123456789abcdefABCDEF" for char in digits):
            raise ValueError("Exchange JSON Unicode escape is invalid")
        return int(digits, 16)


class _JsonDocument:
    _FRAME = struct.Struct("<QQBB96sQQ")

    def __init__(self, owner, name, source):
        self.owner, self.name, self.source = owner, name, source
        self.root = None

    def key(self, *parts):
        return ("json", self.name, *parts)

    def parse(self):
        table, check = self.owner._table, self.owner._check
        stack = self.owner._new_file(chunk_size=64 * 1024)
        lexer = _JsonInput(self.source, check)
        depth = 0

        def top():
            stack.seek((depth - 1) * self._FRAME.size)
            return list(self._FRAME.unpack(stack.read(self._FRAME.size)))

        def update(frame):
            stack.seek((depth - 1) * self._FRAME.size)
            stack.write(self._FRAME.pack(*frame))

        def attach(node):
            nonlocal depth
            if not depth:
                if self.root is not None:
                    raise ValueError("Exchange JSON has multiple root values")
                self.root = node[1]
                return
            frame = top()
            start, count, kind, state, key, key_start, key_end = frame
            table.put(self.key("child", start, count), [key_start, key_end, node[1]])
            if kind == 1:
                field = self.key("field", start, key.rstrip(b"\0").decode("ascii"))
                if table.get(field) is not None:
                    raise ValueError("Exchange JSON contains duplicate object keys")
                table.put(field, node[1])
            frame[1] += 1
            frame[3] = 1
            update(frame)

        while True:
            check()
            token = lexer.token()
            if token is None:
                if depth or self.root is None:
                    raise ValueError("Exchange JSON is incomplete")
                break
            kind, start, end, digest = token
            if kind == "s":
                digest = self.owner._canonical_string(self.name, start, digest)
            elif kind == "n":
                _validate_finite_number(self.source, start, end, check)
            frame = top() if depth else None
            if frame:
                # State: 0=first item, 1=after value, 2=after comma,
                # 3=object colon, 4=object value.
                container, state = frame[2], frame[3]
                closing = "}" if container == 1 else "]"
                if kind == closing and state in (0, 1):
                    depth -= 1
                    node = ("o" if container == 1 else "a", frame[0], end, frame[1], None)
                    table.put(self.key("node", node[1]), node)
                    attach(node)
                    continue
                if state == 1:
                    if kind != ",":
                        raise ValueError("Exchange JSON separator is invalid")
                    frame[3] = 2
                    update(frame)
                    continue
                if container == 1 and state in (0, 2):
                    if kind != "s":
                        raise ValueError("Exchange JSON object key is invalid")
                    frame[3:] = [3, digest.encode("ascii").ljust(96, b"\0"), start, end]
                    update(frame)
                    continue
                if container == 1 and state == 3:
                    if kind != ":":
                        raise ValueError("Exchange JSON object colon is missing")
                    frame[3] = 4
                    update(frame)
                    continue
            elif self.root is not None:
                raise ValueError("Exchange JSON has trailing content")
            if kind in ("{", "["):
                frame = [start, 0, 1 if kind == "{" else 0, 0, b"\0" * 96, 0, 0]
                depth += 1
                update(frame)
            elif kind in ("s", "n", "t", "f", "z"):
                node = (kind, start, end, 0, digest)
                table.put(self.key("node", start), node)
                attach(node)
            else:
                raise ValueError("Exchange JSON value is invalid")
        stack.close()

    def value(self, offset=None):
        return JsonValue(self, self.root if offset is None else offset)


class JsonValue:
    """A checked JSON value; scalar/container materialization is explicit."""

    def __init__(self, document, offset):
        self.document, self.offset = document, offset
        self._cached_node = None

    @property
    def _node(self):
        self.document.owner._assert_open()
        if self._cached_node is None:
            self._cached_node = self.document.owner._table.get(self.document.key("node", self.offset))
        return self._cached_node

    @property
    def kind(self):
        return self._node[0]

    def get(self, key):
        if self.kind != "o":
            raise ValueError("Exchange JSON value must be an object")
        digest = self.document.owner._canonical_text(key)
        if digest is None:
            return None
        offset = self.document.owner._table.get(self.document.key("field", self.offset, digest))
        return None if offset is None else JsonValue(self.document, offset)

    def __iter__(self):
        kind, _, _, count, _ = self._node
        if kind not in ("a", "o"):
            raise ValueError("Exchange JSON value is not a container")
        for index in range(count):
            self.document.owner._assert_open()
            self.document.owner._check()
            child = self.document.owner._table.get(self.document.key("child", self.offset, index))
            yield JsonValue(self.document, child[2])

    def string_digest(self):
        node = self._node
        if node[0] != "s":
            raise ValueError("Exchange identity/reference must be a String")
        return node[4]

    def scalar(self):
        kind, start, end, _, _ = self._node
        if kind in ("o", "a"):
            raise ValueError("Use materialize() for a JSON container")
        self.document.source.seek(start)
        return json.loads(self.document.source.read(end - start), parse_constant=_invalid_json_constant,
                          parse_float=_finite_float)

    def is_string(self, text):
        return self.kind == "s" and self.string_digest() == self.document.owner._canonical_text(text)

    def materialize(self):
        """Allocate a Python value only at the caller's chosen boundary."""
        def allocate(value):
            return {} if value.kind == "o" else [] if value.kind == "a" else value.scalar()

        root = allocate(self)
        if self.kind not in ("a", "o"):
            return root
        frames = [(self, root, iter(self), 0)]
        while frames:
            value, output, children, index = frames[-1]
            try:
                child = next(children)
            except StopIteration:
                frames.pop()
                continue
            item = allocate(child)
            if value.kind == "o":
                entry = value.document.owner._table.get(value.document.key("child", value.offset, index))
                value.document.source.seek(entry[0])
                key = json.loads(value.document.source.read(entry[1] - entry[0]))
                output[key] = item
            else:
                output.append(item)
            frames[-1] = (value, output, children, index + 1)
            if child.kind in ("a", "o"):
                frames.append((child, item, iter(child), 0))
        return root


def _decoded_string(source, offset, check):
    """Stream canonical UTF-8 String bytes for full collision comparisons."""
    source.seek(offset)
    reader = _JsonInput(source, check)
    if reader.take() != 34:
        raise ValueError("Exchange String span is invalid")
    decoder = codecs.getincrementaldecoder("utf-8")("strict")
    while True:
        reader.peek()
        begin = reader.position
        while reader.position < len(reader.block) and reader.block[reader.position] not in (34, 92):
            reader.position += 1
        count = reader.position - begin
        reader.offset += count
        text = decoder.decode(reader.block[begin:reader.position])
        if text:
            yield text.encode("utf-8")
        if reader.position == len(reader.block):
            if not reader.block:
                raise ValueError("Exchange String span is incomplete")
            continue
        decoder.decode(b"", final=True)
        special = reader.take()
        if special == 34:
            return
        escape = reader.take()
        simple = {34: '"', 92: "\\", 47: "/", 98: "\b", 102: "\f", 110: "\n", 114: "\r", 116: "\t"}
        if escape == 117:
            code = reader._hex_code()
            if 0xD800 <= code <= 0xDBFF:
                reader.take()
                reader.take()
                code = 0x10000 + ((code - 0xD800) << 10) + reader._hex_code() - 0xDC00
            text = chr(code)
        else:
            text = simple[escape]
        yield text.encode("utf-8")
        decoder = codecs.getincrementaldecoder("utf-8")("strict")


def _finite_float(value):
    result = float(value)
    if not math.isfinite(result):
        raise ValueError("Exchange JSON contains a non-finite number")
    return result


def _validate_finite_number(source, start, end, check):
    # Track a bounded significant prefix and the decimal order. Saturating an
    # exponent beyond token length + 400 is enough to establish float overflow
    # or underflow even if its coefficient contains millions of digits.
    prefix = bytearray()
    integer_digits = leading_zeros = exponent = 0
    fractional = exponential = negative_exponent = nonzero = False
    bound = end - start + 400
    for offset in range(start, end, 64 * 1024):
        check()
        source.seek(offset)
        for char in source.read(min(64 * 1024, end - offset)):
            if char in (69, 101):
                exponential = True
            elif exponential:
                if char == 45:
                    negative_exponent = True
                elif 48 <= char <= 57:
                    exponent = min(bound, exponent * 10 + char - 48)
            elif char == 46:
                fractional = True
            elif 48 <= char <= 57:
                if not fractional:
                    integer_digits += 1
                if not nonzero and char == 48:
                    leading_zeros += 1
                else:
                    nonzero = True
                    if len(prefix) < 320:
                        prefix.append(char)
    if nonzero and (fractional or exponential):
        order = integer_digits - leading_zeros - 1 + (-exponent if negative_exponent else exponent)
        text = prefix.decode("ascii")
        _finite_float(f"{text[0]}.{text[1:]}e{order}")


def _parts_equal(first, second):
    a = b = b""
    first_done = second_done = False
    while True:
        while not a and not first_done:
            a = next(first, None)
            if a is None:
                first_done, a = True, b""
        while not b and not second_done:
            b = next(second, None)
            if b is None:
                second_done, b = True, b""
        if first_done or second_done:
            return first_done and second_done and not a and not b
        count = min(len(a), len(b))
        if a[:count] != b[:count]:
            return False
        a, b = a[count:], b[count:]


def _exact(source, length):
    value = source.read(length)
    if len(value) != length:
        raise ValueError("Exchange input is not a complete valid ZIP archive")
    return value


def _zip64_values(extra, values, widths):
    needed = [value == (0xFFFF if width == 2 else 0xFFFFFFFF) for value, width in zip(values, widths)]
    found = False
    position = 0
    while position < len(extra):
        if len(extra) - position < 4:
            raise ValueError("Exchange ZIP extra field is truncated")
        tag, length = struct.unpack_from("<HH", extra, position)
        position += 4
        data = extra[position:position + length]
        if len(data) != length:
            raise ValueError("Exchange ZIP extra field is truncated")
        position += length
        if tag == 1:
            if found:
                raise ValueError("Exchange ZIP64 extra field is duplicated")
            found = True
            cursor = 0
            for index, width in enumerate(widths):
                if needed[index]:
                    width = 4 if width == 2 else 8
                    if len(data) - cursor < width:
                        raise ValueError("Exchange ZIP64 extra field is incomplete")
                    values[index] = int.from_bytes(data[cursor:cursor + width], "little")
                    needed[index] = False
                    cursor += width
    if any(needed):
        raise ValueError("Exchange ZIP64 extra field is missing")
    return values


def _member_name(raw):
    try:
        name = raw.decode("ascii")
    except UnicodeDecodeError as exc:
        raise ValueError("Exchange ZIP contains an unsupported or unsafe entry") from exc
    if name not in ("manifest.json", "graph.json", "history.json") and not _RESOURCE_PATH.fullmatch(name):
        raise ValueError("Exchange ZIP contains an unsupported or unsafe entry")
    return name


class _Archive:
    """Stream ZIP64 central/local metadata without a ZipFile member cache."""

    def __init__(self, source, table, check):
        self.source, self.table, self.check = source, table, check
        source.seek(0, 2)
        size = source.tell()
        source.seek(max(0, size - 65557))
        tail = source.read(min(size, 65557))
        at = len(tail)
        while True:
            at = tail.rfind(b"PK\x05\x06", 0, at)
            if at < 0:
                raise ValueError("Exchange input is not a complete valid ZIP archive")
            if len(tail) - at >= 22 and at + 22 + int.from_bytes(tail[at + 20:at + 22], "little") == len(tail):
                break
        record = struct.unpack_from("<4s4H2IH", tail, at)
        _, disk, directory_disk, disk_count, count, directory_size, directory_offset, _ = record
        eocd_offset = size - len(tail) + at
        boundary = eocd_offset
        if disk or directory_disk or disk_count != count:
            raise ValueError("Exchange multi-disk ZIP archives are unsupported")
        if eocd_offset >= 20:
            source.seek(eocd_offset - 20)
            locator = _exact(source, 20)
            if locator[:4] == b"PK\x06\x07":
                _, disk64, offset64, disks = struct.unpack("<4sIQI", locator)
                source.seek(offset64)
                data = struct.unpack("<4sQ2H2I4Q", _exact(source, 56))
                if (data[0] != b"PK\x06\x06" or data[1] < 44 or disk64 or disks != 1 or
                        data[4] or data[5] or data[6] != data[7] or offset64 + 12 + data[1] != eocd_offset - 20):
                    raise ValueError("Exchange ZIP64 end record is invalid")
                for classic, actual, sentinel in ((count, data[7], 65535), (directory_size, data[8], 0xFFFFFFFF),
                                                   (directory_offset, data[9], 0xFFFFFFFF)):
                    if classic != sentinel and classic != actual:
                        raise ValueError("Exchange ZIP64 end records disagree")
                count, directory_size, directory_offset = data[7:10]
                boundary = offset64
            elif directory_size == 0xFFFFFFFF or directory_offset == 0xFFFFFFFF:
                raise ValueError("Exchange ZIP64 end record is missing")
        if directory_offset + directory_size != boundary:
            raise ValueError("Exchange ZIP central-directory bounds are invalid")
        self.count, self.directory_offset = count, directory_offset
        cursor = directory_offset
        for _ in range(count):
            check()
            source.seek(cursor)
            row = struct.unpack("<4s6H3I5H2I", _exact(source, 46))
            if row[0] != b"PK\x01\x02":
                raise ValueError("Exchange ZIP central-directory record is invalid")
            name = _member_name(_exact(source, row[10]))
            extra = _exact(source, row[11])
            cursor += 46 + row[10] + row[11] + row[12]
            if cursor > boundary:
                raise ValueError("Exchange ZIP central-directory record exceeds its bounds")
            length, compressed, offset, disk = _zip64_values(extra, [row[9], row[8], row[16], row[13]], [4, 4, 4, 2])
            if (disk or row[2] > 45 or row[4] not in (0, 8) or row[3] & ~0x80E or
                    stat.S_IFMT(row[15] >> 16) not in (0, stat.S_IFREG)):
                raise ValueError("Exchange ZIP entry structure is unsupported")
            if offset >= directory_offset or compressed > directory_offset - offset:
                raise ValueError("Exchange ZIP entry offset/length is invalid")
            if table.get(("entry", name)) is not None:
                raise ValueError("Exchange ZIP contains duplicate entry names")
            table.put(("entry", name), [offset, row[3], row[4], row[7], compressed, length, row[2]])
        if cursor != boundary:
            raise ValueError("Exchange ZIP central-directory size is inconsistent")
        if table.get(("entry", "manifest.json")) is None or table.get(("entry", "graph.json")) is None:
            raise ValueError("Exchange ZIP requires manifest.json and graph.json")

    def local_entries(self):
        cursor = count = 0
        while cursor < self.directory_offset:
            self.check()
            self.source.seek(cursor)
            row = struct.unpack("<4s5H3I2H", _exact(self.source, 30))
            if row[0] != b"PK\x03\x04":
                raise ValueError("Exchange ZIP local-header bounds are invalid")
            name = _member_name(_exact(self.source, row[9]))
            extra = _exact(self.source, row[10])
            info = self.table.get(("entry", name))
            if info is None or info[0] != cursor or row[2] != info[1] or row[3] != info[2] or row[1] != info[6]:
                raise ValueError("Exchange ZIP local and central headers disagree")
            length, compressed = _zip64_values(extra, [row[8], row[7]], [4, 4])
            if not row[2] & 8 and [row[6], compressed, length] != [info[3], info[4], info[5]]:
                raise ValueError("Exchange ZIP local integrity metadata disagrees")
            if row[2] & 8 and (row[6] not in (0, info[3]) or compressed not in (0, info[4]) or length not in (0, info[5])):
                raise ValueError("Exchange ZIP local descriptor metadata disagrees")
            payload = self.source.tell()
            end = payload + info[4]
            if end > self.directory_offset:
                raise ValueError("Exchange ZIP entries overlap the central directory")
            if row[2] & 8:
                self.source.seek(end)
                first = _exact(self.source, 4)
                if first == b"PK\x07\x08":
                    first = _exact(self.source, 4)
                    end += 4
                crc = int.from_bytes(first, "little")
                width = 8 if row[7] == 0xFFFFFFFF or row[8] == 0xFFFFFFFF else 4
                sizes = _exact(self.source, width * 2)
                if [crc, int.from_bytes(sizes[:width], "little"), int.from_bytes(sizes[width:], "little")] != [info[3], info[4], info[5]]:
                    raise ValueError("Exchange ZIP data descriptor disagrees")
                end += 4 + width * 2
            info[6] = payload
            self.table.put(("entry", name), info)
            yield name, info
            count += 1
            cursor = end
        if cursor != self.directory_offset or count != self.count:
            raise ValueError("Exchange ZIP local entries are missing, overlapping or duplicated")

    def chunks(self, info):
        self.source.seek(info[6])
        remaining, total, crc = info[4], 0, 0
        decoder = zlib.decompressobj(-15) if info[2] == 8 else None
        while remaining:
            self.check()
            raw = _exact(self.source, min(64 * 1024, remaining))
            remaining -= len(raw)
            while raw:
                self.check()
                block = decoder.decompress(raw, _CHUNK_SIZE) if decoder else raw
                raw = decoder.unconsumed_tail if decoder else b""
                total += len(block)
                crc = zlib.crc32(block, crc)
                if total > info[5]:
                    raise ValueError("Exchange ZIP expanded length exceeds central metadata")
                yield block
            if decoder and decoder.unused_data:
                raise ValueError("Exchange ZIP compressed entry has trailing content")
        if (decoder and not decoder.eof) or total != info[5] or crc != info[3]:
            raise ValueError("Exchange input is not a complete valid ZIP archive: CRC/length mismatch")


class ExchangePackageView:
    """Context-owned ZIP/JSON integrity-checked package view.

    The source is borrowed and must remain immutable and open for the lifetime
    of the view. All derived JSON, parse frames and join indexes use the supplied
    protected store. Closing deletes only records created by this view. Graph
    and history domain semantics are not validated locally; the Deposit Server
    is authoritative before it creates an import plan or applies effects.
    """

    def __init__(self, source, storage, check):
        self._source, self._storage, self._check = source, storage, check
        self._owned = _RetryCleanup()
        self._closed = False
        self._cleanup_complete = False
        self._documents = {}
        self._canonical_cache = OrderedDict()

    def _remember_string(self, digest, value):
        self._canonical_cache.pop(digest, None)
        self._canonical_cache[digest] = value
        if len(self._canonical_cache) > 128:
            self._canonical_cache.popitem(last=False)

    def _new_file(self, **kwargs):
        return ProtectedChunkFile(self._storage, registry=self._owned, **kwargs)

    def _canonical_text(self, text):
        raw = text.encode("utf-8")
        digest = hashlib.sha256(raw).hexdigest()
        cached = self._canonical_cache.get(digest)
        if cached is not None:
            canonical, name, offset = cached
            if _parts_equal(_decoded_string(self._documents[name].source, offset, self._check), iter((raw,))):
                self._remember_string(digest, cached)
                return canonical
        ordinal = 0
        while True:
            entry = self._table.get(("canonical-string", digest, ordinal))
            if entry is None:
                return None
            if _parts_equal(_decoded_string(self._documents[entry[0]].source, entry[1], self._check), iter((raw,))):
                canonical = f"{digest}:{ordinal}"
                self._remember_string(digest, (canonical, entry[0], entry[1]))
                return canonical
            ordinal += 1

    def _canonical_string(self, name, offset, digest):
        cached = self._canonical_cache.get(digest)
        if cached is not None:
            canonical, previous_name, previous_offset = cached
            if _parts_equal(_decoded_string(self._documents[previous_name].source, previous_offset, self._check),
                            _decoded_string(self._documents[name].source, offset, self._check)):
                self._remember_string(digest, cached)
                return canonical
        ordinal = 0
        while True:
            key = ("canonical-string", digest, ordinal)
            entry = self._table.get(key)
            if entry is None:
                self._table.put(key, [name, offset])
                canonical = f"{digest}:{ordinal}"
                self._remember_string(digest, (canonical, name, offset))
                return canonical
            first = _decoded_string(self._documents[entry[0]].source, entry[1], self._check)
            second = _decoded_string(self._documents[name].source, offset, self._check)
            if _parts_equal(first, second):
                canonical = f"{digest}:{ordinal}"
                self._remember_string(digest, (canonical, entry[0], entry[1]))
                return canonical
            ordinal += 1

    def _assert_open(self):
        if self._closed:
            raise ValueError("Exchange package view is closed")

    def __enter__(self):
        self._assert_open()
        return self

    def __exit__(self, *args):
        self.close()

    def close(self):
        if not self._cleanup_complete:
            self._closed = True
            self._canonical_cache.clear()
            self._owned.close()
            self._cleanup_complete = True

    @property
    def manifest(self):
        self._assert_open()
        return self._documents["manifest.json"].value()

    @property
    def graph(self):
        self._assert_open()
        return self._documents["graph.json"].value()

    @property
    def history(self):
        self._assert_open()
        document = self._documents.get("history.json")
        return None if document is None else document.value()

    def iter_records(self, category):
        """Yield checked lazy record views without allocating a category list."""
        self._assert_open()
        yield from _category(self.graph, category)

    def iter_content(self, entry):
        """Read a checked content entry in bounded blocks from the source ZIP."""
        self._assert_open()
        if not isinstance(entry, str) or not _RESOURCE_PATH.fullmatch(entry):
            raise ValueError("Exchange resource entry is unsafe")
        info = self._table.get(("entry", entry))
        if info is None:
            raise ValueError("Exchange resource entry is missing")
        # Each iterator borrows this synchronous source cursor. Concurrent
        # readers are unsupported, just as for the workflow-owned staging file.
        chunks = self._archive.chunks(info)
        while True:
            self._assert_open()
            try:
                block = next(chunks)
            except StopIteration:
                return
            yield block

    def materialize(self):
        self._assert_open()
        result = {"manifest": self.manifest.materialize(), "graph": self.graph.materialize()}
        if self.history is not None:
            result["history"] = self.history.materialize()
        return result

    def storage_usage_bytes(self):
        """Measure current protected-store disk usage, including temporary copies."""
        self._assert_open()
        return self._storage.usage_bytes()


def validate_exchange_archive(source, *, storage=None, checkpoint=None):
    """Validate transport/integrity and return a context-owned lazy view.

    ``storage`` must be protected recovery storage; a ProtectedChunkFile source
    supplies its own store by default. The caller owns and closes the returned
    view. No graph, history, member directory or resource-ID list is accumulated
    in Python memory. This verifies ZIP/JSON structure, manifest/file integrity,
    and resource content joins. It does not validate graph/history domain
    semantics; the Server performs authoritative validation before import plans
    or effects. Materialize explicitly only when the consumer needs it.
    """
    storage = storage if storage is not None else getattr(source, "_storage", None)
    if storage is None:
        raise ValueError("Exchange validation requires protected recovery storage")
    view = ExchangePackageView(source, storage, checkpoint or (lambda: None))
    try:
        view._table = _ProtectedIndex(view)
        archive = view._archive = _Archive(source, view._table, view._check)
        for name, info in archive.local_entries():
            digest = hashlib.sha256()
            staged = view._new_file() if name.endswith(".json") else None
            for block in archive.chunks(info):
                digest.update(block)
                if staged is not None:
                    staged.write(block)
            checksum = digest.hexdigest()
            if name.startswith("resources/") and checksum != name[10:]:
                raise ValueError("Exchange resource content does not match its path")
            view._table.put(("computed", name), [info[5], checksum])
            if staged is not None:
                staged.seek(0)
                document = _JsonDocument(view, name, staged)
                view._documents[name] = document
                document.parse()
        _validate_view(view)
        return view
    except (zlib.error, struct.error, UnicodeError) as exc:
        view.close()
        raise ValueError("Exchange input is not a complete valid ZIP archive or UTF-8 JSON") from exc
    except BaseException:
        view.close()
        raise


def _field(value, name):
    child = value.get(name)
    if child is None:
        raise ValueError(f"Exchange record is missing {name}")
    return child


def _identity(value, name):
    child = _field(value, name)
    if child._node[2] - child.offset <= 2:
        raise ValueError("Exchange identity/reference is empty")
    return child.string_digest()


def _category(value, name):
    child = value.get(name)
    if child is None:
        return iter(())
    if child.kind != "a":
        raise ValueError(f"Exchange {name} records must be an array")
    return iter(child)


def _counter(value):
    node = value._node
    if node[0] != "n" or node[2] - node[1] > 16:
        raise ValueError("Exchange metadata counter is invalid")
    result = value.scalar()
    if type(result) is not int or not 0 <= result <= 2 ** 53 - 1:
        raise ValueError("Exchange metadata counter is invalid")
    return result


def _entry_text(value):
    # An allowed decoded member name is at most 74 ASCII characters; a JSON
    # escape can use at most six input bytes for each one of those characters.
    if value.kind != "s" or value._node[2] - value.offset > 446:
        raise ValueError("Exchange member name is unsafe")
    return _member_name(value.scalar().encode("ascii", "strict"))


def _validate_view(view):
    table, check = view._table, view._check
    manifest, graph = view.manifest, view.graph
    if manifest.kind != "o" or graph.kind != "o":
        raise ValueError("Exchange manifest/graph must be JSON objects")
    for ordinal, _ in enumerate(manifest):
        key = table.get(manifest.document.key("child", manifest.offset, ordinal))
        if key[1] - key[0] > 6 * max(map(len, _MANIFEST_ALLOWED)) + 2:
            raise ValueError("Exchange manifest contains an unknown field")
        manifest.document.source.seek(key[0])
        name = json.loads(manifest.document.source.read(key[1] - key[0]))
        if name not in _MANIFEST_ALLOWED:
            raise ValueError("Exchange manifest contains an unknown field")
    version = _field(manifest, "depositVersion")
    if version.kind not in ("s", "z") or (version.kind == "s" and version.is_string("")):
        raise ValueError("Exchange depositVersion must be a nonempty Server version String or null")
    files = _field(manifest, "files")
    if files.kind != "o" or files._node[3] != view._archive.count - 1:
        raise ValueError("Exchange manifest file index does not match ZIP members")
    for ordinal, metadata in enumerate(files):
        check()
        key = table.get(files.document.key("child", files.offset, ordinal))
        if key[1] - key[0] > 446:
            raise ValueError("Exchange manifest file index contains an unsafe name")
        files.document.source.seek(key[0])
        name = _member_name(json.loads(files.document.source.read(key[1] - key[0])).encode("ascii"))
        actual = table.get(("computed", name))
        if (name == "manifest.json" or actual is None or _counter(_field(metadata, "bytes")) != actual[0]
                or not _field(metadata, "sha256").is_string(actual[1])):
            raise ValueError(f"Exchange file integrity metadata mismatch: {name}")
    resources = _field(graph, "resources")
    if resources.kind != "a":
        raise ValueError("Exchange graph resource records are invalid")
    for record in resources:
        check()
        identity = _identity(record, "id")
        if table.get(("resource", identity)) is not None:
            raise ValueError("Exchange resource identity is duplicated")
        name = _entry_text(_field(record, "entry"))
        actual = table.get(("computed", name))
        if not name.startswith("resources/") or actual is None:
            raise ValueError("Exchange resource record references missing or unsafe content")
        if not _field(record, "sha256").is_string(name[10:]):
            raise ValueError("Exchange resource checksum contradicts its entry")
        if _counter(_field(record, "bytes")) != actual[0]:
            raise ValueError("Exchange resource length contradicts its content")
        table.put(("resource", identity), record.offset)
        table.put(("referenced", name), True)
    for ordinal, _ in enumerate(files):
        key = table.get(files.document.key("child", files.offset, ordinal))
        files.document.source.seek(key[0])
        name = json.loads(files.document.source.read(key[1] - key[0]))
        if name.startswith("resources/") and table.get(("referenced", name)) is None:
            raise ValueError("Exchange archive contains unreferenced resource content")
    # Entity, Pattern, geometry, typed-value and history semantics belong to
    # the Server. Python validates the portable transport and resource bytes
    # before sending the immutable package for authoritative Server validation.


def _iter_json_bytes(root, check):
    """Encode JSON with buffers bounded independently of scalar/container size.

    JSONEncoder.iterencode still emits an entire String as one token. Split
    Strings before escaping, and keep iterators rather than queuing siblings.
    The stack and cycle detector grow only with active nesting depth.
    """
    def string_parts(value):
        yield b'"'
        for start in range(0, len(value), 64 * 1024):
            check()
            yield json.dumps(value[start:start + 64 * 1024], ensure_ascii=False)[1:-1].encode("utf-8")
        yield b'"'

    def scalar_parts(value):
        if isinstance(value, str):
            yield from string_parts(value)
        else:
            yield json.dumps(value, ensure_ascii=False, allow_nan=False).encode("utf-8")

    def key_parts(key):
        # Match json.dumps' accepted object-key coercions.
        if isinstance(key, str):
            yield from string_parts(key)
        elif key is None or isinstance(key, (bool, int, float)):
            yield from string_parts(json.dumps(key, allow_nan=False))
        else:
            raise TypeError("JSON object keys must be strings, numbers, booleans or null")

    def parts():
        active = set()
        frames = []
        value = root
        while True:
            check()
            if isinstance(value, (dict, list, tuple)):
                identity = id(value)
                if identity in active:
                    raise ValueError("Circular reference detected")
                active.add(identity)
                mapping = isinstance(value, dict)
                yield b"{" if mapping else b"["
                frames.append((iter(value.items() if mapping else value), mapping, identity, True))
            else:
                yield from scalar_parts(value)
            while frames:
                iterator, mapping, identity, first = frames[-1]
                try:
                    item = next(iterator)
                except StopIteration:
                    frames.pop()
                    active.remove(identity)
                    yield b"}" if mapping else b"]"
                    continue
                if not first:
                    yield b","
                else:
                    frames[-1] = (iterator, mapping, identity, False)
                if mapping:
                    key, value = item
                    yield from key_parts(key)
                    yield b":"
                else:
                    value = item
                break
            else:
                return

    buffer = bytearray()
    for part in parts():
        if len(buffer) + len(part) > _CHUNK_SIZE:
            yield bytes(buffer)
            buffer.clear()
        buffer.extend(part)
    if buffer:
        yield bytes(buffer)


def _invalid_json_constant(value):
    raise ValueError(f"Exchange JSON contains a non-finite number: {value}")
