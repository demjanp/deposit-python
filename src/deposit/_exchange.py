"""Opaque Exchange intake, relay and leased reads."""

from base64 import b64decode, b64encode
from collections.abc import Mapping, Sequence
from contextlib import contextmanager, suppress
from hashlib import sha256
from io import BytesIO, RawIOBase
import json
import os
from pathlib import Path
import re
import tempfile

from .errors import DepositError, OperationCommittedAfterCancel, OutcomeUnknown, TransportError


CHUNK_BYTES = 1024 * 1024
_RESOURCE = re.compile(r"resources/[a-f0-9]{64}")
_DIGEST = re.compile(r"[a-f0-9]{64}")


def _invalid(message):
    return DepositError("INVALID_RESPONSE", message)


class ExchangeBundle:
    """An immutable package reference with one fixed Server reader lease."""

    def __init__(self, endpoint, reference):
        self._endpoint = endpoint
        self._reference = dict(reference)
        self._closed = False
        self._inspection = None
        if (not all(isinstance(reference.get(key), str) for key in ("packageId", "grant", "sha256"))
                or not _DIGEST.fullmatch(reference["sha256"])
                or type(reference.get("bytes")) is not int or reference["bytes"] < 0):
            raise _invalid("Invalid Exchange package reference")

    @property
    def source(self):
        return {name: self._reference[name] for name in ("packageId", "grant")}

    def _request(self, method, *arguments):
        if self._closed:
            raise ValueError("Exchange bundle is closed")
        return self._endpoint.call(f"deposit.exchange.{method}", *self.source.values(), *arguments)

    def _inspect(self):
        if self._closed:
            raise ValueError("Exchange bundle is closed")
        if self._inspection is None:
            envelope = self._request("inspect")
            if (not isinstance(envelope.get("leaseId"), str) or not isinstance(envelope.get("manifest"), dict)
                    or not isinstance(envelope.get("collections"), dict)):
                raise _invalid("Invalid Exchange inspection envelope")
            self._inspection = envelope
        return self._inspection

    @property
    def manifest(self):
        fields = self._inspect()["manifest"]
        return {name: self._value({"type": value["kind"], **value}) if isinstance(value, dict) else value
                for name, value in fields.items()}

    @property
    def graph(self):
        envelope = self._inspect()
        manifest = envelope["manifest"]
        return {name: self._value({"type": descriptor["kind"], "collection": name, **descriptor})
                for name, descriptor in envelope["collections"].items() if name not in manifest}

    def _pages(self, collection):
        lease = self._inspect()["leaseId"]
        method = "memberPage" if collection == "files" else "graphPage"
        arguments = (lease,) if collection == "files" else (lease, collection)
        cursor = None
        while True:
            page = self._request(method, *arguments, cursor, 256)
            entries = page.get("entries")
            if (page.get("packageId") != self._reference["packageId"] or page.get("collection") != collection
                    or not isinstance(entries, list) or len(entries) > 256):
                raise _invalid("Invalid Exchange collection page")
            yield from entries
            following = page.get("nextCursor")
            if following is None:
                return
            if not isinstance(following, dict) or not entries or following == cursor:
                raise _invalid("Invalid Exchange collection cursor")
            cursor = following

    def _value(self, record):
        kind = record.get("type")
        if kind == "scalar":
            return record["value"]
        name = record.get("collection")
        if not isinstance(name, str):
            raise _invalid("Invalid Exchange collection descriptor")
        if kind == "text":
            return _Text(self, name)
        count = record.get("count")
        if type(count) is not int or count < 0:
            raise _invalid("Invalid Exchange collection count")
        if kind == "mapping":
            return _Mapping(self, name, count)
        if kind == "sequence":
            return _Sequence(self, name, count)
        raise _invalid("Unknown Exchange value type")

    def open_resource(self, entry):
        if not isinstance(entry, str) or not _RESOURCE.fullmatch(entry):
            raise ValueError("Invalid Exchange resource member")
        metadata = self.manifest["files"][entry]
        return _Reader(self, entry, metadata["bytes"], metadata["sha256"])

    def open_package_stream(self):
        self._inspect()
        return _Reader(self, "package", self._reference["bytes"], self._reference["sha256"])

    def save(self, path):
        target = Path(path)
        temporary = None
        try:
            with self.open_package_stream() as source, tempfile.NamedTemporaryFile(
                mode="wb", dir=target.parent, prefix=".deposit-", suffix=".tmp", delete=False
            ) as output:
                temporary = Path(output.name)
                while block := source.read(CHUNK_BYTES):
                    output.write(block)
                output.flush()
                os.fsync(output.fileno())
            os.replace(temporary, target)
            temporary = None
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)

    def close(self):
        if not self._closed:
            try:
                with suppress(DepositError, TransportError):
                    self._request("discard")
            finally:
                self._closed = True
                self._endpoint.close()

    def __enter__(self):
        return self

    def __exit__(self, error_type, *_):
        if error_type is None:
            self.close()
        else:
            with suppress(Exception):
                self.close()


class _Mapping(Mapping):
    def __init__(self, bundle, collection, count):
        self._bundle, self._collection, self._count = bundle, collection, count

    def __len__(self):
        return self._count

    def items(self):
        return self._items()

    def _items(self):
        for entry in self._bundle._pages(self._collection):
            if self._collection == "files":
                yield entry["member"], {"bytes": entry["bytes"], "sha256": entry["sha256"]}
            else:
                key = self._bundle._value(entry["key"])
                yield str(key) if isinstance(key, _Text) else key, self._bundle._value(entry["value"])

    def __iter__(self):
        for key, _ in self._items():
            yield key

    def __getitem__(self, key):
        for current, value in self._items():
            if current == key:
                return value
        raise KeyError(key)


class _Sequence(Sequence):
    def __init__(self, bundle, collection, count):
        self._bundle, self._collection, self._count = bundle, collection, count

    def __len__(self):
        return self._count

    def __iter__(self):
        for entry in self._bundle._pages(self._collection):
            yield self._bundle._value(entry)

    def __getitem__(self, index):
        if isinstance(index, slice):
            return [self[position] for position in range(*index.indices(len(self)))]
        wanted = index if index >= 0 else len(self) + index
        if not 0 <= wanted < len(self):
            raise IndexError(index)
        for position, entry in enumerate(self):
            if position == wanted:
                return entry
        raise _invalid("Exchange collection ended before its declared count")


class _Text:
    def __init__(self, bundle, collection):
        self._bundle, self._collection = bundle, collection

    def chunks(self):
        for entry in self._bundle._pages(self._collection):
            yield entry["value"]

    def __str__(self):
        return "".join(self.chunks())


class _Reader(RawIOBase):
    def __init__(self, bundle, member, size, checksum):
        if type(size) is not int or size < 0 or not isinstance(checksum, str) or not _DIGEST.fullmatch(checksum):
            raise _invalid("Invalid Exchange member metadata")
        self._bundle, self._member, self._size, self._checksum = bundle, member, size, checksum
        self._offset = 0
        self._digest = sha256()
        self._verified = False

    def readable(self):
        return True

    def readinto(self, target):
        if self.closed:
            raise ValueError("Exchange stream is closed")
        if self._verified or not target:
            return 0
        lease = self._bundle._inspect()["leaseId"]
        limit = min(len(target), CHUNK_BYTES)
        result = self._bundle._request("readChunk", lease, self._member, self._offset, limit)
        encoded = result.get("bytesBase64")
        if not isinstance(encoded, str) or len(encoded) > 4 * ((limit + 2) // 3):
            raise _invalid("Invalid Exchange chunk")
        try:
            block = b64decode(encoded, validate=True)
        except ValueError as exc:
            raise _invalid("Invalid Exchange chunk encoding") from exc
        if (result.get("offset") != self._offset or result.get("length") != len(block)
                or len(block) != min(limit, self._size - self._offset)):
            raise _invalid("Invalid Exchange chunk position or length")
        self._digest.update(block)
        self._offset += len(block)
        if self._offset == self._size:
            if self._digest.hexdigest() != self._checksum:
                raise DepositError("RESOURCE_CORRUPT", "Exchange stream checksum differs")
            self._verified = True
        target[:len(block)] = block
        return len(block)


@contextmanager
def _provider(source):
    if isinstance(source, bytes):
        with BytesIO(source) as stream:
            yield stream
    elif isinstance(source, (str, os.PathLike)):
        with Path(source).open("rb") as stream:
            yield stream
    elif callable(getattr(source, "read", None)):
        yield source
    elif callable(source):
        with source() as stream:
            if not callable(getattr(stream, "read", None)):
                raise TypeError("Exchange provider must return a binary stream")
            yield stream
    else:
        raise TypeError("Exchange provider must be bytes, a path, or a binary stream")


def _stream_blocks(stream):
    while True:
        block = stream.read(CHUNK_BYTES)
        if not isinstance(block, bytes) or len(block) > CHUNK_BYTES:
            raise TypeError("Exchange provider must return bounded bytes")
        if not block:
            return
        yield block


def _upload(execution, upload, member, blocks):
    offset = 0
    for block in blocks:
        _chunk(execution, upload, member, offset, block)
        offset += len(block)
    if offset == 0 and member.startswith("resources/"):
        _chunk(execution, upload, member, 0, b"")


def _chunk(execution, upload, member, offset, block):
    result = execution.run("deposit.exchange.uploadChunk", upload, member, offset,
                           b64encode(block).decode("ascii"), sha256(block).hexdigest())
    if (result.get("uploadId"), result.get("member"), result.get("nextOffset")) != (upload, member, offset + len(block)):
        raise _invalid("Exchange upload acknowledgement differs")


def stage_zip(endpoint, source):
    """Send original package bytes from the current stream position."""
    with _provider(source) as stream, _intake(endpoint, "zip", {}) as (execution, upload):
        _upload(execution, upload, "package", _stream_blocks(stream))
        return execution.run("deposit.exchange.finishUpload", upload)


@contextmanager
def _intake(endpoint, kind, metadata):
    with endpoint.execution() as execution:
        reference = execution.run("deposit.exchange.beginUpload", endpoint.scope, kind, metadata)
        upload = reference.get("uploadId")
        if not isinstance(upload, str):
            raise _invalid("Exchange upload acknowledgement has no identity")
        try:
            yield execution, upload
        except (OutcomeUnknown, OperationCommittedAfterCancel):
            raise
        except BaseException:
            with suppress(Exception):
                execution.run("deposit.exchange.abortUpload", upload)
            raise



def _json_fragments(value):
    if isinstance(value, str):
        yield b'"'
        for start in range(0, len(value), 8192):
            text = json.dumps(value[start:start + 8192], ensure_ascii=True)
            yield text[1:-1].encode("ascii")
        yield b'"'
    elif isinstance(value, Mapping):
        yield b"{"
        for index, (key, child) in enumerate(value.items()):
            if not isinstance(key, str):
                raise TypeError("Exchange JSON keys must be strings")
            if index:
                yield b","
            yield from _json_fragments(key)
            yield b":"
            yield from _json_fragments(child)
        yield b"}"
    elif isinstance(value, (list, tuple, _Sequence)):
        yield b"["
        for index, child in enumerate(value):
            if index:
                yield b","
            yield from _json_fragments(child)
        yield b"]"
    elif isinstance(value, _Text):
        yield b'"'
        for chunk in value.chunks():
            yield json.dumps(chunk, ensure_ascii=True)[1:-1].encode("ascii")
        yield b'"'
    else:
        yield json.dumps(value, allow_nan=False).encode("ascii")


def _json_blocks(value):
    buffer = bytearray()
    for fragment in _json_fragments(value):
        if len(buffer) + len(fragment) > CHUNK_BYTES:
            yield bytes(buffer)
            buffer.clear()
        buffer.extend(fragment)
    if buffer:
        yield bytes(buffer)


def stage_snapshot(endpoint, snapshot, *, resources=None, resource_source=None):
    """Encode structured input and send its resource providers without creating a ZIP."""
    if not isinstance(snapshot, Mapping):
        from .networkx import from_networkx
        snapshot = from_networkx(snapshot)
    manifest, graph, history = snapshot["manifest"], snapshot["graph"], snapshot.get("history")
    if not isinstance(manifest, Mapping) or not isinstance(graph, Mapping):
        raise TypeError("Exchange manifest and graph must be mappings")
    manifest = {key: value for key, value in manifest.items() if key not in {"files", "packageId", "depositVersion"}}
    if resources is not None and not isinstance(resources, Mapping):
        raise TypeError("Exchange resources must map member names to providers")
    explicit = resources if resources is not None else {}
    if resource_source is not None:
        if isinstance(resource_source, ExchangeBundle):
            namespace = resource_source.manifest.get("sourceNamespaceId")
        else:
            from .store import Store
            if not isinstance(resource_source, Store):
                raise TypeError("Exchange resource source must be an authenticated Store or bundle")
            namespace = resource_source.namespace_id
        if namespace != manifest.get("sourceNamespaceId"):
            raise ValueError("Resource source belongs to another Namespace")
    with _intake(endpoint, "snapshot", {"includeHistory": history is not None}) as (execution, upload):
        _upload(execution, upload, "manifest", _json_blocks(manifest))
        _upload(execution, upload, "graph", _json_blocks(graph))
        if history is not None:
            _upload(execution, upload, "history", _json_blocks(history))
        for entry, source in explicit.items():
            if not isinstance(entry, str) or not _RESOURCE.fullmatch(entry):
                raise ValueError("Resource providers must use content-addressed member names")
            with _provider(source) as stream:
                _upload(execution, upload, entry, _stream_blocks(stream))
        staged = set(explicit)
        for record in graph.get("resources", ()):
            if not isinstance(record, Mapping) or not isinstance(record.get("entry"), str):
                raise TypeError("Exchange resource records must identify a member")
            entry = record["entry"]
            if entry in explicit:
                continue
            if isinstance(resource_source, ExchangeBundle):
                provider = lambda: resource_source.open_resource(entry)
            elif resource_source is not None:
                from ._imports import _store_resource_provider
                provider = _store_resource_provider(resource_source, record["id"])
            else:
                raise ValueError(f"No content provider for Exchange resource {entry!r}")
            with _provider(provider) as stream:
                if entry in staged:
                    # Opening the exact source resource above still checks its
                    # identity and access when several IDs share its content.
                    continue
                _upload(execution, upload, entry, _stream_blocks(stream))
                staged.add(entry)
        return execution.run("deposit.exchange.finishUpload", upload)


def package_for_target(bundle, endpoint):
    """Hand off a still-authorized local grant, or verify an opaque relay."""
    if bundle._endpoint.live_identity() == endpoint.live_identity():
        first = bundle._pages("files")
        next(first, None)
        return bundle.source
    with bundle.open_package_stream() as reader:
        verified = stage_zip(endpoint, reader)
    if (verified.get("bytes"), verified.get("sha256")) != (
            bundle._reference["bytes"], bundle._reference["sha256"]):
        with suppress(Exception):
            endpoint.call("deposit.exchange.discard", verified["packageId"], verified["grant"])
        raise DepositError("RESOURCE_CORRUPT", "Receiving package digest differs from the source")
    return {name: verified[name] for name in ("packageId", "grant")}
