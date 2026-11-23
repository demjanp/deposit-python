"""Lazy, bounded iteration over revision-bound portable query pages."""

from __future__ import annotations

import json
import math
from typing import Any, Iterator
from collections.abc import Mapping as MappingABC, Sequence as SequenceABC
from types import MappingProxyType

from .errors import DepositError


class QueryPage(dict):
    """Portable page payload; revision and scope remain opaque server cursor state."""


class QueryIterator:
    """Fetch pages only when needed; closing never replays or restarts a cursor."""

    def __init__(self, store, querystr: str, parameters: dict[str, Any], page_size: int):
        self._store = store
        self._querystr = querystr
        self._parameters = parameters
        self._page_size = page_size
        self._cursor: str | None = None
        self._seen_cursors: set[str] = set()
        self._rows: list[dict[str, Any]] = []
        self._row_index = 0
        self._columns = None
        self._first = True
        self._closed = False
        self._finished = False

    @property
    def closed(self) -> bool:
        return self._closed

    def __iter__(self):
        return self

    def __next__(self):
        if self._closed or self._finished:
            raise StopIteration
        while self._row_index >= len(self._rows):
            if not self._first and self._cursor is None:
                self._finished = True
                self.close()
                raise StopIteration
            cursor = None if self._first else self._cursor
            try:
                page = self._store.get_query_page(
                    self._querystr, parameters=self._parameters, page_size=self._page_size, cursor=cursor
                )
                self._validate_page(page)
            except BaseException:
                self.close()
                raise
            self._first = False
            self._rows = page["rows"]
            self._row_index = 0
            self._cursor = page["nextCursor"]
            if self._cursor is not None:
                if self._cursor in self._seen_cursors:
                    self.close()
                    raise DepositError("INVALID_RESPONSE", "Query page cursor repeated")
                self._seen_cursors.add(self._cursor)
            if not self._rows and self._cursor is None:
                self._finished = True
                self.close()
                raise StopIteration
        row = self._rows[self._row_index]
        self._row_index += 1
        return row

    def _validate_page(self, page):
        if not isinstance(page, dict) or set(page) != {"columns", "rows", "nextCursor"}:
            raise DepositError("INVALID_RESPONSE", "Query iterator received an invalid page")
        columns = tuple((column["name"], column["type"]) for column in page["columns"])
        if self._columns is None:
            self._columns = columns
        elif self._columns != columns:
            raise DepositError("REVISION_CONFLICT", "Query page schema changed during iteration")

    def materialize(self, *, max_items: int = 10_000) -> list[dict[str, Any]]:
        if type(max_items) is not int or max_items < 0:
            raise ValueError("max_items must be a nonnegative integer")
        output = []
        try:
            for item in self:
                if len(output) >= max_items:
                    raise ValueError(f"Query materialization exceeds max_items={max_items}")
                output.append(item)
        except BaseException:
            self.close()
            raise
        return output

    def close(self):
        self._closed = True
        self._rows = []
        self._row_index = 0

    def __enter__(self):
        if self._closed:
            raise RuntimeError("Query iterator is closed")
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.close()


class EntityPageIterator:
    """Lazy consumer for one catalogued Domain *Page procedure."""

    def __init__(self, store, procedure: str, selection, *, page_size: int = 200, transform=None,
                 expected_revision: int | None = None):
        if type(page_size) is not int or not 1 <= page_size <= 1000:
            raise ValueError("page_size must be an integer from 1 through 1000")
        self._store = store
        self._procedure = procedure
        self._selection = selection
        self._page_size = page_size
        self._transform = transform or (lambda value, revision: value)
        self._revision = expected_revision
        self._cursor = None
        self._seen_cursors = set()
        self._items = []
        self._index = 0
        self._first = True
        self._closed = False

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def revision(self) -> int | None:
        """Checked Namespace revision, retained even after the final page closes."""
        return self._revision

    def __iter__(self):
        return self

    def __next__(self):
        if self._closed:
            raise StopIteration
        while self._index >= len(self._items):
            if not self._first and self._cursor is None:
                self.close()
                raise StopIteration
            options = {"pageSize": self._page_size}
            if not self._first:
                options["cursor"] = self._cursor
            try:
                page, revision = self._store._checked_entity_page(
                    self._procedure, self._selection, options, expected_revision=self._revision
                )
                self._validate_page(page)
            except BaseException:
                self.close()
                raise
            self._first = False
            self._revision = revision
            self._items = page["items"]
            self._index = 0
            self._cursor = page["nextCursor"]
            if self._cursor is not None:
                if self._cursor in self._seen_cursors:
                    self.close()
                    raise DepositError("INVALID_RESPONSE", "Entity page cursor repeated")
                self._seen_cursors.add(self._cursor)
            if not self._items and self._cursor is None:
                self.close()
                raise StopIteration
        item = self._items[self._index]
        self._index += 1
        try:
            return self._transform(item, self._revision)
        except BaseException:
            self.close()
            raise

    def _validate_page(self, page):
        if (not isinstance(page, dict) or not isinstance(page.get("items"), list)
                or "nextCursor" not in page):
            raise DepositError("INVALID_RESPONSE", "Entity page returned invalid items")
        cursor = page.get("nextCursor")
        if cursor is not None and (not isinstance(cursor, str) or not cursor):
            raise DepositError("INVALID_RESPONSE", "Entity page returned an invalid cursor")
        if any(not isinstance(item, dict) for item in page["items"]):
            raise DepositError("INVALID_RESPONSE", "Entity page contains an invalid item")

    def materialize(self, *, max_items: int = 10_000):
        if type(max_items) is not int or max_items < 0:
            raise ValueError("max_items must be a nonnegative integer")
        result = []
        try:
            for item in self:
                if len(result) >= max_items:
                    raise ValueError(f"Page materialization exceeds max_items={max_items}")
                result.append(item)
        except BaseException:
            self.close()
            raise
        return result

    def close(self):
        self._closed = True
        self._items = []
        self._index = 0

    def __enter__(self):
        if self._closed:
            raise RuntimeError("Entity page iterator is closed")
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.close()


_MAX_SAFE_INTEGER = (1 << 63) - 1
_PAGE_LIMIT = 256
_PAGE_BYTES = 512 * 1024
_ENTRY_BYTES = 32 * 1024
_TEXT_BYTES = 16 * 1024


def _nonnegative_long(value):
    return type(value) is int and 0 <= value <= _MAX_SAFE_INTEGER


def _encoded_size(value):
    return len(json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))


def _invalid_result(message):
    return DepositError("INVALID_RESPONSE", message)


class ImportResult(MappingABC):
    """Immutable result envelope with collections read on demand from the Server."""

    _FIELDS = ("format", "resultId", "leaseId", "leaseExpiresAt", "operationId", "kind",
               "state", "namespaceId", "planId", "revision", "expiresAt", "collections")

    def __init__(self, request, envelope: dict[str, Any], *, plan_observation=None):
        if not valid_import_result_envelope(envelope):
            raise _invalid_result("Invalid import result envelope")
        if plan_observation is not None and not _valid_plan_observation(plan_observation, envelope):
            raise _invalid_result("Import plan observation does not match its immutable result")
        self._request = request
        immutable = dict(envelope)
        immutable["collections"] = MappingProxyType({
            key: MappingProxyType(dict(value)) for key, value in envelope["collections"].items()
        })
        self._envelope = MappingProxyType(immutable)
        self._plan_observation = (None if plan_observation is None
                                  else MappingProxyType(dict(plan_observation)))
        self._closed = False

    def _check_open(self):
        if self._closed:
            raise RuntimeError("ImportResult is closed")

    def _read(self, suffix, *arguments):
        request = self._request
        if request is None:
            self._check_open()
        return request("deposit.get.importResult" + suffix, self.result_id,
                             self._envelope["leaseId"], *arguments)

    def _entries(self, collection, count=None):
        position = 0
        cursor = None
        while True:
            page = self._read("Page", collection, cursor, _PAGE_LIMIT)
            if (not isinstance(page, dict) or set(page) != {"resultId", "collection", "entries", "nextCursor"}
                    or page["resultId"] != self.result_id or page["collection"] != collection
                    or not isinstance(page["entries"], list) or len(page["entries"]) > _PAGE_LIMIT):
                raise _invalid_result("Invalid import result page")
            entries = page["entries"]
            sizes = [_encoded_size(entry) for entry in entries]
            if any(size > _ENTRY_BYTES for size in sizes) or sum(sizes) > _PAGE_BYTES:
                raise _invalid_result("Import result page exceeds its byte bounds")
            position += len(entries)
            cursor = page["nextCursor"]
            if cursor is not None and (
                not isinstance(cursor, dict)
                or set(cursor) != {"resultId", "leaseId", "collection", "position", "integrity"}
                or cursor["resultId"] != self.result_id or cursor["leaseId"] != self._envelope["leaseId"]
                or cursor["collection"] != collection or not _nonnegative_long(cursor["position"])
                or cursor["position"] != position or not entries
                or not isinstance(cursor["integrity"], str) or not cursor["integrity"]
            ):
                raise _invalid_result("Invalid import result cursor")
            if count is not None and (position > count or (cursor is None) != (position == count)):
                raise _invalid_result("Import result collection count differs from its descriptor")
            for entry in entries:
                self._check_open()
                yield entry
            if cursor is None:
                return

    @property
    def result_id(self):
        return self._envelope["resultId"]

    @property
    def operation_id(self):
        return self._envelope["operationId"]

    @property
    def state(self):
        return self._envelope["state"]

    @property
    def plan_observation(self):
        """The separately observed current plan lifecycle, if this is an inspection result."""
        return self._plan_observation

    @property
    def plan_state(self):
        return None if self._plan_observation is None else self._plan_observation["state"]

    @property
    def plan_id(self):
        return self._envelope["planId"]

    def __getitem__(self, key):
        if self._closed:
            raise RuntimeError("ImportResult is closed")
        if key == "plan" and self._plan_observation is not None:
            return self._plan_observation
        if key == "plan":
            raise KeyError(key)
        if key in self._envelope:
            return self._envelope[key]
        descriptor = self._envelope["collections"].get(key)
        if descriptor is None:
            raise KeyError(key)
        return _lazy_collection(key, descriptor["kind"], descriptor["count"], self)

    def __iter__(self):
        plan = ("plan",) if self._plan_observation is not None else ()
        return iter((*self._FIELDS, *plan, *self._envelope["collections"].keys()))

    def __len__(self):
        return len(self._FIELDS) + (self._plan_observation is not None) + len(self._envelope["collections"])

    def close(self):
        """Close this view. The Server lease expires at its fixed deadline."""
        self._closed = True
        self._request = None

    @property
    def closed(self):
        return self._closed

    def __enter__(self):
        if self._closed:
            raise RuntimeError("ImportResult is closed")
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.close()


class CloneResult(ImportResult):
    """Receipt-bound lazy result for a full Namespace clone."""

    @property
    def namespace_id(self):
        return self._envelope["namespaceId"]

    @property
    def name(self):
        return self["metadata"]["name"]

    @property
    def identity_mappings(self):
        return self["identityMappings"]

    @property
    def resource_mapping(self):
        return self["resourceMapping"]


def _lazy_collection(collection, kind, count, owner):
    if kind == "mapping":
        return LazyResultMapping(collection, count, owner)
    return LazyResultSequence(collection, count, owner)


class _LazyResultBase:
    def __init__(self, collection, count, owner):
        self._collection = collection
        self._count = count
        self._owner = owner

    def _entries(self):
        return self._owner._entries(self._collection, self._count)


class LazyResultSequence(_LazyResultBase, SequenceABC):
    """Indexable, bounded-page sequence backed by remote pages."""

    def __len__(self):
        return self._count

    def __iter__(self):
        for descriptor in self._entries():
            yield _decode_import_value(descriptor, self._owner)

    def __getitem__(self, index):
        if isinstance(index, slice):
            raise TypeError("Lazy result sequences do not materialize slices")
        if type(index) is not int:
            raise TypeError("Import result sequence indices must be integers")
        resolved = index + self._count if index < 0 else index
        if not 0 <= resolved < self._count:
            raise IndexError(index)
        for position, value in enumerate(self):
            if position == resolved:
                return value
        raise IndexError(index)


class LazyResultMapping(_LazyResultBase, MappingABC):
    """Read-only mapping whose entries are decoded one bounded page at a time."""

    def __len__(self):
        return self._count

    def _pairs(self):
        for entry in self._entries():
            if isinstance(entry, dict) and set(entry) == {"key", "value"}:
                key, value = entry["key"], entry["value"]
            else:
                raise DepositError("INVALID_RESPONSE", "Import mapping entry is invalid")
            decoded_key = _decode_import_value(key, self._owner)
            if not isinstance(decoded_key, (str, LazyText)):
                raise _invalid_result("Invalid import mapping key")
            yield decoded_key, _decode_import_value(value, self._owner)

    def __iter__(self):
        for key, _value in self._pairs():
            yield key

    def items(self):
        return self._pairs()

    def __getitem__(self, key):
        if isinstance(key, str) and len(key.encode("utf-8")) <= _TEXT_BYTES:
            response = self._owner._read("Entry", self._collection, key)
            if (not isinstance(response, dict) or type(response.get("found")) is not bool
                    or response.get("resultId") != self._owner.result_id
                    or response.get("collection") != self._collection):
                raise _invalid_result("Invalid import mapping lookup")
            fields = {"resultId", "collection", "found"}
            if response["found"] and set(response) == fields | {"value"}:
                if _encoded_size(response["value"]) > _ENTRY_BYTES:
                    raise _invalid_result("Import mapping lookup exceeds its byte bound")
                return _decode_import_value(response["value"], self._owner)
            if not response["found"] and set(response) == fields:
                raise KeyError(key)
            raise _invalid_result("Invalid import mapping lookup")
        for actual_key, value in self._pairs():
            if actual_key == key:
                return value
        raise KeyError(key)


def _decode_import_value(descriptor: Any, owner: ImportResult):
    if not isinstance(descriptor, dict) or not isinstance(descriptor.get("type"), str):
        raise _invalid_result("Invalid import result descriptor")
    kind = descriptor["type"]
    if kind == "scalar" and set(descriptor) == {"type", "value"}:
        value = descriptor["value"]
        if (value is None or isinstance(value, (str, bool))
                or type(value) is int and -_MAX_SAFE_INTEGER - 1 <= value <= _MAX_SAFE_INTEGER
                or type(value) is float and math.isfinite(value)):
            return value
    collection = descriptor.get("collection")
    if isinstance(collection, str) and 0 < len(collection) <= 512:
        if (kind in {"mapping", "sequence"} and set(descriptor) == {"type", "collection", "count"}
                and _nonnegative_long(descriptor["count"])):
            return _lazy_collection(collection, kind, descriptor["count"], owner)
        if (kind == "text" and set(descriptor) == {"type", "collection", "bytes"}
                and _nonnegative_long(descriptor["bytes"])):
            return LazyText(collection, descriptor["bytes"], owner)
    raise _invalid_result("Invalid import result descriptor")


class LazyText:
    """Chunk-readable UTF-8 text that never joins the complete value implicitly."""

    def __init__(self, collection, byte_count, owner):
        self._collection = collection
        self._byte_count = byte_count
        self._owner = owner
        self._chunks = None
        self._pending = ""

    def __len__(self):
        return self._byte_count

    def iter_chunks(self) -> Iterator[str]:
        total = 0
        for descriptor in self._owner._entries(self._collection):
            value = _decode_import_value(descriptor, self._owner)
            if not isinstance(value, str):
                raise _invalid_result("Invalid import text chunk")
            size = len(value.encode("utf-8"))
            total += size
            if not 0 < size <= _TEXT_BYTES or total > self._byte_count:
                raise _invalid_result("Import text exceeds its descriptor bounds")
            yield value
        if total != self._byte_count:
            raise _invalid_result("Import text length differs from its descriptor")

    def read(self, size: int) -> str:
        if type(size) is not int or size < 0:
            raise ValueError("LazyText.read requires a nonnegative character count")
        if self._owner.closed:
            raise RuntimeError("ImportResult is closed")
        if size == 0:
            return ""
        if self._chunks is None:
            self._chunks = iter(self.iter_chunks())
        output = []
        remaining = size
        while remaining and self._pending:
            part = self._pending[:remaining]
            output.append(part)
            self._pending = self._pending[len(part):]
            remaining -= len(part)
        while remaining:
            try:
                self._pending = next(self._chunks)
            except StopIteration:
                break
            part = self._pending[:remaining]
            output.append(part)
            self._pending = self._pending[len(part):]
            remaining -= len(part)
        return "".join(output)

    def __iter__(self):
        return self.iter_chunks()

    def __eq__(self, other):
        if not isinstance(other, str):
            return NotImplemented
        offset = 0
        for chunk in self.iter_chunks():
            if other[offset:offset + len(chunk)] != chunk:
                return False
            offset += len(chunk)
        return offset == len(other)


def valid_import_result_envelope(value: Any, *, leased: bool = True) -> bool:
    fields = {
        "format", "resultId", "operationId", "kind", "state", "namespaceId",
        "planId", "revision", "expiresAt", "collections",
    }
    if leased:
        fields.update({"leaseId", "leaseExpiresAt"})
    if not isinstance(value, dict) or set(value) != fields:
        return False
    if leased and (not isinstance(value["leaseId"], str) or not 0 < len(value["leaseId"]) <= 2048
                   or not _nonnegative_long(value["leaseExpiresAt"])):
        return False
    if (value["format"] != "deposit.import-result"
            or not isinstance(value["resultId"], str) or not value["resultId"]
            or (not isinstance(value["operationId"], str) or not value["operationId"])
            or not isinstance(value["kind"], str) or value["kind"] not in {"analysis", "apply", "clone"}
            or not isinstance(value["state"], str) or not value["state"]
            or not isinstance(value["namespaceId"], str) or not value["namespaceId"]
            or (value["planId"] is not None and not isinstance(value["planId"], str))
            or type(value["revision"]) is not int or not 0 <= value["revision"] <= _MAX_SAFE_INTEGER
            or type(value["expiresAt"]) is not int or not 0 <= value["expiresAt"] <= _MAX_SAFE_INTEGER
            or not isinstance(value["collections"], dict)):
        return False
    if value["kind"] == "apply" and value["state"] != "APPLIED":
        return False
    if value["kind"] == "analysis" and (value["state"], value["planId"] is not None) not in {
        ("PLAN_READY", True), ("IMPORT_CONFLICT", False),
    }:
        return False
    if value["kind"] == "clone" and (
        value["state"] != "COMMITTED" or value["planId"] is not None
        or set(value["collections"]) != {"mapping", "identityMappings", "resourceMapping", "metadata"}
        or any(not isinstance(value["collections"].get(key), dict)
               for key in value["collections"])
        or any(value["collections"][key].get("kind") != "mapping"
               for key in value["collections"])
    ):
        return False
    for name, descriptor in value["collections"].items():
        if (not isinstance(name, str) or not name or len(name) > 256
                or name in fields or name in {"plan", "leaseId", "leaseExpiresAt"}
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
            or not isinstance(value["state"], str) or value["state"] not in {"ready", "applied", "cancelled", "expired"}
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
