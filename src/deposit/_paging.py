"""Lazy, bounded iteration over revision-bound portable query pages."""

from __future__ import annotations

import threading
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


class ImportResult(MappingABC):
    """Immutable scalar import envelope with locally persisted lazy collections."""

    _FIELDS = ("format", "resultId", "operationId", "kind", "state", "namespaceId",
               "planId", "revision", "expiresAt")

    def __init__(self, journal, action_key: str, envelope: dict[str, Any], *, plan_observation=None,
                 deferred_loader=None):
        if plan_observation is not None:
            from ._recovery.journal import _valid_plan_observation
            if not _valid_plan_observation(plan_observation, envelope):
                raise DepositError("INVALID_RESPONSE", "Import plan observation does not match its immutable result")
        self._journal = journal
        self._action_key = action_key
        immutable = dict(envelope)
        immutable["collections"] = MappingProxyType({
            key: MappingProxyType(dict(value)) for key, value in envelope["collections"].items()
        })
        self._envelope = MappingProxyType(immutable)
        self._plan_observation = (None if plan_observation is None
                                  else MappingProxyType(dict(plan_observation)))
        self._closed = False
        self._deferred_loader = deferred_loader
        self._attachment_lock = threading.RLock()
        self._pin_name = None
        self._pin_lock = None
        if deferred_loader is None:
            self._attach_backing()

    def _attach_backing(self):
        if self._pin_name is None:
            self._pin_name, self._pin_lock = self._journal.pin_import_result(self._action_key)

    def _ensure_attached(self):
        if self._closed:
            raise RuntimeError("ImportResult is closed")
        with self._attachment_lock:
            if self._pin_name is not None:
                return
            if self._deferred_loader is not None:
                self._deferred_loader()
                self._deferred_loader = None
            self._attach_backing()

    @property
    def result_id(self):
        return self._envelope["resultId"]

    @property
    def operation_id(self):
        return self._envelope["operationId"]

    @property
    def action_key(self):
        return self._action_key

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
        self._ensure_attached()
        return _lazy_collection(self._journal, self._action_key, key, descriptor["kind"], descriptor["count"], self)

    def __iter__(self):
        plan = ("plan",) if self._plan_observation is not None else ()
        return iter((*self._FIELDS, *plan, *self._envelope["collections"].keys()))

    def __len__(self):
        return len(self._FIELDS) + (self._plan_observation is not None) + len(self._envelope["collections"])

    def close(self):
        """Release this reader; protected result segments remain retained."""
        with self._attachment_lock:
            if not self._closed:
                self._closed = True
                if self._pin_name is not None:
                    self._journal.release_import_result_pin(self._pin_name, self._pin_lock)

    @property
    def closed(self):
        return self._closed

    def __enter__(self):
        if self._closed:
            raise RuntimeError("ImportResult is closed")
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.close()


def _lazy_collection(journal, action_key, collection, kind, count, owner):
    if kind == "mapping":
        return LazyResultMapping(journal, action_key, collection, count, owner)
    return LazyResultSequence(journal, action_key, collection, count, owner)


class _LazyResultBase:
    def __init__(self, journal, action_key, collection, count, owner):
        self._journal = journal
        self._action_key = action_key
        self._collection = collection
        self._count = count
        self._owner = owner

    def _check_open(self):
        if self._owner.closed:
            raise RuntimeError("ImportResult is closed")

    def _entries(self):
        self._check_open()
        meta = self._journal.read_import_collection_meta(self._action_key, self._collection)
        for page_index in range(meta["pageCount"]):
            response = self._journal.read_import_page(self._action_key, self._collection, page_index)["response"]
            yield from response["entries"]


class LazyResultSequence(_LazyResultBase, SequenceABC):
    """Indexable, bounded-page sequence backed by protected local segments."""

    def __len__(self):
        return self._count

    def __iter__(self):
        for descriptor in self._entries():
            yield _decode_import_value(descriptor, self)

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
            yield _decode_import_value(key, self), _decode_import_value(value, self)

    def __iter__(self):
        for key, _value in self._pairs():
            yield key

    def items(self):
        return self._pairs()

    def __getitem__(self, key):
        for actual_key, value in self._pairs():
            if actual_key == key:
                return value
        raise KeyError(key)


def _decode_import_value(descriptor: Any, parent: _LazyResultBase):
    if not isinstance(descriptor, dict) or not isinstance(descriptor.get("type"), str):
        raise DepositError("INVALID_RESPONSE", "Import result descriptor is invalid")
    kind = descriptor["type"]
    if kind == "scalar" and set(descriptor) == {"type", "value"}:
        return descriptor["value"]
    if kind in {"mapping", "sequence"} and set(descriptor) == {"type", "collection", "count"}:
        return _lazy_collection(parent._journal, parent._action_key, descriptor["collection"],
                                kind, descriptor["count"], parent._owner)
    if kind == "text" and set(descriptor) == {"type", "collection", "bytes"}:
        return LazyText(parent._journal, parent._action_key, descriptor["collection"],
                        descriptor["bytes"], parent._owner)
    raise DepositError("INVALID_RESPONSE", "Import result descriptor type is unsupported")


class LazyText:
    """Chunk-readable UTF-8 text that never joins the complete value implicitly."""

    def __init__(self, journal, action_key, collection, byte_count, owner):
        self._journal = journal
        self._action_key = action_key
        self._collection = collection
        self._byte_count = byte_count
        self._owner = owner
        self._chunks = None
        self._pending = ""

    def __len__(self):
        return self._byte_count

    def iter_chunks(self) -> Iterator[str]:
        if self._owner.closed:
            raise RuntimeError("ImportResult is closed")
        meta = self._journal.read_import_collection_meta(self._action_key, self._collection)
        if meta["kind"] != "text" or meta["textBytes"] != self._byte_count:
            raise DepositError("INVALID_RESPONSE", "Import text descriptor disagrees with its local index")
        for page_index in range(meta["pageCount"]):
            page = self._journal.read_import_page(self._action_key, self._collection, page_index)["response"]
            for chunk in page["entries"]:
                if (not isinstance(chunk, dict) or set(chunk) != {"type", "value"}
                        or chunk.get("type") != "scalar" or not isinstance(chunk.get("value"), str)):
                    raise DepositError("INVALID_RESPONSE", "Import text chunk is invalid")
                yield chunk["value"]

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
