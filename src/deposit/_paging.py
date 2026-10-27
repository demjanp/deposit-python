"""Lazy, bounded iteration over revision-bound portable query pages."""

from __future__ import annotations

from typing import Any

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
