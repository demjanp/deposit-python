from __future__ import annotations

from datetime import datetime, timezone
from copy import deepcopy
import threading
import weakref

import pytest

from deposit.errors import DepositError
from deposit.store import Store
from deposit._paging import EntityPageIterator, QueryIterator, QueryPage
from deposit.entities import DClass, DObject
from deposit.values import typed


class _Rows:
    def __init__(self, values):
        self._values = iter(values)
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.closed = True

    def __next__(self):
        return next(self._values)


def _store(monkeypatch, page):
    store = object.__new__(Store)
    store._batch_collection = None
    captured = {}

    def call(_self, procedure, *arguments):
        captured["procedure"] = procedure
        captured["arguments"] = arguments
        captured["rows"] = _Rows([{"data": page}])
        return captured["rows"]

    monkeypatch.setattr(Store, "call", call)
    return store, captured


def test_query_page_decodes_typed_columns_and_keeps_presence_sidecar(monkeypatch):
    data = {
        "columns": [{"name": "count", "type": "Integer"}, {"name": "missing", "type": "String"}],
        "rows": [{"values": {"count": 9223372036854775807, "missing": None},
                  "presence": {"count": True, "missing": False}}],
        "nextCursor": "opaque-cursor",
    }
    store, captured = _store(monkeypatch, data)
    page = store.get_query_page("SELECT C.Code", parameters={"created": datetime(2026, 1, 1, tzinfo=timezone.utc)},
                                page_size=50, cursor="prior")
    assert page == data
    assert captured["procedure"] == "deposit.get.queryPage"
    query, parameters, options = captured["arguments"]
    assert query == "SELECT C.Code"
    assert parameters["created"].year == 2026
    assert options == {"pageSize": 50, "cursor": "prior"}
    assert captured["rows"].closed


def test_query_page_converts_explicit_typed_parameters_and_empty_page(monkeypatch):
    data = {"columns": [{"name": "v", "type": "Integer"}], "rows": [], "nextCursor": None}
    store, captured = _store(monkeypatch, data)
    assert store.get_query_page("SELECT $v", parameters={"v": typed("Integer", 12)}) == data
    assert captured["arguments"][1] == {"v": {"type": "Integer", "value": 12}}


@pytest.mark.parametrize("bad_page", [
    {},
    {"columns": [], "rows": [{"values": {}, "presence": {"extra": True}}], "nextCursor": None},
    {"columns": [{"name": "n", "type": "Integer"}],
     "rows": [{"values": {"n": 2**63}, "presence": {"n": True}}], "nextCursor": None},
])
def test_query_page_rejects_invalid_structure_and_lossy_typed_payload(monkeypatch, bad_page):
    store, _ = _store(monkeypatch, bad_page)
    with pytest.raises(Exception):
        store.get_query_page("SELECT 1")


def test_page_size_and_cursor_validation_precedes_store_call(monkeypatch):
    store, _ = _store(monkeypatch, {"columns": [], "rows": [], "nextCursor": None})
    with pytest.raises(ValueError):
        store.get_query_page("SELECT 1", page_size=1001)
    with pytest.raises(ValueError):
        store.get_query_page("SELECT 1", cursor="")


class _PagedStore:
    def __init__(self, pages):
        self.pages = iter(pages)
        self.requests = []

    def get_query_page(self, query, *, parameters, page_size, cursor):
        self.requests.append((query, cursor))
        item = next(self.pages)
        if isinstance(item, Exception):
            raise item
        return QueryPage(item)


def _page(values, cursor):
    return {"columns": [{"name": "v", "type": "Integer"}],
            "rows": [{"values": {"v": value}, "presence": {"v": True}} for value in values],
            "nextCursor": cursor}


def test_lazy_iterator_fetches_pages_on_demand_and_never_duplicates_rows():
    store = _PagedStore([_page([1, 2], "next"), _page([3], None)])
    iterator = QueryIterator(store, "SELECT v", {}, 2)
    assert next(iterator)["values"]["v"] == 1
    assert len(store.requests) == 1
    assert [row["values"]["v"] for row in iterator] == [2, 3]
    assert len(store.requests) == 2
    assert iterator.closed


def test_lazy_iterator_early_close_and_bounded_materialization_stop_fetching():
    store = _PagedStore([_page([1, 2], "next"), _page([3], None)])
    iterator = QueryIterator(store, "SELECT v", {}, 2)
    assert next(iterator)["values"]["v"] == 1
    iterator.close()
    assert len(store.requests) == 1
    assert list(iterator) == []

    bounded = QueryIterator(_PagedStore([_page([1, 2], "next"), _page([3], None)]), "SELECT v", {}, 2)
    with pytest.raises(ValueError, match="max_items"):
        bounded.materialize(max_items=1)
    assert bounded.closed


@pytest.mark.parametrize("code", ["REVISION_CONFLICT", "SCOPE_MISMATCH", "CURSOR_EXPIRED", "CANCELLED"])
def test_lazy_iterator_propagates_cursor_conflicts_without_restart(code):
    conflict = DepositError(code, "server rejected opaque query cursor")
    store = _PagedStore([_page([1], "next"), conflict, _page([1], None)])
    iterator = QueryIterator(store, "SELECT v", {}, 1)
    assert next(iterator)["values"]["v"] == 1
    with pytest.raises(DepositError) as failure:
        next(iterator)
    assert failure.value.code == code
    assert iterator.closed
    assert len(store.requests) == 2


class _DomainResult(_Rows):
    def __init__(self, row, after_consume=None):
        super().__init__([row])
        self.row = row
        self.after_consume = after_consume

    def single(self, strict=False):
        return self.row

    def keys(self):
        return list(self.row)

    def consume(self):
        if self.after_consume is not None:
            callback, self.after_consume = self.after_consume, None
            callback()


class _DomainTransaction:
    def __init__(self, client):
        self.client = client
        self.committed = False
        self.rolled_back = False

    def run(self, query, parameters):
        client = self.client
        if "YIELD data AS revision" in query:
            revision = client.revision
            client.trigger("snapshot_before_data")
            selection = parameters["selection"]
            identity = selection.get("id") if isinstance(selection, dict) else selection
            kind = "Object" if "node AS object" in query else "Class"
            entity = deepcopy(client.entities[identity])
            schema = deepcopy(client.entities[entity["classId"]]) if kind == "Object" else entity
            return _DomainResult({"revision": {"sequence": revision}, "entity": entity, "schema": schema},
                                 lambda: client.trigger("snapshot_after_data"))
        procedure = query.split("CALL ", 1)[1].split("(", 1)[0]
        if procedure == "deposit.get.revision":
            return _DomainResult({"data": {"sequence": client.revision}})
        client.trigger("page_before_data")
        page = deepcopy(client.pages.pop(0))
        client.page_requests.append((procedure, parameters))
        return _DomainResult({"data": page}, lambda: client.trigger("page_after_data"))

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True


class _DomainSession:
    def __init__(self, client):
        self.client = client
        self.closed = False

    def begin_transaction(self):
        transaction = _DomainTransaction(self.client)
        self.client.transactions.append(transaction)
        return transaction

    def close(self):
        self.closed = True


class _DomainClient:
    def __init__(self, pages):
        self.revision = 7
        self.pages = list(pages)
        self.page_requests = []
        self.transactions = []
        self.sessions = []
        self.hooks = {}
        self.entities = {
            "class-1": {"id": "class-1", "kind": "Class", "namespaceId": "namespace-1", "name": "C",
                        "descriptors": [{"name": "Amount", "type": "Float"}]},
            "object-1": {"id": "object-1", "kind": "Object", "namespaceId": "namespace-1",
                         "classId": "class-1", "descriptors": {"Amount": 7.0}},
            "object-2": {"id": "object-2", "kind": "Object", "namespaceId": "namespace-1",
                         "classId": "class-1", "descriptors": {"Amount": 8.0}},
        }

    def _session(self):
        session = _DomainSession(self)
        self.sessions.append(session)
        return session

    def trigger(self, name):
        callback = self.hooks.pop(name, None)
        if callback:
            callback()

    def commit_external(self):
        self.revision += 1


def _domain_store(pages):
    client = _DomainClient(pages)
    store = object.__new__(Store)
    store._client = client
    store._token = "private-test-token"
    store._namespace_id = "namespace-1"
    store._batch_collection = None
    store._operation_lock = threading.Lock()
    store._state_lock = threading.Lock()
    store._closed = store._closing = store._active = False
    store._handles = weakref.WeakValueDictionary()
    return store, client


def _domain_page(*items, cursor=None):
    return {"items": list(items), "nextCursor": cursor}


@pytest.mark.parametrize("boundary", ["snapshot_before_data", "snapshot_after_data"])
@pytest.mark.parametrize("kind,identity", [("Class", "class-1"), ("Object", "object-1")])
def test_checked_snapshot_rejects_commits_during_data_and_schema_read(boundary, kind, identity):
    store, client = _domain_store([])
    client.hooks[boundary] = client.commit_external
    with pytest.raises(DepositError) as failure:
        store._checked_entity_snapshot(kind, {"id": identity})
    assert failure.value.code == "REVISION_CONFLICT"
    assert not store._handles
    assert client.transactions[-1].rolled_back
    assert not client.transactions[-1].committed
    assert client.sessions[-1].closed and not store._active


@pytest.mark.parametrize("boundary", ["page_before_data", "page_after_data"])
def test_checked_page_rejects_commit_during_collection_and_closes(boundary):
    store, client = _domain_store([_domain_page({"id": "class-1"})])
    client.hooks[boundary] = client.commit_external
    iterator = store.classes()
    with pytest.raises(DepositError) as failure:
        next(iterator)
    assert failure.value.code == "REVISION_CONFLICT"
    assert iterator.closed
    assert not store._handles
    assert client.transactions[-1].rolled_back
    assert client.sessions[-1].closed and not store._active


@pytest.mark.parametrize("factory", ["classes", "objects", "members"])
def test_entity_page_rejects_commit_after_page_before_first_handle(monkeypatch, factory):
    kind = "class" if factory == "classes" else "object"
    store, client = _domain_store([_domain_page({"id": f"{kind}-1"})])
    original = store._checked_entity_page

    def page_then_commit(*args, **kwargs):
        page = original(*args, **kwargs)
        client.commit_external()
        return page

    monkeypatch.setattr(store, "_checked_entity_page", page_then_commit)
    dclass = DClass(store, client.entities["class-1"], {"Amount": "Float"}, snapshot_revision=7)
    iterator = store.members(dclass) if factory == "members" else getattr(store, factory)()
    with pytest.raises(DepositError) as failure:
        next(iterator)
    assert failure.value.code == "REVISION_CONFLICT"
    assert iterator.closed


@pytest.mark.parametrize("cursor", [None, "opaque-next"])
def test_buffered_entity_transform_rejects_commit_even_on_final_page(cursor):
    store, client = _domain_store([_domain_page({"id": "object-1"}, {"id": "object-2"}, cursor=cursor)])
    iterator = store.objects()
    first = next(iterator)
    assert first._baseline_revision == 7
    client.commit_external()
    with pytest.raises(DepositError) as failure:
        next(iterator)
    assert failure.value.code == "REVISION_CONFLICT"
    assert first._baseline_revision == 7
    assert iterator.closed and len(client.page_requests) == 1


def test_entity_traversal_retains_one_revision_across_pages():
    store, client = _domain_store([_domain_page({"id": "object-1"}, cursor="next"),
                                   _domain_page({"id": "object-2"})])
    iterator = store.objects()
    assert [handle._baseline_revision for handle in iterator] == [7, 7]
    assert iterator.closed and iterator.revision == 7
    assert all(session.closed for session in client.sessions)


@pytest.mark.parametrize("after_first_page", [False, True])
def test_descriptor_conversion_cannot_decode_values_with_cached_float_schema(after_first_page):
    store, client = _domain_store([
        _domain_page({"name": "Amount", "value": 7.0}, cursor="next"),
        _domain_page({"name": "Amount", "value": 8}),
    ])
    handle = store.get_object_handle({"id": "object-1"})
    handle._pending = {"Amount": 12.0}
    iterator = store.descriptors(handle)
    if after_first_page:
        assert next(iterator) == ("Amount", 7.0, "Float")
    client.entities["class-1"]["descriptors"][0]["type"] = "Integer"
    client.entities["object-1"]["descriptors"]["Amount"] = 7
    client.commit_external()
    with pytest.raises(DepositError) as failure:
        next(iterator)
    assert failure.value.code == "REVISION_CONFLICT"
    assert iterator.closed and handle.pending == {"Amount": 12.0}
    assert handle._baseline_revision == 7 and handle._schema == {"Amount": "Float"}


def test_descriptor_iterator_copies_schema_and_rejects_already_stale_handles():
    store, client = _domain_store([_domain_page({"name": "Amount", "value": 7.0})])
    handle = store.get_object_handle({"id": "object-1"})
    iterator = store.descriptors(handle)
    handle._schema["Amount"] = "Integer"
    assert next(iterator) == ("Amount", 7.0, "Float")
    handle._mark_stale()
    with pytest.raises(RuntimeError, match="stale"):
        store.descriptors(handle)


@pytest.mark.parametrize("procedure", ["deposit.get.classDescriptorPage", "deposit.get.relationshipPage"])
def test_detached_pages_keep_baseline_for_empty_or_final_pages(procedure):
    store, client = _domain_store([_domain_page({"id": "old"}, cursor="next"), _domain_page()])
    iterator = EntityPageIterator(store, procedure, None)
    assert next(iterator) == {"id": "old"}
    client.commit_external()
    with pytest.raises(DepositError) as failure:
        next(iterator)
    assert failure.value.code == "REVISION_CONFLICT"
    assert iterator.closed and iterator.revision == 7


@pytest.mark.parametrize("exception", [DepositError("CANCELLED", "cancelled"), KeyboardInterrupt()])
def test_entity_transform_failure_closes_iterator(exception):
    store, _ = _domain_store([_domain_page({"id": "item"})])

    def fail(item, revision):
        raise exception

    iterator = EntityPageIterator(store, "deposit.get.objectPage", None, transform=fail)
    with pytest.raises(type(exception)):
        next(iterator)
    assert iterator.closed


@pytest.mark.parametrize("class_relationship", [False, True])
def test_neighbors_share_page_revision_across_directions_and_close(monkeypatch, class_relationship):
    source_id = "class-1" if class_relationship else "object-1"
    other_id = "class-1" if class_relationship else "object-2"
    store, client = _domain_store([
        _domain_page({"id": "relationship-1", "sourceId": source_id, "targetId": other_id}),
        _domain_page({"id": "relationship-2", "sourceId": other_id, "targetId": source_id}),
    ])
    source = store.get_class_handle({"id": source_id}) if class_relationship else store.get_object_handle({"id": source_id})
    iterators = []
    original = store._entity_page

    def track(*args, **kwargs):
        iterator = original(*args, **kwargs)
        iterators.append(iterator)
        return iterator

    monkeypatch.setattr(store, "_entity_page", track)
    neighbors = store.class_neighbors(source) if class_relationship else store.neighbors(source)
    assert next(neighbors)[0]._baseline_revision == 7
    client.commit_external()
    with pytest.raises(DepositError) as failure:
        next(neighbors)
    assert failure.value.code == "REVISION_CONFLICT"
    assert len(iterators) == 2 and all(iterator.closed for iterator in iterators)


def test_neighbor_endpoint_lookup_rejects_commit_after_relationship_page(monkeypatch):
    store, client = _domain_store([_domain_page({"id": "r", "sourceId": "object-1", "targetId": "object-2"})])
    source = store.get_object_handle({"id": "object-1"})
    original = store._checked_entity_page

    def page_then_commit(*args, **kwargs):
        page = original(*args, **kwargs)
        client.commit_external()
        return page

    monkeypatch.setattr(store, "_checked_entity_page", page_then_commit)
    with pytest.raises(DepositError) as failure:
        next(store.neighbors(source, direction="outgoing"))
    assert failure.value.code == "REVISION_CONFLICT"
    assert all(session.closed for session in client.sessions)


def test_final_detached_page_can_yield_buffered_records_after_commit():
    store, client = _domain_store([_domain_page({"id": "old-1"}, {"id": "old-2"})])
    iterator = store.relationships()
    assert next(iterator) == {"id": "old-1"}
    client.commit_external()
    assert list(iterator) == [{"id": "old-2"}]
    assert len(client.page_requests) == 1 and iterator.revision == 7


def test_empty_outgoing_neighbor_page_still_pins_incoming_revision(monkeypatch):
    store, client = _domain_store([_domain_page(), _domain_page()])
    source = store.get_object_handle({"id": "object-1"})
    original = store._entity_page
    iterators = []

    def track(*args, **kwargs):
        if iterators:
            client.commit_external()
        iterator = original(*args, **kwargs)
        iterators.append(iterator)
        return iterator

    monkeypatch.setattr(store, "_entity_page", track)
    with pytest.raises(DepositError) as failure:
        next(store.neighbors(source))
    assert failure.value.code == "REVISION_CONFLICT"
    assert all(iterator.closed and iterator.revision == 7 for iterator in iterators)


def test_buffered_neighbor_endpoint_rejects_commit_on_final_page():
    store, client = _domain_store([_domain_page(
        {"id": "r1", "sourceId": "object-1", "targetId": "object-2"},
        {"id": "r2", "sourceId": "object-1", "targetId": "object-2"},
    )])
    source = store.get_object_handle({"id": "object-1"})
    neighbors = store.neighbors(source, direction="outgoing")
    assert next(neighbors)[0]._baseline_revision == 7
    client.commit_external()
    with pytest.raises(DepositError) as failure:
        next(neighbors)
    assert failure.value.code == "REVISION_CONFLICT"
    assert len(client.page_requests) == 1


def test_unchanged_neighbor_traversal_returns_consistent_endpoints_in_both_directions():
    store, client = _domain_store([
        _domain_page({"id": "r1", "sourceId": "object-1", "targetId": "object-2"}),
        _domain_page({"id": "r2", "sourceId": "object-2", "targetId": "object-1"}),
    ])
    source = store.get_object_handle({"id": "object-1"})
    result = list(store.neighbors(source))
    assert [(handle.id, handle._baseline_revision, relationship["id"])
            for handle, relationship in result] == [("object-2", 7, "r1"), ("object-2", 7, "r2")]
    assert all(session.closed for session in client.sessions)
