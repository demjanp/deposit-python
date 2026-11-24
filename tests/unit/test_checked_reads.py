"""Focused checked-read tests against a simulated driver."""

from contextlib import nullcontext
from types import SimpleNamespace
from weakref import WeakValueDictionary

import pytest

from deposit.entities import DClass
from deposit.errors import DepositError
from deposit.store import Store


class Result:
    def __init__(self, rows, revision, end):
        self.record = {"start": {"sequence": revision}, "rows": rows, "end": {"sequence": end}}
        self.consumed = False

    def keys(self):
        return ("start", "rows", "end")

    def single(self, *, strict):
        assert strict
        return self.record

    def consume(self):
        self.consumed = True


class Driver:
    def __init__(self, response):
        self.response = response
        self.calls = []
        self.finished = []

    def session(self):
        def run(query, parameters):
            assert query.count("CALL deposit.get.revision") == 2
            assert query.index("RETURN collect(") < query.rindex("CALL deposit.get.revision")
            assert "WHERE $expected IS NULL OR start.sequence = $expected" in query
            self.calls.append((query, parameters))
            result = self.response(query, parameters)
            transaction.result = result
            return result

        def commit():
            assert transaction.result.consumed
            self.finished.append("committed")

        transaction = SimpleNamespace(run=run, commit=commit, rollback=lambda: self.finished.append("rolled back"))
        return SimpleNamespace(begin_transaction=lambda: transaction, close=lambda: None)


def make_store(driver):
    store = object.__new__(Store)
    store._namespace_id = "namespace"
    store._token = "token"
    store._client = SimpleNamespace(_session=driver.session)
    store._batch_collection = None
    store._handles = WeakValueDictionary()
    store._serialized = nullcontext
    return store


def class_data(identity, name):
    return {
        "id": identity, "kind": "Class", "namespaceId": "namespace", "name": name,
        "descriptors": [{"name": "Collected", "type": "Date"}],
    }


def object_data(identity, class_id):
    return {
        "id": identity, "kind": "Object", "namespaceId": "namespace", "classId": class_id,
        "descriptors": {"Collected": "2026-11-24"},
    }


def test_snapshot_and_object_iteration_fetch_only_missing_distinct_schemas():
    from datetime import date

    classes = {"a": class_data("a", "First"), "b": class_data("b", "Second")}
    first, second, third = object_data("1", "a"), object_data("2", "b"), object_data("3", "a")

    def response(query, parameters):
        if "UNWIND" in query:
            assert parameters["ids"] == ["a", "b"]
            assert parameters["expected"] == 4
            return Result([classes["b"], classes["a"]], 4, 4)
        if "AS classPage" in query:
            assert parameters["selection"] == {"id": "1"}
            return Result([{"entity": first, "schema": classes["a"]}], 4, 4)
        if "deposit.get.classPage" in query:
            return Result([{"items": [classes["a"]], "nextCursor": None}], 4, 4)
        assert "deposit.get.objectPage" in query
        if "cursor" in parameters["a1"]:
            assert parameters["expected"] == 4 and parameters["a1"]["cursor"] == "continue"
            return Result([{"items": [third], "nextCursor": None}], 4, 4)
        return Result([{"items": [first, second], "nextCursor": "continue"}], 4, 4)

    driver = Driver(response)
    store = make_store(driver)
    def single(query, parameters):
        assert "deposit.get.classPage" in query and "deposit.utils.toData" not in query
        assert parameters["selection"] == "First"
        return {"data": classes["a"]}

    store._client._single = single
    assert store.get_class("First") == classes["a"]
    assert store.get_object_handle({"id": "1"})["Collected"] == date(2026, 11, 24)
    assert len(driver.calls) == 1
    assert list(store.classes())[0].declarations["Collected"]["type"] == "Date"
    assert len(driver.calls) == 2
    rows = store.objects(page_size=2)
    assert len(driver.calls) == 2
    assert next(rows)["Collected"] == date(2026, 11, 24)
    assert len(driver.calls) == 4
    assert [obj.id for obj in rows] == ["2", "3"]
    assert len(driver.calls) == 5
    assert driver.finished == ["committed"] * 5


def test_checked_pages_reject_changed_revisions_before_publishing():
    driver = Driver(lambda _query, _parameters: Result([{"items": [], "nextCursor": None}], 4, 5))
    store = make_store(driver)
    with pytest.raises(DepositError, match="Namespace changed") as failure:
        list(store.objects())
    assert failure.value.code == "REVISION_CONFLICT"
    assert len(driver.calls) == 1
    assert driver.finished == ["rolled back"]

    driver = Driver(lambda _query, _parameters: Result([], 5, 5))
    with pytest.raises(DepositError, match="Namespace changed") as failure:
        make_store(driver)._checked_entity_page("deposit.get.objectPage", None, {"pageSize": 1}, expected_revision=4)
    assert failure.value.code == "REVISION_CONFLICT"
    assert driver.finished == ["rolled back"]


def test_member_reuses_checked_class_and_neighbor_page_groups_related_objects():
    clazz = class_data("a", "First")
    object_one, object_two = object_data("1", "a"), object_data("2", "a")
    calls = []

    def response(query, parameters):
        if "deposit.get.classMembersPage" in query:
            return Result([{"items": [object_one, object_two], "nextCursor": None}], 4, 4)
        if "deposit.get.relationshipPage" in query:
            calls.append(parameters["a0"])
            items = [
                {"id": "edge", "sourceId": "1", "targetId": "2"},
                {"id": "self", "sourceId": "1", "targetId": "1"},
                {"id": "another", "sourceId": "1", "targetId": "2"},
            ]
            if "targetId" in parameters["a0"]:
                assert parameters["expected"] == 4
                items = [items[1]]
            return Result([{"items": items, "nextCursor": None}], 4, 4)
        if "deposit.get.objectPage" in query:
            assert "UNWIND" in query and parameters["ids"] == ["2", "1"] and parameters["expected"] == 4
            return Result([object_one, object_two], 4, 4)
        assert "deposit.get.classPage" in query and parameters["ids"] == ["a"] and parameters["expected"] == 4
        return Result([clazz], 4, 4)

    driver = Driver(response)
    store = make_store(driver)
    dclass = DClass(store, clazz, {"Collected": "Date"}, snapshot_revision=4)
    members = list(store.members(dclass))
    assert [member.id for member in members] == ["1", "2"]
    assert len(driver.calls) == 1
    neighbors = list(store.neighbors(members[0]))
    assert [(other.id, rel["id"]) for other, rel in neighbors] == [
        ("2", "edge"), ("1", "self"), ("2", "another"),
    ]
    assert neighbors[0][0] is not neighbors[2][0]
    assert calls == [{"sourceId": "1"}, {"targetId": "1"}]
    assert len(driver.calls) == 5
