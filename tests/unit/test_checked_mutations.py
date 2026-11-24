"""Focused invocation tests with simulated physical Bolt connections."""

from contextlib import nullcontext
from types import SimpleNamespace
from uuid import UUID

import pytest

from deposit.client import Client
from deposit.entities import DClass, DObject
from deposit.errors import CompatibilityError, DepositError
from deposit.store import Store, _STORE_FACTORY_TOKEN


INFO = {"version": "alpha", "incarnation": "run", "capabilities": [
    "operationControl", "pagedImportResults", "fullNamespaceClone", "exchangeIntake",
]}


def class_data(identity="class", order=0):
    return {"id": identity, "kind": "Class", "namespaceId": "namespace", "order": order,
            "descriptors": [{"name": "Score", "type": "Integer"}]}


def object_data(score=7):
    return {"id": "object", "kind": "Object", "namespaceId": "namespace", "classId": "class",
            "descriptors": {"Score": score}}


class Result:
    def __init__(self, row, summary):
        self.row = row
        self.summary = summary

    def single(self, *, strict):
        assert strict
        return self.row

    def keys(self):
        return self.row.keys()

    def consume(self):
        return self.summary


class Session:
    def __init__(self, driver):
        self.driver = driver

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def close(self):
        pass

    def begin_transaction(self):
        return Transaction(self.driver)


class Transaction:
    def __init__(self, driver):
        self.driver = driver
        self._connection = driver.connection

    def __enter__(self):
        return self

    def __exit__(self, kind, *_):
        if kind is not None:
            self.rollback()

    def run(self, query, parameters=None):
        return self.driver.protocol.run(self, query, parameters or {})

    def commit(self):
        self.driver.protocol.finished.append(("commit", self))

    def rollback(self):
        self.driver.protocol.finished.append(("rollback", self))


class Driver:
    def __init__(self, protocol):
        self.protocol = protocol
        self.connection = object()

    def session(self, **_):
        return Session(self)


class Protocol:
    def __init__(self):
        self.address = SimpleNamespace(host="localhost", port=7687)
        self.summary = SimpleNamespace(database="neo4j", server=SimpleNamespace(address=self.address))
        self.execution, self.control, self.read = [Driver(self) for _ in range(3)]
        self.calls, self.finished = [], []
        self.profile = {"user": {"id": "actor"}, "namespaceId": "namespace", "observed": {"namespace": 4}}
        self.info = dict(INFO)
        self.replace_after_begin = False
        self.malformed_snapshot = False
        self.registration = None

    def make_store(self, monkeypatch):
        from deposit._exchange_transport import OperationEndpoint

        drivers = iter((self.execution, self.control))
        monkeypatch.setattr(OperationEndpoint, "_new_driver", lambda _: next(drivers))
        client = Client(self.read)
        client._driver._pool = SimpleNamespace(pool_config={}, workspace_config=SimpleNamespace(impersonated_user=None))
        client._single_with_summary = lambda *_: ({"data": dict(INFO)}, self.summary)
        store = Store(client, token="token", actor_id="actor", namespace_id="namespace",
                      observed={"namespace": 4}, _factory_token=_STORE_FACTORY_TOKEN)
        self.calls.clear()
        self.finished.clear()
        return store

    def run(self, transaction, query, parameters):
        route = query.split("CALL ", 1)[1].split("(", 1)[0]
        self.calls.append((route, transaction, query, parameters))
        if route == "deposit.get.info":
            row = {"data": self.info}
        elif route == "deposit.get.session":
            row = {"data": self.profile}
        elif route == "deposit.action.begin":
            assert query == "CYPHER 25 CALL deposit.action.begin($a0, $a1, $a2, $a3, $token) YIELD data RETURN data"
            assert UUID(parameters["a0"]).version == 7
            self.registration = parameters
            row = {"data": self.status("REGISTERED")}
            if self.replace_after_begin:
                self.execution.connection = object()
        elif route.startswith(("deposit.set.", "deposit.create.", "deposit.delete.")) or route == "deposit.apply.batch":
            assert transaction._connection is self.execution.connection
            if route == "deposit.set.descriptor":
                row = {"node": {"namespaceId": "namespace", "id": "descriptor", "value": parameters["a2"]}}
            elif route == "deposit.delete.descriptor":
                row = {"data": {"changed": True}}
            elif route == "deposit.create.object":
                row = {"node": object_data()}
            elif route == "deposit.apply.batch":
                row = {"data": {"results": {command["key"]: {} for command in parameters["a0"]}}}
            else:
                row = {"value": True}
        elif route == "deposit.get.operation":
            assert transaction.driver is self.control
            row = {"data": self.status("REGISTERED")}
        elif route == "deposit.action.cancel":
            assert transaction.driver is self.control
            row = {"data": self.status("CANCELLED_ROLLED_BACK")}
        elif route == "deposit.get.revision":
            assert transaction.driver is self.read
            assert self.finished[-1][0] == "commit"
            if "AS classPage" in query:
                entity = object_data(9)
                if self.registration["a2"] == "deposit.delete.descriptor":
                    entity["descriptors"] = {}
                if self.malformed_snapshot:
                    entity["id"] = "wrong"
                rows = [{"entity": entity, "schema": class_data()}]
            else:
                assert parameters["ids"] == ["first", "second"]
                rows = [class_data("second", 1), class_data("first", 2)]
            row = {"start": {"sequence": 5}, "rows": rows, "end": {"sequence": 5}}
        else:
            raise AssertionError(route)
        return Result(row, self.summary)

    def status(self, state):
        return {"operationId": self.registration["a0"], "scope": self.registration["a1"],
                "procedure": self.registration["a2"], "state": state}

    @property
    def routes(self):
        return [route for route, *_ in self.calls]


def test_warm_descriptor_write_keeps_one_checked_post_commit_refresh(monkeypatch):
    protocol = Protocol()
    store = protocol.make_store(monkeypatch)
    obj = DObject(store, object_data(), {"Score": "Integer"}, snapshot_revision=4)
    other = DClass(store, class_data(), {"Score": "Integer"}, snapshot_revision=4)
    assert store.set_descriptor(obj, "Score", 9)["value"] == 9
    assert obj["Score"] == 9 and obj._baseline_revision == 5
    assert other.stale and not obj.stale
    assert protocol.routes == ["deposit.get.session", "deposit.action.begin", "deposit.set.descriptor",
                               "deposit.get.revision"]
    profile, begin, write, checked = protocol.calls
    assert profile[1] is begin[1] and begin[1] is not write[1]
    assert begin[1]._connection is write[1]._connection
    assert sum(query.count("CALL deposit.") for _, _, query, _ in protocol.calls) == 8
    assert "deposit.utils.toData" in write[2] and checked[2].count("CALL deposit.get.revision") == 2


def test_descriptor_delete_preserves_selector_refresh_and_stale_guards(monkeypatch):
    protocol = Protocol()
    store = protocol.make_store(monkeypatch)
    obj = DObject(store, object_data(), {"Score": "Integer"}, snapshot_revision=4)
    stale_class = DClass(store, class_data(), {"Score": "Integer"}, snapshot_revision=4)

    assert store.delete_descriptor(obj, "Score") == {"changed": True}
    assert protocol.registration["a3"] == [{"objectId": "object", "name": "Score"}]
    assert obj["descriptors"] == {} and obj._baseline_revision == 5
    assert protocol.routes == ["deposit.get.session", "deposit.action.begin", "deposit.delete.descriptor",
                               "deposit.get.revision"]

    obj._mark_stale()
    protocol.calls.clear()
    with pytest.raises(RuntimeError, match="snapshot is stale"):
        store.delete_descriptor(obj, "Score")
    with pytest.raises(RuntimeError, match="snapshot is stale"):
        store.delete_class_descriptor(stale_class, "Score")
    assert protocol.calls == []


def test_stale_pin_and_unverified_endpoint_are_rejected_before_registration(monkeypatch):
    protocol = Protocol()
    store = protocol.make_store(monkeypatch)
    obj = DObject(store, object_data(), {"Score": "Integer"}, snapshot_revision=3)
    with pytest.raises(DepositError, match="baseline is stale") as exc:
        store.set_descriptor(obj, "Score", 9)
    assert exc.value.code == "REVISION_CONFLICT" and obj.stale
    assert protocol.routes == ["deposit.get.session"]
    assert protocol.registration is None

    protocol.calls.clear()
    protocol.profile["namespaceId"] = "other"
    with pytest.raises(DepositError) as exc:
        store.create_user_tool("name", "data")
    assert exc.value.code == "SCOPE_MISMATCH"
    assert protocol.routes == ["deposit.get.session"]

    protocol.calls.clear()
    protocol.execution.connection = object()
    protocol.info = dict(INFO, incarnation="replacement")
    with pytest.raises(DepositError) as exc:
        store.create_user_tool("name", "data")
    assert exc.value.code == "SCOPE_MISMATCH"
    assert protocol.routes == ["deposit.get.info"] and protocol.calls[0][3] == {}


def test_replaced_execution_connection_settles_on_independent_control(monkeypatch):
    from deposit.errors import DepositCancelled

    protocol = Protocol()
    store = protocol.make_store(monkeypatch)
    protocol.replace_after_begin = True
    with pytest.raises(DepositCancelled):
        store.create_user_tool("name", "data")
    assert protocol.routes == ["deposit.get.session", "deposit.action.begin",
                               "deposit.get.info", "deposit.get.operation",
                               "deposit.get.info", "deposit.action.cancel"]
    assert all(transaction.driver is protocol.control for _, transaction, _, _ in protocol.calls[2:])


def test_known_commit_keeps_operation_id_when_checked_refresh_is_invalid(monkeypatch):
    protocol = Protocol()
    store = protocol.make_store(monkeypatch)
    protocol.malformed_snapshot = True
    obj = DObject(store, object_data(), {"Score": "Integer"}, snapshot_revision=4)
    with pytest.raises(DepositError) as exc:
        store.set_descriptor(obj, "Score", 9)
    assert exc.value.code == "RESULT_UNAVAILABLE"
    assert exc.value.details == {"operationId": protocol.registration["a0"], "scope": "namespace", "state": "COMMITTED"}
    assert obj.stale and protocol.routes.count("deposit.set.descriptor") == 1


def test_order_switch_groups_class_refresh_and_preserves_batch_order(monkeypatch):
    protocol = Protocol()
    store = protocol.make_store(monkeypatch)
    first = DClass(store, class_data("first", 1), {"Score": "Integer"}, snapshot_revision=4)
    second = DClass(store, class_data("second", 2), {"Score": "Integer"}, snapshot_revision=4)
    assert store.switch_class_order(first, second) == {"changed": True}
    assert (first.order, second.order) == (2, 1)
    assert first._baseline_revision == second._baseline_revision == 5
    assert protocol.routes == ["deposit.get.session", "deposit.action.begin", "deposit.apply.batch", "deposit.get.revision"]
    commands = protocol.calls[2][3]["a0"]
    assert [command["arguments"] for command in commands] == [
        [{"id": "first"}, {"order": 2}], [{"id": "second"}, {"order": 1}],
    ]


def test_create_from_stable_class_id_without_values_skips_class_preflight(monkeypatch):
    protocol = Protocol()
    store = protocol.make_store(monkeypatch)
    obj = store.create_object({"id": "class"})
    assert obj["Score"] == 9 and obj._baseline_revision == 5
    assert protocol.routes == ["deposit.get.session", "deposit.action.begin", "deposit.create.object", "deposit.get.revision"]
    assert protocol.registration["a3"] == [{"id": "class"}, {}]


def test_verified_capabilities_are_reused_and_rechecked_on_replacement(monkeypatch):
    protocol = Protocol()
    store = protocol.make_store(monkeypatch)
    store._endpoint._require_capabilities("pagedImportResults", "fullNamespaceClone")
    store._endpoint.require_exchange()
    assert protocol.routes == []
    protocol.execution.connection = object()
    protocol.info = dict(INFO, capabilities=["operationControl"])
    with pytest.raises(CompatibilityError):
        store.create_user_tool("name", "data")
    assert protocol.routes == ["deposit.get.info"] and protocol.registration is None
