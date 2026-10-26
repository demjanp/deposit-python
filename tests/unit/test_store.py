from __future__ import annotations

from contextlib import contextmanager
import json
import os
from pathlib import Path
import threading
import uuid

import pytest
from neo4j.graph import Graph, Node, Path as GraphPath
from neo4j.exceptions import Neo4jError

from deposit._recovery.identity import DeploymentIdentity
from deposit._recovery.journal import ProtectedJournal
from deposit._recovery.storage import ProtectedStorageError
from deposit._gateway import bind_arguments, build_query, detached_result, store_procedure
from deposit.errors import DepositError, TransportError
from deposit.recovery import recovered_operation
from deposit.store import Store, _STORE_FACTORY_TOKEN


class _MemoryStorage:
    MAX_RECORD_BYTES = 16 * 1024 * 1024

    def __init__(self):
        self.records = {}
        self.guard = threading.RLock()
        self.fail_state = None
        self.quota_bytes = None

    @contextmanager
    def lock(self, name="journal", *, timeout=0):
        with self.guard:
            yield self

    def write(self, name, plaintext):
        value = json.loads(plaintext)
        if self.fail_state == value.get("state"):
            self.fail_state = None
            raise ProtectedStorageError("simulated journal write failure")
        if self.quota_bytes is not None:
            used = sum(len(data) for data in self.records.values())
            prior = len(self.records.get(name, b""))
            if used - prior + len(plaintext) > self.quota_bytes:
                raise ProtectedStorageError("Per-user recovery storage quota (1 GiB) is exhausted")
        self.records[name] = bytes(plaintext)

    def read(self, name):
        return self.records[name]

    def list_records(self):
        return tuple(sorted(self.records))

    def delete(self, name):
        self.records.pop(name, None)


class _Client:
    def __init__(self, storage=None):
        self.storage = storage or _MemoryStorage()
        self.journal = ProtectedJournal(store=self.storage)
        self.server_actions = {}
        self.effects = []
        self.revision = 0
        self.sessions = {}
        self.active = {}
        self.closed_driver = False
        self.block = None
        self.on_execute = None
        self.on_cancel = None
        self.cancel_before_commit = False
        self.lose_prepare_reply = False
        self.lose_status_reply = False
        self.lose_execute_reply = False
        self.lose_release_reply = False
        self.cancelled = False
        self.requests = []
        self.cancelled_request_ids = []
        self.control_error = None
        self.control_started = None
        self.control_release = None
        self.administrative_modes = {}
        self.recovery_reads = []
        self.gateway_action_result = None
        self.gateway_rows = {}
        self.gateway_calls = []
        self.control_calls = []
        self.transactions = []

    def _session(self):
        return _GatewaySession(self)

    def get_administrative_mode(self, token):
        return self.administrative_modes.get(token, False)

    def set_administrative_mode(self, token, enabled):
        self.administrative_modes[token] = enabled
        return enabled

    def get_operation(self, token, operation_id, *, scope):
        self.recovery_reads.append(("operation", token, operation_id, scope))
        return {"operationId": operation_id, "state": "committed", "outcome": None, "scope": scope}

    def get_last_operation(self, token):
        self.recovery_reads.append(("last", token))
        return {"operationId": None, "state": "none", "outcome": None}

    def _run(self, query, parameters):
        self.gateway_calls.append((query, parameters))
        if self.control_error:
            raise self.control_error
        if "deposit.get.requests" in query:
            if self.control_started is not None:
                self.control_started.set()
            if self.control_release is not None and not self.control_release.wait(3):
                raise TimeoutError("test control response was not released")
            return _GatewayResult(("data",), [{"data": row} for row in self.requests])
        raise AssertionError(query)

    def get_session(self, token):
        session = self.sessions[token]
        actor_id = session.get("actor_id", "actor-id")
        namespace_id = session.get("namespace_id", "ns-id")
        return {"user": {"id": actor_id}, "namespaceId": namespace_id,
                "observed": {namespace_id: session["revision"]}}

    def logout(self, token):
        self.sessions.pop(token, None)

    def _store_closed(self, store):
        pass

    def make_store(self, token):
        self.sessions[token] = {"revision": self.revision, "actor_id": "actor-id", "namespace_id": "ns-id"}
        identity = DeploymentIdentity(1, "db.example:7687", "neo4j", "db-id", "dbms-id", "actor-id", "ns-id")
        return Store(self, token=token, actor_id="actor-id", namespace_id="ns-id", identity=identity,
                     recovery_directory=None, observed={"ns-id": self.revision},
                     _factory_token=_STORE_FACTORY_TOKEN, _journal=self.journal)

    def _single(self, query, parameters=None):
        parameters = parameters or {}
        if self.control_error and ("deposit.utils.cancel" in query or "deposit.utils.cancelRequest" in query):
            raise self.control_error
        if self.control_started is not None and (
            "deposit.utils.cancel" in query or "deposit.utils.cancelRequest" in query
        ):
            self.control_started.set()
        if self.control_release is not None and (
            "deposit.utils.cancel" in query or "deposit.utils.cancelRequest" in query
        ) and not self.control_release.wait(3):
            raise TimeoutError("test control response was not released")
        if "deposit.utils.refresh" in query:
            self.sessions[parameters["token"]]["revision"] = self.revision
            return {"data": {"scope": "namespace"}}
        if "deposit.utils.cancelRequest" in query:
            self.control_calls.append((query, parameters))
            self.cancelled_request_ids.append(parameters["a0"])
            return {"value": True}
        if "deposit.utils.cancel" in query:
            self.control_calls.append((query, parameters))
            if self.cancel_before_commit:
                self.cancelled = True
            if self.on_cancel:
                self.on_cancel()
            return {"value": True}
        if "deposit.action.status" in query:
            if self.lose_status_reply:
                self.lose_status_reply = False
                raise TransportError("simulated status reply loss")
            key = parameters["a0"]
            action = self.server_actions.get(key)
            if action is None:
                raise DepositError("NOT_FOUND", "Prepared action not found")
            return {"data": self._status(action)}
        if "deposit.action.prepare" in query:
            key, procedure, arguments = parameters["a0"], parameters["a1"], parameters["a2"]
            action = self.server_actions.setdefault(key, {
                "procedure": procedure, "arguments": arguments, "state": "prepared",
                "operationId": str(uuid.uuid4()), "outcome": None,
                "expected_revision": self.sessions[parameters["token"]]["revision"],
            })
            if self.lose_prepare_reply:
                self.lose_prepare_reply = False
                raise TransportError("simulated prepare reply loss")
            return {"data": self._status(action)}
        if "deposit.action.resume" in query:
            key = parameters["a0"]
            action = self.server_actions[key]
            self.active[parameters["token"]] = key
            return {"data": self._status(action)}
        if "deposit.action.release" in query:
            key = self.active.pop(parameters["token"], None)
            if key is None:
                raise DepositError("SESSION_BUSY", "No prepared action attached")
            if self.lose_release_reply:
                self.lose_release_reply = False
                raise TransportError("simulated release reply loss")
            return {"value": True}
        if "deposit.get.session" in query:
            session = self.sessions[parameters["token"]]
            return {"data": {"user": {"id": session.get("actor_id", "actor-id")},
                              "namespaceId": session.get("namespace_id", "ns-id"),
                              "observed": {session.get("namespace_id", "ns-id"): session["revision"]}}}
        if "deposit.create.class" in query or "deposit.create.object" in query:
            if self.block:
                if self.on_execute:
                    self.on_execute()
                self.block.wait()
            if self.cancelled:
                raise DepositError("CANCELLED", "Simulated cancellation before commit")
            token = parameters["token"]
            key = self.active[token]
            action = self.server_actions[key]
            if action["state"] == "committed":
                return {"data": action["outcome"]}
            if action["expected_revision"] != self.revision:
                raise DepositError("REVISION_CONFLICT", "Namespace revision changed")
            name = action["arguments"][0]
            entity = {
                "id": str(uuid.uuid4()), "namespaceId": "ns-id",
                "kind": "Class" if "create.class" in query else "Object",
                "name": name if isinstance(name, str) else "object",
                "descriptors": action["arguments"][1].get("descriptors", {}),
                "revision": self.revision + 1,
            }
            self.effects.append(entity)
            self.revision += 1
            self.sessions[token]["revision"] = self.revision
            action["state"] = "committed"
            action["outcome"] = entity
            if self.lose_execute_reply:
                self.lose_execute_reply = False
                raise TransportError("simulated execute reply loss")
            return {"data": entity}
        if "deposit.get.class" in query or "deposit.get.object" in query:
            selector = parameters["selection"]
            selected_id = selector.get("id") if isinstance(selector, dict) else None
            found = next((item for item in self.effects if item["id"] == selected_id or item["name"] == selector), None)
            if found is None:
                raise DepositError("NOT_FOUND", "Entity not found")
            return {"data": found}
        if "deposit.set.versioningType" in query:
            token = parameters["token"]
            key = self.active[token]
            action = self.server_actions[key]
            outcome = self.gateway_action_result
            if outcome is None:
                outcome = {"mode": parameters["a0"], "namespaceId": "ns-id"}
            action["state"] = "committed"
            action["outcome"] = outcome
            self.sessions[token]["revision"] = self.revision
            return {"data": outcome}
        if "deposit.resource.finishUpload" in query:
            token = parameters["token"]
            key = self.active[token]
            action = self.server_actions[key]
            outcome = {"uploadId": parameters["a0"], "finished": True}
            action["state"] = "committed"
            action["outcome"] = outcome
            return {"data": outcome}
        if "deposit.utils.logout" in query:
            self.logout(parameters["token"])
            return {"value": True}
        raise AssertionError(query)

    @staticmethod
    def _status(action):
        return {key: action[key] for key in ("state", "operationId", "outcome")}


class _GatewaySession:
    def __init__(self, client):
        self.client = client
        self.closed = False
        self.transaction = None

    def begin_transaction(self):
        self.transaction = _GatewayTransaction(self.client)
        self.client.transactions.append(self.transaction)
        return self.transaction

    def run(self, query, parameters):
        return self.client._run(query, parameters)

    def close(self):
        self.closed = True


class _GatewayResult:
    def __init__(self, columns, rows):
        self._columns = tuple(columns)
        self._rows = list(rows)
        self._index = 0
        self.consumed = False

    def keys(self):
        return list(self._columns)

    def __iter__(self):
        return self

    def __next__(self):
        if self._index >= len(self._rows):
            raise StopIteration
        value = self._rows[self._index]
        self._index += 1
        return value

    def consume(self):
        self._index = len(self._rows)
        self.consumed = True
        return None

    def single(self, strict=False):
        if strict and len(self._rows) != 1:
            raise ValueError("Expected exactly one record")
        return self._rows[0] if self._rows else None


class _GatewayTransaction:
    def __init__(self, client):
        self.client = client
        self.committed = False
        self.rolled_back = False
        self.calls = []

    def run(self, query, parameters):
        self.calls.append((query, parameters))
        self.client.gateway_calls.append((query, parameters))
        if "deposit.utils.toData" in query:
            return _GatewayResult(("data",), [{"data": {"id": "native-id", "namespaceId": "ns-id"}}])
        procedure = query.split("CALL ", 1)[1].split("(", 1)[0]
        response = self.client.gateway_rows.get(procedure)
        if response is None:
            raise AssertionError(query)
        columns, rows = response
        return _GatewayResult(columns, rows)

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True


def test_two_stores_are_independent_and_create_read_is_portable():
    client = _Client()
    first = client.make_store("first-token")
    second = client.make_store("second-token")

    created = first.create_class("Find", {"comment": "portable"})
    assert first.get_class({"id": created["id"]}) == created
    assert second.get_class({"id": created["id"]}) == created
    assert first._token != second._token
    assert first.namespace_id == second.namespace_id == "ns-id"
    assert len(client.effects) == 1


def test_manifest_gateway_defaults_bind_positions_and_returns_named_columns():
    client = _Client()
    store = client.make_store("manifest-gateway")
    client.gateway_rows["deposit.get.class"] = (("node",), [{"node": {"id": "class-id"}}])

    result = store.call("deposit.get.class")
    assert result.columns == ("node",)
    assert next(result) == {"node": {"id": "class-id"}}
    with pytest.raises(StopIteration):
        next(result)
    query, parameters = client.gateway_calls[-1]
    assert "CALL deposit.get.class($a0, $token) YIELD node RETURN node" in query
    assert parameters == {"a0": None, "token": "manifest-gateway"}
    assert client.transactions[-1].committed
    assert not client.transactions[-1].rolled_back
    assert not store._active

    client.gateway_rows["deposit.get.classPage"] = (("data",), [{"data": {"items": []}}])
    page = store.call("deposit.get.classPage")
    assert next(page) == {"data": {"items": []}}
    query, parameters = client.gateway_calls[-1]
    assert "CALL deposit.get.classPage($a0, $a1, $token) YIELD data RETURN data" in query
    assert parameters == {"a0": None, "a1": {}, "token": "manifest-gateway"}
    page.close()
    assert client.transactions[-1].rolled_back


def test_manifest_gateway_rejects_non_store_routes_and_store_retargeting():
    client = _Client()
    store = client.make_store("manifest-allowlist")
    for procedure in (
        "deposit.utils.login",            # Client owner
        "deposit.get.user",               # Client owner/server scope
        "deposit.action.status",          # RecoveryCoordinator owner
        "deposit.admin.pruneResources",  # Client owner/prune scope
        "deposit.notReviewed",
    ):
        with pytest.raises(ValueError, match="allowlisted Store"):
            store.call(procedure)
    with pytest.raises(ValueError, match="another Namespace"):
        store.call("deposit.get.class", {"id": "class-id", "namespaceId": "foreign-ns"})
    injected_session = type("Session", (), {"__module__": "neo4j._sync.work.session"})()
    with pytest.raises(TypeError, match="session, transaction, or result"):
        store.call("deposit.get.class", injected_session)
    with pytest.raises(TypeError):
        store.call("deposit.get.class", "Find", token="injected")
    assert not client.gateway_calls
    assert not client.transactions
    assert not client.server_actions


def test_every_operational_manifest_entry_has_exact_store_gateway_classification():
    manifest = json.loads(
        (Path(os.environ.get("DEPOSIT_PYTHON_SOURCE_ROOT", Path.cwd()))
         / "src/deposit/_procedure_manifest.json").read_text(encoding="utf-8")
    )
    operational = [
        entry for entry in manifest["procedures"]
        if entry["invocation_owner"] == "Store" and entry["scope"] == "operational"
    ]
    assert len(operational) == 17
    for entry in operational:
        assert store_procedure(entry["name"]) == entry
        assert entry["invocation_owner"] == "Store"
        assert entry["scope"] == "operational"
        assert entry["requires_token"] is True
        assert entry["action"] is True
        assert entry["columns"] == ["data"]
        assert entry["batch_eligible"] is False
        assert entry["batch_exclusion"] == "not registered in DomainService.COMMANDS"
        arguments = tuple(f"arg-{index}" for index, _ in enumerate(entry["args"]))
        bound = bind_arguments(entry, arguments, namespace_id="ns-id")
        query, parameters = build_query(entry, bound, "store-token")
        assert bound == arguments
        assert entry["name"] in query
        assert "YIELD data RETURN data" in query
        assert parameters == {
            **{f"a{index}": value for index, value in enumerate(arguments)},
            "token": "store-token",
        }


def test_namespace_action_call_uses_existing_prepare_journal_and_releases_saved_rows():
    client = _Client()
    store = client.make_store("gateway-action")

    result = store.call("deposit.set.versioningType", "Disabled")

    assert result.columns == ("data",)
    original_rows = tuple(result)
    assert original_rows == ({"data": {"mode": "Disabled", "namespaceId": "ns-id"}},)
    records = store._journal.records(store._identity)
    assert len(records) == 1 and records[0]["state"] == "released"
    assert records[0]["result_format"] == "named_rows_v1"
    assert records[0]["outcome"] == {
        "columns": ["data"],
        "rows": [{"data": {"mode": "Disabled", "namespaceId": "ns-id"}}],
    }
    recovered = recovered_operation(records[0])
    assert recovered.result["columns"] == result.columns
    assert recovered.result["rows"] == original_rows
    assert list(client.server_actions.values())[0]["state"] == "committed"


def test_detached_advanced_results_preserve_empty_and_multi_column_rows():
    entry = {"columns": ["value", "data"]}
    empty = detached_result(entry, {"columns": ["value", "data"], "rows": []}, "token")
    assert empty.columns == ("value", "data")
    assert tuple(empty) == ()

    multiple = detached_result(
        entry,
        {
            "columns": ["value", "data"],
            "rows": [
                {"value": 1, "data": {"items": [True]}},
                {"value": 2, "data": {"items": []}},
            ],
        },
        "token",
    )
    rows = tuple(multiple)
    assert multiple.columns == ("value", "data")
    assert rows == (
        {"value": 1, "data": {"items": [True]}},
        {"value": 2, "data": {"items": []}},
    )


def test_namespace_gateway_action_recovers_map_rows_after_lost_release_reply():
    client = _Client()
    first = client.make_store("gateway-action-before-reconnect")
    client.lose_release_reply = True

    with pytest.raises(TransportError, match="release reply loss"):
        first.call("deposit.set.versioningType", "Disabled")
    first._client._store_closed(first)

    resumed = client.make_store("gateway-action-after-reconnect")
    recovered = resumed._journal.records(resumed._identity)
    assert len(recovered) == 1 and recovered[0]["state"] == "released"
    assert recovered[0]["result_format"] == "named_rows_v1"
    assert recovered[0]["outcome"] == {
        "columns": ["data"],
        "rows": [{"data": {"mode": "Disabled", "namespaceId": "ns-id"}}],
    }
    assert len(client.effects) == 0


def test_operational_action_recovery_ignores_changed_namespace_revision_but_reuses_exact_key_and_inputs():
    client = _Client()
    first = client.make_store("operational-before-change")
    client.lose_prepare_reply = True

    with pytest.raises(TransportError, match="prepare reply loss"):
        first.call("deposit.resource.finishUpload", "upload-id-original")
    pending = first._journal.records(first._identity)
    assert len(pending) == 1
    original_key = pending[0]["key"]
    original_inputs = pending[0]["arguments"]
    original_revision = pending[0]["snapshot_revision"]
    assert original_inputs == ["upload-id-original"]
    assert pending[0]["state"] == "prepare_sent"

    # The preparation never survived on the server, while another Namespace
    # action advanced the observed revision before recovery attempted it again.
    client.server_actions.clear()
    client.revision = original_revision + 1
    client.sessions[first._token]["revision"] = client.revision
    first._client._store_closed(first)
    resumed = client.make_store("operational-after-change")

    recovered = resumed._journal.records(resumed._identity)
    assert len(recovered) == 1
    assert recovered[0]["key"] == original_key
    assert recovered[0]["arguments"] == original_inputs
    assert recovered[0]["snapshot_revision"] == original_revision
    assert recovered[0]["state"] == "released"
    assert recovered[0]["result_format"] == "named_rows_v1"
    assert recovered[0]["outcome"] == {
        "columns": ["data"],
        "rows": [{"data": {"uploadId": "upload-id-original", "finished": True}}],
    }
    assert len(client.server_actions) == 1


def test_advanced_action_result_map_can_contain_former_internal_key():
    client = _Client()
    store = client.make_store("gateway-sentinel-collision")
    legitimate_data = {
        "__deposit_gateway_result__": {"ordinary": [1, {"value": "caller data"}]},
        "columns": ["these", "are", "data"],
    }
    client.gateway_action_result = legitimate_data

    result = store.call("deposit.set.versioningType", "Disabled")
    original_rows = tuple(result)
    record = store._journal.records(store._identity)[0]

    assert original_rows == ({"data": legitimate_data},)
    assert record["outcome"] == {"columns": ["data"], "rows": [{"data": legitimate_data}]}
    recovered = recovered_operation(record).result
    assert recovered["columns"] == ("data",)
    assert recovered["rows"][0]["data"]["__deposit_gateway_result__"]["ordinary"] == (
        1,
        {"value": "caller data"},
    )
    with pytest.raises(TypeError):
        recovered["rows"][0]["data"]["new"] = True


def test_advanced_store_result_native_values_are_transaction_bound_and_convert_in_owner_tx():
    client = _Client()
    store = client.make_store("native-result")
    graph = Graph()
    native = Node(graph, "4:driver-id", 4, None, {"name": "Find"})
    client.gateway_rows["deposit.get.class"] = (("node",), [{"node": native}])

    result = store.call("deposit.get.class", "Find")
    row = next(result)
    node = row["node"]
    assert node.element_id == "4:driver-id"
    assert node["name"] == "Find"
    assert result.to_data(node) == {"id": "native-id", "namespaceId": "ns-id"}
    query, parameters = client.transactions[-1].calls[-1]
    assert "MATCH (entity) WHERE elementId(entity) = $element_id" in query
    assert "CALL deposit.utils.toData(entity, $token)" in query
    assert parameters == {"element_id": "4:driver-id", "token": store._token}
    with pytest.raises(StopIteration):
        next(result)
    assert client.transactions[-1].committed
    with pytest.raises(RuntimeError, match="no longer valid"):
        _ = node["id"]
    with pytest.raises(RuntimeError, match="open native StoreResult"):
        result.to_data(node)


def test_advanced_store_result_converts_dynamic_relationship_and_rejects_paths():
    client = _Client()
    store = client.make_store("native-relationship-result")
    graph = Graph()
    relationship_type = graph.relationship_type("Stage3Link")
    native_relationship = relationship_type(graph, "7:relationship-id", 7, {"label": "link"})
    client.gateway_rows["deposit.get.relationship"] = (
        ("rel",), [{"rel": native_relationship}]
    )

    result = store.call("deposit.get.relationship", {"id": "relationship-id"})
    relationship = next(result)["rel"]
    assert relationship.element_id == "7:relationship-id"
    assert result.to_data(relationship) == {"id": "native-id", "namespaceId": "ns-id"}
    query, parameters = client.transactions[-1].calls[-1]
    assert "MATCH ()-[entity]->() WHERE elementId(entity) = $element_id" in query
    assert "CALL deposit.utils.toData(entity, $token)" in query
    assert parameters == {"element_id": "7:relationship-id", "token": store._token}
    with pytest.raises(StopIteration):
        next(result)

    path_store = client.make_store("native-path-result")
    native_node = Node(graph, "8:path-node", 8, None, {})
    native_path = GraphPath(native_node)
    client.gateway_rows["deposit.get.query"] = (
        ("data", "columns", "presence"),
        [{"data": {"path": native_path}, "columns": ["path"], "presence": [True]}],
    )
    path_result = path_store.call("deposit.get.query", "RETURN 1", {}, {})
    path_value = next(path_result)["data"]["path"]
    with pytest.raises(TypeError, match="nodes and relationships"):
        path_result.to_data(path_value)
    assert path_result.closed
    assert client.transactions[-1].rolled_back


def test_advanced_store_result_does_not_expose_graph_or_lazy_native_iterators_after_close():
    client = _Client()
    store = client.make_store("native-lifetime-escape")
    graph = Graph()
    native = Node(graph, "9:driver-id", 9, None, {"alpha": 1, "beta": 2})
    client.gateway_rows["deposit.get.class"] = (("node",), [{"node": native}])

    result = store.call("deposit.get.class", "Lifetime")
    node = next(result)["node"]
    with pytest.raises(TypeError, match="graph container is not exposed"):
        _ = node.graph

    iterator = iter(node)
    assert next(iterator) == "alpha"
    property_keys = node.keys()
    result.close()
    with pytest.raises(RuntimeError, match="no longer valid"):
        next(iterator)
    assert property_keys == ("alpha", "beta")
    assert tuple(property_keys) == ("alpha", "beta")


def test_advanced_store_result_early_close_and_iteration_error_release_transaction():
    client = _Client()
    store = client.make_store("result-cleanup")
    client.gateway_rows["deposit.get.query"] = (
        ("data", "columns", "presence"),
        [
            {"data": {"rows": [[1]]}, "columns": ["n"], "presence": [True]},
            {"data": {"rows": [[2]]}, "columns": ["n"], "presence": [True]},
        ],
    )
    result = store.call("deposit.get.query", "RETURN 1", {}, {})
    assert result.columns == ("data", "columns", "presence")
    assert next(result)["columns"] == ["n"]
    result.close()
    assert client.transactions[-1].rolled_back
    assert not store._active


def test_advanced_store_result_iteration_error_rolls_back_and_releases_store():
    client = _Client()
    store = client.make_store("iteration-failure")
    class _BrokenResult(_GatewayResult):
        def __next__(self):
            raise RuntimeError("iteration failed")

    class _BrokenTransaction(_GatewayTransaction):
        def run(self, query, parameters):
            self.calls.append((query, parameters))
            self.client.gateway_calls.append((query, parameters))
            return _BrokenResult(("data", "columns", "presence"), [])

    class _BrokenSession(_GatewaySession):
        def begin_transaction(self):
            self.transaction = _BrokenTransaction(self.client)
            self.client.transactions.append(self.transaction)
            return self.transaction

    client._session = lambda: _BrokenSession(client)
    failed = store.call("deposit.get.query", "RETURN 1", {}, {})
    with pytest.raises(TransportError, match="Neo4j request failed"):
        next(failed)
    assert client.transactions[-1].rolled_back
    assert not store._active


def test_advanced_store_result_maps_wrapped_deposit_error():
    client = _Client()
    store = client.make_store("wrapped-result-error")

    class _WrappedNeo4jError(Neo4jError):
        def __init__(self):
            super().__init__()

        @property
        def message(self):
            return (
                "Failed to invoke procedure `deposit.get.query`: Caused by: "
                "deposit.common.ExceptionHandler$DepositException: DEPOSIT_ERROR "
                '{"code":"FORBIDDEN","details":{},"message":"Read denied",'
                '"requestId":"9cd08e5a-7ad5-4b0c-9e58-50de7e0412bc","retryable":false}'
            )

        @property
        def code(self):
            return "Neo.ClientError.Procedure.ProcedureCallFailed"

    class _BrokenResult(_GatewayResult):
        def __next__(self):
            raise _WrappedNeo4jError()

    class _BrokenTransaction(_GatewayTransaction):
        def run(self, query, parameters):
            self.calls.append((query, parameters))
            self.client.gateway_calls.append((query, parameters))
            return _BrokenResult(("data", "columns", "presence"), [])

    class _BrokenSession(_GatewaySession):
        def begin_transaction(self):
            self.transaction = _BrokenTransaction(self.client)
            self.client.transactions.append(self.transaction)
            return self.transaction

    client._session = lambda: _BrokenSession(client)
    result = store.call("deposit.get.query", "RETURN 1", {}, {})
    with pytest.raises(DepositError) as failure:
        next(result)
    assert failure.value.code == "FORBIDDEN"
    assert failure.value.request_id == "9cd08e5a-7ad5-4b0c-9e58-50de7e0412bc"
    assert client.transactions[-1].rolled_back
    assert not store._active


def test_advanced_store_result_conversion_failure_rolls_back_and_releases_store():
    client = _Client()
    store = client.make_store("conversion-failure")
    client.gateway_rows["deposit.get.class"] = (("node",), [{"node": {"id": "native-id"}}])

    result = store.call("deposit.get.class", "Find")
    next(result)
    with pytest.raises(TypeError, match="owned by this StoreResult"):
        result.to_data({"id": "foreign"})
    assert result.closed
    assert client.transactions[-1].rolled_back
    assert not store._active


def test_to_data_rejects_foreign_expired_and_detached_native_values():
    client = _Client()
    first_store = client.make_store("conversion-first")
    second_store = client.make_store("conversion-second")
    graph = Graph()
    native = Node(graph, "9:native-id", 9, None, {"id": "native-id"})
    client.gateway_rows["deposit.get.class"] = (("node",), [{"node": native}])

    first_result = first_store.call("deposit.get.class", "Find")
    first_native = next(first_result)["node"]
    second_result = second_store.call("deposit.get.class", "Find")
    with pytest.raises(TypeError, match="owned by this StoreResult"):
        second_result.to_data(first_native)
    assert second_result.closed
    assert client.transactions[-1].rolled_back

    assert first_result.to_data(first_native) == {"id": "native-id", "namespaceId": "ns-id"}
    first_result.close()
    with pytest.raises(RuntimeError, match="open native StoreResult"):
        first_result.to_data(first_native)

    detached = first_store.call("deposit.set.versioningType", "Disabled")
    with pytest.raises(RuntimeError, match="open native StoreResult"):
        detached.to_data(first_native)


def test_store_close_releases_an_open_advanced_result():
    client = _Client()
    store = client.make_store("close-open-result")
    client.gateway_rows["deposit.get.revision"] = (("data",), [{"data": {"revision": 0}}])
    result = store.call("deposit.get.revision")
    assert store._active
    store.close()
    assert result.closed
    assert client.transactions[-1].rolled_back
    assert not store._active


@pytest.mark.parametrize(
    "properties",
    [
        {"nested": {1: "integer key", "1": "colliding string key"}},
        {"outer": [{"inner": {1: "nested non-string key"}}]},
    ],
)
def test_store_rejects_colliding_or_nested_non_string_keys_before_journaling_or_server_calls(properties):
    client = _Client()
    store = client.make_store("reject-lossy-keys")

    with pytest.raises(TypeError, match="string mapping keys"):
        store.create_class("Invalid", properties)

    assert store._journal.records(store._identity) == []
    assert client.server_actions == {}
    assert client.effects == []


def test_store_preserves_valid_nested_json_mapping_inputs_exactly():
    client = _Client()
    store = client.make_store("nested-json-input")
    properties = {
        "outer": {
            "alpha": 1,
            "2": {"nested": [True, None, 1.5, {"leaf": "value"}]},
        }
    }

    store.create_class("Nested", properties)

    record = store._journal.records(store._identity)[0]
    action = next(iter(client.server_actions.values()))
    assert record["arguments"] == ["Nested", properties]
    assert action["arguments"] == ["Nested", properties]


def test_store_rejects_public_token_and_namespace_substitution():
    client = _Client()
    store = client.make_store("bound-token")

    with pytest.raises(TypeError):
        store.create_class("Injected", token="other-token")
    with pytest.raises(TypeError):
        store.get_class({"id": "entity-id"}, namespace_id="other-namespace")
    assert not hasattr(store, "token")
    assert store.namespace_id == "ns-id"

    client.sessions["other-token"] = {
        "revision": client.revision,
        "actor_id": "different-actor",
        "namespace_id": "ns-id",
    }
    store._token = "other-token"
    with pytest.raises(DepositError) as failure:
        store.create_class("Substituted")
    assert failure.value.code == "IDENTITY_MISMATCH"
    assert client.server_actions == {} and client.effects == []

    store._token = "bound-token"
    client.sessions["bound-token"]["namespace_id"] = "other-namespace"
    with pytest.raises(DepositError) as failure:
        store.create_class("Retargeted")
    assert failure.value.code == "SCOPE_MISMATCH"
    assert client.server_actions == {} and client.effects == []


def test_lost_execute_reply_recovers_original_outcome_without_duplicate_effect():
    client = _Client()
    first = client.make_store("before-restart")
    client.lose_execute_reply = True
    with pytest.raises(TransportError, match="execute reply loss"):
        first.create_class("Find")
    assert len(client.effects) == 1
    first._client._store_closed(first)
    second = client.make_store("after-restart")
    recovered = [record for record in second._journal.records(second._identity) if record["state"] == "released"]
    assert len(recovered) == 1
    assert recovered[0]["outcome"]["name"] == "Find"
    assert len(client.effects) == 1


def test_lost_prepare_reply_reuses_same_key_and_exact_arguments():
    client = _Client()
    first = client.make_store("before-restart")
    client.lose_prepare_reply = True
    with pytest.raises(TransportError, match="prepare reply loss"):
        first.create_class("Original", {"comment": "unchanged"})
    first._client._store_closed(first)
    second = client.make_store("after-restart")
    assert len(client.server_actions) == 1
    assert len(client.effects) == 1
    record = second._journal.records(second._identity)[0]
    assert record["arguments"][1]["comment"] == "unchanged"
    assert record["outcome"]["name"] == "Original"


def test_lost_status_reply_preserves_intent_and_recovers_without_duplicate_effect():
    client = _Client()
    first = client.make_store("before-status-loss")
    client.lose_status_reply = True
    with pytest.raises(TransportError, match="status reply loss"):
        first.create_class("Status-loss")
    before = first._journal.records(first._identity)
    assert len(before) == 1 and before[0]["state"] == "prepare_sent"
    first._client._store_closed(first)

    second = client.make_store("after-status-loss")
    recovered = second._journal.records(second._identity)
    assert len(recovered) == 1
    assert recovered[0]["state"] == "released"
    assert recovered[0]["outcome"]["name"] == "Status-loss"
    assert len(client.effects) == 1


def test_lost_release_reply_recovers_durable_original_result():
    client = _Client()
    first = client.make_store("before-release-loss")
    client.lose_release_reply = True
    with pytest.raises(TransportError, match="release reply loss"):
        first.create_class("Release-loss", {"comment": "portable"})
    pending = first._journal.records(first._identity)
    assert len(pending) == 1 and pending[0]["state"] == "release_sent"
    original_id = client.effects[0]["id"]
    first._client._store_closed(first)

    second = client.make_store("after-release-loss")
    recovered = second._journal.records(second._identity)
    assert recovered[0]["state"] == "released"
    assert recovered[0]["outcome"] == client.effects[0]
    assert recovered[0]["outcome"]["id"] == original_id
    assert len(client.effects) == 1


def test_simultaneous_same_action_resume_commits_once():
    client = _Client()
    store = client.make_store("same-action")
    client.lose_execute_reply = True
    with pytest.raises(TransportError):
        store.create_class("One-effect")
    record = store._journal.records(store._identity)[0]
    start = threading.Barrier(3)
    results = []
    failures = []

    def recover():
        try:
            start.wait()
            results.append(store._resolve_action_locked(record, allow_new_prepare=False))
        except Exception as exc:
            failures.append(exc)

    workers = [threading.Thread(target=recover) for _ in range(2)]
    for worker in workers:
        worker.start()
    start.wait()
    for worker in workers:
        worker.join(3)
    assert all(not worker.is_alive() for worker in workers)
    assert not failures
    assert len(results) == 2 and results[0] == results[1]
    assert len(client.effects) == 1
    assert store._journal.load(store._identity, record["key"])["state"] == "released"


def test_outcome_journal_write_failure_never_releases_and_recovers_same_action():
    storage = _MemoryStorage()
    client = _Client(storage)
    store = client.make_store("write-failure")
    storage.fail_state = "outcome_saved"
    with pytest.raises(ProtectedStorageError, match="simulated journal write failure"):
        store.create_class("Durable-before-release")
    record = store._journal.records(store._identity)[0]
    assert record["state"] == "executing"
    assert len(client.effects) == 1
    assert client.active.get(store._token) == record["key"]

    resumed = store._resolve_action_locked(record, allow_new_prepare=False)
    assert resumed == client.effects[0]
    assert store._journal.load(store._identity, record["key"])["state"] == "released"
    assert len(client.effects) == 1


def test_quota_failure_preserves_existing_unresolved_record():
    storage = _MemoryStorage()
    storage.quota_bytes = 700
    client = _Client(storage)
    store = client.make_store("quota")
    first = store._journal.create(store._identity, "deposit.create.class", ["Keep", {}], client.revision)
    with pytest.raises(ProtectedStorageError, match="1 GiB"):
        store._journal.create(store._identity, "deposit.create.class", ["x" * 5000, {}], client.revision)
    assert store._journal.load(store._identity, first["key"]) == first
    assert store._journal.unresolved(store._identity) == [first]


def test_cancel_near_commit_keeps_prepared_action_pending_without_effect():
    client = _Client()
    store = client.make_store("cancel-near-commit")
    started = threading.Event()
    release = threading.Event()
    client.block = release
    failure = []
    client.on_execute = started.set
    client.cancel_before_commit = True

    def create():
        try:
            store.create_class("Cancelled-before-commit")
        except Exception as exc:
            failure.append(exc)

    worker = threading.Thread(target=create)
    worker.start()
    assert started.wait(3)  # The worker reached the execution call and is blocked before commit.
    client.on_cancel = lambda: release.set()
    assert store.cancel() is True
    worker.join(3)
    assert not worker.is_alive()
    assert len(failure) == 1 and isinstance(failure[0], DepositError)
    assert failure[0].code == "CANCELLED"
    pending = store._journal.unresolved(store._identity)
    assert len(pending) == 1 and pending[0]["state"] == "executing"
    assert len(client.effects) == 0


def test_request_list_and_targeted_cancel_use_parallel_control_path_while_store_is_busy():
    client = _Client()
    store = client.make_store("parallel-controls")
    client.requests = [
        {"requestId": "request-one", "actorId": "actor-one", "namespaceId": "ns-one"},
        {"requestId": "request-two", "actorId": "actor-two", "namespaceId": None},
    ]
    finished = threading.Event()
    outcome = []
    store._operation_lock.acquire()

    def controls():
        try:
            listed = store.get_requests()
            cancelled = store.cancel_request("request-two")
            outcome.extend((listed, cancelled))
        except Exception as exc:
            outcome.append(exc)
        finally:
            finished.set()

    worker = threading.Thread(target=controls)
    try:
        worker.start()
        assert finished.wait(1), "parallel controls waited behind the busy Store operation lock"
    finally:
        store._operation_lock.release()
        worker.join(3)

    assert len(outcome) == 2
    listed, cancelled = outcome
    assert [dict(row) for row in listed] == client.requests
    assert listed[1]["namespaceId"] is None
    with pytest.raises(TypeError):
        listed[0]["requestId"] = "changed"
    assert cancelled is True
    assert client.cancelled_request_ids == ["request-two"]
    assert client.gateway_calls[0][1] == {"token": "parallel-controls"}
    assert client.control_calls[0][1] == {"a0": "request-two", "token": "parallel-controls"}
    assert "CALL deposit.get.requests($token) YIELD data RETURN data" in client.gateway_calls[0][0]
    assert "CALL deposit.utils.cancelRequest($a0, $token) YIELD value RETURN value" in client.control_calls[0][0]


def test_request_list_empty_and_control_errors_are_not_hidden():
    client = _Client()
    store = client.make_store("control-errors")
    assert store.get_requests() == ()

    denial = DepositError("ACCESS_DENIED", "Not authorized")
    client.control_error = denial
    with pytest.raises(DepositError) as listed:
        store.get_requests()
    assert listed.value.code == "ACCESS_DENIED"
    with pytest.raises(DepositError) as cancelled:
        store.cancel_request("request-id")
    assert cancelled.value.code == "ACCESS_DENIED"
    with pytest.raises(DepositError) as cancel_all:
        store.cancel()
    assert cancel_all.value.code == "ACCESS_DENIED"


def test_store_cancel_propagates_transport_errors_and_requires_boolean_result():
    client = _Client()
    store = client.make_store("cancel-errors")
    client.control_error = TransportError("control transport failed")
    with pytest.raises(TransportError, match="control transport failed"):
        store.cancel()
    client.control_error = None
    client._single = lambda query, parameters=None: {"value": "yes"}
    with pytest.raises(DepositError, match="no boolean result"):
        store.cancel()


def test_store_administrative_mode_is_explicit_and_isolated_per_session():
    client = _Client()
    first = client.make_store("admin-mode-first")
    second = client.make_store("admin-mode-second")

    assert first.get_administrative_mode() is False
    assert second.get_administrative_mode() is False
    assert first.set_administrative_mode(True) is True
    assert first.get_administrative_mode() is True
    assert second.get_administrative_mode() is False
    assert client.administrative_modes == {"admin-mode-first": True}

    with pytest.raises(TypeError, match="enabled must be a bool"):
        first.set_administrative_mode(1)

    first._operation_lock.acquire()
    finished = threading.Event()
    update = []

    def set_mode():
        try:
            update.append(first.set_administrative_mode(False))
        finally:
            finished.set()

    worker = threading.Thread(target=set_mode)
    try:
        worker.start()
        assert not finished.wait(0.1), "administrative-mode setter bypassed Store serialization"
    finally:
        first._operation_lock.release()
        worker.join(3)
    assert finished.is_set() and update == [False]

def test_store_operation_reads_are_serialized_and_fixed_to_namespace_scope():
    client = _Client()
    store = client.make_store("operation-read")
    operation_id = str(uuid.uuid4())
    assert store.get_operation(operation_id) == {
        "operationId": operation_id, "state": "committed", "outcome": None, "scope": "namespace"
    }
    assert store.get_last_operation() == {"operationId": None, "state": "none", "outcome": None}
    assert client.recovery_reads == [
        ("operation", "operation-read", operation_id, "namespace"),
        ("last", "operation-read"),
    ]


@pytest.mark.parametrize("method", ["get_requests", "cancel_request", "cancel"])
def test_close_waits_for_inflight_parallel_controls_before_logout(method):
    client = _Client()
    store = client.make_store("close-control-race-" + method)
    client.requests = [{"requestId": "request-id", "actorId": "actor-id", "namespaceId": "ns-id"}]
    client.control_started = threading.Event()
    client.control_release = threading.Event()
    failure = []

    def invoke_control():
        try:
            if method == "cancel_request":
                store.cancel_request("request-id")
            else:
                getattr(store, method)()
        except Exception as exc:
            failure.append(exc)

    control_thread = threading.Thread(target=invoke_control)
    close_thread = threading.Thread(target=store.close)
    control_thread.start()
    assert client.control_started.wait(2)
    close_thread.start()
    for _ in range(100):
        with store._state_lock:
            if store._closing:
                break
        threading.Event().wait(0.01)
    assert store._closing
    assert not store.closed
    assert "close-control-race-" + method in client.sessions
    client.control_release.set()
    control_thread.join(3)
    close_thread.join(3)
    assert not control_thread.is_alive() and not close_thread.is_alive()
    assert not failure
    assert store.closed
    assert "close-control-race-" + method not in client.sessions


def test_missing_preparation_with_changed_revision_is_never_recreated():
    client = _Client()
    first = client.make_store("before-restart")
    client.lose_prepare_reply = True
    with pytest.raises(TransportError):
        first.create_class("Original")
    client.server_actions.clear()  # The prepare request never reached the server.
    first._client._store_closed(first)
    client.revision += 1
    second = client.make_store("after-restart")
    assert client.server_actions == {}
    assert client.effects == []
    assert second._journal.unresolved(second._identity)[0]["state"] == "conflicted"


def test_conflict_preserves_pending_inputs_and_refresh_allows_explicit_reapplication():
    client = _Client()
    first = client.make_store("first-token")
    second = client.make_store("second-token")
    first.create_class("Find")
    assert second.get_class("Find")["name"] == "Find"
    assert second._observed_revision == 0

    with pytest.raises(DepositError) as failure:
        second.create_class("Stale", {"comment": "keep me"})
    assert failure.value.code == "REVISION_CONFLICT"
    conflicted = next(record for record in second._journal.records(second._identity) if record["state"] == "conflicted")
    assert conflicted["arguments"] == ["Stale", {"comment": "keep me"}]

    second.refresh()
    created = second.create_class("Reapplied", {"comment": "keep me"})
    assert created["name"] == "Reapplied"
    assert len(client.effects) == 2


def test_close_cancels_through_control_path_before_waiting_for_active_operation():
    client = _Client()
    store = client.make_store("token")
    started = threading.Event()
    release = threading.Event()
    client.block = release
    client.on_cancel = lambda: (started.set(), release.set())
    failure = []

    def create():
        try:
            store.create_class("Find")
        except Exception as exc:
            failure.append(exc)

    worker = threading.Thread(target=create)
    worker.start()
    assert started.wait(1) is False  # the control callback has not run yet
    closer = threading.Thread(target=store.close)
    closer.start()
    worker.join(3)
    closer.join(3)
    assert not worker.is_alive() and not closer.is_alive()
    assert store.closed
    assert not failure
