from __future__ import annotations

import json
import multiprocessing
from pathlib import Path
import os
from contextlib import contextmanager
import threading
from collections.abc import Mapping

import pytest
from neo4j.exceptions import Neo4jError

from deposit import Client, CompatibilityError, DepositError, TransportError
from deposit.recovery import PendingOperation, RecoveredOperation
from deposit._recovery.identity import DeploymentIdentity
from deposit._recovery.journal import ProtectedJournal


class _EncodedNeo4jError(Neo4jError):
    def __init__(self, message, code="Neo.ClientError.Procedure.ProcedureCallFailed"):
        super().__init__()
        self._test_message = message
        self._test_code = code

    @property
    def message(self):
        return self._test_message

    @property
    def code(self):
        return self._test_code


class _Record(dict):
    pass


class _Result:
    def __init__(self, records):
        self.records = records if isinstance(records, list) else [records]

    def single(self, strict=False):
        if strict and len(self.records) != 1:
            raise ValueError("Expected exactly one record")
        return self.records[0] if self.records else None

    def __iter__(self):
        return iter(self.records)


class _Session:
    def __init__(self, driver):
        self.driver = driver

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None

    def run(self, query, parameters):
        self.driver.calls.append((query, parameters))
        records = self.driver.responses.pop(0)
        if isinstance(records, Exception):
            raise records
        if "dbms.components()" in query and isinstance(records, list):
            records = [
                _Record(version=record["versions"][0])
                for record in records
                if record.get("name") == "Neo4j Kernel"
            ]
        return _Result(records)


class _Driver:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []
        self.closed = False

    def session(self, **kwargs):
        return _Session(self)

    def close(self):
        self.closed = True


def _hold_action_lock_in_child(root, key, ready, release):
    from deposit._recovery.storage import ProtectedStore

    storage = object.__new__(ProtectedStore)
    storage.root = Path(root)
    journal = ProtectedJournal(store=storage)
    with journal.action_lock(key):
        ready.set()
        if not release.wait(15):
            raise TimeoutError("Parent did not release the action-lock test")


def test_discovery_login_session_logout_and_driver_ownership():
    manifest = json.loads((Path(os.environ.get("DEPOSIT_PYTHON_SOURCE_ROOT", Path.cwd())) / "src/deposit/_procedure_manifest.json").read_text())
    version = manifest["support"]["target_server_version"]
    native_version = manifest["support"]["target_neo4j_version"]
    def discovery():
        return [
            _Record(data={"version": version}),
            [
                _Record(name="Neo4j Kernel", versions=[native_version]),
                _Record(name="Cypher", versions=["5", "25"]),
            ],
        ]
    driver = _Driver([
        *discovery(),
        *discovery(),
        _Record(value="opaque-token"),
        *discovery(),
        _Record(data={"user": "alice", "expiresAt": "later"}),
        *discovery(),
        _Record(data="loggedOut"),
    ])
    client = Client(driver)
    assert client.discover() == {"version": version}
    component_query = next(query for query, _ in driver.calls if "dbms.components()" in query)
    assert "YIELD name, versions WHERE name = 'Neo4j Kernel'" in component_query
    token = client.login("alice", "correct-horse")
    assert token == "opaque-token"
    assert client.get_session(token)["user"] == "alice"
    assert client.logout(token) is None
    client.close()
    assert not driver.closed
    login_call = next(call for call in driver.calls if "utils.login" in call[0])
    assert login_call[1] == {"username": "alice", "password": "correct-horse"}


def test_client_manifest_gateway_binds_allowlisted_non_action_calls_and_converts_native_values():
    client_methods = [
        ("set_active_namespace", (), "deposit.set.activeNamespace", (None,), "node", {"id": "active-id"}),
        ("set_active_namespace", ({"id": "target-id"},), "deposit.set.activeNamespace", ({"id": "target-id"},), "node", {"id": "target-id"}),
        ("get_administrative_mode", (), "deposit.get.administrativeMode", (), "value", True),
        ("set_administrative_mode", (False,), "deposit.set.administrativeMode", (False,), "value", False),
        ("get_user", ({"userId": "alice-id"},), "deposit.get.user", ({"userId": "alice-id"},), "data", {"username": "alice"}),
        ("get_user_page", (), "deposit.get.userPage", (None, {}), "data", {"items": []}),
        ("get_namespace", ("Research",), "deposit.get.namespace", ("Research",), "node", {"id": "ns-id"}),
        ("get_namespace_page", (None, {"limit": 10}), "deposit.get.namespacePage", (None, {"limit": 10}), "data", {"items": []}),
        ("prune_resources_status", ("prune-id",), "deposit.admin.pruneResourcesStatus", ("prune-id",), "data", {"state": "complete"}),
        ("coordinates_to_wkt", ("Point", [1, 2]), "deposit.utils.coordinatesToWKT", ("Point", [1, 2], {}), "data", {"wkt": "POINT (1 2)"}),
        ("wkt_to_coordinates", ("POINT (1 2)",), "deposit.utils.wktToCoordinates", ("POINT (1 2)", {}), "data", {"coordinates": [1, 2]}),
    ]
    responses = []
    for _, _, _, _, column, result in client_methods:
        responses.append(_Record(**{column: result}))
    driver = _Driver(responses)
    client = Client(driver)
    client.discover = lambda: {"version": "tested"}

    for method_name, call_args, procedure, bound_args, column, result in client_methods:
        assert getattr(client, method_name)("session-token", *call_args) == result
        query, parameters = driver.calls[-1]
        expected_arguments = {
            **{f"a{index}": value for index, value in enumerate(bound_args)},
            "token": "session-token",
        }
        assert procedure in query
        assert query.endswith(f"RETURN {column}")
        assert parameters == expected_arguments
        if column == "node":
            assert "CALL deposit.utils.toData(node, $token)" in query
            assert "YIELD data AS __deposit_portable_node" in query


def test_get_active_namespace_preserves_legitimate_absence_without_to_data_null():
    driver = _Driver([_Record(data=None)])
    client = Client(driver)
    client.discover = lambda: {"version": "tested"}

    assert client.get_active_namespace("session-token") is None
    query, parameters = driver.calls[-1]
    assert "WHERE node IS NOT NULL" in query
    assert "WHERE node IS NULL RETURN null AS data" in query
    assert "CALL deposit.utils.toData(node, $token)" in query
    assert parameters == {"token": "session-token"}


@pytest.mark.parametrize("method,procedure", [("get_user", "deposit.get.user"), ("get_namespace", "deposit.get.namespace")])
def test_entity_singular_methods_require_an_explicit_selector(method, procedure):
    client = Client(_Driver([]))
    with pytest.raises(TypeError):
        getattr(client, method)("session-token")
    with pytest.raises(ValueError, match="selection must identify"):
        getattr(client, method)("session-token", None)
    with pytest.raises(ValueError, match="selection must identify"):
        getattr(client, method)("session-token", {})
    assert client._driver.calls == []


@pytest.mark.parametrize(
    "method,procedure,column",
    [("get_users", "deposit.get.user", "data"), ("get_namespaces", "deposit.get.namespace", "node")],
)
@pytest.mark.parametrize("rows", [[], [{"id": "one"}], [{"id": "one"}, {"id": "two"}]])
def test_entity_plural_methods_preserve_cardinality_and_detach_dicts(method, procedure, column, rows):
    driver = _Driver([[_Record(**{column: row}) for row in rows]])
    client = Client(driver)
    client.discover = lambda: {"version": "tested"}

    result = getattr(client, method)("session-token")
    assert result == tuple(rows)
    assert isinstance(result, tuple)
    if result:
        assert result[0] is not rows[0]
        result[0]["changed"] = True
        assert "changed" not in rows[0]
    query, parameters = driver.calls[-1]
    assert procedure in query and "RETURN " + column in query
    assert parameters == {"a0": None, "token": "session-token"}


def test_client_manifest_gateway_rejects_actions_and_unreviewed_routes_before_network():
    from deposit._gateway import client_procedure

    client = Client(_Driver([]))
    for procedure in (
        "deposit.create.user",
        "deposit.set.user",
        "deposit.delete.namespace",
        "deposit.import.namespaceData",
        "deposit.admin.pruneResources",
        "deposit.get.info",
        "deposit.get.session",
        "deposit.utils.login",
        "deposit.notReviewed",
    ):
        with pytest.raises(ValueError, match="allowlisted Client procedure"):
            client_procedure(procedure)
        with pytest.raises(ValueError, match="allowlisted Client procedure"):
            client._invoke_client_procedure(procedure, "session-token")
    with pytest.raises(TypeError, match="missing required argument 'pruneId'"):
        client._invoke_client_procedure("deposit.admin.pruneResourcesStatus", "session-token")
    assert client._driver.calls == []


def test_client_prune_resources_allows_only_direct_dry_run():
    from deposit._gateway import prune_dry_run_procedure

    driver = _Driver([_Record(data={"dryRun": True, "resources": []})])
    client = Client(driver)
    client.discover = lambda: {"version": "tested"}

    assert client.prune_resources("session-token", {"dryRun": True, "scope": "all"}) == {
        "dryRun": True,
        "resources": [],
    }
    query, parameters = driver.calls[-1]
    assert "CALL deposit.admin.pruneResources($a0, $token) YIELD data RETURN data" in query
    assert "deposit.action.prepare" not in query
    assert parameters == {"a0": {"dryRun": True, "scope": "all"}, "token": "session-token"}
    assert prune_dry_run_procedure({"dryRun": True})["scope"] == "prune"

    previous_count = len(driver.calls)
    with pytest.raises(DepositError, match="Prune execution is deferred"):
        client.prune_resources("session-token", {"dryRun": False})
    with pytest.raises(DepositError, match="Prune execution is deferred"):
        client.prune_resources("session-token", {})
    assert len(driver.calls) == previous_count


def test_driver_error_mapping_exposes_only_structured_server_errors():
    from deposit._gateway import map_driver_error
    request_id = "9cd08e5a-7ad5-4b0c-9e58-50de7e0412bc"

    safe = _EncodedNeo4jError(
        'DEPOSIT_ERROR {"code":"VALIDATION_FAILED","message":"Invalid option",'
        f'"requestId":"{request_id}","retryable":false,"details":{{"field":"options"}}}}'
    )
    mapped = map_driver_error(safe)
    assert isinstance(mapped, DepositError)
    assert mapped.code == "VALIDATION_FAILED"
    assert mapped.message == "Invalid option"
    assert mapped.request_id == request_id
    assert mapped.details == {"field": "options"}

    raw = _EncodedNeo4jError("Syntax error near private query text")
    transport = map_driver_error(raw)
    assert isinstance(transport, TransportError)
    assert "private query text" not in str(transport)


def test_driver_error_mapping_accepts_only_the_deposit_procedure_wrapper():
    from deposit._gateway import map_driver_error

    request_id = "9cd08e5a-7ad5-4b0c-9e58-50de7e0412bc"
    payload = (
        '{"code":"AUTH_FAILED","details":{},"message":"Invalid credentials",'
        f'"requestId":"{request_id}","retryable":false}}'
    )
    wrapped = _EncodedNeo4jError(
        "Failed to invoke procedure `deposit.utils.login`: Caused by: "
        "deposit.common.ExceptionHandler$DepositException: DEPOSIT_ERROR " + payload
    )
    mapped = map_driver_error(wrapped)
    assert isinstance(mapped, DepositError)
    assert mapped.code == "AUTH_FAILED"
    assert mapped.message == "Invalid credentials"
    assert mapped.request_id == request_id

    rejected = (
        _EncodedNeo4jError(
            "Failed to invoke procedure `deposit.utils.login`: Caused by: "
            "foreign.Exception: DEPOSIT_ERROR " + payload
        ),
        _EncodedNeo4jError(
            "Failed to invoke procedure `deposit.notRegistered`: Caused by: "
            "deposit.common.ExceptionHandler$DepositException: DEPOSIT_ERROR " + payload
        ),
        _EncodedNeo4jError(
            "Driver diagnostic containing DEPOSIT_ERROR " + payload
        ),
        _EncodedNeo4jError(
            "Failed to invoke procedure `deposit.utils.login`: Caused by: "
            "deposit.common.ExceptionHandler$DepositException: DEPOSIT_ERROR "
            + payload + " trailing text"
        ),
        _EncodedNeo4jError(
            "Failed to invoke procedure `deposit.utils.login`: Caused by: "
            "deposit.common.ExceptionHandler$DepositException: DEPOSIT_ERROR " + payload,
            code="Neo.ClientError.Statement.SyntaxError",
        ),
    )
    for error in rejected:
        assert isinstance(map_driver_error(error), TransportError)


def test_driver_error_mapping_requires_bounded_exact_server_envelope():
    from deposit._gateway import map_driver_error

    valid_id = "9cd08e5a-7ad5-4b0c-9e58-50de7e0412bc"
    envelope = (
        '{{"code":"VALIDATION_FAILED","message":"{message}","requestId":"{request_id}",'
        '"retryable":false,"details":{details}}}'
    )
    oversized_details = "{" + '"value":"' + ("x" * 17000) + '"}'
    safe = map_driver_error(_EncodedNeo4jError(
        "DEPOSIT_ERROR " + envelope.format(message="m" * 3000, request_id="invalid", details=oversized_details)
    ))
    assert isinstance(safe, DepositError)
    assert len(safe.message) == 2048 and safe.message.endswith("...")
    assert safe.request_id is None
    assert safe.details is None

    not_envelope = map_driver_error(_EncodedNeo4jError(
        'Driver text {"code":"INJECTED","message":"wrong source"}'
    ))
    invalid_shape = map_driver_error(_EncodedNeo4jError(
        'DEPOSIT_ERROR {"code":"VALIDATION_FAILED","message":"Bad",'
        f'"requestId":"{valid_id}","details":{{}}}}'
    ))
    invalid_code = map_driver_error(_EncodedNeo4jError(
        'DEPOSIT_ERROR {"code":"bad code","message":"Bad",'
        f'"requestId":"{valid_id}","retryable":false,"details":null}}'
    ))
    huge_raw = map_driver_error(_EncodedNeo4jError("DEPOSIT_ERROR " + (" " * (64 * 1024))))
    for value in (not_envelope, invalid_shape, invalid_code, huge_raw):
        assert isinstance(value, TransportError)


def test_operation_inspection_uses_only_narrow_recovery_reads_and_preserves_payloads():
    operation = {"operationId": "operation-1", "state": "committed", "outcome": None, "extra": {"value": 3}}
    last = {"operationId": None, "state": "none", "outcome": None, "scope": None}
    driver = _Driver([_Record(data=operation), _Record(data=operation), _Record(data=last)])
    client = Client(driver)
    client.discover = lambda: {"version": "tested"}

    assert client.get_operation("session-token", "operation-1", scope="server") == operation
    assert client.get_operation("session-token", "operation-1", scope="namespace") == operation
    assert client.get_last_operation("session-token") == last
    assert driver.calls == [
        (
            "CYPHER 25 CALL deposit.get.operation($a0, $a1, $token) YIELD data RETURN data",
            {"a0": "operation-1", "a1": "server", "token": "session-token"},
        ),
        (
            "CYPHER 25 CALL deposit.get.operation($a0, $a1, $token) YIELD data RETURN data",
            {"a0": "operation-1", "a1": "namespace", "token": "session-token"},
        ),
        (
            "CYPHER 25 CALL deposit.get.lastOperation($token) YIELD data RETURN data",
            {"token": "session-token"},
        ),
    ]
    assert all("deposit.action." not in query for query, _ in driver.calls)


def test_operation_inspection_rejects_invalid_scope_before_network_and_propagates_authorization():
    client = Client(_Driver([]))
    client.discover = lambda: {"version": "tested"}
    with pytest.raises(ValueError, match="scope must"):
        client.get_operation("session-token", "operation-1", scope="active")
    with pytest.raises(ValueError, match="operation_id"):
        client.get_operation("session-token", "")
    assert client._driver.calls == []

    denied = _EncodedNeo4jError(
        'DEPOSIT_ERROR {"code":"ACCESS_DENIED","message":"Not authorized",'
        '"requestId":null,"retryable":false,"details":null}'
    )
    denied_client = Client(_Driver([denied]))
    denied_client.discover = lambda: {"version": "tested"}
    with pytest.raises(DepositError) as error:
        denied_client.get_operation("session-token", "operation-1", scope="namespace")
    assert error.value.code == "ACCESS_DENIED"


def test_unsupported_server_fails_before_login_call():
    driver = _Driver([_Record(data={"version": "1.9.0"})])
    with pytest.raises(CompatibilityError):
        Client(driver).login("alice", "secret")
    assert len(driver.calls) == 1


def test_unsupported_neo4j_server_fails_before_login_call():
    version = json.loads((Path(os.environ.get("DEPOSIT_PYTHON_SOURCE_ROOT", Path.cwd())) / "src/deposit/_procedure_manifest.json").read_text())["support"]["target_server_version"]
    driver = _Driver([_Record(data={"version": version}), _Record(version="2026.10.0")])
    with pytest.raises(CompatibilityError) as error:
        Client(driver).login("alice", "secret")
    assert error.value.component == "Neo4j Server"
    assert len(driver.calls) == 2


def test_empty_discovery_result_is_not_reported_as_success():
    with pytest.raises(TransportError):
        Client(_Driver([None])).discover()


def test_owned_driver_closes_with_client():
    driver = _Driver([])
    Client(driver, owns_driver=True).close()
    assert driver.closed


def test_public_recovery_references_hide_internal_keys_and_freeze_results(monkeypatch):
    from deposit._recovery import journal as journal_module

    class MemoryStorage:
        def __init__(self):
            self.records = {}
            self.guard = threading.RLock()
        @contextmanager
        def lock(self, name="journal", *, timeout=0):
            with self.guard:
                yield self
        def write(self, name, value): self.records[name] = bytes(value)
        def read(self, name): return self.records[name]
        def list_records(self): return tuple(sorted(self.records))
        def delete(self, name): self.records.pop(name, None)

    memory = MemoryStorage()
    backing = ProtectedJournal(store=memory)
    identity = DeploymentIdentity(1, "db.example:7687", "neo4j", "db-id", "dbms-id", "actor-id", "ns-id")
    record = backing.create(identity, "deposit.create.class", ["Find", {"secret": "private input"}], 0)
    monkeypatch.setattr(journal_module, "ProtectedJournal", lambda root=None: ProtectedJournal(store=memory))
    client = Client(_Driver([]))

    pending = client.pending_operations(recovery_directory="custom-root")
    assert len(pending) == 1
    assert isinstance(pending[0], PendingOperation)
    assert pending[0].id == record["public_id"]
    assert not hasattr(pending[0], "key") and not hasattr(pending[0], "arguments")
    with pytest.raises((AttributeError, TypeError)):
        pending[0].state = "released"

    backing.update(identity, record["key"], state="prepare_sent")
    assert "result_format" not in record  # Existing v1 pending records remain legacy.
    portable_outcome = {
        "__deposit_gateway_result__": {"ordinary": [1, {"value": "caller data"}]},
        "nested": {"items": [1, 2]},
    }
    backing.update(identity, record["key"], state="outcome_saved", outcome=portable_outcome)
    backing.update(identity, record["key"], state="released")
    persisted = backing.load(identity, record["key"])["outcome"]
    assert persisted == portable_outcome
    assert isinstance(persisted["nested"]["items"], list)
    recovered = client.recovered_operations(recovery_directory="custom-root")
    assert len(recovered) == 1 and isinstance(recovered[0], RecoveredOperation)
    assert recovered[0].result["__deposit_gateway_result__"]["ordinary"][1]["value"] == "caller data"
    assert isinstance(recovered[0].result, Mapping)
    assert recovered[0].result["nested"]["items"] == (1, 2)
    with pytest.raises(TypeError):
        recovered[0].result["nested"]["new"] = True
    with pytest.raises(TypeError):
        recovered[0].result["__deposit_gateway_result__"]["ordinary"][1]["new"] = True
    with pytest.raises(AttributeError):
        recovered[0].result["nested"]["items"].append(3)
    repeated = client.recovered_operations(recovery_directory="custom-root")
    assert repeated == recovered
    assert repeated[0].result["nested"]["items"] == (1, 2)
    assert client.pending_operations(recovery_directory="custom-root") == ()


def test_advanced_recovered_result_has_immutable_columns_and_rows_without_internal_wrapper(monkeypatch):
    from deposit._recovery import journal as journal_module

    class MemoryStorage:
        def __init__(self):
            self.records = {}
            self.guard = threading.RLock()
        @contextmanager
        def lock(self, name="journal", *, timeout=0):
            with self.guard:
                yield self
        def write(self, name, value): self.records[name] = bytes(value)
        def read(self, name): return self.records[name]
        def list_records(self): return tuple(sorted(self.records))
        def delete(self, name): self.records.pop(name, None)

    from deposit._recovery.journal import RESULT_FORMAT_NAMED_ROWS_V1

    memory = MemoryStorage()
    backing = ProtectedJournal(store=memory)
    identity = DeploymentIdentity(1, "db.example:7687", "neo4j", "db-id", "dbms-id", "actor-id", "ns-id")
    record = backing.create(
        identity,
        "deposit.get.query",
        ["RETURN 1", {}, {}],
        7,
        result_format=RESULT_FORMAT_NAMED_ROWS_V1,
    )
    monkeypatch.setattr(journal_module, "ProtectedJournal", lambda root=None: ProtectedJournal(store=memory))
    backing.update(identity, record["key"], state="prepare_sent")
    outcome = {
        "columns": ["data", "presence"],
        "rows": [
            {"data": {"items": [1, {"nested": [True, None]}]}, "presence": True},
            {"data": {"items": []}, "presence": False},
        ],
    }
    backing.update(identity, record["key"], state="outcome_saved", outcome=outcome)
    backing.update(identity, record["key"], state="released")
    client = Client(_Driver([]))

    recovered = client.recovered_operations(recovery_directory="custom-root")
    assert len(recovered) == 1 and isinstance(recovered[0], RecoveredOperation)
    assert recovered[0].result == {
        "columns": ("data", "presence"),
        "rows": (
            {"data": {"items": (1, {"nested": (True, None)})}, "presence": True},
            {"data": {"items": ()}, "presence": False},
        ),
    }
    assert isinstance(recovered[0].result, Mapping)
    with pytest.raises(TypeError):
        recovered[0].result["columns"] = ()
    with pytest.raises(TypeError):
        recovered[0].result["rows"][0]["data"]["items"][1]["nested"][0] = False


def test_client_recovers_completed_namespace_deletion_from_original_receipt(monkeypatch):
    from deposit._recovery import identity as identity_module
    from deposit._recovery import journal as journal_module

    class MemoryStorage:
        def __init__(self):
            self.records = {}
            self.guard = threading.RLock()
        @contextmanager
        def lock(self, name="journal", *, timeout=0):
            with self.guard:
                yield self
        def write(self, name, value): self.records[name] = bytes(value)
        def read(self, name): return self.records[name]
        def list_records(self): return tuple(sorted(self.records))
        def delete(self, name): self.records.pop(name, None)

    memory = MemoryStorage()
    backing = ProtectedJournal(store=memory)
    identity = DeploymentIdentity(1, "db.example:7687", "neo4j", "db-id", "dbms-id", "actor-id", "ns-id")
    record = backing.create(identity, "deposit.delete.namespace", ["old-name"], 4)
    monkeypatch.setattr(journal_module, "ProtectedJournal", lambda root=None: ProtectedJournal(store=memory))
    monkeypatch.setattr(identity_module, "read_deployment_identity", lambda *args, **kwargs: identity)
    client = Client(_Driver([]))
    monkeypatch.setattr(Client, "login", lambda _self, username, password: "fresh-token")
    monkeypatch.setattr(Client, "get_session", lambda _self, token: {"user": {"id": "actor-id"}, "namespaceId": None})
    monkeypatch.setattr(Client, "logout", lambda _self, token: None)
    calls = []

    def call(query, parameters=None):
        calls.append(query)
        if "deposit.action.status" in query:
            return {"data": {"state": "committed", "operationId": "receipt-id", "outcome": {"id": "ns-id", "deleted": True}}}
        if "deposit.action.resume" in query:
            return {"data": {"state": "committed", "operationId": "receipt-id", "outcome": {"id": "ns-id", "deleted": True}}}
        if "deposit.action.release" in query:
            return {"value": True}
        raise AssertionError(f"Deleted Namespace recovery must not issue {query}")

    monkeypatch.setattr(Client, "_single", lambda _self, query, parameters=None: call(query, parameters))
    operation = client.pending_operations(recovery_directory="custom-root")[0]
    recovered = client.recover(operation, username="alice", password="provided-now")

    assert recovered.id == record["public_id"]
    assert recovered.result["deleted"] is True
    assert all("get.namespace" not in query and "set.activeNamespace" not in query for query in calls)
    assert backing.load(identity, record["key"])["state"] == "released"


def test_deleted_namespace_release_retry_returns_original_committed_result(monkeypatch):
    from deposit._recovery import identity as identity_module
    from deposit._recovery import journal as journal_module

    class MemoryStorage:
        def __init__(self):
            self.records = {}
            self.guard = threading.RLock()
        @contextmanager
        def lock(self, name="journal", *, timeout=0):
            with self.guard:
                yield self
        def write(self, name, value): self.records[name] = bytes(value)
        def read(self, name): return self.records[name]
        def list_records(self): return tuple(sorted(self.records))
        def delete(self, name): self.records.pop(name, None)

    memory = MemoryStorage()
    journal = ProtectedJournal(store=memory)
    identity = DeploymentIdentity(1, "db.example:7687", "neo4j", "db-id", "dbms-id", "actor-id", "ns-id")
    original_outcome = {"id": "ns-id", "name": "removed-name", "deleted": True}
    record = journal.create(identity, "deposit.delete.namespace", ["removed-name"], 4)
    monkeypatch.setattr(journal_module, "ProtectedJournal", lambda root=None: ProtectedJournal(store=memory))
    monkeypatch.setattr(identity_module, "read_deployment_identity", lambda *_args, **_kwargs: identity)
    monkeypatch.setattr(Client, "login", lambda _self, *_args: "token")
    monkeypatch.setattr(Client, "get_session", lambda _self, _token: {"user": {"id": "actor-id"}})
    monkeypatch.setattr(Client, "logout", lambda _self, _token: None)
    releases = 0
    calls = []

    def call(_self, query, _parameters=None):
        nonlocal releases
        calls.append(query)
        if "deposit.action.status" in query:
            return {"data": {"state": "committed", "operationId": "receipt-id", "outcome": original_outcome}}
        if "deposit.action.resume" in query:
            return {"data": {"state": "committed", "operationId": "receipt-id", "outcome": original_outcome}}
        if "deposit.action.release" in query:
            releases += 1
            return {"value": False if releases == 1 else True}
        raise AssertionError(f"Unexpected deleted-Namespace recovery call: {query}")

    monkeypatch.setattr(Client, "_single", call)
    client = Client(_Driver([]))
    operation = client.pending_operations(recovery_directory="custom-root")[0]

    with pytest.raises(DepositError) as failure:
        client.recover(operation, username="alice", password="fresh")
    assert failure.value.code == "RELEASE_UNCONFIRMED"
    pending = journal.load(identity, record["key"])
    assert pending["state"] == "release_sent"
    assert pending["outcome"] == original_outcome

    recovered = client.recover(operation, username="alice", password="fresh")
    final = journal.load(identity, record["key"])
    assert recovered.result == original_outcome
    assert final["state"] == "released"
    assert final["outcome"] == original_outcome
    assert sum("deposit.action.status" in query for query in calls) == 2
    assert sum("deposit.action.resume" in query for query in calls) == 2
    assert sum("deposit.action.release" in query for query in calls) == 2


def test_deleted_namespace_not_found_after_lost_release_reply_preserves_saved_outcome(monkeypatch):
    from deposit._recovery import identity as identity_module
    from deposit._recovery import journal as journal_module

    class MemoryStorage:
        def __init__(self):
            self.records = {}
            self.guard = threading.RLock()
        @contextmanager
        def lock(self, name="journal", *, timeout=0):
            with self.guard:
                yield self
        def write(self, name, value): self.records[name] = bytes(value)
        def read(self, name): return self.records[name]
        def list_records(self): return tuple(sorted(self.records))
        def delete(self, name): self.records.pop(name, None)

    memory = MemoryStorage()
    journal = ProtectedJournal(store=memory)
    identity = DeploymentIdentity(1, "db.example:7687", "neo4j", "db-id", "dbms-id", "actor-id", "ns-id")
    original_outcome = {"id": "ns-id", "name": "removed-name", "deleted": True}
    record = journal.create(identity, "deposit.delete.namespace", ["removed-name"], 4)
    monkeypatch.setattr(journal_module, "ProtectedJournal", lambda root=None: ProtectedJournal(store=memory))
    monkeypatch.setattr(identity_module, "read_deployment_identity", lambda *_args, **_kwargs: identity)
    monkeypatch.setattr(Client, "login", lambda _self, *_args: "token")
    monkeypatch.setattr(Client, "get_session", lambda _self, _token: {"user": {"id": "actor-id"}})
    server_released = False
    status_checks = 0

    def call(_self, query, _parameters=None):
        nonlocal server_released, status_checks
        if "deposit.action.status" in query:
            status_checks += 1
            if server_released:
                raise DepositError("NOT_FOUND", "Prepared action no longer exists")
            return {"data": {"state": "committed", "operationId": "receipt-id", "outcome": original_outcome}}
        if "deposit.action.resume" in query:
            return {"data": {"state": "committed", "operationId": "receipt-id", "outcome": original_outcome}}
        if "deposit.action.release" in query:
            server_released = True
            raise TransportError("Release succeeded but its reply was lost")
        raise AssertionError(f"Unexpected deleted-Namespace recovery call: {query}")

    monkeypatch.setattr(Client, "_single", call)
    client = Client(_Driver([]))
    operation = client.pending_operations(recovery_directory="custom-root")[0]

    with pytest.raises(TransportError, match="reply was lost"):
        client.recover(operation, username="alice", password="fresh")
    assert server_released
    pending = journal.load(identity, record["key"])
    assert pending["state"] == "release_sent"
    assert pending["outcome"] == original_outcome

    # An unexpected missing receipt is not proof of release. Preserve the
    # durable release intent and its original result without replaying work.
    fresh_client = Client(_Driver([]))
    with pytest.raises(DepositError) as failure:
        fresh_client.recover(operation, username="alice", password="fresh")
    assert failure.value.code == "RECOVERY_PENDING"
    assert "server release is unconfirmed" in failure.value.message
    assert status_checks == 2
    assert journal.load(identity, record["key"])["state"] == "release_sent"
    assert journal.load(identity, record["key"])["outcome"] == original_outcome


def test_deleted_namespace_lost_release_reply_recovers_saved_result_from_committed_status(monkeypatch):
    from deposit._recovery import identity as identity_module
    from deposit._recovery import journal as journal_module

    class MemoryStorage:
        def __init__(self):
            self.records = {}
            self.guard = threading.RLock()
        @contextmanager
        def lock(self, name="journal", *, timeout=0):
            with self.guard:
                yield self
        def write(self, name, value): self.records[name] = bytes(value)
        def read(self, name): return self.records[name]
        def list_records(self): return tuple(sorted(self.records))
        def delete(self, name): self.records.pop(name, None)

    memory = MemoryStorage()
    journal = ProtectedJournal(store=memory)
    identity = DeploymentIdentity(1, "db.example:7687", "neo4j", "db-id", "dbms-id", "actor-id", "ns-id")
    original_outcome = {"id": "ns-id", "name": "removed-name", "deleted": True}
    record = journal.create(identity, "deposit.delete.namespace", ["removed-name"], 4)
    monkeypatch.setattr(journal_module, "ProtectedJournal", lambda root=None: ProtectedJournal(store=memory))
    monkeypatch.setattr(identity_module, "read_deployment_identity", lambda *_args, **_kwargs: identity)
    monkeypatch.setattr(Client, "login", lambda _self, *_args: "token")
    monkeypatch.setattr(Client, "get_session", lambda _self, _token: {"user": {"id": "actor-id"}})
    server_released = False
    releases = 0
    calls = []

    def call(_self, query, _parameters=None):
        nonlocal server_released, releases
        calls.append(query)
        if "deposit.action.status" in query:
            return {"data": {"state": "committed", "operationId": "receipt-id", "outcome": original_outcome}}
        if "deposit.action.resume" in query:
            return {"data": {"state": "committed", "operationId": "receipt-id", "outcome": original_outcome}}
        if "deposit.action.release" in query:
            releases += 1
            if not server_released:
                server_released = True
                raise TransportError("Release succeeded but its reply was lost")
            return {"value": True}
        raise AssertionError(f"Unexpected deleted-Namespace recovery call: {query}")

    monkeypatch.setattr(Client, "_single", call)
    original_client = Client(_Driver([]))
    operation = original_client.pending_operations(recovery_directory="custom-root")[0]
    with pytest.raises(TransportError, match="reply was lost"):
        original_client.recover(operation, username="alice", password="fresh")
    assert journal.load(identity, record["key"])["state"] == "release_sent"

    recovered = Client(_Driver([])).recover(operation, username="alice", password="fresh")

    assert recovered.id == record["public_id"]
    assert recovered.result == original_outcome
    assert journal.load(identity, record["key"])["state"] == "released"
    assert releases == 2  # the second release confirms detachment; no delete is replayed
    assert sum("deposit.action.status" in query for query in calls) == 2
    assert not any("deposit.delete.namespace" in query for query in calls)
    assert not any("deposit.get.namespace" in query or "deposit.set.activeNamespace" in query for query in calls)


@pytest.mark.parametrize(
    ("saved_state", "saved_outcome", "message_fragment"),
    [
        ("outcome_saved", {"id": "ns-id", "deleted": True}, "server action status is unconfirmed"),
        ("release_sent", None, "Namespace deletion is not confirmed"),
        ("release_sent", "corrupt outcome", "Namespace deletion is not confirmed"),
    ],
)
def test_deleted_namespace_not_found_preserves_journal_without_replay(
    monkeypatch, saved_state, saved_outcome, message_fragment
):
    from deposit._recovery import identity as identity_module
    from deposit._recovery import journal as journal_module

    class MemoryStorage:
        def __init__(self):
            self.records = {}
            self.guard = threading.RLock()
        @contextmanager
        def lock(self, name="journal", *, timeout=0):
            with self.guard:
                yield self
        def write(self, name, value): self.records[name] = bytes(value)
        def read(self, name): return self.records[name]
        def list_records(self): return tuple(sorted(self.records))
        def delete(self, name): self.records.pop(name, None)

    memory = MemoryStorage()
    journal = ProtectedJournal(store=memory)
    identity = DeploymentIdentity(1, "db.example:7687", "neo4j", "db-id", "dbms-id", "actor-id", "ns-id")
    record = journal.create(identity, "deposit.delete.namespace", ["removed-name"], 4)
    if saved_state in {"outcome_saved", "release_sent"}:
        journal.update(identity, record["key"], state="outcome_saved", outcome=saved_outcome)
    if saved_state == "release_sent":
        journal.update(identity, record["key"], state="release_sent")
    original = journal.load(identity, record["key"])
    monkeypatch.setattr(journal_module, "ProtectedJournal", lambda root=None: ProtectedJournal(store=memory))
    monkeypatch.setattr(identity_module, "read_deployment_identity", lambda *_args, **_kwargs: identity)
    monkeypatch.setattr(Client, "login", lambda _self, *_args: "token")
    monkeypatch.setattr(Client, "get_session", lambda _self, _token: {"user": {"id": "actor-id"}})
    monkeypatch.setattr(Client, "logout", lambda _self, _token: None)
    calls = []

    def call(_self, query, _parameters=None):
        calls.append(query)
        if "deposit.action.status" in query:
            raise DepositError("NOT_FOUND", "Prepared action does not exist")
        raise AssertionError(f"Missing/corrupt outcome must not trigger {query}")

    monkeypatch.setattr(Client, "_single", call)
    client = Client(_Driver([]))
    operation = client.pending_operations(recovery_directory="custom-root")[0]

    with pytest.raises(DepositError) as failure:
        client.recover(operation, username="alice", password="fresh")

    assert failure.value.code == "RECOVERY_PENDING"
    assert message_fragment in failure.value.message
    action_calls = [query for query in calls if "deposit.action." in query]
    assert len(action_calls) == 1 and "deposit.action.status" in action_calls[0]
    assert journal.load(identity, record["key"]) == original


def test_recover_after_client_close_fails_before_network_calls(monkeypatch):
    from deposit._recovery import journal as journal_module

    class MemoryStorage:
        def __init__(self):
            self.records = {}
            self.guard = threading.RLock()
        @contextmanager
        def lock(self, name="journal", *, timeout=0):
            with self.guard:
                yield self
        def write(self, name, value): self.records[name] = bytes(value)
        def read(self, name): return self.records[name]
        def list_records(self): return tuple(sorted(self.records))
        def delete(self, name): self.records.pop(name, None)

    memory = MemoryStorage()
    journal = ProtectedJournal(store=memory)
    identity = DeploymentIdentity(1, "db.example:7687", "neo4j", "db-id", "dbms-id", "actor-id", "ns-id")
    journal.create(identity, "deposit.create.class", ["Pending", {}], 1)
    monkeypatch.setattr(journal_module, "ProtectedJournal", lambda root=None: ProtectedJournal(store=memory))
    driver = _Driver([])
    client = Client(driver)
    operation = client.pending_operations(recovery_directory="custom-root")[0]
    client.close()

    with pytest.raises(RuntimeError, match="Client is closed"):
        client.recover(operation, username="alice", password="fresh")

    assert driver.calls == []


@pytest.mark.parametrize("mismatch", ["endpoint", "database", "dbms", "actor", "namespace", "credentials"])
def test_coordinator_identity_or_credential_mismatch_preserves_pending_without_action_calls(monkeypatch, mismatch):
    from deposit._recovery import identity as identity_module
    from deposit._recovery import journal as journal_module

    class MemoryStorage:
        def __init__(self):
            self.records = {}
            self.guard = threading.RLock()
        @contextmanager
        def lock(self, name="journal", *, timeout=0):
            with self.guard:
                yield self
        def write(self, name, value): self.records[name] = bytes(value)
        def read(self, name): return self.records[name]
        def list_records(self): return tuple(sorted(self.records))
        def delete(self, name): self.records.pop(name, None)

    memory = MemoryStorage()
    backing = ProtectedJournal(store=memory)
    identity = DeploymentIdentity(1, "db.example:7687", "neo4j", "db-id", "dbms-id", "actor-id", "ns-id")
    record = backing.create(identity, "deposit.create.class", ["Preserve", {}], 5)
    monkeypatch.setattr(journal_module, "ProtectedJournal", lambda root=None: ProtectedJournal(store=memory))
    driver = _Driver([])
    client = Client(driver)
    operation = client.pending_operations(recovery_directory="custom-root")[0]

    if mismatch == "credentials":
        monkeypatch.setattr(Client, "login", lambda *_args: (_ for _ in ()).throw(DepositError("AUTH_FAILED", "bad credentials")))
        monkeypatch.setattr(identity_module, "read_deployment_identity", lambda *_args, **_kwargs: identity)
        expected = "AUTH_FAILED"
    else:
        monkeypatch.setattr(Client, "login", lambda *_args: "fresh-token")
        monkeypatch.setattr(Client, "get_session", lambda _self, _token: {"user": {"id": "other-actor" if mismatch == "actor" else "actor-id"}})
        changes = {
            "endpoint": {"endpoint": "wrong.example:7687"},
            "database": {"database_name": "otherdb"},
            "dbms": {"dbms_id": "other-dbms"},
        }
        if mismatch in changes:
            other = DeploymentIdentity(**(identity.to_dict() | changes[mismatch]))
            monkeypatch.setattr(identity_module, "read_deployment_identity", lambda *_args, **_kwargs: other)
        elif mismatch == "namespace":
            other = DeploymentIdentity(**(identity.to_dict() | {"namespace_id": "other-ns"}))
            reads = iter((identity, identity, other))
            monkeypatch.setattr(identity_module, "read_deployment_identity", lambda *_args, **_kwargs: next(reads))
        else:
            monkeypatch.setattr(identity_module, "read_deployment_identity", lambda *_args, **_kwargs: identity)
        expected = "IDENTITY_MISMATCH"

    with pytest.raises(DepositError) as failure:
        client.recover(operation, username="alice", password="provided-now")
    assert failure.value.code == expected
    assert not any(
        any(marker in query for marker in ("deposit.action.status", "deposit.action.resume", "deposit.action.prepare", "deposit.action.release", "deposit.create.class"))
        for query, _parameters in driver.calls
    )
    current = backing.load(identity, record["key"])
    assert current["state"] == "intent"
    assert current["key"] == record["key"]
    assert current["arguments"] == ["Preserve", {}]
    assert client.pending_operations(recovery_directory="custom-root")[0] == operation


@pytest.mark.parametrize("database", ["db-b", None], ids=["explicit-target", "driver-default-target"])
def test_recovery_rejects_configured_database_mismatch_before_auth_or_action(monkeypatch, database):
    from types import SimpleNamespace
    from deposit._recovery import journal as journal_module

    class MemoryStorage:
        def __init__(self):
            self.records = {}
            self.guard = threading.RLock()
        @contextmanager
        def lock(self, name="journal", *, timeout=0):
            with self.guard:
                yield self
        def write(self, name, value): self.records[name] = bytes(value)
        def read(self, name): return self.records[name]
        def list_records(self): return tuple(sorted(self.records))
        def delete(self, name): self.records.pop(name, None)

    class Summary:
        server = SimpleNamespace(address=SimpleNamespace(host="db.example", port=7687))
        def __init__(self, database): self.database = database

    class Result(_Result):
        def __init__(self, records, summary=None):
            super().__init__(records)
            self._summary = summary
        def consume(self): return self._summary

    class DatabaseSession:
        def __init__(self, driver, database): self.driver, self.database = driver, database
        def __enter__(self): return self
        def __exit__(self, *args): return None
        def run(self, query, parameters=None):
            self.driver.calls.append((self.database, query))
            if "CALL db.info()" in query:
                name = self.database or "db-b"
                return Result([_Record(id=f"id-{name}", name=name)], Summary(name))
            if "CALL dbms.info()" in query:
                return Result([_Record(id="dbms-id")], Summary("system"))
            raise AssertionError(f"Unexpected query before identity rejection: {query}")

    class DatabaseDriver:
        def __init__(self): self.calls = []
        def session(self, *, database=None): return DatabaseSession(self, database)

    memory = MemoryStorage()
    backing = ProtectedJournal(store=memory)
    journal_identity = DeploymentIdentity(1, "db.example:7687", "db-a", "id-db-a", "dbms-id", "actor-id", "ns-id")
    record = backing.create(journal_identity, "deposit.create.class", ["Preserve", {}], 5)
    monkeypatch.setattr(journal_module, "ProtectedJournal", lambda root=None: ProtectedJournal(store=memory))
    driver = DatabaseDriver()
    client = Client(driver, database=database)
    operation = client.pending_operations(recovery_directory="custom-root")[0]

    with pytest.raises(DepositError) as failure:
        client.recover(operation, username="alice", password="fresh")

    assert failure.value.code == "IDENTITY_MISMATCH"
    assert [(target, query) for target, query in driver.calls] == [
        (database, "CALL db.info() YIELD id, name RETURN id, name"),
        ("system", "CALL dbms.info() YIELD id RETURN id"),
    ]
    assert backing.load(journal_identity, record["key"]) == record


@pytest.mark.skipif(os.name != "nt", reason="DPAPI and Windows byte-range locks are required")
def test_deleted_namespace_recovery_does_not_query_status_while_other_process_holds_action_lock(monkeypatch, tmp_path):
    from deposit._recovery import identity as identity_module
    from deposit._recovery.storage import ProtectedStorageError, ProtectedStore

    coordinator = tmp_path / "coordinator"
    monkeypatch.setattr(ProtectedStore, "_coordinator_root", staticmethod(lambda: coordinator))
    identity = DeploymentIdentity(1, "db.example:7687", "neo4j", "db-id", "dbms-id", "actor-id", "ns-id")
    recovery_root = tmp_path / "recovery"
    journal = ProtectedJournal(recovery_root)
    record = journal.create(identity, "deposit.delete.namespace", ["deleted-name"], 2)
    monkeypatch.setattr(identity_module, "read_deployment_identity", lambda *_args, **_kwargs: identity)
    monkeypatch.setattr(Client, "login", lambda _self, *_args: "token")
    monkeypatch.setattr(Client, "get_session", lambda _self, _token: {"user": {"id": "actor-id"}})
    calls = []
    monkeypatch.setattr(Client, "_single", lambda _self, query, _parameters=None: calls.append(query))
    client = Client(_Driver([]))
    operation = client.pending_operations(recovery_directory=str(recovery_root))[0]

    context = multiprocessing.get_context("spawn")
    ready = context.Event()
    release = context.Event()
    owner = context.Process(
        target=_hold_action_lock_in_child,
        args=(str(recovery_root), record["key"], ready, release),
    )
    owner.start()
    try:
        assert ready.wait(10)
        with pytest.raises(ProtectedStorageError, match="locked by another process"):
            client.recover(operation, username="alice", password="fresh")
    finally:
        release.set()
        owner.join(10)
        if owner.is_alive():
            owner.terminate()
            owner.join(5)

    assert owner.exitcode == 0
    assert not any("deposit.action." in query for query in calls)
    assert journal.load(identity, record["key"]) == record
