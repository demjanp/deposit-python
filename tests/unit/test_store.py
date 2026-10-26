from __future__ import annotations

from contextlib import contextmanager
import json
import threading
import uuid

import pytest

from deposit._recovery.identity import DeploymentIdentity
from deposit._recovery.journal import ProtectedJournal
from deposit._recovery.storage import ProtectedStorageError
from deposit.errors import DepositError, TransportError
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
        if "deposit.utils.refresh" in query:
            self.sessions[parameters["token"]]["revision"] = self.revision
            return {"data": {"scope": "namespace"}}
        if "deposit.utils.cancel" in query:
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
        if "deposit.utils.logout" in query:
            self.logout(parameters["token"])
            return {"value": True}
        raise AssertionError(query)

    @staticmethod
    def _status(action):
        return {key: action[key] for key in ("state", "operationId", "outcome")}


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
