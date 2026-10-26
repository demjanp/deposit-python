"""Installed-package process-death acceptance, run only by the server harness."""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time
import uuid

import pytest
from neo4j import GraphDatabase

from deposit import Client, DepositError
from deposit._recovery.identity import DeploymentIdentity, read_deployment_identity
from deposit._recovery.journal import ProtectedJournal
from deposit._recovery.storage import ProtectedStore


_BARRIERS = (
    "before_prepare", "after_prepare", "before_execute", "after_execute",
    "after_result_persist", "after_release",
)
_EXPECTED_JOURNAL_STATE = {
    "before_prepare": "prepare_sent",
    "after_prepare": "prepare_sent",
    "before_execute": "executing",
    "after_execute": "executing",
    "after_result_persist": "outcome_saved",
    "after_release": "release_sent",
}
_EXPECTED_SERVER_STATE = {
    "before_prepare": None,
    "after_prepare": "prepared",
    "before_execute": "prepared",
    "after_execute": "committed",
    "after_result_persist": "committed",
    "after_release": "committed",
}


def _assert_action_prestate(client, token, journal, identities, barrier):
    raw_identity = identities[barrier]
    identity = raw_identity if isinstance(raw_identity, DeploymentIdentity) else DeploymentIdentity.from_dict(raw_identity)
    records = journal.records(identity)
    assert len(records) == 1
    record = records[0]
    assert record["state"] == _EXPECTED_JOURNAL_STATE[barrier]
    client._single(
        "CYPHER 25 CALL deposit.set.activeNamespace($namespace, $token) YIELD node RETURN node",
        {"namespace": {"id": identity.namespace_id}, "token": token},
    )
    try:
        status = client._recovery_control("deposit.action.status", record["key"], token)
    except DepositError as exc:
        if exc.code != "NOT_FOUND" or _EXPECTED_SERVER_STATE[barrier] is not None:
            raise
        status = None
    assert (status.get("state") if status else None) == _EXPECTED_SERVER_STATE[barrier]
    return record


def _plain(value):
    if isinstance(value, dict) or hasattr(value, "items"):
        return {key: _plain(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_plain(item) for item in value]
    return value


def _prepare_server_scope_namespace(client, journal, identity, token, name, *, commit):
    arguments = [name, {}]
    record = journal.create(identity, "deposit.create.namespace", arguments, 0)
    journal.update(identity, record["key"], state="prepare_sent")
    prepared = client._single(
        "CYPHER 25 CALL deposit.action.prepare($key, $procedure, $arguments, $token) YIELD data RETURN data",
        {"key": record["key"], "procedure": record["procedure"], "arguments": arguments, "token": token},
    )["data"]
    journal.update(identity, record["key"], state="prepared", operation_id=prepared["operationId"])
    _release_prepared_action(client, token)
    if not commit:
        return journal.load(identity, record["key"])

    client._recovery_control("deposit.action.resume", record["key"], token)
    journal.update(identity, record["key"], state="executing")
    outcome = client._single(
        "CYPHER 25 CALL deposit.create.namespace($name, $properties, $token) YIELD node "
        "CALL deposit.utils.toData(node, $token) YIELD data RETURN data",
        {"name": name, "properties": {}, "token": token},
    )["data"]
    journal.update(identity, record["key"], state="outcome_saved", outcome=outcome)
    _release_prepared_action(client, token)
    return journal.update(identity, record["key"], state="released")


def _seed_committed_class_action(client, journal, identity, token, name):
    client._single(
        "CYPHER 25 CALL deposit.set.activeNamespace($namespace, $token) YIELD node RETURN node",
        {"namespace": {"id": identity.namespace_id}, "token": token},
    )
    client._single(
        "CYPHER 25 CALL deposit.utils.refresh($scope, $token) YIELD data RETURN data",
        {"scope": "namespace", "token": token},
    )
    observed = client.get_session(token).get("observed", {}).get(identity.namespace_id)
    assert isinstance(observed, int)
    record = journal.create(identity, "deposit.create.class", [name, {}], observed)
    journal.update(identity, record["key"], state="prepare_sent")
    prepared = client._single(
        "CYPHER 25 CALL deposit.action.prepare($key, $procedure, $arguments, $token) YIELD data RETURN data",
        {"key": record["key"], "procedure": record["procedure"], "arguments": record["arguments"], "token": token},
    )["data"]
    journal.update(identity, record["key"], state="prepared", operation_id=prepared["operationId"])
    _release_prepared_action(client, token)
    client._recovery_control("deposit.action.resume", record["key"], token)
    journal.update(identity, record["key"], state="executing")
    client._single(
        "CYPHER 25 CALL deposit.create.class($name, $properties, $token) YIELD node "
        "CALL deposit.utils.toData(node, $token) YIELD data RETURN data",
        {"name": name, "properties": {}, "token": token},
    )  # Lose the client response after the server commits its durable receipt.
    _release_prepared_action(client, token)
    return journal.load(identity, record["key"])


def _release_prepared_action(client, token):
    return client._single(
        "CYPHER 25 CALL deposit.action.release($token) YIELD value RETURN value",
        {"token": token},
    )


@pytest.mark.integration
def test_prepared_recovery_survives_client_process_termination_and_server_restart():
    uri = os.environ["DEPOSIT_TEST_URI"]
    bolt_password = os.environ["DEPOSIT_TEST_PASSWORD"]
    phase = os.environ["DEPOSIT_IDENTITY_PHASE"]
    identity_path = Path(os.environ["DEPOSIT_IDENTITY_RECORD_PATH"])
    recovery_root = Path(os.environ["DEPOSIT_STAGE2_RECOVERY_ROOT"])
    password_store = ProtectedStore(recovery_root)
    driver = GraphDatabase.driver(uri, auth=("neo4j", bolt_password))
    client = Client(driver)
    store = None
    try:
        if phase == "Seed":
            password = os.environ.get("DEPOSIT_STAGE2_ADMIN_PASSWORD")
            if not password:
                raise AssertionError("Stage 1 Smoke must provide the fresh-runtime test account")
            password_store.write("stage2-admin-password", password.encode("utf-8"))
            token = client.login("stage1-admin", password)
            # Verify an already-open Store follows its immutable UUID through rename.
            rename_name = "stage2-rename-" + uuid.uuid4().hex[:8]
            rename_namespace = client._single(
                "CYPHER 25 CALL deposit.create.namespace($name, $properties, $token) YIELD node RETURN node",
                {"name": rename_name, "properties": {}, "token": token},
            )["node"]
            rename_store = client.open_store(rename_name, username="stage1-admin", password=password,
                                             recovery_directory=str(recovery_root))
            assert rename_store.namespace_id == rename_namespace["id"]
            renamed_class = rename_store.create_class("Stage2-Rename-bound")
            renamed_name = rename_name + "-renamed"
            client._single(
                "CYPHER 25 CALL deposit.set.activeNamespace($namespace, $token) YIELD node RETURN node",
                {"namespace": {"id": rename_namespace["id"]}, "token": token},
            )
            client._single(
                "CYPHER 25 CALL deposit.set.namespace($patch, $token) YIELD node RETURN node",
                {"patch": {"name": renamed_name}, "token": token},
            )
            assert rename_store.get_class({"id": renamed_class["id"]}) == renamed_class
            assert renamed_class["namespaceId"] == rename_namespace["id"]
            rename_store.close()
            with driver.session(database="neo4j") as session:
                assert session.run("RETURN 1").single(strict=True)[0] == 1
            client._single(
                "CYPHER 25 CALL deposit.delete.namespace($name, $token) YIELD data RETURN data",
                {"name": renamed_name, "token": token},
            )

            namespaces = {"rename": {"id": rename_namespace["id"], "name": renamed_name}}
            for barrier in _BARRIERS:
                namespace_name = f"stage2-{barrier}-" + uuid.uuid4().hex[:8]
                namespace_row = client._single(
                    "CYPHER 25 CALL deposit.create.namespace($name, $properties, $token) YIELD node RETURN node",
                    {"name": namespace_name, "properties": {}, "token": token},
                )
                namespaces[barrier] = {"id": namespace_row["node"]["id"], "name": namespace_name}
            actor_id = client.get_session(token)["user"]["id"]
            identities = {
                barrier: read_deployment_identity(
                    driver, database="neo4j", actor_id=actor_id, namespace_id=namespaces[barrier]["id"]
                ).to_dict()
                for barrier in _BARRIERS
            }
            identity_path.parent.mkdir(parents=True, exist_ok=True)
            (identity_path.parent / "stage2-namespaces.json").write_text(
                json.dumps(namespaces, sort_keys=True), encoding="utf-8"
            )
            journal = ProtectedJournal(recovery_root)
            seeded = []
            for barrier in _BARRIERS:
                child_env = dict(os.environ)
                child_env["DEPOSIT_STAGE2_BARRIER"] = barrier
                child_env["DEPOSIT_STAGE2_NAMESPACE_ID"] = namespaces[barrier]["id"]
                child_env["DEPOSIT_STAGE2_CLASS_NAME"] = "Stage2-" + barrier
                result = subprocess.run(
                    [sys.executable, "-c", _PROCESS_BARRIER_SCRIPT],
                    capture_output=True,
                    text=True,
                    timeout=180,
                    env=child_env,
                    check=False,
                )
                if result.returncode != 73:
                    raise AssertionError(
                        f"Python client did not terminate at {barrier}; exit={result.returncode}; "
                        f"stdout={result.stdout[-2000:]}; stderr={result.stderr[-2000:]}"
                    )
                seeded.append(barrier)
                # A later Store may auto-recover only its bound Namespace. Prove
                # all earlier records remain pending on both client and server.
                for prior in seeded:
                    _assert_action_prestate(client, token, journal, identities, prior)
            pending_records = journal.records_all()
            pending_records = [
                record for record in pending_records
                if record["identity"]["namespace_id"] in {value["namespace_id"] for value in identities.values()}
            ]
            assert len(pending_records) == len(_BARRIERS)
            assert all(record["state"] != "released" for record in pending_records)
            (identity_path.parent / "stage2-identities.json").write_text(
                json.dumps(identities, sort_keys=True), encoding="utf-8"
            )
            server_anchor_name = "stage2-server-scope-anchor-" + uuid.uuid4().hex[:8]
            server_anchor = client._single(
                "CYPHER 25 CALL deposit.create.namespace($name, $properties, $token) YIELD node "
                "CALL deposit.utils.toData(node, $token) YIELD data RETURN data",
                {"name": server_anchor_name, "properties": {}, "token": token},
            )["data"]
            server_identity = read_deployment_identity(
                driver, database="neo4j", actor_id=actor_id, namespace_id=server_anchor["id"]
            )
            committed_name = "stage2-server-committed-" + uuid.uuid4().hex[:8]
            pending_name = "stage2-server-pending-" + uuid.uuid4().hex[:8]
            committed_record = _prepare_server_scope_namespace(
                client, journal, server_identity, token, committed_name, commit=True
            )
            pending_server_record = _prepare_server_scope_namespace(
                client, journal, server_identity, token, pending_name, commit=False
            )
            assert committed_record["state"] == "released"
            assert pending_server_record["state"] == "prepared"
            password_store.write("stage2-old-token", token.encode("utf-8"))
            (identity_path.parent / "stage2-server-scope.json").write_text(
                json.dumps({
                    "identity": server_identity.to_dict(),
                    "anchor_name": server_anchor_name,
                    "anchor_id": server_anchor["id"],
                    "committed_name": committed_name,
                    "pending_name": pending_name,
                }, sort_keys=True), encoding="utf-8"
            )
            return

        identity = DeploymentIdentity.from_dict(json.loads(identity_path.read_text(encoding="utf-8")))
        namespaces = json.loads((identity_path.parent / "stage2-namespaces.json").read_text(encoding="utf-8"))
        identities = {
            barrier: DeploymentIdentity.from_dict(value)
            for barrier, value in json.loads((identity_path.parent / "stage2-identities.json").read_text(encoding="utf-8")).items()
        }
        password = password_store.read("stage2-admin-password").decode("utf-8")
        current = read_deployment_identity(
            driver,
            database=identity.database_name,
            actor_id=identity.actor_id,
            namespace_id=identity.namespace_id,
        )
        assert current == identity
        outcomes = []
        journal = ProtectedJournal(recovery_root)
        recovery_token = client.login("stage1-admin", password)
        server_scope = json.loads((identity_path.parent / "stage2-server-scope.json").read_text(encoding="utf-8"))
        server_identity = DeploymentIdentity.from_dict(server_scope["identity"])
        assert read_deployment_identity(
            driver,
            database=server_identity.database_name,
            actor_id=server_identity.actor_id,
            namespace_id=server_identity.namespace_id,
        ) == server_identity
        old_token = password_store.read("stage2-old-token").decode("utf-8")
        with pytest.raises(DepositError) as old_token_failure:
            client.get_session(old_token)
        assert old_token_failure.value.code == "AUTH_FAILED"

        server_records = journal.records(server_identity)
        committed_server_record = next(
            record for record in server_records
            if record["procedure"] == "deposit.create.namespace" and record["arguments"][0] == server_scope["committed_name"]
        )
        pending_server_record = next(
            record for record in server_records
            if record["procedure"] == "deposit.create.namespace" and record["arguments"][0] == server_scope["pending_name"]
        )
        assert committed_server_record["state"] == "released"
        committed_server_status = client._recovery_control(
            "deposit.action.status", committed_server_record["key"], recovery_token
        )
        assert committed_server_status["state"] == "committed"
        assert _plain(committed_server_status["outcome"]) == committed_server_record["outcome"]
        committed_reference = next(
            operation for operation in client.recovered_operations(recovery_directory=str(recovery_root))
            if operation.id == committed_server_record["public_id"]
        )
        assert _plain(committed_reference.result) == committed_server_record["outcome"]

        assert pending_server_record["state"] == "prepared"
        pending_reference = next(
            operation for operation in client.pending_operations(recovery_directory=str(recovery_root))
            if operation.id == pending_server_record["public_id"]
        )
        assert client._recovery_control(
            "deposit.action.status", pending_server_record["key"], recovery_token
        )["state"] == "prepared"
        with driver.session(database="neo4j") as session:
            before_count = session.run(
                "MATCH (n:Namespace {name:$name}) RETURN count(n) AS count",
                {"name": server_scope["pending_name"]},
            ).single(strict=True)["count"]
        assert before_count == 0
        client._recovery_control("deposit.action.resume", pending_server_record["key"], recovery_token)
        with pytest.raises(DepositError) as epoch_failure:
            client._single(
                "CYPHER 25 CALL deposit.create.namespace($name, $properties, $token) YIELD node "
                "CALL deposit.utils.toData(node, $token) YIELD data RETURN data",
                {"name": server_scope["pending_name"], "properties": {}, "token": recovery_token},
            )
        assert epoch_failure.value.code == "REVISION_CONFLICT"
        _release_prepared_action(client, recovery_token)
        still_pending = journal.load(server_identity, pending_server_record["key"])
        assert still_pending["state"] == "prepared"
        assert still_pending["key"] == pending_server_record["key"]
        assert any(
            operation.id == pending_reference.id
            for operation in client.pending_operations(recovery_directory=str(recovery_root))
        )
        assert client._recovery_control(
            "deposit.action.status", pending_server_record["key"], recovery_token
        )["state"] == "prepared"
        with driver.session(database="neo4j") as session:
            after_count = session.run(
                "MATCH (n:Namespace {name:$name}) RETURN count(n) AS count",
                {"name": server_scope["pending_name"]},
            ).single(strict=True)["count"]
        assert after_count == 0

        pending_records = ProtectedJournal(recovery_root).records_all()
        pending_records = [
            record for record in pending_records
            if record["identity"]["namespace_id"] in {value.namespace_id for value in identities.values()}
        ]
        assert len(pending_records) == len(_BARRIERS)
        assert all(record["state"] != "released" for record in pending_records)
        processed = set()
        # Two independent Python processes recover the same committed receipt
        # from the same protected journal at once. Both enumerate the handle
        # before the go-file is created so the race targets one exact record.
        race_barrier = "after_execute"
        race_identity = identities[race_barrier]
        race_record = journal.records(race_identity)[0]
        assert race_record["state"] == "executing"
        race_root = identity_path.parent / "same-action-process-race"
        race_root.mkdir(parents=True, exist_ok=True)
        race_go = race_root / "go"
        race_processes = []
        for index in range(2):
            child_env = dict(os.environ)
            child_env["DEPOSIT_STAGE2_RACE_PUBLIC_ID"] = race_record["public_id"]
            child_env["DEPOSIT_STAGE2_ADMIN_PASSWORD"] = password
            child_env["DEPOSIT_STAGE2_RACE_READY"] = str(race_root / f"ready-{index}")
            child_env["DEPOSIT_STAGE2_RACE_GO"] = str(race_go)
            child_env["DEPOSIT_STAGE2_RACE_OUTPUT"] = str(race_root / f"result-{index}.json")
            race_processes.append(subprocess.Popen(
                [sys.executable, "-c", _MULTIPROCESS_RECOVERY_SCRIPT],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                env=child_env,
            ))
        ready_files = [race_root / f"ready-{index}" for index in range(2)]
        ready_deadline = time.monotonic() + 180
        while not all(path.exists() for path in ready_files):
            exited = [process for process in race_processes if process.poll() is not None]
            if exited or time.monotonic() >= ready_deadline:
                diagnostics = [process.communicate(timeout=5) for process in exited]
                raise AssertionError(f"Recovery race children failed to become ready: {diagnostics}")
            time.sleep(0.02)
        race_go.write_text("go", encoding="utf-8")
        race_results = []
        for process in race_processes:
            stdout, stderr = process.communicate(timeout=180)
            if process.returncode != 0:
                raise AssertionError(f"Recovery race child failed: {stdout[-2000:]} {stderr[-2000:]}")
        race_results = [
            json.loads((race_root / f"result-{index}.json").read_text(encoding="utf-8"))
            for index in range(2)
        ]
        successes = [result for result in race_results if result["status"] == "recovered"]
        assert successes, race_results
        assert all(
            result["status"] == "recovered" or "lock" in result.get("message", "").lower()
            for result in race_results
        ), race_results
        raced_record = journal.load(race_identity, race_record["key"])
        assert raced_record["state"] == "released"
        assert all(result.get("id") == raced_record["public_id"] for result in successes)
        assert all(_plain(result["result"]) == raced_record["outcome"] for result in successes)
        with driver.session(database="neo4j") as session:
            assert session.run(
                "MATCH (n:Class {name:$name}) RETURN count(n) AS count",
                {"name": "Stage2-" + race_barrier},
            ).single(strict=True)["count"] == 1
        outcomes.append(raced_record)
        processed.add(race_barrier)
        for pending_barrier in _BARRIERS:
            if pending_barrier not in processed:
                _assert_action_prestate(client, recovery_token, journal, identities, pending_barrier)
        for barrier in _BARRIERS:
            if barrier in processed:
                continue
            action_identity = identities[barrier]
            current = read_deployment_identity(
                driver,
                database=action_identity.database_name,
                actor_id=action_identity.actor_id,
                namespace_id=action_identity.namespace_id,
            )
            assert current == action_identity
            _assert_action_prestate(client, recovery_token, journal, identities, barrier)

            # Inspect the original journal/server pair before open_store triggers
            # automatic recovery of this specific Namespace.
            barrier_store = client.open_store(
                {"id": action_identity.namespace_id},
                username="stage1-admin",
                password=password,
                recovery_directory=str(recovery_root),
            )
            entity = barrier_store.get_class("Stage2-" + barrier)
            assert entity["namespaceId"] == action_identity.namespace_id
            record = next(item for item in journal.records(action_identity) if item["procedure"] == "deposit.create.class")
            assert record["state"] == "released"
            assert record["outcome"]["id"] == entity["id"]
            assert record["outcome"]["namespaceId"] == entity["namespaceId"]
            saved = next(
                item for item in client.recovered_operations(recovery_directory=str(recovery_root))
                if item.id == record["public_id"]
            )
            assert _plain(saved.result) == record["outcome"]
            outcomes.append(record)
            barrier_store.close()
            processed.add(barrier)
            for pending_barrier in _BARRIERS:
                if pending_barrier not in processed:
                    _assert_action_prestate(client, recovery_token, journal, identities, pending_barrier)
        assert len(outcomes) == 6
        assert all(record["state"] == "released" and isinstance(record["outcome"], dict) for record in outcomes)
        assert len({record["outcome"]["id"] for record in outcomes}) == 6
        token = recovery_token
        # A saved portable result remains inspectable after the live entity is
        # later deleted from its Namespace.
        first_identity = identities[_BARRIERS[0]]
        first_namespace = namespaces[_BARRIERS[0]]
        first_result_record = next(
            record for record in outcomes
            if record["identity"]["namespace_id"] == first_identity.namespace_id
        )
        client._single(
            "CYPHER 25 CALL deposit.set.activeNamespace($namespace, $token) YIELD node RETURN node",
            {"namespace": {"id": first_namespace["id"]}, "token": token},
        )
        client._single(
            "CYPHER 25 CALL deposit.utils.refresh($scope, $token) YIELD data RETURN data",
            {"scope": "namespace", "token": token},
        )
        client._single(
            "CYPHER 25 CALL deposit.delete.class($class, $token) YIELD data RETURN data",
            {"class": {"id": first_result_record["outcome"]["id"]}, "token": token},
        )
        saved = next(
            operation for operation in client.recovered_operations(recovery_directory=str(recovery_root))
            if operation.id == first_result_record["public_id"]
        )
        assert _plain(saved.result) == first_result_record["outcome"]
        assert saved.namespace_id == first_identity.namespace_id
        for barrier in _BARRIERS:
            namespace_record = namespaces[barrier]
            client._single(
                "CYPHER 25 CALL deposit.set.activeNamespace($namespace, $token) YIELD node RETURN node",
                {"namespace": {"id": namespace_record["id"]}, "token": token},
            )
            client._single(
                "CYPHER 25 CALL deposit.utils.refresh($scope, $token) YIELD data RETURN data",
                {"scope": "namespace", "token": token},
            )
            client._single(
                "CYPHER 25 CALL deposit.delete.namespace($name, $token) YIELD data RETURN data",
                {"name": namespace_record["name"], "token": token},
            )
        client.logout(token)
        password_store.delete("stage2-old-token")
    finally:
        if store is not None:
            store.close()
        client.close()
        driver.close()


@pytest.mark.integration
def test_live_authority_changes_preserve_committed_recovery():
    if os.environ["DEPOSIT_IDENTITY_PHASE"] != "Verify":
        pytest.skip("Live authorization recovery checks run after the owner restart")
    uri = os.environ["DEPOSIT_TEST_URI"]
    bolt_password = os.environ["DEPOSIT_TEST_PASSWORD"]
    recovery_root = Path(os.environ["DEPOSIT_STAGE2_RECOVERY_ROOT"])
    password_store = ProtectedStore(recovery_root)
    admin_password = password_store.read("stage2-admin-password").decode("utf-8")
    driver = GraphDatabase.driver(uri, auth=("neo4j", bolt_password))
    client = Client(driver)
    user_name = "stage2-recovery-" + uuid.uuid4().hex[:8]
    user_password = "stage2-user-initial-password"
    changed_password = "stage2-user-changed-password"
    user_token = None
    admin_token = None
    try:
        admin_token = client.login("stage1-admin", admin_password)
        user = client._single(
            "CYPHER 25 CALL deposit.create.user($username, $password, $properties, $token) YIELD data RETURN data",
            {"username": user_name, "password": user_password, "properties": {"serverRole": "user"}, "token": admin_token},
        )["data"]
        namespace_name = "stage2-recovery-ns-" + uuid.uuid4().hex[:8]
        namespace = client._single(
            "CYPHER 25 CALL deposit.create.namespace($name, $properties, $token) YIELD node "
            "CALL deposit.utils.toData(node, $token) YIELD data RETURN data",
            {"name": namespace_name, "properties": {}, "token": admin_token},
        )["data"]

        def admin_set_namespace_role(role):
            nonlocal admin_token
            if admin_token:
                client.logout(admin_token)
            admin_token = client.login("stage1-admin", admin_password)
            client._single(
                "CYPHER 25 CALL deposit.set.activeNamespace($namespace, $token) YIELD node RETURN node",
                {"namespace": {"id": namespace["id"]}, "token": admin_token},
            )
            client._single(
                "CYPHER 25 CALL deposit.utils.refresh($scope, $token) YIELD data RETURN data",
                {"scope": "namespace", "token": admin_token},
            )
            client._single(
                "CYPHER 25 CALL deposit.set.namespaceRole($user, $role, $token) YIELD data RETURN data",
                {"user": {"id": user["id"]}, "role": role, "token": admin_token},
            )

        def admin_set_user(**patch):
            nonlocal admin_token
            if admin_token:
                client.logout(admin_token)
            admin_token = client.login("stage1-admin", admin_password)
            client._single(
                "CYPHER 25 CALL deposit.set.user($user, $patch, $token) YIELD data RETURN data",
                {"user": {"id": user["id"]}, "patch": patch, "token": admin_token},
            )

        admin_set_namespace_role("editor")
        client.logout(admin_token)
        admin_token = None
        identity = read_deployment_identity(
            driver, database="neo4j", actor_id=user["id"], namespace_id=namespace["id"]
        )
        journal = ProtectedJournal(recovery_root)

        def seed_action(name):
            nonlocal user_token
            if user_token:
                client.logout(user_token)
            user_token = client.login(user_name, user_password)
            record = _seed_committed_class_action(client, journal, identity, user_token, name)
            client.logout(user_token)
            user_token = None
            operation = next(
                item for item in client.pending_operations(recovery_directory=str(recovery_root))
                if item.id == record["public_id"]
            )
            return record, operation

        def expect_pending(record, operation, username, password, expected_code):
            with pytest.raises(DepositError) as failure:
                client.recover(operation, username=username, password=password)
            assert failure.value.code == expected_code
            assert journal.load(identity, record["key"])["state"] == "executing"
            assert any(
                item.id == operation.id
                for item in client.pending_operations(recovery_directory=str(recovery_root))
            )

        role_record, role_operation = seed_action("authority-role-committed")
        # A valid credential for a different actor must not authorize recovery
        # of this operation. Capture the recovery control path and prove it
        # rejects the actor mismatch before consulting or changing the prepared
        # action on the server.
        action_queries = []
        original_single = client._single

        def capture_action_queries(query, *args, **kwargs):
            action_queries.append(query)
            return original_single(query, *args, **kwargs)

        client._single = capture_action_queries
        try:
            with pytest.raises(DepositError) as wrong_actor_failure:
                client.recover(role_operation, username="stage1-admin", password=admin_password)
            assert wrong_actor_failure.value.code == "IDENTITY_MISMATCH"
        finally:
            client._single = original_single
        assert not any(
            marker in query
            for query in action_queries
            for marker in (
                "deposit.action.status", "deposit.action.resume", "deposit.action.prepare",
                "deposit.action.release", "deposit.create.class",
            )
        )
        assert journal.load(identity, role_record["key"])["state"] == "executing"
        assert any(
            item.id == role_operation.id
            for item in client.pending_operations(recovery_directory=str(recovery_root))
        )
        admin_set_namespace_role("viewer")
        expect_pending(role_record, role_operation, user_name, user_password, "RECOVERY_PENDING")
        admin_set_namespace_role("editor")
        role_result = client.recover(role_operation, username=user_name, password=user_password)
        assert role_result.result["name"] == "authority-role-committed"
        assert journal.load(identity, role_record["key"])["state"] == "released"

        disabled_record, disabled_operation = seed_action("authority-disabled-committed")
        disabled_token = client.login(user_name, user_password)
        client.logout(disabled_token)
        admin_set_user(enabled=False)
        expect_pending(disabled_record, disabled_operation, user_name, user_password, "AUTH_FAILED")
        with pytest.raises(DepositError) as disabled_token_failure:
            client.get_session(disabled_token)
        assert disabled_token_failure.value.code == "AUTH_FAILED"
        admin_set_user(enabled=True)
        disabled_result = client.recover(disabled_operation, username=user_name, password=user_password)
        assert disabled_result.result["name"] == "authority-disabled-committed"
        assert journal.load(identity, disabled_record["key"])["state"] == "released"

        password_record, password_operation = seed_action("authority-password-committed")
        stale_token = client.login(user_name, user_password)
        client.logout(stale_token)
        admin_set_user(newPassword=changed_password)
        expect_pending(password_record, password_operation, user_name, user_password, "AUTH_FAILED")
        with pytest.raises(DepositError) as revoked_token_failure:
            client.get_session(stale_token)
        assert revoked_token_failure.value.code == "AUTH_FAILED"
        password_result = client.recover(password_operation, username=user_name, password=changed_password)
        assert password_result.result["name"] == "authority-password-committed"
        assert journal.load(identity, password_record["key"])["state"] == "released"

        admin_token = client.login("stage1-admin", admin_password)
        client._single(
            "CYPHER 25 CALL deposit.set.activeNamespace($namespace, $token) YIELD node RETURN node",
            {"namespace": {"id": namespace["id"]}, "token": admin_token},
        )
        client._single(
            "CYPHER 25 CALL deposit.utils.refresh($scope, $token) YIELD data RETURN data",
            {"scope": "namespace", "token": admin_token},
        )
        client._single(
            "CYPHER 25 CALL deposit.delete.namespace($name, $token) YIELD data RETURN data",
            {"name": namespace_name, "token": admin_token},
        )
    finally:
        if user_token:
            try:
                client.logout(user_token)
            except Exception:
                pass
        if admin_token:
            try:
                client.logout(admin_token)
            except Exception:
                pass
        client.close()
        driver.close()


@pytest.mark.integration
def test_live_close_cancels_after_server_commit_without_deadlock_or_duplicate_effect():
    if os.environ["DEPOSIT_IDENTITY_PHASE"] != "Verify":
        pytest.skip("Live cancellation recovery checks run after the owner restart")
    uri = os.environ["DEPOSIT_TEST_URI"]
    bolt_password = os.environ["DEPOSIT_TEST_PASSWORD"]
    recovery_root = Path(os.environ["DEPOSIT_STAGE2_RECOVERY_ROOT"])
    password_store = ProtectedStore(recovery_root)
    admin_password = password_store.read("stage2-admin-password").decode("utf-8")
    driver = GraphDatabase.driver(uri, auth=("neo4j", bolt_password))
    client = Client(driver)
    admin_token = None
    store = None
    worker = None
    closer = None
    release_reply = threading.Event()
    server_committed = threading.Event()
    cancel_completed = threading.Event()
    operation_result = []
    operation_failure = []
    original_single = client._single
    namespace_name = "stage2-cancel-race-ns-" + uuid.uuid4().hex[:8]
    class_name = "stage2-cancel-race-class-" + uuid.uuid4().hex[:8]
    try:
        admin_token = client.login("stage1-admin", admin_password)
        namespace = client._single(
            "CYPHER 25 CALL deposit.create.namespace($name, $properties, $token) YIELD node RETURN node",
            {"name": namespace_name, "properties": {}, "token": admin_token},
        )["node"]
        store = client.open_store(
            {"id": namespace["id"]}, username="stage1-admin", password=admin_password,
            recovery_directory=str(recovery_root),
        )

        def pause_domain_reply(query, parameters=None):
            row = original_single(query, parameters)
            if "deposit.create.class" in query and parameters.get("a0") == class_name:
                # The real server transaction and portable reply have completed;
                # hold the reply before Store can persist it or return to caller.
                with driver.session(database="neo4j") as session:
                    committed_count = session.run(
                        "MATCH (n:Class {name:$name}) RETURN count(n) AS count",
                        {"name": class_name},
                    ).single(strict=True)["count"]
                assert committed_count == 1
                server_committed.set()
                if not release_reply.wait(30):
                    raise AssertionError("Timed out waiting to release the committed server reply")
            if "deposit.utils.cancel" in query:
                cancel_completed.set()
            return row

        client._single = pause_domain_reply

        def create_in_background():
            try:
                operation_result.append(store.create_class(class_name))
            except Exception as exc:
                operation_failure.append(exc)

        worker = threading.Thread(target=create_in_background)
        worker.start()
        assert server_committed.wait(30), f"Create never reached the committed server reply barrier: {operation_failure}"
        closer = threading.Thread(target=store.close)
        closer.start()
        assert cancel_completed.wait(30), "Store.close did not issue cancellation on its parallel control path"
        release_reply.set()
        worker.join(30)
        closer.join(30)
        assert not worker.is_alive() and not closer.is_alive(), "Close/cancel deadlocked around the committed reply"
        assert not operation_failure, operation_failure
        assert len(operation_result) == 1
        client._single = original_single

        records = ProtectedJournal(recovery_root).records_all()
        record = next(
            item for item in records
            if item["procedure"] == "deposit.create.class" and item["arguments"][0] == class_name
        )
        assert record["state"] == "released"
        assert operation_result[0] == record["outcome"]
        with driver.session(database="neo4j") as session:
            count = session.run(
                "MATCH (n:Class {name:$name}) RETURN count(n) AS count", {"name": class_name}
            ).single(strict=True)["count"]
        assert count == 1
        saved = next(
            item for item in client.recovered_operations(recovery_directory=str(recovery_root))
            if item.id == record["public_id"]
        )
        assert _plain(saved.result) == record["outcome"]
    finally:
        release_reply.set()
        client._single = original_single
        if worker is not None:
            worker.join(30)
        if closer is not None:
            closer.join(30)
        if store is not None and not store.closed:
            try:
                store.close()
            except Exception:
                pass
        if admin_token:
            try:
                client._single(
                    "CYPHER 25 CALL deposit.set.activeNamespace($namespace, $token) YIELD node RETURN node",
                    {"namespace": {"id": namespace["id"]}, "token": admin_token},
                )
                client._single(
                    "CYPHER 25 CALL deposit.utils.refresh($scope, $token) YIELD data RETURN data",
                    {"scope": "namespace", "token": admin_token},
                )
                client._single(
                    "CYPHER 25 CALL deposit.delete.namespace($name, $token) YIELD data RETURN data",
                    {"name": namespace_name, "token": admin_token},
                )
                client.logout(admin_token)
            except Exception:
                pass
        client.close()
        driver.close()
        password_store.delete("stage2-admin-password")


_MULTIPROCESS_RECOVERY_SCRIPT = r'''
import json
import os
from pathlib import Path
import time

from neo4j import GraphDatabase
from deposit import Client

def plain(value):
    if hasattr(value, "items"):
        return {key: plain(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [plain(item) for item in value]
    return value

driver = GraphDatabase.driver(os.environ["DEPOSIT_TEST_URI"], auth=("neo4j", os.environ["DEPOSIT_TEST_PASSWORD"]))
client = Client(driver)
deadline = time.monotonic() + 60
while True:
    try:
        operation = next(
            item for item in client.pending_operations(recovery_directory=os.environ["DEPOSIT_STAGE2_RECOVERY_ROOT"])
            if item.id == os.environ["DEPOSIT_STAGE2_RACE_PUBLIC_ID"]
        )
        break
    except Exception as exc:
        if "locked by another process" not in str(exc).lower() or time.monotonic() >= deadline:
            raise
        time.sleep(0.02)
Path(os.environ["DEPOSIT_STAGE2_RACE_READY"]).write_text("ready", encoding="utf-8")
while not Path(os.environ["DEPOSIT_STAGE2_RACE_GO"]).exists():
    time.sleep(0.01)
try:
    result = client.recover(
        operation,
        username="stage1-admin",
        password=os.environ["DEPOSIT_STAGE2_ADMIN_PASSWORD"],
    )
    payload = {"status": "recovered", "id": result.id, "result": plain(result.result)}
except Exception as exc:
    payload = {"status": "error", "code": getattr(exc, "code", None), "message": str(exc)}
finally:
    driver.close()
Path(os.environ["DEPOSIT_STAGE2_RACE_OUTPUT"]).write_text(json.dumps(payload), encoding="utf-8")
'''


_PROCESS_BARRIER_SCRIPT = r'''
import os
from neo4j import GraphDatabase
from deposit import Client

driver = GraphDatabase.driver(os.environ["DEPOSIT_TEST_URI"], auth=("neo4j", os.environ["DEPOSIT_TEST_PASSWORD"]))
client = Client(driver)
store = client.open_store(
    {"id": os.environ["DEPOSIT_STAGE2_NAMESPACE_ID"]},
    username="stage1-admin",
    password=os.environ["DEPOSIT_STAGE2_ADMIN_PASSWORD"],
    recovery_directory=os.environ["DEPOSIT_STAGE2_RECOVERY_ROOT"],
)
barrier = os.environ["DEPOSIT_STAGE2_BARRIER"]
original_single = client._single

def intercept(query, parameters=None):
    if barrier == "before_prepare" and "deposit.action.status" in query:
        os._exit(73)
    if barrier == "before_execute" and "deposit.create.class" in query:
        os._exit(73)
    row = original_single(query, parameters)
    if barrier == "after_prepare" and "deposit.action.prepare" in query:
        os._exit(73)
    if barrier == "after_execute" and "deposit.create.class" in query:
        os._exit(73)
    if barrier == "after_release" and "deposit.action.release" in query:
        os._exit(73)
    return row

client._single = intercept
if barrier == "after_result_persist":
    original_update = store._journal.update
    def persist_then_exit(identity, key, **changes):
        result = original_update(identity, key, **changes)
        if changes.get("state") == "outcome_saved":
            os._exit(73)
        return result
    store._journal.update = persist_then_exit
store.create_class(os.environ["DEPOSIT_STAGE2_CLASS_NAME"])
os._exit(0)
'''
