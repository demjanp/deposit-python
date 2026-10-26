"""Installed-package gateway acceptance through the existing server harness."""

from __future__ import annotations

import json
import os
from pathlib import Path
import secrets
import uuid

import pytest
from neo4j import GraphDatabase

from deposit import Client, DepositError
from deposit.errors import TransportError
from deposit._recovery.storage import ProtectedStore


@pytest.mark.integration
def test_installed_store_gateway_and_recovered_named_result_survive_restart():
    phase = os.environ["DEPOSIT_IDENTITY_PHASE"]
    uri = os.environ["DEPOSIT_TEST_URI"]
    bolt_password = os.environ["DEPOSIT_TEST_PASSWORD"]
    recovery_root = Path(os.environ["DEPOSIT_STAGE2_RECOVERY_ROOT"])
    protected = ProtectedStore(recovery_root)
    # A one-record fetch size makes the live cancellation case below exercise
    # an open server stream without relying on driver-side buffering.
    driver = GraphDatabase.driver(uri, auth=("neo4j", bolt_password), fetch_size=1)
    client = Client(driver)
    token = None
    store = None
    fixture = None
    try:
        if phase == "Seed":
            password = os.environ.get("DEPOSIT_STAGE2_ADMIN_PASSWORD")
            if not password:
                raise AssertionError("Stage 1 Smoke must provide the fresh-runtime test account")
            token = client.login("stage1-admin", password)
            assert client.get_active_namespace(token) is None
            namespace_name = "stage3-gateway-" + uuid.uuid4().hex[:10]
            created = client._single(
                "CYPHER 25 CALL deposit.create.namespace($name, $properties, $token) YIELD node RETURN node",
                {"name": namespace_name, "properties": {}, "token": token},
            )["node"]
            namespace_id = created.get("id")
            assert isinstance(namespace_id, str) and namespace_id
            store = client.open_store(
                {"id": namespace_id}, username="stage1-admin", password=password,
                recovery_directory=str(recovery_root),
            )

            selected = client.set_active_namespace(token, {"id": namespace_id})
            assert selected["id"] == namespace_id
            assert client.get_active_namespace(token)["id"] == namespace_id
            assert client.get_namespace(token, {"id": namespace_id})["id"] == namespace_id
            actor_id = client.get_session(token)["user"]["id"]
            assert client.get_user(token, {"id": actor_id})["id"] == actor_id
            assert client.get_namespace_page(token, {"id": namespace_id}, {})["items"]
            assert client.get_user_page(token, {"id": actor_id}, {})["items"]
            with pytest.raises(DepositError) as invalid_selector:
                client.get_namespace(token, {"id": "not-a-uuid"})
            assert invalid_selector.value.code == "VALIDATION_FAILED"

            # Administrative mode is a per-session value, and mode changes
            # serialize with Store work and commit before becoming visible.
            second_store = client.open_store(
                {"id": namespace_id}, username="stage1-admin", password=password,
                recovery_directory=str(recovery_root),
            )
            try:
                assert store.get_administrative_mode() is False
                assert second_store.get_administrative_mode() is False
                assert store.set_administrative_mode(True) is True
                assert store.get_administrative_mode() is True
                assert second_store.get_administrative_mode() is False
                assert store.set_administrative_mode(False) is False
            finally:
                second_store.close()

            # Exercise an actual permission denial through the Client
            # dry-run route. Test setup uses the private fixture route;
            # the public Client action remains deferred pending its identity design.
            reader_name = "stage3-reader-" + uuid.uuid4().hex[:10]
            reader_password = "S3-" + secrets.token_urlsafe(24)
            reader = client._single(
                "CYPHER 25 CALL deposit.create.user($username, $password, $properties, $token) "
                "YIELD data RETURN data",
                {"username": reader_name, "password": reader_password,
                 "properties": {"serverRole": "user"}, "token": token},
            )["data"]
            client._single(
                "CYPHER 25 CALL deposit.utils.refresh($scope, $token) YIELD data RETURN data",
                {"scope": "namespace", "token": token},
            )
            client._single(
                "CYPHER 25 CALL deposit.set.namespaceRole($user, $role, $token) YIELD data RETURN data",
                {"user": {"id": reader["id"]}, "role": "viewer", "token": token},
            )
            reader_token = client.login(reader_name, reader_password)
            try:
                with pytest.raises(DepositError) as denied:
                    client.prune_resources(reader_token, {"dryRun": True, "scope": "all"})
                assert denied.value.code == "FORBIDDEN"
            finally:
                client.logout(reader_token)

            store.refresh()
            # Exercise operational export and analysis through the same public
            # Store gateway. Both are prepared actions with named portable rows.
            exported = store.call("deposit.save.data", {}, {})
            assert exported.columns == ("data",)
            export_data = next(exported)["data"]
            assert export_data["state"] == "ready"
            assert isinstance(export_data["resourceId"], str) and export_data["resourceId"]
            with pytest.raises(StopIteration):
                next(exported)
            export_record = next(
                record for record in store._journal.records(store._identity)
                if record["procedure"] == "deposit.save.data"
                and record.get("outcome", {}).get("rows", [{}])[0]
                    .get("data", {}).get("resourceId") == export_data["resourceId"]
            )
            assert export_record["state"] == "released"
            assert export_record["result_format"] == "named_rows_v1"

            analyzed = store.call(
                "deposit.import.analyze", {"packageResourceId": export_data["resourceId"]}, {}
            )
            assert analyzed.columns == ("data",)
            analysis_data = next(analyzed)["data"]
            assert isinstance(analysis_data["planId"], str) and analysis_data["planId"]
            with pytest.raises(StopIteration):
                next(analyzed)
            analysis_record = next(
                record for record in store._journal.records(store._identity)
                if record["procedure"] == "deposit.import.analyze"
                and record.get("outcome", {}).get("rows", [{}])[0]
                    .get("data", {}).get("planId") == analysis_data["planId"]
            )
            assert analysis_record["state"] == "released"
            assert analysis_record["result_format"] == "named_rows_v1"

            class_name = "Stage3Gateway" + uuid.uuid4().hex[:10]
            created_class = store.call("deposit.create.class", class_name, {})
            assert created_class.columns == ("node",)
            class_row = next(created_class)
            class_data = class_row["node"]
            class_id = class_data["id"]
            assert class_data["name"] == class_name
            with pytest.raises(StopIteration):
                next(created_class)

            target_class = store.create_class("Stage3GatewayTarget" + uuid.uuid4().hex[:10])
            relationship_name = "Stage3GatewayLink" + uuid.uuid4().hex[:8]
            created_relationship = store.call(
                "deposit.create.classRelationship",
                {"id": class_id}, {"id": target_class["id"]}, relationship_name, {},
            )
            assert created_relationship.columns == ("rel",)
            relationship_data = next(created_relationship)["rel"]
            relationship_id = relationship_data["id"]
            with pytest.raises(StopIteration):
                next(created_relationship)

            # Store controls use the authenticated Store token. Operation reads
            # inspect server receipts and do not enter prepared-action recovery.
            requests = store.get_requests()
            assert isinstance(requests, tuple)
            assert all(set(item) == {"requestId", "actorId", "namespaceId"} for item in requests)
            assert all(isinstance(item["requestId"], str) and item["actorId"] for item in requests)
            assert all(item["namespaceId"] is None or isinstance(item["namespaceId"], str) for item in requests)

            action_record = next(
                record for record in store._journal.records(store._identity)
                if record["procedure"] == "deposit.create.class"
                and record.get("outcome", {}).get("rows", [{}])[0].get("node", {}).get("name") == class_name
            )
            server_operation = store.get_operation(action_record["operation_id"])
            assert server_operation["operationId"] == action_record["operation_id"]
            assert server_operation["state"] == "committed"
            assert server_operation["outcome"]["id"] == class_id
            last_operation = store.get_last_operation()
            assert last_operation["state"] in {"committed", "none", "unresolved"}

            # The advanced read returns the server's native node. Convert it
            # inside its owning result transaction, then verify it expires.
            read_result = store.call("deposit.get.class", {"id": class_id})
            assert read_result.columns == ("node",)
            native_row = next(read_result)
            portable = read_result.to_data(native_row["node"])
            assert portable["id"] == class_id and portable["name"] == class_name
            with pytest.raises(StopIteration):
                next(read_result)
            with pytest.raises(RuntimeError, match="no longer valid"):
                _ = native_row["node"]["id"]

            relationship_result = store.call(
                "deposit.get.classRelationship", {"id": relationship_id}
            )
            assert relationship_result.columns == ("rel",)
            native_relationship = next(relationship_result)["rel"]
            relationship_portable = relationship_result.to_data(native_relationship)
            assert relationship_portable["id"] == relationship_id
            with pytest.raises(StopIteration):
                next(relationship_result)
            with pytest.raises(RuntimeError, match="no longer valid"):
                _ = native_relationship.element_id

            with pytest.raises(DepositError) as missing_class:
                next(store.call("deposit.get.class", {"id": str(uuid.uuid4())}))
            assert missing_class.value.code == "NOT_FOUND"
            empty_page = store.call(
                "deposit.get.classRelationshipPage",
                {"label": "Stage3MissingLink" + uuid.uuid4().hex[:10]}, {},
            )
            assert empty_page.columns == ("data",)
            assert next(empty_page)["data"]["items"] == []
            with pytest.raises(StopIteration):
                next(empty_page)

            # Keep a streaming query open after its first row so a parallel
            # control can discover and cancel the request while Store work is
            # active. A second actor cannot cancel the first actor's request.
            cancel_class = "Stage3Cancellation" + uuid.uuid4().hex[:8]
            cancel_class_data = store.create_class(cancel_class)
            for _ in range(3):
                store.create_object({"id": cancel_class_data["id"]}, {})
            reader_token = client.login(reader_name, reader_password)
            reader_store = client.open_store(
                {"id": namespace_id}, username=reader_name, password=reader_password,
                recovery_directory=str(recovery_root),
            )
            active_query = store.call(
                "deposit.get.query", f"SELECT {cancel_class} AS item", {}, {}
            )
            first_query_row = next(active_query)
            assert first_query_row["data"]["item"]
            requests = store.get_requests()
            owned_request = next(
                request for request in requests
                if request["actorId"] == actor_id and request["namespaceId"] == namespace_id
            )
            with pytest.raises(DepositError) as other_actor_denied:
                reader_store.cancel_request(owned_request["requestId"])
            assert other_actor_denied.value.code == "FORBIDDEN"
            assert store.cancel_request(owned_request["requestId"]) is True
            try:
                for _ in range(2500):
                    try:
                        next(active_query)
                    except DepositError as cancelled:
                        assert cancelled.code == "CANCELLED"
                        break
                    except TransportError:
                        # Neo4j may terminate the Bolt transaction before the
                        # procedure's safe error envelope reaches the driver.
                        break
                    except StopIteration:
                        raise AssertionError("The active query completed instead of observing cancellation")
                else:
                    raise AssertionError("The active query did not observe cancellation after draining buffered rows")
                assert all(
                    request["requestId"] != owned_request["requestId"]
                    for request in store.get_requests()
                )
            finally:
                reader_store.close()
                client.logout(reader_token)

            recovered = client.recovered_operations(recovery_directory=str(recovery_root))
            matching = []
            for item in recovered:
                rows = item.result.get("rows", ())
                if (
                    item.procedure == "deposit.create.class"
                    and item.result.get("columns") == ("node",)
                    and rows
                    and rows[0]["node"].get("name") == class_name
                ):
                    matching.append(item)
            assert len(matching) == 1
            fixture = {
                "namespace_id": namespace_id,
                "namespace_name": namespace_name,
                "class_id": class_id,
                "class_name": class_name,
                "operation_id": matching[0].id,
            }
            protected.write("stage3-admin-password", password.encode("utf-8"))
            protected.write("stage3-gateway-fixture", json.dumps(fixture, sort_keys=True).encode("utf-8"))
            store.close()
            store = None
        else:
            fixture_bytes = protected.read("stage3-gateway-fixture")
            fixture = json.loads(fixture_bytes.decode("utf-8"))
            recovered = client.recovered_operations(recovery_directory=str(recovery_root))
            matching = [item for item in recovered if item.id == fixture["operation_id"]]
            assert len(matching) == 1
            operation = matching[0]
            assert operation.namespace_id == fixture["namespace_id"]
            assert operation.procedure == "deposit.create.class"
            assert operation.result["columns"] == ("node",)
            assert operation.result["rows"][0]["node"]["id"] == fixture["class_id"]
            assert operation.result["rows"][0]["node"]["name"] == fixture["class_name"]

            # Stage 2's Verify cleanup deletes its own password record before
            # this file runs, so Stage 3 keeps a separately named protected copy.
            password_bytes = protected.read("stage3-admin-password")
            password = password_bytes.decode("utf-8")
            token = client.login("stage1-admin", password)
            client.set_active_namespace(token, {"id": fixture["namespace_id"]})
            store = client.open_store(
                {"id": fixture["namespace_id"]}, username="stage1-admin", password=password,
                recovery_directory=str(recovery_root),
            )
            persisted = store.get_class({"id": fixture["class_id"]})
            assert persisted["id"] == fixture["class_id"]
            assert persisted["name"] == fixture["class_name"]
            store.close()
            store = None
            client._single(
                "CYPHER 25 CALL deposit.delete.namespace($confirmNamespaceName, $token) YIELD data RETURN data",
                {"confirmNamespaceName": fixture["namespace_name"], "token": token},
            )
            protected.delete("stage3-admin-password")
            protected.delete("stage3-gateway-fixture")
    finally:
        if store is not None:
            store.close()
        if token:
            try:
                client.logout(token)
            except Exception:
                pass
        client.close()
        driver.close()
