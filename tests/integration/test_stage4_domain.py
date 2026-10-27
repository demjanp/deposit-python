"""Installed-package acceptance for typed values, snapshots, pages and batches."""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
from datetime import datetime, time, timedelta, timezone
import uuid

import pytest
from neo4j import GraphDatabase
from neo4j.spatial import WGS84Point
from neo4j.time import DateTime, Duration

from deposit import Client, DDict, DObject, DepositError, TypedList, typed
from deposit._recovery.storage import ProtectedStore


def _plain(value):
    if hasattr(value, "items"):
        return {key: _plain(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_plain(item) for item in value]
    return value


def _wire_object(client, store, object_id):
    """Read the Server's portable result directly, without Python value decoding."""
    with client._session() as session:
        result = session.run(
            "CYPHER 25 CALL deposit.get.object($selection, $token) YIELD node "
            "CALL deposit.utils.toData(node, $token) YIELD data RETURN data",
            {"selection": {"id": object_id}, "token": store._token},
        )
        record = result.single(strict=True)
        result.consume()
        return record["data"]


@pytest.mark.integration
def test_installed_domain_values_snapshots_pages_and_batch_recovery():
    phase = os.environ["DEPOSIT_IDENTITY_PHASE"]
    uri = os.environ["DEPOSIT_TEST_URI"]
    bolt_password = os.environ["DEPOSIT_TEST_PASSWORD"]
    recovery_root = Path(os.environ["DEPOSIT_STAGE2_RECOVERY_ROOT"])
    protected = ProtectedStore(recovery_root)
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
            namespace_name = "stage4-domain-" + uuid.uuid4().hex[:10]
            namespace = client._single(
                "CYPHER 25 CALL deposit.create.namespace($name, $properties, $token) YIELD node RETURN node",
                {"name": namespace_name, "properties": {}, "token": token},
            )["node"]
            namespace_id = namespace["id"]
            client.set_active_namespace(token, {"id": namespace_id})
            store = client.open_store(
                {"id": namespace_id}, username="stage1-admin", password=password,
                recovery_directory=str(recovery_root),
            )

            # Schema conversion is an explicit Namespace action. Successful
            # conversion invalidates other cached values and checked schemas;
            # failed exact conversion leaves both declaration and values intact.
            conversion_class = store.create_class("Stage4Conversion" + uuid.uuid4().hex[:10])
            store.create_class_descriptor(conversion_class, "Amount", "Integer")
            conversion_object = store.create_object(
                conversion_class, {"descriptors": {"Amount": 10}}
            )
            conversion_observer = store.get_object_handle({"id": conversion_object.id})
            conversion_schema_observer = store.get_class_handle({"id": conversion_class.id})
            # Object creation advances the Namespace revision and stales older
            # handles; explicitly refresh before using the class declaration for
            # the schema conversion edit.
            conversion_class.refresh()
            store.change_class_descriptor_type(
                conversion_class, "Amount", "Float", conversion="integerToFloat"
            )
            assert conversion_observer.stale
            assert conversion_schema_observer.stale
            conversion_observer.refresh()
            assert conversion_observer.get_descriptor_type("Amount") == "Float"
            assert conversion_observer["Amount"] == 10.0
            conversion_class = store.get_class_handle({"id": conversion_class.id})
            assert conversion_class.declarations["Amount"]["type"] == "Float"
            conversion_observer["Amount"] = 10.5
            conversion_class.refresh()
            with pytest.raises(DepositError) as inexact_conversion:
                store.change_class_descriptor_type(
                    conversion_class, "Amount", "Integer", conversion="floatToIntegerExact"
                )
            assert inexact_conversion.value.code == "TYPE_CONFLICT"
            assert conversion_class.declarations["Amount"]["type"] == "Float"
            assert store.get_object_handle({"id": conversion_object.id})["Amount"] == 10.5

            # An independently checked schema cannot convert against an
            # obsolete Namespace baseline after another Store commits.
            stale_conversion_store = client.open_store(
                {"id": namespace_id}, username="stage1-admin", password=password,
                recovery_directory=str(recovery_root),
            )
            try:
                stale_conversion_class = stale_conversion_store.get_class_handle(
                    {"id": conversion_class.id}
                )
                current_conversion_object = store.get_object_handle({"id": conversion_object.id})
                current_conversion_object["Amount"] = 11.0
                with pytest.raises(DepositError) as stale_conversion:
                    stale_conversion_store.change_class_descriptor_type(
                        stale_conversion_class, "Amount", "Integer", conversion="floatToIntegerExact"
                    )
                assert stale_conversion.value.code == "REVISION_CONFLICT"
                assert stale_conversion_class.stale
            finally:
                stale_conversion_store.close()

            # The declaration is the decode authority for Object snapshots.
            schema = store.create_class("Stage4Domain" + uuid.uuid4().hex[:10])
            store.create_class_descriptor(schema, "Code", "String")
            store.create_class_descriptor(schema, "Count", "Integer")
            store.create_class_descriptor(schema, "Optional", "String")
            store.create_class_descriptor(schema, "Meta", "DDict")
            store.create_class_descriptor(schema, "At", "DateTime")
            store.create_class_descriptor(schema, "LocalAt", "LocalDateTime")
            store.create_class_descriptor(schema, "LocalClock", "LocalTime")
            store.create_class_descriptor(schema, "Span", "Duration")
            store.create_class_descriptor(schema, "Location", "Point")
            store.create_class_descriptor(schema, "Samples", "List")
            store.create_class_descriptor(schema, "Empty", "List")
            store.create_class_descriptor(schema, "AllNull", "List")
            journal_records = len(store._journal.records(store._identity))
            with pytest.raises(TypeError, match="declared as Integer"):
                store.create_object(schema, {"descriptors": {"Count": typed("String", "bad")}})
            assert len(store._journal.records(store._identity)) == journal_records
            maximum = 2**63 - 1
            precise_time = DateTime(
                2026, 10, 27, 1, 2, 3, nanosecond=123456789,
                tzinfo=timezone(timedelta(hours=5, minutes=30)),
            )
            calendar_span = Duration(months=-13, days=-2, seconds=7, nanoseconds=123456789)
            location = WGS84Point((14.25, 50.08))
            first = store.create_object(schema, {"descriptors": {
                "Code": "a", "Count": typed("Integer", maximum), "Optional": None,
                "Meta": typed("DDict", DDict({"flag": typed("Boolean", True)})),
                "At": typed("DateTime", precise_time), "Span": typed("Duration", calendar_span),
                "LocalAt": datetime(2026, 10, 27, 1, 2, 3, 456789),
                "LocalClock": time(4, 5, 6, 123456),
                "Location": typed("Point", location),
                "Samples": typed("List", TypedList([1, None, 2], item_type="Integer")),
                "Empty": typed("List", TypedList([], item_type="String")),
                "AllNull": typed("List", TypedList([None, None], item_type="Integer")),
            }})
            assert isinstance(first, DObject)
            assert first["Count"] == maximum
            assert first["Optional"] is None
            assert first["At"].nanosecond == 123456789
            assert first["LocalAt"].iso_format() == "2026-10-27T01:02:03.456789000"
            assert first["LocalClock"].iso_format() == "04:05:06.123456000"
            assert first["Span"] == calendar_span
            assert first["Location"].srid == 4326
            second = store.create_object(schema, {"descriptors": {"Code": "b", "Count": 2**53 + 1}})
            with pytest.raises(KeyError):
                _ = second["Optional"]

            wire = _wire_object(client, store, first.id)
            assert wire["descriptors"]["Count"] == maximum
            assert wire["descriptors"]["Optional"] is None
            assert wire["descriptors"]["Meta"] == {"flag": {"type": "Boolean", "value": True}}
            assert "123456789+05:30" in wire["descriptors"]["At"]
            assert wire["descriptors"]["LocalAt"] == "2026-10-27T01:02:03.456789"
            assert wire["descriptors"]["LocalClock"] == "04:05:06.123456"
            assert wire["descriptors"]["Span"] == {
                "months": -13, "days": -2, "seconds": 7, "nanoseconds": 123456789,
            }
            assert wire["descriptors"]["Location"] == {"srid": 4326, "coordinates": [14.25, 50.08]}
            assert wire["descriptors"]["Samples"] == {
                "itemType": "Integer", "items": [1, None, 2],
            }
            assert wire["descriptors"]["Empty"] == {"itemType": "String", "items": []}
            assert wire["descriptors"]["AllNull"] == {"itemType": "Integer", "items": [None, None]}

            first.refresh()
            first["Unknown"] = typed("String", None)
            assert first._schema["Unknown"] == "String"
            assert _wire_object(client, store, first.id)["descriptors"]["Unknown"] is None

            detached_meta = first["Meta"]
            detached_meta["local"] = typed("String", "only in memory")
            assert "local" not in _wire_object(client, store, first.id)["descriptors"]["Meta"]
            first["Meta"] = detached_meta
            assert _wire_object(client, store, first.id)["descriptors"]["Meta"]["local"] == {
                "type": "String", "value": "only in memory",
            }

            schema.refresh()
            assert store.get_class(schema.name)["id"] == schema.id
            assert store.get_class({"id": schema.id})["id"] == schema.id

            store.create_class_descriptor(schema, "Later", "String")
            assert first.stale
            with pytest.raises(RuntimeError, match="stale"):
                _ = first["Code"]
            first.refresh()
            assert first._schema["Later"] == "String"
            with pytest.raises(KeyError):
                _ = first["Later"]

            # A different Store's checked snapshot pins an independent edit
            # baseline. Conflict keeps the user's intended value for reapply.
            other = client.open_store(
                {"id": namespace_id}, username="stage1-admin", password=password,
                recovery_directory=str(recovery_root),
            )
            try:
                stale = other.get_object_handle({"id": second.id})
                first.refresh()
                first["Count"] = maximum - 1
                with pytest.raises(DepositError) as conflict:
                    stale["Count"] = 77
                assert conflict.value.code == "REVISION_CONFLICT"
                assert stale.pending == {"Count": 77}
                assert stale.stale
                stale.refresh().reapply()
                assert stale["Count"] == 77
                assert stale.pending == {}
            finally:
                other.close()

            schema.refresh()
            query = f"SELECT {schema.name}.Code AS code, {schema.name}.Optional AS optional ORDER BY code"
            iterator = store.iter_query(query, page_size=1)
            first_row = next(iterator)
            assert first_row["presence"]["code"] is True
            scoped_page = store.get_query_page(query, page_size=1)
            foreign_namespace_name = "stage4-scope-" + uuid.uuid4().hex[:10]
            foreign_namespace = client._single(
                "CYPHER 25 CALL deposit.create.namespace($name, $properties, $token) YIELD node RETURN node",
                {"name": foreign_namespace_name, "properties": {}, "token": token},
            )["node"]
            foreign_store = None
            try:
                foreign_store = client.open_store(
                    {"id": foreign_namespace["id"]}, username="stage1-admin", password=password,
                    recovery_directory=str(recovery_root),
                )
                with pytest.raises(DepositError) as scope_conflict:
                    foreign_store.get_query_page(query, page_size=1, cursor=scoped_page["nextCursor"])
                assert scope_conflict.value.code == "SCOPE_MISMATCH"
            finally:
                if foreign_store is not None:
                    foreign_store.close()
                client.set_active_namespace(token, {"id": foreign_namespace["id"]})
                client._single(
                    "CYPHER 25 CALL deposit.delete.namespace($confirmNamespaceName, $token) YIELD data RETURN data",
                    {"confirmNamespaceName": foreign_namespace_name, "token": token},
                )
                client.set_active_namespace(token, {"id": namespace_id})
            # A namespace revision change invalidates the opaque cursor. The
            # iterator propagates the rejection and never restarts at row one.
            mutation_target = store.get_object_handle({"id": second.id})
            mutation_target["Code"] = "c"
            with pytest.raises(DepositError) as cursor_conflict:
                next(iterator)
            assert cursor_conflict.value.code == "REVISION_CONFLICT"
            assert iterator.closed

            rows = store.iter_query(query, page_size=1).materialize(max_items=3)
            assert len(rows) == 2
            assert {row["values"]["code"] for row in rows} == {"a", "c"}
            nullable = next(row for row in rows if row["values"]["code"] == "a")
            assert nullable["values"]["optional"] is None
            assert nullable["presence"]["optional"] is True
            missing = next(row for row in rows if row["values"]["code"] == "c")
            assert missing["presence"]["optional"] is False

            # A failed batch must roll back its earlier create command.
            failed_name = "Stage4Rollback" + uuid.uuid4().hex[:8]
            with pytest.raises(DepositError) as failed_batch:
                with store.transaction() as batch:
                    batch.create_class(failed_name)
                    batch.create_object({"id": str(uuid.uuid4())}, {})
            with pytest.raises(DepositError) as absent:
                store.get_class(failed_name)
            assert absent.value.code == "NOT_FOUND"
            failure_notices = [
                item for item in client.pending_operations(recovery_directory=str(recovery_root))
                if item.procedure == "deposit.apply.batch" and item.state == "failed"
            ]
            assert len(failure_notices) == 1

            # The dependent object is bound through an opaque earlier-command
            # reference; its exact saved outcome survives deletion and restart.
            batch_class_name = "Stage4Batch" + uuid.uuid4().hex[:8]
            pattern_name = "Stage4Pattern" + uuid.uuid4().hex[:8]
            pattern_definition = {"objects": {"sample": {
                "className": batch_class_name, "root": True,
                "descriptors": [], "relationships": [],
            }}}
            with store.transaction() as batch:
                class_ref = batch.create_class(batch_class_name, {"descriptors": [
                    {"name": "Count", "type": "Integer"},
                ]})
                object_ref = batch.create_object(
                    class_ref, {"descriptors": {"Count": typed("Integer", 17)}}
                )
                count_ref = batch.set_descriptor(object_ref, "Count", typed("Integer", 18))
                pattern_ref = batch.call("deposit.create.pattern", pattern_name, pattern_definition, {})
                batch.call("deposit.set.pattern", pattern_ref, {"comment": "updated in batch"})
                batch.call("deposit.delete.pattern", pattern_ref)
            class_data = batch.result(class_ref)
            object_data = batch.result(object_ref)
            count_data = batch.result(count_ref)
            pattern_data = batch.result(pattern_ref)
            assert object_data["descriptors"]["Count"] == 17
            assert count_data["kind"] == "Descriptor"
            assert count_data["value"] == 18
            operation = next(
                item for item in client.recovered_operations(recovery_directory=str(recovery_root))
                if item.procedure == "deposit.apply.batch"
                and any(value.get("id") == object_data["id"]
                        for value in item.result["results"].values())
            )
            with store.call("deposit.delete.object", object_data["id"]):
                pass
            with pytest.raises(DepositError) as deleted:
                store.get_object({"id": object_data["id"]})
            assert deleted.value.code == "NOT_FOUND"

            # Execute a real batch in a child, then terminate after the server
            # returns its committed reply but before the coordinator receives
            # it. Verification recovers this unresolved journal after restart.
            store.close()
            store = None
            lost_reply_class_name = "Stage4LostReply" + uuid.uuid4().hex[:8]
            child_env = dict(os.environ)
            child_env["DEPOSIT_STAGE4_NAMESPACE_ID"] = namespace_id
            child_env["DEPOSIT_STAGE4_LOST_REPLY_CLASS"] = lost_reply_class_name
            child = subprocess.run(
                [sys.executable, "-c", _LOST_BATCH_REPLY_SCRIPT],
                capture_output=True, text=True, timeout=180, env=child_env, check=False,
            )
            assert child.returncode == 73, (
                f"Lost-reply child did not stop after committed batch; exit={child.returncode}; "
                f"stdout={child.stdout[-2000:]}; stderr={child.stderr[-2000:]}"
            )
            lost_reply = [item for item in client.pending_operations(recovery_directory=str(recovery_root))
                          if item.procedure == "deposit.apply.batch"
                          and item.namespace_id == namespace_id and item.state == "executing"]
            assert len(lost_reply) == 1
            fixture = {
                "namespace_id": namespace_id,
                "namespace_name": namespace_name,
                "operation_id": operation.id,
                "failed_notice_id": failure_notices[0].id,
                "failed_error_code": failed_batch.value.code,
                "failed_class_name": failed_name,
                "class_id": class_data["id"],
                "object_id": object_data["id"],
                "object_result": object_data,
                "count_result": count_data,
                "pattern_id": pattern_data["id"],
                "pattern_result": pattern_data,
                "lost_reply_operation_id": lost_reply[0].id,
                "lost_reply_class_name": lost_reply_class_name,
            }
            protected.write("stage4-admin-password", password.encode("utf-8"))
            protected.write("stage4-domain-fixture", json.dumps(fixture, sort_keys=True).encode("utf-8"))
        else:
            fixture = json.loads(protected.read("stage4-domain-fixture").decode("utf-8"))
            password = protected.read("stage4-admin-password").decode("utf-8")
            unresolved = [item for item in client.pending_operations(recovery_directory=str(recovery_root))
                          if item.id == fixture["lost_reply_operation_id"]]
            assert len(unresolved) == 1 and unresolved[0].state == "executing"
            assert unresolved[0].procedure == "deposit.apply.batch"
            recovered_lost_reply = client.recover(
                unresolved[0], username="stage1-admin", password=password,
            )
            assert recovered_lost_reply.id == fixture["lost_reply_operation_id"]
            assert recovered_lost_reply.procedure == "deposit.apply.batch"
            lost_results = recovered_lost_reply.result["results"]
            assert len(lost_results) == 3
            lost_class = next(value for value in lost_results.values()
                              if value.get("kind") == "Class"
                              and value.get("name") == fixture["lost_reply_class_name"])
            lost_object = next(value for value in lost_results.values()
                               if value.get("kind") == "Object"
                               and value.get("classId") == lost_class["id"])
            lost_descriptor = next(value for value in lost_results.values()
                                   if value.get("kind") == "Descriptor"
                                   and value.get("objectId") == lost_object["id"]
                                   and value.get("name") == "Count")
            assert lost_object["descriptors"]["Count"] == 41
            assert lost_descriptor["value"] == 42

            recovered = client.recovered_operations(recovery_directory=str(recovery_root))
            matching = [item for item in recovered if item.id == fixture["operation_id"]]
            assert len(matching) == 1
            operation = matching[0]
            assert operation.procedure == "deposit.apply.batch"
            saved_results = operation.result["results"]
            saved_object = next(value for value in saved_results.values()
                                if value.get("id") == fixture["object_id"])
            assert _plain(saved_object) == fixture["object_result"]
            saved_count = next(value for value in saved_results.values()
                               if value.get("kind") == "Descriptor"
                               and value.get("objectId") == fixture["object_id"]
                               and value.get("name") == "Count")
            assert saved_count["value"] == 18
            assert _plain(saved_count) == fixture["count_result"]
            saved_pattern = next(value for value in saved_results.values()
                                 if value.get("id") == fixture["pattern_id"])
            assert _plain(saved_pattern) == fixture["pattern_result"]

            token = client.login("stage1-admin", password)
            client.set_active_namespace(token, {"id": fixture["namespace_id"]})
            store = client.open_store(
                {"id": fixture["namespace_id"]}, username="stage1-admin", password=password,
                recovery_directory=str(recovery_root),
            )
            assert store.get_class(fixture["lost_reply_class_name"])["id"] == lost_class["id"]
            persisted_lost_object = store.get_object({"id": lost_object["id"]})
            assert persisted_lost_object["descriptors"]["Count"] == 42
            with pytest.raises(DepositError) as deleted:
                store.get_object({"id": fixture["object_id"]})
            assert deleted.value.code == "NOT_FOUND"
            with pytest.raises(DepositError) as failed_rollback:
                store.get_class(fixture["failed_class_name"])
            assert failed_rollback.value.code == "NOT_FOUND"
            notices = [item for item in client.pending_operations(recovery_directory=str(recovery_root))
                       if item.id == fixture["failed_notice_id"]]
            assert len(notices) == 1 and notices[0].state == "failed"
            with pytest.raises(DepositError) as failed_recovery:
                client.recover(notices[0], username="stage1-admin", password=password)
            assert failed_recovery.value.code == fixture["failed_error_code"]
            assert any(item.id == fixture["failed_notice_id"]
                       for item in client.pending_operations(recovery_directory=str(recovery_root)))
            persisted_class = store.get_class({"id": fixture["class_id"]})
            assert persisted_class["id"] == fixture["class_id"]
            store.close()
            store = None
            client.set_active_namespace(token, {"id": fixture["namespace_id"]})
            client._single(
                "CYPHER 25 CALL deposit.delete.namespace($confirmNamespaceName, $token) YIELD data RETURN data",
                {"confirmNamespaceName": fixture["namespace_name"], "token": token},
            )
            protected.delete("stage4-admin-password")
            protected.delete("stage4-domain-fixture")
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


_LOST_BATCH_REPLY_SCRIPT = r'''
import os
from neo4j import GraphDatabase
from deposit import Client

driver = GraphDatabase.driver(os.environ["DEPOSIT_TEST_URI"], auth=("neo4j", os.environ["DEPOSIT_TEST_PASSWORD"]))
client = Client(driver)
store = client.open_store(
    {"id": os.environ["DEPOSIT_STAGE4_NAMESPACE_ID"]},
    username="stage1-admin",
    password=os.environ["DEPOSIT_STAGE2_ADMIN_PASSWORD"],
    recovery_directory=os.environ["DEPOSIT_STAGE2_RECOVERY_ROOT"],
)
original_single = client._single

def execute_then_drop_reply(query, parameters=None):
    row = original_single(query, parameters)
    if "deposit.apply.batch" in query:
        os._exit(73)
    return row

client._single = execute_then_drop_reply
with store.transaction() as batch:
    class_ref = batch.create_class(os.environ["DEPOSIT_STAGE4_LOST_REPLY_CLASS"], {
        "descriptors": [{"name": "Count", "type": "Integer"}],
    })
    object_ref = batch.create_object(class_ref, {"descriptors": {"Count": 41}})
    batch.set_descriptor(object_ref, "Count", 42)
os._exit(0)
'''
