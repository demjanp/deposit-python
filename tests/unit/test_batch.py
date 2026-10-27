from __future__ import annotations

import pytest

from deposit._batch import Batch
from deposit.entities import DObject
from deposit.store import Store
from deposit.values import DDict, typed
from neo4j.time import Date, DateTime
from datetime import timezone


class _BatchStore:
    def __init__(self):
        self.active = None
        self.submissions = []
        self.pinned_revisions = []
        self.namespace_id = "namespace-a"

    def _validate_handle(self, handle, kind=None):
        if handle._store is not self or (kind is not None and handle.kind != kind):
            raise ValueError("handle belongs to another Store")

    def _begin_batch_collection(self, batch):
        if self.active is not None:
            raise RuntimeError("Nested batches are not supported")
        self.active = batch

    def _end_batch_collection(self, batch):
        if self.active is batch:
            self.active = None

    def _submit_batch(self, commands, *, pinned_revision=None):
        self.submissions.append(commands)
        self.pinned_revisions.append(pinned_revision)
        return {command["key"]: {"id": "saved-" + command["key"]} for command in commands}


def test_batch_submits_once_after_success_and_exposes_exact_saved_results():
    store = _BatchStore()
    with Batch(store) as batch:
        cls = batch.create_class("Find")
        obj = batch.create_object(cls)
    assert len(store.submissions) == 1
    assert store.submissions[0][1]["arguments"][0] == {"ref": store.submissions[0][0]["key"], "field": "id"}
    assert batch.result(cls)["id"] == "saved-" + store.submissions[0][0]["key"]
    assert batch.result(obj)["id"].startswith("saved-")


def test_collection_failure_submits_nothing_and_references_are_batch_local():
    store = _BatchStore()
    with pytest.raises(RuntimeError):
        with Batch(store) as batch:
            reference = batch.create_class("Find")
            raise RuntimeError("abort collection")
    assert store.submissions == []
    with pytest.raises(RuntimeError):
        batch.result(reference)

    first, second = _BatchStore(), _BatchStore()
    with Batch(first) as left:
        left_ref = left.create_class("First")
        with Batch(second) as right:
            with pytest.raises(ValueError):
                right.create_object(left_ref)
            right_ref = right.create_class("Second")
            with pytest.raises(ValueError):
                right.create_object({"id": right_ref})
    assert len(second.submissions) == 1
    assert len(second.submissions[0]) == 1


def test_batch_rejects_reads_nested_batches_and_noneligible_actions():
    store = _BatchStore()
    with Batch(store) as batch:
        created = batch.create_class("Find")
        with pytest.raises(ValueError):
            batch.call("deposit.get.class", "Find")
        with pytest.raises(ValueError):
            batch.call("deposit.apply.batch", [])
        with pytest.raises(ValueError):
            batch.call("deposit.create.object", "Find", {"nested": {"id": created}})
    assert len(store.submissions[0]) == 1


def test_pattern_commands_are_allowed_with_top_level_prior_result_references():
    store = _BatchStore()
    definition = {"objects": {"sample": {
        "className": "Specimen", "root": True, "descriptors": [], "relationships": [],
    }}}
    with Batch(store) as batch:
        created = batch.call("deposit.create.pattern", "Specimen form", definition, {})
        updated = batch.call("deposit.set.pattern", created, {"comment": "revised"})
        deleted = batch.call("deposit.delete.pattern", created)
    commands = store.submissions[0]
    assert [command["procedure"] for command in commands] == [
        "deposit.create.pattern", "deposit.set.pattern", "deposit.delete.pattern"
    ]
    assert commands[1]["arguments"][0] == {"ref": commands[0]["key"], "field": "id"}
    assert commands[2]["arguments"][0] == {"ref": commands[0]["key"], "field": "id"}
    assert batch.result(created)["id"].startswith("saved-")
    assert batch.result(updated)["id"].startswith("saved-")
    assert batch.result(deleted)["id"].startswith("saved-")


def test_pattern_reference_must_be_top_level_id_selector_and_failed_collection_rolls_back():
    store = _BatchStore()
    with pytest.raises(RuntimeError, match="abort"):
        with Batch(store) as batch:
            created = batch.call("deposit.create.pattern", "Specimen form", {"objects": {}}, {})
            batch.call("deposit.set.pattern", created, {"comment": "revised"})
            raise RuntimeError("abort before submission")
    assert store.submissions == []
    with Batch(store) as batch:
        created = batch.call("deposit.create.pattern", "Specimen form", {"objects": {}}, {})
        with pytest.raises(ValueError, match="top-level"):
            batch.call("deposit.set.pattern", {"id": created}, {"comment": "nested ref"})
        with pytest.raises(ValueError, match="identity argument"):
            batch.call("deposit.create.pattern", created, {"objects": {}}, {})
    assert len(store.submissions[0]) == 1


def test_batch_rejects_forged_wire_reference_markers_and_revalidates_eligibility():
    store = _BatchStore()
    with Batch(store) as batch:
        created = batch.create_class("safe")
        previous_key = batch.commands[0]["key"]
        with pytest.raises(ValueError, match="opaque BatchReference"):
            batch.create_object({"ref": previous_key, "field": "id"})
        batch.create_object(created)
    assert len(store.submissions[0]) == 2

    other = _BatchStore()
    with pytest.raises(ValueError, match="not eligible"):
        with Batch(other) as batch:
            batch.create_class("safe")
            batch._commands[0]["procedure"] = "deposit.get.class"
    assert other.submissions == []


def test_invalid_or_oversized_commands_do_not_poison_collector():
    store = _BatchStore()
    with Batch(store) as batch:
        with pytest.raises(TypeError):
            batch.create_class("invalid", {"x": object()})
        assert batch.commands == ()
        with pytest.raises(ValueError):
            batch.create_class("x" * (8 * 1024 * 1024))
        assert batch.commands == ()
        reference = batch.create_class("valid")
    assert batch.result(reference)["id"].startswith("saved-")


def test_batch_freezes_nested_inputs_before_caller_mutation_and_request_growth():
    store = _BatchStore()
    properties = {"nested": {"values": [1, {"comment": "before"}]}}
    with Batch(store) as batch:
        batch.create_class("Frozen", properties)
        properties["nested"]["values"][1]["comment"] = "x" * (8 * 1024 * 1024)
    frozen = store.submissions[0][0]["arguments"][1]
    assert frozen == {"nested": {"values": [1, {"comment": "before"}]}}


def test_batch_revalidates_the_whole_request_before_submission():
    store = _BatchStore()
    with pytest.raises(ValueError, match="request limit"):
        with Batch(store) as batch:
            batch.create_class("Frozen")
            batch._commands[0]["arguments"][1]["comment"] = "x" * (8 * 1024 * 1024)
    assert store.submissions == []


def test_descriptor_helpers_encode_against_pinned_handle_schema_and_preserve_wire_types():
    store = _BatchStore()
    handle = DObject(store, {
        "kind": "Object", "id": "object-1", "namespaceId": store.namespace_id,
        "classId": "class-1", "descriptors": {"count": 1, "when": None, "meta": None},
    }, {"count": "Integer", "when": "DateTime", "meta": "DDict"}, snapshot_revision=17)
    nested = DDict({"observed": typed("Date", Date(2026, 10, 27))})
    with Batch(store) as batch:
        count = batch.set_descriptor(handle, "count", typed("Integer", 9223372036854775807))
        moment = batch.set_descriptor(handle, "when", typed(
            "DateTime", DateTime(2026, 10, 27, 1, 2, 3, nanosecond=456, tzinfo=timezone.utc)
        ))
        metadata = batch.set_descriptor(handle, "meta", typed("DDict", nested))
    commands = store.submissions[0]
    assert [command["arguments"][2] for command in commands] == [
        9223372036854775807,
        "2026-10-27T01:02:03.000000456+00:00",
        {"observed": {"type": "Date", "value": "2026-10-27"}},
    ]
    assert batch.result(count)["id"].startswith("saved-")
    assert batch.result(moment)["id"].startswith("saved-")
    assert batch.result(metadata)["id"].startswith("saved-")
    assert store.pinned_revisions == [17]


def test_batch_descriptor_helpers_validate_nulls_mismatches_foreign_stale_and_baselines():
    store = _BatchStore()
    data = {"kind": "Object", "id": "object-1", "namespaceId": store.namespace_id,
            "classId": "class-1", "descriptors": {"count": 1}}
    handle = DObject(store, data, {"count": "Integer"}, snapshot_revision=17)
    foreign_store = _BatchStore()
    foreign = DObject(foreign_store, {**data, "namespaceId": foreign_store.namespace_id},
                      {"count": "Integer"}, snapshot_revision=17)
    stale = DObject(store, data, {"count": "Integer"}, snapshot_revision=17)
    stale._mark_stale()
    later = DObject(store, data, {"count": "Integer"}, snapshot_revision=18)
    with Batch(store) as batch:
        null_value = batch.set_descriptor(handle, "unknown", typed("String", None))
        assert batch.commands[0]["arguments"][2] == {"type": "String", "value": None}
        with pytest.raises(TypeError, match="declared as Integer"):
            batch.set_descriptor(handle, "count", typed("String", "wrong"))
        with pytest.raises(ValueError, match="another Store"):
            batch.set_descriptor(foreign, "count", 2)
        with pytest.raises(ValueError, match="Stale"):
            batch.set_descriptor(stale, "count", 2)
        with pytest.raises(ValueError, match="conflicting"):
            batch.set_descriptor(later, "count", 2)
    assert batch.result(null_value)["id"].startswith("saved-")
    assert len(store.submissions[0]) == 1


def test_prebatch_handle_schema_is_not_used_after_batch_schema_or_membership_changes():
    store = _BatchStore()
    handle = DObject(store, {
        "kind": "Object", "id": "object-1", "namespaceId": store.namespace_id,
        "classId": "class-1", "descriptors": {"count": 1},
    }, {"count": "Integer"}, snapshot_revision=17)
    with Batch(store) as batch:
        batch.create_class_descriptor({"id": "class-1"}, "new", "String")
        with pytest.raises(ValueError, match="schema or membership change"):
            batch.set_descriptor(handle, "count", 2)
    assert len(store.submissions[0]) == 1


@pytest.mark.parametrize("patch", [{"type": "Float"}, {"name": "renamed"}])
def test_class_descriptor_type_or_name_update_invalidates_batch_schema_knowledge(patch):
    store = _BatchStore()
    handle = DObject(store, {
        "kind": "Object", "id": "object-1", "namespaceId": store.namespace_id,
        "classId": "class-1", "descriptors": {"count": 1},
    }, {"count": "Integer"}, snapshot_revision=17)
    with Batch(store) as batch:
        class_ref = batch.create_class("Specimen", {"descriptors": [
            {"name": "count", "type": "Integer"},
        ]})
        object_ref = batch.create_object(class_ref, {"descriptors": {"count": 1}})
        batch.call("deposit.set.classDescriptor", {"id": "descriptor-1"}, patch)
        with pytest.raises(ValueError, match="no known Class schema"):
            batch.create_object(class_ref, {"descriptors": {"count": typed("String", "unknown")}})
        with pytest.raises(ValueError, match="no known Object schema"):
            batch.set_descriptor(object_ref, "count", typed("Integer", 2))
        with pytest.raises(ValueError, match="schema or membership change"):
            batch.set_descriptor(handle, "count", 2)
    assert len(store.submissions[0]) == 3


def test_class_descriptor_comment_and_order_updates_keep_schema_knowledge():
    store = _BatchStore()
    with Batch(store) as batch:
        class_ref = batch.create_class("Specimen", {"descriptors": [
            {"name": "count", "type": "Integer"},
        ]})
        batch.call("deposit.set.classDescriptor", {"id": "descriptor-1"}, {"comment": "note", "order": 2})
        batch.create_object(class_ref, {"descriptors": {"count": 2}})
    assert len(store.submissions[0]) == 3


@pytest.mark.parametrize("operation", ["read", "refresh", "cancel", "admin"])
def test_public_store_operations_are_rejected_during_batch_collection(operation):
    store = object.__new__(Store)
    store._batch_collection = object()
    if operation == "read":
        call = lambda: store.get_class("Find")
    elif operation == "refresh":
        call = store.refresh
    elif operation == "cancel":
        call = store.cancel
    else:
        call = store.get_administrative_mode
    with pytest.raises(RuntimeError, match="batch"):
        call()
