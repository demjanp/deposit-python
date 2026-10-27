from __future__ import annotations

from copy import deepcopy

import pytest

from deposit.entities import DClass, DObject, decode_snapshot
from deposit.errors import DepositError
from deposit.store import _encode_object_properties
from deposit.values import TypedList, typed


class _HandleStore:
    namespace_id = "namespace-a"
    _observed_revision = 7

    def __init__(self):
        self.handles = []
        self.calls = []
        self.conflict_once = True
        self.data = {"kind": "Object", "id": "object-1", "namespaceId": "namespace-a", "classId": "class-1",
                     "descriptors": {"nullable": None, "value": 4}}
        self.schema = {"kind": "Class", "id": "class-1", "namespaceId": "namespace-a",
                       "descriptors": [{"name": "nullable", "type": "String"},
                                       {"name": "value", "type": "Integer"}]}

    def _register_handle(self, handle):
        self.handles.append(handle)

    def _checked_entity_snapshot(self, kind, identity):
        assert kind == "Object" and identity == {"id": "object-1"}
        return deepcopy(self.data), {item["name"]: item["type"] for item in self.schema["descriptors"]}, 7

    def set_descriptor(self, handle, name, value):
        self.calls.append((name, value))
        if self.conflict_once:
            self.conflict_once = False
            raise DepositError("REVISION_CONFLICT", "stale handle")
        self.data["descriptors"][name] = value
        handle._snapshot = deepcopy(self.data)
        handle._stale = False
        handle._baseline_revision = 8

    def refresh(self):
        self._observed_revision = 8
        for handle in self.handles:
            handle._mark_stale()


def _handle(store):
    return decode_snapshot(store.data, store.schema, store.namespace_id, store)


def test_object_handle_distinguishes_missing_from_explicit_null_and_uses_checked_schema():
    store = _HandleStore()
    dobject = _handle(store)
    assert isinstance(dobject, DObject)
    assert dobject["nullable"] is None
    assert dobject.get("nullable", "default") is None
    assert dobject.get("missing", "default") == "default"
    with pytest.raises(KeyError):
        _ = dobject["missing"]
    with pytest.raises(DepositError, match="declaration"):
        decode_snapshot({**store.data, "descriptors": {"wrong": 1}}, store.schema, store.namespace_id, store)


def test_stale_handle_preserves_edit_until_explicit_refresh_and_reapply():
    store = _HandleStore()
    dobject = _handle(store)
    with pytest.raises(DepositError) as conflict:
        dobject["value"] = 12
    assert conflict.value.code == "REVISION_CONFLICT"
    assert dobject.pending == {"value": 12}
    store.refresh()
    with pytest.raises(RuntimeError, match="stale"):
        _ = dobject["value"]
    dobject.refresh()
    assert dobject["value"] == 4
    assert dobject.pending == {"value": 12}
    dobject.reapply()
    assert dobject["value"] == 12
    assert dobject.pending == {}
    assert [value for _, value in store.calls] == [12, 12]


def test_handles_are_bound_to_exact_store_and_namespace_identity():
    store = _HandleStore()
    foreign = _HandleStore()
    foreign.namespace_id = "namespace-b"
    handle = _handle(store)
    with pytest.raises(DepositError, match="Namespace"):
        DObject(foreign, {**store.data, "namespaceId": "namespace-a"})
    assert handle.id == "object-1"
    assert handle.namespace_id == "namespace-a"
    assert handle.kind == "Object"


def test_class_handle_declarations_are_detached_and_id_stable():
    store = _HandleStore()
    data = deepcopy(store.schema)
    dclass = DClass(store, data, {"nullable": "String", "value": "Integer"})
    declarations = dclass.declarations
    assert declarations["value"]["type"] == "Integer"
    declarations["value"]["type"] = "Float"
    assert dclass.declarations["value"]["type"] == "Integer"
    assert dclass.id == "class-1"


def test_object_descriptor_inputs_are_encoded_before_the_action_journal():
    assert _encode_object_properties({"descriptors": {"value": 12}}, {"value": "Integer"}) == {
        "descriptors": {"value": 12}
    }
    assert _encode_object_properties({"descriptors": {"new": typed("Integer", 12)}}, {}) == {
        "descriptors": {"new": {"type": "Integer", "value": 12}}
    }
    with pytest.raises(TypeError):
        _encode_object_properties({"descriptors": {"value": typed("String", "wrong")}}, {"value": "Integer"})
    with pytest.raises(TypeError):
        _encode_object_properties({"descriptors": {"bad": TypedList([], item_type="DGeometry")}}, {})


def test_invalid_descriptor_value_is_not_retained_as_a_pending_edit():
    store = _HandleStore()
    dobject = _handle(store)
    with pytest.raises(TypeError, match="declared as Integer"):
        dobject["value"] = typed("String", "wrong")
    assert dobject.pending == {}
    assert store.calls == []
