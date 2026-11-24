"""Synchronous operations on an authenticated, Namespace-bound Store."""

from __future__ import annotations

from collections.abc import Mapping
from contextlib import contextmanager, nullcontext, suppress
import json
import math
import threading
import weakref
from typing import Any, Iterator
import uuid

from ._exchange import ExchangeBundle, discard_package, package_for_target, stage_snapshot, stage_zip
from ._exchange_transport import OperationEndpoint, _IMPORT_RESULTS
from ._paging import ImportResult, _valid_plan_observation, valid_import_plan_wrapper
from ._resources import ResourceReader, upload_resource
from ._gateway import (
    StoreResult,
    bind_arguments,
    build_query,
    detached_result,
    domain_arguments,
    immutable_request,
    map_driver_error,
    store_procedure,
    store_control_procedure,
)
from .errors import DepositError, OperationCommittedAfterCancel, OutcomeUnknown, TransportError
from .entities import DClass, DObject, DNode
from .values import DResource


_STORE_FACTORY_TOKEN = object()
_DOMAIN_VALUE_TYPES = {
    "String", "Integer", "Float", "Boolean", "Date", "Time", "LocalTime", "DateTime",
    "LocalDateTime", "Duration", "Point", "List", "DDict", "DGeometry", "DResource",
}


class Store:
    """An independently authenticated session bound to one immutable Namespace UUID."""

    def __init__(
        self,
        client,
        *,
        token: str,
        actor_id: str,
        namespace_id: str,
        observed: Any,
        _factory_token: object | None = None,
    ):
        if _factory_token is not _STORE_FACTORY_TOKEN:
            raise TypeError("Store instances are created by Client.open_store()")
        self._client = client
        self._token = token
        self._actor_id = actor_id
        self._namespace_id = namespace_id
        self._operation_lock = threading.Lock()
        self._close_lock = threading.Lock()
        self._state_lock = threading.Lock()
        self._control_condition = threading.Condition(self._state_lock)
        self._inflight_controls = 0
        self._closed = False
        self._closing = False
        self._active = False
        self._active_result: StoreResult | None = None
        self._active_endpoint = None
        self._bundles = set()
        self._readers = weakref.WeakValueDictionary()
        self._ready_plans = set()
        self._plan_packages = {}
        self._result_packages = []
        self._batch_collection: Any = None
        # DNode equality intentionally follows stable domain identity.  A
        # WeakSet would therefore collapse separate snapshots of the same
        # entity; key by Python object identity so each cached snapshot is
        # invalidated independently.
        self._handles = weakref.WeakValueDictionary()
        self._endpoint = OperationEndpoint(client, token, namespace_id, actor=actor_id)
        self._observed_revision = self._get_revision(observed)
        try:
            if self._observed_revision is None:
                self.refresh()
        except BaseException:
            self._endpoint.close()
            raise

    @property
    def namespace_id(self) -> str:
        return self._namespace_id

    @property
    def closed(self) -> bool:
        with self._state_lock:
            return self._closed

    def create_class(self, name: str, properties: dict[str, Any] | None = None, *,
                     descriptors=None, position=None, order=None, comment=None) -> DClass:
        if not isinstance(name, str) or not name:
            raise ValueError("Class name must be a nonempty string")
        if properties is not None and not isinstance(properties, dict):
            raise TypeError("Class properties must be a dictionary")
        props = dict(properties or {})
        if descriptors is not None:
            props["descriptors"] = _class_declarations(descriptors)
        if position is not None:
            props["position"] = list(_position_value(position))
        if order is not None:
            if type(order) is not int or order < 0:
                raise ValueError("Class order must be a nonnegative integer")
            props["order"] = order
        if comment is not None:
            if not isinstance(comment, str):
                raise TypeError("Class comment must be a string")
            props["comment"] = comment
        props = _portable_input(props)
        created, (data, schema, revision) = self._run_action(
            "deposit.create.class", [name, props], post_snapshot=True
        )
        return DClass(self, data, schema, snapshot_revision=revision)

    def create_object(self, class_selector: str | dict[str, Any] | DClass, properties: dict[str, Any] | None = None, *,
                      descriptors: Mapping[str, Any] | None = None, position=None) -> DObject:
        class_handle = class_selector if isinstance(class_selector, DClass) else self.get_class_handle(class_selector)
        self._validate_handle(class_handle, "Class")
        selector = {"id": class_handle.id}
        if class_handle.stale:
            # Creation is a new request whose explicitly supplied values are
            # validated against the current checked Class schema.  Do not use
            # a stale cached declaration as the encoding authority.
            _class_data, class_schema, class_revision = self._checked_entity_snapshot(
                "Class", {"id": class_handle.id}
            )
        else:
            class_schema = class_handle._schema
            class_revision = class_handle._baseline_revision
        if properties is not None and not isinstance(properties, dict):
            raise TypeError("Object properties must be a dictionary")
        object_properties = dict(properties or {})
        if descriptors is not None:
            if not isinstance(descriptors, Mapping):
                raise TypeError("Object descriptors must be a mapping")
            if "descriptors" in object_properties:
                raise ValueError("Supply descriptors either in properties or as a keyword")
            object_properties["descriptors"] = dict(descriptors)
        if position is not None:
            object_properties["position"] = list(_position_value(position))
        props = _encode_object_properties(
            object_properties, class_schema, geometry_encoder=self._geometry_write_value,
            resource_encoder=self._resource_write_value,
        )
        created, (data, schema, revision) = self._run_action(
            "deposit.create.object", [selector, props], pinned_revision=class_revision,
            stale_handle=class_handle, post_snapshot=True,
        )
        return DObject(self, data, schema, snapshot_revision=revision)

    def delete_class(self, dclass: DClass | str | dict[str, Any]):
        handle = dclass if isinstance(dclass, DClass) else self.get_class_handle(dclass)
        self._validate_handle(handle, "Class")
        return self._run_handle_action(handle, "deposit.delete.class", [{"id": handle.id}])

    def delete_object(self, dobject: DObject | str | dict[str, Any]):
        handle = dobject if isinstance(dobject, DObject) else self.get_object_handle(dobject)
        self._validate_handle(handle, "Object")
        return self._run_handle_action(handle, "deposit.delete.object", [handle.id])

    def create_descriptor(self, dobject: DObject, name: str, value: Any):
        self._validate_handle(dobject, "Object")
        if not isinstance(name, str) or not name:
            raise ValueError("Descriptor name must be a nonempty string")
        payload = self._prepare_descriptor_payload(value, dobject._schema.get(name))
        result = self._run_handle_action(dobject, "deposit.create.descriptor", [dobject.id, name, payload])
        self._refresh_handle_snapshot(dobject)
        return result

    def delete_descriptor(self, dobject: DObject, name: str):
        self._validate_handle(dobject, "Object")
        return self._delete_descriptor(dobject, name)

    def delete_class_descriptor(self, dclass: DClass, name: str):
        self._validate_handle(dclass, "Class")
        return self._delete_descriptor(dclass, name)

    def get_class(self, selection: str | dict[str, Any]) -> dict[str, Any]:
        if isinstance(selection, DNode):
            self._validate_handle(selection, "Class")
        return self._read_entity("deposit.get.class", _selector(selection, "Class"))

    def get_object(self, selection: str | dict[str, Any]) -> dict[str, Any]:
        if isinstance(selection, DNode):
            self._validate_handle(selection, "Object")
        return self._read_entity("deposit.get.object", _selector(selection, "Object"))

    def get_class_handle(self, selection: str | dict[str, Any] | DClass) -> DClass:
        identity = self._handle_id_or_selector(selection, "Class")
        data, schema, revision = self._checked_entity_snapshot("Class", identity)
        return DClass(self, data, schema, snapshot_revision=revision)

    def get_object_handle(self, selection: str | dict[str, Any] | DObject) -> DObject:
        identity = self._handle_id_or_selector(selection, "Object")
        data, schema, revision = self._checked_entity_snapshot("Object", identity)
        return DObject(self, data, schema, snapshot_revision=revision)

    def _handle_id_or_selector(self, selection, kind: str):
        if isinstance(selection, DNode):
            self._validate_handle(selection, kind)
            return {"id": selection.id}
        if kind == "Class":
            return _selector(selection, kind)
        if isinstance(selection, str) and selection:
            return selection
        if isinstance(selection, dict) and set(selection) == {"id"} and isinstance(selection["id"], str):
            return selection
        raise TypeError("Object selector must be a stable UUID or an Object handle")

    def _validate_handle(self, handle, kind: str | None = None) -> None:
        if not isinstance(handle, DNode) or handle._store is not self:
            raise ValueError("Entity handle belongs to another Store")
        if handle.namespace_id != self._namespace_id or (kind is not None and handle.kind != kind):
            raise ValueError("Entity handle has the wrong Store Namespace or kind")

    def _set_descriptor(self, dobject: DObject, name: str, value: Any) -> dict[str, Any]:
        return self.set_descriptor(dobject, name, value)

    def _register_handle(self, handle) -> None:
        self._handles[id(handle)] = handle

    def _invalidate_handles(self, *, except_handle=None) -> None:
        for handle in tuple(self._handles.values()):
            if handle is not except_handle:
                handle._mark_stale()

    def _checked_entity_snapshot(self, kind: str, selection, *, _already_serialized: bool = False,
                                 expected_revision: int | None = None):
        if kind == "Class":
            calls = (
                "CALL deposit.get.classPage($selection, {pageSize: 1}, $token) YIELD data AS page "
                "WITH page.items[0] AS entity "
            )
            projection = "{entity: entity, schema: entity}"
        elif kind == "Object":
            calls = (
                "CALL deposit.get.objectPage($selection, {pageSize: 1}, $token) YIELD data AS page "
                "WITH page.items[0] AS entity "
                "CALL deposit.get.classPage({id: entity.classId}, {pageSize: 1}, $token) YIELD data AS classPage "
            )
            projection = "{entity: entity, schema: classPage.items[0]}"
        else:
            raise ValueError("Only Class and Object snapshots are supported")
        rows, revision = self._checked_read(
            calls, projection, {"selection": selection}, expected_revision=expected_revision,
            _already_serialized=_already_serialized,
        )
        if len(rows) != 1 or not isinstance(rows[0], dict):
            raise DepositError("INVALID_RESPONSE", "Checked entity snapshot returned no single record")
        entity, schema = rows[0].get("entity"), rows[0].get("schema")
        if not isinstance(entity, dict) or not isinstance(schema, dict):
            raise DepositError("INVALID_RESPONSE", "Checked entity snapshot returned invalid data")
        if entity.get("namespaceId") != self._namespace_id or schema.get("namespaceId") != self._namespace_id:
            raise DepositError("SCOPE_MISMATCH", "Snapshot belongs to another Store Namespace")
        if kind == "Class":
            return entity, _schema_from_class_data(entity), revision
        if schema.get("id") != entity.get("classId"):
            raise DepositError("INVALID_RESPONSE", "Object snapshot returned another Class schema")
        from .entities import decode_snapshot
        decoded = decode_snapshot(entity, schema, self._namespace_id, self, snapshot_revision=revision)
        return decoded._snapshot, decoded._schema, revision

    @staticmethod
    def _require_revision(revision: int, expected_revision: int | None) -> None:
        if expected_revision is not None and revision != expected_revision:
            raise DepositError("REVISION_CONFLICT", "Namespace changed during checked iteration or snapshot read")

    def _checked_read(self, calls: str, projection: str, parameters, *, expected_revision=None,
                      _already_serialized=False):
        self._assert_not_collecting_batch()
        # The ungrouped collect consumes all portable results before the end
        # revision call and emits one row even when the subquery has no results.
        query = (
            "CYPHER 25 CALL deposit.get.revision($token) YIELD data AS start "
            "CALL (start) { WITH start WHERE $expected IS NULL OR start.sequence = $expected "
            f"{calls} RETURN collect({projection}) AS rows }} "
            "CALL deposit.get.revision($token) YIELD data AS end "
            "RETURN start, rows, end"
        )
        parameters = {**parameters, "token": self._token, "expected": expected_revision}
        with (nullcontext() if _already_serialized else self._serialized()):
            session = None
            transaction = None
            try:
                session = self._client._session()
                transaction = session.begin_transaction()
                result = transaction.run(query, parameters)
                if tuple(result.keys()) != ("start", "rows", "end"):
                    raise DepositError("INVALID_RESPONSE", "Checked read returned unexpected columns")
                record = result.single(strict=True)
                result.consume()
                if record is None or not isinstance(record.get("rows"), list):
                    raise DepositError("INVALID_RESPONSE", "Checked read returned an invalid result")
                start, end = record.get("start"), record.get("end")
                first = start.get("sequence") if isinstance(start, dict) else None
                last = end.get("sequence") if isinstance(end, dict) else None
                if type(first) is not int or first < 0 or type(last) is not int or last < 0:
                    raise DepositError("INVALID_RESPONSE", "Read returned no checked Namespace revision")
                self._require_revision(first, expected_revision)
                self._require_revision(last, first)
                rows = _portable_input(record["rows"])
                transaction.commit()
                return rows, first
            except BaseException as exc:
                if transaction is not None:
                    try: transaction.rollback()
                    except Exception: pass
                if isinstance(exc, DepositError) or not isinstance(exc, Exception):
                    raise
                raise map_driver_error(exc) from exc
            finally:
                if session is not None:
                    try: session.close()
                    except Exception: pass

    def _checked_entity_page(self, procedure: str, selection, options, *, expected_revision=None):
        entry = store_procedure(procedure)
        if entry["action"] or tuple(entry["columns"]) != ("data",) or not procedure.endswith("Page"):
            raise ValueError("Checked pages require a read-only Domain Page procedure")
        bound = bind_arguments(entry, (selection, options), namespace_id=self._namespace_id)
        query, parameters = build_query(entry, bound, self._token)
        call = query.removeprefix("CYPHER 25 ").removesuffix(" RETURN data")
        rows, revision = self._checked_read(call, "data", parameters, expected_revision=expected_revision)
        if len(rows) != 1:
            raise DepositError("INVALID_RESPONSE", f"{procedure} returned no single page")
        return rows[0], revision

    def _checked_page_entities(self, kind: str, identities, revision: int):
        if not identities:
            return {}
        procedure = "deposit.get.classPage" if kind == "Class" else "deposit.get.objectPage"
        calls = (
            "UNWIND $ids AS id "
            f"CALL {procedure}({{id: id}}, {{pageSize: 1}}, $token) YIELD data AS page "
        )
        rows, _ = self._checked_read(calls, "page.items[0]", {"ids": list(identities)},
                                     expected_revision=revision)
        entities = {}
        for row in rows:
            if not isinstance(row, dict) or row.get("kind") != kind:
                raise DepositError("INVALID_RESPONSE", f"Expected portable {kind} data")
            if row.get("namespaceId") != self._namespace_id:
                raise DepositError("SCOPE_MISMATCH", "Page entity belongs to another Store Namespace")
            entities[_item_id(row)] = row
        if len(rows) != len(identities) or entities.keys() != set(identities):
            raise DepositError("INVALID_RESPONSE", "Checked read returned unexpected entity identities")
        return entities

    def _class_page_handles(self, items, revision):
        handles = []
        for item in items:
            _item_id(item)
            handles.append(DClass(self, item, _schema_from_class_data(item), snapshot_revision=revision))
        return handles

    def _object_page_handles(self, items, revision, schemas, known_class=None):
        from .entities import decode_snapshot
        class_ids = {}
        for item in items:
            _item_id(item)
            class_id = item.get("classId")
            if item.get("kind") != "Object" or not isinstance(class_id, str) or not class_id:
                raise DepositError("INVALID_RESPONSE", "Object page omitted Class identity")
            if item.get("namespaceId") != self._namespace_id:
                raise DepositError("SCOPE_MISMATCH", "Page entity belongs to another Store Namespace")
            class_ids[class_id] = None
        if known_class is not None:
            snapshot = known_class._snapshot
            if (snapshot is not None and not known_class.stale and known_class._baseline_revision == revision
                    and snapshot is known_class._snapshot):
                schemas[known_class.id] = snapshot
        missing = [class_id for class_id in class_ids if class_id not in schemas]
        schemas.update(self._checked_page_entities("Class", missing, revision))
        handles = [decode_snapshot(item, schemas[item["classId"]], self._namespace_id, self,
                                   snapshot_revision=revision) for item in items]
        # Only retain schemas used by the latest page. A single iterator pins its revision.
        used = {class_id: schemas[class_id] for class_id in class_ids}
        schemas.clear()
        schemas.update(used)
        return handles

    def set_descriptor(self, dobject: DObject, name: str, value: Any) -> dict[str, Any]:
        self._validate_handle(dobject, "Object")
        if not isinstance(name, str) or not name:
            raise ValueError("Descriptor name must be a nonempty string")
        payload = self._prepare_descriptor_payload(value, dobject._schema.get(name))
        return self._submit_descriptor_payload(dobject, name, payload)

    def _prepare_descriptor_payload(self, value: Any, declared: str | None) -> Any:
        return _descriptor_payload(
            value, declared, geometry_encoder=self._geometry_write_value,
            resource_encoder=self._resource_write_value,
        )

    def _submit_descriptor_payload(self, dobject: DObject, name: str, payload: Any) -> dict[str, Any]:
        self._validate_handle(dobject, "Object")
        if not isinstance(name, str) or not name:
            raise ValueError("Descriptor name must be a nonempty string")
        result = self._run_handle_action(dobject, "deposit.set.descriptor", [dobject.id, name, payload])
        data, schema, revision = self._checked_entity_snapshot("Object", {"id": dobject.id})
        dobject._snapshot, dobject._schema = data, schema
        dobject._stale = False
        dobject._baseline_revision = revision
        return result

    def _geometry_write_value(self, value: Any) -> dict[str, Any]:
        from .values import DGeometry, TypedValue
        if isinstance(value, TypedValue):
            value = value.value
        if not isinstance(value, DGeometry):
            if isinstance(value, Mapping) and value.get("type") == "DGeometry":
                value = DGeometry(from_dict=value)
            elif isinstance(value, Mapping):
                value = DGeometry(from_dict={"type": "DGeometry", "value": dict(value)})
            else:
                raise TypeError("DGeometry assignments require a DGeometry value")
        value.resolve(self)
        # Identity and cached coordinates remain local. Server owns and updates
        # the geometry attached to the destination Descriptor.
        return {key: value._value[key] for key in
                ("wkt", "srid", "verticalSrid", "axisOrder", "dimensions")}

    @staticmethod
    def _resource_write_value(value: Any) -> dict[str, str]:
        from .values import DResource, TypedValue
        if isinstance(value, TypedValue):
            value = value.value
        if isinstance(value, DResource):
            value = value._value
        if isinstance(value, Mapping) and value.get("type") == "DResource":
            value = value.get("value")
        if not isinstance(value, Mapping):
            raise TypeError("DResource assignments require a ready resource reference")
        resource_id = value.get("resourceId")
        if not isinstance(resource_id, str) or not resource_id:
            raise ValueError("Upload the resource before assigning it to a Descriptor")
        # Filename and image metadata are local/display fields. The Server
        # accepts only the resource identity for Descriptor assignment.
        return {"resourceId": resource_id}

    def _delete_descriptor(self, owner: DNode, name: str) -> dict[str, Any]:
        if not isinstance(owner, DNode) or owner._store is not self or owner.namespace_id != self._namespace_id:
            raise ValueError("Entity handle belongs to another Store")
        if owner.kind == "Object":
            procedure = "deposit.delete.descriptor"
            selector = {"id": self._descriptor_id(owner, name)}
        elif owner.kind == "Class":
            procedure = "deposit.delete.classDescriptor"
            selector = {"id": self._descriptor_id(owner, name)}
        else:
            raise TypeError("Only Class or Object Descriptor values can be deleted")
        result = self._run_handle_action(owner, procedure, [selector])
        self._refresh_handle_snapshot(owner)
        return result

    def create_class_descriptor(self, dclass: DClass, name: str, type_name: str, comment: str = "") -> dict[str, Any]:
        self._validate_handle(dclass, "Class")
        if not isinstance(name, str) or not name or not isinstance(type_name, str) or not type_name:
            raise ValueError("Descriptor name and type must be nonempty strings")
        result = self._run_handle_action(dclass, "deposit.create.classDescriptor", [{"id": dclass.id}, name, type_name, {"comment": comment}])
        data, schema, revision = self._checked_entity_snapshot("Class", {"id": dclass.id})
        dclass._snapshot, dclass._schema = data, schema
        dclass._baseline_revision = revision
        dclass._stale = False
        return result

    def set_class_descriptor(self, selector: dict[str, Any], patch: dict[str, Any], *,
                             dclass: DClass | None = None) -> dict[str, Any]:
        selector_dclass = selector if isinstance(selector, DClass) else None
        owner = dclass or selector_dclass
        if selector_dclass is not None:
            self._validate_handle(selector_dclass, "Class")
            name = patch.get("name") if isinstance(patch, dict) else None
            declaration = selector_dclass.declarations.get(name) if isinstance(name, str) else None
            if declaration is None:
                raise KeyError(name)
            selector = {"id": declaration["id"]}
        if owner is not None:
            self._validate_handle(owner, "Class")
            descriptor_id = selector.get("id") if isinstance(selector, Mapping) else None
            if descriptor_id is not None and descriptor_id not in {
                item.get("id") for item in owner.declarations.values()
            }:
                raise ValueError("Class Descriptor selector does not belong to the supplied Class handle")
        if not isinstance(selector, dict) or not isinstance(patch, dict):
            raise TypeError("selector and patch must be dictionaries")
        result = (self._run_handle_action(owner, "deposit.set.classDescriptor", [selector, _portable_input(patch)])
                  if owner is not None else self._run_action("deposit.set.classDescriptor", [selector, _portable_input(patch)]))
        if owner is not None:
            self._refresh_handle_snapshot(owner)
        return result

    def set_class_properties(self, dclass: DClass, **patch) -> dict[str, Any]:
        self._validate_handle(dclass, "Class")
        allowed = {"name", "position", "order", "comment"}
        if not patch or set(patch) - allowed:
            raise ValueError("Class patch must contain supported fields")
        if "name" in patch and (not isinstance(patch["name"], str) or not patch["name"]):
            raise ValueError("Class name must be a nonempty string")
        if "comment" in patch and not isinstance(patch["comment"], str):
            raise TypeError("Class comment must be a string")
        if "order" in patch and (type(patch["order"]) is not int or patch["order"] < 0):
            raise ValueError("Class order must be a nonnegative integer")
        if "position" in patch:
            patch["position"] = _position_value(patch["position"])
        result = self._run_handle_action(dclass, "deposit.set.class", [{"id": dclass.id}, _portable_input(patch)])
        self._refresh_handle_snapshot(dclass)
        return result

    def set_position(self, node: DNode, x: float, y: float) -> dict[str, Any]:
        self._validate_handle(node)
        position = _position_value((x, y))
        if node.kind == "Class":
            procedure, selector = "deposit.set.class", {"id": node.id}
        elif node.kind == "Object":
            procedure, selector = "deposit.set.object", node.id
        else:
            raise TypeError("Only Class and Object handles have a position")
        result = self._run_handle_action(node, procedure, [selector, {"position": list(position)}])
        self._refresh_handle_snapshot(node)
        return result

    def change_class_descriptor_type(self, dclass: DClass, name: str, type_name: str, *, conversion: str):
        self._validate_handle(dclass, "Class")
        declaration = dclass.declarations.get(name)
        if declaration is None:
            raise KeyError(name)
        if not isinstance(type_name, str) or not type_name:
            raise ValueError("type_name must be a nonempty string")
        if conversion not in {"identity", "integerToFloat", "floatToIntegerExact"}:
            raise ValueError("Unsupported class Descriptor conversion")
        result = self._run_handle_action(dclass, "deposit.set.classDescriptor", [
            {"id": declaration["id"]}, {"type": type_name, "conversion": conversion},
        ])
        self._refresh_handle_snapshot(dclass)
        return result

    def switch_descriptor_order(self, dclass: DClass, name1: str, name2: str):
        self._validate_handle(dclass, "Class")
        first, second = dclass.declarations.get(name1), dclass.declarations.get(name2)
        if first is None or second is None:
            raise KeyError(name1 if first is None else name2)
        if first["id"] == second["id"]:
            return {"changed": False}
        with self.transaction() as batch:
            batch._pin_handle(dclass, "Class")
            batch.call("deposit.set.classDescriptor", {"id": first["id"]}, {"order": second.get("order", 0)})
            batch.call("deposit.set.classDescriptor", {"id": second["id"]}, {"order": first.get("order", 0)})
        self._refresh_handle_snapshot(dclass)
        return {"changed": True}

    def switch_class_order(self, first: DClass, second: DClass):
        self._validate_handle(first, "Class")
        self._validate_handle(second, "Class")
        if first.id == second.id:
            return {"changed": False}
        if first._baseline_revision != second._baseline_revision:
            raise ValueError("Class handles have conflicting checked snapshot revisions")
        with self.transaction() as batch:
            batch._pin_handle(first, "Class")
            batch._pin_handle(second, "Class")
            batch.call("deposit.set.class", {"id": first.id}, {"order": second.order})
            batch.call("deposit.set.class", {"id": second.id}, {"order": first.order})
        self._refresh_handle_snapshot(first)
        self._refresh_handle_snapshot(second)
        return {"changed": True}

    def set_membership(self, dobject: DObject, dclass: DClass | str | dict[str, Any]):
        self._validate_handle(dobject, "Object")
        target = dclass if isinstance(dclass, DClass) else self.get_class_handle(dclass)
        self._validate_handle(target, "Class")
        revision = _matching_handle_revision(dobject, target)
        try:
            result = self._run_action("deposit.set.membership", [dobject.id, {"id": target.id}],
                                      pinned_revision=revision)
        except DepositError as exc:
            if exc.code == "REVISION_CONFLICT":
                dobject._mark_stale()
                target._mark_stale()
            raise
        self._refresh_handle_snapshot(dobject)
        return result

    def object_class(self, dobject: DObject) -> DClass:
        self._validate_handle(dobject, "Object")
        data = self._call_one("deposit.get.objectClass", dobject.id)
        if not isinstance(data, dict) or not isinstance(data.get("id"), str):
            raise DepositError("INVALID_RESPONSE", "Object class lookup returned no Class identity")
        return self.get_class_handle({"id": data["id"]})

    def create_relationship(self, src_object: DObject, tgt_object: DObject, label: str, weight=None):
        self._validate_handle(src_object, "Object")
        self._validate_handle(tgt_object, "Object")
        if not isinstance(label, str) or not label:
            raise ValueError("Relationship label must be a nonempty string")
        revision = _matching_handle_revision(src_object, tgt_object)
        properties = {} if weight is None else {"weight": weight}
        return self._run_multi_handle_action("deposit.create.relationship",
                                             [src_object.id, tgt_object.id, label, properties],
                                             revision, (src_object, tgt_object))

    def create_class_relationship(self, src_class: DClass, tgt_class: DClass, label: str, comment=""):
        self._validate_handle(src_class, "Class")
        self._validate_handle(tgt_class, "Class")
        if not isinstance(label, str) or not label or not isinstance(comment, str):
            raise ValueError("Class relationship label and comment must be strings")
        revision = _matching_handle_revision(src_class, tgt_class)
        return self._run_multi_handle_action(
            "deposit.create.classRelationship", [
                {"id": src_class.id}, {"id": tgt_class.id}, label, {"comment": comment},
            ], revision, (src_class, tgt_class),
        )

    def set_relationship(self, relationship_id: str, **patch):
        if not isinstance(relationship_id, str) or not relationship_id:
            raise ValueError("relationship_id must be a nonempty stable ID")
        if not patch:
            raise ValueError("Relationship patch must not be empty")
        return self._run_action("deposit.set.relationship", [relationship_id, _portable_input(patch)])

    def set_class_relationship(self, relationship_id: str, **patch):
        if not isinstance(relationship_id, str) or not relationship_id:
            raise ValueError("relationship_id must be a nonempty stable ID")
        if not patch:
            raise ValueError("Class relationship patch must not be empty")
        return self._run_action("deposit.set.classRelationship", [relationship_id, _portable_input(patch)])

    def delete_relationship(self, relationship_id: str):
        return self._run_action("deposit.delete.relationship", [_stable_id(relationship_id)])

    def delete_class_relationship(self, relationship_id: str):
        return self._run_action("deposit.delete.classRelationship", [_stable_id(relationship_id)])

    def delete_all(self, confirm_namespace_name: str):
        if not isinstance(confirm_namespace_name, str) or not confirm_namespace_name:
            raise ValueError("Namespace name confirmation must be a nonempty string")
        return self._run_action("deposit.delete.all", [confirm_namespace_name])

    def get_relationship(self, selection: str | dict[str, Any]):
        return self._call_one("deposit.get.relationship", selection)

    def get_class_relationship(self, selection: str | dict[str, Any]):
        return self._call_one("deposit.get.classRelationship", selection)

    def create_user_tool(self, name: str, data: str, comment: str = ""):
        if not isinstance(name, str) or not name or not isinstance(data, str) or not isinstance(comment, str):
            raise ValueError("User Tool name, data, and comment must be strings")
        return self._run_action("deposit.create.userTool", [name, {"data": data, "comment": comment}])

    def set_user_tool(self, user_tool_id: str, **patch):
        return self._run_action("deposit.set.userTool", [{"id": _stable_id(user_tool_id)}, _portable_input(patch)])

    def delete_user_tool(self, user_tool_id: str):
        return self._run_action("deposit.delete.userTool", [{"id": _stable_id(user_tool_id)}])

    def upload_resource(self, resource: DResource) -> DResource:
        """Upload a local resource once, consuming a borrowed stream from its current position."""
        self._assert_not_collecting_batch()
        if not isinstance(resource, DResource):
            raise TypeError("resource must be a DResource")
        if resource.resource_id is not None and resource._path is None and resource._stream is None:
            if resource._value.get("namespaceId") not in (None, self.namespace_id):
                raise DepositError("SCOPE_MISMATCH", "Resource belongs to another Namespace")
            return resource
        with self._serialized(), self._using_endpoint() as endpoint:
            return upload_resource(endpoint, resource)

    def _open_resource(self, resource: DResource):
        self._assert_not_collecting_batch()
        if not isinstance(resource, DResource) or not resource.resource_id:
            raise ValueError("resource must be a stored DResource reference")
        if resource._value.get("namespaceId") not in (None, self._namespace_id):
            raise DepositError("SCOPE_MISMATCH", "Resource belongs to another Namespace")
        with self._serialized():
            metadata = self._endpoint.call("deposit.resource.info", resource.resource_id)
            if not isinstance(metadata, dict) or metadata.get("resourceId") != resource.resource_id:
                raise DepositError("INVALID_RESPONSE", "Resource metadata did not match the selected resource")
            resource._value.update(metadata)
            reader = ResourceReader(self._read_resource_chunk, resource.resource_id, metadata)
            self._readers[id(reader)] = reader
            return reader

    def _read_resource_chunk(self, procedure, *arguments):
        self._check_result_reader()
        return self._endpoint.control(procedure, *arguments)

    def _open_import_resource(self, record, *, read):
        """Check the exact source resource even when another ID shares its bytes."""
        self._check_result_reader()
        resource_id = _stable_id(record.get("id"))
        metadata = self._endpoint.control("deposit.resource.info", resource_id)
        if (not isinstance(metadata, dict) or metadata.get("resourceId") != resource_id
                or metadata.get("bytes") != record.get("bytes")
                or record.get("entry") != f"resources/{metadata.get('checksum')}"):
            raise DepositError("RESOURCE_CORRUPT", "Source resource differs from the snapshot")
        if not read:
            return None
        return ResourceReader(self._read_resource_chunk, resource_id, metadata)

    def create_pattern(self, name: str, definition: Mapping[str, Any],
                       properties: Mapping[str, Any] | None = None) -> dict[str, Any]:
        """Create a Pattern and return the server-authored portable entity."""
        if not isinstance(name, str) or not name:
            raise ValueError("Pattern name must be a nonempty string")
        if not isinstance(definition, Mapping):
            raise TypeError("Pattern definition must be a mapping")
        if properties is not None and not isinstance(properties, Mapping):
            raise TypeError("Pattern properties must be a mapping")
        outcome = self._run_action(
            "deposit.create.pattern",
            [name, _portable_input(dict(definition)), _portable_input(dict(properties or {}))],
        )
        if not isinstance(outcome, dict):
            raise DepositError("INVALID_RESPONSE", "Pattern creation returned no portable entity")
        return outcome

    def get_pattern(self, selection: str | Mapping[str, Any]) -> dict[str, Any]:
        """Read one Pattern using a server-supported name or ID selector."""
        return self._read_entity("deposit.get.pattern", _pattern_selector(selection))

    def set_pattern(self, pattern: str | Mapping[str, Any], **patch: Any) -> dict[str, Any]:
        """Apply a Pattern patch and preserve the server's returned definition."""
        if not patch:
            raise ValueError("Pattern patch cannot be empty")
        return self._run_action(
            "deposit.set.pattern", [_pattern_selector(pattern), _portable_input(patch)]
        )

    def delete_pattern(self, pattern: str | Mapping[str, Any]) -> Any:
        """Delete one Pattern through the namespace-scoped server procedure."""
        return self._run_action("deposit.delete.pattern", [_pattern_selector(pattern)])

    def patterns(self, name: str | None = None, *, page_size: int = 200):
        """Iterate server-paged Pattern records without interpreting definitions."""
        if name is not None and (not isinstance(name, str) or not name):
            raise ValueError("Pattern name filter must be a nonempty string")
        return self._entity_page(
            "deposit.get.patternPage", name, page_size=page_size,
            transform=lambda item, _revision: item,
        )

    def objects_by_pattern(
        self,
        pattern: Mapping[str, Any],
        values: Mapping[str, Any] | None = None,
        rules: Mapping[str, Any] | None = None,
        *,
        page_size: int = 200,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        """Request one server-matched Pattern page using an ID-only selector."""
        if not isinstance(pattern, Mapping):
            raise TypeError("Pattern search requires an {id: UUID} selector mapping")
        if values is not None and not isinstance(values, Mapping):
            raise TypeError("Pattern values must be a mapping")
        if rules is not None and not isinstance(rules, Mapping):
            raise TypeError("Pattern rules must be a mapping")
        if type(page_size) is not int or not 1 <= page_size <= 1000:
            raise ValueError("page_size must be an integer from 1 through 1000")
        if cursor is not None and (not isinstance(cursor, str) or not cursor):
            raise ValueError("cursor must be a nonempty string")
        page = {"pageSize": page_size}
        if cursor is not None:
            page["cursor"] = cursor
        result = self._call_one(
            "deposit.get.objectsByPatternPage",
            _pattern_id_selector(pattern),
            _portable_input(dict(values or {})),
            _portable_input(dict(rules or {})),
            page,
        )
        if not isinstance(result, dict):
            raise DepositError("INVALID_RESPONSE", "Pattern search returned no portable page")
        return result

    def import_data_by_pattern(
        self,
        pattern: Mapping[str, Any],
        values: Mapping[str, Any],
        rules: Mapping[str, Any] | None = None,
        *, operation_id=None,
    ):
        """Submit form data for an ID-selected Pattern through the server workflow."""
        if not isinstance(pattern, Mapping) or not isinstance(values, Mapping):
            raise TypeError("Pattern must be an {id: UUID} selector and form values must be a mapping")
        if rules is not None and not isinstance(rules, Mapping):
            raise TypeError("Pattern rules must be a mapping")
        return self._analyze("deposit.import.dataByPattern", [
            _portable_input(dict(pattern)), _portable_input(dict(values)), _import_rules(rules),
        ], operation_id=operation_id)

    def get_layout(
        self, selection: Mapping[str, Any], options: Mapping[str, Any] | None = None
    ) -> dict[str, Any]:
        """Return the server-computed graph layout for the selected entities."""
        if not isinstance(selection, Mapping):
            raise TypeError("Layout selection must be a mapping")
        if options is not None and not isinstance(options, Mapping):
            raise TypeError("Layout options must be a mapping")
        result = self._call_one(
            "deposit.get.layout",
            _portable_input(dict(selection)),
            _portable_input(dict(options or {})),
        )
        if not isinstance(result, dict):
            raise DepositError("INVALID_RESPONSE", "Server returned no layout result")
        positions = result.get("positions")
        if not isinstance(positions, dict):
            raise DepositError("INVALID_RESPONSE", "Server returned an invalid layout result")
        return positions

    def get_subtree(
        self, roots=None, *, max_hops: int | None = None, root_class_boundary: bool = True
    ) -> dict[str, Any]:
        """Return a revision-pinned snapshot of the current scientific graph."""
        self._assert_not_collecting_batch()
        if roots is not None and not isinstance(roots, (str, bytes)):
            roots = list(roots)
        roots, options = self._export_arguments(roots, max_hops, root_class_boundary)
        options["includeHistory"] = False
        with self._serialized(), self._using_endpoint() as endpoint:
            with endpoint.execution() as execution:
                data = execution.run("deposit.get.data", roots, options)
            if isinstance(data, dict) and "manifest" in data and "graph" in data:
                return {"manifest": data["manifest"], "graph": data["graph"]}
            from ._exchange import materialize_snapshot
            try:
                return materialize_snapshot(ExchangeBundle(endpoint, data))
            finally:
                discard_package(endpoint, data)

    def to_networkx(
        self, roots=None, *, max_hops: int | None = None, root_class_boundary: bool = True
    ):
        """Export the selected snapshot as an optional NetworkX MultiDiGraph."""
        from .networkx import require_networkx, to_networkx
        require_networkx()
        return to_networkx(self.get_subtree(
            roots, max_hops=max_hops, root_class_boundary=root_class_boundary,
        ))

    def from_networkx(self, graph, *, resources=None, resource_source=None, rules=None, operation_id=None):
        """Analyze a Deposit MultiDiGraph using Server-owned intake."""
        from .networkx import from_networkx
        return self.from_dict(from_networkx(graph), resources=resources, resource_source=resource_source,
                              rules=rules, operation_id=operation_id)

    def import_rows(self, rows, relationships=None, *, targets=None, rules=None, operation_id=None):
        """Analyze tabular input, leaving matching and plan validation to the Server."""
        frozen_rows = _freeze_import_rows(rows)
        if relationships is None:
            frozen_relationships = []
        else:
            if not isinstance(relationships, (list, tuple)):
                raise TypeError("relationships must be a list of relationship records")
            frozen_relationships = []
            for relationship in relationships:
                if not isinstance(relationship, Mapping):
                    raise TypeError("relationships must contain mapping records")
                frozen_relationships.append(dict(relationship))
        if targets is not None and not isinstance(targets, Mapping):
            raise TypeError("targets must be a mapping")
        return self._analyze("deposit.import.rows", [
            _portable_input({"rows": frozen_rows}), _portable_input(dict(targets or {})),
            _portable_input(frozen_relationships), _import_rules(rules),
        ], operation_id=operation_id)

    def import_subtree(self, subtree, *, resources=None, resource_source=None, rules=None, operation_id=None):
        """Analyze a public subgraph with optional resource content."""
        return self.from_dict(subtree, resources=resources, resource_source=resource_source,
                              rules=rules, operation_id=operation_id)

    def from_dict(self, data, *, resources=None, resource_source=None, rules=None, operation_id=None):
        """Analyze a public graph and its resource providers without applying a plan."""
        if not isinstance(data, Mapping):
            raise TypeError("Import snapshot must be a manifest/graph mapping")
        return self._analyze_package(
            lambda endpoint: stage_snapshot(endpoint, data, resources=resources, resource_source=resource_source),
            rules, operation_id=operation_id,
        )

    def _export_arguments(self, roots, max_hops, root_class_boundary):
        selection = None
        if roots is not None:
            if not isinstance(roots, (list, tuple)):
                raise TypeError("roots must be a list or tuple of Object UUIDs")
            if len(roots) > (1_048_576 - 1024) // 39:
                raise DepositError("LIMIT_EXCEEDED", "Explicit export selector exceeds request budget")
            selection = []
            for root in roots:
                if isinstance(root, DObject):
                    self._validate_handle(root, "Object")
                    root = root.id
                if isinstance(root, Mapping) and set(root) == {"id"}:
                    root = root["id"]
                root = _stable_id(root)
                if len(root) != 36 or str(uuid.UUID(root)) != root.lower():
                    raise ValueError("Explicit export roots must be Object UUIDs")
                selection.append(root)
        options = {"rootClassBoundary": root_class_boundary}
        if max_hops is not None:
            options["maxHops"] = max_hops
        return selection, options

    def export_exchange(self, *, roots=None, max_hops=None, root_class_boundary=True, operation_id=None):
        """Export a canonical Server package and return its owning bundle."""
        self._assert_not_collecting_batch()
        selection, options = self._export_arguments(roots, max_hops, root_class_boundary)
        with self._serialized():
            endpoint = OperationEndpoint(self._client, self._token, self.namespace_id,
                                         expected=self._endpoint.binding, actor=self._actor_id)
            try:
                with self._using_endpoint(endpoint), endpoint.execution() as execution:
                    endpoint.require_exchange()
                    reference = execution.run("deposit.exchange.export", selection, options, operation_id=operation_id)
                bundle = ExchangeBundle(endpoint, reference, on_close=self._bundles.discard)
                self._bundles.add(bundle)
                return bundle
            except BaseException:
                endpoint.close()
                raise

    def import_exchange(self, source, *, rules=None, operation_id=None):
        """Analyze ZIP bytes or an authorized Exchange bundle for explicit apply."""
        if isinstance(source, ExchangeBundle):
            return self._analyze_package(lambda endpoint: package_for_target(source, endpoint),
                                         rules, operation_id=operation_id, source=source)
        return self._analyze_package(lambda endpoint: stage_zip(endpoint, source),
                                     rules, operation_id=operation_id)

    def import_from(self, source_store, *, roots=None, max_hops=None,
                    root_class_boundary=True, rules=None, operation_id=None):
        """Analyze a source Store export using independently authenticated Stores."""
        if not isinstance(source_store, Store):
            raise TypeError("source_store must be an authenticated Store")
        _import_rules(rules)
        with source_store.export_exchange(roots=roots, max_hops=max_hops,
                                          root_class_boundary=root_class_boundary) as source:
            return self._analyze_package(lambda endpoint: package_for_target(source, endpoint),
                                         rules, operation_id=operation_id, source=source, own_source=True)

    def _analyze_package(self, stage, rules, *, operation_id, source=None, own_source=False):
        self._assert_not_collecting_batch()
        rules = _import_rules(rules)
        self._client.require_paged_import_results()
        with self._serialized(), self._using_endpoint() as endpoint:
            endpoint.require_exchange()
            reference = stage(endpoint)
            direct = source is not None and reference == source.source
            owned = not direct
            retained = False
            try:
                with endpoint.execution() as execution:
                    envelope = execution.run("deposit.import.analyze", {
                        name: reference[name] for name in ("packageId", "grant")
                    }, rules, operation_id=operation_id)
                result = self._import_result(envelope)
                if result.plan_id:
                    with self._state_lock:
                        self._ready_plans.add(result.plan_id)
                        if owned or own_source:
                            self._plan_packages[result.plan_id] = (reference, direct)
                            retained = True
                elif direct and own_source:
                    # The conflict diagnostic itself still checks source authorization.
                    self._result_packages.append(reference)
                    retained = True
                if direct and own_source and retained:
                    source._detach()
                if result.state == "IMPORT_CONFLICT":
                    self._raise_import_diagnostic(result)
                return result
            except OperationCommittedAfterCancel as exc:
                self._discard_cancelled_analysis(exc)
                raise
            except OutcomeUnknown:
                # Admission may still be using the package. Expiry owns cleanup.
                retained = True
                if direct and own_source:
                    source._detach()
                raise
            finally:
                if owned and not retained:
                    discard_package(endpoint, reference)

    def _analyze(self, procedure, arguments, *, operation_id=None):
        self._assert_not_collecting_batch()
        self._client.require_paged_import_results()
        with self._serialized():
            try:
                result = self._import_result(self._run_action(
                    procedure, arguments, operation_id=operation_id, _already_serialized=True,
                ))
            except OperationCommittedAfterCancel as exc:
                self._discard_cancelled_analysis(exc)
                raise
            if result.plan_id:
                with self._state_lock:
                    self._ready_plans.add(result.plan_id)
            if result.state == "IMPORT_CONFLICT":
                self._raise_import_diagnostic(result)
            return result

    def _discard_cancelled_analysis(self, error):
        summary = error.details.get("result")
        if isinstance(summary, dict) and isinstance(summary.get("planId"), str):
            with suppress(DepositError, TransportError):
                self._discard_plan(summary["planId"])

    @staticmethod
    def _raise_import_diagnostic(result):
        try:
            conflicts = result["conflicts"]
        except KeyError as exc:
            result.close()
            raise DepositError("INVALID_RESPONSE", "Import diagnostic result has no conflicts") from exc
        failure = DepositError(
            "IMPORT_CONFLICT", "Import analysis found conflicts",
            {"conflicts": conflicts}, result.operation_id,
        )
        # Keep the lazy detail reader alive for as long as the exception is
        # retained; callers can close it explicitly after inspecting details.
        failure.import_result = result
        raise failure

    def import_apply(self, plan_id: str, *, operation_id=None):
        """Explicitly apply a ready plan in one Server transaction."""
        _stable_id(plan_id)
        self._client.require_paged_import_results()
        with self._serialized():
            try:
                envelope = self._run_action("deposit.import.apply", [plan_id],
                                            operation_id=operation_id, _already_serialized=True)
            except OutcomeUnknown:
                with self._state_lock:
                    self._ready_plans.discard(plan_id)
                    self._plan_packages.pop(plan_id, None)
                raise
            except OperationCommittedAfterCancel:
                self._finish_plan(plan_id, keep_source=True)
                raise
            except DepositError as exc:
                if isinstance(exc.details, dict) and exc.details.get("state") == "COMMITTED":
                    self._finish_plan(plan_id, keep_source=True)
                elif exc.code != "OPERATION_MISMATCH":
                    with suppress(DepositError, TransportError):
                        self._discard_plan(plan_id)
                raise
            self._finish_plan(plan_id, keep_source=True)
            return self._import_result(envelope)

    def discard_import_plan(self, plan_id: str):
        """Discard an unused plan and report the Server's plan state."""
        self._assert_not_collecting_batch()
        _stable_id(plan_id)
        # Independent of the ordinary lock, so a concurrent apply and discard
        # are ordered by the Server rather than by this Store.
        if not self._begin_parallel_control():
            raise RuntimeError("Store is closed")
        try:
            return self._discard_plan(plan_id)
        finally:
            self._end_parallel_control()

    def _discard_plan(self, plan_id):
        result = self._endpoint.control("deposit.import.discardPlan", plan_id)
        if (not _valid_plan_observation(result) or result["planId"] != plan_id
                or result["namespaceId"] != self.namespace_id):
            raise DepositError("INVALID_RESPONSE", "Plan discard returned an invalid observation")
        self._finish_plan(plan_id, keep_source=result["state"] == "applied")
        return result

    def _finish_plan(self, plan_id, *, keep_source):
        with self._state_lock:
            self._ready_plans.discard(plan_id)
            owned = self._plan_packages.pop(plan_id, None)
            if owned is None:
                return
            reference, direct = owned
            if direct and keep_source:
                self._result_packages.append(reference)
                return
        discard_package(self._endpoint, reference, control=True)

    def import_inspect(self, plan_id: str):
        """Read a plan's immutable analysis result without applying it."""
        _stable_id(plan_id)
        self._client.require_paged_import_results()
        wrapper = self._call_one("deposit.get.importPlan", plan_id)
        if not valid_import_plan_wrapper(wrapper, plan_id=plan_id, namespace_id=self._namespace_id):
            raise DepositError("INVALID_RESPONSE", "Import plan inspection returned an invalid wrapper")
        return self._import_result(wrapper["analysisResult"], plan_observation=wrapper["plan"])

    def _import_result(self, envelope: Any, *, plan_observation=None):
        if not isinstance(envelope, dict) or envelope.get("namespaceId") != self._namespace_id:
            raise DepositError("INVALID_RESPONSE", "Import action returned an invalid result envelope")
        result = self._client._result_view(self._token, envelope, ImportResult,
                                           plan_observation=plan_observation, check_open=self._check_result_reader,
                                           requester=self._endpoint.control)
        self._readers[id(result)] = result
        return result

    def _check_result_reader(self):
        if self._closed or self._closing:
            raise RuntimeError("Store is closed")

    def call(self, procedure: str, *arguments: Any):
        """Call an allowlisted Store procedure using its domain arguments."""
        self._assert_not_collecting_batch()
        entry = store_procedure(procedure)
        if procedure == "deposit.import.apply":
            return self.import_apply(*arguments)
        if procedure == "deposit.import.discardPlan":
            return self.discard_import_plan(*arguments)
        if entry.get("tracked") == "always":
            if procedure in _IMPORT_RESULTS:
                return self._analyze(procedure, arguments)
            outcome = self._run_action(procedure, arguments)
            columns = entry["columns"]
            row = {columns[0]: outcome} if len(columns) == 1 else outcome
            return detached_result(entry, row, self._token)
        bound = bind_arguments(entry, arguments, namespace_id=self._namespace_id)
        return self._open_result(entry, bound)

    def get_requests(self) -> tuple[Mapping[str, Any], ...]:
        """List requests visible to this Store's authenticated actor.

        This control query uses a fresh session and does not wait for the
        Store's ordinary operation lock, so it remains usable while a request
        is running on the Store session.
        """
        self._assert_not_collecting_batch()
        if not self._begin_parallel_control():
            raise RuntimeError("Store is closed")
        try:
            entry = store_control_procedure("deposit.get.requests")
            bound = bind_arguments(entry, ())
            query, parameters = build_query(entry, bound, self._token)
            session = None
            try:
                session = self._client._session()
                result = session.run(query, parameters)
                columns = tuple(result.keys())
                expected = tuple(entry["columns"])
                if columns != expected:
                    raise DepositError("INVALID_RESPONSE", "Request listing returned unexpected columns")
                requests = tuple(immutable_request(record.get("data")) for record in result)
                result.consume()
                return requests
            except DepositError:
                raise
            except Exception as exc:
                raise map_driver_error(exc) from exc
            finally:
                if session is not None:
                    try:
                        session.close()
                    except Exception:
                        pass
        finally:
            self._end_parallel_control()

    def cancel_request(self, request_id: str) -> bool:
        """Request cancellation of one server-authorized active request."""
        self._assert_not_collecting_batch()
        if not isinstance(request_id, str) or not request_id:
            raise ValueError("request_id must be a nonempty string")
        if not self._begin_parallel_control():
            return False
        try:
            entry = store_control_procedure("deposit.utils.cancelRequest")
            bound = bind_arguments(entry, (request_id,))
            query, parameters = build_query(entry, bound, self._token)
            row = self._client._single(query, parameters)
            value = row.get("value")
            if not isinstance(value, bool):
                raise DepositError("INVALID_RESPONSE", "Request cancellation returned no boolean result")
            return value
        finally:
            self._end_parallel_control()

    def get_administrative_mode(self) -> bool:
        """Read this Store session's administrative-mode state."""
        self._assert_not_collecting_batch()
        with self._serialized():
            value = self._client.get_administrative_mode(self._token)
            if not isinstance(value, bool):
                raise DepositError("INVALID_RESPONSE", "Administrative-mode lookup returned no boolean result")
            return value

    def get_operation(self, operation_id: str) -> Any:
        """Inspect an exact operation on the independent control connection."""
        return self._operation_status(operation_id, cancel=False)

    def cancel_operation(self, operation_id: str) -> Any:
        """Request cancellation in this Store's immutable Namespace."""
        return self._operation_status(operation_id, cancel=True)

    def _operation_status(self, operation_id, *, cancel):
        _stable_id(operation_id)
        if not self._begin_parallel_control():
            raise RuntimeError("Store is closed")
        try:
            if cancel:
                with self._state_lock:
                    endpoint = self._active_endpoint
                if endpoint is not None:
                    endpoint.note_cancel(operation_id)
            return self._endpoint._operation_status(operation_id, cancel)
        finally:
            self._end_parallel_control()

    def get_last_operation(self) -> Any:
        """Return the last operation observed by this Store session."""
        self._assert_not_collecting_batch()
        with self._serialized():
            return self._client.get_last_operation(self._token)

    def set_administrative_mode(self, enabled: bool) -> bool:
        """Explicitly set this Store session's administrative-mode state."""
        self._assert_not_collecting_batch()
        if not isinstance(enabled, bool):
            raise TypeError("enabled must be a bool")
        with self._serialized():
            value = self._client.set_administrative_mode(self._token, enabled)
            if not isinstance(value, bool):
                raise DepositError("INVALID_RESPONSE", "Administrative-mode update returned no boolean result")
            return value

    def refresh(self) -> None:
        """Explicitly refresh this session's Namespace revision baseline."""
        self._assert_not_collecting_batch()
        with self._serialized(allow_closing=True):
            self._client._single(
                "CYPHER 25 CALL deposit.utils.refresh($scope, $token) YIELD data RETURN data",
                {"scope": "namespace", "token": self._token},
            )
            self._update_observed_revision()
            self._invalidate_handles()

    def transaction(self):
        """Collect one bounded atomic ``deposit.apply.batch`` action."""
        from ._batch import Batch
        if self._batch_collection is not None:
            raise RuntimeError("Nested batches are not supported")
        return Batch(self)

    def get_query_page(self, querystr: str, *, parameters: dict[str, Any] | None = None,
                       page_size: int | None = None, cursor: str | None = None) -> dict[str, Any]:
        """Return one typed, revision-bound page from the server query engine."""
        from .values import decode_value
        if not isinstance(querystr, str) or not querystr:
            raise ValueError("querystr must be a nonempty string")
        if parameters is None:
            parameters = {}
        if not isinstance(parameters, dict) or any(not isinstance(k, str) for k in parameters):
            raise TypeError("parameters must be a string-keyed dictionary")
        if page_size is not None and (type(page_size) is not int or not 1 <= page_size <= 1000):
            raise ValueError("page_size must be an integer from 1 through 1000")
        if cursor is not None and (not isinstance(cursor, str) or not cursor):
            raise ValueError("cursor must be a nonempty string")
        options = {}
        if page_size is not None:
            options["pageSize"] = page_size
        if cursor is not None:
            options["cursor"] = cursor
        bound_parameters = _query_parameters(parameters)
        with self.call("deposit.get.queryPage", querystr, bound_parameters, options) as result:
            first = next(result, None)
            if first is None:
                raise DepositError("INVALID_RESPONSE", "Query page returned no page record")
            if next(result, None) is not None:
                raise DepositError("INVALID_RESPONSE", "Query page returned multiple page records")
            data = first.get("data")
        if not isinstance(data, dict) or set(data) != {"columns", "rows", "nextCursor"}:
            raise DepositError("INVALID_RESPONSE", "Query page returned an invalid page")
        columns = data["columns"]
        rows = data["rows"]
        next_cursor = data["nextCursor"]
        if (not isinstance(columns, list) or not isinstance(rows, list)
                or (next_cursor is not None and not isinstance(next_cursor, str))):
            raise DepositError("INVALID_RESPONSE", "Query page returned invalid columns, rows, or cursor")
        names = []
        for column in columns:
            if not isinstance(column, dict) or set(column) != {"name", "type"}:
                raise DepositError("INVALID_RESPONSE", "Query page returned invalid column metadata")
            if not isinstance(column["name"], str) or not isinstance(column["type"], str) or column["name"] in names:
                raise DepositError("INVALID_RESPONSE", "Query page returned duplicate or invalid columns")
            names.append(column["name"])
        detached_rows = []
        for row in rows:
            if not isinstance(row, dict) or set(row) != {"values", "presence"}:
                raise DepositError("INVALID_RESPONSE", "Query page returned an invalid row")
            values, presence = row["values"], row["presence"]
            if not isinstance(values, dict) or not isinstance(presence, dict) or set(values) != set(names) or set(presence) != set(names):
                raise DepositError("INVALID_RESPONSE", "Query page row does not match its columns")
            if any(type(flag) is not bool for flag in presence.values()):
                raise DepositError("INVALID_RESPONSE", "Query page presence flags must be booleans")
            decoded = {}
            for column in columns:
                name, kind = column["name"], column["type"]
                try:
                    decoded[name] = decode_value(kind, values[name]) if kind in _DOMAIN_VALUE_TYPES else values[name]
                except (TypeError, ValueError, OverflowError) as exc:
                    raise DepositError("INVALID_RESPONSE", "Query page contains an invalid typed value") from exc
            detached_rows.append({"values": decoded, "presence": dict(presence)})
        from ._paging import QueryPage
        return QueryPage({"columns": [dict(column) for column in columns], "rows": detached_rows,
                          "nextCursor": next_cursor})

    def iter_query(self, querystr: str, *, parameters: dict[str, Any] | None = None,
                   page_size: int = 200):
        """Create a lazy row iterator over cursor-bound query pages."""
        from ._paging import QueryIterator
        if not isinstance(querystr, str) or not querystr:
            raise ValueError("querystr must be a nonempty string")
        if parameters is None:
            parameters = {}
        if not isinstance(parameters, dict):
            raise TypeError("parameters must be a dictionary")
        if type(page_size) is not int or not 1 <= page_size <= 1000:
            raise ValueError("page_size must be an integer from 1 through 1000")
        if self._batch_collection is not None:
            raise RuntimeError("Store operations are unavailable while collecting a batch")
        return QueryIterator(self, querystr, _query_parameters(parameters), page_size)

    def query_from(self, queries):
        """Yield one first page for each independent ``(query, parameters)`` pair."""
        for item in queries:
            if not isinstance(item, (tuple, list)) or len(item) != 2:
                raise TypeError("queries must yield (querystr, parameters) pairs")
            querystr, parameters = item
            yield self.get_query_page(querystr, parameters=parameters)

    def _entity_page(self, procedure: str, selection, *, page_size=200, transform=None, prepare=None,
                     expected_revision=None):
        from ._paging import EntityPageIterator
        return EntityPageIterator(self, procedure, selection, page_size=page_size, transform=transform,
                                  prepare=prepare, expected_revision=expected_revision)

    def classes(self, name: str | None = None, *, page_size=200):
        return self._entity_page("deposit.get.classPage", name, page_size=page_size,
                                 prepare=self._class_page_handles)

    def objects(self, *, class_name: str | None = None, descriptors: Mapping[str, Any] | None = None,
                object_id: str | None = None, page_size=200):
        from .values import DType, TypedValue
        if object_id is not None:
            if class_name is not None or descriptors:
                raise ValueError("object_id is exclusive with Class and Descriptor filters")
            return iter((self.get_object_handle({"id": _stable_id(object_id)}),))
        selection = {}
        schema = None
        schema_revision = None
        dclass = None
        if class_name is not None:
            dclass = self.get_class_handle(class_name)
            selection["classId"] = dclass.id
            schema = dclass._schema
            schema_revision = dclass._baseline_revision
        if descriptors is not None:
            if not isinstance(descriptors, Mapping) or any(not isinstance(k, str) for k in descriptors):
                raise TypeError("descriptors must be a string-keyed mapping")
            encoded = {}
            for name, value in descriptors.items():
                if isinstance(value, (TypedValue, DType)):
                    if schema is None:
                        raise TypeError("Typed Descriptor filters require class_name schema context")
                    kind = schema.get(name)
                    if kind is None:
                        raise KeyError(name)
                    encoded[name] = _descriptor_payload(
                        value, kind, geometry_encoder=self._geometry_write_value,
                        resource_encoder=self._resource_write_value,
                    )
                else:
                    encoded[name] = value
            selection["descriptors"] = encoded
        schemas = {}
        return self._entity_page("deposit.get.objectPage", selection or None, page_size=page_size,
                                 expected_revision=schema_revision,
                                 prepare=lambda items, revision: self._object_page_handles(items, revision, schemas, dclass))

    def members(self, dclass: DClass | str | dict[str, Any], *, page_size=200):
        handle = dclass if isinstance(dclass, DClass) else self.get_class_handle(dclass)
        self._validate_handle(handle, "Class")
        schemas = {}
        return self._entity_page(
            "deposit.get.classMembersPage", {"id": handle.id}, page_size=page_size,
            prepare=lambda items, revision: self._object_page_handles(items, revision, schemas, handle),
        )

    def is_member(self, dobject: DObject, dclass: DClass | str | dict[str, Any]) -> bool:
        self._validate_handle(dobject, "Object")
        return dobject.has_class(dclass)

    def descriptors(self, dobject: DObject, name: str | None = None, *, page_size=200):
        self._validate_handle(dobject, "Object")
        self._ensure_handle_current(dobject)
        revision = _matching_handle_revision(dobject)
        schema = dict(dobject._schema)
        selection = {"objectId": dobject.id}
        if name is not None:
            if not isinstance(name, str) or not name:
                raise ValueError("Descriptor name must be a nonempty string")
            selection["name"] = name

        def decode(item, page_revision):
            descriptor_name = item.get("name")
            if not isinstance(descriptor_name, str):
                raise DepositError("INVALID_RESPONSE", "Descriptor page item has no name")
            kind = schema.get(descriptor_name)
            if kind is None:
                raise DepositError("INVALID_RESPONSE", "Descriptor page item has no checked declaration")
            from .values import decode_value
            try:
                value = decode_value(kind, item.get("value"))
            except (TypeError, ValueError, OverflowError) as exc:
                raise DepositError("INVALID_RESPONSE", "Descriptor page value does not match its declaration") from exc
            return descriptor_name, value, kind

        return self._entity_page("deposit.get.descriptorPage", selection, page_size=page_size, transform=decode,
                                 expected_revision=revision)

    def class_descriptors(self, dclass: DClass, name: str | None = None, *, page_size=200):
        self._validate_handle(dclass, "Class")
        selection = {"classId": dclass.id}
        if name is not None:
            if not isinstance(name, str) or not name:
                raise ValueError("Descriptor name must be a nonempty string")
            selection["name"] = name
        return self._entity_page(
            "deposit.get.classDescriptorPage", selection, page_size=page_size,
            transform=lambda item, revision: (item.get("name"), item.get("type"), item.get("comment", "")),
        )

    def relationships(self, src_object: DObject | None = None, tgt_object: DObject | None = None,
                      label: str | None = None, *, page_size=200):
        selection = {}
        if src_object is not None:
            self._validate_handle(src_object, "Object")
            selection["sourceId"] = src_object.id
        if tgt_object is not None:
            self._validate_handle(tgt_object, "Object")
            selection["targetId"] = tgt_object.id
        if label is not None:
            if not isinstance(label, str) or not label:
                raise ValueError("Relationship label must be a nonempty string")
            selection["label"] = label
        return self._entity_page("deposit.get.relationshipPage", selection or None, page_size=page_size)

    def class_relationships(self, src_class: DClass | None = None, tgt_class: DClass | None = None,
                            label: str | None = None, *, page_size=200):
        selection = {}
        if src_class is not None:
            self._validate_handle(src_class, "Class")
            selection["sourceId"] = src_class.id
        if tgt_class is not None:
            self._validate_handle(tgt_class, "Class")
            selection["targetId"] = tgt_class.id
        if label is not None:
            if not isinstance(label, str) or not label:
                raise ValueError("Relationship label must be a nonempty string")
            selection["label"] = label
        return self._entity_page("deposit.get.classRelationshipPage", selection or None, page_size=page_size)

    def neighbors(self, dobject: DObject, *, direction="both", page_size=200):
        return self._neighbors(dobject, direction=direction, page_size=page_size, class_relationship=False)

    def class_neighbors(self, dclass: DClass, *, direction="both", page_size=200):
        return self._neighbors(dclass, direction=direction, page_size=page_size, class_relationship=True)

    def _neighbors(self, source: DNode, *, direction, page_size, class_relationship):
        kind = "Class" if class_relationship else "Object"
        self._validate_handle(source, kind)
        if direction not in {"outgoing", "incoming", "both"}:
            raise ValueError("direction must be 'outgoing', 'incoming', or 'both'")
        procedure = "deposit.get.classRelationshipPage" if class_relationship else "deposit.get.relationshipPage"
        seen = set()
        selections = []
        if direction in {"outgoing", "both"}:
            selections.append(("sourceId", "targetId"))
        if direction in {"incoming", "both"}:
            selections.append(("targetId", "sourceId"))
        revision = None
        schemas = {}
        for filter_key, other_key in selections:
            def prepare(relationships, checked_revision):
                selected = []
                identities = {}
                for relationship in relationships:
                    identity, other_id = relationship.get("id"), relationship.get(other_key)
                    if not isinstance(identity, str) or not isinstance(other_id, str):
                        raise DepositError("INVALID_RESPONSE", "Relationship page omitted stable endpoint identity")
                    if identity in seen:
                        continue
                    seen.add(identity)
                    selected.append((other_id, relationship))
                    identities[other_id] = None
                entities = self._checked_page_entities(kind, identities, checked_revision)
                items = [entities[other_id] for other_id, _ in selected]
                if kind == "Class":
                    handles = self._class_page_handles(items, checked_revision)
                else:
                    handles = self._object_page_handles(items, checked_revision, schemas)
                return [(handle, relationship) for handle, (_, relationship) in zip(handles, selected, strict=True)]

            iterator = self._entity_page(procedure, {filter_key: source.id}, page_size=page_size,
                                         expected_revision=revision, prepare=prepare)
            try:
                yield from iterator
            finally:
                revision = iterator.revision
                iterator.close()

    def get_user_tool(self, selection: str | dict[str, Any]):
        if isinstance(selection, str):
            selection = {"name": selection}
        return self._call_one("deposit.get.userTool", selection)

    def user_tools(self, name: str | None = None, *, page_size=200):
        if name is not None and (not isinstance(name, str) or not name):
            raise ValueError("User Tool name filter must be a nonempty string")
        selection = name
        return self._entity_page("deposit.get.userToolPage", selection, page_size=page_size)

    def _begin_batch_collection(self, batch) -> None:
        with self._state_lock:
            if self._closed or self._closing:
                raise RuntimeError("Store is closed")
            if self._batch_collection is not None:
                raise RuntimeError("Nested batches are not supported")
            self._batch_collection = batch

    def _end_batch_collection(self, batch) -> None:
        with self._state_lock:
            if self._batch_collection is batch:
                self._batch_collection = None

    def _submit_batch(self, commands: list[dict[str, Any]], *, pinned_revision: int | None = None) -> dict[str, Any]:
        if not commands:
            return {}
        result = self._run_action("deposit.apply.batch", [commands], pinned_revision=pinned_revision)
        if not isinstance(result, dict) or not isinstance(result.get("results"), dict):
            raise DepositError("INVALID_RESPONSE", "Batch action returned no portable command results")
        return result["results"]

    def cancel(self) -> bool:
        """Request cancellation of this Store's current operation or read."""
        return self._cancel(allow_closing=False)

    def _cancel(self, *, allow_closing: bool) -> bool:
        if not self._begin_parallel_control(allow_closing=allow_closing):
            return False
        try:
            with self._state_lock:
                endpoint = self._active_endpoint
            if endpoint is not None:
                endpoint.cancel_current()
                return True
            entry = store_control_procedure("deposit.utils.cancel")
            query, parameters = build_query(entry, (), self._token)
            row = self._client._single(query, parameters)
            value = row.get("value")
            if not isinstance(value, bool):
                raise DepositError("INVALID_RESPONSE", "Cancellation returned no boolean result")
            return value
        finally:
            self._end_parallel_control()

    def _begin_parallel_control(self, *, allow_closing: bool = False) -> bool:
        with self._control_condition:
            if self._closed or (self._closing and not allow_closing):
                return False
            self._inflight_controls += 1
            return True

    def _end_parallel_control(self) -> None:
        with self._control_condition:
            self._inflight_controls -= 1
            self._control_condition.notify_all()

    def close(self) -> None:
        with self._close_lock:
            with self._state_lock:
                if self._closed:
                    return
                self._closing = True
                active = self._active
                active_result = self._active_result
            if active:
                try:
                    self._cancel(allow_closing=True)
                except Exception:
                    # Release local readers and the session even when cancellation fails.
                    pass
            failure: Exception | None = None
            if active_result is not None:
                try:
                    active_result.close()
                except Exception as exc:
                    failure = exc
            with self._control_condition:
                while self._inflight_controls:
                    self._control_condition.wait()
            with self._operation_lock:
                for reader in tuple(self._readers.values()):
                    with suppress(Exception):
                        reader.close()
                for plan_id in tuple(self._ready_plans):
                    with suppress(DepositError, TransportError):
                        self._discard_plan(plan_id)
                for reference in self._result_packages:
                    discard_package(self._endpoint, reference, control=True)
                self._ready_plans.clear()
                self._plan_packages.clear()
                self._result_packages.clear()
                for bundle in tuple(self._bundles):
                    with suppress(Exception):
                        bundle.close()
                try:
                    self._endpoint.close()
                except Exception as exc:
                    failure = failure or exc
                try:
                    self._client.logout(self._token)
                except Exception as exc:
                    if failure is None and not _session_already_unavailable(exc):
                        failure = exc
                with self._state_lock:
                    self._closed = True
                    self._closing = False
                self._client._store_closed(self)
            if failure is not None:
                raise failure

    def __enter__(self) -> "Store":
        with self._state_lock:
            if self._closed or self._closing:
                raise RuntimeError("Store is closed")
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.close()

    @contextmanager
    def _serialized(self, *, allow_closing: bool = False) -> Iterator[None]:
        self._operation_lock.acquire()
        try:
            with self._state_lock:
                if self._closed or (self._closing and not allow_closing):
                    raise RuntimeError("Store is closed")
                self._active = True
            yield
        finally:
            with self._state_lock:
                self._active = False
            self._operation_lock.release()

    def _read_entity(self, procedure: str, selection: dict[str, Any] | str) -> dict[str, Any]:
        self._assert_not_collecting_batch()
        with self._serialized():
            query = (
                f"CYPHER 25 CALL {procedure}Page($selection, {{pageSize: 1}}, $token) YIELD data "
                "RETURN data.items[0] AS data"
            )
            row = self._client._single(query, {"selection": selection, "token": self._token})
            data = row.get("data")
            if not isinstance(data, dict):
                raise DepositError("INVALID_RESPONSE", f"{procedure} returned no portable entity")
            if data.get("namespaceId") != self._namespace_id:
                raise DepositError("SCOPE_MISMATCH", "Entity belongs to another Namespace")
            return data

    def _call_one(self, procedure: str, *arguments: Any, _already_serialized: bool = False) -> Any:
        entry = store_procedure(procedure)
        if _already_serialized:
            bound = bind_arguments(entry, arguments, namespace_id=self._namespace_id)
            result = self._open_result(entry, bound, _already_serialized=True)
        else:
            result = self.call(procedure, *arguments)
        with result:
            row = next(result, None)
            if row is None:
                raise DepositError("INVALID_RESPONSE", f"{procedure} returned no row")
            if len(entry["columns"]) == 1:
                column = entry["columns"][0]
                value = row.get(column)
                if not entry["action"] and column in {"node", "rel"}:
                    value = result.to_data(value)
            else:
                value = dict(row)
            if next(result, None) is not None:
                raise DepositError("INVALID_RESPONSE", f"{procedure} returned multiple rows")
            return _portable_input(value)

    def _refresh_handle_snapshot(self, handle: DNode) -> None:
        data, schema, revision = self._checked_entity_snapshot(handle.kind, {"id": handle.id})
        handle._snapshot = data
        handle._schema = dict(schema)
        handle._baseline_revision = revision
        handle._stale = False

    def _descriptor_id(self, owner: DNode, name: str) -> str:
        if owner.kind == "Class":
            declaration = owner.declarations.get(name)
            if declaration is None:
                raise KeyError(name)
            identity = declaration.get("id")
        else:
            self._ensure_handle_current(owner)
            descriptor_ids = owner._snapshot.get("descriptorIds", {})
            identity = descriptor_ids.get(name) if isinstance(descriptor_ids, Mapping) else None
            if not identity:
                # Portable Object snapshots intentionally omit Descriptor IDs.
                # Resolve this one stable Descriptor selector in a checked read.
                descriptor = self._call_one("deposit.get.descriptor", {"objectId": owner.id, "name": name})
                identity = descriptor.get("id") if isinstance(descriptor, dict) else None
        if not isinstance(identity, str) or not identity:
            raise DepositError("INVALID_RESPONSE", "Descriptor snapshot returned no stable ID")
        return identity

    @staticmethod
    def _ensure_handle_current(handle: DNode) -> None:
        if handle.stale:
            raise RuntimeError("Entity snapshot is stale; call refresh() before reading or editing")

    def _run_multi_handle_action(self, procedure: str, arguments: list[Any], revision: int,
                                 handles: tuple[DNode, ...]):
        try:
            return self._run_action(procedure, arguments, pinned_revision=revision)
        except DepositError as exc:
            if exc.code == "REVISION_CONFLICT":
                for handle in handles:
                    handle._mark_stale()
            raise

    def _open_result(self, entry: dict[str, Any], arguments: tuple[Any, ...], *, _already_serialized: bool = False) -> StoreResult:
        # Convenience calls may already hold the operation lock.
        guard = nullcontext() if _already_serialized else self._serialized()
        guard.__enter__()
        session = None
        transaction = None
        store_result = None
        closing_error = False
        try:
            with self._state_lock:
                if self._closed or (self._closing and not _already_serialized):
                    closing_error = True
                    raise RuntimeError("Store is closed")
            session = self._client._session()
            transaction = session.begin_transaction()
            query, parameters = build_query(entry, arguments, self._token)
            result = transaction.run(query, parameters)
            columns = tuple(result.keys())
            expected = tuple(entry["columns"])
            if columns != expected:
                raise DepositError("INVALID_RESPONSE", f"{entry['name']} returned unexpected columns")

            def finish_result(closed_result: StoreResult) -> None:
                with self._state_lock:
                    if self._active_result is closed_result:
                        self._active_result = None
                guard.__exit__(None, None, None)

            store_result = StoreResult(
                columns,
                result=result,
                transaction=transaction,
                session=session,
                on_close=finish_result,
                token=self._token,
            )
            with self._state_lock:
                closing_error = self._closed or (self._closing and not _already_serialized)
                if not closing_error:
                    self._active_result = store_result
            if closing_error:
                store_result.close()
                raise RuntimeError("Store is closed")
            return store_result
        except BaseException as exc:
            if store_result is None or not store_result.closed:
                if transaction is not None:
                    try:
                        transaction.rollback()
                    except Exception:
                        pass
                if session is not None:
                    try:
                        session.close()
                    except Exception:
                        pass
                guard.__exit__(None, None, None)
            if isinstance(exc, (DepositError, TransportError)) or closing_error:
                raise
            if isinstance(exc, Exception):
                raise map_driver_error(exc) from exc
            raise

    def _run_handle_action(self, handle: DNode, procedure: str, arguments: list[Any]) -> dict[str, Any]:
        self._validate_handle(handle)
        self._ensure_handle_current(handle)
        return self._run_action(procedure, arguments, pinned_revision=handle._baseline_revision,
                                stale_handle=handle)

    @contextmanager
    def _using_endpoint(self, endpoint=None):
        endpoint = self._endpoint if endpoint is None else endpoint
        with self._state_lock:
            if self._closing or self._closed:
                raise RuntimeError("Store is closed")
            endpoint.reset_cancellation()
            self._active_endpoint = endpoint
        try:
            yield endpoint
        finally:
            with self._state_lock:
                self._active_endpoint = None

    def _run_action(self, procedure: str, arguments, *, pinned_revision: int | None = None,
                    stale_handle: DNode | None = None, post_snapshot: bool = False,
                    operation_id=None, _already_serialized: bool = False):
        if self._batch_collection is not None and procedure != "deposit.apply.batch":
            raise RuntimeError("Store mutations are unavailable while collecting a batch")
        entry = store_procedure(procedure)
        if entry.get("tracked") != "always":
            raise ValueError("Procedure does not accept a tracked operation")
        bound = bind_arguments(domain_arguments(entry), tuple(arguments), namespace_id=self.namespace_id)
        frozen = _portable_input(list(bound))
        operation_id = self._client.new_operation_id() if operation_id is None else operation_id
        with nullcontext() if _already_serialized else self._serialized():
            self._update_observed_revision()
            if pinned_revision is not None and pinned_revision != self._observed_revision:
                if stale_handle is not None:
                    stale_handle._mark_stale()
                raise DepositError("REVISION_CONFLICT", "Entity handle baseline is stale; refresh and reapply explicitly")
            try:
                with self._using_endpoint() as endpoint, endpoint.execution() as execution:
                    outcome = execution.run(procedure, *frozen, operation_id=operation_id)
            except DepositError as exc:
                if exc.code == "REVISION_CONFLICT" and stale_handle is not None:
                    stale_handle._mark_stale()
                if isinstance(exc, (OutcomeUnknown, OperationCommittedAfterCancel)) or (
                        isinstance(exc.details, dict) and exc.details.get("state") == "COMMITTED"):
                    self._invalidate_handles()
                raise
            if entry["scope"] == "namespace" or procedure == "deposit.import.apply":
                self._invalidate_handles()
            if entry["columns"] in (["node"], ["rel"]) and (
                    not isinstance(outcome, dict) or outcome.get("namespaceId") != self.namespace_id):
                raise DepositError("SCOPE_MISMATCH", "Mutation result belongs to another Namespace",
                                   {"operationId": operation_id})
            if post_snapshot:
                kind = "Class" if procedure == "deposit.create.class" else "Object"
                try:
                    snapshot = self._checked_entity_snapshot(kind, {"id": outcome["id"]}, _already_serialized=True)
                except (DepositError, TransportError, KeyError) as exc:
                    raise DepositError("RESULT_UNAVAILABLE", "The mutation committed. Inspect its Server receipt.",
                                       {"operationId": operation_id, "scope": self.namespace_id, "state": "COMMITTED"}) from exc
                return outcome, snapshot
            return _portable_input(outcome)

    def _assert_not_collecting_batch(self) -> None:
        if self._batch_collection is not None:
            raise RuntimeError("Store operations are unavailable while collecting a batch")

    def _update_observed_revision(self) -> None:
        session = self._validate_store_session()
        revision = self._get_revision(session.get("observed"))
        if revision is None:
            raise DepositError("REVISION_UNAVAILABLE", "Server returned no Namespace revision baseline")
        self._observed_revision = revision

    def _validate_store_session(self) -> dict[str, Any]:
        session = self._endpoint.call("deposit.get.session")
        user = session.get("user")
        if not isinstance(user, dict) or user.get("id") != self._actor_id:
            raise DepositError("IDENTITY_MISMATCH", "Store session actor changed")
        namespace_id = session.get("namespaceId")
        if namespace_id != self._namespace_id:
            raise DepositError("SCOPE_MISMATCH", "Store session Namespace changed")
        return session

    def _get_revision(self, observed: Any) -> int | None:
        if not isinstance(observed, dict):
            return None
        revision = observed.get(self._namespace_id)
        return revision if isinstance(revision, int) and not isinstance(revision, bool) and revision >= 0 else None


def _selector(value: str | dict[str, Any], kind: str) -> str | dict[str, Any]:
    if isinstance(value, DNode):
        if value.kind != kind:
            raise TypeError(f"{kind} selector has the wrong handle kind")
        return {"id": value.id, "kind": value.kind, "namespaceId": value.namespace_id}
    if isinstance(value, str) and value:
        return value
    if isinstance(value, dict):
        if set(value) == {"id"} and isinstance(value["id"], str) and value["id"]:
            return {"id": value["id"]}
        if value.get("kind") == kind and isinstance(value.get("id"), str):
            return {"id": value["id"]}
    raise TypeError(f"{kind} selector must be a name or a stable-ID record")


def _pattern_selector(value: str | Mapping[str, Any]) -> str | dict[str, Any]:
    if isinstance(value, str) and value:
        return value
    if isinstance(value, Mapping):
        selector = dict(value)
        if set(selector) == {"id"} and isinstance(selector["id"], str) and selector["id"]:
            return selector
        if set(selector) == {"name"} and isinstance(selector["name"], str) and selector["name"]:
            return selector
    raise TypeError("Pattern selector must be a nonempty name or an {id: UUID} mapping")


def _pattern_id_selector(value: Mapping[str, Any]) -> dict[str, str]:
    selector = dict(value)
    if set(selector) != {"id"} or not isinstance(selector["id"], str) or not selector["id"]:
        raise TypeError("Pattern selector must contain only a nonempty 'id' string")
    return {"id": selector["id"]}


def _import_rules(rules):
    if rules is not None and not isinstance(rules, Mapping):
        raise TypeError("rules must be a mapping")
    return _portable_input(dict(rules or {}))


def _freeze_import_rows(rows: Any) -> list[dict[str, Any]]:
    if not isinstance(rows, (list, tuple)):
        raise TypeError("rows must be a list or tuple of row records")
    frozen = []
    for row in rows:
        if not isinstance(row, Mapping):
            raise TypeError("rows must contain mapping records")
        record = dict(row)
        if isinstance(record.get("values"), Mapping):
            record["values"] = dict(record["values"])
        frozen.append(record)
    return frozen


def _schema_from_class_data(data: Any) -> dict[str, str]:
    if not isinstance(data, Mapping):
        return {}
    declarations = data.get("descriptors", [])
    if not isinstance(declarations, list):
        return {}
    schema = {}
    for declaration in declarations:
        if not isinstance(declaration, Mapping):
            continue
        name, type_name = declaration.get("name"), declaration.get("type")
        if isinstance(name, str) and isinstance(type_name, str):
            schema[name] = type_name
    return schema


def _stable_id(value: Any) -> str:
    if not isinstance(value, str) or not value:
        raise TypeError("Entity selector must be a nonempty stable ID")
    return value


def _item_id(item: Mapping[str, Any]) -> str:
    """Extract a stable public entity ID from a portable page item."""
    identity = item.get("id")
    if not isinstance(identity, str) or not identity:
        raise DepositError("INVALID_RESPONSE", "Entity page item has no stable public ID")
    return identity


def _position_value(value: Any) -> tuple[int | float, int | float]:
    if not isinstance(value, (tuple, list)) or len(value) != 2:
        raise TypeError("position must contain exactly two numeric coordinates")
    if any(type(item) not in {int, float} or not math.isfinite(item) for item in value):
        raise TypeError("position coordinates must be finite numbers")
    return value[0], value[1]


def _matching_handle_revision(*handles: DNode) -> int:
    if not handles:
        raise ValueError("At least one entity handle is required")
    revisions = []
    for handle in handles:
        if handle.stale:
            raise RuntimeError("Entity snapshot is stale; call refresh() before editing")
        revision = handle._baseline_revision
        if type(revision) is not int or revision < 0:
            raise ValueError("Entity handle has no checked snapshot revision")
        revisions.append(revision)
    if len(set(revisions)) != 1:
        raise ValueError("Entity handles have conflicting checked snapshot revisions")
    return revisions[0]


def _class_declarations(values: Any) -> list[dict[str, Any]]:
    if isinstance(values, Mapping):
        source = []
        for name, item in values.items():
            if isinstance(item, str):
                source.append((name, item))
            elif isinstance(item, (tuple, list)):
                source.append((name, *item))
            elif isinstance(item, Mapping):
                source.append({"name": name, **item})
            else:
                raise TypeError("Class declaration values must be type names or declaration records")
    elif isinstance(values, (tuple, list)):
        source = list(values)
    else:
        raise TypeError("descriptors must be a mapping or sequence")
    declarations = []
    names = set()
    for order, item in enumerate(source):
        if isinstance(item, Mapping):
            declaration = dict(item)
            name, type_name = declaration.get("name"), declaration.get("type")
            comment = declaration.get("comment", "")
            order_value = declaration.get("order", order)
        elif isinstance(item, (tuple, list)) and len(item) in {2, 3}:
            name, type_name = item[:2]
            comment = item[2] if len(item) == 3 else ""
            order_value = order
            declaration = {}
        else:
            raise TypeError("Each Class declaration must be (name, type[, comment]) or a mapping")
        if not isinstance(name, str) or not name or not isinstance(type_name, str) or not type_name:
            raise ValueError("Class declaration name and type must be nonempty strings")
        if name in names:
            raise ValueError(f"Duplicate Class declaration {name!r}")
        if not isinstance(comment, str) or type(order_value) is not int or order_value < 0:
            raise ValueError("Class declaration comment/order is invalid")
        names.add(name)
        declarations.append({"name": name, "type": type_name, "order": order_value, "comment": comment})
    return declarations


def _descriptor_payload(value: Any, declared: str | None, *, geometry_encoder=None, resource_encoder=None) -> Any:
    from .values import DType, TypedValue, _infer_type, encode_value
    if declared is not None:
        if isinstance(value, TypedValue):
            if value.type != declared:
                raise TypeError(f"Descriptor is declared as {declared}")
            value = value.value
        elif isinstance(value, DType):
            if value._type != declared:
                raise TypeError(f"Descriptor is declared as {declared}")
            value = value.value
        if declared == "DGeometry" and geometry_encoder is not None:
            value = geometry_encoder(value)
        elif declared == "DResource" and resource_encoder is not None:
            value = resource_encoder(value)
        return encode_value(declared, value)
    if isinstance(value, TypedValue):
        kind, raw = value.type, value.value
        if kind == "DGeometry" and geometry_encoder is not None:
            raw = geometry_encoder(raw)
        elif kind == "DResource" and resource_encoder is not None:
            raw = resource_encoder(raw)
        return {"type": kind, "value": encode_value(kind, raw)}
    if isinstance(value, DType):
        if value._type == "DGeometry" and geometry_encoder is not None:
            raw = geometry_encoder(value)
            return {"type": value._type, "value": encode_value(value._type, raw)}
        if value._type == "DResource" and resource_encoder is not None:
            raw = resource_encoder(value)
            return {"type": value._type, "value": encode_value(value._type, raw)}
        return value.to_dict()
    kind = _infer_type(value)
    if kind == "DGeometry" and geometry_encoder is not None:
        raw = geometry_encoder(value)
    elif kind == "DResource" and resource_encoder is not None:
        raw = resource_encoder(value)
    else:
        raw = value
    return {"type": kind, "value": encode_value(kind, raw)}


def _encode_object_properties(properties: dict[str, Any], schema: Mapping[str, str], *,
                              geometry_encoder=None, resource_encoder=None) -> dict[str, Any]:
    from .values import DType, TypedValue, _infer_type, encode_value
    if not isinstance(properties, dict):
        raise TypeError("Object properties must be a dictionary")
    result = dict(properties)
    if "descriptors" in result:
        descriptors = result["descriptors"]
        if not isinstance(descriptors, Mapping) or any(not isinstance(name, str) for name in descriptors):
            raise TypeError("Object descriptors must be a string-keyed mapping")
        encoded = {}
        for name, value in descriptors.items():
            type_name = schema.get(name)
            if type_name is None:
                encoded[name] = _descriptor_payload(
                    value, None, geometry_encoder=geometry_encoder,
                    resource_encoder=resource_encoder,
                )
            else:
                if isinstance(value, TypedValue):
                    if value.type != type_name: raise TypeError(f"Descriptor {name!r} is declared as {type_name}")
                    value = value.value
                elif isinstance(value, DType):
                    if value._type != type_name: raise TypeError(f"Descriptor {name!r} is declared as {type_name}")
                    value = value.value
                if type_name == "DGeometry" and geometry_encoder is not None:
                    value = geometry_encoder(value)
                elif type_name == "DResource" and resource_encoder is not None:
                    value = resource_encoder(value)
                encoded[name] = encode_value(type_name, value)
        result["descriptors"] = encoded
    return _portable_input(result)


def _query_parameters(values: dict[str, Any]) -> dict[str, Any]:
    from .values import DType, TypedValue
    result = {}
    for key, value in values.items():
        if isinstance(value, (TypedValue, DType)):
            result[key] = value.to_dict()
        elif isinstance(value, dict):
            result[key] = _query_parameters(value)
        elif isinstance(value, list):
            result[key] = [_query_parameters(item) if isinstance(item, dict) else item.to_dict()
                           if isinstance(item, (TypedValue, DType)) else item for item in value]
        else:
            result[key] = value
    return result


def _portable_input(value: Any) -> Any:
    _validate_portable_value(value)
    try:
        encoded = json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
        return json.loads(encoded)
    except (TypeError, ValueError, OverflowError) as exc:
        raise TypeError("Procedure arguments must be losslessly JSON-portable") from exc


def _validate_portable_value(value: Any) -> None:
    """Reject values JSON would coerce or silently alter before sending them."""
    if value is None or type(value) in {bool, int, str}:
        return
    if type(value) is float:
        if not math.isfinite(value):
            raise TypeError("Procedure arguments must be losslessly JSON-portable")
        return
    if isinstance(value, Mapping):
        if type(value) is not dict:
            raise TypeError("Procedure arguments require built-in JSON mappings")
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError("Procedure arguments require string mapping keys")
            _validate_portable_value(item)
        return
    if type(value) is list:
        for item in value:
            _validate_portable_value(item)
        return
    raise TypeError("Procedure arguments must be losslessly JSON-portable")


def _session_already_unavailable(exc: Exception) -> bool:
    return isinstance(exc, DepositError) and exc.code in {"AUTH_FAILED", "AUTH_REQUIRED", "SESSION_EXPIRED", "NOT_FOUND"}
