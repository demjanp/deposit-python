"""Namespace-bound Store and the Stage 2 prepared-action coordinator."""

from __future__ import annotations

from collections.abc import Mapping
from contextlib import contextmanager, nullcontext
import base64
import hashlib
import json
import math
import mimetypes
from pathlib import Path
import threading
import weakref
from typing import Any, Iterator
import uuid

from ._recovery.identity import DeploymentIdentity
from ._recovery.journal import (
    RESULT_FORMAT_NAMED_ROWS_V1,
    JournalError,
    ProtectedJournal,
    UNRESOLVED_STATES,
)
from ._recovery.transfers import ProtectedTransferSpool, TransferError, TRANSFER_CHUNK_BYTES
from ._gateway import (
    StoreResult,
    bind_arguments,
    build_query,
    detached_result,
    immutable_request,
    map_driver_error,
    store_procedure,
    store_control_procedure,
    named_rows_result,
)
from .errors import DepositError, TransportError, _saved_failure_error
from .entities import DClass, DObject, DNode
from .values import DResource


_ENTITY_QUERY = {
    "deposit.create.class": ("name", "properties"),
    "deposit.create.object": ("class", "properties"),
    "deposit.get.class": ("selection",),
    "deposit.get.object": ("selection",),
    "deposit.apply.batch": ("data",),
}
_STORE_FACTORY_TOKEN = object()
_DOMAIN_VALUE_TYPES = {
    "String", "Integer", "Float", "Boolean", "Date", "Time", "LocalTime", "DateTime",
    "LocalDateTime", "Duration", "Point", "List", "DDict", "DGeometry", "DResource",
}
_DEFINITIVE_DOMAIN_FAILURE_CODES = frozenset({
    "VALIDATION_FAILED", "TYPE_CONFLICT", "NOT_FOUND", "ALREADY_EXISTS",
    "HISTORY_UNAVAILABLE", "IMPORT_CONFLICT", "PLAN_STALE",
})


class Store:
    """An independently authenticated session bound to one immutable Namespace UUID."""

    def __init__(
        self,
        client,
        *,
        token: str,
        actor_id: str,
        namespace_id: str,
        identity: DeploymentIdentity,
        recovery_directory: str | None,
        observed: Any,
        _factory_token: object | None = None,
        _journal: ProtectedJournal | None = None,
    ):
        if _factory_token is not _STORE_FACTORY_TOKEN:
            raise TypeError("Store instances are created by Client.open_store()")
        self._client = client
        self._token = token
        self._actor_id = actor_id
        self._namespace_id = namespace_id
        self._identity = identity
        self._journal = _journal or ProtectedJournal(recovery_directory)
        self._operation_lock = threading.Lock()
        self._close_lock = threading.Lock()
        self._state_lock = threading.Lock()
        self._control_condition = threading.Condition(self._state_lock)
        self._inflight_controls = 0
        self._closed = False
        self._closing = False
        self._active = False
        self._active_result: StoreResult | None = None
        self._batch_collection: Any = None
        # DNode equality intentionally follows stable domain identity.  A
        # WeakSet would therefore collapse separate snapshots of the same
        # entity; key by Python object identity so each cached snapshot is
        # invalidated independently.
        self._handles = weakref.WeakValueDictionary()
        self._observed_revision = self._get_revision(observed)
        if self._observed_revision is None:
            self.refresh()
        self._journal.cleanup_expired(self._identity)
        with self._operation_lock:
            self._recover_pending_locked()
            self._resume_pending_uploads_locked()

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
        self._assert_not_collecting_batch()
        if kind not in {"Class", "Object"}:
            raise ValueError("Only Class and Object snapshots are supported")
        procedure = "deposit.get.class" if kind == "Class" else "deposit.get.object"
        if kind == "Class":
            query = (
                "CYPHER 25 CALL deposit.get.revision($token) YIELD data AS revision "
                f"CALL {procedure}($selection, $token) YIELD node "
                "CALL deposit.utils.toData(node, $token) YIELD data "
                "RETURN revision, data AS entity, data AS schema"
            )
            parameters = {"selection": selection, "token": self._token}
        else:
            query = (
                "CYPHER 25 CALL deposit.get.revision($token) YIELD data AS revision "
                "CALL deposit.get.object($selection, $token) YIELD node AS object "
                "CALL deposit.utils.toData(object, $token) YIELD data AS entity "
                "CALL deposit.get.class({id: entity.classId}, $token) YIELD node AS class "
                "CALL deposit.utils.toData(class, $token) YIELD data AS schema "
                "RETURN revision, entity, schema"
            )
            parameters = {"selection": selection, "token": self._token}
        with (nullcontext() if _already_serialized else self._serialized()):
            session = None
            transaction = None
            try:
                session = self._client._session()
                transaction = session.begin_transaction()
                result = transaction.run(query, parameters)
                record = result.single(strict=True)
                result.consume()
                if record is None:
                    raise DepositError("INVALID_RESPONSE", "Checked entity snapshot returned no record")
                entity = record.get("entity")
                schema = record.get("schema")
                revision = record.get("revision")
                if not isinstance(entity, dict) or not isinstance(schema, dict) or not isinstance(revision, dict):
                    raise DepositError("INVALID_RESPONSE", "Checked entity snapshot returned invalid data")
                if entity.get("namespaceId") != self._namespace_id:
                    raise DepositError("SCOPE_MISMATCH", "Snapshot belongs to another Store Namespace")
                tx_revision = revision.get("sequence")
                if isinstance(tx_revision, bool) or not isinstance(tx_revision, int) or tx_revision < 0:
                    raise DepositError("INVALID_RESPONSE", "Snapshot returned no checked Namespace revision")
                self._require_revision(tx_revision, expected_revision)
                # Procedure invocations can observe intervening commits even
                # within one Neo4j transaction. Fence all materialized entity
                # and declaration data before publishing a checked snapshot.
                self._require_revision(self._read_namespace_revision(transaction), tx_revision)
                transaction.commit()
                if kind == "Class":
                    declarations = _schema_from_class_data(entity)
                    return entity, declarations, tx_revision
                from .entities import decode_snapshot
                decoded = decode_snapshot(entity, schema, self._namespace_id, self, snapshot_revision=tx_revision)
                return decoded._snapshot, decoded._schema, tx_revision
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

    @staticmethod
    def _require_revision(revision: int, expected_revision: int | None) -> None:
        if expected_revision is not None and revision != expected_revision:
            raise DepositError("REVISION_CONFLICT", "Namespace changed during checked iteration or snapshot read")

    def _read_namespace_revision(self, transaction) -> int:
        result = transaction.run(
            "CYPHER 25 CALL deposit.get.revision($token) YIELD data RETURN data", {"token": self._token}
        )
        record = result.single(strict=True)
        result.consume()
        data = record.get("data") if record is not None else None
        revision = data.get("sequence") if isinstance(data, dict) else None
        if type(revision) is not int or revision < 0:
            raise DepositError("INVALID_RESPONSE", "Read returned no checked Namespace revision")
        return revision

    def _checked_entity_page(self, procedure: str, selection, options, *, expected_revision=None):
        """Fully detach a page between fresh, equal Namespace sequence reads."""
        self._assert_not_collecting_batch()
        entry = store_procedure(procedure)
        if entry["action"] or tuple(entry["columns"]) != ("data",) or not procedure.endswith("Page"):
            raise ValueError("Checked pages require a read-only Domain Page procedure")
        bound = bind_arguments(entry, (selection, options), namespace_id=self._namespace_id)
        query, parameters = build_query(entry, bound, self._token)
        with self._serialized():
            session = None
            transaction = None
            try:
                session = self._client._session()
                transaction = session.begin_transaction()
                revision = self._read_namespace_revision(transaction)
                self._require_revision(revision, expected_revision)
                result = transaction.run(query, parameters)
                if tuple(result.keys()) != ("data",):
                    raise DepositError("INVALID_RESPONSE", f"{procedure} returned unexpected columns")
                record = result.single(strict=True)
                result.consume()
                if record is None:
                    raise DepositError("INVALID_RESPONSE", f"{procedure} returned no page")
                page = _portable_input(record.get("data"))
                self._require_revision(self._read_namespace_revision(transaction), revision)
                transaction.commit()
                return page, revision
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

    def _entity_handle_at_revision(self, kind: str, identity: str, revision: int):
        data, schema, checked_revision = self._checked_entity_snapshot(
            kind, {"id": identity}, expected_revision=revision
        )
        handle_type = DClass if kind == "Class" else DObject
        return handle_type(self, data, schema, snapshot_revision=checked_revision)

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
        """Upload a frozen local resource source and return its ready reference."""
        self._assert_not_collecting_batch()
        if not isinstance(resource, DResource):
            raise TypeError("resource must be a DResource")
        if resource.resource_id is not None and resource._path is None and resource._stream is None:
            return resource
        if resource._transfer_id is None:
            if resource._path is not None:
                with open(resource._path, "rb") as source:
                    resource._transfer_id = ProtectedTransferSpool(self._journal, self._identity).freeze(
                        source,
                        filename=resource.filename,
                        media_type=resource._value.get("mediaType") or mimetypes.guess_type(resource.filename)[0] or "application/octet-stream",
                    )["transfer_id"]
            else:
                resource._transfer_id = ProtectedTransferSpool(self._journal, self._identity).freeze(
                    resource._stream,
                    filename=resource.filename,
                    media_type=resource._value.get("mediaType") or mimetypes.guess_type(resource.filename)[0] or "application/octet-stream",
                )["transfer_id"]
        spool = ProtectedTransferSpool(self._journal, self._identity)
        manifest = spool.load(resource._transfer_id)
        if manifest["begin_action_key"] is None:
            spool.ensure_action_capacity(manifest)
        with self._serialized():
            result = self._resume_upload_locked(resource._transfer_id, spool=spool, already_serialized=True)
        resource._value.update(result)
        resource._path = None
        resource._stream = None
        resource._transfer_id = None
        return resource

    def _open_resource(self, resource: DResource):
        self._assert_not_collecting_batch()
        if not isinstance(resource, DResource) or not resource.resource_id:
            raise ValueError("resource must be a stored DResource reference")
        if resource._value.get("namespaceId") not in (None, self._namespace_id):
            raise DepositError("SCOPE_MISMATCH", "Resource belongs to another Namespace")
        metadata = self._call_one("deposit.resource.info", resource.resource_id)
        if not isinstance(metadata, dict) or metadata.get("resourceId") != resource.resource_id:
            raise DepositError("INVALID_RESPONSE", "Resource metadata did not match the selected resource")
        resource._value.update(metadata)
        from ._recovery.transfers import ResourceReader
        return ResourceReader(self, resource.resource_id, metadata)

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
        selection = None if name is None else {"name": name}
        return self._entity_page(
            "deposit.get.patternPage", selection, page_size=page_size,
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
        """Request one server-matched Pattern page, including its diagnostics."""
        if not isinstance(pattern, Mapping):
            raise TypeError("Pattern search requires the server Pattern mapping")
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
            _portable_input(dict(pattern)),
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
    ) -> dict[str, Any]:
        """Submit form data to the server's Pattern import workflow."""
        if not isinstance(pattern, Mapping) or not isinstance(values, Mapping):
            raise TypeError("Pattern and form values must be mappings")
        if rules is not None and not isinstance(rules, Mapping):
            raise TypeError("Pattern rules must be a mapping")
        result = self._run_action(
            "deposit.import.dataByPattern",
            [
                _portable_input(dict(pattern)),
                _portable_input(dict(values)),
                _portable_input(dict(rules or {})),
            ],
        )
        if not isinstance(result, dict):
            raise DepositError("INVALID_RESPONSE", "Pattern form import returned no diagnostics")
        return result

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
        return result

    def import_rows(self, rows, relationships=None, *, targets=None, rules=None) -> dict[str, Any]:
        """Analyze a frozen tabular submission; matching remains server-owned."""
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
        if rules is not None and not isinstance(rules, Mapping):
            raise TypeError("rules must be a mapping")
        request = [
            _portable_input({"rows": frozen_rows}),
            _portable_input(dict(targets or {})),
            _portable_input(frozen_relationships),
            _portable_input(dict(rules or {})),
        ]
        return _validated_import_plan(self._run_action("deposit.import.rows", request))

    def import_subtree(
        self, subtree: Mapping[str, Any], *, rules: Mapping[str, Any] | None = None
    ) -> dict[str, Any]:
        """Analyze a fixed, resource-free public subgraph snapshot."""
        return self._analyze_import_snapshot(subtree, rules=rules, require_subgraph=True)

    def from_dict(
        self, data: Mapping[str, Any], *, rules: Mapping[str, Any] | None = None
    ) -> dict[str, Any]:
        """Analyze a fixed, resource-free public graph/package dictionary."""
        return self._analyze_import_snapshot(data, rules=rules, require_subgraph=False)

    def _analyze_import_snapshot(
        self,
        source: Mapping[str, Any],
        *,
        rules: Mapping[str, Any] | None,
        require_subgraph: bool,
    ) -> dict[str, Any]:
        if not isinstance(source, Mapping):
            raise TypeError("Import snapshot must be a manifest/graph mapping")
        if rules is not None and not isinstance(rules, Mapping):
            raise TypeError("rules must be a mapping")
        frozen_source = _portable_input(dict(source))
        frozen_rules = _portable_input(dict(rules or {}))
        from ._imports import resource_free_exchange_package

        package_bytes = resource_free_exchange_package(frozen_source, require_subgraph=require_subgraph)
        from io import BytesIO

        package_stream = BytesIO(package_bytes)
        package_resource = DResource(stream=package_stream, filename="deposit-import.zip")
        try:
            self.upload_resource(package_resource)
        finally:
            package_stream.close()
        resource_id = package_resource.resource_id
        if not isinstance(resource_id, str) or not resource_id:
            raise DepositError("INVALID_RESPONSE", "Exchange package upload returned no resource ID")
        request = [{"packageResourceId": resource_id}, frozen_rules]
        return _validated_import_plan(self._run_action("deposit.import.analyze", request))

    def import_apply(self, plan_id: str) -> dict[str, Any]:
        """Atomically apply the complete server plan through prepared recovery."""
        if not isinstance(plan_id, str) or not plan_id:
            raise ValueError("plan_id must be a nonempty string")
        result = self._run_action("deposit.import.apply", [plan_id])
        if not isinstance(result, dict):
            raise DepositError("INVALID_RESPONSE", "Import application returned no portable result")
        return result

    def call(self, procedure: str, *arguments: Any) -> StoreResult:
        """Call an allowlisted namespace procedure using its manifest signature.

        Results retain their transaction until exhausted or explicitly closed.
        Action procedures use the same durable coordinator as the convenience
        methods and return detached portable rows.
        """
        if self._batch_collection is not None:
            raise RuntimeError("Store calls are unavailable while collecting a batch")
        entry = store_procedure(procedure)
        bound = bind_arguments(entry, arguments, namespace_id=self._namespace_id)
        if entry["action"]:
            portable = _portable_input(list(bound))
            with self._serialized():
                self._update_observed_revision()
                revision = self._observed_revision
                if revision is None:
                    raise DepositError("REVISION_UNAVAILABLE", "Store has no observed Namespace revision")
                record = self._journal.create(
                    self._identity,
                    procedure,
                    portable,
                    revision,
                    result_format=RESULT_FORMAT_NAMED_ROWS_V1,
                )
                outcome = self._resolve_action_locked(record, allow_new_prepare=True)
                return detached_result(entry, outcome, self._token)
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
        """Inspect an operation in this Store's fixed Namespace scope."""
        self._assert_not_collecting_batch()
        if not isinstance(operation_id, str) or not operation_id:
            raise ValueError("operation_id must be a nonempty string")
        with self._serialized():
            return self._client.get_operation(self._token, operation_id, scope="namespace")

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

    def _entity_page(self, procedure: str, selection, *, page_size=200, transform=None, expected_revision=None):
        from ._paging import EntityPageIterator
        return EntityPageIterator(self, procedure, selection, page_size=page_size, transform=transform,
                                  expected_revision=expected_revision)

    def classes(self, name: str | None = None, *, page_size=200):
        selection = None if name is None else {"name": name}
        return self._entity_page("deposit.get.classPage", selection, page_size=page_size,
                                 transform=lambda item, revision: self._entity_handle_at_revision(
                                     "Class", _item_id(item), revision))

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
        return self._entity_page("deposit.get.objectPage", selection or None, page_size=page_size,
                                 expected_revision=schema_revision,
                                 transform=lambda item, revision: self._entity_handle_at_revision(
                                     "Object", _item_id(item), revision))

    def members(self, dclass: DClass | str | dict[str, Any], *, page_size=200):
        handle = dclass if isinstance(dclass, DClass) else self.get_class_handle(dclass)
        self._validate_handle(handle, "Class")
        return self._entity_page(
            "deposit.get.classMembersPage", {"id": handle.id}, page_size=page_size,
            transform=lambda item, revision: self._entity_handle_at_revision("Object", _item_id(item), revision),
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
        for filter_key, other_key in selections:
            iterator = self._entity_page(procedure, {filter_key: source.id}, page_size=page_size,
                                         expected_revision=revision)
            try:
                for relationship in iterator:
                    identity = relationship.get("id")
                    other_id = relationship.get(other_key)
                    if not isinstance(identity, str) or not isinstance(other_id, str):
                        raise DepositError("INVALID_RESPONSE", "Relationship page omitted stable endpoint identity")
                    if identity in seen:
                        continue
                    seen.add(identity)
                    yield self._entity_handle_at_revision(kind, other_id, iterator.revision), relationship
            finally:
                revision = iterator.revision
                iterator.close()

    def get_user_tool(self, selection: str | dict[str, Any]):
        if isinstance(selection, str):
            selection = {"name": selection}
        return self._call_one("deposit.get.userTool", selection)

    def user_tools(self, name: str | None = None, *, page_size=200):
        selection = None if name is None else {"name": name}
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
        """Request cancellation of all requests for this token.

        Cancellation requests termination; it does not establish rollback.
        Any action whose outcome is uncertain remains under its normal durable
        receipt and recovery path.
        """
        self._assert_not_collecting_batch()
        return self._cancel(allow_closing=False)

    def _cancel(self, *, allow_closing: bool) -> bool:
        if not self._begin_parallel_control(allow_closing=allow_closing):
            return False
        try:
            entry = store_control_procedure("deposit.utils.cancel")
            bound = bind_arguments(entry, ())
            query, parameters = build_query(entry, bound, self._token)
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
        self._assert_not_collecting_batch()
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
                    # Closing is best-effort; recovery and logout still need
                    # to run if the separate cancellation request fails.
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
                try:
                    self._recover_pending_locked()
                except Exception as exc:
                    if failure is None:
                        failure = exc
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
                f"CYPHER 25 CALL {procedure}($selection, $token) YIELD node "
                "CALL deposit.utils.toData(node, $token) YIELD data RETURN data"
            )
            row = self._client._single(query, {"selection": selection, "token": self._token})
            data = row.get("data")
            if not isinstance(data, dict):
                raise DepositError("INVALID_RESPONSE", f"{procedure} returned no portable entity")
            if data.get("namespaceId") != self._namespace_id:
                raise DepositError("SCOPE_MISMATCH", "Entity belongs to another Namespace")
            return data

    def _call_one(self, procedure: str, *arguments: Any) -> Any:
        entry = store_procedure(procedure)
        with self.call(procedure, *arguments) as result:
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

    def _open_result(self, entry: dict[str, Any], arguments: tuple[Any, ...]) -> StoreResult:
        guard = self._serialized()
        guard.__enter__()
        session = None
        transaction = None
        store_result = None
        closing_error = False
        try:
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
                closing_error = self._closed or self._closing
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

    def _run_action(self, procedure: str, arguments: list[Any], *, pinned_revision: int | None = None,
                    stale_handle: DNode | None = None, post_snapshot: bool = False,
                    _recovery_key: str | None = None, _already_serialized: bool = False):
        if self._batch_collection is not None and procedure != "deposit.apply.batch":
            raise RuntimeError("Store mutations are unavailable while collecting a batch")
        entry = store_procedure(procedure)
        if entry.get("action") is not True:
            raise ValueError("Prepared action coordinator accepts only declared action procedures")
        _portable_input(arguments)
        with nullcontext() if _already_serialized else self._serialized():
            self._update_observed_revision()
            revision = self._observed_revision
            if revision is None:
                raise DepositError("REVISION_UNAVAILABLE", "Store has no observed Namespace revision")
            if pinned_revision is not None and pinned_revision != revision:
                if stale_handle is not None:
                    stale_handle._mark_stale()
                raise DepositError("REVISION_CONFLICT", "Entity handle baseline is stale; refresh and reapply explicitly")
            record = self._journal.create(self._identity, procedure, arguments, revision, key=_recovery_key)
            try:
                outcome = self._resolve_action_locked(record, allow_new_prepare=True)
                if post_snapshot:
                    kind = "Class" if procedure == "deposit.create.class" else "Object"
                    snapshot = self._checked_entity_snapshot(kind, {"id": outcome["id"]},
                                                             _already_serialized=True)
                    return outcome, snapshot
                return outcome
            except DepositError as exc:
                if exc.code == "REVISION_CONFLICT" and stale_handle is not None:
                    stale_handle._mark_stale()
                raise

    def _assert_not_collecting_batch(self) -> None:
        if self._batch_collection is not None:
            raise RuntimeError("Store operations are unavailable while collecting a batch")

    def _recover_pending_locked(self) -> None:
        for record in self._journal.unresolved(self._identity):
            try:
                self._resolve_action_locked(record, allow_new_prepare=True)
            except DepositError as exc:
                # A definitive failure notice is retained for Client discovery,
                # while Store lifecycle recovery only finishes its detachment.
                if self._journal.load(self._identity, record["key"])["state"] == "failed":
                    continue
                if exc.code == "REVISION_CONFLICT":
                    continue
                raise DepositError("RECOVERY_PENDING", "An unresolved action could not be safely recovered") from exc
            except (TransportError, JournalError):
                raise

    def _resume_pending_uploads_locked(self) -> None:
        spool = ProtectedTransferSpool(self._journal, self._identity)
        for manifest in spool.pending():
            try:
                self._resume_upload_locked(manifest["transfer_id"], spool=spool, already_serialized=True)
            except DepositError:
                if spool.load(manifest["transfer_id"])["state"] != "failed":
                    raise

    def _terminalize_upload_failure_locked(self, spool, manifest):
        steps = {"begin_action_key": "deposit.resource.beginUpload",
                 "pending_action_key": "deposit.resource.uploadChunk",
                 "finish_action_key": "deposit.resource.finishUpload"}
        references = {manifest[field]: field for field in steps if manifest[field] is not None}
        for record in self._journal.records(self._identity):
            field = references.get(record["key"])
            if field is None or record["state"] != "failed":
                continue
            arguments = record["arguments"]
            matches = record["procedure"] == steps[field]
            if field == "begin_action_key":
                matches &= arguments == [manifest["filename"], manifest["bytes"], manifest["sha256"], manifest["media_type"]]
            elif field == "finish_action_key":
                matches &= manifest["upload_id"] is not None and arguments == [manifest["upload_id"]]
            else:
                try:
                    index = self._upload_chunk_index(manifest, manifest["pending_offset"])
                    chunk = manifest["chunks"][index]
                    block = base64.b64decode(arguments[2], validate=True)
                    matches &= (len(arguments) == 4 and arguments[0] == manifest["upload_id"]
                                and arguments[1] == manifest["pending_offset"]
                                and arguments[3] == chunk["sha256"]
                                and len(block) == manifest["pending_length"] == chunk["bytes"]
                                and hashlib.sha256(block).hexdigest() == chunk["sha256"])
                except (IndexError, TypeError, ValueError, TransferError):
                    matches = False
            if not matches:
                raise TransferError("Failed transfer action does not match its immutable inputs")
            return spool.update(manifest["transfer_id"], _locked=True, state="failed",
                                failed_action_key=record["key"], failure=record["failure"])
        return manifest

    def _run_transfer_step_locked(self, spool, manifest, procedure, arguments, key):
        try:
            return self._execute_transfer_action(procedure, arguments, key)
        except Exception:
            # The common coordinator alone proves rollback and detachment.
            # Persist terminal state before allowing a later Store to skip work.
            self._terminalize_upload_failure_locked(spool, manifest)
            raise

    def _resume_upload_locked(self, transfer_id: str, *, spool=None, already_serialized: bool = False) -> dict[str, Any]:
        spool = spool or ProtectedTransferSpool(self._journal, self._identity)
        lock = nullcontext() if already_serialized else self._serialized()
        with lock, self._journal.storage.lock(f"transfer-{transfer_id}", timeout=0.0):
            manifest = spool.load(transfer_id)
            if manifest["state"] in {"staged", "uploading"}:
                manifest = self._terminalize_upload_failure_locked(spool, manifest)
            if manifest["state"] == "failed":
                raise _saved_failure_error(manifest["failure"])
            if manifest["state"] == "finished":
                try:
                    record = self._journal.load(self._identity, manifest["finish_action_key"])
                except JournalError as exc:
                    raise TransferError("Finished transfer outcome is unavailable") from exc
                result = record.get("outcome")
                if not isinstance(result, dict):
                    raise TransferError("Finished transfer outcome is invalid")
                spool.cleanup_resolved(transfer_id, _locked=True)
                return result

            spool.verify_content(manifest)
            if manifest["begin_action_key"] is None:
                spool.ensure_action_capacity(manifest)

            begin_args = [manifest["filename"], manifest["bytes"], manifest["sha256"], manifest["media_type"]]
            begin_key = manifest["begin_action_key"]
            if begin_key is None:
                begin_key = str(uuid.uuid4())
                manifest = spool.update(transfer_id, _locked=True, begin_action_key=begin_key)
            begin = self._run_transfer_step_locked(spool, manifest, "deposit.resource.beginUpload", begin_args, begin_key)
            if (
                not isinstance(begin, dict)
                or not isinstance(begin.get("uploadId"), str)
                or type(begin.get("nextOffset")) is not int
                or type(begin.get("chunkBytes")) is not int
                or not 1 <= begin["chunkBytes"] <= TRANSFER_CHUNK_BYTES
            ):
                raise TransferError("Server returned invalid upload continuation metadata")
            if manifest["upload_id"] not in (None, begin["uploadId"]):
                raise TransferError("Server upload identity changed during recovery")
            offset = begin["nextOffset"]
            if offset < 0 or offset > manifest["bytes"]:
                raise TransferError("Server upload offset is outside the frozen input")
            manifest = spool.update(transfer_id, _locked=True, upload_id=begin["uploadId"], state="uploading")

            pending_offset = manifest["pending_offset"]
            pending_length = manifest["pending_length"]
            pending_key = manifest["pending_action_key"]
            if pending_offset is not None:
                if pending_length is None or pending_key is None:
                    raise TransferError("Protected transfer has incomplete pending chunk metadata")
                if offset not in {pending_offset, pending_offset + pending_length}:
                    raise TransferError("Server upload offset does not match the protected pending chunk")
                if offset == pending_offset:
                    index = self._upload_chunk_index(manifest, pending_offset)
                    block = spool.read_chunk(transfer_id, index)
                    if len(block) != pending_length:
                        raise TransferError("Pending upload chunk length changed")
                    arguments = [begin["uploadId"], pending_offset, base64.b64encode(block).decode("ascii"), hashlib.sha256(block).hexdigest()]
                    result = self._run_transfer_step_locked(spool, manifest, "deposit.resource.uploadChunk", arguments, pending_key)
                    if not isinstance(result, dict) or result.get("nextOffset") != pending_offset + pending_length:
                        raise TransferError("Server did not confirm the pending upload chunk")
                    offset = result["nextOffset"]
                manifest = spool.update(
                    transfer_id,
                    _locked=True,
                    offset=offset,
                    pending_action_key=None,
                    pending_offset=None,
                    pending_length=None,
                )
            elif manifest["offset"] != offset:
                # The server's contiguous offset is authoritative after a
                # restart; a saved local begin result is only provenance.
                manifest = spool.update(transfer_id, _locked=True, offset=offset)

            while offset < manifest["bytes"]:
                index = self._upload_chunk_index(manifest, offset)
                block = spool.read_chunk(transfer_id, index)
                action_key = str(uuid.uuid4())
                manifest = spool.update(
                    transfer_id,
                    _locked=True,
                    pending_action_key=action_key,
                    pending_offset=offset,
                    pending_length=len(block),
                )
                arguments = [begin["uploadId"], offset, base64.b64encode(block).decode("ascii"), hashlib.sha256(block).hexdigest()]
                result = self._run_transfer_step_locked(spool, manifest, "deposit.resource.uploadChunk", arguments, action_key)
                if not isinstance(result, dict) or type(result.get("nextOffset")) is not int or result["nextOffset"] != offset + len(block):
                    raise TransferError("Server did not confirm the uploaded chunk")
                offset = result["nextOffset"]
                manifest = spool.update(
                    transfer_id,
                    _locked=True,
                    offset=offset,
                    pending_action_key=None,
                    pending_offset=None,
                    pending_length=None,
                )

            finish_key = manifest["finish_action_key"]
            if finish_key is None:
                finish_key = str(uuid.uuid4())
                manifest = spool.update(transfer_id, _locked=True, finish_action_key=finish_key)
            result = self._run_transfer_step_locked(spool, manifest, "deposit.resource.finishUpload", [begin["uploadId"]], finish_key)
            if (
                not isinstance(result, dict)
                or result.get("state") != "ready"
                or not isinstance(result.get("resourceId"), str)
                or result.get("checksum") != manifest["sha256"]
                or result.get("bytes") != manifest["bytes"]
            ):
                raise TransferError("Server returned invalid completed resource metadata")
            result = {
                **result,
                "filename": manifest["filename"],
                "mediaType": manifest["media_type"],
                "checksum": manifest["sha256"],
                "bytes": manifest["bytes"],
                "state": "ready",
            }
            spool.update(transfer_id, _locked=True, state="finished", offset=manifest["bytes"])
            spool.cleanup_resolved(transfer_id, _locked=True)
            return result

    @staticmethod
    def _upload_chunk_index(manifest: dict[str, Any], offset: int) -> int:
        if type(offset) is not int or offset < 0:
            raise TransferError("Server upload offset is invalid")
        total = 0
        for index, chunk in enumerate(manifest["chunks"]):
            if total == offset:
                return index
            total += chunk["bytes"]
        if offset == total:
            return len(manifest["chunks"])
        raise TransferError("Server upload offset is not a frozen chunk boundary")

    def _execute_transfer_action(self, procedure: str, arguments: list[Any], key: str) -> dict[str, Any]:
        """Execute or replay one transfer action through the common coordinator."""
        record = self._journal.load_if_present(self._identity, key)
        if record is None:
            return self._run_action(procedure, arguments, _recovery_key=key, _already_serialized=True)
        if record["procedure"] != procedure or record["arguments"] != arguments:
            raise TransferError("Transfer action key is bound to different immutable inputs")
        if record["state"] != "released":
            return self._resolve_action_locked(record, allow_new_prepare=True)
        return self._replay_released_action(key)

    def _replay_released_action(self, key: str) -> dict[str, Any]:
        """Reattach a durable receipt and reissue its exact call for dynamic data."""
        with self._journal.action_lock(key):
            record = self._journal.load(self._identity, key)
            if record["state"] != "released":
                outcome = self._resolve_action_with_lock(record, allow_new_prepare=True)
                if not isinstance(outcome, dict):
                    raise TransferError("Recovered action returned an invalid result")
                return outcome
            self._control("deposit.action.resume", key)
            try:
                entry = store_procedure(record["procedure"])
                arguments = bind_arguments(entry, tuple(record["arguments"]), namespace_id=self._namespace_id)
                query, parameters = build_query(entry, arguments, self._token, portable=True)
                row = self._client._single(query, parameters)
                if tuple(row) != tuple(entry["columns"]):
                    raise DepositError("INVALID_RESPONSE", f"{record['procedure']} returned unexpected columns")
                result = _portable_input(row[entry["columns"][0]])
                if not isinstance(result, dict):
                    raise DepositError("INVALID_RESPONSE", f"{record['procedure']} returned no portable result")
                return result
            finally:
                self._control("deposit.action.release")

    def _resolve_action_locked(self, record: dict[str, Any], *, allow_new_prepare: bool) -> dict[str, Any]:
        with self._journal.action_lock(record["key"]):
            # Another process may have resolved or recorded a definitive
            # failure after this caller enumerated the journal.
            record = self._journal.load(self._identity, record["key"])
            return self._resolve_action_with_lock(record, allow_new_prepare=allow_new_prepare)

    def _resolve_action_with_lock(self, record: dict[str, Any], *, allow_new_prepare: bool) -> dict[str, Any]:
        key = record["key"]
        state = record["state"]
        if state == "conflicted":
            raise DepositError("REVISION_CONFLICT", "Pending action requires explicit refresh and reapplication")
        if state == "failed":
            raise _saved_failure_error(record["failure"])
        if state == "failure_saved":
            return self._finish_failed_action(record)

        if state in {"intent", "prepare_sent"}:
            self._journal.update(self._identity, key, state="prepare_sent")
            try:
                status = self._control("deposit.action.status", key)
            except DepositError as exc:
                if exc.code != "NOT_FOUND":
                    raise
                self._assert_preconditions_unchanged(record, allow_new_prepare)
                prepared = self._control("deposit.action.prepare", key, record["procedure"], record["arguments"])
            else:
                prepared = status
            if prepared.get("state") == "committed":
                return self._save_and_release(record, prepared)
            self._journal.update(self._identity, key, state="prepared", operation_id=prepared.get("operationId"))
            state = "prepared"

        if state in {"prepared", "executing"}:
            status = self._control("deposit.action.status", key)
            if status.get("state") == "committed":
                return self._save_and_release(record, status)
            resumed = self._control("deposit.action.resume", key)
            if resumed.get("state") == "committed":
                return self._save_and_release(record, resumed)
            self._journal.update(self._identity, key, state="executing", operation_id=resumed.get("operationId"))
            try:
                result = self._execute_entity_action(record)
            except DepositError as exc:
                status = self._control("deposit.action.status", key)
                if status.get("state") == "committed":
                    return self._save_and_release(record, status)
                if exc.code == "REVISION_CONFLICT":
                    self._release_conflict(record)
                    raise
                if (status.get("state") == "prepared"
                        and getattr(exc, "_server_reported", False) is True
                        and exc.retryable is False
                        and exc.code in _DEFINITIVE_DOMAIN_FAILURE_CODES):
                    failed = self._journal.update(
                        self._identity, key, state="failure_saved",
                        failure={"code": exc.code, "message": exc.message, "details": exc.details,
                                 "request_id": exc.request_id, "retryable": False},
                    )
                    return self._finish_failed_action(failed, status=status)
                raise
            return self._save_and_release(
                record,
                {"state": "committed", "operationId": resumed.get("operationId"), "outcome": result},
                outcome_is_named_rows=record.get("result_format") == RESULT_FORMAT_NAMED_ROWS_V1,
            )

        if state in {"outcome_saved", "release_sent"}:
            status = self._control("deposit.action.status", key)
            if status.get("state") not in {"prepared", "committed"}:
                raise DepositError("RECOVERY_PENDING", "Prepared action is unavailable; the saved outcome remains protected")
            self._control("deposit.action.resume", key)
            self._journal.update(self._identity, key, state="release_sent")
            self._control("deposit.action.release")
            saved = self._journal.update(self._identity, key, state="released")
            self._update_observed_revision()
            self._invalidate_handles()
            return saved["outcome"]

        if state == "released":
            return record["outcome"]
        raise JournalError("Recovery journal state cannot be resumed")

    def _finish_failed_action(self, record: dict[str, Any], *, status: dict[str, Any] | None = None):
        """Reconcile and detach a known rollback without executing it again."""
        key = record["key"]
        status = status if status is not None else self._control("deposit.action.status", key)
        if status.get("state") == "committed":
            return self._save_and_release(record, status)
        if status.get("state") != "prepared":
            raise DepositError("RECOVERY_PENDING", "Failed action release remains unconfirmed")
        resumed = self._control("deposit.action.resume", key)
        if resumed.get("state") == "committed":
            return self._save_and_release(record, resumed)
        if resumed.get("state") != "prepared":
            raise DepositError("RECOVERY_PENDING", "Failed action release remains unconfirmed")
        self._control("deposit.action.release")
        saved = self._journal.update(self._identity, key, state="failed")
        raise _saved_failure_error(saved["failure"])

    def _assert_preconditions_unchanged(self, record: dict[str, Any], allowed: bool) -> None:
        if not allowed:
            raise DepositError("RECOVERY_PENDING", "An absent prepared action cannot be recreated")
        entry = store_procedure(record["procedure"])
        # Operational preparations retain the same Store identity and exact
        # inputs, but deliberately do not capture a Namespace revision on the
        # server. Keep the observed revision as provenance; do not turn it into
        # a client-side precondition for recreating this same prepared key.
        if entry["scope"] == "operational":
            self._validate_store_session()
            return
        self._update_observed_revision()
        if self._observed_revision != record["snapshot_revision"]:
            current = self._journal.load(self._identity, record["key"])
            if current["state"] in {"intent", "prepare_sent"}:
                self._journal.update(self._identity, record["key"], state="conflicted")
            raise DepositError("REVISION_CONFLICT", "Namespace revision changed before the action was prepared")

    def _execute_entity_action(self, record: dict[str, Any]) -> dict[str, Any]:
        entry = store_procedure(record["procedure"])
        arguments = bind_arguments(entry, tuple(record["arguments"]), namespace_id=self._namespace_id)
        query, parameters = build_query(entry, arguments, self._token, portable=True)
        row = self._client._single(query, parameters)
        values = {}
        for column in entry["columns"]:
            value = row.get(column)
            # Stage 2's narrow fake client returns the converted entity as data;
            # real server calls return the manifest column name.
            if column not in row and column in {"node", "rel"}:
                value = row.get("data")
            values[column] = value
        if record.get("result_format") == RESULT_FORMAT_NAMED_ROWS_V1:
            return self._normalize_named_rows_result(
                entry,
                named_rows_result(tuple(entry["columns"]), [_portable_input(values)]),
                outcome_is_named_rows=True,
            )
        if len(entry["columns"]) == 1 and entry["columns"][0] in {"node", "rel"}:
            outcome = values[entry["columns"][0]]
            if not isinstance(outcome, dict):
                raise DepositError("INVALID_RESPONSE", "Mutation returned no portable entity")
            if entry["scope"] == "namespace" and outcome.get("namespaceId") != self._namespace_id:
                raise DepositError("INVALID_RESPONSE", "Mutation returned no portable entity in the bound Namespace")
            return _portable_input(outcome)
        if len(entry["columns"]) == 1:
            return _portable_input(values[entry["columns"][0]])
        return _portable_input(values)

    def _normalize_named_rows_result(
        self,
        entry: dict[str, Any],
        value: Any,
        *,
        outcome_is_named_rows: bool = False,
    ) -> dict[str, Any]:
        columns = tuple(entry["columns"])
        if outcome_is_named_rows:
            payload = value
        elif len(columns) == 1:
            payload = named_rows_result(columns, [{columns[0]: value}])
        elif isinstance(value, dict) and set(value) == set(columns):
            payload = named_rows_result(columns, [value])
        else:
            raise DepositError("INVALID_RESPONSE", "Prepared action has no portable named result")
        if (
            not isinstance(payload, dict)
            or set(payload) != {"columns", "rows"}
            or not isinstance(payload["columns"], list)
            or tuple(payload["columns"]) != columns
            or not isinstance(payload["rows"], list)
            or any(not isinstance(row, dict) or set(row) != set(columns) for row in payload["rows"])
        ):
            raise DepositError("INVALID_RESPONSE", "Prepared action has invalid named result rows")
        if entry["scope"] == "namespace":
            for row in payload["rows"]:
                for column in ("node", "rel"):
                    if column in row:
                        entity = row[column]
                        if not isinstance(entity, dict) or entity.get("namespaceId") != self._namespace_id:
                            raise DepositError("SCOPE_MISMATCH", "Prepared result belongs to another Namespace")
        return _portable_input(payload)

    def _save_and_release(
        self,
        record: dict[str, Any],
        status: dict[str, Any],
        *,
        outcome_is_named_rows: bool = False,
    ) -> dict[str, Any]:
        outcome = status.get("outcome")
        if not isinstance(outcome, dict):
            raise DepositError("INVALID_RESPONSE", "Prepared action has no saved portable outcome")
        entry = store_procedure(record["procedure"])
        if record.get("result_format") == RESULT_FORMAT_NAMED_ROWS_V1:
            outcome = self._normalize_named_rows_result(
                entry,
                outcome,
                outcome_is_named_rows=outcome_is_named_rows,
            )
        elif (
            entry["scope"] == "namespace"
            and len(entry["columns"]) == 1
            and entry["columns"][0] in {"node", "rel"}
        ):
            if outcome.get("namespaceId") != self._namespace_id:
                raise DepositError("SCOPE_MISMATCH", "Recovered outcome belongs to another Namespace")
        else:
            outcome = _portable_input(outcome)
        key = record["key"]
        current = self._journal.load(self._identity, key)
        if current["state"] not in {"outcome_saved", "release_sent", "released"}:
            failure_changes = {"failure": None} if "failure" in current else {}
            current = self._journal.update(
                self._identity,
                key,
                state="outcome_saved",
                outcome=_portable_input(outcome),
                operation_id=status.get("operationId"),
                **failure_changes,
            )
        if current["state"] == "released":
            return current["outcome"]
        # A resumed Store has a new token/session, so even a committed receipt
        # must be attached to that session before action.release can confirm it.
        self._control("deposit.action.resume", key)
        self._journal.update(self._identity, key, state="release_sent")
        self._control("deposit.action.release")
        saved = self._journal.update(self._identity, key, state="released")
        self._update_observed_revision()
        self._invalidate_handles()
        return saved["outcome"]

    def _release_conflict(self, record: dict[str, Any]) -> None:
        self._control("deposit.action.release")
        current = self._journal.load(self._identity, record["key"])
        if current["state"] in {"prepared", "executing", "prepare_sent"}:
            self._journal.update(self._identity, record["key"], state="conflicted")

    def _control(self, procedure: str, *arguments: Any) -> dict[str, Any]:
        placeholders = ", ".join(f"$a{i}" for i in range(len(arguments)))
        if placeholders:
            placeholders += ", "
        params = {f"a{i}": value for i, value in enumerate(arguments)}
        params["token"] = self._token
        column = "value" if procedure == "deposit.action.release" else "data"
        query = f"CYPHER 25 CALL {procedure}({placeholders}$token) YIELD {column} RETURN {column}"
        row = self._client._single(query, params)
        value = row.get(column)
        if procedure == "deposit.action.release":
            if value is not True:
                raise DepositError("RELEASE_UNCONFIRMED", "Prepared action release was not confirmed")
            return {"released": True}
        if not isinstance(value, dict):
            raise DepositError("INVALID_RESPONSE", f"{procedure} returned no recovery state")
        return value

    def _update_observed_revision(self) -> None:
        session = self._validate_store_session()
        revision = self._get_revision(session.get("observed"))
        if revision is None:
            raise DepositError("REVISION_UNAVAILABLE", "Server returned no Namespace revision baseline")
        self._observed_revision = revision

    def _validate_store_session(self) -> dict[str, Any]:
        session = self._client.get_session(self._token)
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


def _freeze_import_rows(rows: Any) -> list[dict[str, Any]]:
    if not isinstance(rows, (list, tuple)):
        raise TypeError("rows must be a list or tuple of row records")
    frozen = []
    seen = set()
    for row in rows:
        if not isinstance(row, Mapping) or set(row) != {"id", "values"}:
            raise TypeError("each row must contain exactly an id and values mapping")
        row_id = row["id"]
        if not isinstance(row_id, str) or not row_id:
            raise ValueError("row IDs must be nonempty strings")
        if "/" in row_id:
            raise ValueError("row IDs cannot contain a slash")
        if row_id in seen:
            raise ValueError("row IDs must be unique within one submission")
        values = row["values"]
        if not isinstance(values, Mapping) or any(not isinstance(name, str) for name in values):
            raise TypeError("each row values field must be a string-keyed values mapping")
        seen.add(row_id)
        frozen.append({"id": row_id, "values": dict(values)})
    return frozen


def _validated_import_plan(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise DepositError("INVALID_RESPONSE", "Import analysis returned no portable plan")
    plan_id = value.get("planId")
    target_revision = value.get("targetRevision")
    if not isinstance(plan_id, str) or not plan_id:
        raise DepositError("INVALID_RESPONSE", "Import analysis returned no executable plan ID")
    if type(target_revision) is not int or target_revision < 0:
        raise DepositError("INVALID_RESPONSE", "Import analysis returned no target revision")
    if not isinstance(value.get("groups"), list) or not isinstance(value.get("summary"), dict):
        raise DepositError("INVALID_RESPONSE", "Import analysis returned invalid plan diagnostics")
    return value


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
        raise TypeError("Stage 2 recovery accepts only losslessly JSON-portable values") from exc


def _validate_portable_value(value: Any) -> None:
    """Reject values JSON would coerce or silently alter before journaling them."""
    if value is None or type(value) in {bool, int, str}:
        return
    if type(value) is float:
        if not math.isfinite(value):
            raise TypeError("Stage 2 recovery accepts only losslessly JSON-portable values")
        return
    if isinstance(value, Mapping):
        if type(value) is not dict:
            raise TypeError("Stage 2 recovery accepts only built-in JSON mappings")
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError("Stage 2 recovery requires string mapping keys")
            _validate_portable_value(item)
        return
    if type(value) is list:
        for item in value:
            _validate_portable_value(item)
        return
    raise TypeError("Stage 2 recovery accepts only losslessly JSON-portable values")


def _session_already_unavailable(exc: Exception) -> bool:
    return isinstance(exc, DepositError) and exc.code in {"AUTH_FAILED", "AUTH_REQUIRED", "SESSION_EXPIRED", "NOT_FOUND"}
