"""Stable-UUID, Store-bound entity handles with detached editable snapshots."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from copy import deepcopy
from typing import Any

from .errors import DepositError
from .values import decode_value


class DNode(Mapping):
    """Base stable identity handle; it never stores Neo4j internal IDs."""

    _kind = ""

    def __init__(self, store, data: Mapping[str, Any], schema: Mapping[str, str] | None = None,
                 *, snapshot_revision: int | None = None):
        if isinstance(data, str):
            if not data:
                raise ValueError("Entity UUID must be a nonempty string")
            data, schema, snapshot_revision = store._checked_entity_snapshot(self._kind, {"id": data})
        if not isinstance(data, Mapping) or data.get("kind") != self._kind:
            raise TypeError(f"Expected portable {self._kind} data")
        identity = data.get("id")
        namespace_id = data.get("namespaceId")
        if not isinstance(identity, str) or not identity or namespace_id != store.namespace_id:
            raise DepositError("SCOPE_MISMATCH", "Entity does not belong to this Store Namespace")
        self._store = store
        self._id = identity
        self._namespace_id = namespace_id
        self._snapshot = deepcopy(dict(data))
        self._schema = dict(schema or {})
        self._baseline_revision = snapshot_revision
        self._pending: dict[str, Any] = {}
        self._stale = False
        register = getattr(store, "_register_handle", None)
        if register is not None:
            register(self)

    @property
    def id(self) -> str:
        return self._id

    @property
    def namespace_id(self) -> str:
        return self._namespace_id

    @property
    def uuid(self) -> str:
        """Read-only compatibility alias for the stable public ID."""
        return self._id

    @property
    def kind(self) -> str:
        return self._kind

    @property
    def stale(self) -> bool:
        return self._stale

    @property
    def pending(self) -> Mapping[str, Any]:
        return deepcopy(self._pending)

    @property
    def name(self):
        self._ensure_current()
        return self._snapshot.get("name")

    @property
    def position(self) -> tuple[float, float]:
        self._ensure_current()
        value = self._snapshot.get("position", (0.0, 0.0))
        if not isinstance(value, (list, tuple)) or len(value) < 2:
            raise DepositError("INVALID_RESPONSE", "Entity position is invalid")
        return float(value[0]), float(value[1])

    @position.setter
    def position(self, value) -> None:
        if not isinstance(value, (list, tuple)) or len(value) != 2:
            raise TypeError("position must contain exactly two coordinates")
        self._store.set_position(self, value[0], value[1])

    @property
    def order(self) -> int:
        self._ensure_current()
        if self._kind != "Class":
            raise AttributeError("Only Class handles have an order")
        return self._snapshot.get("order", 0)

    @order.setter
    def order(self, value: int) -> None:
        self._store.set_class_properties(self, order=value)

    @property
    def comment(self) -> str:
        self._ensure_current()
        if self._kind != "Class":
            raise AttributeError("Only Class handles have a comment")
        return self._snapshot.get("comment", "")

    @comment.setter
    def comment(self, value: str) -> None:
        self._store.set_class_properties(self, comment=value)

    def __getitem__(self, key: str) -> Any:
        # Stable identity and scope do not depend on the cached data revision.
        # Keep those fields available to callers using a handle as a selector.
        if key == "id":
            return self._id
        if key == "namespaceId":
            return self._namespace_id
        if key == "kind":
            return self._kind
        self._ensure_current()
        if key == "descriptors" and self._kind == "Object":
            return self._decoded_descriptors()
        if key in self._snapshot:
            return deepcopy(self._snapshot[key])
        if self._kind in {"Class", "Object"}:
            return self._descriptor(key)
        raise KeyError(key)

    def __iter__(self) -> Iterator[str]:
        self._ensure_current()
        names = set(self._snapshot)
        if self._kind == "Object":
            names.update(self._snapshot.get("descriptors", {}))
        elif self._kind == "Class":
            names.update(self.declarations)
        return iter(sorted(names))

    def __len__(self) -> int:
        return sum(1 for _ in self.__iter__())

    def __eq__(self, other):
        if isinstance(other, DNode):
            return self._kind == other._kind and self._id == other._id and self._namespace_id == other._namespace_id
        if isinstance(other, Mapping):
            try:
                return {key: self[key] for key in self} == dict(other)
            except (KeyError, RuntimeError):
                return False
        return NotImplemented

    def __hash__(self):
        return hash((self._namespace_id, self._kind, self._id))

    def _ensure_current(self):
        if self._stale or self._snapshot is None:
            raise RuntimeError("Entity snapshot is stale; call refresh() before reading or editing")

    def _descriptor(self, name: str):
        self._ensure_current()
        if self._kind == "Class":
            declaration = self.declarations.get(name)
            if declaration is None:
                raise KeyError(name)
            return deepcopy(declaration)
        values = self._snapshot.get("descriptors", {})
        if name not in values:
            raise KeyError(name)
        return deepcopy(values[name])

    def get(self, name: str, default=None):
        try:
            return self._descriptor(name)
        except KeyError:
            return default

    def __setitem__(self, name: str, value: Any) -> None:
        if self._kind != "Object":
            raise TypeError("Only Object handles assign Descriptor values")
        self._ensure_current()
        if not isinstance(name, str) or not name:
            raise ValueError("Descriptor name must be a nonempty string")
        # Prepare (including Server-owned DGeometry resolution) before retaining
        # this as a pending edit. A type/value error is not unresolved intent.
        prepare = getattr(self._store, "_prepare_descriptor_payload", None)
        submit = getattr(self._store, "_submit_descriptor_payload", None)
        if prepare is None or submit is None:
            from .store import _descriptor_payload
            _descriptor_payload(value, self._schema.get(name))
            payload = None
        else:
            payload = prepare(value, self._schema.get(name))
        self._pending[name] = deepcopy(value)
        if payload is None:
            self._store.set_descriptor(self, name, value)
        else:
            submit(self, name, payload)
        self._pending.pop(name, None)

    def __delitem__(self, name: str) -> None:
        if self._kind not in {"Object", "Class"}:
            raise TypeError("Only Class or Object Descriptor values can be deleted")
        self._ensure_current()
        self._store._delete_descriptor(self, name)

    def exists(self) -> bool:
        self._ensure_current()
        try:
            if self._kind == "Class":
                self._store.get_class({"id": self._id})
            else:
                self._store.get_object({"id": self._id})
        except DepositError as exc:
            if exc.code == "NOT_FOUND":
                return False
            raise
        return True

    def get_descriptor_comment(self, name: str) -> str:
        if self._kind != "Class":
            raise AttributeError("Only Class handles have Descriptor comments")
        declaration = self.declarations.get(name)
        if declaration is None:
            raise KeyError(name)
        return declaration.get("comment", "")

    def set_descriptor_comment(self, name: str, comment: str) -> None:
        if self._kind != "Class":
            raise AttributeError("Only Class handles have Descriptor comments")
        declaration = self.declarations.get(name)
        if declaration is None:
            raise KeyError(name)
        self._store.set_class_descriptor({"id": declaration["id"]}, {"comment": comment}, dclass=self)

    def _decoded_descriptors(self):
        values = self._snapshot.get("descriptors", {})
        return {name: deepcopy(value) for name, value in values.items()}

    def _mark_stale(self):
        self._snapshot = None
        self._schema = {}
        self._stale = True

    def refresh(self):
        pending = deepcopy(self._pending)
        self._store.refresh()
        data, schema, revision = self._store._checked_entity_snapshot(self._kind, {"id": self._id})
        self._snapshot = deepcopy(data)
        self._schema = dict(schema)
        self._baseline_revision = revision
        self._stale = False
        self._pending = pending
        return self

    def reapply(self):
        self._ensure_current()
        for name, value in tuple(self._pending.items()):
            self._store.set_descriptor(self, name, value)
            self._pending.pop(name, None)
        return self

    def as_dict(self) -> dict[str, Any]:
        self._ensure_current()
        return deepcopy(self._snapshot)

    def __repr__(self):
        return f"<{self._kind} id={self._id!r} namespace_id={self._namespace_id!r}>"


class DClass(DNode):
    _kind = "Class"

    @property
    def declarations(self) -> dict[str, dict[str, Any]]:
        self._ensure_current()
        raw = self._snapshot.get("descriptors", [])
        if isinstance(raw, list):
            return {item["name"]: deepcopy(item) for item in raw if isinstance(item, dict) and isinstance(item.get("name"), str)}
        return {name: {"name": name, "type": kind} for name, kind in self._schema.items()}

    def __setitem__(self, name: str, type_name: str) -> None:
        self._ensure_current()
        if not isinstance(name, str) or not name or not isinstance(type_name, str) or not type_name:
            raise TypeError("Class Descriptor assignment requires a name and type name")
        declaration = self.declarations.get(name)
        if declaration is not None:
            if declaration.get("type") != type_name:
                raise ValueError("Changing an existing Descriptor type requires an explicit conversion")
            return
        self._store.create_class_descriptor(self, name, type_name)

    def switch_descriptor_order(self, name1: str, name2: str):
        return self._store.switch_descriptor_order(self, name1, name2)


class DObject(DNode):
    _kind = "Object"

    @property
    def class_id(self) -> str:
        self._ensure_current()
        return self._snapshot["classId"]

    def get_class(self) -> DClass:
        return self._store.object_class(self)

    def set_class(self, dclass: DClass | str | Mapping[str, Any]):
        return self._store.set_membership(self, dclass)

    def has_class(self, dclass: DClass | str | Mapping[str, Any]) -> bool:
        if isinstance(dclass, DClass):
            self._store._validate_handle(dclass, "Class")
            class_id = dclass.id
        else:
            class_id = self._store.get_class_handle(dclass).id
        return self.class_id == class_id

    def get_descriptor_type(self, name: str) -> str:
        self._ensure_current()
        if name not in self._snapshot.get("descriptors", {}):
            raise KeyError(name)
        try:
            return self._schema[name]
        except KeyError as exc:
            raise DepositError("INVALID_RESPONSE", "Descriptor has no declaration in its checked schema") from exc


def decode_snapshot(data: Mapping[str, Any], schema_data: Mapping[str, Any], namespace_id: str, store,
                    *, snapshot_revision: int | None = None):
    """Decode Object Descriptor payloads against declarations from the same read transaction."""
    if not isinstance(data, Mapping) or data.get("namespaceId") != namespace_id:
        raise DepositError("SCOPE_MISMATCH", "Snapshot belongs to another Store Namespace")
    raw_declarations = schema_data.get("descriptors", []) if isinstance(schema_data, Mapping) else []
    declarations = {}
    for item in raw_declarations:
        if not isinstance(item, Mapping) or not isinstance(item.get("name"), str) or not isinstance(item.get("type"), str):
            raise DepositError("INVALID_RESPONSE", "Class snapshot returned invalid declarations")
        declarations[item["name"]] = item["type"]
    copied = deepcopy(dict(data))
    descriptors = copied.get("descriptors", {})
    if not isinstance(descriptors, Mapping):
        raise DepositError("INVALID_RESPONSE", "Object snapshot returned invalid Descriptors")
    decoded = {}
    for name, payload in descriptors.items():
        kind = declarations.get(name)
        if kind is None:
            raise DepositError("INVALID_RESPONSE", "Object Descriptor has no declaration in the checked snapshot")
        try:
            decoded[name] = decode_value(kind, payload)
        except (TypeError, ValueError, OverflowError) as exc:
            raise DepositError("INVALID_RESPONSE", "Object Descriptor payload does not match its checked declaration") from exc
    copied["descriptors"] = decoded
    schema = declarations
    handle_type = DObject if copied.get("kind") == "Object" else DClass
    return handle_type(store, copied, schema, snapshot_revision=snapshot_revision)
