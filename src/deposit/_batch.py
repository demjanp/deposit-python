"""Atomic, bounded command collector for ``deposit.apply.batch``."""

from __future__ import annotations

import json
import uuid
from collections.abc import Mapping
from typing import Any

from ._gateway import domain_arguments, store_procedure
from .store import _portable_input
from .values import DType, TypedValue, encode_value


MAX_COMMANDS = 1_000
MAX_REQUEST_BYTES = 8 * 1024 * 1024
_SERVER_IDENTITY_BINDINGS = {"id", "objectId", "classId", "sourceId", "targetId"}


class BatchReference:
    """Opaque identity reference to an earlier command in its owning Batch."""

    __slots__ = ("_owner", "_key")

    def __init__(self, owner: "Batch", key: str):
        self._owner = owner
        self._key = key

    def __repr__(self) -> str:
        return "<Deposit batch reference>"


class Batch:
    def __init__(self, store):
        self._store = store
        self._geometry_encoder = getattr(store, "_geometry_write_value", None)
        self._resource_encoder = getattr(store, "_resource_write_value", None)
        self._commands: list[dict[str, Any]] = []
        self._keys: dict[BatchReference, str] = {}
        self._pinned_revision: int | None = None
        self._refresh_handles = ()
        self._results: dict[str, Any] | None = None
        self._entered = False
        self._closed = False

    def __enter__(self) -> "Batch":
        if self._entered or self._closed:
            raise RuntimeError("Batch contexts cannot be reused")
        self._store._begin_batch_collection(self)
        self._entered = True
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        try:
            if exc_type is None:
                # Revalidate and detach the complete request immediately before
                # submission. This also protects against accidental mutation of
                # private collector state between individual calls.
                commands = self._freeze_commands(self._commands)
                options = {"refresh_handles": self._refresh_handles} if self._refresh_handles else {}
                self._results = self._store._submit_batch(commands, pinned_revision=self._pinned_revision, **options)
        finally:
            self._closed = True
            self._store._end_batch_collection(self)

    def call(self, procedure: str, *arguments: Any) -> BatchReference:
        self._ensure_collecting()
        entry = domain_arguments(store_procedure(procedure))
        if entry.get("action") is not True or entry.get("batch_eligible") is not True:
            raise ValueError(f"{procedure!r} is not eligible for an atomic batch")
        if len(arguments) != len(entry["args"]):
            raise TypeError(f"{procedure} requires exactly {len(entry['args'])} batch arguments")
        index = len(self._commands)
        if index >= MAX_COMMANDS:
            raise ValueError(f"A batch may contain at most {MAX_COMMANDS} commands")
        key = f"c{index}_{uuid.uuid4().hex}"
        args = [self._encode_argument(entry, i, value) for i, value in enumerate(arguments)]
        command = {"key": key, "procedure": procedure, "arguments": args}
        descriptor_types = self._encode_descriptors(procedure, arguments, args)
        if descriptor_types:
            command["descriptorTypes"] = descriptor_types
        # Freeze before storing. Caller-owned dictionaries/lists can change
        # immediately after this method returns.
        candidate = self._freeze_commands([*self._commands, command])
        reference = BatchReference(self, key)
        self._commands = candidate
        self._keys[reference] = key
        return reference

    def create_class(self, name: str, properties: dict[str, Any] | None = None) -> BatchReference:
        return self.call("deposit.create.class", name, {} if properties is None else properties)

    def set_class(self, selector: Any, patch: dict[str, Any]) -> BatchReference:
        if _is_entity_handle(selector, "Class"):
            self._pin_handle(selector, "Class")
            selector = {"id": selector.id}
        return self.call("deposit.set.class", selector, patch)

    def delete_class(self, selector: Any) -> BatchReference:
        if _is_entity_handle(selector, "Class"):
            self._pin_handle(selector, "Class")
            selector = {"id": selector.id}
        return self.call("deposit.delete.class", selector)

    def create_object(self, class_selector: Any, properties: dict[str, Any] | None = None) -> BatchReference:
        if _is_entity_handle(class_selector, "Class"):
            self._pin_handle(class_selector, "Class")
            class_selector = {"id": class_selector.id}
        return self.call("deposit.create.object", class_selector, {} if properties is None else properties)

    def set_object(self, object_id: Any, patch: dict[str, Any]) -> BatchReference:
        return self.call("deposit.set.object", self._object_id(object_id), patch)

    def delete_object(self, object_id: Any) -> BatchReference:
        if _is_entity_handle(object_id, "Object"):
            self._pin_handle(object_id, "Object")
            object_id = object_id.id
        return self.call("deposit.delete.object", object_id)

    def create_class_descriptor(self, selector: Any, name: str, type_name: str, properties=None) -> BatchReference:
        if _is_entity_handle(selector, "Class"):
            self._pin_handle(selector, "Class")
            selector = {"id": selector.id}
        return self.call("deposit.create.classDescriptor", selector, name, type_name,
                         {} if properties is None else properties)

    def create_descriptor(self, object_id: Any, name: str, value: Any) -> BatchReference:
        return self.call("deposit.create.descriptor", self._object_id(object_id), name, value)

    def set_descriptor(self, object_id: Any, name: str, value: Any) -> BatchReference:
        return self.call("deposit.set.descriptor", self._object_id(object_id), name, value)

    def result(self, reference: BatchReference) -> Any:
        if self._results is None:
            raise RuntimeError("Batch results are available only after successful submission")
        key = self._reference_key(reference)
        if key not in self._results:
            raise KeyError("Batch result is unavailable")
        value = self._results[key]
        return json.loads(json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":")))

    @property
    def commands(self) -> tuple[dict[str, Any], ...]:
        return tuple(self._freeze_commands(self._commands))

    def _object_id(self, selector):
        if _is_entity_handle(selector, "Object"):
            self._pin_handle(selector, "Object")
            return selector.id
        return selector

    def _encode_descriptors(self, procedure, arguments, encoded):
        # Carry explicit types separately so a scientific dictionary containing
        # "type" and "value" keys is still an ordinary descriptor value.
        def is_typed(value):
            return isinstance(value, (TypedValue, DType))

        if procedure in {"deposit.create.descriptor", "deposit.set.descriptor"} and is_typed(arguments[2]):
            typed = encoded[2]
            encoded[2] = typed["value"]
            return {arguments[1]: typed["type"]}
        if procedure in {"deposit.create.object", "deposit.set.object"} and isinstance(arguments[1], Mapping):
            descriptors = arguments[1].get("descriptors")
            if isinstance(descriptors, Mapping):
                values = encoded[1]["descriptors"]
                types = {}
                for name, value in descriptors.items():
                    if is_typed(value):
                        typed = values[name]
                        types[name] = typed["type"]
                        values[name] = typed["value"]
                return types
        return {}

    def _pin_handle(self, handle, kind: str) -> None:
        self._store._validate_handle(handle, kind)
        if handle.stale:
            raise ValueError("Stale entity handles cannot be used in a batch; refresh before collection")
        revision = handle._baseline_revision
        if type(revision) is not int or revision < 0:
            raise ValueError("Entity handle has no checked snapshot revision")
        if self._pinned_revision is not None and revision != self._pinned_revision:
            raise ValueError("Batch handles have conflicting checked snapshot revisions")
        self._pinned_revision = revision

    def _encode_argument(self, entry, index, value):
        spec = entry["args"][index]
        if isinstance(value, BatchReference):
            key = self._reference_key(value)
            binding = spec["binding"]
            identity_name = binding.startswith("@") or binding in _SERVER_IDENTITY_BINDINGS
            if not identity_name:
                raise ValueError("Batch references are allowed only in server identity argument positions")
            return {"ref": key, "field": "id"}
        if _contains_reference(value):
            raise ValueError("Batch references must occupy a top-level identity argument")
        binding = spec["binding"]
        if (binding.startswith("@") or binding in _SERVER_IDENTITY_BINDINGS) and _contains_ref_marker(value):
            raise ValueError("Batch references must use an earlier command's opaque BatchReference")
        return _wire_values(value, geometry_encoder=self._geometry_encoder,
                            resource_encoder=self._resource_encoder)

    def _reference_key(self, reference: BatchReference) -> str:
        if not isinstance(reference, BatchReference) or reference._owner is not self:
            raise ValueError("Batch reference belongs to another Batch")
        key = self._keys.get(reference)
        if key is None or not any(command["key"] == key for command in self._commands):
            raise ValueError("Batch reference is not owned by an earlier command")
        return key

    def _freeze_commands(self, commands):
        # JSON round-tripping both validates that the exact command is
        # portable and returns a detached copy immune to caller mutation.
        try:
            frozen = _portable_input(commands)
            encoded = json.dumps(frozen, ensure_ascii=False, allow_nan=False,
                                 separators=(",", ":")).encode("utf-8")
        except (TypeError, ValueError, OverflowError) as exc:
            raise TypeError("Batch commands must be losslessly JSON-portable") from exc
        if len(encoded) > MAX_REQUEST_BYTES:
            raise ValueError(f"Encoded batch exceeds the {MAX_REQUEST_BYTES}-byte request limit")
        if len(frozen) > MAX_COMMANDS:
            raise ValueError(f"A batch may contain at most {MAX_COMMANDS} commands")
        prior_keys = set()
        for command in frozen:
            if (not isinstance(command, dict)
                    or set(command) - {"key", "procedure", "arguments", "descriptorTypes"}
                    or not {"key", "procedure", "arguments"} <= command.keys()):
                raise ValueError("Batch command has an invalid shape")
            key, procedure, arguments = command["key"], command["procedure"], command["arguments"]
            if not isinstance(key, str) or not key or key in prior_keys:
                raise ValueError("Batch command keys must be unique nonempty strings")
            entry = domain_arguments(store_procedure(procedure))
            if entry.get("action") is not True or entry.get("batch_eligible") is not True:
                raise ValueError(f"{procedure!r} is not eligible for an atomic batch")
            if not isinstance(arguments, list) or len(arguments) != len(entry["args"]):
                raise ValueError(f"{procedure} has an invalid batch argument list")
            for spec, argument in zip(entry["args"], arguments):
                binding = spec["binding"]
                identity_name = binding.startswith("@") or binding in _SERVER_IDENTITY_BINDINGS
                tokens = _ref_tokens(argument)
                if tokens and identity_name:
                    if (not identity_name or not isinstance(argument, dict)
                            or set(argument) != {"ref", "field"} or argument.get("field") != "id"
                            or len(tokens) != 1 or tokens[0] not in prior_keys):
                        raise ValueError("Batch references must target an earlier command in an identity position")
            prior_keys.add(key)
        return frozen

    def _ensure_collecting(self):
        if not self._entered or self._closed:
            raise RuntimeError("Batch commands can be added only inside its transaction context")


def _is_entity_handle(value: Any, kind: str) -> bool:
    from .entities import DNode
    return isinstance(value, DNode) and value.kind == kind


def _wire_values(value: Any, *, geometry_encoder=None, resource_encoder=None) -> Any:
    """Convert explicit public typed values without guessing server schema."""
    if isinstance(value, (TypedValue, DType)):
        kind = value.type if isinstance(value, TypedValue) else value._type
        if kind == "DGeometry" and geometry_encoder is not None:
            raw = value.value if isinstance(value, TypedValue) else value
            return {"type": kind, "value": encode_value(kind, geometry_encoder(raw))}
        if kind == "DResource" and resource_encoder is not None:
            raw = value.value if isinstance(value, TypedValue) else value
            return {"type": kind, "value": encode_value(kind, resource_encoder(raw))}
        return value.to_dict()
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise TypeError("Batch map keys must be strings")
        return {key: _wire_values(child, geometry_encoder=geometry_encoder,
                                  resource_encoder=resource_encoder) for key, child in value.items()}
    if isinstance(value, (list, tuple)):
        return [_wire_values(child, geometry_encoder=geometry_encoder,
                             resource_encoder=resource_encoder) for child in value]
    return value


def _contains_reference(value: Any) -> bool:
    if isinstance(value, BatchReference):
        return True
    if isinstance(value, Mapping):
        return any(_contains_reference(child) for child in value.values())
    if isinstance(value, (list, tuple)):
        return any(_contains_reference(child) for child in value)
    return False


def _ref_tokens(value: Any) -> list[str]:
    if isinstance(value, Mapping):
        tokens = []
        if "ref" in value:
            token = value.get("ref")
            tokens.append(token if isinstance(token, str) else "")
        for child in value.values():
            tokens.extend(_ref_tokens(child))
        return tokens
    if isinstance(value, (list, tuple)):
        return [token for child in value for token in _ref_tokens(child)]
    return []


def _contains_ref_marker(value: Any) -> bool:
    return bool(_ref_tokens(value))
