"""Manifest-backed namespace Store calls and their transaction-bound results."""

from __future__ import annotations

from collections.abc import ItemsView, Iterator, KeysView, Mapping, ValuesView
from importlib.resources import files
import json
import math
import re
import threading
from types import MappingProxyType
from typing import Any
from uuid import UUID

from neo4j.graph import Node, Path, Relationship
from neo4j.exceptions import Neo4jError

from .errors import DepositError, TransportError


_MANIFEST: dict[str, Any] | None = None

_CLIENT_GATEWAY_PROCEDURES = frozenset(
    {
        "deposit.action.cancel",
        "deposit.get.operation",
        "deposit.get.activeNamespace",
        "deposit.set.activeNamespace",
        "deposit.get.administrativeMode",
        "deposit.set.administrativeMode",
        "deposit.get.user",
        "deposit.get.userPage",
        "deposit.get.namespace",
        "deposit.get.namespacePage",
        "deposit.get.importResult",
        "deposit.get.importResultPage",
        "deposit.get.importResultEntry",
        "deposit.admin.pruneResourcesStatus",
        "deposit.utils.coordinatesToWKT",
        "deposit.utils.wktToCoordinates",
    }
)

_STORE_CONTROL_PROCEDURES = {
    "deposit.get.requests": "SessionControl",
    "deposit.get.lastOperation": "SessionControl",
    "deposit.utils.cancel": "SessionControl",
    "deposit.utils.cancelRequest": "SessionControl",
}

def _manifest() -> dict[str, Any]:
    global _MANIFEST
    if _MANIFEST is None:
        _MANIFEST = json.loads(files("deposit").joinpath("_procedure_manifest.json").read_text(encoding="utf-8"))
    return _MANIFEST


def store_procedure(name: str) -> dict[str, Any]:
    if not isinstance(name, str):
        raise TypeError("procedure must be a manifest procedure name")
    entry = next((item for item in _manifest()["procedures"] if item["name"] == name), None)
    if (
        entry is None
        or entry.get("invocation_owner") != "Store"
        or entry.get("scope") not in {"namespace", "operational"}
        or entry.get("requires_token") is not True
    ):
        raise ValueError("procedure is not an allowlisted Store procedure")
    return entry


def client_procedure(name: str) -> dict[str, Any]:
    """Return only explicitly reviewed Client-owned, non-action server routes."""
    if not isinstance(name, str):
        raise TypeError("procedure must be a manifest procedure name")
    entry = next((item for item in _manifest()["procedures"] if item["name"] == name), None)
    if (
        name not in _CLIENT_GATEWAY_PROCEDURES
        or entry is None
        or entry.get("invocation_owner") != "Client"
        or entry.get("scope") != "server"
        or entry.get("requires_token") is not True
        or entry.get("action") is not False
    ):
        raise ValueError("procedure is not an allowlisted Client procedure")
    return entry


def store_control_procedure(name: str) -> dict[str, Any]:
    """Return only reviewed controls that may run beside a busy Store request."""
    if not isinstance(name, str):
        raise TypeError("procedure must be a manifest procedure name")
    entry = next((item for item in _manifest()["procedures"] if item["name"] == name), None)
    if (
        entry is None
        or _STORE_CONTROL_PROCEDURES.get(name) != entry.get("invocation_owner")
        or entry.get("scope") != "server"
        or entry.get("requires_token") is not True
        or entry.get("action") is not False
    ):
        raise ValueError("procedure is not an allowlisted Store control")
    return entry


def immutable_request(value: Any) -> Mapping[str, Any]:
    """Validate and detach the stable public request identity fields."""
    if not isinstance(value, Mapping):
        raise DepositError("INVALID_RESPONSE", "Request listing returned an invalid request")
    request_id = value.get("requestId")
    actor_id = value.get("actorId")
    namespace_id = value.get("namespaceId")
    if (
        not isinstance(request_id, str)
        or not request_id
        or not isinstance(actor_id, str)
        or not actor_id
        or (namespace_id is not None and not isinstance(namespace_id, str))
        or set(value) != {"requestId", "actorId", "namespaceId"}
    ):
        raise DepositError("INVALID_RESPONSE", "Request listing returned an invalid request")
    return MappingProxyType({"requestId": request_id, "actorId": actor_id, "namespaceId": namespace_id})


def tracked_procedure(name: str) -> dict[str, Any]:
    entry = next((item for item in _manifest()["procedures"] if item["name"] == name), None)
    if (entry is None or entry.get("tracked") not in {"always", "conditional"}
            or not entry["args"] or entry["args"][-1]["name"] != "operationId"):
        raise ValueError("Procedure does not accept a tracked operation")
    return entry


def domain_arguments(entry: dict[str, Any]) -> dict[str, Any]:
    """Expose domain arguments without the transport-owned operation ID."""
    if entry.get("tracked") in {"always", "conditional"}:
        return {**entry, "args": entry["args"][:-1]}
    return entry


def bind_client_arguments(entry: dict[str, Any], supplied: tuple[Any, ...]) -> tuple[Any, ...]:
    specs = entry["args"]
    if len(supplied) > len(specs):
        raise TypeError(f"{entry['name']} accepts at most {len(specs)} positional arguments")
    values = list(supplied)
    while len(values) < len(specs):
        spec = specs[len(values)]
        binding = spec["binding"]
        if "default" in spec:
            values.append(spec["default"])
        elif binding.startswith(("?", "~")):
            values.append(None)
        elif binding == "+":
            values.append({})
        else:
            raise TypeError(f"{entry['name']} is missing required argument {spec['name']!r}")
    for value in values:
        _reject_injected_values(value)
    return tuple(values)


def bind_arguments(
    entry: dict[str, Any], supplied: tuple[Any, ...], *, namespace_id: str | None = None
) -> tuple[Any, ...]:
    specs = entry["args"]
    if len(supplied) > len(specs):
        raise TypeError(f"{entry['name']} accepts at most {len(specs)} positional arguments")
    values = list(supplied)
    while len(values) < len(specs):
        spec = specs[len(values)]
        binding = spec["binding"]
        if "default" in spec:
            values.append(spec["default"])
        elif binding.startswith(("?", "~")):
            values.append(None)
        elif binding == "+":
            values.append({})
        else:
            raise TypeError(f"{entry['name']} is missing required argument {spec['name']!r}")
    for spec, value in zip(specs, values):
        _reject_injected_values(value)
        binding = spec["binding"]
        if binding.startswith("?") and value is None:
            continue
        if binding.startswith("@") or binding.startswith("?"):
            _validate_selector_provenance(value, binding, entry["name"], namespace_id)
    return tuple(values)


def build_query(entry: dict[str, Any], arguments: tuple[Any, ...], token: str, *, portable: bool = False) -> tuple[str, dict[str, Any]]:
    params = {f"a{index}": value for index, value in enumerate(arguments)}
    call_args = [f"$a{index}" for index in range(len(arguments))]
    token_index = entry["token_index"]
    if token_index is not None:
        params["token"] = token
        call_args.insert(token_index, "$token")
    columns = tuple(entry["columns"])
    query = f"CYPHER 25 CALL {entry['name']}({', '.join(call_args)}) YIELD {', '.join(columns)}"
    if portable:
        for column in columns:
            if column in {"node", "rel"}:
                retained = [item for item in columns if item != column]
                alias = f"__deposit_portable_{column}"
                with_items = [f"{alias} AS {column}", *retained]
                query += (
                    f" CALL deposit.utils.toData({column}, $token) YIELD data AS {alias}"
                    f" WITH {', '.join(with_items)}"
                )
        query += f" RETURN {', '.join(columns)}"
    else:
        query += f" RETURN {', '.join(columns)}"
    return query, params


class _LifetimeNative:
    """A small guard around driver graph values returned by a live result."""

    __slots__ = ("_owner", "_value")

    def __init__(self, owner: "StoreResult", value: Any):
        self._owner = owner
        self._value = value

    def _check(self) -> Any:
        if self._owner.closed:
            raise RuntimeError("Native result value is no longer valid after its StoreResult closes")
        return self._value

    def __getitem__(self, key: Any) -> Any:
        return _wrap(self._owner, self._check()[key])

    def __iter__(self):
        iterator = iter(self._check())
        while True:
            self._check()
            try:
                value = next(iterator)
            except StopIteration:
                return
            yield _wrap(self._owner, value)

    def __len__(self) -> int:
        return len(self._check())

    def __getattr__(self, name: str) -> Any:
        if name == "graph":
            raise TypeError("The native graph container is not exposed outside a StoreResult")
        value = getattr(self._check(), name)
        if callable(value):
            def guarded(*args: Any, **kwargs: Any) -> Any:
                return _wrap(self._owner, getattr(self._check(), name)(*args, **kwargs))
            return guarded
        return _wrap(self._owner, value)

    def __repr__(self) -> str:
        return repr(self._check())


def _wrap(owner: "StoreResult", value: Any) -> Any:
    if isinstance(value, (Node, Relationship, Path)):
        return _LifetimeNative(owner, value)
    if isinstance(value, list):
        return [_wrap(owner, item) for item in value]
    if isinstance(value, tuple):
        return tuple(_wrap(owner, item) for item in value)
    if isinstance(value, dict):
        return {key: _wrap(owner, item) for key, item in value.items()}
    # Neo4j entity property views (keys/values/items) are lazy. Detach them
    # while the owning result is open so they cannot expose data afterward.
    if isinstance(value, (Iterator,)):
        def guarded_iterator():
            iterator = iter(value)
            while True:
                owner._check_open()
                try:
                    item = next(iterator)
                except StopIteration:
                    return
                yield _wrap(owner, item)
        return guarded_iterator()
    if isinstance(value, (set, frozenset)):
        return tuple(_wrap(owner, item) for item in value)
    if isinstance(value, (KeysView, ValuesView, ItemsView)):
        return tuple(_wrap(owner, item) for item in value)
    return value


class StoreResult(Iterator[dict[str, Any]]):
    """Named-column results whose native values are valid only until close."""

    def __init__(
        self,
        columns: tuple[str, ...],
        *,
        result: Any = None,
        transaction: Any = None,
        session: Any = None,
        on_close=None,
        detached_rows: tuple[dict[str, Any], ...] | None = None,
        token: str | None = None,
    ):
        self.columns = columns
        self._result = result
        self._transaction = transaction
        self._session = session
        self._on_close = on_close
        self._token = token
        self._rows = iter(detached_rows) if detached_rows is not None else None
        self._iterator = iter(result) if result is not None else None
        self._closed = False
        self._completed = False
        self._lock = threading.RLock()

    @property
    def closed(self) -> bool:
        return self._closed

    def _check_open(self) -> None:
        if self._closed:
            raise RuntimeError("Native result value is no longer valid after its StoreResult closes")

    def __iter__(self) -> "StoreResult":
        return self

    def __next__(self) -> dict[str, Any]:
        with self._lock:
            if self._closed:
                raise StopIteration
            try:
                if self._rows is not None:
                    row = next(self._rows)
                else:
                    record = next(self._iterator)
                    row = {column: record[column] for column in self.columns}
                    row = {key: _wrap(self, value) for key, value in row.items()}
            except StopIteration:
                self._finish(commit=True)
                raise
            except BaseException as exc:
                self._finish(commit=False, suppress_errors=True)
                if isinstance(exc, Neo4jError):
                    raise _map_driver_error(exc) from exc
                if isinstance(exc, (DepositError, TransportError)):
                    raise
                raise TransportError("Neo4j request failed") from exc
            return row

    def to_data(self, value: Any) -> dict[str, Any]:
        with self._lock:
            if self._closed or self._transaction is None:
                raise RuntimeError("Portable conversion requires an open native StoreResult")
            if not isinstance(value, _LifetimeNative) or value._owner is not self:
                self._finish(commit=False, suppress_errors=True)
                raise TypeError("value must be a native result value owned by this StoreResult")
            try:
                native = value._check()
                if isinstance(native, Node):
                    match = "MATCH (entity)"
                elif isinstance(native, Relationship):
                    match = "MATCH ()-[entity]->()"
                else:
                    raise TypeError("to_data supports native Deposit nodes and relationships")
                element_id = native.element_id
                if not isinstance(element_id, str) or not element_id:
                    raise TypeError("native result value has no Neo4j element ID")
                result = self._transaction.run(
                    f"CYPHER 25 {match} WHERE elementId(entity) = $element_id "
                    "CALL deposit.utils.toData(entity, $token) YIELD data RETURN data",
                    {"element_id": element_id, "token": self._token},
                )
                record = result.single()
                if record is None:
                    raise DepositError("NOT_FOUND", "Native result entity is no longer available")
                data = record.get("data")
                if not isinstance(data, dict):
                    raise DepositError("INVALID_RESPONSE", "Native value conversion returned no portable data")
                return data
            except BaseException as exc:
                self._finish(commit=False, suppress_errors=True)
                if isinstance(exc, (DepositError, TransportError, TypeError)):
                    raise
                if isinstance(exc, Exception):
                    raise _map_driver_error(exc) from exc
                raise

    def close(self) -> None:
        self._finish(commit=False)

    def _finish(self, *, commit: bool, suppress_errors: bool = False) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            first_error: Exception | None = None
            if self._result is not None and commit:
                try:
                    self._result.consume()
                except Exception as exc:
                    first_error = exc
                    commit = False
            if self._transaction is not None:
                try:
                    if commit:
                        self._transaction.commit()
                    else:
                        self._transaction.rollback()
                except Exception as exc:
                    first_error = first_error or exc
                    if commit:
                        try:
                            self._transaction.rollback()
                        except Exception:
                            pass
            if self._session is not None:
                try:
                    self._session.close()
                except Exception as exc:
                    first_error = first_error or exc
            if self._on_close is not None:
                try:
                    self._on_close(self)
                except Exception as exc:
                    first_error = first_error or exc
            if first_error is not None and not suppress_errors:
                if isinstance(first_error, Neo4jError):
                    raise _map_driver_error(first_error) from first_error
                raise TransportError("Neo4j request failed") from first_error

    def __enter__(self) -> "StoreResult":
        if self._closed:
            raise RuntimeError("StoreResult is closed")
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        if not self._closed:
            self._finish(commit=False, suppress_errors=exc is not None)


def detached_result(entry: dict[str, Any], row: dict[str, Any], token: str) -> StoreResult:
    columns = tuple(entry["columns"])
    if set(row) != set(columns):
        raise DepositError("INVALID_RESPONSE", "Tracked procedure returned unexpected columns")
    result = StoreResult(columns, detached_rows=(row,))
    result._token = token
    return result


def _validate_selector_provenance(
    value: Any, binding: str, procedure: str, namespace_id: str | None
) -> None:
    if value is None:
        return
    if isinstance(value, str):
        return
    if not isinstance(value, Mapping):
        raise TypeError(f"{procedure} selector must be a name or mapping")
    expected_kind = binding[1:].split(":", 1)[0]
    kind = value.get("kind")
    if kind is not None and kind != expected_kind:
        raise ValueError(f"{procedure} selector has the wrong entity kind")
    bound_namespace = value.get("namespaceId", value.get("namespace_id"))
    if bound_namespace is not None and bound_namespace != namespace_id:
        raise ValueError(f"{procedure} selector belongs to another Namespace")


def _reject_injected_values(value: Any) -> None:
    cls = type(value)
    names = {base.__name__ for base in cls.__mro__}
    if "Store" in names or "_LifetimeNative" in names or (
        cls.__module__.startswith("neo4j.")
        and names.intersection({"Driver", "Session", "Transaction", "Result"})
    ):
        raise TypeError("Store calls cannot accept Store, driver, session, transaction, or result objects")
    if isinstance(value, Mapping):
        for item in value.values():
            _reject_injected_values(item)
    elif isinstance(value, (list, tuple)):
        for item in value:
            _reject_injected_values(item)


def _map_driver_error(exc: Exception) -> Exception:
    """Expose only the server's structured Deposit error envelope.

    Driver error text may include query details or other diagnostics. Treat it as
    transport text unless the server supplied its explicit safe error payload.
    """
    raw_message = getattr(exc, "message", None)
    prefix = "DEPOSIT_ERROR "
    if (
        not isinstance(raw_message, str)
        or len(raw_message) > 64 * 1024
    ):
        return TransportError("Neo4j request failed")
    if raw_message.startswith(prefix):
        encoded = raw_message[len(prefix) :]
    else:
        # Neo4j wraps procedure exceptions in a human-readable message. Accept
        # only the exact wrapper emitted for Deposit's safe public exception;
        # never search arbitrary diagnostics for an embedded marker.
        if not isinstance(exc, Neo4jError) or getattr(exc, "code", None) != (
            "Neo.ClientError.Procedure.ProcedureCallFailed"
        ):
            return TransportError("Neo4j request failed")
        wrapped = re.fullmatch(
            r"Failed to invoke procedure `([^`]+)`: Caused by: "
            r"deposit\.common\.ExceptionHandler\$DepositException: DEPOSIT_ERROR (.+)",
            raw_message,
        )
        if wrapped is None:
            return TransportError("Neo4j request failed")
        procedure = wrapped.group(1)
        if not any(item.get("name") == procedure for item in _manifest()["procedures"]):
            return TransportError("Neo4j request failed")
        encoded = wrapped.group(2)

    def unique_object(pairs):
        value = {}
        for key, item in pairs:
            if key in value:
                raise ValueError("duplicate key in Deposit error envelope")
            value[key] = item
        return value

    try:
        if len(encoded.encode("utf-8", errors="strict")) > 64 * 1024:
            return TransportError("Neo4j request failed")
        envelope = json.loads(
            encoded,
            object_pairs_hook=unique_object,
            parse_constant=lambda _value: (_ for _ in ()).throw(ValueError()),
        )
    except (TypeError, ValueError, UnicodeError, RecursionError):
        return TransportError("Neo4j request failed")
    if not isinstance(envelope, dict) or set(envelope) != {
        "code", "message", "requestId", "retryable", "details"
    }:
        return TransportError("Neo4j request failed")
    code = envelope.get("code")
    message = envelope.get("message")
    if (
        not isinstance(code, str)
        or not 1 <= len(code) <= 64
        or re.fullmatch(r"[A-Z][A-Z0-9_]*", code, flags=re.ASCII) is None
        or not isinstance(message, str)
        or not message
        or not isinstance(envelope.get("retryable"), bool)
    ):
        return TransportError("Neo4j request failed")
    try:
        message.encode("utf-8", errors="strict")
    except UnicodeError:
        return TransportError("Neo4j request failed")
    if len(message) > 2048:
        message = message[:2045] + "..."
    request_id = envelope.get("requestId")
    if not isinstance(request_id, str):
        request_id = None
    else:
        try:
            UUID(request_id)
        except (ValueError, AttributeError):
            request_id = None
    details = _bounded_details(envelope.get("details"))
    failure = DepositError(code, message, details, request_id)
    failure.retryable = envelope["retryable"]
    failure._server_reported = True
    return failure


def _bounded_details(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        return None
    nodes = 0

    def validate(item: Any, depth: int) -> bool:
        nonlocal nodes
        nodes += 1
        if nodes > 256 or depth > 8:
            return False
        if item is None or type(item) is bool:
            return True
        if type(item) is int:
            return True
        if type(item) is float:
            return math.isfinite(item)
        if isinstance(item, str):
            return len(item) <= 2048
        if isinstance(item, list):
            return all(validate(child, depth + 1) for child in item)
        if isinstance(item, dict):
            return all(
                isinstance(key, str)
                and len(key) <= 128
                and validate(child, depth + 1)
                for key, child in item.items()
            )
        return False

    if not validate(value, 0):
        return None
    try:
        encoded = json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode("utf-8")
    except (TypeError, ValueError, UnicodeError, RecursionError):
        return None
    if len(encoded) > 16 * 1024:
        return None
    return value


def map_driver_error(exc: Exception) -> Exception:
    return _map_driver_error(exc)
