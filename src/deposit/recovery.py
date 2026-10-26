"""Public immutable references to protected recovery work and saved results."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from types import MappingProxyType
from typing import Any, Mapping


@dataclass(frozen=True, slots=True)
class PendingOperation:
    """A safe reference to unresolved work; it contains no key or input payload."""

    id: str
    procedure: str
    namespace_id: str
    state: str
    created_at: datetime


@dataclass(frozen=True, slots=True)
class RecoveredOperation:
    """An immutable operation reference and its saved portable result."""

    id: str
    procedure: str
    namespace_id: str
    completed_at: datetime
    result: Any


def pending_operation(record: dict[str, Any]) -> PendingOperation:
    return PendingOperation(
        id=record["public_id"],
        procedure=record["procedure"],
        namespace_id=record["identity"]["namespace_id"],
        state=record["state"],
        created_at=datetime.fromisoformat(record["created_at"]),
    )


def recovered_operation(record: dict[str, Any]) -> RecoveredOperation:
    if record["state"] != "released" or record["resolved_at"] is None:
        raise ValueError("Only released operations have a recovered result")
    return RecoveredOperation(
        id=record["public_id"],
        procedure=record["procedure"],
        namespace_id=record["identity"]["namespace_id"],
        completed_at=datetime.fromisoformat(record["resolved_at"]),
        result=_freeze(record["outcome"]),
    )


def _freeze(value: Any) -> Any:
    if isinstance(value, dict):
        return MappingProxyType({key: _freeze(item) for key, item in value.items()})
    if isinstance(value, list):
        return tuple(_freeze(item) for item in value)
    return value
