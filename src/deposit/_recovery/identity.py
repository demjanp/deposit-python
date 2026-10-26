"""Native Neo4j deployment identity used to bind durable recovery records."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any
from urllib.parse import urlsplit

from neo4j import Driver

class IdentityUnavailableError(RuntimeError):
    """Required native identity metadata could not be read and validated."""


@dataclass(frozen=True, slots=True)
class DeploymentIdentity:
    schema_version: int
    endpoint: str
    database_name: str
    database_id: str
    dbms_id: str
    actor_id: str
    namespace_id: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "DeploymentIdentity":
        expected = {"schema_version", "endpoint", "database_name", "database_id", "dbms_id", "actor_id", "namespace_id"}
        if not isinstance(value, dict) or set(value) != expected or value.get("schema_version") != 1:
            raise IdentityUnavailableError("Stored deployment identity is invalid or unsupported")
        if any(not isinstance(value[key], str) or not value[key] for key in expected - {"schema_version"}):
            raise IdentityUnavailableError("Stored deployment identity is incomplete")
        return cls(**value)

    def matches(self, current: "DeploymentIdentity") -> bool:
        return self == current


def normalize_endpoint(uri: str) -> str:
    """Return an exact endpoint constraint without credentials or path data."""
    try:
        parts = urlsplit(uri)
        if parts.scheme.lower() not in {"bolt", "bolt+s", "bolt+ssc", "neo4j", "neo4j+s", "neo4j+ssc"}:
            raise ValueError
        if not parts.hostname or parts.username or parts.password or parts.path not in {"", "/"}:
            raise ValueError
        port = parts.port or 7687
        return f"{parts.scheme.lower()}://{parts.hostname.lower()}:{port}"
    except (TypeError, ValueError) as exc:
        raise IdentityUnavailableError("Recovery identity requires a credential-free Bolt endpoint") from exc


def _single(driver: Driver, database: str, query: str) -> dict[str, Any]:
    try:
        with driver.session(database=database) as session:
            return dict(session.run(query).single(strict=True))
    except Exception as exc:
        raise IdentityUnavailableError("Required Neo4j store identity is unavailable") from exc


def read_deployment_identity(
    driver: Driver, *, endpoint: str, database: str, actor_id: str, namespace_id: str
) -> DeploymentIdentity:
    """Read target database and system-store DBMS IDs; fail closed on omissions.

    A cloned/restored Neo4j store may retain these IDs. The values detect fresh
    replacement and accidental retargeting; they do not attest unique history.
    """
    normalized_endpoint = normalize_endpoint(endpoint)
    if not all(isinstance(value, str) and value for value in (database, actor_id, namespace_id)):
        raise IdentityUnavailableError("Database, actor, and Namespace identities are required")
    target = _single(driver, database, "CALL db.info() YIELD id, name RETURN id, name")
    system = _single(driver, "system", "CALL dbms.info() YIELD id RETURN id")
    database_id = target.get("id")
    database_name = target.get("name")
    dbms_id = system.get("id")
    if not all(isinstance(value, str) and value for value in (database_id, database_name, dbms_id)):
        raise IdentityUnavailableError("Neo4j returned incomplete store identity metadata")
    if database_name != database:
        raise IdentityUnavailableError("Resolved Neo4j database differs from the requested database")
    return DeploymentIdentity(1, normalized_endpoint, database_name, database_id, dbms_id, actor_id, namespace_id)
