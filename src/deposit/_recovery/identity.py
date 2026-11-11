"""Native Neo4j deployment identity used to bind durable recovery records."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any
from neo4j import Driver

class IdentityUnavailableError(RuntimeError):
    """Required native identity metadata could not be read and validated."""


@dataclass(frozen=True, slots=True)
class DeploymentIdentity:
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
        expected = {"endpoint", "database_name", "database_id", "dbms_id", "actor_id", "namespace_id"}
        if not isinstance(value, dict) or set(value) != expected:
            raise IdentityUnavailableError("Stored deployment identity is invalid")
        if any(not isinstance(value[key], str) or not value[key] for key in expected):
            raise IdentityUnavailableError("Stored deployment identity is incomplete")
        return cls(**value)

    def matches(self, current: "DeploymentIdentity") -> bool:
        return self == current


def normalize_contacted_endpoint(address: Any) -> str:
    """Normalize the host and port that the driver actually contacted.

    A configured routing URI is not evidence of the server that answered a
    query. Neo4j 6.4 exposes the contacted address on ResultSummary.server.
    """
    try:
        host = address.host
        port = address.port
        if not isinstance(host, str) or not host or not isinstance(port, int) or not 1 <= port <= 65535:
            raise ValueError
        host = host.lower().rstrip(".")
        if not host:
            raise ValueError
        if ":" in host and not host.startswith("["):
            host = f"[{host}]"
        return f"{host}:{port}"
    except (AttributeError, TypeError, ValueError) as exc:
        raise IdentityUnavailableError("Neo4j did not report a valid contacted server address") from exc


def _single(driver: Driver, database: str, query: str) -> tuple[dict[str, Any], Any]:
    try:
        with driver.session(database=database) as session:
            result = session.run(query)
            record = result.single(strict=True)
            summary = result.consume()
            return dict(record), summary
    except Exception as exc:
        raise IdentityUnavailableError("Required Neo4j store identity is unavailable") from exc


def read_deployment_identity(
    driver: Driver, *, database: str | None, actor_id: str, namespace_id: str
) -> DeploymentIdentity:
    """Read target database and system-store DBMS IDs; fail closed on omissions.

    A cloned/restored Neo4j store may retain these IDs. The values detect fresh
    replacement and accidental retargeting; they do not attest unique history.
    """
    if not all(isinstance(value, str) and value for value in (actor_id, namespace_id)):
        raise IdentityUnavailableError("Actor and Namespace identities are required")
    target, summary = _single(driver, database, "CALL db.info() YIELD id, name RETURN id, name")
    system, _ = _single(driver, "system", "CALL dbms.info() YIELD id RETURN id")
    database_id = target.get("id")
    database_name = target.get("name")
    dbms_id = system.get("id")
    summary_database = getattr(summary, "database", None)
    server_info = getattr(summary, "server", None)
    address = getattr(server_info, "address", None)
    endpoint = normalize_contacted_endpoint(address)
    if not all(isinstance(value, str) and value for value in (database_id, database_name, dbms_id, summary_database)):
        raise IdentityUnavailableError("Neo4j returned incomplete store identity metadata")
    if database_name != summary_database or (database is not None and database_name != database):
        raise IdentityUnavailableError("Resolved Neo4j database differs from the requested database")
    return DeploymentIdentity(endpoint, database_name, database_id, dbms_id, actor_id, namespace_id)
