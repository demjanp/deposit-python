"""Client-side discovery and Deposit authentication smoke path."""

from __future__ import annotations

import json
from importlib.metadata import version as distribution_version
from importlib.resources import files
from typing import Any

from neo4j import Driver
from neo4j.exceptions import Neo4jError

from .errors import CompatibilityError, DepositError, TransportError


def _manifest() -> dict[str, Any]:
    return json.loads(files("deposit").joinpath("_procedure_manifest.json").read_text(encoding="utf-8"))


class Client:
    """A synchronous connection facade.

    The caller owns a driver unless ``owns_driver=True`` is supplied. Client
    authentication is intentionally separate from Store construction.
    """

    def __init__(self, driver: Driver, *, owns_driver: bool = False, database: str | None = None):
        self._driver = driver
        self._owns_driver = owns_driver
        self._database = database
        self._closed = False
        self._version: str | None = None

    def _session(self):
        if self._closed:
            raise RuntimeError("Client is closed")
        options = {"database": self._database} if self._database else {}
        return self._driver.session(**options)

    def _single(self, query: str, parameters: dict[str, Any] | None = None) -> dict[str, Any]:
        try:
            with self._session() as session:
                record = session.run(query, parameters or {}).single(strict=True)
                return dict(record)
        except Neo4jError as exc:
            code = getattr(exc, "code", "NEO4J_ERROR")
            message = getattr(exc, "message", None) or "Deposit request failed"
            raise DepositError(code, message) from exc
        except DepositError:
            raise
        except Exception as exc:
            raise TransportError("Neo4j request failed") from exc

    def discover(self) -> dict[str, Any]:
        """Return server discovery and enforce the exact tested server build."""
        support = _manifest()["support"]
        driver_version = distribution_version("neo4j")
        try:
            driver_major = int(driver_version.split(".", 1)[0])
        except (TypeError, ValueError) as exc:
            raise CompatibilityError(driver_version, support["neo4j_driver"], "Neo4j Python driver") from exc
        if driver_major != 6:
            raise CompatibilityError(driver_version, support["neo4j_driver"], "Neo4j Python driver")
        record = self._single("CYPHER 25 CALL deposit.get.info() YIELD data RETURN data")
        data = record.get("data")
        if not isinstance(data, dict) or not isinstance(data.get("version"), str):
            raise DepositError("INVALID_RESPONSE", "Server discovery returned no version")
        version = data["version"]
        supported = support["target_server_version"]
        if version != supported:
            raise CompatibilityError(version, f"Deposit Server {supported}")
        neo4j_record = self._single(
            "CYPHER 25 CALL dbms.components() YIELD name, versions "
            "WHERE name = 'Neo4j Kernel' RETURN versions[0] AS version"
        )
        neo4j_version = neo4j_record.get("version")
        target_neo4j = support["target_neo4j_version"]
        if neo4j_version != target_neo4j:
            raise CompatibilityError(neo4j_version or "unknown", target_neo4j, "Neo4j Server")
        self._version = version
        return dict(data)

    def login(self, username: str, password: str) -> str:
        """Authenticate and return the session token; credentials are never logged."""
        self.discover()
        record = self._single(
            "CYPHER 25 CALL deposit.utils.login($username, $password) YIELD value RETURN value",
            {"username": username, "password": password},
        )
        token = record.get("value")
        if not isinstance(token, str) or not token:
            raise DepositError("INVALID_RESPONSE", "Login returned no session token")
        return token

    def bootstrap(self, username: str, password: str) -> dict[str, Any]:
        """Create the initial Deposit administrator on a fresh server only."""
        self.discover()
        record = self._single(
            "CYPHER 25 CALL deposit.utils.bootstrap($username, $password) YIELD data RETURN data",
            {"username": username, "password": password},
        )
        data = record.get("data")
        if not isinstance(data, dict):
            raise DepositError("INVALID_RESPONSE", "Bootstrap returned no profile")
        return dict(data)

    def get_session(self, token: str) -> dict[str, Any]:
        """Return the sanitized profile for this authenticated session."""
        self.discover()
        record = self._single(
            "CYPHER 25 CALL deposit.get.session($token) YIELD data RETURN data", {"token": token}
        )
        data = record.get("data")
        if not isinstance(data, dict):
            raise DepositError("INVALID_RESPONSE", "Session inspection returned no profile")
        return dict(data)

    def logout(self, token: str) -> None:
        """Revoke a Deposit session without closing a caller-owned driver."""
        self.discover()
        self._single("CYPHER 25 CALL deposit.utils.logout($token) YIELD value RETURN value", {"token": token})

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._owns_driver:
            self._driver.close()

    def __enter__(self) -> "Client":
        if self._closed:
            raise RuntimeError("Client is closed")
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.close()
