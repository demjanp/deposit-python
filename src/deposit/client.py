"""Client-side discovery and Deposit authentication smoke path."""

from __future__ import annotations

import json
from importlib.metadata import version as distribution_version
from importlib.resources import files
import re
from pathlib import Path
from typing import Any

from neo4j import Driver
from neo4j.exceptions import Neo4jError

from .errors import CompatibilityError, DepositError, TransportError
from .recovery import PendingOperation, RecoveredOperation


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
        self._stores = set()
        self._recovery_journals = {}

    def _session(self):
        if self._closed:
            raise RuntimeError("Client is closed")
        options = {"database": self._database} if self._database else {}
        return self._driver.session(**options)

    def _single(self, query: str, parameters: dict[str, Any] | None = None) -> dict[str, Any]:
        return self._single_with_summary(query, parameters)[0]

    def _single_with_summary(self, query: str, parameters: dict[str, Any] | None = None):
        try:
            with self._session() as session:
                result = session.run(query, parameters or {})
                record = result.single(strict=True)
                consume = getattr(result, "consume", None)
                summary = consume() if consume is not None else None
                return dict(record), summary
        except Neo4jError as exc:
            code = getattr(exc, "code", "NEO4J_ERROR")
            message = getattr(exc, "message", None) or "Deposit request failed"
            details = None
            request_id = None
            envelope = re.search(r"\{[^\r\n]*\}", message)
            if envelope:
                try:
                    value = json.loads(envelope.group(0))
                    code = value.get("code", code)
                    message = value.get("message", message)
                    details = value.get("details")
                    request_id = value.get("requestId")
                except (TypeError, ValueError):
                    pass
            raise DepositError(code, message, details, request_id) from exc
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

    def open_store(
        self,
        namespace: str | dict[str, Any],
        *,
        username: str,
        password: str,
        recovery_directory: str | None = None,
    ):
        """Open an independently authenticated Store bound to one Namespace UUID."""
        if not isinstance(namespace, (str, dict)) or (isinstance(namespace, dict) and not namespace):
            raise TypeError("namespace must be a name or an explicit {id: UUID} selector")
        if isinstance(namespace, dict) and (set(namespace) != {"id"} or not isinstance(namespace["id"], str)):
            raise ValueError("Explicit Namespace selectors must contain only a string id")
        token = self.login(username, password)
        try:
            session = self.get_session(token)
            user = session.get("user")
            actor_id = user.get("id") if isinstance(user, dict) else None
            if not isinstance(actor_id, str) or not actor_id:
                raise DepositError("INVALID_RESPONSE", "Session inspection returned no actor identity")
            selector = namespace if isinstance(namespace, str) else {"id": namespace["id"]}
            query = (
                "CYPHER 25 CALL deposit.get.namespace($selection, $token) YIELD node "
                "CALL deposit.utils.toData(node, $token) YIELD data RETURN data"
            )
            row = self._single(query, {"selection": selector, "token": token})
            data = row.get("data")
            namespace_id = data.get("id") if isinstance(data, dict) else None
            if not isinstance(namespace_id, str) or not namespace_id:
                raise DepositError("INVALID_RESPONSE", "Namespace selection returned no UUID")
            self._single(
                "CYPHER 25 CALL deposit.set.activeNamespace($namespace, $token) YIELD node RETURN node",
                {"namespace": {"id": namespace_id}, "token": token},
            )
            active_query = (
                "CYPHER 25 CALL deposit.get.activeNamespace($token) YIELD node "
                "CALL deposit.utils.toData(node, $token) YIELD data RETURN data"
            )
            active = self._single(active_query, {"token": token}).get("data")
            if not isinstance(active, dict) or active.get("id") != namespace_id:
                raise DepositError("SCOPE_MISMATCH", "Store session selected a different Namespace")
            session = self.get_session(token)
            from ._recovery.identity import read_deployment_identity
            from .store import Store, _STORE_FACTORY_TOKEN

            identity = read_deployment_identity(
                self._driver,
                database=self._database,
                actor_id=actor_id,
                namespace_id=namespace_id,
            )
            store = Store(
                self,
                token=token,
                actor_id=actor_id,
                namespace_id=namespace_id,
                identity=identity,
                recovery_directory=recovery_directory,
                observed=session.get("observed"),
                _factory_token=_STORE_FACTORY_TOKEN,
            )
            self._stores.add(store)
            return store
        except Exception:
            try:
                self.logout(token)
            except Exception:
                pass
            raise

    def pending_operations(self, *, recovery_directory: str | Path | None = None) -> tuple[PendingOperation, ...]:
        """List protected unresolved work as immutable references without its keys or inputs."""
        from ._recovery.journal import UNRESOLVED_STATES, ProtectedJournal
        from .recovery import pending_operation

        journal = ProtectedJournal(recovery_directory)
        records = journal.records_all()
        self._recovery_journals.update({record["public_id"]: journal for record in records})
        return tuple(pending_operation(record) for record in records if record["state"] in UNRESOLVED_STATES)

    def recovered_operations(self, *, recovery_directory: str | Path | None = None) -> tuple[RecoveredOperation, ...]:
        """List retained results whose server release was confirmed."""
        from ._recovery.journal import ProtectedJournal
        from .recovery import recovered_operation

        journal = ProtectedJournal(recovery_directory)
        records = journal.records_all()
        self._recovery_journals.update({record["public_id"]: journal for record in records})
        return tuple(recovered_operation(record) for record in records if record["state"] == "released")

    def recover(self, operation: PendingOperation, *, username: str, password: str) -> RecoveredOperation:
        """Authenticate and resolve one operation reference without exposing its recovery key."""
        from ._recovery.identity import DeploymentIdentity, IdentityUnavailableError, read_deployment_identity
        from ._recovery.journal import ProtectedJournal
        from .recovery import PendingOperation, RecoveredOperation, recovered_operation

        if self._closed:
            raise RuntimeError("Client is closed")
        if not isinstance(operation, PendingOperation):
            raise TypeError("operation must be a PendingOperation returned by this Client")
        journal = self._recovery_journals.get(operation.id) or ProtectedJournal()
        record = journal.find_public_id(operation.id)
        expected_identity = DeploymentIdentity.from_dict(record["identity"])
        # Resolve and validate the Client's actual target before authenticating.
        # In particular, never follow the database name from the journal when
        # this Client is explicitly configured for another database.
        try:
            initial_identity = read_deployment_identity(
                self._driver,
                database=self._database,
                actor_id=expected_identity.actor_id,
                namespace_id=expected_identity.namespace_id,
            )
        except IdentityUnavailableError as exc:
            raise DepositError("IDENTITY_MISMATCH", "Recovery identity could not be verified") from exc
        if not expected_identity.matches(initial_identity):
            raise DepositError("IDENTITY_MISMATCH", "Recovery record belongs to a different server or database")
        # Pin this recovery attempt to the database actually resolved above.
        # A private facade avoids changing the shared Client configuration.
        recovery_client = Client(self._driver, database=initial_identity.database_name)
        recovery_client._stores = self._stores
        token = recovery_client.login(username, password)
        store = None
        try:
            stored_identity = read_deployment_identity(
                self._driver,
                database=initial_identity.database_name,
                actor_id=expected_identity.actor_id,
                namespace_id=expected_identity.namespace_id,
            )
            if not stored_identity.matches(expected_identity):
                raise DepositError("IDENTITY_MISMATCH", "Recovery record belongs to a different server or database")
            profile = recovery_client.get_session(token)
            user = profile.get("user")
            actor_id = user.get("id") if isinstance(user, dict) else None
            if actor_id != stored_identity.actor_id:
                raise DepositError("IDENTITY_MISMATCH", "Credentials authenticate a different Deposit actor")
            current_identity = read_deployment_identity(
                self._driver,
                database=initial_identity.database_name,
                actor_id=actor_id,
                namespace_id=expected_identity.namespace_id,
            )
            if not stored_identity.matches(current_identity):
                raise DepositError("IDENTITY_MISMATCH", "Recovery record belongs to a different server or database")
            record = journal.load(stored_identity, record["key"])
            if record["state"] == "released":
                return recovered_operation(record)

            if record["procedure"] == "deposit.delete.namespace":
                with journal.action_lock(record["key"]):
                    # The status check and receipt recovery form one action. A
                    # concurrent Store/recovery process must not resume or release
                    # the same prepared action while this receipt is being saved.
                    record = journal.load(stored_identity, record["key"])
                    if record["state"] == "released":
                        return recovered_operation(record)
                    try:
                        state = recovery_client._recovery_control("deposit.action.status", record["key"], token)
                    except DepositError as exc:
                        if exc.code != "NOT_FOUND":
                            raise
                    else:
                        if state.get("state") == "committed":
                            return recovery_client._finish_deleted_namespace_recovery(
                                journal, stored_identity, record, state, token
                            )
                if record["state"] == "release_sent" and isinstance(record["outcome"], dict):
                    message = "The committed result is saved, but server release is unconfirmed; recovery remains pending"
                elif record["state"] == "outcome_saved" and isinstance(record["outcome"], dict):
                    message = "A result is saved locally, but server action status is unconfirmed; recovery remains pending"
                else:
                    message = "Namespace deletion is not confirmed and remains protected"
                raise DepositError("RECOVERY_PENDING", message)

            namespace_query = (
                "CYPHER 25 CALL deposit.get.namespace($selection, $token) YIELD node "
                "CALL deposit.utils.toData(node, $token) YIELD data RETURN data"
            )
            namespace = recovery_client._single(namespace_query, {"selection": {"id": stored_identity.namespace_id}, "token": token}).get("data")
            if not isinstance(namespace, dict) or namespace.get("id") != stored_identity.namespace_id:
                raise DepositError("SCOPE_MISMATCH", "Recovery Namespace is unavailable or changed")
            recovery_client._single(
                "CYPHER 25 CALL deposit.set.activeNamespace($namespace, $token) YIELD node RETURN node",
                {"namespace": {"id": stored_identity.namespace_id}, "token": token},
            )
            session = recovery_client.get_session(token)
            from .store import Store, _STORE_FACTORY_TOKEN

            store = Store(
                recovery_client,
                token=token,
                actor_id=actor_id,
                namespace_id=stored_identity.namespace_id,
                identity=stored_identity,
                recovery_directory=str(journal.storage.root),
                observed=session.get("observed"),
                _factory_token=_STORE_FACTORY_TOKEN,
            )
            self._stores.add(store)
            record = journal.load(stored_identity, record["key"])
            if record["state"] != "released":
                raise DepositError("RECOVERY_PENDING", "Operation remains unresolved after safe recovery")
            result = recovered_operation(record)
            store.close()
            store = None
            return result
        except IdentityUnavailableError as exc:
            raise DepositError("IDENTITY_MISMATCH", "Recovery identity could not be verified") from exc
        finally:
            if store is not None:
                store.close()
            else:
                try:
                    recovery_client.logout(token)
                except Exception:
                    pass

    def _recovery_control(self, procedure: str, key: str, token: str) -> dict[str, Any]:
        column = "value" if procedure == "deposit.action.release" else "data"
        row = self._single(
            f"CYPHER 25 CALL {procedure}($key, $token) YIELD {column} RETURN {column}",
            {"key": key, "token": token},
        )
        value = row.get(column)
        if not isinstance(value, dict):
            raise DepositError("INVALID_RESPONSE", f"{procedure} returned no recovery state")
        return value

    def _finish_deleted_namespace_recovery(self, journal, identity, record, state, token):
        from .recovery import recovered_operation

        outcome = state.get("outcome")
        if not isinstance(outcome, dict):
            raise DepositError("INVALID_RESPONSE", "Deleted Namespace action has no saved result")
        current = journal.load(identity, record["key"])
        if current["state"] not in {"outcome_saved", "release_sent", "released"}:
            current = journal.update(
                identity,
                record["key"],
                state="outcome_saved",
                outcome=outcome,
                operation_id=state.get("operationId"),
            )
        if current["state"] != "released":
            self._recovery_control("deposit.action.resume", record["key"], token)
            journal.update(identity, record["key"], state="release_sent")
            released = self._single(
                "CYPHER 25 CALL deposit.action.release($token) YIELD value RETURN value",
                {"token": token},
            )
            if released.get("value") is not True:
                raise DepositError("RELEASE_UNCONFIRMED", "Prepared action release was not confirmed")
            current = journal.update(identity, record["key"], state="released")
        return recovered_operation(current)

    def close(self) -> None:
        if self._closed:
            return
        failure = None
        for store in tuple(self._stores):
            try:
                store.close()
            except Exception as exc:
                if failure is None:
                    failure = exc
        self._closed = True
        if self._owns_driver:
            self._driver.close()
        if failure is not None:
            raise failure

    def _store_closed(self, store: Any) -> None:
        self._stores.discard(store)

    def __enter__(self) -> "Client":
        if self._closed:
            raise RuntimeError("Client is closed")
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.close()
