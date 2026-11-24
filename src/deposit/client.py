"""Client-side discovery and Deposit authentication smoke path."""

from __future__ import annotations

import json
import uuid
from contextlib import contextmanager, suppress
from functools import cache
from importlib.metadata import version as distribution_version
from importlib.resources import files
from threading import Condition, Lock
from typing import Any
from weakref import WeakValueDictionary

from neo4j import Driver
from neo4j.exceptions import Neo4jError

from ._gateway import (
    bind_client_arguments,
    build_query,
    client_procedure,
    domain_arguments,
    map_driver_error,
    store_control_procedure,
    tracked_procedure,
)
from .errors import CompatibilityError, DepositError, OutcomeUnknown, TransportError
from ._exchange_transport import OperationEndpoint


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
        self._tokens = set()
        self._readers = WeakValueDictionary()
        self._active_endpoints = set()
        self._state = Condition()
        self._close_lock = Lock()
        self._closing = False

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
            raise map_driver_error(exc) from exc
        except DepositError:
            raise
        except Exception as exc:
            raise TransportError("Neo4j request failed") from exc

    @staticmethod
    @cache
    def _check_driver_support() -> None:
        support = _manifest()["support"]
        driver_version = distribution_version("neo4j")
        try:
            driver_components = driver_version.split(".")
            driver_major, driver_minor = int(driver_components[0]), int(driver_components[1])
        except (IndexError, TypeError, ValueError) as exc:
            raise CompatibilityError(driver_version, support["neo4j_driver"], "Neo4j Python driver") from exc
        if driver_major != 6 or driver_minor < 1:
            raise CompatibilityError(driver_version, support["neo4j_driver"], "Neo4j Python driver")

    def discover(self) -> dict[str, Any]:
        """Return discovery after checking the installed Neo4j Python driver."""
        self._check_driver_support()
        record = self._single("CYPHER 25 CALL deposit.get.info() YIELD data RETURN data")
        data = record.get("data")
        if not isinstance(data, dict) or not isinstance(data.get("version"), str):
            raise DepositError("INVALID_RESPONSE", "Server discovery returned no version")
        version = data["version"]
        self._version = version
        return dict(data)

    def require_paged_import_results(self) -> dict[str, Any]:
        """Require the final paged-result contract before import operations."""
        data = self.discover()
        capabilities = data.get("capabilities")
        if (not isinstance(capabilities, list) or any(not isinstance(item, str) for item in capabilities)
                or "pagedImportResults" not in capabilities):
            raise CompatibilityError(str(capabilities if capabilities is not None else "missing"),
                                     "pagedImportResults", "Deposit import result capability")
        return data

    def require_clone_capabilities(self) -> dict[str, Any]:
        """Require full Namespace clone and paged result support from the Server."""
        data = self.discover()
        capabilities = data.get("capabilities")
        required = {"fullNamespaceClone", "pagedImportResults"}
        malformed = not isinstance(capabilities, list) or any(not isinstance(item, str) for item in capabilities)
        normalized = [item for item in capabilities if isinstance(item, str)] if isinstance(capabilities, list) else []
        if malformed or not required.issubset(normalized):
            missing = sorted(required.difference(normalized))
            detail = "malformed" if malformed else ", ".join(missing)
            raise CompatibilityError(detail, ", ".join(sorted(required)), "Deposit Namespace clone capability")
        return data

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
        with self._state:
            if self._closing or self._closed:
                with suppress(Exception):
                    self.logout(token)
                raise RuntimeError("Client is closed")
            self._tokens.add(token)
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
        with self._state:
            self._tokens.discard(token)

    @contextmanager
    def _operation_endpoint(self, token, scope):
        with self._state:
            if self._closed or self._closing:
                raise RuntimeError("Client is closed")
            endpoint = OperationEndpoint(self, token, scope)
            self._active_endpoints.add(endpoint)
        try:
            yield endpoint
        finally:
            try:
                endpoint.close()
            finally:
                with self._state:
                    self._active_endpoints.remove(endpoint)
                    self._state.notify_all()

    def _result_request(self, token, procedure, *arguments):
        entry = client_procedure(procedure)
        bound = bind_client_arguments(entry, arguments)
        query, parameters = build_query(entry, bound, token)
        return self._single(query, parameters).get("data")

    def _result_view(self, token, envelope, result_type, *, plan_observation=None, check_open=None, requester=None):
        from ._paging import valid_import_result_envelope

        def request(procedure, *arguments):
            if self._closed or self._closing:
                raise RuntimeError("Client is closed")
            if check_open is not None:
                check_open()
            try:
                if requester is not None:
                    return requester(procedure, *arguments)
                return self._result_request(token, procedure, *arguments)
            except TransportError as exc:
                raise DepositError("RESULT_UNAVAILABLE", "Could not reach the Server for the result",
                                   {"operationId": envelope["operationId"], "resultId": envelope["resultId"]}) from exc

        if valid_import_result_envelope(envelope, leased=False):
            acquired = request("deposit.get.importResult", envelope["resultId"])
            if (not valid_import_result_envelope(acquired)
                    or any(acquired[key] != value for key, value in envelope.items())):
                raise DepositError("INVALID_RESPONSE", "Result lease does not match its immutable envelope")
            envelope = acquired
        return result_type(request, envelope, plan_observation=plan_observation)

    def _invoke_client_procedure(self, procedure: str, token: str, *arguments: Any) -> Any:
        entry = client_procedure(procedure)
        bound = bind_client_arguments(entry, arguments)
        query, parameters = build_query(entry, bound, token, portable=True)
        self.discover()
        record = self._single(query, parameters)
        column = entry["columns"][0]
        if column not in record:
            raise DepositError("INVALID_RESPONSE", f"{procedure} returned no {column} result")
        value = record[column]
        if column in {"node", "rel"} and not isinstance(value, dict):
            raise DepositError("INVALID_RESPONSE", f"{procedure} returned no portable entity")
        return value

    def _invoke_client_rows(self, procedure: str, token: str, *arguments: Any) -> tuple[dict[str, Any], ...]:
        """Read portable entity rows, preserving zero/one/many cardinality."""
        entry = client_procedure(procedure)
        bound = bind_client_arguments(entry, arguments)
        query, parameters = build_query(entry, bound, token, portable=True)
        self.discover()
        try:
            with self._session() as session:
                result = session.run(query, parameters)
                column = entry["columns"][0]
                rows = []
                for record in result:
                    if column not in record.keys():
                        raise DepositError("INVALID_RESPONSE", f"{procedure} returned no {column} result")
                    value = record[column]
                    if not isinstance(value, dict):
                        raise DepositError("INVALID_RESPONSE", f"{procedure} returned no portable entity")
                    rows.append(dict(value))
                consume = getattr(result, "consume", None)
                if consume is not None:
                    consume()
                return tuple(rows)
        except Neo4jError as exc:
            raise map_driver_error(exc) from exc
        except DepositError:
            raise
        except Exception as exc:
            raise TransportError("Neo4j request failed") from exc

    @staticmethod
    def new_operation_id() -> str:
        """Create a fresh public operation identity."""
        return str(uuid.uuid7())

    def get_operation(self, token: str, operation_id: str, *, scope: str = "server") -> Any:
        """Inspect an operation under its original explicit scope."""
        return self._invoke_client_procedure("deposit.get.operation", token, operation_id, scope)

    def cancel_operation(self, token: str, operation_id: str, *, scope: str = "server") -> Any:
        """Request cancellation and return the Server's authoritative state."""
        with self._state:
            endpoints = tuple(self._active_endpoints)
        for endpoint in endpoints:
            if endpoint.scope == scope and endpoint.token == token and endpoint.note_cancel(operation_id):
                return endpoint._operation_status(operation_id, True)
        return self._invoke_client_procedure("deposit.action.cancel", token, operation_id, scope)

    def get_last_operation(self, token: str) -> Any:
        """Return the last operation observed by this authenticated session."""
        entry = store_control_procedure("deposit.get.lastOperation")
        query, parameters = build_query(entry, (), token)
        return self._single(query, parameters).get("data")

    def get_active_namespace(self, token: str) -> dict[str, Any] | None:
        """Return the active Namespace as portable data, or None if unselected."""
        self.discover()
        query = (
            "CYPHER 25 CALL deposit.get.activeNamespace($token) YIELD node "
            "CALL { WITH node WITH node WHERE node IS NOT NULL "
            "CALL deposit.utils.toData(node, $token) YIELD data RETURN data "
            "UNION ALL WITH node WITH node WHERE node IS NULL RETURN null AS data } "
            "RETURN data"
        )
        data = self._single(query, {"token": token}).get("data")
        if data is not None and not isinstance(data, dict):
            raise DepositError("INVALID_RESPONSE", "Active Namespace lookup returned invalid data")
        return dict(data) if isinstance(data, dict) else None

    def set_active_namespace(self, token: str, namespace: Any = None) -> dict[str, Any]:
        """Set and return the active Namespace as portable data."""
        return self._invoke_client_procedure("deposit.set.activeNamespace", token, namespace)

    def get_administrative_mode(self, token: str) -> Any:
        """Return the authenticated session's administrative mode value."""
        return self._invoke_client_procedure("deposit.get.administrativeMode", token)

    def set_administrative_mode(self, token: str, enabled: bool) -> Any:
        """Set and return the authenticated session's administrative mode value."""
        return self._invoke_client_procedure("deposit.set.administrativeMode", token, enabled)

    def get_user(self, token: str, selection: Any) -> Any:
        """Read a user record using an explicit manifest-defined selector."""
        if selection is None or (isinstance(selection, dict) and not selection):
            raise ValueError("selection must identify one user; use get_users for unfiltered results")
        return self._invoke_client_procedure("deposit.get.user", token, selection)

    def get_users(self, token: str, selection: Any = None) -> tuple[dict[str, Any], ...]:
        """Read zero or more selected users as detached portable dictionaries."""
        return self._invoke_client_rows("deposit.get.user", token, selection)

    def get_user_page(self, token: str, selection: Any = None, page: dict[str, Any] | None = None) -> Any:
        """Read a page of user records."""
        return self._invoke_client_procedure(
            "deposit.get.userPage", token, selection, {} if page is None else page
        )

    def get_namespace(self, token: str, selection: Any) -> dict[str, Any]:
        """Read a Namespace using an explicit selector as portable data."""
        if selection is None or (isinstance(selection, dict) and not selection):
            raise ValueError("selection must identify one Namespace; use get_namespaces for unfiltered results")
        return self._invoke_client_procedure("deposit.get.namespace", token, selection)

    def get_namespaces(self, token: str, selection: Any = None) -> tuple[dict[str, Any], ...]:
        """Read zero or more selected Namespaces as detached portable dictionaries."""
        return self._invoke_client_rows("deposit.get.namespace", token, selection)

    def get_namespace_page(
        self, token: str, selection: Any = None, page: dict[str, Any] | None = None
    ) -> Any:
        """Read a page of Namespace records."""
        return self._invoke_client_procedure(
            "deposit.get.namespacePage", token, selection, {} if page is None else page
        )

    def prune_resources_status(self, token: str, prune_id: str) -> Any:
        """Read status for an existing resource-pruning operation."""
        return self._invoke_client_procedure("deposit.admin.pruneResourcesStatus", token, prune_id)

    def prune_resources(self, token: str, options: dict[str, Any], *, operation_id=None) -> Any:
        """Inspect or start a Server-authorized resource collection job."""
        if not isinstance(options, dict):
            raise TypeError("prune_resources options must be a dictionary")
        route = "deposit.admin.pruneResources"
        entry = tracked_procedure(route)
        (options,) = bind_client_arguments(domain_arguments(entry), (options,))
        if options.get("dryRun") is not False:
            query, parameters = build_query(entry, (options, None), token)
            self.discover()
            return self._single(query, parameters)["data"]
        scope = "server"
        if options.get("scope", "namespace") != "all":
            scope = self.get_session(token).get("namespaceId")
            if not isinstance(scope, str):
                raise DepositError("NO_ACTIVE_NAMESPACE", "Select a Namespace first")
        with self._operation_endpoint(token, scope) as endpoint, endpoint.execution() as execution:
            return execution.run(route, options, operation_id=operation_id)

    def coordinates_to_wkt(
        self,
        token: str,
        geometry_type: str,
        coordinates: Any,
        options: dict[str, Any] | None = None,
    ) -> Any:
        """Convert coordinates to WKT using the server's geometry rules."""
        return self._invoke_client_procedure(
            "deposit.utils.coordinatesToWKT", token, geometry_type, coordinates,
            {} if options is None else options,
        )

    def wkt_to_coordinates(
        self, token: str, wkt: str, options: dict[str, Any] | None = None
    ) -> Any:
        """Convert WKT to coordinates using the server's geometry rules."""
        return self._invoke_client_procedure(
            "deposit.utils.wktToCoordinates", token, wkt, {} if options is None else options
        )

    def open_store(
        self,
        namespace: str | dict[str, Any],
        *,
        username: str,
        password: str,
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
            from .store import Store, _STORE_FACTORY_TOKEN

            store = Store(
                self, token=token, actor_id=actor_id, namespace_id=namespace_id,
                observed=session.get("observed"), _factory_token=_STORE_FACTORY_TOKEN,
            )
            with self._state:
                if not self._closing and not self._closed:
                    self._stores.add(store)
                    return store
            store.close()
            raise RuntimeError("Client is closed")
        except Exception:
            try:
                self.logout(token)
            except Exception:
                pass
            raise

    def clone_namespace(self, token: str, source, *, operation_id=None):
        """Create a Namespace atomically from a complete Exchange package."""
        from ._exchange import ExchangeBundle, discard_package, package_for_target, stage_zip
        from ._paging import CloneResult

        with self._operation_endpoint(token, "server") as endpoint:
            endpoint._require_capabilities("fullNamespaceClone", "pagedImportResults")
            endpoint.require_exchange()
            endpoint.call("deposit.utils.refresh", "server")
            if isinstance(source, ExchangeBundle):
                reference = package_for_target(source, endpoint)
                owned = reference != source.source
            else:
                reference = stage_zip(endpoint, source)
                owned = True
            try:
                with endpoint.execution() as execution:
                    envelope = execution.run("deposit.import.cloneNamespace", {
                        name: reference[name] for name in ("packageId", "grant")
                    }, operation_id=operation_id)
                result = self._result_view(token, envelope, CloneResult)
                self._readers[id(result)] = result
                return result
            except OutcomeUnknown:
                owned = False
                raise
            finally:
                if owned:
                    discard_package(endpoint, reference)

    def close(self) -> None:
        with self._close_lock:
            with self._state:
                if self._closed:
                    return
                self._closing = True
                endpoints = tuple(self._active_endpoints)
            for endpoint in endpoints:
                with suppress(Exception):
                    endpoint.cancel_current()
            failure = None
            for store in tuple(self._stores):
                try:
                    store.close()
                except Exception as exc:
                    if failure is None:
                        failure = exc
            with self._state:
                self._state.wait_for(lambda: not self._active_endpoints)
            for result in tuple(self._readers.values()):
                result.close()
            for token in tuple(self._tokens):
                try:
                    self.logout(token)
                except DepositError as exc:
                    if exc.code not in {"AUTH_FAILED", "AUTH_REQUIRED", "SESSION_EXPIRED", "NOT_FOUND"} and failure is None:
                        failure = exc
                except Exception as exc:
                    if failure is None:
                        failure = exc
            self._tokens.clear()
            self._closed = True
            if self._owns_driver:
                self._driver.close()
            if failure is not None:
                raise failure

    def _store_closed(self, store: Any) -> None:
        with self._state:
            self._stores.discard(store)

    def __enter__(self) -> "Client":
        if self._closed:
            raise RuntimeError("Client is closed")
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.close()
