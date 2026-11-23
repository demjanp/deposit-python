"""Client-side discovery and Deposit authentication smoke path."""

from __future__ import annotations

import json
import uuid
import base64
import hashlib
from importlib.metadata import version as distribution_version
from importlib.resources import files
from pathlib import Path
from typing import Any

from neo4j import Driver
from neo4j.exceptions import Neo4jError

from ._gateway import (
    bind_client_arguments,
    build_query,
    client_procedure,
    clone_coordinator_procedure,
    map_driver_error,
    prune_dry_run_procedure,
    recovery_read_procedure,
)
from .errors import CompatibilityError, DepositError, TransportError
from .recovery import PendingOperation, RecoveredOperation


def _abandon_local_absent_state(action):
    """True when a Server NOT_FOUND can be trusted for this local receipt.

    Only a local intent, or the durable CANCELLED cancellation written for a
    prepare that never reached the Server, proves no remote effect exists.
    A receipt bound to an operation ID, or a different saved failure, keeps
    the remote outcome unknown and is not terminal.
    """
    if action["state"] in {"intent", "prepare_sent"}:
        return True
    return (action["state"] == "failure_saved" and action["operation_id"] is None
            and (action.get("failure") or {}).get("code") == "CANCELLED")


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
        self._import_workflow_stores = {}
        self._clone_workflow_stores = {}

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

    def discover(self) -> dict[str, Any]:
        """Return discovery after checking the installed Neo4j Python driver."""
        support = _manifest()["support"]
        driver_version = distribution_version("neo4j")
        try:
            driver_components = driver_version.split(".")
            driver_major, driver_minor = int(driver_components[0]), int(driver_components[1])
        except (IndexError, TypeError, ValueError) as exc:
            raise CompatibilityError(driver_version, support["neo4j_driver"], "Neo4j Python driver") from exc
        if driver_major != 6 or driver_minor < 1:
            raise CompatibilityError(driver_version, support["neo4j_driver"], "Neo4j Python driver")
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

    def _invoke_coordinated_server_procedure(self, procedure: str, token: str, *arguments: Any) -> Any:
        """Execute only the declared clone action/recovery coordinator routes."""
        entry = clone_coordinator_procedure(procedure)
        bound = bind_client_arguments(entry, arguments)
        query, parameters = build_query(entry, bound, token)
        record = self._single(query, parameters)
        if "data" not in record or not isinstance(record["data"], dict):
            raise DepositError("INVALID_RESPONSE", f"{procedure} returned no data result")
        return record["data"]

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

    def _invoke_recovery_read(self, procedure: str, token: str, *arguments: Any) -> Any:
        entry = recovery_read_procedure(procedure)
        bound = bind_client_arguments(entry, arguments)
        query, parameters = build_query(entry, bound, token)
        self.discover()
        record = self._single(query, parameters)
        column = entry["columns"][0]
        if column not in record:
            raise DepositError("INVALID_RESPONSE", f"{procedure} returned no {column} result")
        return record[column]

    def get_operation(self, token: str, operation_id: str, *, scope: str = "server") -> Any:
        """Inspect a server operation without changing session selection."""
        if not isinstance(scope, str) or scope not in {"server", "namespace"}:
            raise ValueError("scope must be 'server' or 'namespace'")
        if not isinstance(operation_id, str) or not operation_id:
            raise ValueError("operation_id must be a nonempty string")
        return self._invoke_recovery_read("deposit.get.operation", token, operation_id, scope)

    def get_last_operation(self, token: str) -> Any:
        """Return the last operation observed by this authenticated session."""
        return self._invoke_recovery_read("deposit.get.lastOperation", token)

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

    def prune_resources(self, token: str, options: dict[str, Any]) -> Any:
        """Inspect resource pruning without starting an execution action.

        Stage 3 exposes only the server-authorized dry-run branch. Executing
        pruning remains deferred until Client-owned action recovery is designed.
        """
        entry = prune_dry_run_procedure(options)
        bound = bind_client_arguments(entry, (options,))
        query, parameters = build_query(entry, bound, token)
        self.discover()
        record = self._single(query, parameters)
        value = record.get("data")
        if not isinstance(value, dict):
            raise DepositError("INVALID_RESPONSE", "Resource-pruning dry run returned no data")
        return value

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
        """List unresolved work and retained failure notices without their keys or inputs."""
        from ._recovery.journal import PENDING_STATES, ProtectedJournal
        from .recovery import pending_operation

        journal = ProtectedJournal(recovery_directory)
        records = journal.records_all()
        self._recovery_journals.update({record["public_id"]: journal for record in records})
        return tuple(pending_operation(record) for record in records if record["state"] in PENDING_STATES)

    def clone_namespace(self, token: str, source, *, recovery_directory: str | Path | None = None):
        """Create a Namespace atomically from a complete, checked Exchange package."""
        from ._recovery.identity import read_deployment_identity
        from ._recovery.journal import ProtectedJournal, RESULT_FORMAT_CLONE
        from ._clone_workflows import CloneWorkflowStore

        if not isinstance(token, str) or not token:
            raise ValueError("token must be a nonempty authenticated session token")
        self.require_clone_capabilities()
        if isinstance(source, dict) and set(source) == {"resourceId"}:
            from ._recovery.journal import _is_uuid
            if not _is_uuid(source["resourceId"]):
                raise ValueError("source resourceId must be a UUID")
        profile = self.get_session(token)
        user = profile.get("user")
        actor_id = user.get("id") if isinstance(user, dict) else None
        if not isinstance(actor_id, str) or not actor_id:
            raise DepositError("INVALID_RESPONSE", "Session inspection returned no actor identity")
        identity = read_deployment_identity(
            self._driver, database=self._database, actor_id=actor_id,
            namespace_id=None, server_scope=True,
        )
        journal = ProtectedJournal(recovery_directory)
        workflows = CloneWorkflowStore(journal.storage)
        workflow = workflows.create(identity)
        self._clone_workflow_stores[workflow.id] = workflows
        self._recovery_journals = getattr(self, "_recovery_journals", {})
        self._invoke_coordinated_server_procedure("deposit.utils.refresh", token, "server")
        with workflows.lock(workflow.id):
            self._freeze_clone_source(workflows, workflow.id, journal, identity, source)
            return self._continue_clone_workflow(workflows, workflow.id, journal, identity, token)

    def _freeze_clone_source(self, workflows, workflow_id, journal, identity, source):
        from ._recovery.transfers import ProtectedTransferSpool
        record = workflows.get(workflow_id)
        if record["state"] != "SOURCE_REQUIRED" or record["transfer_id"] is not None:
            raise DepositError("CLONE_SOURCE_ALREADY_FROZEN", "Clone workflow no longer accepts replacement package input")
        from ._recovery.identity import DeploymentIdentity
        direct_resource_id = None
        if isinstance(source, dict) and set(source) == {"resourceId"}:
            direct_resource_id = source["resourceId"]
        elif getattr(source, "package_reference", None) is not None:
            binding = getattr(source, "source_binding", None)
            if (isinstance(binding, dict) and "namespace_id" in binding
                    and DeploymentIdentity.from_dict(binding) == identity):
                direct_resource_id = source.package_reference
        if direct_resource_id is not None:
            if not isinstance(direct_resource_id, str) or not direct_resource_id:
                raise ValueError("source resourceId must be a nonempty string")
            from ._recovery.journal import _is_uuid
            if not _is_uuid(direct_resource_id):
                raise ValueError("source resourceId must be a UUID")
            workflows.update(workflow_id, state="READY", phase="ready", source_resource_id=direct_resource_id)
            return
        spool = ProtectedTransferSpool(journal, identity)
        intent = spool.reserve_clone_source(workflow_id)
        workflows.update(workflow_id, transfer_id=intent["transfer_id"], phase="source_freezing")
        try:
            transfer = Client._clone_source_manifest(spool, intent)
        except DepositError as exc:
            if exc.code != "CLONE_SOURCE_INCOMPLETE":
                raise
            # New reservation: the first durable source checkpoint is absent.
            transfer = {"state": "staging"}
        if transfer["state"] == "staging":
            with self._open_clone_package(source, journal.storage) as package:
                manifest = package.manifest
                scope = getattr(manifest.get("scope"), "scalar", None)
                scope = scope() if callable(scope) else manifest.get("scope")
                if scope != "namespace":
                    raise ValueError("Namespace cloning requires a complete Namespace Exchange package")
                package._source.seek(0)
                transfer = spool.freeze(
                    package._source, filename="deposit-clone.zip",
                    media_type="application/vnd.deposit.namespace-clone",
                    transfer_id=intent["transfer_id"],
                )
        else:
            if source is not None:
                raise DepositError("CLONE_SOURCE_ALREADY_FROZEN", "Replacement input is not allowed after the first source completed")
            spool.verify_content(transfer)
        workflows.update(workflow_id, state="FROZEN", phase="frozen",
                         sha256=transfer["sha256"], total_bytes=transfer["bytes"], verified_offset=0)

    def _continue_clone_workflow(self, workflows, workflow_id, journal, identity, token):
        from ._recovery.journal import RESULT_FORMAT_CLONE
        from ._recovery.transfers import ProtectedTransferSpool, TRANSFER_CHUNK_BYTES

        record = workflows.get(workflow_id)
        if record["identity"] != identity.to_dict():
            raise DepositError("IDENTITY_MISMATCH", "Clone workflow belongs to another server or actor")
        if record["state"] == "COMMITTED":
            clone_action = journal.load(identity, record["clone_action_key"])
            outcome = clone_action.get("outcome")
            if (clone_action["state"] != "released" or not isinstance(outcome, dict)
                    or outcome.get("resultId") != record["result_id"]):
                raise DepositError("RECOVERY_IDENTITY_MISMATCH", "Clone workflow result is unavailable or changed")
            self._cleanup_clone_transfer(workflows, workflow_id, journal, identity)
            from ._paging import CloneResult
            return CloneResult(journal, record["clone_action_key"], outcome)
        if record["state"] == "SOURCE_REQUIRED":
            raise DepositError("CLONE_SOURCE_REQUIRED", "Supply a complete package to resume the interrupted freeze")
        if record["state"] == "FAILED":
            raise DepositError("CLONE_FAILED", "Clone workflow is in a retained failure state")
        if (record["source_resource_id"] is not None or record["phase"] == "clone_pending"
                or record["outstanding_role"] == "clone"):
            clone_key = record["clone_action_key"] or (record["outstanding_action_key"]
                        if record["clone_action_key"] is None
                        and record["outstanding_role"] == "clone"
                        else str(uuid.uuid4()))
            workflows.update(workflow_id, state="CLONE_PENDING", phase="clone_pending",
                             clone_action_key=clone_key, outstanding_role="clone", outstanding_action_key=clone_key)
            record = workflows.get(workflow_id)
            if record["source_resource_id"] is not None:
                arguments = [{"resourceId": record["source_resource_id"]}]
            else:
                if record["upload_id"] is None or record["sha256"] is None:
                    raise DepositError("RECOVERY_IDENTITY_MISMATCH", "Clone action has no frozen package binding")
                arguments = [{"uploadId": record["upload_id"], "sha256": record["sha256"]}]
            action = self._run_server_action(
                journal, identity, token, "deposit.import.cloneNamespace",
                arguments,
                result_format=RESULT_FORMAT_CLONE, recovery_key=clone_key,
            )
            outcome = action["outcome"]
            return self._commit_clone_workflow(workflows, workflow_id, journal, clone_key, outcome)
        spool = ProtectedTransferSpool(journal, identity)
        transfer = spool.load(record["transfer_id"])
        if (transfer["identity"] != identity.to_dict() or transfer["sha256"] != record["sha256"]
                or transfer["bytes"] != record["total_bytes"]):
            raise DepositError("RECOVERY_IDENTITY_MISMATCH", "Clone transfer does not match its protected workflow")
        # Verify every protected chunk before the first durable begin transition, spool
        # update or upload action so a damaged frozen source is rejected locally.
        spool.verify_content(transfer)

        if record["upload_id"] is None or record["state"] == "FROZEN":
            key = (record["outstanding_action_key"] or str(uuid.uuid4()))
            workflows.update(workflow_id, state="FROZEN", phase="begin_pending",
                             outstanding_role="begin", outstanding_action_key=key)
            spool.update(transfer["transfer_id"], state="staged", begin_action_key=key, offset=0)
            action = self._run_server_action(
                journal, identity, token, "deposit.import.beginCloneUpload",
                [transfer["filename"], transfer["bytes"], transfer["sha256"]], recovery_key=key,
            )
            started = action["outcome"]
            upload_id = started.get("uploadId") if isinstance(started, dict) else None
            if not isinstance(upload_id, str) or not upload_id:
                raise DepositError("INVALID_RESPONSE", "Clone upload start returned no upload identity")
            spool.update(transfer["transfer_id"], state="uploading", upload_id=upload_id, begin_action_key=key,
                         offset=0, pending_action_key=None, pending_offset=None, pending_length=None)
            workflows.update(workflow_id, state="UPLOADING", phase="uploading", upload_id=upload_id,
                             verified_offset=0, outstanding_role=None, outstanding_action_key=None)
            record = workflows.get(workflow_id)
        record = workflows.get(workflow_id)
        offset = record["verified_offset"]
        while offset < transfer["bytes"]:
            index = offset // TRANSFER_CHUNK_BYTES
            block = spool.read_chunk(transfer["transfer_id"], index)
            chunk_hash = hashlib.sha256(block).hexdigest()
            key = (record["outstanding_action_key"] or str(uuid.uuid4()))
            workflows.update(workflow_id, state="UPLOADING", phase="chunk_pending",
                             outstanding_role="chunk", outstanding_action_key=key)
            spool.update(transfer["transfer_id"], state="uploading", upload_id=record["upload_id"],
                         pending_action_key=key, pending_offset=offset, pending_length=len(block))
            action = self._run_server_action(
                journal, identity, token, "deposit.import.cloneUploadChunk",
                [record["upload_id"], offset, base64.b64encode(block).decode("ascii"), chunk_hash],
                recovery_key=key,
            )
            outcome = action["outcome"]
            next_offset = outcome.get("nextOffset") if isinstance(outcome, dict) else None
            expected_offset = offset + len(block)
            if next_offset != expected_offset:
                raise DepositError("RECOVERY_IDENTITY_MISMATCH", "Clone chunk receipt differs from its frozen offset")
            spool.update(transfer["transfer_id"], offset=expected_offset, pending_action_key=None,
                         pending_offset=None, pending_length=None)
            workflows.update(workflow_id, phase="uploading", verified_offset=expected_offset,
                             outstanding_role=None, outstanding_action_key=None)
            offset = expected_offset
            record = workflows.get(workflow_id)

        record = workflows.get(workflow_id)
        if record["phase"] not in {"ready", "clone_pending"}:
            key = (record["outstanding_action_key"] or str(uuid.uuid4()))
            workflows.update(workflow_id, state="UPLOADING", phase="finish_pending",
                             outstanding_role="finish", outstanding_action_key=key)
            spool.update(transfer["transfer_id"], finish_action_key=key, offset=transfer["bytes"],
                         pending_action_key=None, pending_offset=None, pending_length=None)
            action = self._run_server_action(
                journal, identity, token, "deposit.import.finishCloneUpload", [record["upload_id"]],
                recovery_key=key,
            )
            finished = action["outcome"]
            if (not isinstance(finished, dict) or finished.get("state") != "ready"
                    or finished.get("sha256") != transfer["sha256"]):
                raise DepositError("INVALID_RESPONSE", "Clone upload finalization changed its package identity")
            spool.update(transfer["transfer_id"], state="finished", finish_action_key=key,
                         offset=transfer["bytes"], pending_action_key=None,
                         pending_offset=None, pending_length=None)
            workflows.update(workflow_id, state="READY", phase="ready", verified_offset=transfer["bytes"],
                             outstanding_role=None, outstanding_action_key=None)
            record = workflows.get(workflow_id)

        clone_key = record["clone_action_key"] or (record["outstanding_action_key"]
                    if record["outstanding_role"] == "clone" else str(uuid.uuid4()))
        workflows.update(workflow_id, state="CLONE_PENDING", phase="clone_pending",
                         clone_action_key=clone_key, outstanding_role="clone", outstanding_action_key=clone_key)
        action = self._run_server_action(
            journal, identity, token, "deposit.import.cloneNamespace",
            [{"uploadId": record["upload_id"], "sha256": transfer["sha256"]}],
            result_format=RESULT_FORMAT_CLONE, recovery_key=clone_key,
        )
        outcome = action["outcome"]
        if not isinstance(outcome, dict) or not isinstance(outcome.get("namespaceId"), str):
            raise DepositError("INVALID_RESPONSE", "Clone action returned no Namespace identity")
        return self._commit_clone_workflow(workflows, workflow_id, journal, clone_key, outcome)

    def _commit_clone_workflow(self, workflows, workflow_id, journal, clone_key, outcome):
        from datetime import datetime, timezone
        from ._paging import CloneResult
        if not isinstance(outcome, dict) or not isinstance(outcome.get("namespaceId"), str):
            raise DepositError("INVALID_RESPONSE", "Clone action returned no Namespace identity")
        workflows.update(workflow_id, state="COMMITTED", phase="committed", result_id=outcome["resultId"],
                         outstanding_role=None, outstanding_action_key=None,
                         resolved_at=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                         cleanup_status="pending")
        from ._recovery.identity import DeploymentIdentity
        self._cleanup_clone_transfer(workflows, workflow_id, journal,
                                     DeploymentIdentity.from_dict(workflows.get(workflow_id)["identity"]))
        self._recovery_journals.update({item["public_id"]: journal for item in journal.records_all()})
        return CloneResult(journal, clone_key, outcome)

    @staticmethod
    def _cleanup_clone_transfer(workflows, workflow_id, journal, identity):
        from ._recovery.transfers import ProtectedTransferSpool
        record = workflows.get(workflow_id)
        transfer_id = record["transfer_id"]
        if transfer_id is None:
            workflows.update(workflow_id, cleanup_status="complete")
            return
        manifest_name = f"transfer-{transfer_id}-manifest"
        try:
            if journal.storage.has_record(manifest_name):
                ProtectedTransferSpool(journal, identity).cleanup_resolved(transfer_id)
            prefix = f"transfer-{transfer_id}-"
            if any(name.startswith(prefix) for name in journal.storage.iter_records()):
                raise RuntimeError("Clone transfer cleanup left protected spool records")
            owned_admissions = []
            for name in journal.storage.iter_records():
                if not name.startswith("admission-"):
                    continue
                admission = json.loads(journal.storage.read(name))
                if admission.get("owner_id") == transfer_id and admission.get("purpose") == "protected-upload-spool":
                    owned_admissions.append(name)
                    if len(owned_admissions) >= 64:
                        break
            for owned_admission in owned_admissions:
                journal.storage.delete(owned_admission)

            def has_owned_admission():
                for name in journal.storage.iter_records():
                    if not name.startswith("admission-"):
                        continue
                    admission = json.loads(journal.storage.read(name))
                    if admission.get("owner_id") == transfer_id and admission.get("purpose") == "protected-upload-spool":
                        return True
                return False

            if has_owned_admission():
                raise RuntimeError("Clone transfer cleanup left protected admission records")
            journal.storage.delete(f"clone-source-{workflow_id}-transfer")
            workflows.update(workflow_id, cleanup_status="complete")
        except Exception:
            workflows.update(workflow_id, cleanup_status="pending")

    @staticmethod
    def _open_clone_package(source, storage):
        from contextlib import contextmanager
        from pathlib import Path
        from ._imports import _checked_package_view, prepared_import_package
        from .exchange import validate_exchange_archive
        import io

        @contextmanager
        def open_package():
            candidate = source
            if isinstance(candidate, dict) and set(candidate) == {"resourceId"}:
                if not isinstance(candidate["resourceId"], str) or not candidate["resourceId"]:
                    raise ValueError("source resourceId must be a nonempty string")
                from ._recovery.journal import _is_uuid
                if not isinstance(candidate["resourceId"], str) or not _is_uuid(candidate["resourceId"]):
                    raise ValueError("source resourceId must be a UUID")
            materialize = getattr(candidate, "materialize", None)
            if callable(materialize):
                candidate = materialize()
            view = _checked_package_view(candidate)
            if view is not None:
                with prepared_import_package(view, storage=storage) as package:
                    yield package
                return
            if isinstance(candidate, (str, Path)):
                stream_context = Path(candidate).open("rb")
            elif isinstance(candidate, bytes):
                stream_context = io.BytesIO(candidate)
            elif callable(getattr(candidate, "read", None)):
                stream_context = candidate
            else:
                raise TypeError("source must be a complete Exchange package or checked package provider")
            close_stream = stream_context is not candidate or isinstance(candidate, (str, Path, bytes))
            try:
                with validate_exchange_archive(stream_context, storage=storage) as package:
                    yield package
            finally:
                if close_stream:
                    stream_context.close()
        return open_package()

    def _run_server_action(self, journal, identity, token, procedure, arguments, *,
                           result_format=None, recovery_key=None):
        from ._recovery.journal import RESULT_FORMAT_CLONE
        if recovery_key is None or not isinstance(recovery_key, str):
            raise ValueError("Server action recovery requires a durable caller-owned key")
        key = recovery_key
        record = journal.load_if_present(identity, key)
        if record is None:
            record = journal.create(identity, procedure, list(arguments), 0,
                                    result_format=result_format, key=key)
        elif (record["procedure"] != procedure or record["arguments"] != list(arguments)
              or record.get("result_format") != result_format):
            raise DepositError("RECOVERY_IDENTITY_MISMATCH", "Saved server action differs from clone workflow intent")
        with journal.action_lock(key):
            current = journal.load(identity, key)
            if current["state"] == "released":
                return {"key": key, "outcome": current["outcome"]}
            if current["state"] == "failed":
                from .errors import _saved_failure_error
                raise _saved_failure_error(current["failure"])
            already_saved = current["state"] in {"outcome_saved", "release_sent"}
            if already_saved:
                outcome = current["outcome"]
                operation_id = current["operation_id"]
            else:
                if current["state"] == "intent":
                    journal.update(identity, key, state="prepare_sent")
                try:
                    prepared = self._recovery_control("deposit.action.status", key, token)
                except DepositError as exc:
                    if exc.code != "NOT_FOUND":
                        raise
                    # Each server-scoped action advances the server revision. Refresh
                    # the session baseline before preparing the next frozen action.
                    self._invoke_coordinated_server_procedure("deposit.utils.refresh", token, "server")
                    prepared = self._invoke_coordinated_server_procedure("deposit.action.prepare", token,
                                                             key, procedure, list(arguments))
                if prepared.get("state") == "committed":
                    status = prepared
                else:
                    for field in ("operationId", "scope"):
                        if not isinstance(prepared.get(field), str) or not prepared[field]:
                            raise DepositError("INVALID_RESPONSE", "Prepared server action returned invalid identity")
                    if (prepared.get("scope") != "server" or prepared.get("procedure") != procedure
                            or prepared.get("recoveryKey") != key):
                        raise DepositError("RECOVERY_IDENTITY_MISMATCH", "Prepared server action does not match the saved operator intent")
                    operation_id = prepared.get("operationId")
                    if current["state"] == "COMMITTED_RESULT_PENDING":
                        raise DepositError("RECOVERY_IDENTITY_MISMATCH", "Clone result journal conflicts with Server action status")
                    if current["state"] not in {"prepared", "executing"}:
                        journal.update(identity, key, state="prepared", operation_id=operation_id)
                    resumed = self._recovery_control("deposit.action.resume", key, token)
                    if resumed.get("state") == "committed":
                        status = resumed
                    else:
                        for field in ("operationId", "scope"):
                            if not isinstance(resumed.get(field), str) or not resumed[field]:
                                raise DepositError("INVALID_RESPONSE", "Resumed server action returned invalid identity")
                        if (resumed.get("scope") != "server" or resumed.get("procedure") != procedure
                                or resumed.get("recoveryKey") != key):
                            raise DepositError("RECOVERY_IDENTITY_MISMATCH", "Resumed server action does not match the saved operator intent")
                        journal.update(identity, key, state="executing", operation_id=resumed.get("operationId"))
                        try:
                            result = self._invoke_coordinated_server_procedure(procedure, token, *arguments)
                        except BaseException:
                            # Keep the same prepared identity; callers can resume it after reconnecting.
                            raise
                        status = {"state": "committed", "operationId": resumed.get("operationId"),
                                  "outcome": result}
                outcome = status.get("outcome")
                operation_id = status.get("operationId")
                if not isinstance(outcome, dict):
                    raise DepositError("INVALID_RESPONSE", "Prepared server action returned no saved result")
                if result_format == RESULT_FORMAT_CLONE:
                    from ._recovery.journal import valid_import_result_envelope
                    if (not valid_import_result_envelope(outcome) or outcome.get("kind") != "clone"
                            or outcome.get("operationId") != operation_id):
                        raise DepositError("RECOVERY_IDENTITY_MISMATCH", "Clone result does not match its committed action")
                    if current["state"] != "COMMITTED_RESULT_PENDING":
                        journal.update(identity, key, state="COMMITTED_RESULT_PENDING", outcome=outcome,
                                       operation_id=operation_id)
                    try:
                        journal.persist_import_result(
                            identity, key, outcome,
                            lambda collection, cursor, limit: self._invoke_coordinated_server_procedure(
                                "deposit.get.cloneResultPage", token, outcome["resultId"], collection, cursor, limit
                            ),
                        )
                    except BaseException as exc:
                        raise DepositError("COMMITTED_RESULT_PENDING", "Clone committed, but its receipt-bound mappings are not fully saved") from exc
                    journal.update(identity, key, state="outcome_saved")
                else:
                    journal.update(identity, key, state="outcome_saved", outcome=outcome,
                                   operation_id=operation_id)
            # Release detaches whichever action is currently on this session.
            # A lost release reply may have left a different key attached since
            # this journal entered release_sent, so bind this key every time.
            self._recovery_control("deposit.action.resume", key, token)
            if current["state"] != "release_sent":
                journal.update(identity, key, state="release_sent")
            release = self._single("CYPHER 25 CALL deposit.action.release($token) YIELD value RETURN value",
                                   {"token": token})
            if release.get("value") is not True:
                raise DepositError("RELEASE_UNCONFIRMED", "Prepared server action release was not confirmed")
            journal.update(identity, key, state="released")
            return {"key": key, "outcome": outcome}

    def recovered_operations(self, *, recovery_directory: str | Path | None = None) -> tuple[RecoveredOperation, ...]:
        """List retained results whose server release was confirmed."""
        from ._recovery.journal import ProtectedJournal
        from .recovery import recovered_operation

        journal = ProtectedJournal(recovery_directory)
        records = journal.records_all()
        self._recovery_journals.update({record["public_id"]: journal for record in records})
        return tuple(recovered_operation(record, journal=journal) for record in records if record["state"] == "released")

    def pending_imports(self, *, recovery_directory: str | Path | None = None):
        """Discover safe durable workflow references, including retained terminal outcomes."""
        from ._import_workflows import ImportWorkflowStore
        from ._recovery.journal import ProtectedJournal
        journal = ProtectedJournal(recovery_directory)
        workflows = ImportWorkflowStore(journal.storage)
        pending = workflows.pending()
        for workflow in pending:
            self._import_workflow_stores[workflow.id] = workflows
        return iter(pending)

    def inspect_import(self, workflow):
        """Inspect one local workflow record without authenticating or acting remotely."""
        from ._import_workflows import ImportWorkflow, ImportWorkflowStore
        if not isinstance(workflow, ImportWorkflow):
            raise TypeError("workflow must be an ImportWorkflow reference")
        workflows = self._import_workflow_stores.get(workflow.id)
        if workflows is not None:
            return workflows.status(workflow.id)
        for store in tuple(self._stores):
            if store._namespace_id == workflow.target_namespace_id:
                workflows = ImportWorkflowStore(store._journal.storage)
                self._import_workflow_stores[workflow.id] = workflows
                return workflows.status(workflow.id)
        raise ValueError("Open the receiving Store or supply its recovery directory to pending_imports")

    def pending_clones(self, *, recovery_directory: str | Path | None = None):
        """Discover local clone workflows without revealing paths or authenticating remotely."""
        from ._clone_workflows import CloneWorkflowStore
        from ._recovery.journal import ProtectedJournal

        workflows = CloneWorkflowStore(ProtectedJournal(recovery_directory).storage)
        pending = workflows.pending()
        self._clone_workflow_stores.update({workflow.id: workflows for workflow in pending})
        return iter(pending)

    def inspect_clone(self, workflow, *, recovery_directory: str | Path | None = None):
        """Inspect one protected clone phase without contacting Neo4j or reading staged bytes."""
        from ._clone_workflows import CloneWorkflow, CloneWorkflowStore
        from ._recovery.journal import ProtectedJournal

        if not isinstance(workflow, CloneWorkflow):
            raise TypeError("workflow must be a CloneWorkflow reference")
        workflows = self._clone_workflow_stores.get(workflow.id)
        if workflows is None:
            workflows = CloneWorkflowStore(ProtectedJournal(recovery_directory).storage)
            self._clone_workflow_stores[workflow.id] = workflows
        return workflows.status(workflow.id)

    def resume_clone(self, workflow, *, username: str, password: str,
                     source=None, recovery_directory: str | Path | None = None):
        """Reauthenticate the original actor and continue a protected server-scoped clone."""
        from ._clone_workflows import CloneWorkflow, CloneWorkflowStore
        from ._recovery.identity import DeploymentIdentity, IdentityUnavailableError, read_deployment_identity
        from ._recovery.journal import ProtectedJournal

        if self._closed:
            raise RuntimeError("Client is closed")
        if not isinstance(workflow, CloneWorkflow):
            raise TypeError("workflow must be a CloneWorkflow reference")
        workflows = self._clone_workflow_stores.get(workflow.id)
        if workflows is None:
            workflows = CloneWorkflowStore(ProtectedJournal(recovery_directory).storage)
            self._clone_workflow_stores[workflow.id] = workflows
        record = workflows.get(workflow.id)
        expected_identity = DeploymentIdentity.from_dict(record["identity"])
        try:
            initial = read_deployment_identity(
                self._driver, database=self._database, actor_id=expected_identity.actor_id,
                namespace_id=None, server_scope=True,
            )
        except IdentityUnavailableError as exc:
            raise DepositError("IDENTITY_MISMATCH", "Clone workflow identity could not be verified") from exc
        if not expected_identity.matches(initial):
            raise DepositError("IDENTITY_MISMATCH", "Clone workflow belongs to another server or database")
        recovery_client = Client(self._driver, database=initial.database_name)
        token = recovery_client.login(username, password)
        try:
            profile = recovery_client.get_session(token)
            user = profile.get("user")
            actor_id = user.get("id") if isinstance(user, dict) else None
            if actor_id != expected_identity.actor_id:
                raise DepositError("IDENTITY_MISMATCH", "Credentials authenticate a different Deposit actor")
            recovery_client.require_clone_capabilities()
            current = read_deployment_identity(
                self._driver, database=initial.database_name, actor_id=actor_id,
                namespace_id=None, server_scope=True,
            )
            if not expected_identity.matches(current):
                raise DepositError("IDENTITY_MISMATCH", "Clone workflow belongs to another server or database")
            journal = ProtectedJournal(store=workflows.storage)
            with workflows.lock(workflow.id):
                record = workflows.get(workflow.id)
                if record["transfer_id"] is not None and record["state"] == "SOURCE_REQUIRED":
                    # Bind the reserved spool before deciding whether the caller
                    # may supply a first source or before continuing recovery.
                    self._recover_clone_source_intent(workflows, workflow.id, journal, current)
                    record = workflows.get(workflow.id)
                if record["state"] == "SOURCE_REQUIRED" and source is not None:
                    self._freeze_clone_source(workflows, workflow.id, journal, current, source)
                elif source is not None:
                    raise DepositError("CLONE_SOURCE_ALREADY_FROZEN", "Replacement input is allowed only before package freeze")
                if record["state"] == "SOURCE_REQUIRED" and source is None:
                    return workflows.status(workflow.id)
                result = recovery_client._continue_clone_workflow(
                    workflows, workflow.id, journal, current, token
                )
                return workflows.status(workflow.id, result=result)
        except IdentityUnavailableError as exc:
            raise DepositError("IDENTITY_MISMATCH", "Clone workflow identity could not be verified") from exc
        finally:
            try:
                recovery_client.logout(token)
            except Exception:
                pass

    @staticmethod
    def _recover_clone_source_intent(workflows, workflow_id, journal, identity):
        from ._recovery.transfers import ProtectedTransferSpool

        record = workflows.get(workflow_id)
        spool = ProtectedTransferSpool(journal, identity)
        intent = spool.read_clone_intent(workflow_id)
        if intent["transfer_id"] != record["transfer_id"]:
            raise DepositError("RECOVERY_IDENTITY_MISMATCH", "Clone workflow and source reservation differ")
        transfer = Client._clone_source_manifest(spool, intent)
        if transfer["state"] == "staging":
            raise DepositError(
                "CLONE_SOURCE_INCOMPLETE",
                "The reserved clone source was interrupted before freezing; retained storage cannot accept replacement input",
            )
        spool.verify_content(transfer)
        workflows.update(workflow_id, state="FROZEN", phase="frozen", sha256=transfer["sha256"],
                         total_bytes=transfer["bytes"], verified_offset=0)

    @staticmethod
    def _clone_source_manifest(spool, intent):
        from ._recovery.transfers import TransferError
        try:
            transfer = spool.load(intent["transfer_id"])
        except TransferError as exc:
            raise DepositError("CLONE_SOURCE_INCOMPLETE", "The reserved clone source was interrupted before its first durable checkpoint") from exc
        if transfer["media_type"] != "application/vnd.deposit.namespace-clone":
            raise DepositError("RECOVERY_IDENTITY_MISMATCH", "Clone source reservation refers to another transfer")
        return transfer

    def resume_import(self, workflow, *, target_store, source_store=None):
        """Resume exact frozen work; PLAN_READY remains an explicit apply barrier."""
        from ._import_workflows import ImportWorkflow, ImportWorkflowStore
        from .exchange import ExchangeBundle
        if not isinstance(workflow, ImportWorkflow):
            raise TypeError("workflow must be an ImportWorkflow reference")
        if getattr(target_store, "_client", None) is not self:
            raise ValueError("target_store must belong to this Client")
        workflows = ImportWorkflowStore(target_store._journal.storage)
        with workflows.lock(workflow.id):
            record = workflows.get(workflow.id)
            self._check_import_bindings(record, target_store, source_store)
            state = record["resume_state"] if record["state"] == "OUTCOME_UNCERTAIN" else record["state"]
            if state in {"ABANDON_REQUESTED", "ABANDON_PENDING"}:
                return self._abandon_import_locked(workflows, workflow.id, target_store, source_store)
            if state in {"APPLIED", "FAILED", "CANCELLED", "EXPIRED", "ABANDONED"}:
                target_store._cleanup_import_workflow(workflows, workflow.id, source_store=source_store)
                return workflows.status(workflow.id)
            if state == "PLAN_READY":
                # This bounded observation never analyzes or applies.
                target_store._observe_import_plan(workflows, workflow.id)
                return workflows.status(workflow.id)
            try:
                if record["apply_requested"]:
                    result = target_store._apply_import_workflow(workflows, workflow.id)
                    return workflows.status(workflow.id, result=result)
                if state in {"TARGET_PACKAGE_READY", "ANALYSIS_REQUESTED"}:
                    if record["routing_decision"] == "same_server_actor_grant" and source_store is not None:
                        with ExchangeBundle(source_store, record["source_export_result"], workflow_id=workflow.id) as bundle:
                            bundle._stage_id = record["source_stage_id"] or bundle._stage_id
                            result = target_store._analyze_import_bundle(bundle, record["frozen_rules"], workflows,
                                                                       workflows.reference(record))
                    else:
                        try:
                            result = target_store._workflow_analysis(workflows, workflow.id)
                        except DepositError as exc:
                            if (record["routing_decision"] == "same_server_actor_grant"
                                    and target_store._relay_after_direct_denial(workflows, workflow.id, exc)):
                                return workflows.status(workflow.id)
                            raise
                    return workflows.status(workflow.id, result=result)
                # A complete receiving stage no longer depends on a source session.
                if record["package_stage_id"]:
                    from .exchange import DurableExchangeFile
                    storage = target_store._journal.storage
                    manifest_name = f"import-stage-{record['package_stage_id']}-manifest"
                    # Only a successful presence check proves the pre-manifest
                    # crash window. Present corrupt/inaccessible checkpoints
                    # remain errors and must never be replaced during recovery.
                    if storage.has_record(manifest_name):
                        stage = DurableExchangeFile(storage, record["package_stage_id"], identity=record["target_identity"])
                        complete = stage.manifest["complete"]
                        stage.close()
                        if complete:
                            result = target_store._continue_import_workflow(workflows, workflow.id)
                            return workflows.status(workflow.id, result=result)
                    if record["source_namespace_id"] is None:
                        raise DepositError("IMPORT_FREEZE_INTERRUPTED", "Caller input was lost before the immutable freeze completed")
                if record["source_namespace_id"] is not None:
                    if source_store is None:
                        return workflows.status(workflow.id)
                    export_result = record["source_export_result"]
                    if export_result is None:
                        export_key = record["action_keys"].get("export")
                        if not isinstance(export_key, str):
                            raise DepositError("RECOVERY_IDENTITY_MISMATCH", "Source export action reference is missing")
                        export_result = source_store._workflow_action(export_key, "deposit.save.data",
                                                                     record["source_export_arguments"])
                        workflows.update(workflow.id, state="SOURCE_REFERENCE_READY", phase="source_reference_ready",
                                         source_export_result=export_result)
                    with ExchangeBundle(source_store, export_result, workflow_id=workflow.id) as bundle:
                        bundle._stage_id = record["source_stage_id"] or bundle._stage_id
                        result = target_store._import_exchange_workflow(bundle, record["frozen_rules"], workflows,
                                                                       workflows.reference(record))
                    return workflows.status(workflow.id, result=result)
                if state in {"CREATED", "SOURCE_STAGING"}:
                    raise DepositError("IMPORT_FREEZE_INTERRUPTED", "Caller input was lost before the immutable freeze completed")
                result = target_store._continue_import_workflow(workflows, workflow.id)
                return workflows.status(workflow.id, result=result)
            except Exception as exc:
                target_store._import_workflow_failure(workflows, workflow.id, exc)
                raise

    @staticmethod
    def _check_import_bindings(record, target_store, source_store):
        if (record["target_namespace_id"] != target_store.namespace_id
                or record["target_identity"] != target_store._identity.to_dict()):
            raise DepositError("IDENTITY_MISMATCH", "Receiving Store does not match the workflow binding")
        if source_store is not None and (source_store.namespace_id != record["source_namespace_id"]
                or source_store._identity.to_dict() != record["source_identity"]):
            raise DepositError("IDENTITY_MISMATCH", "Source Store does not match the workflow binding")

    def abandon_import(self, workflow, *, target_store=None, source_store=None):
        """Release local ownership once existing actions and plan ownership resolve."""
        from ._import_workflows import ImportWorkflow, ImportWorkflowStore
        if not isinstance(workflow, ImportWorkflow):
            raise TypeError("workflow must be an ImportWorkflow reference")
        selected = target_store
        if selected is None:
            selected = next((store for store in tuple(self._stores)
                             if store._namespace_id == workflow.target_namespace_id), None)
        if selected is None or selected._client is not self:
            raise ValueError("An authenticated receiving Store is required to abandon this workflow")
        workflows = ImportWorkflowStore(selected._journal.storage)
        with workflows.lock(workflow.id):
            record = workflows.get(workflow.id)
            self._check_import_bindings(record, selected, source_store)
            if record["state"] in {"APPLIED", "ABANDONED", "FAILED", "EXPIRED", "CANCELLED"}:
                selected._cleanup_import_workflow(workflows, workflow.id, source_store=source_store)
                return workflows.status(workflow.id)
            if record["state"] not in {"ABANDON_REQUESTED", "ABANDON_PENDING"}:
                workflows.update(workflow.id, resume_state=(record["resume_state"] or record["state"]),
                                 state="ABANDON_REQUESTED", phase="abandon_requested")
            return self._abandon_import_locked(workflows, workflow.id, selected, source_store)

    def _abandon_import_locked(self, workflows, workflow_id, target, source):
        from ._recovery.identity import DeploymentIdentity
        from ._recovery.transfers import ProtectedTransferSpool
        record = workflows.get(workflow_id)
        if record["state"] == "OUTCOME_UNCERTAIN":
            # An earlier authorization-hidden observation cannot be upgraded
            # to abandonment merely because a stale local deadline elapsed.
            # Re-enter the abandoned branch so an authoritative observation or
            # a later unavailable response may resolve/retry this same owner.
            workflows.update(workflow_id, state=record["resume_state"], phase="abandon_pending",
                             resume_state=None)
            record = workflows.get(workflow_id)
        unresolved = False
        applied_key = None
        actions = dict(record["action_keys"])
        spool = ProtectedTransferSpool(target._journal, target._identity)
        manifest = None
        if record["upload_id"] and target._journal.storage.has_record(f"transfer-{record['upload_id']}-manifest"):
            manifest = spool.load(record["upload_id"])
            for name, field in (("upload_begin", "begin_action_key"), ("upload_chunk", "pending_action_key"),
                                ("upload_finish", "finish_action_key")):
                if manifest[field] is not None:
                    actions[name] = manifest[field]
        # Observe only known actions. Abandon does not prepare/execute original
        # analysis or apply; a prepared unexecuted action uses existing release.
        for name, key in actions.items():
            if name == "upload_transfer":
                continue
            store = source if name == "export" else target
            identity = (DeploymentIdentity.from_dict(record["source_identity"]) if name == "export" else target._identity)
            journal = store._journal if store is not None else target._journal
            envelope = None
            if store is None:
                # An absent receipt in the receiving root does not establish
                # source ownership. A shared root may contain a durable failed
                # receipt (no execution effect remains) or an executing receipt
                # (whose committed result cannot be retrieved without a Store).
                # Preserve the latter pending until source authentication is
                # supplied; never infer absence from an uninspected source root.
                with target._serialized(), journal.action_lock(key):
                    action = journal.load_if_present(identity, key)
                    if name == "export":
                        unresolved |= action is None or action["state"] not in {
                            "released", "failed", "conflicted"
                        }
                        continue
                    unresolved |= action is not None
                continue
            with store._serialized(), journal.action_lock(key):
                # The lookup, action status and every possible local failure
                # write must share recovery's exact per-action lock.
                action = journal.load_if_present(identity, key)
                if action is None:
                                # Never infer that a genuine absent action from a different
                    # recovery root proves the workflow was uninitiated. NOT_FOUND
                    # is also the server's authorization-hidden response for a
                    # real receiving action; preserve the unresolved ownership
                    # for authorized resume rather than complete a false abandon.
                    unresolved = True
                    if record["state"] in {"SOURCE_EXPORT_REQUESTED", "SOURCE_REFERENCE_READY",
                            "CREATED", "TARGET_PACKAGE_READY", "ANALYSIS_REQUESTED", "APPLYING"}:
                        continue
                    continue
                if action["state"] in {"failed", "conflicted"}:
                    continue
                if action["state"] == "released":
                    envelope = action.get("outcome")
                else:
                    try:
                        status = store._control("deposit.action.status", key)
                    except DepositError as exc:
                        if exc.code != "NOT_FOUND" or not _abandon_local_absent_state(action):
                            raise
                        status = {"state": "absent"}
                    if status.get("state") == "committed":
                        envelope = store._save_and_release(action, status)
                    elif status.get("state") in {"prepared", "absent"}:
                        failure = {"code": "CANCELLED", "message": "Import action ownership was abandoned",
                                   "details": {}, "request_id": None, "retryable": False}
                        if status["state"] == "absent":
                            # NOT_FOUND is terminal only for a local action that
                            # never reached the Server: a bare intent whose
                            # prepare reply was never delivered, or the durable
                            # CANCELLED cancellation already saved for such an
                            # intent (retry after an interrupted abandonment).
                            # A delivered prepare (prepare_sent) or a saved
                            # cancellation bound to an operation stays pending.
                            if action["state"] == "prepare_sent":
                                unresolved = True
                                continue
                            if action["state"] == "failure_saved" and action["operation_id"] is not None:
                                unresolved = True
                                continue
                            if action["state"] == "failure_saved":
                                journal.update(identity, key, state="failed")
                            else:
                                journal.update(identity, key, state="failure_saved", failure=failure)
                                journal.update(identity, key, state="failed")
                        else:
                            # Reconcile an acknowledged prepare whose reply was
                            # lost before running the normal execution boundary.
                            # Keep immutable replay arguments and bind every
                            # durable resume/resume/recovery observation to the
                            # identity proved by the authoritative status.
                            current = action
                            original_operation_id = current.get("operation_id")
                            if original_operation_id is not None and status.get("operationId") != original_operation_id:
                                unresolved = True
                                continue
                            if original_operation_id is None and status.get("operationId") is None:
                                unresolved = True
                                continue
                            if current["state"] in {"intent", "prepare_sent"}:
                                if original_operation_id is None and status.get("operationId") is not None:
                                    current = journal.update(identity, key, state="prepared",
                                                             operation_id=status.get("operationId"))
                                current = journal.update(identity, key, state="executing")
                            if current["state"] == "prepared":
                                current = journal.update(identity, key, state="executing")
                            failed = journal.update(identity, key, state="failure_saved", failure=failure)
                            try:
                                envelope = store._finish_failed_action(failed, status=status)
                            except DepositError as exc:
                                if exc.code != "CANCELLED":
                                    raise
                    else:
                        unresolved = True
            if envelope is not None and name in {"analysis", "apply"}:
                result = target._import_result(key, envelope)
                try:
                    if name == "analysis" and result["kind"] != "diagnostic":
                        workflows.update(workflow_id, plan_id=result.plan_id, result_action_key=key,
                                         plan_expires_at=(workflows.get(workflow_id)["plan_expires_at"]
                                             if workflows.get(workflow_id)["plan_expires_at"] is not None else result["expiresAt"]))
                    elif name == "apply":
                        applied_key = key
                finally:
                    result.close()
        record = workflows.get(workflow_id)
        if applied_key is not None and not unresolved:
            workflows.update(workflow_id, state="APPLIED", phase="applied", result_action_key=applied_key,
                             resume_state=None, failure=None)
            target._cleanup_import_workflow(workflows, workflow_id, source_store=source)
            return workflows.status(workflow_id)
        if record["plan_id"] and not unresolved:
            observation = target._observe_import_plan(workflows, workflow_id, abandoning=True)
            if observation == "ready":
                return workflows.status(workflow_id)
            if observation == "applied":
                workflows.update(workflow_id, state="APPLIED", phase="applied", resume_state=None, failure=None)
        if manifest is not None and manifest["begin_action_key"] is not None and not unresolved:
            begin = target._journal.load_if_present(target._identity, manifest["begin_action_key"])
            upload_id = manifest["upload_id"] or ((begin.get("outcome") or {}).get("uploadId") if begin else None)
            finish = (target._journal.load_if_present(target._identity, manifest["finish_action_key"])
                      if manifest["finish_action_key"] is not None else None)
            finished = (manifest["state"] == "finished" or (finish is not None
                        and finish["state"] == "released" and (finish.get("outcome") or {}).get("state") == "ready"))
            if upload_id is not None and not finished:
                abort_key = record["action_keys"].get("upload_abort") or str(uuid.uuid4())
                workflows.update(workflow_id, action_keys=dict(workflows.get(workflow_id)["action_keys"], upload_abort=abort_key))
                target._workflow_action(abort_key, "deposit.resource.abortUpload", [upload_id])
                # This established action releases the upload lease. No new
                # Server cancellation route or Namespace import effect occurs.
        current = workflows.get(workflow_id)
        if unresolved:
            # An unresolved action can outlast a plan observation. Preserve an
            # existing lease wait so resolving the action cannot renew it early.
            phase = current["phase"] if current["state"] == "ABANDON_PENDING" else "abandon_pending"
            workflows.update(workflow_id, state="ABANDON_PENDING", phase=phase, cleanup_status="pending")
        else:
            if current["state"] != "APPLIED":
                workflows.update(workflow_id, state="ABANDONED", phase="abandoned")
            target._cleanup_import_workflow(workflows, workflow_id, source_store=source)
        return workflows.status(workflow_id)

    def recover(self, operation: PendingOperation, *, username: str, password: str) -> RecoveredOperation:
        """Authenticate and resolve one operation reference without exposing its recovery key."""
        from ._recovery.identity import DeploymentIdentity, IdentityUnavailableError, read_deployment_identity
        from ._recovery.journal import ProtectedJournal, _SERVER_SCOPED_OPERATIONS
        from .recovery import PendingOperation, RecoveredOperation, recovered_operation
        from .errors import _saved_failure_error

        if self._closed:
            raise RuntimeError("Client is closed")
        if not isinstance(operation, PendingOperation):
            raise TypeError("operation must be a PendingOperation returned by this Client")
        journal = self._recovery_journals.get(operation.id) or ProtectedJournal()
        record = journal.find_public_id(operation.id)
        expected_identity = DeploymentIdentity.from_dict(record["identity"])
        server_scope = expected_identity.namespace_id is None
        if server_scope != (record["procedure"] in _SERVER_SCOPED_OPERATIONS):
            raise DepositError("IDENTITY_MISMATCH", "Recovery procedure does not match its recorded scope")
        # Resolve and validate the Client's actual target before authenticating.
        # In particular, never follow the database name from the journal when
        # this Client is explicitly configured for another database.
        try:
            initial_identity = read_deployment_identity(
                self._driver,
                database=self._database,
                actor_id=expected_identity.actor_id,
                namespace_id=expected_identity.namespace_id,
                server_scope=server_scope,
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
                server_scope=server_scope,
            )
            if not stored_identity.matches(expected_identity):
                raise DepositError("IDENTITY_MISMATCH", "Recovery record belongs to a different server or database")
            profile = recovery_client.get_session(token)
            user = profile.get("user")
            actor_id = user.get("id") if isinstance(user, dict) else None
            if actor_id != stored_identity.actor_id:
                raise DepositError("IDENTITY_MISMATCH", "Credentials authenticate a different Deposit actor")
            if record["procedure"] in {
                    "deposit.import.cloneNamespace", "deposit.import.beginCloneUpload",
                    "deposit.import.cloneUploadChunk", "deposit.import.finishCloneUpload",
                    "deposit.import.abortCloneUpload"}:
                recovery_client.require_clone_capabilities()
            current_identity = read_deployment_identity(
                self._driver,
                database=initial_identity.database_name,
                actor_id=actor_id,
                namespace_id=expected_identity.namespace_id,
                server_scope=server_scope,
            )
            if not stored_identity.matches(current_identity):
                raise DepositError("IDENTITY_MISMATCH", "Recovery record belongs to a different server or database")
            record = journal.load(stored_identity, record["key"])
            if record["state"] == "released":
                return recovered_operation(record, journal=journal)
            if record["state"] == "failed":
                raise _saved_failure_error(record["failure"])

            if server_scope:
                if record["procedure"] == "deposit.import.cloneNamespace":
                    return recovery_client._recover_server_clone(journal, stored_identity, record, token)
                return recovery_client._recover_server_action(journal, stored_identity, record, token)

            if record["procedure"] == "deposit.delete.namespace":
                with journal.action_lock(record["key"]):
                    # The status check and receipt recovery form one action. A
                    # concurrent Store/recovery process must not resume or release
                    # the same prepared action while this receipt is being saved.
                    record = journal.load(stored_identity, record["key"])
                    if record["state"] == "released":
                        return recovered_operation(record, journal=journal)
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
            if record["state"] == "failed":
                raise _saved_failure_error(record["failure"])
            if record["state"] != "released":
                raise DepositError("RECOVERY_PENDING", "Operation remains unresolved after safe recovery")
            result = recovered_operation(record, journal=journal)
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

    def _recover_server_action(self, journal, identity, record, token):
        """Resolve one exact server-scoped staging action without a Namespace context."""
        from ._recovery.journal import _SERVER_SCOPED_OPERATIONS
        with journal.action_lock(record["key"]):
            current = journal.load(identity, record["key"])
            if current["state"] == "released":
                from .recovery import recovered_operation
                return recovered_operation(current, journal=journal)
            if current["procedure"] not in _SERVER_SCOPED_OPERATIONS or current["procedure"] == "deposit.import.cloneNamespace":
                raise DepositError("SCOPE_MISMATCH", "Recovery action is not a server-scoped staging operation")
            try:
                state = self._recovery_control("deposit.action.status", current["key"], token)
            except DepositError as exc:
                if exc.code != "NOT_FOUND":
                    raise
                self._invoke_coordinated_server_procedure("deposit.utils.refresh", token, "server")
                state = self._invoke_coordinated_server_procedure("deposit.action.prepare", token, current["key"],
                                                      current["procedure"], current["arguments"])
            operation_id = state.get("operationId") or current.get("operation_id")
            if not isinstance(operation_id, str) or not operation_id:
                raise DepositError("RECOVERY_IDENTITY_MISMATCH", "Server action returned no operation identity")
            if current.get("operation_id") is not None and current["operation_id"] != operation_id:
                raise DepositError("RECOVERY_IDENTITY_MISMATCH", "Server action identity changed")
            if state.get("state") == "committed":
                status = state
            elif current["state"] in {"intent", "prepare_sent", "prepared", "executing"}:
                current = journal.update(identity, current["key"], state="prepared", operation_id=operation_id)
                self._recovery_control("deposit.action.resume", current["key"], token)
                current = journal.update(identity, current["key"], state="executing",
                                         operation_id=operation_id)
                outcome = self._invoke_coordinated_server_procedure(current["procedure"], token, *current["arguments"])
                status = {"state": "committed", "operationId": operation_id, "outcome": outcome}
            elif current["state"] == "outcome_saved":
                status = {"state": "committed", "operationId": operation_id, "outcome": current["outcome"]}
            else:
                raise DepositError("RECOVERY_IDENTITY_MISMATCH", "Saved Server action release cannot resolve before its outcome")
            outcome = current["outcome"] if current["state"] == "outcome_saved" else status.get("outcome")
            if not isinstance(outcome, dict):
                raise DepositError("INVALID_RESPONSE", "Server staging action returned no saved result")
            journal.update(identity, current["key"], state="outcome_saved", outcome=outcome,
                            operation_id=operation_id)
            self._recovery_control("deposit.action.resume", current["key"], token)
            journal.update(identity, current["key"], state="release_sent")
            release = self._single("CYPHER 25 CALL deposit.action.release($token) YIELD value RETURN value",
                                   {"token": token})
            if release.get("value") is not True:
                raise DepositError("RELEASE_UNCONFIRMED", "Prepared server action release was not confirmed")
            from .recovery import recovered_operation
            released = journal.update(identity, current["key"], state="released")
            return recovered_operation(released, journal=journal)

    def _recover_server_clone(self, journal, identity, record, token):
        from ._recovery.journal import RESULT_FORMAT_CLONE, valid_import_result_envelope
        self.require_clone_capabilities()
        with journal.action_lock(record["key"]):
            current = journal.load(identity, record["key"])
            if current["state"] == "released":
                from .recovery import recovered_operation
                return recovered_operation(current, journal=journal)
            if (current["procedure"] != "deposit.import.cloneNamespace"
                    or current.get("result_format") != RESULT_FORMAT_CLONE):
                raise DepositError("SCOPE_MISMATCH", "Recovery action is not a durable Namespace clone")
            try:
                state = self._recovery_control("deposit.action.status", current["key"], token)
            except DepositError as exc:
                if exc.code != "NOT_FOUND":
                    raise
                self._invoke_coordinated_server_procedure("deposit.utils.refresh", token, "server")
                state = self._invoke_coordinated_server_procedure("deposit.action.prepare", token, current["key"],
                                                      current["procedure"], current["arguments"])
            operation_id = state.get("operationId") or current.get("operation_id")
            if not isinstance(operation_id, str) or not operation_id:
                raise DepositError("RECOVERY_IDENTITY_MISMATCH", "Clone Server action returned no operation identity")
            if current.get("operation_id") is not None and current["operation_id"] != operation_id:
                raise DepositError("RECOVERY_IDENTITY_MISMATCH", "Clone Server action identity changed")
            elif current["state"] in {"outcome_saved", "release_sent", "COMMITTED_RESULT_PENDING"}:
                envelope = current["outcome"]
            elif state.get("state") == "committed":
                envelope = state.get("outcome")
            elif current["state"] in {"intent", "prepare_sent", "prepared", "executing"}:
                current = journal.update(identity, current["key"], state="prepared", operation_id=operation_id)
                self._recovery_control("deposit.action.resume", current["key"], token)
                current = journal.update(identity, current["key"], state="executing", operation_id=operation_id)
                envelope = self._invoke_coordinated_server_procedure(current["procedure"], token, *current["arguments"])
            else:
                raise DepositError("RECOVERY_IDENTITY_MISMATCH", "Release-sent clone result has no protected outcome")
            if (not valid_import_result_envelope(envelope) or envelope.get("kind") != "clone"
                    or envelope.get("operationId") != operation_id):
                raise DepositError("RECOVERY_IDENTITY_MISMATCH", "Clone result does not match its committed action")
            if current["state"] not in {"outcome_saved", "release_sent", "COMMITTED_RESULT_PENDING"}:
                current = journal.update(identity, current["key"], state="COMMITTED_RESULT_PENDING", outcome=envelope,
                                        operation_id=operation_id)
            journal.persist_import_result(
                identity, current["key"], envelope,
                lambda collection, cursor, limit: self._invoke_coordinated_server_procedure(
                    "deposit.get.cloneResultPage", token, envelope["resultId"], collection, cursor, limit
                ),
            )
            if current["state"] != "outcome_saved":
                current = journal.update(identity, current["key"], state="outcome_saved")
            self._recovery_control("deposit.action.resume", current["key"], token)
            current = journal.update(identity, current["key"], state="release_sent")
            release = self._single("CYPHER 25 CALL deposit.action.release($token) YIELD value RETURN value",
                                   {"token": token})
            if release.get("value") is not True:
                raise DepositError("RELEASE_UNCONFIRMED", "Prepared clone action release was not confirmed")
            from .recovery import recovered_operation
            released = journal.update(identity, current["key"], state="released")
            return recovered_operation(released, journal=journal)

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
        return recovered_operation(current, journal=journal)

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
