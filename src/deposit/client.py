"""Client-side discovery and Deposit authentication smoke path."""

from __future__ import annotations

import json
import uuid
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
    map_driver_error,
    prune_dry_run_procedure,
    recovery_read_procedure,
)
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
        self._import_workflow_stores = {}

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
        if not isinstance(capabilities, list) or "pagedImportResults" not in capabilities:
            raise CompatibilityError(str(capabilities if capabilities is not None else "missing"),
                                     "pagedImportResults", "Deposit import result capability")
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
        unresolved = False
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
            action = journal.load_if_present(identity, key)
            if action is None:
                # Source journals can live in a different protected root.
                unresolved |= store is None
                continue
            if action["state"] in {"failed", "conflicted"}:
                continue
            envelope = action.get("outcome") if action["state"] == "released" else None
            if envelope is None:
                if store is None:
                    unresolved = True
                    continue
                with store._serialized():
                    try:
                        status = store._control("deposit.action.status", key)
                    except DepositError as exc:
                        if exc.code != "NOT_FOUND" or action["state"] not in {"intent", "prepare_sent"}:
                            raise
                        status = {"state": "absent"}
                    if status.get("state") == "committed":
                        envelope = store._save_and_release(action, status)
                    elif status.get("state") in {"prepared", "absent"}:
                        failure = {"code": "CANCELLED", "message": "Import action ownership was abandoned",
                                   "details": {}, "request_id": None, "retryable": False}
                        if status["state"] == "absent":
                            store._journal.update(identity, key, state="failed", failure=failure)
                        else:
                            failed = store._journal.update(identity, key, state="failure_saved", failure=failure)
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
                        workflows.update(workflow_id, state="APPLIED", phase="applied", result_action_key=key)
                finally:
                    result.close()
        record = workflows.get(workflow_id)
        if record["state"] == "APPLIED":
            target._cleanup_import_workflow(workflows, workflow_id, source_store=source)
            return workflows.status(workflow_id)
        if record["plan_id"]:
            observation = target._observe_import_plan(workflows, workflow_id, abandoning=True)
            if observation in {"ready", "unavailable"}:
                unresolved = True
            elif observation == "applied":
                workflows.update(workflow_id, state="APPLIED", phase="applied")
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
        if current["state"] != "APPLIED":
            workflows.update(workflow_id, state="ABANDON_PENDING" if unresolved else "ABANDONED",
                             phase="abandon_pending" if unresolved else "abandoned")
        if not unresolved:
            target._cleanup_import_workflow(workflows, workflow_id, source_store=source)
        else:
            workflows.update(workflow_id, cleanup_status="pending")
        return workflows.status(workflow_id)

    def recover(self, operation: PendingOperation, *, username: str, password: str) -> RecoveredOperation:
        """Authenticate and resolve one operation reference without exposing its recovery key."""
        from ._recovery.identity import DeploymentIdentity, IdentityUnavailableError, read_deployment_identity
        from ._recovery.journal import ProtectedJournal
        from .recovery import PendingOperation, RecoveredOperation, recovered_operation
        from .errors import _saved_failure_error

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
                return recovered_operation(record, journal=journal)
            if record["state"] == "failed":
                raise _saved_failure_error(record["failure"])

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
