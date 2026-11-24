"""Connection-bound synchronous operation invocation."""

from contextlib import contextmanager, suppress
from threading import Lock
from time import sleep
from uuid import uuid7

from neo4j import GraphDatabase
from neo4j.exceptions import Neo4jError

from ._gateway import _manifest, build_query, map_driver_error, tracked_procedure
from .errors import (
    CompatibilityError, DepositCancelled, DepositError, OperationCommittedAfterCancel,
    OutcomeUnknown, TransportError,
)
from ._paging import valid_import_result_envelope


_INFO = "CYPHER 25 CALL deposit.get.info() YIELD data RETURN data"
_CONTROLS = {
    "deposit.action.begin", "deposit.action.cancel", "deposit.get.operation", "deposit.get.session",
    "deposit.get.importPlan", "deposit.get.importResult", "deposit.get.importResultPage", "deposit.get.importResultEntry",
    "deposit.import.discardPlan",
    "deposit.resource.info", "deposit.resource.downloadChunk", "deposit.utils.refresh",
    *(f"deposit.exchange.{name}" for name in ("inspect", "memberPage", "graphPage", "readChunk", "discard")),
}
_IMPORT_RESULTS = {
    "deposit.import.analyze": "analysis",
    "deposit.import.rows": "analysis",
    "deposit.import.dataByPattern": "analysis",
    "deposit.import.apply": "apply",
    "deposit.import.cloneNamespace": "clone",
}
_TRANSFER_RESULTS = {
    "deposit.resource.beginUpload", "deposit.resource.uploadChunk", "deposit.resource.finishUpload",
    "deposit.get.data", "deposit.get.namespaceData",
}


def _endpoint_address(address):
    try:
        host, port = address.host.lower().rstrip("."), address.port
        if host and 0 < port <= 65535:
            return f"[{host}]:{port}" if ":" in host and not host.startswith("[") else f"{host}:{port}"
    except (AttributeError, TypeError):
        pass
    raise TransportError("Neo4j did not report the contacted server address")


def _call(session, procedure, arguments, token, *, prepare=None):
    if procedure in _CONTROLS:
        entry = next(item for item in _manifest()["procedures"] if item["name"] == procedure)
    else:
        entry = tracked_procedure(procedure)
    query, parameters = build_query(entry, arguments, token, portable=True)
    try:
        with session.begin_transaction() as transaction:
            if prepare is not None:
                prepare(transaction)
            result = transaction.run(query, parameters)
            row = dict(result.single(strict=True))
            result.consume()
            if set(row) != set(entry["columns"]):
                raise TransportError("Server returned unexpected procedure columns")
            transaction.commit()
    except Neo4jError as exc:
        raise map_driver_error(exc) from exc
    except (DepositError, TransportError):
        raise
    except Exception as exc:
        raise TransportError("Neo4j procedure invocation failed") from exc
    return next(iter(row.values())) if len(row) == 1 else row


class OperationEndpoint:
    """Authenticated endpoint with separate one-connection execution and control pools."""

    def __init__(self, client, token, scope, *, expected=None, actor=None):
        self.token = token
        self.scope = scope
        self._actor = actor
        self._lock = Lock()
        self._control_lock = Lock()
        self._active_id = None
        self._stopped = False
        self._closed = False
        self._verified_connection = None
        client._check_driver_support()
        row, summary = client._single_with_summary(_INFO)
        info = row.get("data")
        if not isinstance(info, dict) or not isinstance(summary.database, str) or not summary.database:
            raise TransportError("Operation discovery returned no verified database identity")
        capabilities = info.get("capabilities")
        if (not isinstance(info.get("incarnation"), str) or not isinstance(capabilities, list)
                or any(not isinstance(item, str) for item in capabilities)
                or "operationControl" not in capabilities):
            raise CompatibilityError(str(info.get("version")), "operationControl")
        self.endpoint = _endpoint_address(summary.server.address)
        self.database = summary.database
        self.incarnation = info["incarnation"]
        self._capabilities = frozenset(capabilities)
        if expected is not None and self.binding != expected:
            raise DepositError("SCOPE_MISMATCH", "The Store endpoint changed")
        # Keep the original auth provider, impersonation and TLS trust policy.
        pool = client._driver._pool
        self._config = dict(pool.pool_config)
        self._config.update(max_connection_pool_size=1, max_connection_lifetime=float("inf"))
        self._impersonated = pool.workspace_config.impersonated_user
        self._execution = self._new_driver()
        self._control = None
        try:
            self.live_identity()
        except BaseException:
            self.close()
            raise

    @property
    def binding(self):
        return self.endpoint, self.database, self.incarnation

    def require_exchange(self):
        self._require_capabilities("exchangeIntake")

    def _require_capabilities(self, *required):
        if not self._capabilities.issuperset(required):
            raise CompatibilityError(str(sorted(self._capabilities)), ", ".join(required))

    def _new_driver(self):
        return GraphDatabase.driver(f"bolt://{self.endpoint}", **self._config)

    def _session(self, driver):
        return driver.session(database=self.database, impersonated_user=self._impersonated)

    def _verify_session(self, transaction):
        try:
            result = transaction.run(_INFO)
            info = result.single(strict=True)["data"]
            summary = result.consume()
        except Neo4jError as exc:
            raise map_driver_error(exc) from exc
        except Exception as exc:
            raise TransportError("Could not verify the operation endpoint") from exc
        if (not isinstance(info, dict)
                or (_endpoint_address(summary.server.address), summary.database, info.get("incarnation")) != self.binding):
            raise DepositError("SCOPE_MISMATCH", "The operation endpoint changed")
        capabilities = info.get("capabilities")
        if (not isinstance(capabilities, list) or any(not isinstance(item, str) for item in capabilities)
                or frozenset(capabilities) != self._capabilities):
            raise CompatibilityError(str(capabilities), str(sorted(self._capabilities)))

    def _verify_execution(self, transaction):
        # Session objects can acquire a different Bolt connection between transactions.
        connection = getattr(transaction, "_connection", None)
        if connection is None:
            raise TransportError("Neo4j did not expose the execution connection")
        if connection is not self._verified_connection:
            self._verified_connection = None
            self._verify_session(transaction)
            self._verified_connection = connection
        return connection

    @contextmanager
    def execution(self):
        with self._session(self._execution) as session:
            yield OperationExecution(self, session, verify=self._verify_execution)

    def call(self, procedure, *arguments):
        with self._session(self._execution) as session:
            return _call(session, procedure, arguments, self.token, prepare=self._verify_execution)

    def live_identity(self):
        with self._session(self._execution) as session:
            profile = _call(session, "deposit.get.session", (), self.token, prepare=self._verify_execution)
        self._check_profile(profile)
        return (*self.binding, self._actor)

    def _check_profile(self, profile):
        user = profile.get("user") if isinstance(profile, dict) else None
        actor = user.get("id") if isinstance(user, dict) else None
        if not isinstance(actor, str) or (self._actor is not None and actor != self._actor):
            self._verified_connection = None
            raise DepositError("IDENTITY_MISMATCH", "The authenticated operation actor changed")
        if self.scope != "server" and profile.get("namespaceId") != self.scope:
            self._verified_connection = None
            raise DepositError("SCOPE_MISMATCH", "The authenticated operation Namespace changed")
        self._actor = actor

    def control(self, procedure, *arguments):
        with self._control_lock:
            if self._closed:
                raise TransportError("The operation endpoint is closed")
            if self._control is None:
                self._control = self._new_driver()
            with self._session(self._control) as session:
                return _call(session, procedure, arguments, self.token, prepare=self._verify_session)

    def _operation_status(self, operation_id, cancel):
        procedure = "deposit.action.cancel" if cancel else "deposit.get.operation"
        return self.control(procedure, operation_id, self.scope)

    def interrupt_upload(self):
        """Close the writer connection so a stopped upload cannot be continued."""
        self._execution.close()
        self._verified_connection = None
        self._execution = self._new_driver()

    def check_running(self):
        with self._lock:
            if self._stopped:
                raise DepositError("TRANSFER_INTERRUPTED", "The workflow was stopped between Server operations")

    def reset_cancellation(self):
        with self._lock:
            self._stopped = False

    def cancel_current(self):
        with self._lock:
            self._stopped = True
            operation_id = self._active_id
        if operation_id is not None:
            self._operation_status(operation_id, True)
        return operation_id is not None

    def note_cancel(self, operation_id):
        with self._lock:
            if self._active_id != operation_id:
                return False
            self._stopped = True
            return True

    def close(self):
        with self._control_lock:
            if self._closed:
                return
            self._closed = True
            self._verified_connection = None
            try:
                self._execution.close()
            finally:
                if self._control is not None:
                    self._control.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


class OperationExecution:
    def __init__(self, endpoint, session, *, verify=None):
        self._endpoint = endpoint
        self._session = session
        self._verify = verify

    def run(self, procedure, *arguments, operation_id=None, cleanup=False, observe=None):
        entry = tracked_procedure(procedure)
        operation_id = str(uuid7()) if operation_id is None else operation_id
        endpoint = self._endpoint
        connection = None
        submitted = False

        def prepare_registration(transaction):
            nonlocal connection, submitted
            if self._verify is not None:
                connection = self._verify(transaction)
            if observe is not None:
                result = transaction.run(
                    "CYPHER 25 CALL deposit.get.session($token) YIELD data RETURN data",
                    {"token": endpoint.token},
                )
                profile = result.single(strict=True)["data"]
                result.consume()
                endpoint._check_profile(profile)
                observe(profile)
            submitted = True

        def require_original_connection(transaction):
            if self._verify is not None and transaction._connection is not connection:
                raise TransportError("The registered operation lost its execution connection")

        with endpoint._lock:
            if endpoint._stopped and not cleanup:
                raise DepositError("TRANSFER_INTERRUPTED", "The workflow was stopped between Server operations")
            if endpoint._active_id is not None:
                raise RuntimeError("The execution connection already has an active operation")
            endpoint._active_id = operation_id
        try:
            try:
                registration = (operation_id, endpoint.scope, procedure, list(arguments))
                if self._verify is None and observe is None:
                    submitted = True
                    status = _call(self._session, "deposit.action.begin", registration, endpoint.token)
                else:
                    status = _call(self._session, "deposit.action.begin", registration, endpoint.token,
                                   prepare=prepare_registration)
                if not self._matches(status, operation_id):
                    raise TransportError("Operation registration identity changed")
                if status.get("state") == "CANCELLED_ROLLED_BACK":
                    raise DepositCancelled(operation_id, endpoint.scope)
                if status.get("state") != "REGISTERED":
                    # An existing terminal attempt can be inspected by ID. It
                    # cannot authorize a second execution from this call.
                    return self._settled_result(procedure, operation_id, status)
                checks = {"prepare": require_original_connection} if self._verify is not None else {}
                result = _call(self._session, procedure, (*arguments, operation_id), endpoint.token, **checks)
                if procedure in _IMPORT_RESULTS:
                    self._check_envelope(procedure, operation_id, result)
                with endpoint._lock:
                    stopped = endpoint._stopped
                if stopped and not cleanup:
                    self._completed_after_stop(entry, operation_id, result)
                return result
            except (TransportError, KeyboardInterrupt) as failure:
                if not submitted:
                    raise
                return self._settle(procedure, operation_id, failure, cancel=isinstance(failure, KeyboardInterrupt))
            except DepositError as failure:
                if type(failure) is DepositError and failure.code in {
                    "CANCELLED", "OPERATION_NOT_CLAIMABLE", "OUTCOME_UNKNOWN",
                }:
                    return self._settle(procedure, operation_id, failure)
                raise
        finally:
            with endpoint._lock:
                endpoint._active_id = None

    def _completed_after_stop(self, entry, operation_id, value):
        endpoint = self._endpoint
        procedure = entry["name"]
        status = None
        with suppress(DepositError, TransportError):
            status = endpoint._operation_status(operation_id, False)
        if (not self._matches(status, operation_id) or status.get("procedure") != procedure
                or status.get("state") not in {"COMMITTED", "COMPLETED_NO_DOMAIN_WRITE"}):
            # This invocation already received its COMMIT acknowledgement.
            # An inaccessible status read cannot undo that evidence.
            domain = (entry["scope"] not in {"operational", "exchangeUpload"}
                      or procedure == "deposit.import.apply")
            result = None
            if procedure.startswith("deposit.exchange."):
                result = value
            elif _IMPORT_RESULTS.get(procedure) == "analysis":
                result = {"planId": value["planId"]}
            status = {"operationId": operation_id, "scope": endpoint.scope, "procedure": procedure,
                      "state": "COMMITTED" if domain else "COMPLETED_NO_DOMAIN_WRITE", "result": result}
        raise OperationCommittedAfterCancel(operation_id, endpoint.scope, status)

    def _matches(self, status, operation_id):
        return (isinstance(status, dict) and status.get("operationId") == operation_id
                and status.get("scope") == self._endpoint.scope)

    def _check_envelope(self, procedure, operation_id, envelope):
        kind = _IMPORT_RESULTS[procedure]
        if (not valid_import_result_envelope(envelope) or envelope["kind"] != kind
                or envelope["operationId"] != operation_id
                or (kind != "clone" and envelope["namespaceId"] != self._endpoint.scope)):
            raise TransportError("Import result does not match the executed operation")

    def _settle(self, procedure, operation_id, failure, *, cancel=False):
        endpoint = self._endpoint
        try:
            try:
                status = endpoint._operation_status(operation_id, cancel)
            except (DepositError, TransportError):
                if not cancel:
                    raise
                status = endpoint._operation_status(operation_id, False)
            if self._matches(status, operation_id) and status.get("state") in {"RUNNING", "REGISTERED"} and not cancel:
                cancel = True
                status = endpoint._operation_status(operation_id, True)
            for _ in range(3):
                if not self._matches(status, operation_id) or status.get("state") != "CANCEL_REQUESTED":
                    break
                sleep(0.025)
                status = endpoint._operation_status(operation_id, False)
        except Exception:
            raise OutcomeUnknown(operation_id, endpoint.scope) from failure
        return self._settled_result(procedure, operation_id, status, failure, cancel=cancel)

    def _settled_result(self, procedure, operation_id, status, failure=None, *, cancel=False):
        endpoint = self._endpoint
        if not self._matches(status, operation_id):
            raise OutcomeUnknown(operation_id, endpoint.scope) from failure
        state = status.get("state")
        if state == "CANCELLED_ROLLED_BACK" and status.get("procedure") in {None, procedure}:
            raise DepositCancelled(operation_id, endpoint.scope) from failure
        if status.get("procedure") != procedure:
            raise OutcomeUnknown(operation_id, endpoint.scope) from failure
        if state == "FAILED_ROLLED_BACK":
            raise DepositError(state, "The operation failed and rolled back", status) from failure
        if state not in {"COMMITTED", "COMPLETED_NO_DOMAIN_WRITE"}:
            raise OutcomeUnknown(operation_id, endpoint.scope) from failure
        if cancel:
            raise OperationCommittedAfterCancel(operation_id, endpoint.scope, status) from failure
        summary = status.get("result")
        code = status.get("code")
        if code is None and isinstance(summary, dict):
            if procedure.startswith("deposit.exchange.") or procedure in _TRANSFER_RESULTS:
                return summary
            if procedure in _IMPORT_RESULTS and isinstance(summary.get("resultId"), str):
                try:
                    envelope = endpoint.call("deposit.get.importResult", summary["resultId"])
                    self._check_envelope(procedure, operation_id, envelope)
                    if envelope["resultId"] != summary["resultId"]:
                        raise TransportError("Result lease has another identity")
                    return envelope
                except (DepositError, TransportError) as exc:
                    code = exc.code if isinstance(exc, DepositError) and exc.code == "RESULT_EXPIRED" else "RESULT_UNAVAILABLE"
                    failure = exc
        raise DepositError(code or "RESULT_UNAVAILABLE", "The operation finished. Inspect its Server receipt.", status) from failure
