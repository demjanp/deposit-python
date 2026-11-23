"""Connection-bound transport for the canonical Exchange procedures."""

from contextlib import contextmanager
from uuid import uuid7

from neo4j import GraphDatabase
from neo4j.exceptions import Neo4jError

from ._gateway import _manifest, build_query, map_driver_error
from ._recovery.identity import normalize_contacted_endpoint
from .errors import CompatibilityError, DepositCancelled, DepositError, OperationCommittedAfterCancel, OutcomeUnknown, TransportError


_INFO = "CYPHER 25 CALL deposit.get.info() YIELD data RETURN data"
_ROUTES = frozenset({
    "deposit.action.begin", "deposit.action.cancel", "deposit.get.operation", "deposit.get.session",
    "deposit.import.analyze", "deposit.import.apply", "deposit.import.cloneNamespace",
    "deposit.get.importResult",
    *(f"deposit.exchange.{name}" for name in (
        "export", "beginUpload", "uploadChunk", "finishUpload", "abortUpload",
        "inspect", "memberPage", "graphPage", "readChunk", "discard",
    )),
})


def _call(session, procedure, arguments, token):
    if procedure not in _ROUTES:
        raise ValueError("Procedure is outside the Exchange transport")
    entry = next(item for item in _manifest()["procedures"] if item["name"] == procedure)
    query, parameters = build_query(entry, arguments, token)
    try:
        with session.begin_transaction() as transaction:
            result = transaction.run(query, parameters)
            data = result.single(strict=True)["data"]
            result.consume()
            transaction.commit()
    except Neo4jError as exc:
        raise map_driver_error(exc) from exc
    except Exception as exc:
        raise TransportError("Exchange request failed") from exc
    if not isinstance(data, dict):
        raise TransportError("Exchange request returned an invalid response")
    return data


class ExchangeEndpoint:
    """An authenticated endpoint with a private one-connection execution pool."""

    def __init__(self, client, token, scope):
        self.token = token
        self.scope = scope
        row, summary = client._single_with_summary(_INFO)
        info = row.get("data", {})
        incarnation = info.get("incarnation")
        capabilities = info.get("capabilities")
        if (not isinstance(incarnation, str) or not isinstance(capabilities, list)
                or "exchangeIntake" not in capabilities or "operationControl" not in capabilities):
            raise CompatibilityError(str(info.get("version")), "exchangeIntake and operationControl")
        self.endpoint = normalize_contacted_endpoint(summary.server.address)
        self.database = summary.database
        self.incarnation = incarnation
        # The driver configuration carries the original auth provider and TLS
        # trust policy. Only the connection pool is replaced.
        pool = client._driver._pool
        self._config = dict(pool.pool_config)
        self._config.update(max_connection_pool_size=1, max_connection_lifetime=float("inf"))
        self._impersonated = pool.workspace_config.impersonated_user
        self._execution = self._new_driver()
        self._control = None
        try:
            with self._session(self._execution) as session:
                result = session.run(_INFO)
                current = result.single(strict=True)["data"]
                result.consume()
            if current.get("incarnation") != self.incarnation:
                raise DepositError("SCOPE_MISMATCH", "The Exchange endpoint changed")
        except BaseException:
            self.close()
            raise

    def _new_driver(self):
        return GraphDatabase.driver(f"bolt://{self.endpoint}", **self._config)

    def _session(self, driver):
        return driver.session(database=self.database, impersonated_user=self._impersonated)

    @contextmanager
    def execution(self):
        with self._session(self._execution) as session:
            yield ExchangeExecution(self, session)

    def call(self, procedure, *arguments):
        with self._session(self._execution) as session:
            return _call(session, procedure, arguments, self.token)

    def live_identity(self):
        with self._session(self._execution) as session:
            result = session.run(_INFO)
            incarnation = result.single(strict=True)["data"].get("incarnation")
            result.consume()
            profile = _call(session, "deposit.get.session", (), self.token)
        actor = profile.get("user", {}).get("id")
        if incarnation != self.incarnation or not isinstance(actor, str):
            raise DepositError("SCOPE_MISMATCH", "The authenticated Exchange endpoint changed")
        return self.endpoint, self.database, incarnation, actor

    def _operation_status(self, operation_id, cancel):
        if self._control is None:
            self._control = self._new_driver()
        procedure = "deposit.action.cancel" if cancel else "deposit.get.operation"
        with self._session(self._control) as session:
            return _call(session, procedure, (operation_id, self.scope), self.token)

    def close(self):
        try:
            self._execution.close()
        finally:
            if self._control is not None:
                self._control.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


class ExchangeExecution:
    def __init__(self, endpoint, session):
        self._endpoint = endpoint
        self._session = session

    def run(self, procedure, *arguments, operation_id=None):
        endpoint = self._endpoint
        operation_id = operation_id or str(uuid7())
        try:
            status = _call(self._session, "deposit.action.begin",
                           (operation_id, endpoint.scope, procedure, list(arguments)), endpoint.token)
            if status.get("operationId") != operation_id or status.get("scope") != endpoint.scope:
                raise TransportError("Exchange registration identity changed")
            if status.get("state") == "CANCELLED_ROLLED_BACK":
                raise DepositCancelled(operation_id, endpoint.scope)
            if status.get("state") != "REGISTERED":
                raise OutcomeUnknown(operation_id, endpoint.scope)
            return _call(self._session, procedure, (*arguments, operation_id), endpoint.token)
        except (TransportError, KeyboardInterrupt) as failure:
            return self._settle(procedure, operation_id, failure)
        except DepositError as failure:
            if failure.code in {"CANCELLED", "OPERATION_NOT_CLAIMABLE", "OUTCOME_UNKNOWN"} and type(failure) is DepositError:
                return self._settle(procedure, operation_id, failure)
            raise

    def _settle(self, procedure, operation_id, failure):
        endpoint = self._endpoint
        cancel = isinstance(failure, KeyboardInterrupt)
        try:
            status = endpoint._operation_status(operation_id, cancel)
        except Exception:
            raise OutcomeUnknown(operation_id, endpoint.scope) from failure
        if status.get("operationId") != operation_id or status.get("scope") != endpoint.scope:
            raise OutcomeUnknown(operation_id, endpoint.scope) from failure
        state = status.get("state")
        if state == "CANCELLED_ROLLED_BACK":
            raise DepositCancelled(operation_id, endpoint.scope) from failure
        if status.get("procedure") != procedure:
            raise OutcomeUnknown(operation_id, endpoint.scope) from failure
        if state == "FAILED_ROLLED_BACK":
            raise failure
        if state in {"COMMITTED", "COMPLETED_NO_DOMAIN_WRITE"}:
            if cancel:
                raise OperationCommittedAfterCancel(operation_id, endpoint.scope, status) from failure
            if isinstance(status.get("result"), dict) and status.get("code") is None and procedure.startswith("deposit.exchange."):
                return status["result"]
            if procedure in {"deposit.import.analyze", "deposit.import.apply", "deposit.import.cloneNamespace"}:
                summary = status.get("result")
                if status.get("code") is None and isinstance(summary, dict) and isinstance(summary.get("resultId"), str):
                    from ._paging import valid_import_result_envelope
                    try:
                        envelope = endpoint.call("deposit.get.importResult", summary["resultId"])
                    except (DepositError, TransportError) as exc:
                        code = exc.code if isinstance(exc, DepositError) and exc.code == "RESULT_EXPIRED" else "RESULT_UNAVAILABLE"
                        raise DepositError(code, "The operation finished but its result cannot be read", status) from exc
                    if (valid_import_result_envelope(envelope) and envelope["resultId"] == summary["resultId"]
                            and envelope["operationId"] == operation_id and envelope["namespaceId"] == endpoint.scope):
                        return envelope
                code = status.get("code") or "RESULT_UNAVAILABLE"
                raise DepositError(code, "The operation finished but its result cannot be read", status) from failure
            raise DepositError("COMMITTED_RESULT_UNAVAILABLE", "Inspect the committed Exchange operation", status) from failure
        raise OutcomeUnknown(operation_id, endpoint.scope) from failure
