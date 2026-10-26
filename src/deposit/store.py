"""Namespace-bound Store and the Stage 2 prepared-action coordinator."""

from __future__ import annotations

from collections.abc import Mapping
from contextlib import contextmanager
import json
import math
from pathlib import Path
import threading
from typing import Any, Iterator

from ._recovery.identity import DeploymentIdentity
from ._recovery.journal import JournalError, ProtectedJournal, UNRESOLVED_STATES
from .errors import DepositError, TransportError


_ENTITY_QUERY = {
    "deposit.create.class": ("name", "properties"),
    "deposit.create.object": ("class", "properties"),
    "deposit.get.class": ("selection",),
    "deposit.get.object": ("selection",),
}
_STORE_FACTORY_TOKEN = object()


class Store:
    """An independently authenticated session bound to one immutable Namespace UUID."""

    def __init__(
        self,
        client,
        *,
        token: str,
        actor_id: str,
        namespace_id: str,
        identity: DeploymentIdentity,
        recovery_directory: str | None,
        observed: Any,
        _factory_token: object | None = None,
        _journal: ProtectedJournal | None = None,
    ):
        if _factory_token is not _STORE_FACTORY_TOKEN:
            raise TypeError("Store instances are created by Client.open_store()")
        self._client = client
        self._token = token
        self._actor_id = actor_id
        self._namespace_id = namespace_id
        self._identity = identity
        self._journal = _journal or ProtectedJournal(recovery_directory)
        self._operation_lock = threading.Lock()
        self._close_lock = threading.Lock()
        self._state_lock = threading.Lock()
        self._closed = False
        self._closing = False
        self._active = False
        self._observed_revision = self._get_revision(observed)
        if self._observed_revision is None:
            self.refresh()
        self._journal.cleanup_expired(self._identity)
        with self._operation_lock:
            self._recover_pending_locked()

    @property
    def namespace_id(self) -> str:
        return self._namespace_id

    @property
    def closed(self) -> bool:
        with self._state_lock:
            return self._closed

    def create_class(self, name: str, properties: dict[str, Any] | None = None) -> dict[str, Any]:
        if not isinstance(name, str) or not name:
            raise ValueError("Class name must be a nonempty string")
        props = _portable_input(properties or {})
        return self._run_action("deposit.create.class", [name, props])

    def create_object(self, class_selector: str | dict[str, Any], properties: dict[str, Any] | None = None) -> dict[str, Any]:
        selector = _selector(class_selector, "Class")
        props = _portable_input(properties or {})
        return self._run_action("deposit.create.object", [selector, props])

    def get_class(self, selection: str | dict[str, Any]) -> dict[str, Any]:
        return self._read_entity("deposit.get.class", _selector(selection, "Class"))

    def get_object(self, selection: str | dict[str, Any]) -> dict[str, Any]:
        return self._read_entity("deposit.get.object", _selector(selection, "Object"))

    def refresh(self) -> None:
        """Explicitly refresh this session's Namespace revision baseline."""
        with self._serialized(allow_closing=True):
            self._client._single(
                "CYPHER 25 CALL deposit.utils.refresh($scope, $token) YIELD data RETURN data",
                {"scope": "namespace", "token": self._token},
            )
            self._update_observed_revision()

    def cancel(self) -> bool:
        """Request cancellation through a separate control transaction."""
        with self._state_lock:
            if self._closed:
                return False
        try:
            row = self._client._single(
                "CYPHER 25 CALL deposit.utils.cancel($token) YIELD value RETURN value",
                {"token": self._token},
            )
            return bool(row.get("value"))
        except Exception:
            return False

    def close(self) -> None:
        with self._close_lock:
            with self._state_lock:
                if self._closed:
                    return
                self._closing = True
                active = self._active
            if active:
                self.cancel()
            failure: Exception | None = None
            with self._operation_lock:
                try:
                    self._recover_pending_locked()
                except Exception as exc:
                    failure = exc
                try:
                    self._client.logout(self._token)
                except Exception as exc:
                    if failure is None and not _session_already_unavailable(exc):
                        failure = exc
                with self._state_lock:
                    self._closed = True
                    self._closing = False
                self._client._store_closed(self)
            if failure is not None:
                raise failure

    def __enter__(self) -> "Store":
        with self._state_lock:
            if self._closed or self._closing:
                raise RuntimeError("Store is closed")
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.close()

    @contextmanager
    def _serialized(self, *, allow_closing: bool = False) -> Iterator[None]:
        self._operation_lock.acquire()
        try:
            with self._state_lock:
                if self._closed or (self._closing and not allow_closing):
                    raise RuntimeError("Store is closed")
                self._active = True
            yield
        finally:
            with self._state_lock:
                self._active = False
            self._operation_lock.release()

    def _read_entity(self, procedure: str, selection: dict[str, Any] | str) -> dict[str, Any]:
        with self._serialized():
            query = (
                f"CYPHER 25 CALL {procedure}($selection, $token) YIELD node "
                "CALL deposit.utils.toData(node, $token) YIELD data RETURN data"
            )
            row = self._client._single(query, {"selection": selection, "token": self._token})
            data = row.get("data")
            if not isinstance(data, dict):
                raise DepositError("INVALID_RESPONSE", f"{procedure} returned no portable entity")
            if data.get("namespaceId") != self._namespace_id:
                raise DepositError("SCOPE_MISMATCH", "Entity belongs to another Namespace")
            return data

    def _run_action(self, procedure: str, arguments: list[Any]) -> dict[str, Any]:
        if procedure not in _ENTITY_QUERY:
            raise ValueError("Unsupported Stage 2 action")
        _portable_input(arguments)
        with self._serialized():
            self._update_observed_revision()
            revision = self._observed_revision
            if revision is None:
                raise DepositError("REVISION_UNAVAILABLE", "Store has no observed Namespace revision")
            record = self._journal.create(self._identity, procedure, arguments, revision)
            return self._resolve_action_locked(record, allow_new_prepare=True)

    def _recover_pending_locked(self) -> None:
        for record in self._journal.unresolved(self._identity):
            try:
                self._resolve_action_locked(record, allow_new_prepare=True)
            except DepositError as exc:
                if exc.code == "REVISION_CONFLICT":
                    continue
                raise DepositError("RECOVERY_PENDING", "An unresolved action could not be safely recovered") from exc
            except (TransportError, JournalError):
                raise

    def _resolve_action_locked(self, record: dict[str, Any], *, allow_new_prepare: bool) -> dict[str, Any]:
        with self._journal.action_lock(record["key"]):
            return self._resolve_action_with_lock(record, allow_new_prepare=allow_new_prepare)

    def _resolve_action_with_lock(self, record: dict[str, Any], *, allow_new_prepare: bool) -> dict[str, Any]:
        key = record["key"]
        state = record["state"]
        if state == "conflicted":
            raise DepositError("REVISION_CONFLICT", "Pending action requires explicit refresh and reapplication")

        if state in {"intent", "prepare_sent"}:
            self._journal.update(self._identity, key, state="prepare_sent")
            try:
                status = self._control("deposit.action.status", key)
            except DepositError as exc:
                if exc.code != "NOT_FOUND":
                    raise
                self._assert_preconditions_unchanged(record, allow_new_prepare)
                prepared = self._control("deposit.action.prepare", key, record["procedure"], record["arguments"])
            else:
                prepared = status
            if prepared.get("state") == "committed":
                return self._save_and_release(record, prepared)
            self._journal.update(self._identity, key, state="prepared", operation_id=prepared.get("operationId"))
            state = "prepared"

        if state in {"prepared", "executing"}:
            status = self._control("deposit.action.status", key)
            if status.get("state") == "committed":
                return self._save_and_release(record, status)
            resumed = self._control("deposit.action.resume", key)
            if resumed.get("state") == "committed":
                return self._save_and_release(record, resumed)
            self._journal.update(self._identity, key, state="executing", operation_id=resumed.get("operationId"))
            try:
                result = self._execute_entity_action(record)
            except DepositError as exc:
                status = self._control("deposit.action.status", key)
                if status.get("state") == "committed":
                    return self._save_and_release(record, status)
                if exc.code == "REVISION_CONFLICT":
                    self._release_conflict(record)
                    raise
                raise
            return self._save_and_release(record, {"state": "committed", "operationId": resumed.get("operationId"), "outcome": result})

        if state in {"outcome_saved", "release_sent"}:
            status = self._control("deposit.action.status", key)
            if status.get("state") not in {"prepared", "committed"}:
                raise DepositError("RECOVERY_PENDING", "Prepared action is unavailable; the saved outcome remains protected")
            self._control("deposit.action.resume", key)
            self._journal.update(self._identity, key, state="release_sent")
            self._control("deposit.action.release")
            saved = self._journal.update(self._identity, key, state="released")
            self._update_observed_revision()
            return saved["outcome"]

        if state == "released":
            return record["outcome"]
        raise JournalError("Recovery journal state cannot be resumed")

    def _assert_preconditions_unchanged(self, record: dict[str, Any], allowed: bool) -> None:
        if not allowed:
            raise DepositError("RECOVERY_PENDING", "An absent prepared action cannot be recreated")
        self._update_observed_revision()
        if self._observed_revision != record["snapshot_revision"]:
            current = self._journal.load(self._identity, record["key"])
            if current["state"] in {"intent", "prepare_sent"}:
                self._journal.update(self._identity, record["key"], state="conflicted")
            raise DepositError("REVISION_CONFLICT", "Namespace revision changed before the action was prepared")

    def _execute_entity_action(self, record: dict[str, Any]) -> dict[str, Any]:
        arguments = record["arguments"]
        parameters = {f"a{i}": value for i, value in enumerate(arguments)}
        parameters["token"] = self._token
        placeholders = ", ".join(f"${name}" for name in parameters if name != "token")
        query = (
            f"CYPHER 25 CALL {record['procedure']}({placeholders}, $token) YIELD node "
            "CALL deposit.utils.toData(node, $token) YIELD data RETURN data"
        )
        row = self._client._single(query, parameters)
        outcome = row.get("data")
        if not isinstance(outcome, dict) or outcome.get("namespaceId") != self._namespace_id:
            raise DepositError("INVALID_RESPONSE", "Mutation returned no portable entity in the bound Namespace")
        return outcome

    def _save_and_release(self, record: dict[str, Any], status: dict[str, Any]) -> dict[str, Any]:
        outcome = status.get("outcome")
        if not isinstance(outcome, dict):
            raise DepositError("INVALID_RESPONSE", "Prepared action has no saved portable outcome")
        if outcome.get("namespaceId") != self._namespace_id:
            raise DepositError("SCOPE_MISMATCH", "Recovered outcome belongs to another Namespace")
        key = record["key"]
        current = self._journal.load(self._identity, key)
        if current["state"] not in {"outcome_saved", "release_sent", "released"}:
            current = self._journal.update(
                self._identity,
                key,
                state="outcome_saved",
                outcome=_portable_input(outcome),
                operation_id=status.get("operationId"),
            )
        if current["state"] == "released":
            return current["outcome"]
        # A resumed Store has a new token/session, so even a committed receipt
        # must be attached to that session before action.release can confirm it.
        self._control("deposit.action.resume", key)
        self._journal.update(self._identity, key, state="release_sent")
        self._control("deposit.action.release")
        saved = self._journal.update(self._identity, key, state="released")
        self._update_observed_revision()
        return saved["outcome"]

    def _release_conflict(self, record: dict[str, Any]) -> None:
        self._control("deposit.action.release")
        current = self._journal.load(self._identity, record["key"])
        if current["state"] in {"prepared", "executing", "prepare_sent"}:
            self._journal.update(self._identity, record["key"], state="conflicted")

    def _control(self, procedure: str, *arguments: Any) -> dict[str, Any]:
        placeholders = ", ".join(f"$a{i}" for i in range(len(arguments)))
        if placeholders:
            placeholders += ", "
        params = {f"a{i}": value for i, value in enumerate(arguments)}
        params["token"] = self._token
        column = "value" if procedure == "deposit.action.release" else "data"
        query = f"CYPHER 25 CALL {procedure}({placeholders}$token) YIELD {column} RETURN {column}"
        row = self._client._single(query, params)
        value = row.get(column)
        if procedure == "deposit.action.release":
            if value is not True:
                raise DepositError("RELEASE_UNCONFIRMED", "Prepared action release was not confirmed")
            return {"released": True}
        if not isinstance(value, dict):
            raise DepositError("INVALID_RESPONSE", f"{procedure} returned no recovery state")
        return value

    def _update_observed_revision(self) -> None:
        session = self._client.get_session(self._token)
        user = session.get("user")
        if not isinstance(user, dict) or user.get("id") != self._actor_id:
            raise DepositError("IDENTITY_MISMATCH", "Store session actor changed")
        namespace_id = session.get("namespaceId")
        if namespace_id != self._namespace_id:
            raise DepositError("SCOPE_MISMATCH", "Store session Namespace changed")
        revision = self._get_revision(session.get("observed"))
        if revision is None:
            raise DepositError("REVISION_UNAVAILABLE", "Server returned no Namespace revision baseline")
        self._observed_revision = revision

    def _get_revision(self, observed: Any) -> int | None:
        if not isinstance(observed, dict):
            return None
        revision = observed.get(self._namespace_id)
        return revision if isinstance(revision, int) and not isinstance(revision, bool) and revision >= 0 else None


def _selector(value: str | dict[str, Any], kind: str) -> str | dict[str, Any]:
    if isinstance(value, str) and value:
        return value
    if isinstance(value, dict):
        if set(value) == {"id"} and isinstance(value["id"], str) and value["id"]:
            return {"id": value["id"]}
        if value.get("kind") == kind and isinstance(value.get("id"), str):
            return {"id": value["id"]}
    raise TypeError(f"{kind} selector must be a name or a stable-ID record")


def _portable_input(value: Any) -> Any:
    _validate_portable_value(value)
    try:
        encoded = json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
        return json.loads(encoded)
    except (TypeError, ValueError, OverflowError) as exc:
        raise TypeError("Stage 2 recovery accepts only losslessly JSON-portable values") from exc


def _validate_portable_value(value: Any) -> None:
    """Reject values JSON would coerce or silently alter before journaling them."""
    if value is None or type(value) in {bool, int, str}:
        return
    if type(value) is float:
        if not math.isfinite(value):
            raise TypeError("Stage 2 recovery accepts only losslessly JSON-portable values")
        return
    if isinstance(value, Mapping):
        if type(value) is not dict:
            raise TypeError("Stage 2 recovery accepts only built-in JSON mappings")
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError("Stage 2 recovery requires string mapping keys")
            _validate_portable_value(item)
        return
    if type(value) is list:
        for item in value:
            _validate_portable_value(item)
        return
    raise TypeError("Stage 2 recovery accepts only losslessly JSON-portable values")


def _session_already_unavailable(exc: Exception) -> bool:
    return isinstance(exc, DepositError) and exc.code in {"AUTH_FAILED", "AUTH_REQUIRED", "SESSION_EXPIRED", "NOT_FOUND"}
