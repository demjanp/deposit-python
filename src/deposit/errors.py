"""Stable, safe error types exposed by the Deposit client."""

from __future__ import annotations

from typing import Any


class DepositError(Exception):
    """A server-reported Deposit failure with a sanitized public envelope."""

    def __init__(self, code: str, message: str, details: Any = None, request_id: str | None = None):
        self.code = code
        self.message = message
        self.details = details
        self.request_id = request_id
        # Only the validated driver error mapper sets these fields. Local
        # validation errors carry no claim about a server transaction outcome.
        self.retryable: bool | None = None
        self._server_reported = False
        super().__init__(f"{code}: {message}")


class CompatibilityError(DepositError):
    """The connected server is outside the package's tested support matrix."""

    def __init__(self, version: str, supported: str, component: str = "Deposit Server"):
        self.version = version
        self.supported = supported
        self.component = component
        super().__init__("UNSUPPORTED_VERSION", f"{component} {version!r} is unsupported; this package supports {supported}.")


class TransportError(Exception):
    """A Neo4j driver or transport failure, kept distinct from server errors."""


class DepositCancelled(DepositError):
    """The Server established cancelled rollback."""

    def __init__(self, operation_id: str, scope: str):
        self.operation_id = operation_id
        self.scope = scope
        super().__init__("CANCELLED_ROLLED_BACK", "The operation was cancelled and rolled back")


class OperationCommittedAfterCancel(DepositError):
    """The Server recorded completion before cancellation took effect."""

    def __init__(self, operation_id: str, scope: str, status: dict):
        self.operation_id = operation_id
        self.scope = scope
        super().__init__("OPERATION_COMMITTED", "The operation completed before cancellation", status)


class OutcomeUnknown(DepositError):
    """The original attempt requires further inspection."""

    def __init__(self, operation_id: str, scope: str):
        self.operation_id = operation_id
        self.scope = scope
        super().__init__("OUTCOME_UNKNOWN", "Inspect the original operation before starting a new attempt")
