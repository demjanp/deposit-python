"""Synchronous client foundation for Deposit Server 2."""

from ._meta import date, title
from .client import Client
from .errors import (CompatibilityError, DepositCancelled, DepositError,
                     OperationCommittedAfterCancel, OutcomeUnknown, TransportError)
from .store import Store
from .entities import DClass, DObject
from .values import DType, DDict, DGeometry, DResource, TypedList, TypedValue, typed
from ._paging import CloneResult, ImportResult, LazyResultMapping, LazyResultSequence, LazyText

from importlib.metadata import version as _distribution_version

__version__ = _distribution_version("deposit-python")
__all__ = ["Client", "Store", "DClass", "DObject", "ImportResult", "CloneResult", "LazyResultMapping", "LazyResultSequence", "LazyText", "DepositError", "DepositCancelled", "OperationCommittedAfterCancel", "OutcomeUnknown", "CompatibilityError", "TransportError", "DType", "DDict", "DGeometry", "DResource", "TypedValue", "TypedList", "typed", "title", "date", "__version__"]
