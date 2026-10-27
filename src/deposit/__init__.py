"""Synchronous client foundation for Deposit Server 2."""

from ._meta import date, title
from .client import Client
from .errors import CompatibilityError, DepositError, TransportError
from .store import Store
from .recovery import PendingOperation, RecoveredOperation
from .entities import DClass, DObject
from .values import DType, DDict, DGeometry, DResource, TypedList, TypedValue, typed

__version__ = "2.0.0.dev0"
__all__ = ["Client", "Store", "DClass", "DObject", "PendingOperation", "RecoveredOperation", "DepositError", "CompatibilityError", "TransportError", "DType", "DDict", "DGeometry", "DResource", "TypedValue", "TypedList", "typed", "title", "date", "__version__"]
