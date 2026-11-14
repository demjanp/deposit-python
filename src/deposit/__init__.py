"""Synchronous client foundation for Deposit Server 2."""

from ._meta import date, title
from .client import Client
from .errors import CompatibilityError, DepositError, TransportError
from .store import Store
from .recovery import PendingOperation, RecoveredOperation
from .entities import DClass, DObject
from .values import DType, DDict, DGeometry, DResource, TypedList, TypedValue, typed
from ._paging import CloneResult, ImportResult, LazyResultMapping, LazyResultSequence, LazyText
from ._import_workflows import ImportWorkflow, ImportWorkflowStatus
from ._clone_workflows import CloneWorkflow, CloneWorkflowStatus

from importlib.metadata import version as _distribution_version

__version__ = _distribution_version("deposit")
__all__ = ["Client", "Store", "DClass", "DObject", "PendingOperation", "RecoveredOperation", "ImportResult", "CloneResult", "ImportWorkflow", "ImportWorkflowStatus", "CloneWorkflow", "CloneWorkflowStatus", "LazyResultMapping", "LazyResultSequence", "LazyText", "DepositError", "CompatibilityError", "TransportError", "DType", "DDict", "DGeometry", "DResource", "TypedValue", "TypedList", "typed", "title", "date", "__version__"]
