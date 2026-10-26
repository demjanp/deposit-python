"""Synchronous client foundation for Deposit Server 2."""

from ._meta import date, title
from .client import Client
from .errors import CompatibilityError, DepositError, TransportError

__version__ = "2.0.0.dev0"
__all__ = ["Client", "DepositError", "CompatibilityError", "TransportError", "title", "date", "__version__"]
