"""Small, standard-library-only remote batch execution."""

from .client import Client, Pool, RemoteError
from .tasks import command, ngspice, verilator

__all__ = ["Client", "Pool", "RemoteError", "command", "ngspice", "verilator"]
__version__ = "0.7.3"
