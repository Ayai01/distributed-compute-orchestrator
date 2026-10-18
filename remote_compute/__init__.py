"""Distributed Compute Orchestrator: standard-library-only compute coordination."""

PROJECT_NAME = "Distributed Compute Orchestrator"
ABBREVIATION = "DCO"
__version__ = "0.8.6"

from .client import Client, Pool, RemoteError
from .tasks import command, ngspice, verilator

__all__ = ["Client", "Pool", "RemoteError", "command", "ngspice", "verilator"]
