"""Distributed Compute Orchestrator: standard-library-only compute coordination."""

PROJECT_NAME = "Distributed Compute Orchestrator"
ABBREVIATION = "DCO"
__version__ = "0.11.0"

from .client import Client, Pool, RemoteError, RequestCancelled
from .tasks import command, ngspice, verilator, python_map, python_call
from .execution import map_items, run_call
from .pipeline import Deferred,Skipped,ExecutionFailure,ExecutionPaused

__all__ = ["Client", "Pool", "RemoteError", "RequestCancelled", "command", "ngspice", "verilator", "python_map", "python_call", "map_items", "run_call", "Deferred", "Skipped", "ExecutionFailure", "ExecutionPaused"]
