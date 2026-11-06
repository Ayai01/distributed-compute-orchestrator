"""Distributed Compute Orchestrator: standard-library-only compute coordination."""

PROJECT_NAME = "Distributed Compute Orchestrator"
ABBREVIATION = "DCO"
__version__ = "0.11.9"

from .client import Client, Pool, RemoteError, RequestCancelled
from .tasks import command, ngspice, verilator, python_map, python_call, python_task
from .execution import map_items, run_call, iter_items, StreamResultsPending
from .deadline import remaining_time, check_deadline
from .pipeline import Deferred,Skipped,ExecutionFailure,ExecutionPaused

__all__ = ["Client", "Pool", "RemoteError", "RequestCancelled", "command", "ngspice", "verilator", "python_map", "python_call", "python_task", "map_items", "run_call", "iter_items", "StreamResultsPending", "remaining_time", "check_deadline", "Deferred", "Skipped", "ExecutionFailure", "ExecutionPaused"]
