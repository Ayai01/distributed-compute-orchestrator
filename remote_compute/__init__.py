"""Distributed Compute Orchestrator: standard-library-only compute coordination."""

PROJECT_NAME = "Distributed Compute Orchestrator"
ABBREVIATION = "DCO"
__version__ = "1.0.5"

__all__ = ["Client", "Pool", "RemoteError", "RequestCancelled", "command", "ngspice", "verilator", "python_map", "python_call", "python_task", "map_items", "run_call", "iter_items", "StreamResultsPending", "remaining_time", "check_deadline", "report_progress", "Deferred", "Skipped", "ExecutionFailure", "ExecutionPaused"]

_exports={name:module for module,names in (
    ('client',('Client','Pool','RemoteError','RequestCancelled')),
    ('tasks',('command','ngspice','verilator','python_map','python_call','python_task')),
    ('execution',('map_items','run_call','iter_items','StreamResultsPending')),
    ('deadline',('remaining_time','check_deadline')),
    ('progress',('report_progress',)),
    ('pipeline',('Deferred','Skipped','ExecutionFailure','ExecutionPaused')),
)for name in names}


def __getattr__(name):
    # An isolated numerical item needs no HTTP server, scheduler, or pipeline.
    # Keep public imports identical while loading only the requested layer.
    module=_exports.get(name)
    if module is None:raise AttributeError("module 'remote_compute' has no attribute "+repr(name))
    from importlib import import_module
    value=getattr(import_module('.'+module,__name__),name)
    globals()[name]=value
    return value


def __dir__():
    return sorted(set(globals())|set(__all__))
