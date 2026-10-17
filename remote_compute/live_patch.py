"""Apply supported resource and transport fixes without replacing active jobs.

Called through CPython 3.14 remote_exec during an operator update. Existing
jobs, classes, locks, connections and process handles keep their identities.
"""
import ast
from contextlib import ExitStack
import gc
import importlib
import os
from pathlib import Path
import sys


def apply(*, source_root=None, expected_root=None, expected_pid=None):
    if expected_pid is not None and os.getpid()!=expected_pid:
        raise RuntimeError('The service PID changed before the update')
    source=Path(source_root or Path(__file__).resolve().parents[1]).resolve()
    worker=importlib.import_module('remote_compute.worker')
    current=Path(worker.__file__).resolve().parents[1]
    if current!=Path(expected_root or source).resolve():
        raise RuntimeError('The service belongs to another checkout')
    package=importlib.import_module('remote_compute')
    legacy=package.__version__ in ('0.7.11','0.7.12','0.7.13')
    if not legacy and package.__version__ not in ('0.7.28','0.7.29','0.7.30'):
        raise RuntimeError('Use an idle restart for this service version')
    coordinator=importlib.import_module('remote_compute.coordinator')
    states=[obj for obj in gc.get_objects() if type(obj) in (worker.Worker,coordinator.Coordinator)]
    centers=[state for state in states if type(state) is coordinator.Coordinator]
    workers=[state for state in states if type(state) is worker.Worker]
    if len(centers)>1 or len(workers)>1 or not states:
        raise RuntimeError('Expected one service and its optional private worker')
    plan={
        'allocation':{'':('plan_resize','weighted_node_threads')},
        'client':{'Client':('__init__','health','resize','admission'),'Pool':('__init__','_validate_node','configure','add','snapshot','_matching','_acquire','_speed_priors')},
        'worker':{'Worker':('_probe_tools','control','health','resize','_execute'),'Handler':('_connected','_handle'),'':('validate_task',)},
        'coordinator':{'Coordinator':('admission','_health','project_resources','resize','_callback','_update','_execute','_ensure_assets','_schedule','_recover_running','artifacts','delete')},
        'local':{'LocalClient':('request',)},
        'admission':{'AdmissionWindow':('current',)},
        'console':{'FocusRefresh':('due',)},
        'tasks':{'':('command',)},
        'commands':{'':('table','prepare','submission_parser'),'SchedulerShell':('do_save',)},
        'view':{'':('_device_address',),'StatusRows':('_build','_resources','_resource_rows','_tasks')},
    }
    if not legacy:
        plan={
            'allocation':{'':('plan_resize','weighted_node_threads')},
            'client':{'Client':('request','_request','health','status','fetch')},
            'worker':{'Handler':('handle_one_request','_connected','_reply','_handle')},
            'coordinator':{'Coordinator':('admission','project_resources','resize','artifacts','delete','_execute','_schedule','_recover_running')},
            'view':{'':('_device_address',),'StatusRows':('_build',)},
        }
    prepared=[]
    for name,classes in plan.items():
        module=importlib.import_module('remote_compute.'+name)
        if name=='client':
            module.__dict__.setdefault('os',os)
            module.__dict__.setdefault('ipaddress',importlib.import_module('ipaddress'))
        if name=='worker':
            module.__dict__.setdefault('select',importlib.import_module('select'))
        path=source/'remote_compute'/(name+'.py')
        tree=ast.parse(path.read_text(encoding='utf-8'),filename=str(path))
        if name=='client' and not legacy:
            helper=next(node for node in tree.body if isinstance(node,ast.ClassDef) and node.name=='_ConnectionPool')
            exec(compile(ast.Module(body=[helper],type_ignores=[]),str(path),'exec'),module.__dict__)
            module.__dict__.setdefault('select',importlib.import_module('select'))
        for class_name,methods in classes.items():
            owner=getattr(module,class_name) if class_name else module
            body=next(node.body for node in tree.body if isinstance(node,ast.ClassDef) and node.name==class_name) if class_name else tree.body
            for method in methods:
                node=next(item for item in body if isinstance(item,ast.FunctionDef) and item.name==method)
                node.decorator_list=[]
                fragment=ast.Module(body=[node],type_ignores=[])
                scope={}
                exec(compile(fragment,str(path),'exec'),module.__dict__,scope)
                old,new=(getattr(owner,method) if method in owner.__dict__ else None),scope[method]
                if (old is not None and old.__code__.co_freevars) or new.__code__.co_freevars:
                    raise RuntimeError('Resource patch methods must have no closures')
                prepared.append((owner,method,old,new))
    resize_plan=next(new for owner,method,old,new in prepared if method=='plan_resize')
    worker.plan_resize=coordinator.plan_resize=resize_plan
    coordinator.weighted_node_threads=next(new for owner,method,old,new in prepared if method=='weighted_node_threads')
    metadata=ast.parse((source/'remote_compute/__init__.py').read_text(encoding='utf-8'))
    constants={target.id:ast.literal_eval(node.value) for node in metadata.body if isinstance(node,ast.Assign)
               for target in node.targets if isinstance(target,ast.Name) and target.id in ('__version__','PROJECT_NAME','ABBREVIATION')}
    with ExitStack() as locks:
        for state in centers:
            locks.enter_context(state.condition)
            locks.enter_context(state.pool.condition)
        for state in workers:
            locks.enter_context(state.condition)
        def snapshot(state):
            return {key:(job['result'].get('cpus'),job['result'].get('memory_mb')) for key,job in state.jobs.items()}
        before={id(state):snapshot(state) for state in states}
        for owner,method,old,new in prepared:
            if old is None:
                setattr(owner,method,new)
            else:
                old.__code__,old.__defaults__,old.__kwdefaults__=new.__code__,new.__defaults__,new.__kwdefaults__
        for name,value in constants.items():setattr(package,name,value)
        worker.__version__=coordinator.__version__=constants['__version__']
        worker.Handler.idle_timeout=10
        if 'remote_compute.view' in sys.modules:
            sys.modules['remote_compute.view'].ABBREVIATION=constants['ABBREVIATION']
        if 'remote_compute.repository' in sys.modules:
            sys.modules['remote_compute.repository'].URL='https://github.com/Ayai01/distributed-compute-orchestrator.git'
        for state in states:state.condition.notify_all()
        for state in centers:state.pool.condition.notify_all()
        unchanged=all(snapshot(state)==before[id(state)] for state in states)
    # Refresh cached capabilities after the methods become visible on both
    # private and remote workers. No scheduler or execution thread is replaced.
    if legacy:
        for state in centers:state.pool.refresh()
    (centers or workers)[0].activity.notify('DCO resource and network update applied; jobs retained')
    return dict(pid=os.getpid(),version=constants['__version__'],methods=len(prepared),jobs_unchanged=unchanged)
