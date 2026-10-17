"""Apply the 0.7.11 resource fix to its running standard-library service.

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
    if package.__version__ not in ('0.7.11','0.7.12','0.7.13'):
        raise RuntimeError('Use an idle restart for this service version')
    coordinator=importlib.import_module('remote_compute.coordinator')
    states=[obj for obj in gc.get_objects() if type(obj) in (worker.Worker,coordinator.Coordinator)]
    centers=[state for state in states if type(state) is coordinator.Coordinator]
    workers=[state for state in states if type(state) is worker.Worker]
    if len(centers)>1 or len(workers)>1 or not states:
        raise RuntimeError('Expected one service and its optional private worker')
    plan={
        'client':{'Client':('health','resize','admission'),'Pool':('_acquire','_speed_priors')},
        'worker':{'Worker':('health','resize'),'Handler':('_handle',),'':('validate_task',)},
        'coordinator':{'Coordinator':('admission','_health','project_resources','resize','_update','_execute','_schedule')},
        'local':{'LocalClient':('request',)},
        'tasks':{'':('command',)},
        'commands':{'':('table','prepare','submission_parser')},
        'view':{'StatusRows':('_build','_resources','_resource_rows','_tasks')},
    }
    prepared=[]
    for name,classes in plan.items():
        module=importlib.import_module('remote_compute.'+name)
        path=source/'remote_compute'/(name+'.py')
        tree=ast.parse(path.read_text(encoding='utf-8'),filename=str(path))
        for class_name,methods in classes.items():
            owner=getattr(module,class_name) if class_name else module
            body=next(node.body for node in tree.body if isinstance(node,ast.ClassDef) and node.name==class_name) if class_name else tree.body
            for method in methods:
                node=next(item for item in body if isinstance(item,ast.FunctionDef) and item.name==method)
                node.decorator_list=[]
                fragment=ast.Module(body=[node],type_ignores=[])
                scope={}
                exec(compile(fragment,str(path),'exec'),module.__dict__,scope)
                old,new=getattr(owner,method,None),scope[method]
                if (old is not None and old.__code__.co_freevars) or new.__code__.co_freevars:
                    raise RuntimeError('Resource patch methods must have no closures')
                prepared.append((owner,method,old,new))
    metadata=ast.parse((source/'remote_compute/__init__.py').read_text(encoding='utf-8'))
    constants={target.id:ast.literal_eval(node.value) for node in metadata.body if isinstance(node,ast.Assign)
               for target in node.targets if isinstance(target,ast.Name) and target.id in ('__version__','PROJECT_NAME','ABBREVIATION')}
    with ExitStack() as locks:
        for state in centers:
            locks.enter_context(state.condition)
            locks.enter_context(state.pool.condition)
        for state in workers:
            locks.enter_context(state.condition)
        before={id(state):set(state.jobs) for state in states}
        for owner,method,old,new in prepared:
            if old is None:
                setattr(owner,method,new)
            else:
                old.__code__,old.__defaults__,old.__kwdefaults__=new.__code__,new.__defaults__,new.__kwdefaults__
        for name,value in constants.items():setattr(package,name,value)
        worker.__version__=coordinator.__version__=constants['__version__']
        sys.modules['remote_compute.view'].ABBREVIATION=constants['ABBREVIATION']
        if 'remote_compute.repository' in sys.modules:
            sys.modules['remote_compute.repository'].URL='https://github.com/Ayai01/distributed-compute-orchestrator.git'
        for state in states:state.condition.notify_all()
        for state in centers:state.pool.condition.notify_all()
        unchanged=all(set(state.jobs)==before[id(state)] for state in states)
    # Refresh cached capabilities after the methods become visible on both
    # private and remote workers. No scheduler or execution thread is replaced.
    for state in centers:state.pool.refresh()
    (centers or workers)[0].activity.notify('DCO resource admission and RAM display updated; jobs retained')
    return dict(pid=os.getpid(),version=constants['__version__'],methods=len(prepared),jobs_unchanged=unchanged)
