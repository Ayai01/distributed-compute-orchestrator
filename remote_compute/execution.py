"""DCO-owned execution of business functions and independent input items."""
import importlib
import json
import os
from pathlib import Path
import re
import pickle
import subprocess
import sys
import threading
import tempfile
from collections import deque
import time
from concurrent.futures import ThreadPoolExecutor,wait,FIRST_COMPLETED

if __package__ in (None, ''):
    sys.path.insert(0,str(Path(__file__).resolve().parent.parent))
    sys.path.insert(0,os.getcwd())


def validate_execution(value):
    if not isinstance(value,dict) or set(value)-{'function','items','kwargs','mode','backend'}:
        raise ValueError('execution requires function, items, kwargs and mode')
    if not isinstance(value.get('function'),str) or not re.fullmatch(r'[A-Za-z_][\w.]*:[A-Za-z_][\w.]*',value['function']):
        raise ValueError('function requires an importable module:function name')
    if value.get('mode') not in ('map','call') or not isinstance(value.get('items'),list) or not isinstance(value.get('kwargs'),dict):
        raise ValueError('execution requires mode map/call, list items and dictionary kwargs')
    if value.get('backend','process')not in ('process','thread'):raise ValueError('backend requires process or thread')
    json.dumps(value)


def _function(value):
    if not isinstance(value,str):return value
    module,name=value.split(':',1)
    result=importlib.import_module(module)
    for part in name.split('.'):result=getattr(result,part)
    return result


def _item(function,item,kwargs):
    return _function(function)(item,**kwargs)


def _single_threaded(item_process=True):
    if item_process:os.environ['DCO_ITEM_PROCESS']='1'
    os.environ['REMOTE_COMPUTE_CPUS']='1'
    for key in ('OMP_NUM_THREADS','OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','NUMEXPR_NUM_THREADS','NUMBA_NUM_THREADS'):
        os.environ[key]='1'


_local=threading.local()


def _thread_item(function,item,kwargs):
    _local.item=True
    numba=sys.modules.get('numba')
    if numba is not None:numba.set_num_threads(1)
    try:return _item(function,item,kwargs)
    finally:_local.item=False


def _process_item(function,item,kwargs):
    # One process lifetime per business item returns its allocator/model RAM
    # when that item completes, including a map's long tail.
    with tempfile.TemporaryDirectory(prefix='dco-item-')as folder:
        root=Path(folder);request=root/'request.pkl';result=root/'result.pkl'
        request.write_bytes(pickle.dumps((function,item,kwargs),protocol=4))
        prefix=json.loads(os.environ.get('REMOTE_COMPUTE_PYTHON_ARGV','[]'))or[sys.executable]
        child=subprocess.Popen([*prefix,'-B',str(Path(__file__).resolve()),'--item',str(request),str(result)],
            stdout=subprocess.PIPE,stderr=subprocess.STDOUT,creationflags=subprocess.CREATE_NO_WINDOW if os.name=='nt'else 0)
        tail=deque(maxlen=8)
        try:
            for block in iter(lambda:child.stdout.read(8192),b''):tail.append(block)
            code=child.wait()
        finally:child.stdout.close()
        if code or not result.is_file():
            raise RuntimeError(b''.join(tail)[-8192:].decode('utf8','replace')or'DCO item process failed')
        value=pickle.loads(result.read_bytes())
        if not value['ok']:raise RuntimeError(value['error'])
        return value['value']


class _Grants:
    """Only the executor exchanges resource messages; business code sees results."""
    def __init__(self,maximum):
        self.maximum=maximum
        self.cpus=min(maximum,max(1,int(os.environ.get('REMOTE_COMPUTE_CPUS','1'))))
        self.condition=threading.Condition();self.active=self.ready=0
        self.pending=False;self.fenced=False;self.pending_floor=0;self.error=None;self.stopping=False
        self.thread=None;self.lease=None
        url=os.environ.get('REMOTE_COMPUTE_URL','')
        if url and os.environ.get('REMOTE_COMPUTE_RESIZE')=='1':
            from remote_compute import Client
            client=Client(url,os.environ.get('REMOTE_COMPUTE_TOKEN',''),ca_file=os.environ.get('REMOTE_COMPUTE_CA')or None)
            model=json.loads(os.environ.get('REMOTE_COMPUTE_MEMORY_MODEL','{}'))
            self.lease=client.cpu_lease(os.environ['REMOTE_COMPUTE_JOB_ID'],max_cpus=maximum,cpus=self.cpus,**model)
            self.thread=threading.Thread(target=self._run,name='dco-executor-resources',daemon=True)
            self.thread.start()

    def _run(self):
        while True:
            with self.condition:
                if self.stopping:return
                if not self.active and not self.ready:
                    self.condition.wait(.1);continue
                active,ready=self.active,self.ready
                self.pending=True;self.pending_floor=active
            try:
                actual=self.lease.refresh(active_cpus=active,ready_cpus=ready)
            except Exception as error:
                with self.condition:
                    self.error=error;self.pending=False;self.condition.notify_all()
                return
            with self.condition:
                self.cpus=actual;self.pending=False
                self.fenced=self.active<active
                if self.fenced:self.pending_floor=self.active
                self.condition.notify_all()
                if self.stopping:return
                if not self.fenced:self.condition.wait(1)

    def limit(self):
        with self.condition:
            if self.error:return 0
            # While a shrinking request is in flight, new items must stay
            # within the active floor already reported in that request.
            return min(self.cpus,self.pending_floor)if self.pending or self.fenced else self.cpus

    def report(self,active,ready):
        with self.condition:
            changed=(active,ready)!=(self.active,self.ready)
            if self.lease is not None and active<self.active and not self.pending:
                self.fenced=True;self.pending_floor=active
            self.active,self.ready=active,ready
            if changed:self.condition.notify_all()

    def close(self):
        with self.condition:
            self.stopping=True;self.condition.notify_all()
        if self.thread:self.thread.join()
        if self.lease and not self.error:
            self.lease.refresh(active_cpus=0,ready_cpus=0)


def map_items(function,items,*,kwargs=None,backend='process',on_result=None):
    """Evaluate independent business inputs; DCO owns all resource management.

    Functions are importable module:function names or top-level callables.
    Each item receives one process and one CPU; results keep input order.
    """
    items=list(items);kwargs=dict(kwargs or {})
    if not items:return []
    if os.environ.get('REMOTE_COMPUTE_WORKER')!='1':
        raise RuntimeError('Submit python_map to DCO before executing its items')
    if backend not in ('process','thread'):raise ValueError('backend requires process or thread')
    if os.environ.get('DCO_ITEM_PROCESS')=='1' or getattr(_local,'item',False)or getattr(_local,'kernel',False):
        values=[]
        for index,item in enumerate(items):
            value=_item(function,item,kwargs);values.append(value)
            if on_result:on_result(index,value,None)
        return values
    maximum=min(len(items),max(1,int(os.environ.get('REMOTE_COMPUTE_MAX_CPUS',os.cpu_count()or 1))))
    grants=_Grants(maximum);position=0;active={};values=[None]*len(items);failures=[]
    if backend=='thread':
        _single_threaded(False);function=_function(function)
        try:
            from threadpoolctl import threadpool_limits
        except ImportError:pass
        else:threadpool_limits(limits=1)
        torch=sys.modules.get('torch')
        if torch is not None:torch.set_num_threads(1)
    pool=ThreadPoolExecutor(max_workers=maximum,thread_name_prefix='dco-item')
    try:
        while active or position<len(items):
            limit=grants.limit()
            while not failures and position<len(items)and len(active)<limit:
                operation=_process_item if backend=='process'else _thread_item
                future=pool.submit(operation,function,items[position],kwargs)
                active[future]=position;position+=1
            grants.report(len(active),0 if failures else len(items)-position)
            if not active:
                if failures or grants.error:break
                time.sleep(.02);continue
            done,_=wait(active,timeout=.1,return_when=FIRST_COMPLETED)
            for future in done:
                index=active.pop(future)
                try:
                    value=future.result();values[index]=value
                    if on_result:on_result(index,value,None)
                except BaseException as error:
                    failures.append(error)
                    if on_result:on_result(index,None,str(error))
            if grants.error:failures.append(grants.error)
            grants.report(len(active),0 if failures else len(items)-position)
        if failures:raise RuntimeError('DCO execution failed: '+str(failures[0]))from failures[0]
        if grants.error:raise grants.error
        return values
    finally:
        pool.shutdown(wait=True)
        grants.report(0,0);grants.close()


def run_call(function,**kwargs):
    """Execute a numerical kernel using the grant prepared by the DCO worker."""
    if os.environ.get('REMOTE_COMPUTE_WORKER')!='1':
        raise RuntimeError('Submit python_call to DCO before executing its kernel')
    if os.environ.get('DCO_ITEM_PROCESS')=='1'or getattr(_local,'item',False)or getattr(_local,'kernel',False):return _function(function)(**kwargs)
    lease=None;actual=max(1,int(os.environ.get('REMOTE_COMPUTE_CPUS','1')))
    maximum=max(1,int(os.environ.get('REMOTE_COMPUTE_MAX_CPUS',actual)))
    if os.environ.get('REMOTE_COMPUTE_URL')and os.environ.get('REMOTE_COMPUTE_RESIZE')=='1':
        from remote_compute import Client
        client=Client(os.environ['REMOTE_COMPUTE_URL'],os.environ.get('REMOTE_COMPUTE_TOKEN',''),ca_file=os.environ.get('REMOTE_COMPUTE_CA')or None)
        model=json.loads(os.environ.get('REMOTE_COMPUTE_MEMORY_MODEL','{}'))
        lease=client.cpu_lease(os.environ['REMOTE_COMPUTE_JOB_ID'],max_cpus=maximum,cpus=actual,**model)
        actual=lease.refresh(active_cpus=0,ready_cpus=maximum)
        lease.refresh(active_cpus=actual,ready_cpus=0)
    def threads(count):
        os.environ['REMOTE_COMPUTE_CPUS']=str(count)
        for key in ('OMP_NUM_THREADS','OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','NUMEXPR_NUM_THREADS','NUMBA_NUM_THREADS'):
            os.environ[key]=str(count)
        try:
            from threadpoolctl import threadpool_limits
        except ImportError:pass
        else:threadpool_limits(limits=count)
        torch=sys.modules.get('torch')
        if torch is not None:torch.set_num_threads(count)
        numba=sys.modules.get('numba')
        if numba is not None:numba.set_num_threads(min(count,numba.config.NUMBA_NUM_THREADS))
    try:
        _local.kernel=True
        threads(actual)
        return _function(function)(**kwargs)
    finally:
        _local.kernel=False
        threads(1)
        if lease:lease.refresh(active_cpus=0,ready_cpus=0)


def main(path):
    spec=json.loads(Path(path).read_text(encoding='utf8'));validate_execution(spec)
    output=Path('.dco-results.json');records={};error=None
    def publish(index,value,failure):
        from remote_compute.bundle import atomic_json
        records[index]=dict(index=index,status='failed'if failure else'succeeded',value=value,error=failure)
        atomic_json(output,dict(completed=len(records),total=1 if spec['mode']=='call'else len(spec['items']),items=[records[i]for i in sorted(records)]))
    try:
        if spec['mode']=='call':
            value=run_call(spec['function'],**spec['kwargs']);publish(0,value,None)
        else:map_items(spec['function'],spec['items'],kwargs=spec['kwargs'],backend=spec.get('backend','process'),on_result=publish)
    except BaseException as caught:
        error=caught
    finally:
        from remote_compute.bundle import atomic_json
        atomic_json(output,dict(completed=len(records),total=1 if spec['mode']=='call'else len(spec['items']),
            status='failed'if error else'succeeded',items=[records[i]for i in sorted(records)],error=str(error)if error else None))
    if error:raise error


def item_main(request,result):
    _single_threaded()
    try:
        function,item,kwargs=pickle.loads(Path(request).read_bytes())
        value=dict(ok=True,value=_item(function,item,kwargs))
    except BaseException:
        import traceback
        value=dict(ok=False,error=traceback.format_exc()[-8192:])
    Path(result).write_bytes(pickle.dumps(value,protocol=4))


if __name__=='__main__':
    # Business imports and the launcher must share the same runtime state.
    from remote_compute import execution
    if sys.argv[1]=='--item':execution.item_main(sys.argv[2],sys.argv[3])
    else:execution.main(sys.argv[1])
