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
from collections import deque,namedtuple
from queue import Queue,Empty
import time
from concurrent.futures import ThreadPoolExecutor

if __package__ in (None, ''):
    sys.path.insert(0,str(Path(__file__).resolve().parent.parent))
    sys.path.insert(0,os.getcwd())


def validate_execution(value):
    if not isinstance(value,dict) or set(value)-{'function','items','kwargs','mode','backend'}:
        raise ValueError('execution requires function, items, kwargs and mode')
    if not isinstance(value.get('function'),str) or not re.fullmatch(r'[A-Za-z_][\w.]*:[A-Za-z_][\w.]*',value['function']):
        raise ValueError('function requires an importable module:function name')
    if value.get('mode') not in ('map','call','entry') or not isinstance(value.get('items'),list) or not isinstance(value.get('kwargs'),dict):
        raise ValueError('execution requires mode map/call/entry, list items and dictionary kwargs')
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
_streams=set()
_streams_lock=threading.Lock()


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
                if not self.active and not self.ready and self.cpus==1 and not self.fenced:
                    self.condition.wait(.1);continue
                active,ready=self.active,self.ready
                self.pending=True;self.pending_floor=active
            try:
                actual=self.lease.refresh(active_cpus=active,ready_cpus=ready)
            except Exception as error:
                if (getattr(error,'status_code',None)==409 and
                        'Insufficient memory for resource resize; existing grant retained'in str(error)):
                    actual=self.lease.cpus
                else:
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


def _map_items(function,items,*,kwargs=None,backend='process',on_result=None,collect=True,stop=None,ready_slots=None,on_start=None,on_capacity=None,ready_demand=None):
    """Evaluate independent business inputs; DCO owns all resource management.

    Functions are importable module:function names or top-level callables.
    Each item receives one process and one CPU; results keep input order.
    """
    items=list(items);kwargs=dict(kwargs or {})
    if not items:return []if collect else 0
    if os.environ.get('REMOTE_COMPUTE_WORKER')!='1':
        raise RuntimeError('Submit python_map to DCO before executing its items')
    if backend not in ('process','thread'):raise ValueError('backend requires process or thread')
    if os.environ.get('DCO_ITEM_PROCESS')=='1' or getattr(_local,'item',False)or getattr(_local,'kernel',False):
        values=[];position=0
        for index,item in enumerate(items):
            while ready_slots and not ready_slots()and not(stop and stop()):time.sleep(.02)
            if stop and stop():break
            position+=1
            if on_start:on_start(index)
            try:value=_item(function,item,kwargs)
            except BaseException as error:
                if on_result:on_result(index,None,str(error))
                raise
            if collect:values.append(value)
            if on_result:on_result(index,value,None)
        return values if collect else position
    maximum=min(len(items),max(1,int(os.environ.get('REMOTE_COMPUTE_MAX_CPUS',os.cpu_count()or 1))))
    grants=_Grants(maximum);position=0;active={};values=[None]*len(items)if collect else None;failures=[];completed=Queue()
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
            if on_capacity:on_capacity(grants.cpus)
            while (not failures and not(stop and stop())and position<len(items)and len(active)<limit
                    and (ready_slots is None or ready_slots()>0)):
                operation=_process_item if backend=='process'else _thread_item
                future=pool.submit(operation,function,items[position],kwargs)
                if on_start:on_start(position)
                active[future]=position;position+=1
                future.add_done_callback(completed.put)
            stopped=bool(failures or stop and stop())
            def ready():
                remaining=len(items)-position
                if failures or stop and stop():return 0
                if ready_demand:return ready_demand(len(active),remaining)
                return min(remaining,ready_slots()if ready_slots else len(items))
            grants.report(len(active),0 if stopped else ready())
            if not active:
                if stopped or grants.error:break
                time.sleep(.02);continue
            try:done=[completed.get(timeout=.1)]
            except Empty:done=[]
            while not completed.empty():done.append(completed.get_nowait())
            for future in done:
                index=active.pop(future)
                try:
                    value=future.result()
                    if collect:values[index]=value
                    if on_result:on_result(index,value,None)
                except BaseException as error:
                    failures.append(error)
                    if on_result:on_result(index,None,str(error))
            if grants.error:failures.append(grants.error)
            grants.report(len(active),ready())
        if failures:raise RuntimeError('DCO execution failed: '+str(failures[0]))from failures[0]
        if grants.error:raise grants.error
        return values if collect else position
    finally:
        pool.shutdown(wait=True)
        grants.report(0,0);grants.close()


def map_items(function,items,*,kwargs=None,backend='process',on_result=None):
    """Evaluate independent inputs; return values in input order."""
    return _map_items(function,items,kwargs=kwargs,backend=backend,on_result=on_result)


ItemResult=namedtuple('ItemResult','index value error')


class StreamResultsPending(RuntimeError):
    """Started results remain available after an early stream exit."""
    def __init__(self,results,recovery_dir):
        self.results,self.recovery_dir=results,recovery_dir
        super().__init__('Unconsumed DCO stream results: '+str(len(results))+'; recovery: '+str(recovery_dir))


class ItemStream:
    """SDK-owned completion stream; stop new work and drain accepted items."""
    def __init__(self,function,items,kwargs,backend,stop_when):
        items=list(items)
        ceiling=min(max(1,len(items)),max(1,int(os.environ.get('REMOTE_COMPUTE_MAX_CPUS',os.cpu_count()or 1))))
        self.window=min(ceiling,max(1,int(os.environ.get('REMOTE_COMPUTE_CPUS','1'))))
        self.queue=Queue(maxsize=ceiling+1);self.cancel=threading.Event();self.finished=False
        self.error=None;self.started=0;self.items=list(items);self.end=object()
        self.guard=threading.Lock();self.outstanding=0;self.current=None
        self.pending_results=[];self.recovery_dir=None;self.closed=False
        item,kernel=getattr(_local,'item',False),getattr(_local,'kernel',False)
        def produce():
            _local.item,_local.kernel=item,kernel
            def started(index):
                with self.guard:self.started=index+1;self.outstanding+=1
            def slots():
                with self.guard:return self.window-self.outstanding
            def capacity(cpus):
                with self.guard:self.window=max(self.window,min(ceiling,cpus))
            def demand(active,remaining):
                with self.guard:
                    # Grow a fully active window. Once results await the
                    # consumer, reserve only immediately usable launch slots.
                    return remaining if self.outstanding==active else min(remaining,self.window-self.outstanding)
            def publish(index,value,error):
                self.queue.put_nowait(ItemResult(index,value,error))
            try:
                _map_items(function,self.items,kwargs=kwargs,backend=backend,collect=False,
                    stop=lambda:self.cancel.is_set()or bool(stop_when and stop_when()),
                    on_result=publish,ready_slots=slots,on_start=started,on_capacity=capacity,ready_demand=demand)
            except BaseException as error:self.error=error
            finally:self.queue.put_nowait(self.end)
        self.thread=threading.Thread(target=produce,name='dco-completions')
        with _streams_lock:_streams.add(self)
        self.thread.start()

    def __iter__(self):return self

    def __next__(self):
        if self.finished:raise StopIteration
        self._acknowledge()
        item=self.queue.get()
        if item is self.end:
            self.finished=True;self.thread.join()
            with _streams_lock:_streams.discard(self)
            if self.error:raise self.error
            raise StopIteration
        self.current=item
        return item

    def _acknowledge(self):
        if self.current is not None:
            with self.guard:self.outstanding-=1
            self.current=None

    def stop(self):self.cancel.set()

    def close(self):
        return self._close(False)

    def _close(self,consumed):
        if self.closed:return self.pending_results
        self.stop();self.thread.join()
        queued=[]
        while not self.queue.empty():
            item=self.queue.get_nowait()
            if item is not self.end:queued.append(item)
        if self.current is not None:
            if not consumed or queued or self.error:self.pending_results.append(self.current)
            self._acknowledge()
        self.pending_results.extend(queued)
        if self.pending_results:
            # Only unconsumed started work is persisted. The normal streaming
            # path leaves ownership and publication with the business consumer.
            from remote_compute.bundle import atomic_json,native_path
            import uuid
            root=Path(native_path(Path(os.environ.get('REMOTE_COMPUTE_JOB_DIR',Path.cwd().parent))/'input'/'.dco-streams'/uuid.uuid4().hex,force=True))
            root.mkdir(parents=True,exist_ok=True);self.recovery_dir=str(root)
            for record in self.pending_results:
                target=root/(str(record.index)+'.pkl');pending=target.with_suffix('.part')
                try:
                    with pending.open('wb')as output:
                        pickle.dump(dict(index=record.index,input=self.items[record.index],value=record.value,error=record.error),output,protocol=4)
                        output.flush();os.fsync(output.fileno())
                    pending.replace(target)
                finally:
                    if pending.exists():pending.unlink()
            atomic_json(root/'manifest.json',dict(started=self.started,indices=[r.index for r in self.pending_results],error=str(self.error)if self.error else None))
        self.finished=self.closed=True
        with _streams_lock:_streams.discard(self)
        return self.pending_results

    def __enter__(self):return self

    def __exit__(self,*error):
        pending=self._close(error[0]is None)
        if error[0]is None:
            if pending:raise StreamResultsPending(pending,self.recovery_dir)
            if self.error:raise self.error


def iter_items(function,items,*,kwargs=None,backend='process',stop_when=None):
    """Yield index/value/error in completion order; use as a context manager.

    stop() stops new launches; continue iteration to publish in-flight results.
    Outstanding results plus active items fit one useful execution window.
    Early exit preserves unconsumed results and reports StreamResultsPending.
    """
    return ItemStream(function,items,kwargs,backend,stop_when)


def run_call(function,*,limits=None,**kwargs):
    """Execute a numerical kernel using the grant prepared by the DCO worker."""
    if os.environ.get('REMOTE_COMPUTE_WORKER')!='1':
        raise RuntimeError('Submit python_call to DCO before executing its kernel')
    if os.environ.get('DCO_ITEM_PROCESS')=='1'or getattr(_local,'item',False)or getattr(_local,'kernel',False):return _function(function)(**kwargs)
    # A final item can be consumed just before its stream returns resources.
    # Finish that SDK-owned boundary before borrowing for the numerical stage.
    with _streams_lock:streams=list(_streams)
    for stream in streams:
        if stream.thread is threading.current_thread():continue
        if stream.error:raise stream.error
        if stream.thread.is_alive()and stream.started<len(stream.items):
            raise RuntimeError('Drain the DCO item stream before entering a numerical kernel')
        stream.thread.join()
        if stream.error:raise stream.error
    lease=None;actual=max(1,int(os.environ.get('REMOTE_COMPUTE_CPUS','1')))
    maximum=max(1,int(os.environ.get('REMOTE_COMPUTE_MAX_CPUS',actual)))
    limits=dict(limits or {})
    memory_fields={'memory_mb','base_memory_mb','memory_per_cpu_mb','memory_limit_mb'}
    if set(limits)-memory_fields-{'max_cpus'}:raise ValueError('Kernel limits declare max_cpus and memory requirements')
    if 'max_cpus'in limits:
        if type(limits['max_cpus'])is not int or limits['max_cpus']<1:raise ValueError('Kernel max_cpus requires a positive integer')
        maximum=min(maximum,limits['max_cpus'])
    if limits.get('memory_mb')is not None and any(limits.get(key)is not None for key in memory_fields-{'memory_mb'}):
        raise ValueError('Declare fixed memory_mb or a base/per-CPU memory model')
    actual=min(actual,maximum)
    if os.environ.get('REMOTE_COMPUTE_URL')and os.environ.get('REMOTE_COMPUTE_RESIZE')=='1':
        from remote_compute import Client
        client=Client(os.environ['REMOTE_COMPUTE_URL'],os.environ.get('REMOTE_COMPUTE_TOKEN',''),ca_file=os.environ.get('REMOTE_COMPUTE_CA')or None)
        model=json.loads(os.environ.get('REMOTE_COMPUTE_MEMORY_MODEL','{}'))
        if memory_fields.intersection(limits):model={key:value for key,value in limits.items()if key in memory_fields}
        lease=client.cpu_lease(os.environ['REMOTE_COMPUTE_JOB_ID'],max_cpus=max(maximum,actual),cpus=actual,**model)
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
        atomic_json(output,dict(completed=len(records),total=len(spec['items'])if spec['mode']=='map'else 1,items=[records[i]for i in sorted(records)]))
    try:
        if spec['mode']=='call':
            value=run_call(spec['function'],**spec['kwargs']);publish(0,value,None)
        elif spec['mode']=='entry':
            _single_threaded(False)
            value=_function(spec['function'])(**spec['kwargs']);publish(0,value,None)
        else:map_items(spec['function'],spec['items'],kwargs=spec['kwargs'],backend=spec.get('backend','process'),on_result=publish)
    except BaseException as caught:
        error=caught
    finally:
        from remote_compute.bundle import atomic_json
        atomic_json(output,dict(completed=len(records),total=len(spec['items'])if spec['mode']=='map'else 1,
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
