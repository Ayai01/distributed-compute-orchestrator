"""DCO-owned execution of business functions and independent input items."""
import importlib
import json
import math
import os
from pathlib import Path
import re
import pickle
import sys
import threading
from collections import namedtuple
import time

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
    from remote_compute.deadline import check_deadline
    from remote_compute.memory import memory_boundary
    memory_boundary()
    check_deadline()
    target=_function(function)
    if getattr(_local,'item_channel',None)is not None:_kernel_threads(1)
    return target(item,**kwargs)


def _single_threaded(item_process=True):
    if item_process:os.environ['DCO_ITEM_PROCESS']='1'
    os.environ['REMOTE_COMPUTE_CPUS']='1'
    for key in ('OMP_NUM_THREADS','OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','NUMEXPR_NUM_THREADS','NUMBA_NUM_THREADS'):
        os.environ[key]='1'


_local=threading.local()
_streams=set()
_streams_lock=threading.Lock()
_threadpool_controller=None
_threadpool_legacy_limits=None
_threadpool_module_count=-1
_kernel_thread_count=None


def _thread_item(function,item,kwargs):
    _local.item=True
    numba=sys.modules.get('numba')
    if numba is not None:numba.set_num_threads(1)
    try:return _item(function,item,kwargs)
    finally:_local.item=False


def _process_item(function,item,kwargs,channel=None):
    # One process lifetime per business item returns its allocator/model RAM
    # when that item completes, including a map's long tail.
    import subprocess,tempfile
    from collections import deque
    with tempfile.TemporaryDirectory(prefix='dco-item-')as folder:
        root=Path(folder);request=root/'request.pkl';result=root/'result.pkl'
        request.write_bytes(pickle.dumps((function,item,kwargs,channel),protocol=4))
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


def _phase_resources(maximum,limits):
    limits=dict(limits or {})
    memory_fields={'memory_mb','base_memory_mb','memory_per_cpu_mb','memory_limit_mb'}
    if set(limits)-memory_fields-{'max_cpus'}:raise ValueError('Phase limits declare max_cpus and memory requirements')
    if 'max_cpus'in limits:
        if type(limits['max_cpus'])is not int or limits['max_cpus']<1:raise ValueError('Phase max_cpus requires a positive integer')
        maximum=min(maximum,limits['max_cpus'])
    if limits.get('memory_mb')is not None and any(limits.get(key)is not None for key in memory_fields-{'memory_mb'}):
        raise ValueError('Declare fixed memory_mb or a base/per-CPU memory model')
    model=json.loads(os.environ.get('REMOTE_COMPUTE_MEMORY_MODEL','{}'))
    if memory_fields.intersection(limits):model={key:value for key,value in limits.items()if key in memory_fields}
    return maximum,model


class _Grants:
    """Only the executor exchanges resource messages; business code sees results."""
    def __init__(self,maximum,limits=None):
        maximum,model=_phase_resources(maximum,limits)
        self.maximum=maximum
        self.cpus=min(maximum,max(1,int(os.environ.get('REMOTE_COMPUTE_CPUS','1'))))
        self.condition=threading.Condition();self.active=self.ready=0
        self.pending=False;self.fenced=False;self.pending_floor=0;self.error=None;self.stopping=False
        self.thread=None;self.lease=None
        url=os.environ.get('REMOTE_COMPUTE_URL','')
        if url and os.environ.get('REMOTE_COMPUTE_RESIZE')=='1':
            from remote_compute import Client
            client=Client(url,os.environ.get('REMOTE_COMPUTE_TOKEN',''),ca_file=os.environ.get('REMOTE_COMPUTE_CA')or None)
            self.lease=client.cpu_lease(os.environ['REMOTE_COMPUTE_JOB_ID'],max_cpus=maximum,cpus=self.cpus,**model)
            # Task-entry environment can precede a completed stage that
            # returned its CPUs. Every map first acknowledges its own phase.
            self.cpus=self.lease.refresh(active_cpus=0,ready_cpus=maximum)
            self.ready=maximum
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
                with self.condition:
                    self.error=error;self.pending=False;self.condition.notify_all()
                return
            with self.condition:
                self.cpus=actual;self.pending=False
                self.fenced=self.active<active
                if self.fenced:self.pending_floor=self.active
                observer=getattr(self,'observer',None)
                if observer:observer()
                self.condition.notify_all()
                if self.stopping:return
                # A report arriving during RPC needs its own acknowledged snapshot.
                if not self.fenced and (self.active,self.ready)==(active,ready):self.condition.wait(1)

    def limit(self):
        with self.condition:
            if self.error:return 0
            # While a shrinking request is in flight, new items must stay
            # within the active floor already reported in that request.
            return min(self.cpus,self.pending_floor)if self.pending or self.fenced else self.cpus

    def reserve(self,requested):
        with self.condition:
            count=max(0,min(requested,self.limit()-self.active))
            # Include promised starts in the same snapshot used by resize.
            # Publication callbacks and process creation run after this lock.
            self.active+=count
            self.ready=max(0,self.ready-count)
            if count:self.condition.notify_all()
            return count

    def report(self,active,ready):
        with self.condition:
            changed=(active,ready)!=(self.active,self.ready)
            if self.lease is not None and active<self.active and not self.pending:
                self.fenced=True;self.pending_floor=active
            self.active,self.ready=active,ready
            if changed:self.condition.notify_all()

    def close(self,*,task_finished=False):
        with self.condition:
            self.stopping=True;pending=self.pending;self.condition.notify_all()
        # A whole-task map exits immediately after publishing its results.
        # Its daemon updater belongs to that exiting process. The worker then
        # releases the original task and ends any pending lease request.
        if task_finished:
            if self.thread and not pending:self.thread.join()
            return
        if self.thread:self.thread.join()
        if self.lease and not self.error:
            self.lease.refresh(active_cpus=0,ready_cpus=0)


def _map_items(function,items,*,kwargs=None,backend='process',on_result=None,collect=True,stop=None,ready_slots=None,on_start=None,on_capacity=None,ready_demand=None,limits=None,item_max_cpus=1,owns_task=False):
    """Evaluate independent business inputs; DCO owns all resource management.

    Functions are importable module:function names or top-level callables.
    Each item receives one process; optional item kernel stages share the
    parent's grant. Results keep input order.
    """
    items=list(items);kwargs=dict(kwargs or {})
    if not items:return []if collect else 0
    if os.environ.get('REMOTE_COMPUTE_WORKER')!='1':
        raise RuntimeError('Submit python_map to DCO before executing its items')
    if backend not in ('process','thread'):raise ValueError('backend requires process or thread')
    if type(item_max_cpus)is not int or item_max_cpus<1:raise ValueError('item_max_cpus requires a positive integer')
    if item_max_cpus>1 and backend!='process':raise ValueError('Multicore items require the process backend')
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
    maximum=min(len(items)*item_max_cpus,max(1,int(os.environ.get('REMOTE_COMPUTE_MAX_CPUS',os.cpu_count()or 1))))
    grants=_Grants(maximum,limits);maximum=grants.maximum
    resources=None
    if item_max_cpus>1:
        from remote_compute.item_resources import ItemResources
        resources=ItemResources(grants,item_max_cpus,len(items))
    from concurrent.futures import ThreadPoolExecutor
    from queue import Queue,Empty
    position=0;active={};values=[None]*len(items)if collect else None;failures=[];completed=Queue()
    if backend=='thread':
        _single_threaded(False);function=_function(function)
        try:
            from threadpoolctl import threadpool_limits
        except ImportError:pass
        else:threadpool_limits(limits=1)
        torch=sys.modules.get('torch')
        if torch is not None:torch.set_num_threads(1)
    pool=ThreadPoolExecutor(max_workers=min(len(items),maximum),thread_name_prefix='dco-item')
    try:
        while active or position<len(items):
            from remote_compute.memory import memory_waiting,memory_boundary
            if on_capacity:on_capacity(grants.cpus)
            requested=len(items)-position
            if failures or stop and stop() or memory_waiting():requested=0
            elif ready_slots is not None:requested=min(requested,ready_slots())
            starts=resources.reserve(position,requested)if resources else grants.reserve(requested)
            for _ in range(starts):
                if stop and stop():break
                from remote_compute.deadline import check_deadline
                try:check_deadline()
                except TimeoutError as error:
                    failures.append(error);break
                operation=_process_item if backend=='process'else _thread_item
                if on_start:on_start(position)
                if resources:
                    future=pool.submit(operation,function,items[position],kwargs,resources.channel(position))
                else:future=pool.submit(operation,function,items[position],kwargs)
                active[future]=position;position+=1
                future.add_done_callback(completed.put)
            stopped=bool(failures or stop and stop())
            def ready():
                remaining=len(items)-position
                if failures or stop and stop():return 0
                if ready_demand:return ready_demand(len(active),remaining)
                return min(remaining,ready_slots()if ready_slots else len(items))
            if resources:resources.report(0 if stopped else ready())
            else:grants.report(len(active),0 if stopped else ready())
            if not active:
                if stopped or grants.error:break
                memory_boundary()
                time.sleep(.02);continue
            try:done=[completed.get(timeout=.1)]
            except Empty:done=[]
            while not completed.empty():done.append(completed.get_nowait())
            for future in done:
                index=active.pop(future)
                if resources:resources.finish(index)
                try:
                    value=future.result()
                    if collect:values[index]=value
                    if on_result:on_result(index,value,None)
                except BaseException as error:
                    failures.append(error)
                    if on_result:on_result(index,None,str(error))
            if grants.error:failures.append(grants.error)
            if resources:resources.report(ready())
            else:grants.report(len(active),ready())
        if failures:raise RuntimeError('DCO execution failed: '+str(failures[0]))from failures[0]
        if grants.error:raise grants.error
        return values if collect else position
    finally:
        pool.shutdown(wait=True)
        if resources:resources.close()
        if resources and owns_task:grants.close(task_finished=True)
        else:grants.report(0,0);grants.close()


def map_items(function,items,*,kwargs=None,backend='process',on_result=None,limits=None,item_max_cpus=1):
    """Evaluate independent inputs with optional phase CPU/RAM limits."""
    return _map_items(function,items,kwargs=kwargs,backend=backend,on_result=on_result,limits=limits,item_max_cpus=item_max_cpus)


ItemResult=namedtuple('ItemResult','index value error')


class StreamResultsPending(RuntimeError):
    """Started results remain available after an early stream exit."""
    def __init__(self,results,recovery_dir):
        self.results,self.recovery_dir=results,recovery_dir
        super().__init__('Unconsumed DCO stream results: '+str(len(results))+'; recovery: '+str(recovery_dir))


class ItemStream:
    """SDK-owned completion stream; stop new work and drain accepted items."""
    def __init__(self,function,items,kwargs,backend,stop_when,limits=None):
        from queue import Queue
        items=list(items)
        ceiling=min(max(1,len(items)),max(1,int(os.environ.get('REMOTE_COMPUTE_MAX_CPUS',os.cpu_count()or 1))))
        limits=dict(limits or {});ceiling,_=_phase_resources(ceiling,limits)
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
                    on_result=publish,ready_slots=slots,on_start=started,on_capacity=capacity,ready_demand=demand,limits=limits)
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


def iter_items(function,items,*,kwargs=None,backend='process',stop_when=None,limits=None):
    """Yield index/value/error in completion order; use as a context manager.

    stop() stops new launches; continue iteration to publish in-flight results.
    Outstanding results plus active items fit one useful execution window.
    Early exit preserves unconsumed results and reports StreamResultsPending.
    """
    return ItemStream(function,items,kwargs,backend,stop_when,limits)


def _kernel_threads(count,*,reset=False):
    global _threadpool_controller,_threadpool_legacy_limits,_threadpool_module_count,_kernel_thread_count
    changed=_kernel_thread_count!=count or os.environ.get('REMOTE_COMPUTE_CPUS')!=str(count)
    imported=len(sys.modules)!=_threadpool_module_count
    if not(changed or imported or reset):return
    if changed or reset:
        for key in ('REMOTE_COMPUTE_CPUS','OMP_NUM_THREADS','OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','NUMEXPR_NUM_THREADS'):
            if os.environ.get(key)!=str(count):os.environ[key]=str(count)
        ceiling=int(os.environ.get('REMOTE_COMPUTE_ITEM_MAX_CPUS',count))
        if os.environ.get('NUMBA_NUM_THREADS')!=str(max(count,ceiling)):os.environ['NUMBA_NUM_THREADS']=str(max(count,ceiling))
    if imported:
        # Discover DLLs once after imports, never once per evaluation batch.
        try:
            from threadpoolctl import ThreadpoolController
        except ImportError:
            _threadpool_controller=None
            try:from threadpoolctl import threadpool_limits
            except ImportError:_threadpool_legacy_limits=None
            else:_threadpool_legacy_limits=threadpool_limits
        else:_threadpool_controller=ThreadpoolController();_threadpool_legacy_limits=None
        _threadpool_module_count=len(sys.modules)
    if _threadpool_controller is not None:_threadpool_controller.limit(limits=count)
    elif _threadpool_legacy_limits is not None:_threadpool_legacy_limits(limits=count)
    torch=sys.modules.get('torch')
    if torch is not None and torch.get_num_threads()!=count:torch.set_num_threads(count)
    numba=sys.modules.get('numba')
    if numba is not None:
        actual=min(count,numba.config.NUMBA_NUM_THREADS)
        if numba.get_num_threads()!=actual:numba.set_num_threads(actual)
    _kernel_thread_count=count


def _run_exact_kernel(function,kwargs,maximum,model,start_timeout):
    """Start one fixed-width body only after its actual grant is committed."""
    from remote_compute import Client
    from remote_compute.client import RemoteError
    from remote_compute.deadline import check_deadline,resource_window
    from remote_compute.memory import memory_boundary
    if not os.environ.get('REMOTE_COMPUTE_URL')or os.environ.get('REMOTE_COMPUTE_RESIZE')!='1':
        raise RuntimeError('exact_cpus requires a DCO CPU lease')
    client=Client(os.environ['REMOTE_COMPUTE_URL'],os.environ.get('REMOTE_COMPUTE_TOKEN',''),ca_file=os.environ.get('REMOTE_COMPUTE_CA')or None)
    lease=client.cpu_lease(os.environ['REMOTE_COMPUTE_JOB_ID'],max_cpus=maximum,
        cpus=min(maximum,max(1,int(os.environ.get('REMOTE_COMPUTE_CPUS','1')))),**model)
    # No kernel is active during admission. Keep the complete ready demand
    # visible until the provider can fund this indivisible numerical stage.
    actual=0
    try:
        try:
            with resource_window(start_timeout):
                check_deadline();memory_boundary()
                while True:
                    check_deadline()
                    actual=lease.refresh(active_cpus=0,ready_cpus=maximum)
                    if actual>=maximum and lease.confirmed is not None and not lease.phase_pending:
                        actual=lease.refresh(active_cpus=maximum,ready_cpus=0)
                        if actual>=maximum and lease.confirmed is not None and not lease.phase_pending:break
                    time.sleep(min(.1,check_deadline()or .1))
                _kernel_threads(maximum);check_deadline()
        except TimeoutError as error:
            raise TimeoutError('Fixed kernel needs %d CPUs; obtained %d; startup wait expired before execution'%(maximum,actual))from error
        check_deadline()
        _local.kernel=True
        return _function(function)(**kwargs)
    finally:
        _local.kernel=False
        try:_kernel_threads(1,reset=True)
        finally:
            # A late release reply must not replace a paid result or its
            # primary startup error. Final task cleanup also returns the grant.
            try:
                with resource_window(1):lease.refresh(active_cpus=0,ready_cpus=0)
            except (RemoteError,TimeoutError):pass


def run_call(function,*,limits=None,**kwargs):
    """Run a flexible kernel, or wait for limits.exact_cpus before starting."""
    from remote_compute.deadline import check_deadline
    check_deadline()
    if os.environ.get('REMOTE_COMPUTE_WORKER')!='1':
        raise RuntimeError('Submit python_call to DCO before executing its kernel')
    limits=dict(limits or {})
    exact=limits.pop('exact_cpus',None)
    start_timeout=limits.pop('start_timeout',None)
    if exact is not None:
        if type(exact)is not int or exact<1:raise ValueError('exact_cpus requires a positive integer')
        if 'max_cpus'in limits:raise ValueError('Use exact_cpus or max_cpus for a kernel stage')
        if exact>int(os.environ.get('REMOTE_COMPUTE_MAX_CPUS','1')):raise ValueError('exact_cpus exceeds task max_cpus')
        if (getattr(_local,'kernel',False)or getattr(_local,'item_channel',None)is not None
                or os.environ.get('DCO_ITEM_PROCESS')=='1'or getattr(_local,'item',False)):
            raise ValueError('exact_cpus belongs on a top-level run_call stage')
        if start_timeout is not None:
            start_timeout=float(start_timeout)
            if not math.isfinite(start_timeout)or start_timeout<=0:raise ValueError('start_timeout requires a finite positive number')
        limits['max_cpus']=exact
    elif start_timeout is not None:raise ValueError('start_timeout requires exact_cpus')
    if getattr(_local,'kernel',False):return _function(function)(**kwargs)
    from remote_compute.memory import memory_boundary
    if exact is None:memory_boundary()
    channel=getattr(_local,'item_channel',None)
    if channel is not None:
        if set(limits or {})-{'max_cpus'}:
            raise ValueError('Declare aggregate item RAM on the parent map; item stages accept max_cpus')
        maximum,_=_phase_resources(int(os.environ['REMOTE_COMPUTE_ITEM_MAX_CPUS']),limits)
        actual=channel.request('enter',maximum)
        try:
            _local.kernel=True
            target=_function(function);_kernel_threads(actual)
            return target(**kwargs)
        finally:
            _local.kernel=False
            try:_kernel_threads(1,reset=True)
            finally:channel.request('leave')
    if os.environ.get('DCO_ITEM_PROCESS')=='1'or getattr(_local,'item',False):return _function(function)(**kwargs)
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
    maximum,model=_phase_resources(maximum,limits)
    if exact is not None:return _run_exact_kernel(function,kwargs,maximum,model,start_timeout)
    actual=min(actual,maximum)
    if os.environ.get('REMOTE_COMPUTE_URL')and os.environ.get('REMOTE_COMPUTE_RESIZE')=='1':
        from remote_compute import Client
        client=Client(os.environ['REMOTE_COMPUTE_URL'],os.environ.get('REMOTE_COMPUTE_TOKEN',''),ca_file=os.environ.get('REMOTE_COMPUTE_CA')or None)
        lease=client.cpu_lease(os.environ['REMOTE_COMPUTE_JOB_ID'],max_cpus=max(maximum,actual),cpus=actual,**model)
        actual=lease.refresh(active_cpus=0,ready_cpus=maximum)
        if actual<maximum:
            # A cold RAM probe can fund one slot before the phase is known.
            # Confirm the still-ready width once before freezing the kernel;
            # busy hosts may retain the smaller grant without waiting for peers.
            check_deadline()
            if lease.memory_calibrating:
                from remote_compute.deadline import resource_window
                try:
                    with resource_window(3):
                        while actual<maximum and lease.memory_calibrating:
                            time.sleep(.1);check_deadline()
                            actual=lease.refresh(active_cpus=0,ready_cpus=maximum)
                except TimeoutError:
                    check_deadline()
                    actual=lease.cpus
            else:actual=lease.refresh(active_cpus=actual,ready_cpus=maximum-actual)
        actual=lease.refresh(active_cpus=actual,ready_cpus=0)
    try:
        _local.kernel=True
        target=_function(function);_kernel_threads(actual)
        return target(**kwargs)
    finally:
        _local.kernel=False
        _kernel_threads(1,reset=True)
        if lease:lease.refresh(active_cpus=0,ready_cpus=0)


def finish_execution(path,status,error=None,*,spec=None):
    """Finalize only SDK state after its process tree exits; retain paid outputs."""
    from remote_compute.bundle import atomic_json
    path=Path(path)
    if path.is_file():
        value=json.loads(path.read_text(encoding='utf8'))
        if 'item_states'not in value:return
    elif spec is not None:
        total=len(spec['items'])if spec['mode']=='map'else 1
        value=dict(completed=0,total=total,items=[],item_states=[dict(index=i,status='pending')for i in range(total)])
    else:return
    now=time.time()
    for item in value['item_states']:
        if item['status']in ('pending','running'):
            item.update(status='not_started'if item['status']=='pending'else 'interrupted',finished=now)
    value.update(status=status,error=error or value.get('error'))
    atomic_json(path,value)


def main(path):
    from remote_compute.bundle import atomic_json
    from remote_compute.deadline import check_deadline
    spec=json.loads(Path(path).read_text(encoding='utf8'));validate_execution(spec)
    output=Path('.dco-results.json');records={};error=None
    total=len(spec['items'])if spec['mode']=='map'else 1
    states=[dict(index=i,status='pending')for i in range(total)]
    resume=Path('.dco-resource-resume.json')
    if spec['mode']=='map'and resume.is_file():
        previous=json.loads(resume.read_text(encoding='utf8'))
        if (previous['id']!=os.environ.get('REMOTE_COMPUTE_JOB_ID')
                or previous['function']!=spec['function']or previous['total']!=total):
            raise ValueError('Resource recovery does not match the original map')
        for row in previous['items']:
            index=row['index']
            if type(index)is not int or not 0<=index<total or row['status']!='succeeded' or index in records:
                raise ValueError('Invalid completed resource recovery item')
            records[index]=row
            states[index].update(status='succeeded',resource_recovered=True)
    def save(status='running'):
        atomic_json(output,dict(completed=len(records),total=total,status=status,
            items=[records[i]for i in sorted(records)],item_states=states,error=str(error)if error else None))
        if spec['mode']=='map':
            from remote_compute.progress import report_items
            report_items(len(records),total)
    def started(index):
        states[index].update(status='running',started=time.time());save()
    def publish(index,value,failure):
        records[index]=dict(index=index,status='failed'if failure else'succeeded',value=value,error=failure)
        states[index].update(status=records[index]['status'],finished=time.time());save()
    save()
    try:
        check_deadline()
        from remote_compute.memory import memory_boundary
        memory_boundary()
        if spec['mode']=='call':
            started(0)
            value=run_call(spec['function'],**spec['kwargs']);publish(0,value,None)
        elif spec['mode']=='entry':
            started(0)
            _single_threaded(False)
            value=_function(spec['function'])(**spec['kwargs']);publish(0,value,None)
        else:
            pending=[i for i in range(total)if i not in records]
            _map_items(spec['function'],[spec['items'][i]for i in pending],kwargs=spec['kwargs'],backend=spec.get('backend','process'),
                on_start=lambda i:started(pending[i]),on_result=lambda i,value,failure:publish(pending[i],value,failure),
                item_max_cpus=int(os.environ.get('REMOTE_COMPUTE_ITEM_MAX_CPUS','1')),owns_task=True)
    except BaseException as caught:
        error=caught
        if spec['mode']!='map'and states[0]['status']=='running':
            states[0].update(status='failed',finished=time.time())
    finally:
        save('failed'if error else'succeeded')
        finish_execution(output,'failed'if error else'succeeded')
    if error:raise error


def item_main(request,result):
    _single_threaded()
    channel=None
    try:
        function,item,kwargs,*channels=pickle.loads(Path(request).read_bytes())
        endpoint=channels[0]if channels else None
        if endpoint is not None:
            from remote_compute.item_resources import ItemChannel
            channel=_local.item_channel=ItemChannel(*endpoint)
            os.environ['REMOTE_COMPUTE_ITEM_MAX_CPUS']=str(channel.maximum)
            os.environ['NUMBA_NUM_THREADS']=os.environ['REMOTE_COMPUTE_ITEM_MAX_CPUS']
        value=dict(ok=True,value=_item(function,item,kwargs))
    except BaseException:
        import traceback
        value=dict(ok=False,error=traceback.format_exc()[-8192:])
    finally:
        if channel is not None:channel.close();_local.item_channel=None
    Path(result).write_bytes(pickle.dumps(value,protocol=4))


if __name__=='__main__':
    # Business imports and the launcher must share the same runtime state.
    from remote_compute import execution
    if sys.argv[1]=='--item':execution.item_main(sys.argv[2],sys.argv[3])
    else:execution.main(sys.argv[1])
