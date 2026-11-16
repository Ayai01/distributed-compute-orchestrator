"""Concurrent durable calls share credits through short state transactions."""
from contextlib import contextmanager
from concurrent.futures import Future,ThreadPoolExecutor,wait
from hashlib import sha256
from pathlib import Path
import json,os,threading,time

from .bundle import atomic_json,native_path
from .worker import Conflict,lock_root


_controllers={}
_controllers_lock=threading.Lock()
_callback=threading.local()
_unpublished={'submitting','accepted','fetched','publication_failed','published'}


def _owner(root):return os.path.normcase(str(native_path(Path(root).resolve(),force=True)))


def _unfinished(row):
    return row['state']in _unpublished or row.get('recovery')and row['state']=='new'


class _CallPool:
    """Join this call's work without shutting down a peer's shared executor."""
    def __init__(self,executor,identity):
        self.executor=executor;self.identity=identity;self.futures=set();self.lock=threading.Lock()

    def submit(self,function,*args,**kwargs):
        future=Future()
        def discard(completed):
            with self.lock:self.futures.discard(completed)
        def run():
            if not future.set_running_or_notify_cancel():return
            previous=getattr(_callback,'identity',None);_callback.identity=self.identity
            try:
                try:value=function(*args,**kwargs)
                except BaseException as error:future.set_exception(error)
                else:future.set_result(value)
            finally:_callback.identity=previous
        # Register before enqueueing. An interrupt after enqueue still leaves
        # a joined owner; a cancelled queued wrapper never runs business work.
        try:
            with self.lock:self.futures.add(future)
            future.add_done_callback(discard)
            self.executor.submit(run)
        except BaseException:
            if future.cancel():
                with self.lock:self.futures.discard(future)
            raise
        return future

    def join(self):
        with self.lock:futures=list(self.futures)
        if futures:wait(futures)


class _Controller:
    def __init__(self,path,project):
        self.path=path;self.identity=_owner(path);self.project=project;self.users=0
        self.configuration=threading.Lock();self.window=None;self.pools=();self.next_reap=0.
        self.registry=self.path/'executions.json'

    def _read(self):
        value=json.loads(self.registry.read_bytes())if self.registry.exists()else dict(
            project=self.project,ready_tasks=None,directories={})
        if value['project']!=self.project:raise ValueError('Execution registry belongs to another project')
        return value

    def read(self):
        # Windows readers must share the writer's short index lock: an open
        # read handle otherwise denies the writer's atomic replacement.
        with self.transaction()as value:return value

    @contextmanager
    def transaction(self):
        # The native lock protects only the read/modify/write of this index.
        # RPC, preparation, publication and cold row reads stay outside it.
        deadline=time.monotonic()+30
        while True:
            try:lock=lock_root(self.path,'pipeline');break
            except Conflict:
                if time.monotonic()>=deadline:raise
                time.sleep(.01)
        try:yield self._read()
        finally:lock.close()

    def save(self,value):atomic_json(self.registry,value)

    def pending(self,owner,entries):
        result={}
        for identity,key in entries.items():
            path=Path(owner)/'jobs'/key/'state.json'
            if not path.exists()or _unfinished(json.loads(path.read_bytes())):result[identity]=key
        return result

    def restore_index(self,owner):
        previous=self.read()
        if not isinstance(previous['directories'],list):return
        # Import existing receipt references once, while their output owners
        # are inactive or this call already owns the same output directory.
        holds=[];directories={}
        try:
            for root in previous['directories']:
                root=_owner(root)
                if root!=owner and Path(root).is_dir():holds.append(lock_root(Path(root),'pipeline'))
                pending={}
                for path in (Path(root)/'jobs').glob('*/state.json'):
                    row=json.loads(path.read_bytes())
                    if _unfinished(row):pending[row['id']]=path.parent.name
                if pending:directories[root]=dict(pending=pending,recovering=False)
            with self.transaction()as value:
                if isinstance(value['directories'],list):
                    value.update(ready_tasks=self.ready_tasks,directories=directories);self.save(value)
        finally:
            for hold in holds:hold.close()

    def import_legacy(self,path,owner):
        """Import the caller's old cwd index once, retaining unpublished originals."""
        if _owner(path)==self.identity or not(path/'executions.json').is_file():return
        identity=_owner(path)
        if identity in self.read().get('imported_indexes',()):return
        source=_Controller(path,self.project)
        previous=source.read()
        directories=previous['directories']
        if isinstance(directories,list):
            # Old list indexes have no pending IDs. Reuse their bounded
            # directory migration while leaving the original index in place.
            holds=[];entries={}
            try:
                for root in directories:
                    root=_owner(root)
                    if root!=owner and Path(root).is_dir():holds.append(lock_root(Path(root),'pipeline'))
                    pending={}
                    for row_path in (Path(root)/'jobs').glob('*/state.json'):
                        row=json.loads(row_path.read_bytes())
                        if _unfinished(row):pending[row['id']]=row_path.parent.name
                    if pending:entries[root]=dict(pending=pending,recovering=False)
                directories=entries
            finally:
                for hold in holds:hold.close()
        with self.transaction()as value:
            imports=value.setdefault('imported_indexes',[])
            if identity in imports:return
            share=previous.get('ready_tasks',self.ready_tasks)
            if directories and value['directories']and value['ready_tasks']!=share:
                raise ValueError('Concurrent executions of one project require the same ready_tasks')
            if directories and not value['directories']:value['ready_tasks']=share
            for root,entry in directories.items():
                current=value['directories'].setdefault(root,dict(pending={},recovering=False))
                current['pending'].update(entry['pending'])
            imports.append(identity);self.save(value)

    def reap(self,owner):
        for root,entry in self.read()['directories'].items():
            if root==owner:continue
            if not Path(root).is_dir():
                with self.transaction()as value:
                    current=value['directories'].get(root)
                    if current is not None:
                        if not current['pending']:value['directories'].pop(root)
                        else:current['recovering']=False
                        if current!=entry or not current['pending']:self.save(value)
                continue
            try:hold=lock_root(Path(root),'pipeline')
            except Conflict:continue
            try:
                pending=self.pending(root,entry['pending'])
                with self.transaction()as value:
                    current=value['directories'].get(root)
                    if current is None:continue
                    if not pending:value['directories'].pop(root)
                    else:
                        current['pending']=pending;current['recovering']=False
                    if current!=entry or not pending:self.save(value)
            finally:hold.close()

    def initialize(self,client,ready_tasks,owner):
        with self.configuration:
            if self.window is not None:
                if ready_tasks!=self.ready_tasks:
                    raise ValueError('Concurrent executions of one project require the same ready_tasks')
                return
            self.ready_tasks=ready_tasks
            self.restore_index(owner)
            window=client.window(self.project,**({}if ready_tasks is None else dict(ready_tasks=ready_tasks)))
            self.pools=tuple(ThreadPoolExecutor(width,thread_name_prefix='dco-'+name)
                             for width,name in ((4,'prepare'),(2,'submit'),(2,'collect')))
            self.window=window

    def register(self,owner):
        self.reap(owner)
        with self.transaction()as value:
            if value['directories']and value['ready_tasks']!=self.ready_tasks:
                raise ValueError('Concurrent executions of one project require the same ready_tasks')
            value['ready_tasks']=self.ready_tasks
            entry=value['directories'].setdefault(owner,dict(pending={},recovering=False))
            entry['recovering']=True;self.save(value)

    def recovered(self,owner):
        with self.transaction()as value:
            entry=value['directories'].get(owner)
            if entry and entry['recovering']:
                entry['recovering']=False;self.save(value)

    def hold(self,row,owner,*,recovered=False):
        maximum=None if recovered else self.window.current()
        with self.transaction()as value:
            directories=value['directories']
            for root,entry in directories.items():
                if row['id']in entry['pending']:
                    if root!=owner:raise ValueError('Original job already belongs to another execution directory: '+row['id'])
                    return True
            blocked=(not recovered and any(entry['recovering']for entry in directories.values())or
                     maximum is not None and sum(len(entry['pending'])for entry in directories.values())>=maximum)
            if not blocked:
                directories[owner]['pending'][row['id']]=Path(row['directory']).name
                self.save(value);return True
        # A peer may exit after this call registered. Reconcile only on
        # backpressure, outside the project transaction, at most once a second.
        now=time.monotonic()
        if now>=self.next_reap:
            self.next_reap=now+1.;self.reap(owner)
        return False

    def release(self,row,owner):
        if _unfinished(row):return
        with self.transaction()as value:
            pending=value['directories'][owner]['pending']
            if row['id']in pending:
                pending.pop(row['id']);self.save(value)

    def reconcile(self,owner):
        entry=self.read()['directories'].get(owner)
        if entry is None:return
        pending=self.pending(owner,entry['pending'])
        with self.transaction()as value:
            current=value['directories'].get(owner)
            if current is None:return
            if pending:current.update(pending=pending,recovering=False)
            else:value['directories'].pop(owner)
            self.save(value)

    def close(self):
        for pool in self.pools:pool.shutdown(wait=True)


class _Call:
    def __init__(self,controller,root):self.controller=controller;self.owner=_owner(root)

    @property
    def window(self):return self.controller.window

    def hold(self,row,*,recovered=False):return self.controller.hold(row,self.owner,recovered=recovered)

    def release(self,row):self.controller.release(row,self.owner)

    def recovered(self):self.controller.recovered(self.owner)

    def wait(self):
        time.sleep(.05)

    @contextmanager
    def pools(self):
        pools=tuple(_CallPool(pool,self.controller.identity)for pool in self.controller.pools)
        try:yield pools
        finally:
            for pool in pools:pool.join()


@contextmanager
def project_call(client,project,root,ready_tasks):
    identity=sha256((client.url+'\0'+project).encode()).hexdigest()
    path=client.state_dir/identity
    key=_owner(path)
    if getattr(_callback,'identity',None)==key:
        raise Conflict('Execute callbacks cannot reenter their project; declare business dependencies with after')
    with _controllers_lock:
        controller=_controllers.get(key)
        if controller is None:controller=_controllers[key]=_Controller(path,project)
        controller.users+=1
    call=None
    try:
        controller.initialize(client,ready_tasks,_owner(root))
        controller.import_legacy(Path.cwd()/'.local/dco-client'/identity,_owner(root))
        call=_Call(controller,root);controller.register(call.owner)
        yield call
    finally:
        try:
            if call:
                call.recovered();controller.reconcile(call.owner)
        finally:
            with _controllers_lock:
                controller.users-=1
                if not controller.users:
                    controller.close();_controllers.pop(key)
