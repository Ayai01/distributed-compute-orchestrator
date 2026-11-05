"""One local project controller for concurrent durable execution directories."""
from contextlib import contextmanager
from concurrent.futures import Future,ThreadPoolExecutor,wait
from hashlib import sha256
from pathlib import Path
import json,os,threading

from .bundle import atomic_json
from .worker import Conflict,lock_root


_controllers={}
_controllers_lock=threading.Lock()
_callback=threading.local()
_unpublished={'submitting','accepted','fetched','publication_failed','published'}


def _owner(root):return os.path.normcase(str(Path(root).resolve()))


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
        self.configuration=threading.Lock();self.condition=threading.Condition()
        self.window=None;self.file_lock=None;self.pools=();self.roots=set();self.held={};self.recovering=set()

    def initialize(self,client,ready_tasks):
        # A slow center or a cold recovery blocks only this project.
        with self.configuration:
            if self.window is not None:
                if ready_tasks!=self.ready_tasks:
                    raise ValueError('Concurrent executions of one project require the same ready_tasks')
                return
            self.file_lock=lock_root(self.path,'pipeline')
            try:
                self.held={}
                self.registry=self.path/'executions.json'
                if self.registry.exists():
                    saved=json.loads(self.registry.read_bytes())
                    if saved['project']!=self.project:raise ValueError('Execution registry belongs to another project')
                    for root in saved['directories']:
                        for path in (Path(root)/'jobs').glob('*/state.json'):
                            row=json.loads(path.read_bytes())
                            if _unfinished(row):self._adopt(row,root)
                self.roots={owner for owner,_ in self.held.values()}
                self.ready_tasks=ready_tasks
                self.window=client.window(self.project,**({}if ready_tasks is None else dict(ready_tasks=ready_tasks)))
                self.pools=tuple(ThreadPoolExecutor(width,thread_name_prefix='dco-'+name)
                                 for width,name in ((4,'prepare'),(2,'submit'),(2,'collect')))
            except BaseException:
                self.window=None;self.file_lock.close();self.file_lock=None;raise

    def _adopt(self,row,owner):
        previous=self.held.get(row['id'])
        if previous is not None and previous[0]!=owner:
            raise ValueError('Original job already belongs to another execution directory: '+row['id'])
        self.held[row['id']]=(owner,str(Path(row['directory'])/'state.json'))

    def _save_roots(self):
        atomic_json(self.registry,dict(project=self.project,directories=sorted(self.roots)))

    def register(self,owner):
        with self.condition:
            self.roots.add(owner);self.recovering.add(owner);self._save_roots()

    def recovered(self,owner):
        with self.condition:
            self.recovering.discard(owner);self.condition.notify_all()

    def hold(self,row,owner,*,recovered=False):
        maximum=None if recovered else self.window.current()
        with self.condition:
            if row['id']in self.held:
                self._adopt(row,owner);return True
            if not recovered and self.recovering:return False
            if maximum is not None and len(self.held)>=maximum:return False
            self._adopt(row,owner);return True

    def release(self,row,owner):
        with self.condition:
            if not _unfinished(row)and self.held.get(row['id'],(None,))[0]==owner:
                self.held.pop(row['id']);self.condition.notify_all()

    def reconcile(self,owner):
        with self.condition:
            for identity,(current,path)in list(self.held.items()):
                if current==owner and not _unfinished(json.loads(Path(path).read_bytes())):
                    self.held.pop(identity)
            if not any(current==owner for current,_ in self.held.values()):self.roots.discard(owner)
            self._save_roots();self.condition.notify_all()

    def close(self):
        for pool in self.pools:pool.shutdown(wait=True)
        if self.file_lock:self.file_lock.close()


class _Call:
    def __init__(self,controller,root):self.controller=controller;self.owner=_owner(root)

    @property
    def window(self):return self.controller.window

    def hold(self,row,*,recovered=False):return self.controller.hold(row,self.owner,recovered=recovered)

    def release(self,row):self.controller.release(row,self.owner)

    def recovered(self):self.controller.recovered(self.owner)

    def wait(self):
        with self.controller.condition:self.controller.condition.wait(.05)

    @contextmanager
    def pools(self):
        pools=tuple(_CallPool(pool,self.controller.identity)for pool in self.controller.pools)
        try:yield pools
        finally:
            for pool in pools:pool.join()


@contextmanager
def project_call(client,project,root,ready_tasks):
    identity=sha256((client.url+'\0'+project).encode()).hexdigest()
    path=Path.cwd()/'.local/dco-client'/identity
    key=_owner(path)
    if getattr(_callback,'identity',None)==key:
        raise Conflict('Execute callbacks cannot reenter their project; declare business dependencies with after')
    with _controllers_lock:
        controller=_controllers.get(key)
        if controller is None:controller=_controllers[key]=_Controller(path,project)
        controller.users+=1
    call=None
    try:
        controller.initialize(client,ready_tasks)
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
