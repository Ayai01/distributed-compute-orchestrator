"""HTTP client and weighted, adaptive dispatch across independent workers."""

from collections import OrderedDict, deque
from concurrent.futures import ThreadPoolExecutor, wait as wait_futures
from dataclasses import dataclass, field
import hashlib
import gzip
import http.client
import io
import ipaddress
import json
import os
from pathlib import Path
import re
import select
import socket
import ssl
import threading
import time
from urllib.parse import quote
from urllib.parse import urlsplit
import uuid
import weakref
import zipfile

from .bundle import MAX_BODY, PROTOCOL, TERMINAL, json_bytes, pack, task_bundle, tree, unpack
from .policy import normalize_accelerator_requirement, canonical_capabilities
from .auth import lan_endpoint


class RemoteError(RuntimeError):
    pass


class PoolBusy(RemoteError):
    """The local connection queue timed out before any request was sent."""


class RequestCancelled(RemoteError):
    """An optional pre-submission transfer was abandoned by its caller."""


def _check_cancelled(cancel):
    if cancel is not None and cancel():
        raise RequestCancelled('Transfer cancelled before task submission')


class _UploadBody(io.BytesIO):
    def __init__(self, data, cancel):
        super().__init__(data)
        self.cancel = cancel

    def read(self, size=-1):
        _check_cancelled(self.cancel)
        return super().read(size)


class _ConnectionPool:
    def __init__(self):
        self.idle = []
        self.lock = threading.Lock()
        self.condition = threading.Condition()
        self.available = 8
        self.bulk_active = 0
        self.waiting = deque()

    def acquire(self, timeout, *, bulk=False, cancel=None):
        ticket = (object(), bulk)
        deadline = None if timeout is None else time.monotonic() + timeout
        with self.condition:
            self.waiting.append(ticket)
            try:
                # Reserve one of the same eight slots for health, status and
                # grants, even when slow uploads have queued more transfers.
                while not self.available or next((entry for entry in self.waiting
                        if not entry[1] or self.bulk_active < 7), None) is not ticket:
                    _check_cancelled(cancel)
                    remaining = None if deadline is None else deadline - time.monotonic()
                    if remaining is not None and remaining <= 0:
                        return False
                    wait_for=remaining
                    if cancel is not None:wait_for=.1 if remaining is None else min(.1,remaining)
                    self.condition.wait(wait_for)
                _check_cancelled(cancel)
                self.available -= 1
                self.bulk_active += int(bulk)
                return True
            finally:
                self.waiting.remove(ticket)
                self.condition.notify_all()

    def release(self, *, bulk=False):
        with self.condition:
            self.available += 1
            self.bulk_active -= int(bulk)
            self.condition.notify_all()

    def __del__(self):
        for connection in self.idle:
            connection.close()


class Client:
    def __init__(self, url="http://127.0.0.1:26133", token="", *, timeout=None, ca_file=None, operator=False, operator_token=None, pool_timeout=None, lease_timeout=None):
        self.url, self.token, self.operator = url.rstrip("/"), token, bool(operator)
        self.operator_token = operator_token if self.operator else None
        if timeout is None:
            timeout = (float(os.environ.get('REMOTE_COMPUTE_TIMEOUT',10))
                       if self.url==os.environ.get('REMOTE_COMPUTE_URL','').rstrip('/') else 10)
        self.timeout = timeout
        self.lease_timeout = max(timeout, 130) if lease_timeout is None else lease_timeout
        self.pool_timeout = pool_timeout
        self.endpoint = urlsplit(self.url)
        self.connections = threading.local()
        self._capabilities = None
        self._asset_ids = OrderedDict()
        self._asset_lock = threading.Lock()
        self._asset_transfers = weakref.WeakValueDictionary()
        self.ca_file=ca_file or (os.environ.get('REMOTE_COMPUTE_CA') if self.url==os.environ.get('REMOTE_COMPUTE_URL','').rstrip('/') else None)
        self.ssl_context=ssl.create_default_context(cafile=self.ca_file) if self.ca_file else None

    def request(self, path, data=None, *, method=None, binary=False, brief=False, extra_headers=None, cancel=None):
        pool=self.__dict__.get('_transport')
        if pool is None:
            pool=self.__dict__.setdefault('_transport',_ConnectionPool())
        # Local FIFO backpressure is independent of a socket's network timeout.
        # A busy client waits for its bounded slot without failing accepted work.
        # Cache lookups/handoffs and collection cleanup may wait behind disk I/O,
        # even with tiny JSON bodies. They must leave the control slot available.
        bulk = (isinstance(data, bytes) or binary or path.startswith('/assets/')
                or path.endswith(('/asset-handoff', '/logs'))
                or method == 'DELETE' and path.startswith('/jobs/'))
        if not pool.acquire(getattr(self, 'pool_timeout', None), bulk=bulk, cancel=cancel):
            raise PoolBusy(f'{self.url}{path}: connection pool is busy')
        # Old live instances may still own a per-thread cached connection.
        previous=getattr(self.connections,'connection',None)
        if previous is not None:previous.close()
        self.connections.owner=None
        with pool.lock:
            connection=pool.idle.pop() if pool.idle else None
        if connection is not None and connection.sock is not None:
            try:
                if select.select([connection.sock],[],[],0)[0]:connection.close()
            except (OSError,ValueError):connection.close()
        self.connections.connection=connection
        try:
            return self._request(path,data,method=method,binary=binary,brief=brief,extra_headers=extra_headers,cancel=cancel)
        except RequestCancelled:
            connection=getattr(self.connections,'connection',None)
            if connection is not None:connection.close()
            self.connections.connection=None
            raise
        finally:
            connection=getattr(self.connections,'connection',None)
            self.connections.connection=None
            if connection is not None:
                with pool.lock:pool.idle.append(connection)
            pool.release(bulk=bulk)

    def _request(self, path, data=None, *, method=None, binary=False, brief=False, extra_headers=None, cancel=None):
        if self.endpoint.scheme not in {'http', 'https'}:
            raise RemoteError('Private worker is available only inside its owning hybrid scheduler')
        if self.endpoint.scheme == 'http' and not getattr(self, '_lan_http_checked', False):
            if not lan_endpoint(self.endpoint.hostname):
                raise RemoteError('Non-LAN endpoints require HTTPS with certificate verification')
            self._lan_http_checked = True
        # Both resource routes can wait for the center/worker handshake. A
        # direct hybrid resize needs the same deadline as a worker-side lease;
        # replies and ordinary requests keep their normal network deadline.
        resource_wait = (method in (None, 'POST') and isinstance(data, dict) and (
            re.fullmatch(r'/jobs/[0-9a-f]{32}/resize', path)
            or 'request_id' not in data and re.fullmatch(r'/jobs/[0-9a-f]{32}/lease', path)))
        network_timeout = getattr(self, 'lease_timeout', max(self.timeout, 130)) if resource_wait else self.timeout
        headers = {"Content-Type": "application/zip" if isinstance(data, bytes) else "application/json",
                   'Accept-Encoding': 'gzip','X-DCO-Keepalive':'1'}
        auth_token = (self.operator_token or self.token) if self.operator else self.token
        if auth_token:
            headers["Authorization"] = "Bearer " + auth_token
        if self.operator:
            headers['X-DCO-Role'] = 'operator'
        if brief:
            headers['X-DCO-Brief'] = '1'
        headers.update(extra_headers or {})
        # Project clients cannot elevate a request by smuggling the role in
        # per-call headers; only an operator Client instance sets it.
        if self.operator:
            headers['X-DCO-Role'] = 'operator'
        else:
            headers.pop('X-DCO-Role', None)
        stream_body=isinstance(data,bytes)
        if stream_body:
            headers['Content-Length']=str(len(data))
        elif data is not None:
            data = json_bytes(data)
        if data is not None and len(data)>MAX_BODY:
            raise ValueError('Request body has %d bytes; protocol limit is %d bytes. Use separate content assets for large inputs.'%(len(data),MAX_BODY))
        method = method or ('POST' if data is not None else 'GET')
        partial, etag = bytearray(), None
        attempts = 4 if binary and method == 'GET' else 2
        retryable=(method=='GET' or method=='POST' and (path=='/admission' or bool(re.fullmatch(
            r'/(jobs/[0-9a-f]{32}(/cancel)?|assets/[0-9a-f]{64})',path))))
        asset_upload=method=='POST' and stream_body and bool(re.fullmatch(r'/assets/[0-9a-f]{64}',path))
        for attempt in range(attempts):
            _check_cancelled(cancel)
            if attempt and asset_upload:
                # A lost reply can follow a fully validated upload. Probe within
                # this pool slot before sending the same content again.
                try:
                    published=self._request(path,method='GET',extra_headers=extra_headers,cancel=cancel)
                except RequestCancelled:
                    raise
                except RemoteError:
                    pass
                else:
                    if published['exists']:
                        return json_bytes(published) if binary else published
            connection = getattr(self.connections, 'connection', None)
            if connection is None:
                factory = http.client.HTTPSConnection if self.endpoint.scheme == 'https' else http.client.HTTPConnection
                options={'context':getattr(self,'ssl_context',None)} if self.endpoint.scheme=='https' else {}
                connection = factory(self.endpoint.hostname, self.endpoint.port, timeout=network_timeout,**options)
                self.connections.connection = connection
            connection.timeout = network_timeout
            if connection.sock is not None:
                connection.sock.settimeout(network_timeout)
            try:
                if partial and etag:
                    headers.update(Range='bytes='+str(len(partial))+'-', **{'If-Range':etag})
                else:
                    headers.pop('Range',None);headers.pop('If-Range',None)
                # A bytes body uses one sendall with a whole-body deadline.
                # The reader loop sends bounded blocks, so active uploads can
                # outlast the socket timeout. Each retry starts at byte zero.
                body=_UploadBody(data,cancel) if stream_body else data
                connection.request(method, self.endpoint.path.rstrip('/') + path, body=body, headers=headers)
                response = connection.getresponse()
                self.connections.response_status=response.status
                self.connections.etag=response.getheader("ETag")
                if binary and response.status < 400:
                    continued=(partial and response.status==206 and response.getheader('ETag')==etag
                               and response.getheader('Content-Range','').startswith('bytes '+str(len(partial))+'-'))
                    if response.status==206 and not continued:
                        partial.clear();etag=None
                        raise http.client.HTTPException('Invalid artifact resume range')
                    if not continued:
                        partial.clear()
                    etag=response.getheader('ETag') if response.getheader('Accept-Ranges')=='bytes' else None
                    while True:
                        chunk=response.read1(128*1024)
                        if not chunk:
                            if response.length not in (None,0):
                                raise http.client.IncompleteRead(b'',response.length)
                            break
                        partial.extend(chunk)
                    raw=bytes(partial)
                else:
                    raw = response.read()
                if response.getheader('Content-Encoding') == 'gzip':
                    raw = gzip.decompress(raw)
                if response.status >= 400:
                    detail = raw.decode('utf-8', errors='replace')
                    error = RemoteError(f'{self.url}{path}: HTTP {response.status}: {detail}')
                    error.status_code = response.status
                    raise error
                break
            except (http.client.HTTPException, OSError) as exc:
                connection.close()
                self.connections.connection = None
                if not etag:
                    partial.clear()
                if not retryable or attempt==attempts-1:
                    error=RemoteError(f'{self.url}{path}: {exc}')
                    error.transport_code=getattr(exc,'winerror',None) or getattr(exc,'errno',None)
                    error.transport_retryable=isinstance(exc,(ConnectionError,TimeoutError,http.client.HTTPException))
                    raise error from None
                time.sleep(min(1,.1*2**attempt))
        return raw if binary else json.loads(raw)

    def health(self, *, brief=False, sample=False):
        result = self.request("/health", brief=brief, **({"extra_headers":{"X-DCO-Sample":"1"}} if sample else {}))
        if result.get("protocol") != PROTOCOL:
            raise RemoteError("Worker protocol differs: " + self.url)
        self._capabilities = result.get('capabilities',getattr(self,'_capabilities',None) or [])
        if not brief and result.get('role')=='worker' and 'active_jobs' in result:
            self._status_sample=(time.monotonic(),{j['id']:j for j in result['active_jobs'] or []})
        return result

    def capabilities(self):
        """Small aggregate view intended for project adapters."""
        result = self.request('/capabilities')
        if result.get('protocol') != PROTOCOL:
            raise RemoteError("Worker protocol differs: " + self.url)
        # Existing services can retain legacy bindings for accepted jobs while
        # callers use the same canonical view before the next idle reload.
        for field in ('tags', 'tools'):
            if field in result:
                result[field] = canonical_capabilities(result[field])
        self._capabilities = result.get('capabilities', getattr(self, '_capabilities', None) or [])
        return result

    def control(self, **changes):
        return self.request("/control", changes)

    def restart(self, node=None, *, wait=False, wait_timeout=None):
        started = time.monotonic()
        while True:
            try:
                return self.request("/restart", {"node": node} if node else {})
            except RemoteError as error:
                if not wait or getattr(error, 'status_code', None) != 409:
                    raise
                if wait_timeout is not None and time.monotonic()-started >= wait_timeout:
                    raise RemoteError('Restart wait limit reached; current jobs continue') from None
                time.sleep(.25)

    def submit(self, task, *, inputs=None, files=None, job_id=None):
        identity = job_id or uuid.uuid4().hex
        # A stable id can be queried or resubmitted to THIS node after a lost reply.
        return self.submit_bundle(identity, task_bundle(task, inputs, files))

    def submit_bundle(self, identity, payload, *, callback=None):
        if callback:
            return self.request('/jobs/'+identity,payload,
                                extra_headers={'X-DCO-Callback':json.dumps(callback)})
        return self.request("/jobs/" + identity, payload)

    def resize(self, identity, cpus, *, memory_mb=None, base_memory_mb=None, memory_per_cpu_mb=None, memory_limit_mb=None):
        """Borrow/return resources at a safe boundary; use the actual returned grant."""
        modeled=base_memory_mb is not None or memory_per_cpu_mb is not None or memory_limit_mb is not None
        if memory_mb is not None or cpus==0 or modeled:
            if getattr(self, '_capabilities', None) is None:
                self._read_reconnecting(lambda: self.health(brief=True))
            if (memory_mb is not None and not ({'resize_memory','memory_resize'} & set(self._capabilities))
                    or cpus==0 and not ({'resize_auto','cpu_auto_resize'} & set(self._capabilities))
                    or modeled and not ({'resize_memory_model','memory_resize_model'} & set(self._capabilities))):
                state=self._read_reconnecting(lambda: self.status(identity,brief=True))
                return dict(id=identity,node=state.get('assigned_node',state.get('node')),
                            cpus=state['cpus'],memory_mb=state['memory_mb'],requested_cpus=cpus,supported=False)
        request = dict(cpus=cpus)
        if memory_mb is not None:
            request['memory_mb'] = memory_mb
        if modeled:
            request.update(base_memory_mb=base_memory_mb,memory_per_cpu_mb=memory_per_cpu_mb,memory_limit_mb=memory_limit_mb)
        action='lease' if (os.environ.get('REMOTE_COMPUTE_LEASE')=='1' and identity==os.environ.get('REMOTE_COMPUTE_JOB_ID')
                           and self.url==os.environ.get('REMOTE_COMPUTE_URL','').rstrip('/')) else 'resize'
        target=os.environ.get('REMOTE_COMPUTE_WORKER_JOB_ID',identity) if action=='lease' else identity
        try:
            return self.request('/jobs/'+target+'/'+action, request)
        except RemoteError as error:
            # Old centers can fail a RAM return before changing any grant.
            # During their natural drain, confirm this worker's original grant
            # instead of replaying a lease or failing already-prepared work.
            if (action!='lease' or modeled or memory_mb is None or cpus<1
                    or getattr(error,'status_code',None)!=409
                    or 'min() iterable argument is empty' not in str(error)):
                raise
            state=self._read_reconnecting(lambda: self.request('/jobs/'+target,brief=True))
            if (state.get('id')!=target or state.get('status')!='running'
                    or state.get('cpus')!=cpus or state.get('memory_mb',0)<memory_mb):
                raise error
            return dict(id=identity,node=state.get('node'),cpus=state['cpus'],memory_mb=state['memory_mb'],
                        requested_cpus=cpus,supported=False,resize_applied=False,
                        existing_grant_confirmed=True,resize_error=str(error))

    def nodes(self):
        if not self.operator:
            raise RemoteError('Device list is operator-only; use capabilities()')
        return self.request("/nodes")

    def admission(self, project, *, resource_share=1.0, queue_ratio=1.0, cpus=1, requires=(), node=None, max_cpus=None, job_id=None, accelerator=None):
        """Center-owned transport window; execution slots stay in the central queue."""
        if node is not None and not self.operator:
            raise RemoteError('node selection belongs to the DCO operator; use requires for capabilities')
        request=dict(project=project,resource_share=resource_share,queue_ratio=queue_ratio,cpus=cpus,requires=list(requires))
        if accelerator is not None:
            request['accelerator'] = accelerator
        if node:
            request['node']=node
        if max_cpus is not None:
            request['max_cpus'] = max_cpus
        if job_id is not None:request['job_id']=job_id
        # Both forms ask for advice; unlike a resource resize, neither changes a grant.
        if os.environ.get('REMOTE_COMPUTE_LEASE')=='1' and self.url==os.environ.get('REMOTE_COMPUTE_URL','').rstrip('/'):
            identity=os.environ.get('REMOTE_COMPUTE_WORKER_JOB_ID',os.environ['REMOTE_COMPUTE_JOB_ID'])
            return self._read_reconnecting(lambda:self.request('/jobs/'+identity+'/lease',dict(request,_operation='admission')))
        return self._read_reconnecting(lambda:self.request('/admission',request))

    def window(self, project, **options):
        from .admission import AdmissionWindow
        return AdmissionWindow(self,project,**options)

    def machine(self, node, *, cwd="~"):
        """Operate on a named worker through this same center connection."""
        if not self.operator:
            raise RemoteError('Named machine operations are operator-only')
        from .machine import Machine
        return Machine(self, node, cwd=cwd)

    def status(self, identity=None, *, brief=False):
        if brief and identity:
            stamp,active=self.__dict__.get('_status_sample',(0,{}))
            if time.monotonic()-stamp<=2 and identity in active:
                return dict(active[identity])
        return self.request("/jobs" + ("/" + identity if identity else ""), brief=brief)

    def cancel(self, identity):
        return self.request("/jobs/" + identity + "/cancel", {})

    def logs(self, identity):
        return self.request("/jobs/" + identity + "/logs")

    def read_job_file(self, identity, path):
        return self.request('/jobs/' + identity + '/files/' + quote(str(path), safe='/'), binary=True)

    def read_job_file_changed(self, identity, path, version=None):
        headers={'X-DCO-File-Version':'1'}
        if version:headers['If-None-Match']=version
        data=self.request('/jobs/'+identity+'/files/'+quote(str(path),safe='/'),binary=True,extra_headers=headers)
        changed=self.connections.response_status!=304
        return dict(version=self.connections.etag,data=data if changed else None,changed=changed)

    def write_job_file(self, identity, path, data):
        if isinstance(data, str):
            data = data.encode('utf-8')
        return self.request('/jobs/' + identity + '/files/' + quote(str(path), safe='/'), data)

    def delete(self, identity):
        return self.request("/jobs/" + identity, method="DELETE")

    def _read_reconnecting(self, read, *, deadline=None):
        """Keep an existing observation across a bounded transport outage."""
        expires=time.monotonic()+max(120,self.timeout)
        if deadline is not None:expires=min(expires,deadline)
        delay=.2
        while True:
            try:
                return read()
            except RemoteError as error:
                transient=(getattr(error,'status_code',None) in (502,503)
                           or getattr(error,'transport_retryable',False))
                remaining=expires-time.monotonic()
                if not transient or remaining<=0:
                    raise
                time.sleep(min(delay,remaining))
                delay=min(5,delay*2)

    def wait(self, identity, *, poll=0.15, wait_timeout=None):
        started = time.monotonic()
        deadline=None if wait_timeout is None else started+wait_timeout
        while True:
            result = self._read_reconnecting(lambda:self.status(identity,brief=True),deadline=deadline)
            if result["status"] in TERMINAL:
                result = self._read_reconnecting(lambda:self.status(identity),deadline=deadline)
                result["url"] = self.url
                return result
            if wait_timeout is not None and time.monotonic() - started >= wait_timeout:
                raise RemoteError(f"Wait limit reached; query {self.url}/jobs/{identity}")
            time.sleep(poll)

    def artifacts(self, identity):
        # A center may first need to retrieve the complete remote ZIP. Keep
        # that header wait and its connections separate from status/control.
        backend=self
        if self.endpoint.scheme in {'http','https'}:
            backend=self.__dict__.get('_result_client')
            if backend is None:
                backend=self.__dict__.setdefault('_result_client',Client(self.url,self.token,timeout=max(120,self.timeout),ca_file=getattr(self,'ca_file',None),operator=self.operator,operator_token=self.operator_token,pool_timeout=getattr(self,'pool_timeout',None)))
            backend.timeout=max(120,self.timeout)
        return self._read_reconnecting(lambda:backend.request("/jobs/"+identity+"/artifacts",binary=True))

    def fetch(self, identity, output_dir=None, *, delete=True):
        raw = self.artifacts(identity)
        with zipfile.ZipFile(io.BytesIO(raw)) as archive:
            result = json.loads(archive.read("result.json"))
            names = [name for name in archive.namelist() if name.startswith("files/")]
            result["files"] = ({name[6:]: archive.read(name) for name in names} if output_dir is None
                               else sorted({name[6:] for name in names}))
        if output_dir is not None:
            unpack(raw, Path(output_dir))
            result["output_dir"] = str(Path(output_dir).resolve())
        result["url"] = self.url
        if delete:
            self.delete(identity)
        return result

    def run(self, task, *, inputs=None, files=None, output_dir=None, keep=False, on_submit=None, job_id=None):
        identity = job_id or uuid.uuid4().hex
        self.submit(task, inputs=inputs, files=files, job_id=identity)
        if on_submit:
            on_submit(self, identity)
        try:
            self.wait(identity)
        except KeyboardInterrupt:
            self.cancel(identity)
            raise
        return self.fetch(identity, output_dir, delete=not keep)

    def sync(self, root, patterns=("**/*",)):
        return self.sync_files(tree(root, patterns))

    def has_asset(self, identity, *, job_id=None):
        """Read immutable asset presence without preparing or uploading it."""
        options = {'extra_headers':{'X-DCO-Asset-Job':job_id}} if job_id else {}
        return self._read_reconnecting(lambda:self.request('/assets/'+identity, **options))['exists']

    def sync_files(self, files, *, content_id=None, job_id=None):
        """Reuse a caller's immutable content identity before preparing a ZIP.

        content_id must cover relative names, bytes and executable modes. files
        may be a zero-argument factory; it runs only when the asset is missing.
        The small per-client index stores IDs, never source files or archives.
        job_id holds transient inputs until that original request is accepted.
        Abandoned preparation can release the hold with release_assets(job_id).
        """
        if job_id is not None and (not isinstance(job_id,str) or not re.fullmatch(r'[a-f0-9]{32}',job_id)):
            raise ValueError('Job id requires 32 lowercase hex characters')
        options = {'extra_headers':{'X-DCO-Asset-Job':job_id}} if job_id else {}
        transfer = threading.Lock()
        if content_id is not None:
            if not isinstance(content_id,str) or not content_id:
                raise ValueError('content_id must be a nonempty immutable content identifier')
            with self._asset_lock:
                transfer = self._asset_transfers.setdefault(content_id, transfer)
        # Share preparation/upload for one immutable input. Each waiting job
        # still checks presence with its own handoff; unrelated inputs proceed.
        with transfer:
            known = None
            if content_id is not None:
                with self._asset_lock:
                    known=self._asset_ids.get(content_id)
                if known and self.has_asset(known, job_id=job_id):
                    with self._asset_lock:
                        if content_id in self._asset_ids:self._asset_ids.move_to_end(content_id)
                    return known
            payload = pack(files() if callable(files) else files)
            identity = hashlib.sha256(payload).hexdigest()
            if identity == known or not self.has_asset(identity, job_id=job_id):
                self.request("/assets/" + identity, payload, **options)
            if content_id is not None:
                with self._asset_lock:
                    self._asset_ids[content_id]=identity
                    self._asset_ids.move_to_end(content_id)
                    while len(self._asset_ids)>128:self._asset_ids.popitem(last=False)
            return identity

    def release_assets(self, job_id):
        """Release an abandoned input handoff; accepted task references remain."""
        if not isinstance(job_id,str) or not re.fullmatch(r'[a-f0-9]{32}',job_id):
            raise ValueError('Job id requires 32 lowercase hex characters')
        try:
            return self.request('/jobs/'+job_id+'/asset-handoff', method='DELETE')
        except RemoteError as error:
            if getattr(error,'status_code',None) != 404:
                raise
            # Older centers retain their original shutdown-only cache policy.
            return {'id':job_id}


@dataclass
class Node:
    name: str
    client: Client
    weight: float = 1.0
    limit: int = 0
    enabled: bool = True
    health: dict = field(default_factory=dict)
    error: str = ""
    last_connected_at: float = field(default_factory=time.monotonic)
    detached: bool = False
    reconnect_attempts: int = 0
    reconnect_after: float = 0.0
    connection_generation: int = 0
    reserved: int = 0
    external: int = 0
    memory_limit_mb: int = 0
    reserved_memory_mb: int = 0
    external_memory_mb: float = 0
    durations: dict = field(default_factory=dict)
    completed: int = 0
    failed: int = 0
    asset_lock: object = field(default_factory=threading.Lock)
    asset_session: object = None
    known_assets: set = field(default_factory=set)
    known_jobs: set = field(default_factory=set)
    live_jobs: set = field(default_factory=set)
    host_ip: str = ""
    aliases: list = field(default_factory=list)
    accelerator_reservations: dict = field(default_factory=dict)
    external_accelerator_reservations: list = field(default_factory=list)
    network_checked_at: float = 0.0
    network_samples: list = field(default_factory=list)
    network: dict = field(default_factory=dict)

    @property
    def capacity(self):
        available = self.health.get("cpus", 0)
        return min(available, self.limit) if self.limit else available

    @property
    def memory_capacity(self):
        value = self.health.get("memory_capacity_mb", 0)
        return min(value, self.memory_limit_mb) if self.memory_limit_mb else value

    @property
    def memory_free(self):
        if self.health.get('memory_accounting')=='rss':
            return min(self.memory_capacity-self.health.get('memory_reserved_mb',0)-self.memory_unreported,
                       self.health.get('memory_available_mb',0)-self.memory_unreported)
        unreported = max(0, self.reserved_memory_mb - self.health.get("memory_reserved_mb", 0))
        return min(self.memory_capacity - self.reserved_memory_mb - self.external_memory_mb,
                   self.health.get("memory_available_mb", 0) - unreported)

    @property
    def memory_unreported(self):
        reported=sum(job.get('memory_mb',0) for job in self.health.get('active_jobs',()) if job['id'] in self.live_jobs)
        return max(0,self.reserved_memory_mb-reported)

    @property
    def memory_accounted(self):
        if self.health.get('memory_accounting')=='rss':
            return self.health.get('memory_reserved_mb',0)+self.memory_unreported
        return self.reserved_memory_mb+self.external_memory_mb

    def accelerator_units(self):
        units=[]
        for profile_index, profile in enumerate(self.health.get('accelerators', ())):
            ids=profile.get('ids') or [str(index) for index in range(profile.get('count', 0))]
            for index in range(profile.get('count', 0)):
                units.append((profile_index,index,profile.get('kind','any'),profile.get('memory_mb',0),
                              tuple(profile.get('tags',())),ids[index] if index < len(ids) else str(index)))
        return units

    def _select_accelerator(self, requirement, extra_used=(), *, include_reserved=True):
        """Return provider-local accelerator units for a request.

        Selection is kept in the scheduler ledger.  Project clients only send
        semantic requirements; ids are used after dispatch and never leave the
        operator view.
        """
        requirement=normalize_accelerator_requirement(requirement)
        if requirement is None:
            return []
        used=set()
        if include_reserved:
            used.update(unit[:2] for items in self.accelerator_reservations.values() for unit in items)
            used.update(unit[:2] for unit in self.external_accelerator_reservations)
        used.update(unit[:2] for unit in extra_used)
        selected=[]
        needed_tags=set(requirement['requires'])
        for unit in self.accelerator_units():
            if unit[:2] in used:
                continue
            if (requirement['kind'] in ('any',unit[2])
                    and unit[3] >= requirement['memory_mb']
                    and needed_tags.issubset(unit[4])):
                selected.append(unit)
                if len(selected) == requirement['count']:
                    return selected
        return None

    def refresh_external_accelerators(self, jobs):
        """Rebuild reservations for work accepted outside this scheduler."""
        self.external_accelerator_reservations=[]
        for job in jobs or ():
            identity=job.get('id')
            # Reattachment can own CPU credits before a GPU profile returns.
            # Keep that GPU external until a real local reservation exists;
            # completed known jobs still ignore delayed running snapshots.
            if (job.get('status') != 'running' or identity in self.accelerator_reservations
                    or (identity in self.known_jobs and identity not in self.live_jobs)):
                continue
            selected=self._select_accelerator(job.get('accelerator'))
            if selected:
                self.external_accelerator_reservations.extend(selected)

    def can_match_accelerator(self, requirement):
        return self._select_accelerator(requirement) is not None

    def supports_accelerator(self, requirement):
        """Whether the provider advertises a matching profile, regardless of load."""
        return self._select_accelerator(requirement, include_reserved=False) is not None

    def reserve_accelerator(self, requirement, job_id):
        selected=self._select_accelerator(requirement)
        if selected is not None:
            self.accelerator_reservations[job_id]=selected
        return selected

    def release_accelerator(self, job_id):
        self.accelerator_reservations.pop(job_id, None)


class Pool:
    """Shared admission across concurrent batches; workers enforce the final budget.

    CPU pressure chooses the least-busy host; adaptive seeds near ties with mixed
    throughput, then learns same-label elapsed time. Accepted work stays put.
    """
    POLICIES = {"adaptive", "balanced", "round_robin"}

    def __init__(self, nodes, *, policy="adaptive", refresh=2, on_detach=None, on_reconnect=None):
        self.condition = threading.Condition(threading.RLock())
        self.refresh_lock = threading.Lock()
        self.on_detach = on_detach
        self.on_reconnect = on_reconnect
        self.nodes = []
        self.policy = policy
        self.sample_seconds = refresh
        self.refresh_seconds = min(refresh,.2)
        self.closed = False
        self.rotation = 0
        self.active = {}
        self.stop_event = threading.Event()
        if policy not in self.POLICIES:
            raise ValueError("Unknown dispatch policy: " + policy)
        for index, entry in enumerate(nodes):
            if isinstance(entry, Client):
                node = Node(f"node{index + 1}", entry)
            else:
                node = Node(entry.get("name", f"node{index + 1}"),
                            entry.get('client') or Client(entry["url"], entry.get("token", ""),timeout=entry.get('timeout',10),ca_file=entry.get('ca_file')),
                            weight=entry.get("weight", 1.0), limit=entry.get("limit", 0),
                            memory_limit_mb=entry.get("memory_limit_mb", 0),
                            enabled=entry.get("enabled", True))
                node.host_ip = entry.get("host_ip", "")
                node.aliases = entry.get('aliases',[])
                retry = entry.get('reconnect')
                if retry is not None and not node.enabled:
                    attempts, after = (retry.get('attempts'), retry.get('after')) if isinstance(retry,dict) else (None,None)
                    if (type(attempts) is not int or not 0 <= attempts <= 5
                            or isinstance(after, bool) or not isinstance(after, (int, float))
                            or not 0 <= after < float('inf')):
                        raise ValueError('Node reconnect requires attempts 0..5 and a nonnegative timestamp')
                    node.detached = True
                    node.reconnect_attempts, node.reconnect_after = attempts, after
            self._validate_node(node)
            node.error = 'Checking worker'
            self.nodes.append(node)
        if not self.nodes or len({x.name for x in self.nodes}) != len(self.nodes):
            raise ValueError("Configure nodes with unique names")
        if len({x.client.url for x in self.nodes}) != len(self.nodes):
            raise ValueError("Configure each worker URL once")
        self.probes=ThreadPoolExecutor(thread_name_prefix="node-sample")
        self.probe_futures={}
        self.refresh(timeout=1)
        self.monitor = threading.Thread(target=self._monitor, name="compute-nodes", daemon=True)
        self.monitor.start()

    @staticmethod
    def _validate_node(node):
        address=getattr(node,'host_ip','')
        if not isinstance(address,str):
            raise ValueError('Node host_ip must be an IP address or empty')
        if address:
            ipaddress.ip_address(address)
        aliases=getattr(node,'aliases',[])
        if not isinstance(aliases,list) or any(not isinstance(alias,str) or not alias for alias in aliases):
            raise ValueError('Node aliases must be a list of previous node names')
        if (not isinstance(node.weight, (int, float)) or not 0 < node.weight < float("inf")
                or type(node.limit) is not int or node.limit < 0 or not isinstance(node.enabled, bool)
                or type(node.memory_limit_mb) is not int or node.memory_limit_mb < 0):
            raise ValueError("Node weight must be positive; limit is 0/auto or a positive integer")
    def _probe(self, node, full=False, reconnect=False, generation=None):
        client=node.client
        generation = node.connection_generation if generation is None else generation
        try:
            sampled=not full and 'health_sample' in node.health.get('capabilities',[])
            value=client.health(sample=True) if sampled else client.health()
            if sampled and any(value.get(key)!=node.health.get(key) for key in ('pid','configuration_revision')):
                value=client.health()
            error=''
        except RemoteError as exc:
            value,error={},str(exc)
        self._sample_network(node)
        with self.condition:
            if client is not node.client or generation != node.connection_generation:return
            node.sampled_at=time.monotonic()
            node.error=error
            if not error:
                previous_name = node.health.get('node')
                if reconnect and previous_name and value.get('node') != previous_name:
                    node.error = 'Reconnect worker identity differs from the accepted endpoint'
                    return
                node.last_connected_at=time.monotonic()
                node.health={**node.health,**value}
                jobs=value.get('active_jobs') or []
                running=[job for job in jobs if job['status']=='running']
                node.external=sum(job['cpus'] for job in running if job['id'] not in node.known_jobs)
                node.refresh_external_accelerators(running)
                owned_memory=sum(((job.get('rss_mb') or job['memory_mb']) if value.get('memory_accounting')=='rss' else
                                  max(job['memory_mb'],job.get('rss_mb',0))) for job in running if job['id'] in node.known_jobs)
                node.external_memory_mb=max(0,value.get('memory_reserved_mb',0)-owned_memory)
                node.known_jobs.intersection_update({job['id'] for job in jobs}|node.live_jobs)
                if reconnect and node.detached:
                    node.enabled, node.detached = True, False
                    node.reconnect_attempts, node.reconnect_after = 0, 0.0
            self.condition.notify_all()
        if reconnect:self._save_reconnect(node)

    @staticmethod
    def _network_target(node):
        parsed=urlsplit(node.client.url)
        host=parsed.hostname
        if parsed.scheme == 'local' or not host or host in {'127.0.0.1','localhost','::1'}:
            return None
        return host, parsed.port or (443 if parsed.scheme == 'https' else 80)

    def _sample_network(self, node):
        """Take one bounded TCP reachability sample at most every 30 seconds."""
        now=time.monotonic()
        if now-node.network_checked_at < 30:
            return
        target=self._network_target(node)
        node.network_checked_at=now
        if target is None:
            node.network={"local": True}
            return
        started=time.perf_counter()
        success=False
        try:
            with socket.create_connection(target, timeout=min(1.5, max(.2, node.client.timeout))):
                success=True
        except OSError:
            pass
        elapsed=round((time.perf_counter()-started)*1000,1) if success else None
        node.network_samples=(node.network_samples+[(success,elapsed)])[-5:]
        attempts=len(node.network_samples)
        losses=sum(not ok for ok,_ in node.network_samples)
        valid=[rtt for ok,rtt in node.network_samples if ok and rtt is not None]
        node.network={"local": False, "rtt_ms": round(sum(valid)/len(valid),1) if valid else None,
                      "packet_loss_pct": round(100*losses/attempts,1), "samples": attempts,
                      "measured_at": time.time()}

    def refresh(self, *, monitor=False, nodes=None, timeout=None):
        # A slow/offline worker never stalls another worker's CPU or lease sample.
        selected = list(self.nodes if nodes is None else nodes)
        if nodes is None:
            # Disabled hosts still drain accepted jobs, then stop background
            # probes. Explicit maintenance refreshes keep their direct path.
            selected = [node for node in selected if not node.detached and
                        (node.enabled or node.live_jobs or node.reserved or node.external)]
        with self.refresh_lock:
            for node in selected:
                pending=self.probe_futures.get(node.name)
                interval=min(self.sample_seconds,.2) if node.live_jobs else self.sample_seconds
                if pending is not None and not pending.done():
                    if monitor or timeout is not None:continue
                    pending.result()
                if monitor and time.monotonic()-getattr(node,'sampled_at',0)<interval:continue
                self.probe_futures[node.name]=self.probes.submit(self._probe,node,not monitor)
            pending=[self.probe_futures[node.name] for node in selected]
        if not monitor:
            if timeout is not None:
                pending,_=wait_futures(pending,timeout=timeout)
            for future in pending:future.result()

    def _expire_unreachable(self, now=None):
        now=time.monotonic() if now is None else now
        removed=[]
        with self.condition:
            for node in self.nodes:
                if node.enabled and now-node.last_connected_at>=60:
                    node.enabled=False
                    node.detached=True
                    node.reconnect_attempts=0
                    node.reconnect_after=time.time()+60
                    if not node.error:node.error='No successful connection for 60 seconds'
                    removed.append(node)
            if removed:self.condition.notify_all()
        for node in removed:
            if self.on_detach:
                try:self.on_detach(node)
                except (OSError,ValueError) as error:
                    with self.condition:node.error+='; saving disabled configuration: '+str(error)
        return removed

    def _retry_detached(self, now=None):
        now = time.time() if now is None else now
        retry = []
        with self.refresh_lock:
            with self.condition:
                for node in self.nodes:
                    pending = self.probe_futures.get(node.name)
                    if (not node.detached or node.enabled or not node.reconnect_after
                            or now < node.reconnect_after or node.reconnect_attempts >= 5
                            or pending is not None and not pending.done()):
                        continue
                    node.reconnect_attempts += 1
                    node.reconnect_after = now + 60 if node.reconnect_attempts < 5 else 0.0
                    retry.append((node, node.connection_generation))
            # Persist the consumed attempt before starting its network request,
            # so a center restart cannot reset the five-attempt limit.
            retry = [(node, generation) for node, generation in retry if self._save_reconnect(node)]
            for node, generation in retry:
                self.probe_futures[node.name] = self.probes.submit(self._probe, node, True, True, generation)
        return [node for node, _ in retry]

    def _save_reconnect(self,node):
        if self.on_reconnect:
            try:self.on_reconnect(node)
            except (OSError,ValueError) as error:
                with self.condition:node.error+='; saving reconnect configuration: '+str(error)
                return False
        return True

    def _monitor(self):
        while not self.stop_event.wait(self.refresh_seconds):
            self._expire_unreachable()
            self._retry_detached()
            self.refresh(monitor=True)

    def configure(self, name=None, *, policy=None, **changes):
        connection_change=bool(set(changes)&{'enabled','url','ca_file'})
        with self.condition:
            if policy is not None:
                if policy not in self.POLICIES:
                    raise ValueError("Policy: adaptive, balanced, round_robin")
                self.policy = policy
            if name is not None:
                node = next(x for x in self.nodes if x.name == name)
                if set(changes) - {"weight", "limit", "enabled", "memory_limit_mb", "timeout", "host_ip", "url", "ca_file", "aliases"}:
                    raise ValueError("Node settings: weight, limit, enabled, memory_limit_mb, timeout, host_ip, url, ca_file, aliases")
                timeout=changes.pop('timeout',None)
                if timeout is not None and (isinstance(timeout,bool) or not isinstance(timeout,(int,float)) or not 0<timeout<float('inf')):
                    raise ValueError('Node timeout must be positive and finite')
                proposed = Node(node.name, node.client, **{
                    key: changes.get(key, getattr(node, key)) for key in ("weight", "limit", "enabled", "memory_limit_mb")})
                proposed.host_ip = changes.get('host_ip', getattr(node, 'host_ip', ''))
                proposed.aliases = changes.get('aliases',getattr(node,'aliases',[]))
                self._validate_node(proposed)
                address=changes.pop('url',node.client.url)
                ca_file=changes.pop('ca_file',getattr(node.client,'ca_file',None))
                if not isinstance(address,str) or urlsplit(address).scheme not in {'http','https','local'}:
                    raise ValueError('Node URL must use HTTP(S) or the private local endpoint')
                if any(other is not node and other.client.url==address.rstrip('/') for other in self.nodes):
                    raise ValueError('Configure each worker URL once')
                if address.rstrip('/')!=node.client.url or ca_file!=getattr(node.client,'ca_file',None):
                    node.client=Client(address,node.client.token,timeout=timeout or node.client.timeout,ca_file=ca_file)
                    node.connection_generation+=1
                    node.error='Node address changed; checking worker'
                for key, value in changes.items():
                    setattr(node, key, value)
                if 'enabled' in changes:
                    node.connection_generation+=1
                    node.detached=False
                    node.reconnect_attempts,node.reconnect_after=0,0.0
                    if changes['enabled']:node.last_connected_at=time.monotonic()
                if timeout is not None:
                    node.client.timeout=timeout
            self.condition.notify_all()
        if name is not None and connection_change:self._save_reconnect(node)

    def add(self, name, url, *, token="", weight=1.0, limit=0, timeout=10, host_ip="", ca_file=None, aliases=None):
        node = Node(name, Client(url, token,timeout=timeout,ca_file=ca_file), weight=weight, limit=limit)
        node.host_ip = host_ip
        node.aliases=[] if aliases is None else aliases
        self._validate_node(node)
        with self.condition:
            if any(n.name == name or n.client.url == node.client.url for n in self.nodes):
                raise ValueError("Node name or URL already exists")
            self.nodes.append(node)
        self.refresh(nodes=[node])

    def snapshot(self):
        with self.condition:
            return {"policy": self.policy,
                "reconnect_policy": {"offline_seconds":60,"interval_seconds":60,"attempts":5}, "nodes": [
                {"name": n.name, "url": n.client.url, "enabled": n.enabled,
                 "detached": n.detached,
                 **({'reconnect': {'attempts': n.reconnect_attempts, 'after': n.reconnect_after}} if n.detached else {}),
                 "host_ip": getattr(n,'host_ip','') or n.health.get('host_ip'),
                 "timeout": getattr(n.client,'timeout',10),
                 "ca_file":getattr(n.client,'ca_file',None),"aliases":getattr(n,'aliases',[]),
                 "capacity": n.capacity, "reserved": n.reserved, "external_cpus": n.external,
                 "used_cpus": n.health.get("used_cpus", 0), "queued": n.health.get("queued", 0),
                 "cpu_percent": n.health.get("cpu_percent"), "logical_cpus": n.health.get("logical_cpus"),
                 "benchmark": n.health.get("benchmark"), "benchmark_state": n.health.get("benchmark_state", "ready"), "asset_cache": n.health.get("asset_cache"),
                 "service": n.health.get("service"), "scratch_disk": n.health.get("scratch_disk"),
                 "memory_capacity_mb": n.memory_capacity, "memory_available_mb": max(0, int(n.memory_free)),
                 "memory_reserved_mb": n.health.get("memory_reserved_mb", 0),
                 "reserved_memory_mb": n.reserved_memory_mb, "memory_limit_mb": n.memory_limit_mb,
                 "memory_total_mb": n.health.get("memory_total_mb"),
                 "host_memory_available_mb": n.health.get("host_memory_available_mb"),
                 "machine_id": n.health.get("machine_id", n.client.url), "rss_mb": n.health.get("rss_mb", 0),
                 "paused": n.health.get("paused", False), "tags": n.health.get("tags", []),
                 "capabilities": n.health.get("capabilities", []), "home": n.health.get("home"),
                 "accelerators": n.health.get("accelerators", []),
                 "accelerator_reserved": sum(len(items) for items in n.accelerator_reservations.values()),
                 "accelerator_used": n.health.get("accelerator_used", 0),
                 "network": dict(getattr(n, 'network', {}) or {}),
                 "shared_paths": n.health.get("shared_paths", {}),
                 "active_jobs": n.health.get("active_jobs") or [],
                 "unavailable_shared": n.health.get("unavailable_shared", {}),
                 "tool_root": n.health.get("tool_root"),
                 **{key: n.health.get(key) for key in ('node', 'cpu_limit', 'temperature_c', 'peers', 'events', 'root', 'version')},
                 "weight": n.weight, "limit": n.limit, "completed": n.completed, "failed": n.failed,
                  "timing_labels": len(n.durations), "error": n.error} for n in self.nodes]}

    def _matching(self, task):
        cpus=task.get('cpus',1) or 1
        texts=[*task.get('env',{}).values(),task.get('cwd',''),
               *(arg for argv in task.get('steps',()) for arg in argv)]
        required_tools=set(canonical_capabilities(name for text in texts for name in
            re.findall(r'\$\{tool:([A-Za-z_][A-Za-z0-9_+.-]*)\}',text)))
        required_shared={name for text in texts for name in re.findall(r'\$\{shared:([\w-]+)\}',text)}
        required=canonical_capabilities(task.get('requires',()))
        return [n for n in self.nodes if (not task.get('node') or n.name==task['node'])
                and set(required).issubset(canonical_capabilities(n.health.get('tags',())))
                and required_tools.issubset(canonical_capabilities(
                    name for name,info in n.health.get('tools',{}).items() if info.get('available')))
                and required_shared.issubset(n.health.get('shared',()))
                and n.capacity>=cpus and n.memory_capacity>=task.get('memory_mb',256)
                and n.supports_accelerator(task.get('accelerator'))]

    def _acquire(self, task, *, block=True, cancel_event=None, cpu_limit=None, partial_cpus=True, job_id=None, node_headroom=None):
        if not isinstance(task, dict) or not isinstance(task.get("steps"), list) or any(
                not isinstance(argv, list) or not argv for argv in task["steps"]):
            raise ValueError("Task requires nonempty argv steps")
        automatic = task.get('cpus') == 0
        cpus = task.get("cpus", 1) or 1
        memory_mb = task.get("memory_mb", 256)
        label = task.get("label", "")
        with self.condition:
            while True:
                if self.closed:
                    raise RemoteError("Pool has closed")
                if cancel_event is not None and cancel_event.is_set():
                    return None
                matching = self._matching(task)
                if not matching:
                    raise RemoteError("No matching online node for task CPU count, tools, shared paths, node name and tags")
                def needed(node):
                    # Automatic jobs can start with a partial grant. Waiting for
                    # an entire target reserved idle CPUs behind long-running jobs.
                    return min(cpu_limit,node.capacity,task.get('max_cpus',node.capacity)) if automatic and cpu_limit and not partial_cpus else cpus
                def available(node):
                    return node.capacity-node.reserved-node.external-(node_headroom or {}).get(node.name,0)
                ready = [n for n in matching if not n.error and n.enabled and not n.health.get("paused")
                         and available(n) >= needed(n)
                         and n.memory_free >= memory_mb and self._host_memory_free(n) >= memory_mb
                         and n.health.get("queued", 0) == 0
                         and n.can_match_accelerator(task.get('accelerator'))]
                if ready:
                    if self.policy == "round_robin":
                        node = next((n for n in self.nodes[self.rotation:] + self.nodes[:self.rotation] if n in ready))
                        self.rotation = (self.nodes.index(node) + 1) % len(self.nodes)
                    else:
                        pressures = {n.name: self._cpu_pressure(n, cpus) for n in ready}
                        least = min(ready, key=lambda n: pressures[n.name])
                        logical = lambda n: n.health.get('logical_cpus') or n.capacity
                        # CPU load has priority. Below one logical thread of
                        # difference, retain speed learning and operator weights.
                        close = [n for n in ready if pressures[n.name] - pressures[least.name]
                                 < max(1 / logical(n), 1 / logical(least))]
                        priors = self._speed_priors(close,task.get('single_thread_weight',1.0))
                        observations = [n.durations[label]/priors[n.name] for n in close if label in n.durations]
                        typical = sum(observations) / len(observations) if observations else 1.0
                        def score(n):
                            speed = n.durations.get(label, typical * priors[n.name] * 0.8) / typical if self.policy == "adaptive" else priors[n.name]
                            return (speed / n.weight, pressures[n.name],
                                    (n.reserved+n.external+cpus)/n.capacity)
                        unexplored = [n for n in close if label not in n.durations] if self.policy == "adaptive" else []
                        node = min(unexplored or close, key=score)
                    if automatic:
                        cpus = available(node)
                        if cpu_limit is not None:
                            cpus = min(cpus, cpu_limit)
                        cpus = min(cpus, task.get('max_cpus',node.capacity))
                        task['cpus'] = cpus
                    reservation_id=job_id or f'local:{id(task)}'
                    if node.reserve_accelerator(task.get('accelerator'), reservation_id) is None:
                        continue
                    node.reserved += cpus
                    node.reserved_memory_mb += memory_mb
                    if job_id:
                        node.known_jobs.add(job_id)
                        node.live_jobs.add(job_id)
                    return node
                if not block:
                    return None
                self.condition.wait(0.2)

    def _speed_priors(self, nodes, single_thread_weight):
        """A neutral hint balances serial latency and aggregate throughput."""
        benchmarks = [n.health.get('benchmark') or {} for n in nodes]
        valid = lambda b: (b.get('name') == 'native-mixed-v4' and b.get('unit')=='Miter/s'
                           and b.get('score',0)>0 and b.get('threads',0)>0)
        # Incomparable/missing aggregate measurements use configured slot counts.
        # Hash/legacy scores are never treated as native mixed throughput.
        bulk = ([b['score']*n.capacity/b['threads'] for n,b in zip(nodes,benchmarks)]
                if all(valid(b) for b in benchmarks) and len({b.get('name') for b in benchmarks})==1 else [n.capacity for n in nodes])
        singles = [b.get('single') or {} for b in benchmarks]
        serial = [b['score'] for b in singles] if all(valid(b) for b in singles) and len({b.get('name') for b in singles})==1 else [1.0]*len(nodes)
        bulk_mean,serial_mean = sum(bulk)/len(nodes),sum(serial)/len(nodes)
        serial_fraction = single_thread_weight/(1.0+single_thread_weight)
        return {n.name:1/((total/bulk_mean)**(1-serial_fraction)*(single/serial_mean)**serial_fraction)
                for n,total,single in zip(nodes,bulk,serial)}

    def _cpu_pressure(self, node, cpus):
        machine = node.health.get('machine_id', node.client.url)
        siblings = [n for n in self.nodes if not n.error
                    and n.health.get('machine_id', n.client.url) == machine]
        logical = max(n.health.get('logical_cpus') or n.capacity for n in siblings)
        samples=[n.health.get('cpu_percent') for n in siblings if n.health.get('cpu_percent') is not None]
        committed = sum(n.reserved + n.external for n in siblings)
        if not samples:
            return (committed + cpus) / logical
        sampled = max(samples) * logical / 100
        reported = sum(n.health.get('used_cpus', 0) for n in siblings)
        # The sample already includes running work. Replace its reported grants
        # with today's ledger, including dispatches and resizes not sampled yet.
        return (max(0, sampled + committed - reported) + cpus) / logical

    def _host_memory_free(self, node):
        machine = node.health.get("machine_id", node.client.url)
        siblings = [n for n in self.nodes if n.health.get("machine_id", n.client.url) == machine]
        sampled = [n for n in siblings if not n.error]
        # A running task can return a lease while its host probe is failing.
        # Keep its existing grant, but offer no new RAM without a live sample.
        if not sampled:
            return 0
        # One physical memory pool per machine, even with several worker ports.
        physical = min(n.health.get("host_memory_available_mb", 0) - n.health.get("memory_headroom_mb",n.health.get("memory_reserve_mb", 0)) for n in sampled)
        # A failed probe does not release that sibling's outstanding reservations.
        unallocated = sum((n.memory_unreported+max(0,n.health.get('memory_reserved_mb',0)-n.health.get('rss_mb',0))
                           if n.health.get('memory_accounting')=='rss' else
                           max(0,n.reserved_memory_mb+n.external_memory_mb-n.health.get('rss_mb',0))) for n in siblings)
        return physical - unallocated

    def _release(self, node, task, result=None, *, job_id=None):
        label = task.get("label", "")
        with self.condition:
            node.release_accelerator(job_id or f'local:{id(task)}')
            node.reserved -= task.get("cpus", 1)
            node.reserved_memory_mb -= task.get("memory_mb", 256)
            node.live_jobs.discard(job_id)
            if result is None:
                # A lost connection can leave the worker running this job;
                # its next health snapshot must account for that work.
                node.known_jobs.discard(job_id)
            if result is not None:
                node.completed += 1
                if result["status"] == "succeeded":
                    measured = result["elapsed"]
                    previous = node.durations.pop(label, measured)
                    node.durations[label] = 0.7 * previous + 0.3 * measured
                    # Keep exact workload labels; unrelated sizes/seeds are not
                    # merged into a scheduling estimate. Old one-off labels expire.
                    while len(node.durations) > 256:
                        del node.durations[next(iter(node.durations))]
                else:
                    node.failed += 1
            self.condition.notify_all()

    def run(self, task, *, inputs=None, files=None, output_dir=None, keep=False, on_submit=None):
        identity = uuid.uuid4().hex
        node = self._acquire(task, job_id=identity)
        def submitted(client, job_id):
            with self.condition:
                self.active[(node.name, job_id)] = client
            if on_submit:
                on_submit(client, job_id)
        result = None
        try:
            result = node.client.run(task, inputs=inputs, files=files, output_dir=output_dir,
                                     keep=keep, on_submit=submitted, job_id=identity)
            result["assigned_node"] = node.name
            return result
        finally:
            with self.condition:
                self.active.pop((node.name, identity), None)
                self._release(node, task, result, job_id=identity)

    def map(self, tasks, *, inputs=None, output_root=None, keep=False):
        tasks = list(tasks)
        if not tasks:
            return []
        results = [None] * len(tasks)
        def execute(index):
            results[index] = self.run(tasks[index], inputs=inputs,
                                      output_dir=Path(output_root) / str(index) if output_root else None, keep=keep)
        ordered = sorted(range(len(tasks)), key=lambda i: -tasks[i].get("priority", 0))
        threads = min(len(tasks), max(1, sum(n.capacity for n in self.nodes)))
        with ThreadPoolExecutor(max_workers=threads) as executor:
            list(executor.map(execute, ordered))
        return results

    def sync(self, root, patterns=("**/*",)):
        identities = {n.client.sync(root, patterns) for n in self.nodes if n.enabled and not n.error}
        if len(identities) != 1:
            raise RemoteError("Asset synchronization needs at least one online node and one content id")
        return identities.pop()

    def close(self, *, cancel=False):
        with self.condition:
            self.closed = True
            self.condition.notify_all()
            active = list(self.active.items())
        if cancel:
            for (_, identity), client in active:
                client.cancel(identity)
        self.stop_event.set()
        self.monitor.join()
        self.probes.shutdown(wait=True)

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.close()
