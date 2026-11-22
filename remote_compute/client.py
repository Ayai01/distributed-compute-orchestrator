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
import shutil
import socket
import ssl
import stat
import tempfile
import threading
import time
from urllib.parse import quote, urlencode
from urllib.parse import urlsplit
import uuid
import weakref
import zipfile

from .bundle import MAX_BODY, PROTOCOL, TERMINAL, json_bytes, pack, task_bundle, tree, unpack, native_path
from .policy import (normalize_accelerator_requirement, select_accelerator_units, canonical_capabilities,
                     MAX_STATUS_BATCH, validate_ready_tasks, validate_status_ids, validate_platform)
from .auth import lan_endpoint
from .allocation import STARTUP_CPU_FLOOR, elastic_cpus, cpu_capacity, startup_cpus
from .build_refs import provider_abi
from .relay_bulk import MIN_BULK_BYTES, load_relay_bulk_budget
from .packages import package_names, provider_has_package
from .ramdisk import validate_executable_ram_mb


from .runtimes import (supports_python, execution_bindings, execution_tool_set, capability_set,
                       python_accelerator, supports_native_cache, provider_tags)


class RemoteError(RuntimeError):
    def __init__(self, *args):
        super().__init__(*args)
        self.status_code = None
        self.transport_retryable = False


class PoolBusy(RemoteError):
    """The local connection queue timed out before any request was sent."""


class RequestCancelled(RemoteError):
    """A caller stopped its transfer or local wait, preserving accepted work."""


RESOURCE_TRANSPORT_RETAINED='Center resource transport interrupted; existing grant retained'
_RELAY_BULK_BUDGET = load_relay_bulk_budget()
SHORT_DISPATCH_BYTES = 256 * 1024
# Waits after each unsuccessful probe. A relay recovering in the first
# minute has two opportunities before falling back to minute spacing.
RELAY_RECONNECT_INTERVALS = (10, 25, 60, 60, 60, 60)
DIRECT_RECONNECT_INTERVALS = (60, 60, 60, 60)
RECONNECT_BACKOFF_MAX = 300


def _reconnect_intervals(client):
    endpoint = getattr(client, 'endpoint', None)
    if endpoint and _RELAY_BULK_BUDGET and _RELAY_BULK_BUDGET.matches(endpoint):
        return RELAY_RECONNECT_INTERVALS
    return DIRECT_RECONNECT_INTERVALS


def _check_cancelled(cancel):
    if cancel is not None and cancel():
        raise RequestCancelled('Transfer cancelled before task submission')


def _pause(seconds, check=None):
    if check is None:
        time.sleep(seconds)
        return
    deadline = time.monotonic() + seconds
    while True:
        check()
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return
        time.sleep(min(.1, remaining))


class _UploadBody:
    def __init__(self, data, cancel, budget=None):
        self.data = memoryview(data)
        self.offset = 0
        self.cancel = cancel
        self.budget = budget

    def read(self, size=-1):
        _check_cancelled(self.cancel)
        stop = len(self.data) if size < 0 else min(len(self.data), self.offset + size)
        block = self.data[self.offset:stop]
        if self.budget is not None:
            self.budget.pace(len(block), lambda: _check_cancelled(self.cancel),
                             remaining=len(self.data) - self.offset)
        self.offset = stop
        return block


def _short_dispatch_request(path, data, method):
    """Asset preparation shares the bounded short-job slot with its final POST."""
    verb = method or ('GET' if data is None else 'POST')
    if verb == 'GET' and re.fullmatch(r'/assets/[0-9a-f]{64}/upload', path):
        return True
    if re.fullmatch(r'/assets/[0-9a-f]{64}', path):
        return ((verb == 'GET' and data is None) or
                (verb == 'POST' and isinstance(data, bytes) and len(data) < MIN_BULK_BYTES))
    if re.fullmatch(r'/jobs/[0-9a-f]{32}/asset-handoff', path):
        return verb in ('POST', 'DELETE')
    if verb == 'DELETE' and re.fullmatch(r'/jobs/[0-9a-f]{32}', path):
        return True
    return (verb == 'POST' and isinstance(data, bytes) and len(data) < MIN_BULK_BYTES
            and re.fullmatch(r'/jobs/[0-9a-f]{32}', path) is not None)


class _ConnectionPool:
    def __init__(self):
        self.idle = []
        self.condition = threading.Condition()
        self.available = 8
        self.bulk_limit = 7
        self.bulk_active = 0
        self.waiting = deque()

    def set_bulk_limit(self, limit):
        if self.bulk_limit != limit:
            with self.condition:
                self.bulk_limit = limit
                self.condition.notify_all()

    def acquire(self, timeout, *, bulk=False, short_job=False, cancel=None):
        ticket = (object(), 'short_job' if short_job else bulk)

        def eligible(kind):
            if kind == 'short_job':
                return self.available > 1
            return not kind or self.available > 1 and self.bulk_active < self.bulk_limit
        deadline = None if timeout is None else time.monotonic() + timeout
        with self.condition:
            self.waiting.append(ticket)
        try:
            while True:
                # Cancellation may inspect the coordinator. Check after
                # dropping the transport condition.
                _check_cancelled(cancel)
                with self.condition:
                    # Leave one slot for health, status and resource grants.
                    if self.available and next((entry for entry in self.waiting if eligible(entry[1])), None) is ticket:
                        self.available -= 1
                        self.bulk_active += int(bulk)
                        return True
                    remaining = None if deadline is None else deadline - time.monotonic()
                    if remaining is not None and remaining <= 0:
                        return False
                    wait_for=remaining
                    if cancel is not None:wait_for=.1 if remaining is None else min(.1,remaining)
                    self.condition.wait(wait_for)
        finally:
            with self.condition:
                self.waiting.remove(ticket)
                self.condition.notify_all()

    def take_idle(self):
        with self.condition:
            return self.idle.pop() if self.idle else None

    def release(self, *, bulk=False, connection=None):
        with self.condition:
            if connection is not None:
                self.idle.append(connection)
            self.available += 1
            self.bulk_active -= int(bulk)
            self.condition.notify_all()

    def __del__(self):
        for connection in self.idle:
            connection.close()


class Client:
    def __init__(self, url="http://127.0.0.1:26133", token="", *, timeout=None, ca_file=None, operator=False, operator_token=None, pool_timeout=None, lease_timeout=None, state_dir=None, relay_url=None, peer_id=None):
        self.url, self.token, self.operator = url.rstrip("/"), token, bool(operator)
        if not self.token and urlsplit(self.url).scheme in {'http','https'}:
            from .auth import loopback_endpoint
            if loopback_endpoint(urlsplit(self.url).hostname or ''):
                registration=Path(__file__).resolve().parent.parent/'.local/client.json'
                try:
                    registered=json.loads(registration.read_text(encoding='utf-8'))
                except FileNotFoundError:
                    registered={}
                if registered.get('url')==self.url:self.token=registered.get('token','')
        self.relay_url = relay_url.rstrip('/') if relay_url else None
        self.peer_id = peer_id
        self._direct_retry_after = 0.0
        if self.relay_url:
            from .auth import node_url
            self.relay_url = node_url(self.relay_url)
            if not self.token and not operator_token:
                raise ValueError('A relay requires the destination device credential')
        # One installed SDK owns its durable client index across caller cwds.
        # Explicit roots let isolated clients share the same ownership domain.
        self.state_dir = native_path(Path(state_dir).expanduser().resolve() if state_dir is not None
                                     else Path(__file__).resolve().parent.parent / '.local/dco-client', force=True)
        self.operator_token = operator_token if self.operator else None
        if timeout is None:
            timeout = (float(os.environ.get('REMOTE_COMPUTE_TIMEOUT',10))
                       if self.url==os.environ.get('REMOTE_COMPUTE_URL','').rstrip('/') else 10)
        self.timeout = timeout
        self.lease_timeout = max(timeout, 130) if lease_timeout is None else lease_timeout
        self.pool_timeout = pool_timeout
        self._endpoint = urlsplit(self.url)
        self.connections = threading.local()
        self._capabilities = None
        self._asset_ids = OrderedDict()
        self._asset_lock = threading.Lock()
        self._asset_transfers = weakref.WeakValueDictionary()
        self.ca_file=ca_file or (os.environ.get('REMOTE_COMPUTE_CA') if self.url==os.environ.get('REMOTE_COMPUTE_URL','').rstrip('/') else None)
        self.ssl_context=ssl.create_default_context(cafile=self.ca_file) if self.ca_file else None

    @property
    def endpoint(self):
        return getattr(self.connections, 'route', self._endpoint)

    def request(self, path, data=None, *, method=None, binary=False, brief=False, extra_headers=None, cancel=None, _output=None):
        options=dict(method=method,binary=binary,brief=brief,extra_headers=extra_headers,cancel=cancel,_output=_output)
        verb=method or ('POST' if data is not None else 'GET')
        job_upload=verb=='POST' and isinstance(data,bytes) and bool(re.fullmatch(r'/jobs/[0-9a-f]{32}',path))
        replayable=(verb=='GET' or verb=='DELETE' or job_upload or verb=='POST' and (
            path in ('/admission','/jobs/status') or bool(re.fullmatch(r'/assets/[0-9a-f]{64}|/jobs/[0-9a-f]{32}/cancel',path))))
        previous=getattr(self.connections,'route',None)
        errors=[]
        try:
            routes=(self.relay_url,) if self.relay_url and time.monotonic()<self._direct_retry_after else (self.url,self.relay_url)
            for route in routes:
                if not route:continue
                self.connections.route=urlsplit(route)
                try:
                    if errors and job_upload:
                        try:saved=self._routed_request(path,cancel=cancel)
                        except RemoteError as observed:
                            if observed.status_code!=404:raise
                        else:
                            if saved.get('id')!=path.rsplit('/',1)[-1] or saved.get('request_sha256')!=hashlib.sha256(data).hexdigest():
                                conflict=RemoteError('Original job request differs: '+path.rsplit('/',1)[-1]);conflict.status_code=409
                                raise conflict
                            return json_bytes(saved) if binary else saved
                    return self._routed_request(path,data,**options)
                except RemoteError as error:
                    if not error.transport_retryable or (not replayable and getattr(error,'request_may_have_been_sent',True)):
                        raise
                    if route==self.url and self.relay_url:self._direct_retry_after=time.monotonic()+15
                    errors.append(error)
            error=errors[-1]
            error.route_errors=tuple(errors)
            raise error
        finally:
            if previous is None:self.connections.__dict__.pop('route',None)
            else:self.connections.route=previous

    def download(self, path, destination, *, cancel=None):
        """Stream a binary GET to an owned file, retaining ETag/range recovery."""
        destination=Path(destination)
        with destination.open('w+b') as output:
            self.request(path,binary=True,cancel=cancel,_output=output)
        return destination

    def _routed_request(self, path, data=None, *, method=None, binary=False, brief=False, extra_headers=None, cancel=None, _output=None):
        authority = self.endpoint.geturl()
        relayed = _RELAY_BULK_BUDGET is not None and _RELAY_BULK_BUDGET.matches(self.endpoint)
        artifact_get = (binary and data is None and method in (None, 'GET')
                        and re.fullmatch(r'/jobs/[0-9a-f]{32}/(?:artifacts|files/.+)', path))
        retirement = method == 'DELETE' and re.fullmatch(r'/jobs/[0-9a-f]{32}', path)
        pools=self.__dict__.setdefault('_transports',{})
        if artifact_get:
            # Results and input staging have independent connection slots.
            pool = self.__dict__.setdefault('_artifact_transports', {}).setdefault(authority, _ConnectionPool())
        elif retirement:
            pool = self.__dict__.setdefault('_retirement_transports', {}).setdefault(authority, _ConnectionPool())
        else:
            pool=(self.__dict__.setdefault('_transport',_ConnectionPool()) if authority==self.url
                  else pools.setdefault(authority,_ConnectionPool()))
        pool.set_bulk_limit(getattr(self, 'bulk_limit', 7))
        # Local FIFO backpressure is independent of a socket's network timeout.
        # A busy client waits for its bounded slot without failing accepted work.
        # Cache lookups/handoffs and collection cleanup may wait behind disk I/O,
        # even with tiny JSON bodies. They must leave the control slot available.
        bulk = (isinstance(data, bytes) or binary or path.startswith('/assets/')
                or path.endswith(('/asset-handoff', '/logs'))
                or retirement)
        # A near-ready task can acquire its pins, look up assets and send a
        # small missing archive without waiting for a multi-MB result stream.
        short_job = (relayed and not artifact_get and not retirement
                     and _short_dispatch_request(path, data, method))
        bulk = bulk and not short_job
        if not pool.acquire(getattr(self, 'pool_timeout', None), bulk=bulk, short_job=short_job, cancel=cancel):
            raise PoolBusy(f'{self.url}{path}: connection pool is busy')
        try:
            # Old live instances may still own a per-thread cached connection.
            previous=getattr(self.connections,'connection',None)
            self.connections.connection=None
            if previous is not None:previous.close()
            self.connections.owner=None
            connection=pool.take_idle()
            if connection is not None and connection.sock is not None:
                try:
                    if select.select([connection.sock],[],[],0)[0]:connection.close()
                except (OSError,ValueError):connection.close()
            self.connections.connection=connection
            return self._request(path,data,method=method,binary=binary,brief=brief,extra_headers=extra_headers,cancel=cancel,_output=_output)
        except RequestCancelled:
            connection=getattr(self.connections,'connection',None)
            if connection is not None:connection.close()
            self.connections.connection=None
            raise
        finally:
            connection=getattr(self.connections,'connection',None)
            self.connections.connection=None
            pool.release(bulk=bulk, connection=connection)

    def _confirm_asset_publication(self, path, extra_headers, cancel):
        """Give a fully sent archive time to publish before replaying its body."""
        identity = path.rsplit('/', 1)[-1]
        misses = 0
        for delay in (0, .25, 1):
            if delay:
                time.sleep(delay)
            _check_cancelled(cancel)
            try:
                receipt = self._request(path, method='GET', extra_headers=extra_headers, cancel=cancel, _asset_probe=True)
            except RequestCancelled:
                raise
            except RemoteError:
                misses = 0
                continue
            if receipt.get('id') == identity and receipt.get('exists') is True:
                return receipt
            misses = misses + 1 if receipt.get('id') == identity and receipt.get('exists') is False else 0
        if misses >= 2:
            return dict(id=identity, exists=False)
        return None

    def _request(self, path, data=None, *, method=None, binary=False, brief=False, extra_headers=None, cancel=None, _output=None, _reuse=True, _asset_probe=False):
        if self.endpoint.scheme not in {'http', 'https'}:
            raise RemoteError('Private worker is available only inside its owning hybrid scheduler')
        if self.endpoint.scheme == 'http':
            if not lan_endpoint(self.endpoint.hostname):
                raise RemoteError('Non-LAN endpoints require HTTPS with certificate verification')
        # Both resource routes can wait for the center/worker handshake. A
        # direct hybrid resize needs the same deadline as a worker-side lease;
        # replies and ordinary requests keep their normal network deadline.
        resource_wait = (method in (None, 'POST') and isinstance(data, dict) and (
            re.fullmatch(r'/jobs/[0-9a-f]{32}/resize', path)
            or 'request_id' not in data and re.fullmatch(r'/jobs/[0-9a-f]{32}/lease', path)))
        network_timeout = getattr(self, 'lease_timeout', max(self.timeout, 130)) if resource_wait else self.timeout
        if _asset_probe:
            network_timeout = min(2, network_timeout)
        headers = {"Content-Type": "application/zip" if isinstance(data, bytes) else "application/json",
                   'Accept-Encoding': 'identity' if binary else 'gzip','X-DCO-Keepalive':'1'}
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
        relay = _RELAY_BULK_BUDGET
        if relay is not None and not relay.matches(self.endpoint):
            relay = None
        upload_budget = (relay if relay is not None and relay.bytes_per_second is not None
                         and stream_body and len(data) >= MIN_BULK_BYTES else None)
        binary_download = binary and method == 'GET'
        relayed_artifact = (relay is not None and binary_download
                            and re.fullmatch(r'/jobs/[0-9a-f]{32}/artifacts', path))
        partial, etag = _output if _output is not None else io.BytesIO(), None
        receiver = None
        if (_reuse and relayed_artifact and _output is not None
                and isinstance(getattr(_output, 'name', None), (str, os.PathLike))):
            from .artifact_reuse import Receiver
            receiver = Receiver()
        if _output is not None and binary and method=='GET' and headers.get('If-Range'):
            partial.seek(0,os.SEEK_END);etag=headers['If-Range']
        else:
            partial.seek(0);partial.truncate()
        asset_checkpoint = bool(re.fullmatch(r'/assets/[0-9a-f]{64}/upload', path))
        attempts = 1 if _asset_probe or asset_checkpoint else 4 if binary and method == 'GET' else 2
        retryable=(method=='GET' or method=='POST' and (path in ('/admission','/jobs/status') or bool(re.fullmatch(
            r'/(jobs/[0-9a-f]{32}(/cancel)?|assets/[0-9a-f]{64})',path))))
        asset_upload=method=='POST' and stream_body and bool(re.fullmatch(r'/assets/[0-9a-f]{64}',path))
        large_asset = asset_upload and len(data) >= 1024 * 1024
        asset_chunk = method == 'POST' and stream_body and asset_checkpoint
        job_upload=method=='POST' and stream_body and bool(re.fullmatch(r'/jobs/[0-9a-f]{32}',path))
        # Bulk streams tolerate relay stalls without resetting a paid prefix.
        # Control requests keep their short socket deadline.
        short_relay_job = job_upload and len(data) <= SHORT_DISPATCH_BYTES and relay is not None
        if asset_upload or job_upload:network_timeout=max(120,network_timeout)
        if asset_chunk:
            # A bounded socket idle timeout lets the next receiver take over.
            # Streaming continues while the socket makes progress.
            network_timeout = max(30, network_timeout)
        request_digest=None
        for attempt in range(attempts):
            _check_cancelled(cancel)
            # A fixed-width kernel may wait only briefly before preserving its
            # paid preparation. Bound each HTTP attempt by that same window.
            from . import deadline
            remaining=getattr(deadline,'resource_remaining_time',lambda:None)()
            if remaining is not None:remaining=deadline.remaining_time()
            if remaining is not None and remaining<=0:
                raise TimeoutError('DCO kernel startup wait expired')
            attempt_timeout=min(network_timeout,remaining)if remaining is not None else network_timeout
            if asset_upload and (attempt or (relay is not None and large_asset)):
                # Another campus worker may have published this archive while
                # this upload waited for the bulk slot. Check inside the slot.
                # The same check recovers an upload whose reply was lost.
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
                # The 8 KiB default repeatedly yields the GIL to a busy
                # scheduler. Keep uploads bounded without one send per 8 KiB.
                connection = factory(self.endpoint.hostname, self.endpoint.port, timeout=attempt_timeout,
                                     blocksize=64*1024,**options)
                self.connections.connection = connection
            connection.timeout = attempt_timeout
            if connection.sock is not None:
                connection.sock.settimeout(attempt_timeout)
            sent=False
            asset_body_sent = False
            try:
                if partial.tell() and etag:
                    headers.update(Range='bytes='+str(partial.tell())+'-', **{'If-Range':etag})
                else:
                    headers.pop('Range',None);headers.pop('If-Range',None)
                # A bytes body uses one sendall with a whole-body deadline.
                # The reader loop sends bounded blocks, so active uploads can
                # outlast the socket timeout. Each retry starts at byte zero.
                body=_UploadBody(data,cancel,upload_budget) if stream_body else data
                if connection.sock is None:connection.connect()
                sent=True
                connection.request(method, self.endpoint.path.rstrip('/') + path, body=body, headers=headers)
                asset_body_sent = large_asset
                if asset_body_sent and connection.sock is not None:
                    connection.sock.settimeout(min(attempt_timeout, max(15, self.timeout)))
                artifact_socket = connection.sock if relayed_artifact and attempt==0 else None
                if artifact_socket is not None:
                    # Retry a silent relay promptly. A fresh retry still allows
                    # slow archive preparation by older workers.
                    artifact_socket.settimeout(min(attempt_timeout,getattr(self,'_artifact_header_timeout',10)))
                if short_relay_job:
                    # Once the body is sent, a short dispatch uses the normal RPC
                    # deadline. Lost acceptance is reconciled using the same ID.
                    connection.sock.settimeout(min(attempt_timeout,self.timeout))
                response = connection.getresponse()
                if artifact_socket is not None and not response.isclosed():
                    artifact_socket.settimeout(attempt_timeout)
                if self.peer_id and response.getheader('X-DCO-Node') != self.peer_id:
                    connection.close();self.connections.connection=None
                    error=RemoteError('Destination device identity differs: '+str(self.peer_id));error.status_code=409
                    raise error
                self.connections.response_status=response.status
                if _output is None:
                    self.connections.etag=response.getheader("ETag")
                if binary and response.status==202 and _output is not None:
                    raw=response.read()
                elif binary and response.status < 400:
                    continued=(partial.tell() and response.status==206 and response.getheader('ETag')==etag
                               and response.getheader('Content-Range','').startswith('bytes '+str(partial.tell())+'-'))
                    if response.status==206 and not continued:
                        partial.seek(0);partial.truncate();etag=None
                        if receiver is not None:
                            receiver = Receiver()
                        self.connections.etag=None
                        raise http.client.HTTPException('Invalid artifact resume range')
                    if not continued:
                        if partial.tell() and receiver is not None:
                            receiver = Receiver()
                        partial.seek(0);partial.truncate()
                    etag=response.getheader('ETag') if response.getheader('Accept-Ranges')=='bytes' else None
                    if _output is not None:
                        self.connections.etag=etag
                    download_budget = (relay.download if relay is not None and binary_download
                                       and relay.download.bytes_per_second is not None
                                       and (continued or response.length is None or response.length >= MIN_BULK_BYTES) else None)
                    chunk_size = MIN_BULK_BYTES if download_budget is not None else 128 * 1024
                    while True:
                        chunk=response.read1(chunk_size)
                        if not chunk:
                            if response.length not in (None,0):
                                raise http.client.IncompleteRead(b'',response.length)
                            break
                        _check_cancelled(cancel)
                        if download_budget is not None:
                            download_budget.pace(len(chunk), lambda: _check_cancelled(cancel),
                                                 remaining=None if response.length is None else response.length + len(chunk))
                        partial.write(chunk)
                        if receiver is not None:
                            try:
                                candidate = receiver.candidate(partial, etag)
                            except OSError:
                                receiver = None
                                continue
                            if candidate is not None:
                                def resume(output):
                                    # Release the current upstream response before the
                                    # Range GET. Keep its pool slot and byte budget.
                                    response.close()
                                    connection.close()
                                    current = getattr(self.connections, 'connection', None)
                                    if current is not None and current is not connection:
                                        current.close()
                                    self.connections.connection = None
                                    return self._request(path, binary=True, cancel=cancel,
                                        extra_headers={**(extra_headers or {}), 'If-Range': etag},
                                        _output=output, _reuse=False)

                                def fetch_tail(stage):
                                    resume(stage)
                                    return self.connections.response_status, self.connections.etag

                                try:
                                    saved = receiver.complete(partial, etag, candidate, fetch_tail)
                                except (OSError, EOFError):
                                    saved = None
                                except RemoteError as exc:
                                    if isinstance(exc, RequestCancelled) or not getattr(exc, 'transport_retryable', False):
                                        raise
                                    saved = None
                                finally:
                                    # Outer metadata always describes the original
                                    # file, even if the staged request fails.
                                    self.connections.etag = etag
                                if saved is None:
                                    return resume(partial)
                                self.connections.reused_artifact_bytes = saved
                                return b''
                    raw=partial.getvalue() if _output is None else b''
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
            except (http.client.HTTPException, OSError, RemoteError) as exc:
                if isinstance(exc,RequestCancelled):
                    exc.request_may_have_been_sent=sent
                    raise
                if isinstance(exc,RemoteError) and (not job_upload or getattr(exc,'status_code',None)!=404):
                    raise
                connection.close()
                self.connections.connection = None
                if not etag:
                    partial.seek(0);partial.truncate()
                if asset_body_sent:
                    receipt = self._confirm_asset_publication(path, extra_headers, cancel)
                    if receipt is None:
                        # The caller can reconcile on the original provider.
                        # A route fallback must not replay an unconfirmed body.
                        error = RemoteError(f'{self.url}{path}: asset publication acknowledgement pending')
                        error.request_may_have_been_sent = True
                        raise error from None
                    if receipt['exists']:
                        return json_bytes(receipt) if binary else receipt
                if job_upload:
                    # Acceptance can finish before its HTTP reply is lost, or
                    # an old service's post-acceptance notice can return 404.
                    # Read the original receipt before retransmitting the body;
                    # its digest also prevents accepting a different request.
                    try:
                        saved=self._request(path,method='GET',cancel=cancel)
                    except RequestCancelled:
                        raise
                    except RemoteError as observed:
                        if observed.status_code!=404 and not isinstance(exc,RemoteError):
                            raise
                    else:
                        if request_digest is None:
                            request_digest=hashlib.sha256(data).hexdigest()
                        if (isinstance(saved,dict) and saved.get('id')==path.rsplit('/',1)[-1]
                                and saved.get('request_sha256')==request_digest):
                            return json_bytes(saved) if binary else saved
                if isinstance(exc,RemoteError):
                    raise  # A failed 404 recovery never resends the POST body.
                if not retryable or attempt==attempts-1:
                    error=RemoteError(f'{self.url}{path}: {exc}')
                    error.transport_code=getattr(exc,'winerror',None) or getattr(exc,'errno',None)
                    error.transport_retryable=isinstance(exc,(ConnectionError,TimeoutError,http.client.HTTPException,ssl.SSLEOFError,socket.gaierror))
                    error.request_may_have_been_sent=sent
                    raise error from None
                time.sleep(min(1,.1*2**attempt))
        return raw if binary else json.loads(raw)

    def health(self, *, brief=False, sample=False, display=False):
        headers = {}
        if sample:
            headers['X-DCO-Sample'] = '1'
        if display:
            headers['X-DCO-Health-Display'] = '1'
        result = self.request("/health", brief=brief, **({'extra_headers':headers} if headers else {}))
        if result.get("protocol") != PROTOCOL:
            raise RemoteError("Worker protocol differs: " + self.url)
        self._capabilities = result.get('capabilities',getattr(self,'_capabilities',None) or [])
        if not brief and result.get('role')=='worker' and 'active_jobs' in result:
            self._status_sample=(time.monotonic(),{j['id']:j for j in result['active_jobs'] or []})
        return result

    def capabilities(self, *, platform=None):
        """Small aggregate view intended for project adapters."""
        platform = validate_platform(platform)
        if platform is not None and not self.operator:
            raise RemoteError('platform selection is operator/debug-only; ordinary requests must omit platform')
        result = self.request('/capabilities' + ('?' + urlencode(dict(platform=platform)) if platform is not None else ''))
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

    def cache_ref(self,identity,*,content_id=None):
        """Register a successful producer's project cache before its receipt is deleted."""
        from .cache_refs import reference_id
        reference_id(identity)
        if 'cache_references'not in self.health(brief=True).get('capabilities',()):
            raise RemoteError('Cache references require scheduling-center maintenance activation')
        # Registration is idempotent on this producer ID; a lost reply must
        # not strand an otherwise successful build during publication.
        return self._read_reconnecting(lambda:self.request('/cache-references/'+identity,dict(content_id=content_id)))

    def publish_reference(self, identity, *, project, paths, recipe_sha256, kind='artifact', legacy_abi=None):
        """Retain existing files from an original receipt before retiring transport."""
        from .build_refs import declaration
        from .cache_refs import reference_id
        reference_id(identity)
        spec = declaration(dict(paths=list(paths), recipe_sha256=recipe_sha256), kind=kind)
        request = dict(project=project, kind=kind, **spec)
        if legacy_abi is not None:
            request['legacy_abi'] = list(legacy_abi)
        return self._read_reconnecting(lambda: self.request('/jobs/' + identity + '/publish-reference', request))

    def submit(self, task, *, inputs=None, files=None, job_id=None):
        from .policy import validate_operator_fields
        try:validate_operator_fields(task,self.operator)
        except ValueError as error:raise RemoteError(str(error)) from None
        if 'cpus' in task:
            raise ValueError('Declare max_cpus; DCO assigns the actual CPU count')
        # A stable id can be queried or resubmitted to THIS node after a lost reply.
        bundle = task_bundle(task, inputs, files)
        if job_id is None and task.get('produces_artifact_ref'):
            job_id = hashlib.sha256(b'DCO artifact producer\0' + bundle).hexdigest()[:32]
        return self.submit_bundle(job_id or uuid.uuid4().hex, bundle)

    def submit_bundle(self, identity, payload, *, callback=None, cancel=None):
        path='/jobs/'+identity
        options={'extra_headers':{'X-DCO-Callback':json.dumps(callback)}} if callback else {}
        options['cancel']=cancel
        try:
            return self.request(path,payload,**options)
        except RemoteError as error:
            if isinstance(error,RequestCancelled)and not getattr(error,'request_may_have_been_sent',False):
                raise
            if not (error.transport_retryable or error.status_code in (502,503)or isinstance(error,RequestCancelled)):
                raise
        # A busy center can accept a task while both its reply and the first
        # receipt read time out. Reconcile that paid ID before another upload.
        digest=hashlib.sha256(payload).hexdigest()
        def reconcile():
            try:
                saved=self.request(path)
            except RemoteError as error:
                if error.status_code!=404:raise
                _check_cancelled(cancel)
                return self.request(path,payload,**options)
            if saved.get('id')==identity and saved.get('request_sha256')==digest:
                return saved
            conflict=RemoteError('Original job request differs: '+identity)
            conflict.status_code=409
            raise conflict
        return self._read_reconnecting(reconcile)

    def cpu_lease(self, identity, **options):
        """Track ready/active CPU demand at executor boundaries, without a thread."""
        from .leases import CpuLease
        return CpuLease(self, identity, **options)

    def resize(self, identity, cpus, *, active_cpus=None, min_cpus=None, memory_mb=None, base_memory_mb=None, memory_per_cpu_mb=None, memory_limit_mb=None, _memory_budget_mb=None):
        """Borrow idle slots or return them; active_cpus counts in-flight threads."""
        if type(cpus) is not int or cpus < 0:
            raise ValueError('resize cpus must be a nonnegative integer')
        if active_cpus is not None and (type(active_cpus) is not int or active_cpus < 0):
            raise ValueError('active_cpus must count nonnegative in-flight threads')
        if min_cpus is not None and (type(min_cpus) is not int or not 0<=min_cpus<=cpus):
            raise ValueError('min_cpus must be an integer between zero and cpus')
        # An unfunded adaptive estimate is an automatic-budget hint. The
        # worker's explicit memory budget accepts positive MiB values only.
        if type(_memory_budget_mb) is int and _memory_budget_mb==0:
            _memory_budget_mb=None
        request = dict(cpus=cpus)
        for key, value in dict(active_cpus=active_cpus, min_cpus=min_cpus, memory_mb=memory_mb,
                              base_memory_mb=base_memory_mb, memory_per_cpu_mb=memory_per_cpu_mb,
                              memory_limit_mb=memory_limit_mb,memory_budget_mb=_memory_budget_mb).items():
            if value is not None:request[key] = value
        action='lease' if (os.environ.get('REMOTE_COMPUTE_LEASE')=='1' and identity==os.environ.get('REMOTE_COMPUTE_JOB_ID')
                           and self.url==os.environ.get('REMOTE_COMPUTE_URL','').rstrip('/')) else 'resize'
        target=os.environ.get('REMOTE_COMPUTE_WORKER_JOB_ID',identity) if action=='lease' else identity
        path='/jobs/'+target+'/'+action
        def pending(error):
            return error.status_code==409 and 'A resource request is already pending' in str(error)
        try:
            return self.request(path, request)
        except RemoteError as error:
            if error.status_code==409 and RESOURCE_TRANSPORT_RETAINED not in str(error)and any(marker in str(error)for marker in (
                    'UNEXPECTED_EOF_WHILE_READING','EOF occurred in violation of protocol')):
                # A polling center can forward a peer TLS failure as a conflict.
                # The resize outcome is uncertain; observe the retained grant,
                # rather than treating an optional resource reply as map failure.
                retained=RemoteError(RESOURCE_TRANSPORT_RETAINED+': '+str(error))
                retained.status_code=409
                raise retained from None
            if not (error.transport_retryable or error.status_code in (502,503,504) or pending(error)):
                raise
        # Resource changes set a grant; they never start or repeat business
        # items. Observe the original before replaying that same desired state.
        # In polling mode the first POST can still be waiting at the worker.
        def reconnect():
            current=self.status(target)
            if current.get('status')!='running':
                error=RemoteError('Only a running task can change its CPU grant');error.status_code=409
                raise error
            if action=='lease' and current.get('resource_request'):
                error=RemoteError('Waiting for the original resource request to settle');error.transport_retryable=True
                raise error
            try:return self.request(path,request)
            except RemoteError as error:
                if pending(error):error.transport_retryable=True
                raise
        inherited=(identity==os.environ.get('REMOTE_COMPUTE_JOB_ID')
                   and self.url==os.environ.get('REMOTE_COMPUTE_URL','').rstrip('/'))
        from .deadline import remaining_time,check_deadline
        remaining=remaining_time()if inherited else None
        return self._read_reconnecting(reconnect,
            deadline=time.monotonic()+remaining if remaining is not None else None,
            check=check_deadline if inherited else None)

    def nodes(self):
        if not self.operator:
            raise RemoteError('Device list is operator-only; use capabilities()')
        return self.request("/nodes")

    def admission(self, project, *, resource_share=1.0, queue_ratio=1.0, requires=(), node=None, max_cpus=1024, job_id=None, accelerator=None, ready_tasks=None, platform=None, activity=False, executable_ram_mb=None):
        """Center-owned transport window; execution slots stay in the central queue."""
        if node is not None and not self.operator:
            raise RemoteError('node selection belongs to the DCO operator; use requires for capabilities')
        request=dict(project=project,resource_share=resource_share,queue_ratio=queue_ratio,requires=list(requires))
        if executable_ram_mb is not None:
            request['executable_ram_mb'] = validate_executable_ram_mb(executable_ram_mb)
        if activity:
            request['activity'] = True
        platform = validate_platform(platform)
        if platform is not None and not self.operator:
            raise RemoteError('platform selection is operator/debug-only; ordinary requests must omit platform')
        if platform is not None:
            request['platform'] = platform
        if ready_tasks is not None:
            request['ready_tasks'] = validate_ready_tasks(ready_tasks)
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
            value = self._read_reconnecting(lambda:self.request('/jobs/'+identity+'/lease',dict(request,_operation='admission')))
        else:
            value = self._read_reconnecting(lambda:self.request('/admission',request))
        if executable_ram_mb is not None and value.get('executable_ram_mb') != executable_ram_mb:
            raise RemoteError('The scheduling center does not support executable_ram_mb')
        return value

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

    def status_page(self, *, limit=256, offset=0, brief=False, prefix=None):
        """Read a bounded recent task page without scanning retained receipts."""
        limit = max(1, min(int(limit), 512))
        offset = max(0, int(offset))
        query = {'limit': limit, 'offset': offset}
        if prefix is not None:
            if not isinstance(prefix, str) or not 8 <= len(prefix) <= 32 or any(c not in '0123456789abcdef' for c in prefix):
                raise ValueError('Job prefix requires 8 to 32 lowercase hex digits')
            if self._capabilities is None:
                self.health(brief=True)
            if 'job_prefix' not in self._capabilities:
                matches = [j for j in self.status(brief=True) if j['id'].startswith(prefix)]
                return dict(items=matches[offset:offset+limit], total=len(matches), offset=offset,
                            limit=limit, has_more=offset+limit < len(matches), legacy_center=True)
            query['prefix'] = prefix
        value = self.request('/jobs?' + urlencode(query),
                             brief=brief)
        # Older centers return a plain list; keep the page API usable while
        # those processes drain and make the truncation explicit to callers.
        if isinstance(value, dict) and 'items' in value:
            return value
        return dict(items=value if isinstance(value, list) else [],
                    total=len(value) if isinstance(value, list) else 0,
                    offset=offset, limit=limit, has_more=False,
                    legacy_center=True)

    def statuses(self, identities, *, compact=False):
        """Brief states of known original IDs; never list the global job table."""
        identities = list(identities)
        for offset in range(0, len(identities), MAX_STATUS_BATCH):
            validate_status_ids(identities[offset:offset+MAX_STATUS_BATCH])
        identities = list(dict.fromkeys(identities))
        if not identities:
            return {}
        if self._capabilities is None:
            self.health(brief=True)
        values = {}
        for offset in range(0, len(identities), MAX_STATUS_BATCH):
            batch = identities[offset:offset+MAX_STATUS_BATCH]
            if 'batch_status' in self._capabilities:
                body = {'ids': batch}
                if compact:
                    body['compact'] = True
                try:
                    states=self.request('/jobs/status', body)
                except RemoteError as error:
                    if error.status_code!=404:raise
                    # Older centers can lose a cold terminal file to concurrent
                    # collection. Re-read this batch after retirement finishes.
                    states=self.request('/jobs/status', body)
                values.update(states)
            else:
                for identity in batch:
                    try:
                        values[identity] = self.status(identity, brief=True)
                    except RemoteError as error:
                        if getattr(error, 'status_code', None) != 404:
                            raise
                        values[identity] = dict(id=identity, status='unknown', status_code=404)
        if compact:
            from .bundle import batch_status
            return {identity: batch_status(row, compact=True) for identity, row in values.items()}
        return values

    def wait_any(self, identities, *, poll=.5, max_poll=5, wait_timeout=None, cancel=None):
        """Collect terminal brief states in one loop; fetch keeps full receipts."""
        identities = tuple(dict.fromkeys(identities))
        if not identities or poll <= 0:
            raise ValueError('wait_any requires original IDs and a positive poll interval')
        started = time.monotonic()
        deadline = None if wait_timeout is None else started+wait_timeout
        def check():
            if cancel is not None and cancel():
                raise RequestCancelled('Batch wait cancelled; original tasks continue')
        while True:
            states = self._read_reconnecting(lambda:self.statuses(identities), deadline=deadline, check=check)
            finished = {identity: state for identity, state in states.items() if state['status'] in TERMINAL}
            if finished:
                return finished
            missing = [identity for identity, state in states.items() if state['status'] == 'unknown']
            if missing:
                error = RemoteError('Original job missing; reconcile before submitting: '+','.join(missing))
                error.status_code = 404
                error.missing_ids = missing
                raise error
            now = time.monotonic()
            if deadline is not None and now >= deadline:
                raise RemoteError('Batch wait limit reached; original tasks continue')
            delay = min(max(poll, max_poll), max(poll, (now-started)/20))
            if deadline is not None:
                delay = min(delay, deadline-now)
            _pause(delay, check)

    def cancel(self, identity):
        # A force control must not queue behind this client's long resource
        # handshakes or artifact transfers. Keep the same auth/TLS identity.
        control = Client(self.url, self.token, timeout=min(self.timeout, 3), ca_file=self.ca_file,
                         operator=self.operator, operator_token=self.operator_token, pool_timeout=.5,
                         relay_url=self.relay_url,peer_id=self.peer_id)
        return control.request("/jobs/" + identity + "/cancel", {})

    def clean(self):
        """Operator cleanup of queued tasks; active and finished receipts remain."""
        if not self.operator:raise RemoteError('Queue cleanup is operator-only')
        return self.request('/clean',{})

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
        """Confirm idempotent retirement even when its reply was lost."""
        if not isinstance(identity, str) or not re.fullmatch('[0-9a-f]{32}', identity):
            raise ValueError('Job id requires 32 lowercase hex characters')
        for attempt in range(2):
            try:
                return self.request("/jobs/" + identity, method="DELETE")
            except RemoteError as error:
                if error.status_code==404:return {"deleted":identity}
                if not error.transport_retryable and error.status_code not in (502,503,504):raise
                # The retirement marker may have committed before the reply was lost.
                try:
                    current=self.status(identity)
                except RemoteError as observed:
                    if observed.status_code==404:return {"deleted":identity}
                    if observed.status_code is not None and observed.status_code not in (502,503,504):raise
                else:
                    if current.get('status') not in TERMINAL:raise error
                if attempt:raise error

    def _read_reconnecting(self, read, *, deadline=None, check=None):
        """Keep an existing observation across a bounded transport outage."""
        expires=time.monotonic()+max(120,self.timeout)
        if deadline is not None:expires=min(expires,deadline)
        delay=.2
        while True:
            if check is not None:check()
            try:
                return read()
            except RemoteError as error:
                transient=(isinstance(error, PoolBusy) or getattr(error,'status_code',None) in (502,503,504)
                           or getattr(error,'transport_retryable',False))
                remaining=expires-time.monotonic()
                if not transient or remaining<=0:
                    raise
                _pause(min(delay,remaining),check)
                delay=min(5,delay*2)

    def wait(self, identity, *, poll=0.15, max_poll=5, wait_timeout=None, cancel=None):
        """Wait on the original ID; stopping this wait leaves its task running."""
        if poll <= 0:
            raise ValueError('poll must be positive')
        max_poll = max(poll, max_poll)
        started = time.monotonic()
        deadline=None if wait_timeout is None else started+wait_timeout
        phase_started, phase = started, None
        def check():
            if cancel():
                raise RequestCancelled(f'Wait cancelled; query {self.url}/jobs/{identity}')
        check = check if cancel is not None else None
        while True:
            result = self._read_reconnecting(lambda:self.status(identity,brief=True),deadline=deadline,check=check)
            if result["status"] in TERMINAL:
                result = self._read_reconnecting(lambda:self.status(identity),deadline=deadline,check=check)
                result["url"] = self.url
                return result
            now = time.monotonic()
            observed = (result['status'], result.get('phase'))
            if observed != phase:
                phase_started, phase = now, observed
            if deadline is not None and now >= deadline:
                raise RemoteError(f"Wait limit reached; query {self.url}/jobs/{identity}")
            # Short phases retain the original cadence; long black boxes need
            # fewer status requests, without watching logs or adding a monitor.
            delay = min(max_poll, max(poll, (now-phase_started)/20))
            if deadline is not None:delay=min(delay,deadline-now)
            _pause(delay,check)

    def artifacts(self, identity, *, destination=None):
        # A center may first need to retrieve the complete remote ZIP. Keep
        # that header wait and its connections separate from status/control.
        if self.endpoint.scheme not in {'http', 'https'}:
            raw=self._read_reconnecting(lambda:self.request('/jobs/'+identity+'/artifacts',binary=True))
            if destination is None:return raw
            target=Path(destination)
            target.write_bytes(raw)
            return target
        backend=self
        if self.endpoint.scheme in {'http','https'}:
            backend=self.__dict__.get('_result_client')
            if backend is None:
                backend=self.__dict__.setdefault('_result_client',Client(self.url,self.token,timeout=max(120,self.timeout),ca_file=getattr(self,'ca_file',None),operator=self.operator,operator_token=self.operator_token,pool_timeout=getattr(self,'pool_timeout',None),relay_url=self.relay_url,peer_id=self.peer_id))
            backend.timeout=max(120,self.timeout)
            backend._artifact_header_timeout=self.timeout
            backend.bulk_limit=getattr(self,'bulk_limit',7)
            if backend.bulk_limit!=7:
                backend._transport=self.__dict__.setdefault('_transport',_ConnectionPool())
                backend._transports=self.__dict__.setdefault('_transports',{})
        metrics=self.__dict__.setdefault('_artifact_timing',threading.local())
        stats=dict(requests=0,request_seconds=0.0,preparing_responses=0,prepare_sleep_seconds=0.0)
        metrics.value=stats
        def read():
            started=time.monotonic()
            try:
                if destination is None:
                    return backend.request('/jobs/'+identity+'/artifacts',binary=True,extra_headers={'X-DCO-Artifact-Poll':'1'})
                from .bundle import atomic_json
                target=Path(destination);metadata=target.with_name(target.name+'.http.json')
                try:saved=json.loads(metadata.read_text())
                except (OSError,ValueError):saved={}
                headers={'X-DCO-Artifact-Poll':'1'}
                if (saved.get('id')==identity and saved.get('url')==self.url and saved.get('etag')
                        and target.is_file() and saved.get('bytes')==target.stat().st_size):
                    headers['If-Range']=saved['etag']
                with target.open('r+b' if target.exists() else 'w+b') as output:
                    backend.connections.etag=headers.get('If-Range')
                    backend.connections.response_status=None
                    backend.connections.reused_artifact_bytes=0
                    try:
                        raw=backend.request('/jobs/'+identity+'/artifacts',binary=True,extra_headers=headers,_output=output)
                        if backend.connections.response_status==202:return raw
                    finally:
                        output.flush()
                        if backend.connections.response_status!=202:
                            atomic_json(metadata,dict(id=identity,url=self.url,
                                etag=getattr(backend.connections,'etag',None),bytes=target.stat().st_size))
                if (_RELAY_BULK_BUDGET is not None and _RELAY_BULK_BUDGET.matches(backend.endpoint)
                        and target.stat().st_size >= 1024 * 1024):
                    from .artifact_reuse import cache
                    cache.remember(target)
                stats['reused_artifact_bytes'] = backend.connections.reused_artifact_bytes
                return target
            finally:
                stats['requests']+=1;stats['request_seconds']+=time.monotonic()-started
        delay = .2
        while True:
            raw = self._read_reconnecting(read)
            if getattr(backend.connections, 'response_status', 200) != 202:
                return raw
            preparation = json.loads(raw)
            if preparation != {'id': identity, 'status': 'preparing'}:
                raise RemoteError('Invalid artifact preparation response')
            stats['preparing_responses']+=1
            # A live preparation acknowledgement renews observation of this
            # original result. Transport outages keep their bounded reconnect
            # window, independently of how long the provider transfer takes.
            started=time.monotonic();time.sleep(delay)
            stats['prepare_sleep_seconds']+=time.monotonic()-started
            delay = min(.5, delay*2)

    def fetch(self, identity, output_dir=None, *, delete=True):
        if output_dir is None:
            return self._fetch(identity,output_dir,delete=delete)
        # The caller owns TEMP/TMPDIR (including shared RAM scratch). Preserve
        # that policy instead of forcing every transfer onto the Windows SSD.
        with tempfile.TemporaryDirectory(prefix='dco-artifact-') as folder:
            return self._fetch(identity,output_dir,delete=delete,artifact_path=Path(folder)/'result.zip')

    def _fetch(self, identity, output_dir=None, *, delete=True, artifact_path=None):
        started=time.monotonic()
        metrics=self.__dict__.setdefault('_artifact_timing',threading.local());metrics.value=None
        fetched=self.__dict__.setdefault('_fetch_timing',threading.local());fetched.value=dict(id=identity)
        try:raw = self.artifacts(identity,**({'destination':artifact_path} if artifact_path is not None else {}))
        except BaseException:
            fetched.value=dict(id=identity,stage='artifacts',artifacts_seconds=time.monotonic()-started,**(metrics.value or {}))
            raise
        received=time.monotonic()
        source=raw if isinstance(raw,Path) else io.BytesIO(raw)
        archive_bytes=raw.stat().st_size if isinstance(raw,Path) else len(raw)
        with zipfile.ZipFile(source) as archive:
            result = json.loads(archive.read("result.json"))
            expanded=sum(item.file_size for item in archive.infolist())
            names = [name for name in archive.namelist() if name.startswith("files/")]
            result["files"] = ({name[6:]: archive.read(name) for name in names} if output_dir is None
                               else sorted({name[6:] for name in names}))
        decoded=time.monotonic()
        if output_dir is not None:
            directory=Path(output_dir).resolve()
            if not isinstance(source,Path):source.seek(0)
            with zipfile.ZipFile(source)as archive:
                long=any(len(str(directory/name))>=248 for name in archive.namelist())
            directory=native_path(directory,force=long)
            if not isinstance(source,Path):source.seek(0)
            from .bundle import inside
            with zipfile.ZipFile(source) as archive:
                if expanded>2*1024**3:raise ValueError('Expanded bundle exceeds 2 GiB; predeploy large assets with shared roots')
                for item in archive.infolist():
                    target=inside(directory,item.filename);mode=item.external_attr>>16
                    if stat.S_ISLNK(mode):raise ValueError('Bundle requires regular files')
                    if item.is_dir():target.mkdir(parents=True,exist_ok=True);continue
                    target.parent.mkdir(parents=True,exist_ok=True)
                    with archive.open(item) as input_file,target.open('wb') as output:
                        shutil.copyfileobj(input_file,output,128*1024)
                    if os.name!='nt':target.chmod(stat.S_IMODE(mode) or 0o644)
            result["output_dir"] = str(directory)
        unpacked=time.monotonic()
        fetched.value=dict(id=identity,artifacts_seconds=received-started,decode_seconds=decoded-received,
            unpack_seconds=unpacked-decoded,total_seconds=unpacked-started,
            archive_bytes=archive_bytes,expanded_bytes=expanded,**(metrics.value or {}))
        result["url"] = self.url
        retired=time.monotonic()
        if delete:
            try:
                self.delete(identity)
            except RemoteError as error:
                if not error.transport_retryable and error.status_code not in (502,503,504):
                    raise
                # Files are already verified and received. A lost cleanup
                # acknowledgement must not discard the caller's only copy.
                result['retirement_pending']=True
                fetched.value['retirement_error']=str(error)
        fetched.value['delete_seconds']=time.monotonic()-retired if delete else 0.0
        fetched.value['total_seconds']=time.monotonic()-started
        return result

    def last_fetch_timing(self):
        """Local transport timings for this thread; the original receipt stays immutable."""
        return dict(getattr(getattr(self,'_fetch_timing',None),'value',{}))

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

    def execute(self, requests, *, project, output_dir, prepare=None, publish=None, ready_tasks=None,stop_when=None,cancel_when=None):
        """Submit a keyed plan and durably publish/collect it through one pipeline."""
        from .pipeline import execute
        return execute(self,requests,project=project,output_dir=output_dir,prepare=prepare,publish=publish,
                       ready_tasks=ready_tasks,stop_when=stop_when,cancel_when=cancel_when)

    def sync(self, root, patterns=("**/*",)):
        return self.sync_files(tree(root, patterns))

    def has_asset(self, identity, *, job_id=None):
        """Read immutable asset presence without preparing or uploading it."""
        options = {'extra_headers':{'X-DCO-Asset-Job':job_id}} if job_id else {}
        return self._read_reconnecting(lambda:self.request('/assets/'+identity, **options))['exists']

    def upload_resumable(self, identity, archive, *, cancel=None, report=None, fail_fast=False):
        """Resume from the receiver's committed prefix after a lost connection."""
        from .asset_upload import ASSET_CHUNK_BYTES

        if not re.fullmatch(r'[a-f0-9]{64}', identity):
            raise ValueError('Asset id requires SHA256')
        archive = Path(archive)
        total = archive.stat().st_size
        if not 0 < total <= MAX_BODY:
            raise ValueError('Asset upload exceeds the archive size limit')
        path = '/assets/' + identity + '/upload'
        metrics = report if report is not None else {}
        metrics.setdefault('confirmed_bytes', 0)
        metrics.setdefault('attempted_bytes', 0)
        metrics.setdefault('reconciliations', 0)
        metrics.setdefault('resumed_from_bytes', 0)
        metrics['protocol'] = 'resumable'
        metrics['archive_bytes'] = total
        current, failures = None, 0
        with archive.open('rb') as stream:
            while True:
                _check_cancelled(cancel)
                offset = current['offset'] if current is not None else None
                try:
                    if offset is None:
                        receipt = self.request(path, cancel=cancel)
                    elif offset == total:
                        receipt = self.request(path, {'total': total}, cancel=cancel)
                    else:
                        stream.seek(offset)
                        chunk = stream.read(min(ASSET_CHUNK_BYTES, total - offset))
                        if not chunk:
                            raise ValueError('Asset archive changed during upload')
                        metrics['attempted_bytes'] += len(chunk)
                        header = f'bytes {offset}-{offset + len(chunk) - 1}/{total}'
                        receipt = self.request(path, chunk, extra_headers={'Content-Range': header}, cancel=cancel)
                    confirmed = receipt.get('offset')
                    if (receipt.get('id') != identity or type(confirmed) is not int or not 0 <= confirmed <= total
                            or receipt.get('total') not in (None, total) or (confirmed and receipt.get('total') != total)
                            or type(receipt.get('complete')) is not bool
                            or (receipt['complete'] and confirmed != total)):
                        raise ValueError('Invalid asset upload checkpoint from receiver')
                    previous = metrics.get('confirmed_offset', confirmed)
                    if confirmed < previous:
                        stale = RemoteError('Shared asset checkpoint is still becoming visible')
                        stale.transport_retryable = True
                        raise stale
                    if offset is None and confirmed and not receipt['complete']:
                        metrics['resumed_from_bytes'] = max(metrics['resumed_from_bytes'], confirmed)
                    metrics['confirmed_bytes'] += max(0, confirmed - previous)
                    metrics['confirmed_offset'] = confirmed
                    if receipt['complete']:
                        return receipt
                    if offset is not None and confirmed == offset:
                        stalled = RemoteError('Asset receiver has not advanced its checkpoint')
                        stalled.transport_retryable = True
                        raise stalled
                    if confirmed > (offset if offset is not None else previous):
                        failures = 0
                    current = receipt
                except RemoteError as error:
                    if isinstance(error, RequestCancelled) or (not isinstance(error, PoolBusy)
                            and not error.transport_retryable and error.status_code not in (404, 408, 409, 502, 503, 504)):
                        raise
                    failures += 1
                    metrics['reconciliations'] += 1
                    if fail_fast or failures >= 6:
                        pending = RemoteError(f'Asset upload {identity} awaits receiver checkpoint: {error}')
                        pending.transport_retryable = True
                        raise pending from error
                    current = None
                    time.sleep(min(2, .1 * 2**failures))

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
    artifact_claims: dict = field(default_factory=dict)
    known_jobs: set = field(default_factory=set)
    live_jobs: set = field(default_factory=set)
    memory_holds: dict = field(default_factory=dict)
    ram_holds: dict = field(default_factory=dict)
    host_ip: str = ""
    node_ip_observation: dict = field(default_factory=dict)
    native_architecture: str | None = None
    aliases: list = field(default_factory=list)
    accelerator_reservations: dict = field(default_factory=dict)
    external_accelerator_reservations: list = field(default_factory=list)
    network_checked_at: float = 0.0
    network_samples: list = field(default_factory=list)
    network: dict = field(default_factory=dict)
    dispatch_lock: object = field(default_factory=threading.Lock,repr=False,compare=False)
    next_dispatch: float = 0.0
    dispatch_interval: float = .5
    display_name: str = ""

    @property
    def executable_ram_capacity(self):
        return (self.health.get('executable_ram') or {}).get('capacity_mb', 0)

    @property
    def executable_ram_available(self):
        available = (self.health.get('executable_ram') or {}).get('available_mb', 0)
        return max(0, available - sum(self.ram_holds.values()))

    @property
    def base_capacity(self):
        available = self.health.get('base_cpus',self.health.get('cpus',0))
        return min(available, self.limit) if self.limit else available

    @property
    def capacity(self):
        # Transport reservations waiting for startup do not prove a full worker.
        used=min(self.reserved+self.external,self.health.get('used_cpus',self.reserved+self.external))
        return cpu_capacity(self.base_capacity,used,self.health.get('cpu_percent'),
                            self.health.get('cpu_overcommit_factor',1.0),
                            self.health.get('cpu_overcommit_utilization',90))

    @property
    def memory_capacity(self):
        value = self.health.get("memory_capacity_mb", 0)
        return min(value, self.memory_limit_mb) if self.memory_limit_mb else value

    @property
    def memory_free(self):
        pressure=self.health.get('memory_pressure_state') or {}
        if pressure.get('limited'):return 0
        # The worker already deducts funded but not yet materialized memory.
        # Idle host RAM cannot replace that admission budget: starting work
        # still owns its reservation until the worker samples or releases it.
        reported=self.health.get('memory_available_mb',0)
        if self.health.get('memory_accounting')=='rss':
            return min(self.memory_capacity-self.health.get('memory_reserved_mb',0)-self.memory_unreported,
                       reported-self.memory_unreported)
        worker_reserved=self.health.get("memory_reserved_mb",0)
        unreported = max(0, self.reserved_memory_mb-worker_reserved)
        return min(self.memory_capacity-self.reserved_memory_mb-self.external_memory_mb,
                   reported-unreported)

    @property
    def memory_unreported(self):
        reported=sum(job.get('memory_mb',0) for job in self.health.get('active_jobs',()) if job['id'] in self.live_jobs)
        return max(0,self.reserved_memory_mb-reported)+sum(row['amount'] for row in self.memory_holds.values())

    def memory_credit(self, identity, declared):
        job=next((j for j in self.health.get('active_jobs',()) if j['id']==identity),None)
        credit=min(job.get('memory_reservation_mb',declared),job.get('memory_commit_reservation_mb',declared)) if job else declared
        hold=self.memory_holds.get(identity)
        # An acknowledged resize already funded this original's new grant.
        # A stale worker sample must not charge that grant a second time.
        if hold is not None and hold.get('after')is not None:
            credit=max(credit,min(declared,hold.get('credit',credit+hold['amount'])))
        return credit

    def startup_memory(self,task):
        from .memory import startup_memory
        return startup_memory(task)if 'adaptive_memory'in self.health.get('capabilities',())else task.get('memory_mb',256)

    def resize_memory_model(self,identity,model):
        from .memory import calibrated_model
        row=next((j for j in self.health.get('active_jobs',())if j['id']==identity),{})
        return calibrated_model(model,row.get('memory_phase')or row.get('memory_phase_model'),row.get('memory_sample_cpus',row.get('active_cpus',0)),
            row.get('memory_observed_mb',row.get('memory_reservation_mb',0)),
            row.get('memory_commit_observed_mb',row.get('memory_commit_reservation_mb',0)),
            row.get('memory_calibrated',False),row.get('memory_adaptive_model'),row.get('memory_width_samples'),
            cold_probe=row.get('memory_sample_cpus')==0)

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
        used=list(extra_used)
        if include_reserved:
            used.extend(unit for items in self.accelerator_reservations.values() for unit in items)
            used.extend(self.external_accelerator_reservations)
        return select_accelerator_units(self.accelerator_units(), requirement, used)

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

    def execution_accelerator(self, task, *, available=False):
        requirement=task.get('accelerator')
        if requirement is None or requirement.get('kind','any')!='any':return requirement
        used=([unit for items in self.accelerator_reservations.values()for unit in items]
              +self.external_accelerator_reservations)if available else ()
        units=self.accelerator_units();backends=self.health.get('python_backends',['cpu'])
        bound=python_accelerator(task,backends,units,used)
        # A busy compatible backend must keep waiting for its own device kind.
        # Returning raw any here could reserve a free but unsupported kind.
        if available and bound.get('kind','any')=='any':
            bound=python_accelerator(task,backends,units)
        return bound

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
                            entry.get('client') or Client(entry["url"], entry.get("token", ""),timeout=entry.get('timeout',10),ca_file=entry.get('ca_file'),operator=True,relay_url=entry.get('relay_url'),peer_id=entry.get('peer_id')),
                            weight=entry.get("weight", 1.0), limit=entry.get("limit", 0),
                            memory_limit_mb=entry.get("memory_limit_mb", 0),
                            enabled=entry.get("enabled", True))
                bulk_limit=entry.get('bulk_limit',7)
                if type(bulk_limit)is not int or not 1<=bulk_limit<=7:
                    raise ValueError('Node bulk_limit must be an integer in 1..7')
                node.client.bulk_limit=bulk_limit
                node.host_ip = entry.get("host_ip", "")
                node.node_ip_observation = entry.get('node_ip_observation') or {}
                node.native_architecture = entry.get('architecture')
                node.aliases = entry.get('aliases',[])
                node.display_name = entry.get('display_name','')
                retry = entry.get('reconnect')
                if retry is not None and not node.enabled:
                    attempts, after = (retry.get('attempts'), retry.get('after')) if isinstance(retry,dict) else (None,None)
                    if (type(attempts) is not int or not 0 <= attempts <= 1 + len(RELAY_RECONNECT_INTERVALS)
                            or isinstance(after, bool) or not isinstance(after, (int, float))
                            or not 0 <= after < float('inf')):
                        raise ValueError('Invalid persisted reconnect attempts or timestamp')
                    node.detached = True
                    # Releases before 1.0.42 persisted zero once retries were
                    # exhausted. A configured worker remains eligible to recover.
                    node.reconnect_attempts = attempts
                    node.reconnect_after = after or time.time() + RECONNECT_BACKOFF_MAX
            if isinstance(node.client,Client):node.client.operator=True
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
        if not isinstance(getattr(node,'display_name',''),str):
            raise ValueError('Node display_name must be a string')
        if (not isinstance(node.weight, (int, float)) or not 0 < node.weight < float("inf")
                or type(node.limit) is not int or node.limit < 0 or not isinstance(node.enabled, bool)
                or type(node.memory_limit_mb) is not int or node.memory_limit_mb < 0):
            raise ValueError("Node weight must be positive; limit is 0/auto or a positive integer")
    def _probe(self, node, full=False, reconnect=False, generation=None):
        probe_started=time.monotonic()
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
                previous_machine, current_machine = node.health.get('machine_id'), value.get('machine_id')
                # machine_id contains hostname:hardware-address. Renaming a
                # computer leaves the endpoint, credentials and hardware intact.
                same_worker = (previous_machine.rsplit(':',1)[-1] == current_machine.rsplit(':',1)[-1]
                               if previous_machine and current_machine else value.get('node') == previous_name)
                if reconnect and previous_name and not same_worker:
                    node.error = 'Reconnect worker identity differs from the accepted endpoint'
                    return
                node.last_connected_at=time.monotonic()
                if previous_name != value.get('node'):
                    node.health.pop('node_ip',None)
                merged = {**node.health, **value}
                merged['executable_ram'] = value.get('executable_ram')
                merged['tags'] = provider_tags(merged)
                node.health=merged
                for identity,row in list(node.memory_holds.items()):
                    if row.get('after') is not None and probe_started>=row['after']:
                        node.memory_holds.pop(identity,None)
                actual_name = value.get('node')
                renamed = bool(previous_name and actual_name and actual_name != previous_name
                               and actual_name != node.name and actual_name not in node.aliases)
                if renamed:node.aliases.append(actual_name)
                jobs=value.get('active_jobs') or []
                if node.ram_holds:
                    for job in jobs:
                        node.ram_holds.pop(job['id'], None)
                running=[job for job in jobs if job['status']=='running']
                node.external=sum(job['cpus'] for job in running if job['id'] not in node.known_jobs)
                node.refresh_external_accelerators(running)
                owned_memory=sum(job.get('memory_reservation_mb',
                    (job.get('rss_mb') or job['memory_mb']) if value.get('memory_accounting')=='rss' and not job.get('memory_modeled') else
                    max(job['memory_mb'],job.get('rss_mb',0))) for job in running if job['id'] in node.known_jobs)
                node.external_memory_mb=max(0,value.get('memory_reserved_mb',0)-owned_memory)
                node.known_jobs.intersection_update({job['id'] for job in jobs}|node.live_jobs)
                if reconnect and node.detached:
                    node.enabled, node.detached = True, False
                    node.reconnect_attempts, node.reconnect_after = 0, 0.0
            self.condition.notify_all()
        if reconnect or (not error and renamed):self._save_reconnect(node)

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
        pending, in_flight = [], []
        with self.refresh_lock:
            for node in selected:
                future=self.probe_futures.get(node.name)
                interval=min(self.sample_seconds,.2) if node.live_jobs else self.sample_seconds
                # A worker sample carries all active jobs and their lease requests.
                # Keep relayed samples inside Client.status's two-second reuse window.
                endpoint = getattr(node.client, 'endpoint', None)
                if monitor and node.live_jobs and endpoint and _RELAY_BULK_BUDGET and _RELAY_BULK_BUDGET.matches(endpoint):
                    interval = min(self.sample_seconds, 1.25)
                if future is not None and not future.done():
                    if not monitor:
                        if timeout is None:
                            in_flight.append((node, future))
                        else:
                            pending.append(future)
                    continue
                if monitor and time.monotonic()-getattr(node,'sampled_at',0)<interval:continue
                future=self.probes.submit(self._probe,node,not monitor)
                self.probe_futures[node.name]=future
                pending.append(future)
        if not monitor:
            for node, previous in in_flight:
                previous.result()
                with self.refresh_lock:
                    future=self.probe_futures.get(node.name)
                    if future is previous:
                        future=self.probes.submit(self._probe,node,True)
                        self.probe_futures[node.name]=future
                    pending.append(future)
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
                    paced_relay = _reconnect_intervals(node.client) is RELAY_RECONNECT_INTERVALS
                    # A recovered relay need not wait another minute to take
                    # work. Later attempts retain the existing minute spacing.
                    node.reconnect_after=time.time()+(5 if paced_relay else 60)
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
                    intervals = _reconnect_intervals(node.client)
                    if (not node.detached or node.enabled or not node.reconnect_after
                            or now < node.reconnect_after
                            or pending is not None and not pending.done()):
                        continue
                    index = node.reconnect_attempts
                    node.reconnect_after = now + (intervals[index] if index < len(intervals) else RECONNECT_BACKOFF_MAX)
                    node.reconnect_attempts = min(index + 1, len(intervals) + 1)
                    retry.append((node, node.connection_generation))
            # Persist the consumed attempt before starting its network request.
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
        connection_change=bool(set(changes)&{'enabled','url','ca_file','relay_url','peer_id'})
        with self.condition:
            if policy is not None:
                if policy not in self.POLICIES:
                    raise ValueError("Policy: adaptive, balanced, round_robin")
                self.policy = policy
            if name is not None:
                node = next(x for x in self.nodes if x.name == name)
                if set(changes) - {"weight", "limit", "enabled", "memory_limit_mb", "timeout", "host_ip", "url", "ca_file", "aliases", "relay_url", "peer_id", "display_name", "bulk_limit"}:
                    raise ValueError("Node settings: weight, limit, enabled, memory_limit_mb, timeout, host_ip, url, ca_file, aliases, relay_url, peer_id, display_name, bulk_limit")
                bulk_limit=changes.pop('bulk_limit',getattr(node.client,'bulk_limit',7))
                if type(bulk_limit)is not int or not 1<=bulk_limit<=7:
                    raise ValueError('Node bulk_limit must be an integer in 1..7')
                timeout=changes.pop('timeout',None)
                if timeout is not None and (isinstance(timeout,bool) or not isinstance(timeout,(int,float)) or not 0<timeout<float('inf')):
                    raise ValueError('Node timeout must be positive and finite')
                proposed = Node(node.name, node.client, **{
                    key: changes.get(key, getattr(node, key)) for key in ("weight", "limit", "enabled", "memory_limit_mb")})
                proposed.host_ip = changes.get('host_ip', getattr(node, 'host_ip', ''))
                proposed.aliases = changes.get('aliases',getattr(node,'aliases',[]))
                proposed.display_name = changes.get('display_name',getattr(node,'display_name',''))
                self._validate_node(proposed)
                address=changes.pop('url',node.client.url)
                ca_file=changes.pop('ca_file',getattr(node.client,'ca_file',None))
                relay_url=changes.pop('relay_url',getattr(node.client,'relay_url',None))
                peer_id=changes.pop('peer_id',getattr(node.client,'peer_id',None))
                if not isinstance(address,str) or urlsplit(address).scheme not in {'http','https','local'}:
                    raise ValueError('Node URL must use HTTP(S) or the private local endpoint')
                if any(other is not node and other.client.url==address.rstrip('/') for other in self.nodes):
                    raise ValueError('Configure each worker URL once')
                if (address.rstrip('/')!=node.client.url or ca_file!=getattr(node.client,'ca_file',None)
                        or relay_url!=getattr(node.client,'relay_url',None) or peer_id!=getattr(node.client,'peer_id',None)):
                    node.client=Client(address,node.client.token,timeout=timeout or node.client.timeout,ca_file=ca_file,operator=True,relay_url=relay_url,peer_id=peer_id)
                    node.connection_generation+=1
                    node.error='Node address changed; checking worker'
                node.client.bulk_limit=bulk_limit
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
        if name is not None:
            client=node.client
            result_client=getattr(client,'_result_client',None)
            if result_client is not None:
                result_client.bulk_limit=client.bulk_limit
            for routed in (client, result_client) if result_client is not None else (client,):
                pools=[getattr(routed,'_transport',None)]
                for key in ('_transports','_artifact_transports','_retirement_transports'):
                    pools.extend(getattr(routed,key,{}).copy().values())
                for pool in filter(None,pools):
                    pool.set_bulk_limit(client.bulk_limit)
            if connection_change:self._save_reconnect(node)

    def add(self, name, url, *, token="", weight=1.0, limit=0, timeout=10, host_ip="", ca_file=None, aliases=None, relay_url=None, peer_id=None, bulk_limit=7, display_name=""):
        if type(bulk_limit)is not int or not 1<=bulk_limit<=7:
            raise ValueError('Node bulk_limit must be an integer in 1..7')
        node = Node(name, Client(url, token,timeout=timeout,ca_file=ca_file,operator=True,relay_url=relay_url,peer_id=peer_id), weight=weight, limit=limit)
        node.client.bulk_limit=bulk_limit
        node.host_ip = host_ip
        node.aliases=[] if aliases is None else aliases
        node.display_name=display_name
        self._validate_node(node)
        with self.condition:
            if any(n.name == name or n.client.url == node.client.url for n in self.nodes):
                raise ValueError("Node name or URL already exists")
            self.nodes.append(node)
        self.refresh(nodes=[node])

    def snapshot(self):
        with self.condition:
            return {"policy": self.policy,
                "reconnect_policy": {"offline_seconds":60,"interval_seconds":60,
                                     "max_interval_seconds":RECONNECT_BACKOFF_MAX}, "nodes": [
                {"name": n.name, "url": n.client.url, "enabled": n.enabled,
                 "display_name":getattr(n,'display_name','')or n.name,
                 "bulk_limit":getattr(n.client,'bulk_limit',7),
                 "detached": n.detached,
                 **({'reconnect': {'attempts': n.reconnect_attempts, 'after': n.reconnect_after}} if n.detached else {}),
                 "host_ip": getattr(n,'host_ip','') or n.health.get('host_ip'),
                 "node_ip": n.health.get('node_ip') or (
                     n.node_ip_observation.get('ip') if n.node_ip_observation.get('worker') == n.health.get('node') else None),
                 "timeout": getattr(n.client,'timeout',10),
                 "ca_file":getattr(n.client,'ca_file',None),"aliases":getattr(n,'aliases',[]),
                 "relay_url":getattr(n.client,'relay_url',None),"peer_id":getattr(n.client,'peer_id',None),
                 "capacity": n.capacity, "base_capacity":n.base_capacity,
                 "cpu_overcommit_factor":n.health.get('cpu_overcommit_factor',1.0),
                 "cpu_overcommit_utilization":n.health.get('cpu_overcommit_utilization',90),
                 "memory_commit_headroom_mb":n.health.get('memory_commit_headroom_mb'),
                 "configured":n.health.get('configured',{}),
                 "reserved": n.reserved, "external_cpus": n.external,
                 "used_cpus": n.health.get("used_cpus", 0), "queued": n.health.get("queued", 0),
                 "cpu_percent": n.health.get("cpu_percent"), "logical_cpus": n.health.get("logical_cpus"),
                 "cpu_scope": n.health.get("cpu_scope"), "host_cpu_percent": n.health.get("host_cpu_percent"),
                 "benchmark": n.health.get("benchmark"), "benchmark_state": n.health.get("benchmark_state", "ready"), "asset_cache": n.health.get("asset_cache"),
                 "service": n.health.get("service"), "scratch_disk": n.health.get("scratch_disk"),
                 "memory_capacity_mb": n.memory_capacity, "memory_available_mb": max(0, int(n.memory_free)),
                 "memory_worker_available_mb": n.health.get('memory_available_mb',0),
                 "memory_unreported_mb": n.memory_unreported,
                 "memory_reserved_mb": n.health.get("memory_reserved_mb", 0),
                 "reserved_memory_mb": n.reserved_memory_mb, "memory_limit_mb": n.memory_limit_mb,
                 "memory_total_mb": n.health.get("memory_total_mb"),
                 "host_memory_available_mb": n.health.get("host_memory_available_mb"),
                 **{key:n.health.get(key)for key in ('host_commit_limit_mb','host_commit_available_mb','memory_headroom_mb','memory_reserve_mb',
                     'private_mb','memory_unallocated_mb','commit_unallocated_mb','memory_budget_mb')},
                 "machine_id": n.health.get("machine_id", n.client.url), "rss_mb": n.health.get("rss_mb", 0),
                 "paused": n.health.get("paused", False), "tags": n.health.get("tags", []),
                 "packages": package_names(n.health, include_base=False),
                 "capabilities": n.health.get("capabilities", []), "home": n.health.get("home"),
                 "accelerators": n.health.get("accelerators", []),
                 "accelerator_reserved": len({unit[:2] for items in n.accelerator_reservations.values() for unit in items}),
                 "accelerator_jobs": sum(bool(items) for items in n.accelerator_reservations.values()),
                 "accelerator_used": n.health.get("accelerator_used", 0),
                 "network": dict(getattr(n, 'network', {}) or {}),
                 "shared_paths": n.health.get("shared_paths", {}),
                 "active_jobs": n.health.get("active_jobs") or [],
                 "unavailable_shared": n.health.get("unavailable_shared", {}),
                 "tool_root": n.health.get("tool_root"),
                 **{key: n.health.get(key) for key in ('node', 'cpu_limit', 'temperature_c', 'peers', 'events', 'root', 'version','memory_pressure_state','work_reclaim','temp_reclaim')},
                 "weight": n.weight, "limit": n.limit, "completed": n.completed, "failed": n.failed,
                  "timing_labels": len(n.durations), "error": n.error} for n in self.nodes]}

    def _matching(self, task):
        cpus=startup_cpus(task)
        executable_ram_mb = task.get('executable_ram_mb', 0)
        required_tools,required_shared,python,backend=execution_bindings(task)
        required=capability_set(task.get('requires',()))
        package = task.get('_capability_package')
        result=[]
        for node in self.nodes:
            health=node.health
            if ((task.get('node')and node.name!=task['node'])or node.capacity<cpus
                    or task.get('min_start_cpus', 1) > 1 and STARTUP_CPU_FLOOR not in health.get('capabilities', ())
                    or task.get('platform') is not None and health.get('platform') != task['platform']
                    or executable_ram_mb and node.executable_ram_capacity < executable_ram_mb
                    or not supports_native_cache(health, task.get('native_cache'), getattr(node,'native_architecture',None))
                    or node.memory_capacity<node.startup_memory(task)):
                continue
            if any(key in task for key in ('_artifact_abi', 'produces_artifact_ref', 'checkpoint_output')):
                abi = provider_abi(health)
                if abi is None or task.get('_artifact_abi', abi) != abi:
                    continue
            if any(key in task for key in ('step_phases', 'timeout_grace_s', 'short_job', 'checkpoint_output')):
                if 'execution_profiles_v1' not in health.get('capabilities', ()):
                    continue
            if package:
                if not provider_has_package(health, package):
                    continue
            elif (not required.issubset(capability_set(health.get('tags',())))
                    or not required_shared.issubset(health.get('shared',()))
                    or not required_tools.issubset(execution_tool_set(tuple(name
                        for name,info in health.get('tools',{}).items()if info.get('available'))))):
                continue
            requirement=task.get('accelerator')
            if requirement is not None:
                requirement=node.execution_accelerator(task)
                if not node.supports_accelerator(requirement):continue
            selected=requirement.get('kind','any')if requirement is not None else backend
            if python and selected not in health.get('python_backends',['cpu']):continue
            result.append(node)
        return result

    def _ready_nodes(self, matching, task, *, needed_cpus, node_headroom, memory_floor, resource_recovery):
        executable_ram_mb = task.get('executable_ram_mb', 0)
        def available(node):
            return node.capacity-node.reserved-node.external-(node_headroom or {}).get(node.name,0)
        def memory_ready(node):
            floor=max(memory_floor,node.startup_memory(task))
            return (node.memory_free>=floor and self._host_memory_free(node)>=floor
                and not(resource_recovery and node.health.get('memory_pressure_state',{}).get('limited')))
        return [n for n in matching if not n.error and n.enabled and not n.health.get("paused")
                 and available(n) >= needed_cpus(n)
                 and memory_ready(n)
                 and (not executable_ram_mb or n.executable_ram_available >= executable_ram_mb)
                 and n.health.get("queued", 0) == 0
                 and (task.get('accelerator')is None or
                      n.can_match_accelerator(n.execution_accelerator(task,available=True)))]

    def _acquire(self, task, *, block=True, cancel_event=None, cpu_limit=None, partial_cpus=True, job_id=None, node_headroom=None,
                 memory_floor=0,resource_recovery=False):
        if not isinstance(task, dict) or not isinstance(task.get("steps"), list) or any(
                not isinstance(argv, list) or not argv for argv in task["steps"]):
            raise ValueError("Task requires nonempty argv steps")
        automatic = task.get('cpus') == 0
        cpus = startup_cpus(task)
        if automatic and cpu_limit is not None:
            cpu_limit = max(cpus, cpu_limit)
        memory_mb = task.get("memory_mb", 256)
        executable_ram_mb = task.get('executable_ram_mb', 0)
        label = task.get("label", "")
        with self.condition:
            while True:
                if self.closed:
                    raise RemoteError("Pool has closed")
                if cancel_event is not None and cancel_event.is_set():
                    return None
                matching = self._matching(task)
                if not matching:
                    if executable_ram_mb:
                        raise RemoteError('No worker meets the task requirements including executable RAM scratch')
                    if task.get('_capability_package'):
                        raise RemoteError('No worker offers the ' + task['_capability_package']
                                          + ' capability package for these resource requirements')
                    raise RemoteError("No matching online node for task platform, CPU count, tools, shared paths, node name and tags")
                def needed(node):
                    # Automatic jobs can start with a partial grant.
                    return min(cpu_limit,node.capacity,task.get('max_cpus',node.capacity)) if automatic and cpu_limit and not partial_cpus else cpus
                def available(node):
                    return node.capacity-node.reserved-node.external-(node_headroom or {}).get(node.name,0)
                ready = self._ready_nodes(matching, task, needed_cpus=needed, node_headroom=node_headroom,
                                          memory_floor=memory_floor, resource_recovery=resource_recovery)
                affinity = set(task.get('_affinity_assets', ()))
                if ready and affinity:
                    def preference(node):
                        session = tuple(node.health.get(key) for key in ('pid', 'root', 'asset_revision'))
                        known = len(affinity.intersection(node.known_assets)) if node.asset_session == session else 0
                        pending = len(affinity.intersection(set().union(*node.artifact_claims.values())))
                        return known, pending
                    scores = [(preference(node), node) for node in ready]
                    preferred = max(score for score, _ in scores)
                    ready = [node for score, node in scores if score == preferred]
                if ready:
                    if self.policy == "round_robin":
                        node = next((n for n in self.nodes[self.rotation:] + self.nodes[:self.rotation] if n in ready))
                        self.rotation = (self.nodes.index(node) + 1) % len(self.nodes)
                    else:
                        if elastic_cpus(task):
                            # A small startup grant is not its future phase
                            # width. Spread expandable jobs before filling a
                            # device; preserve load/speed ordering within it.
                            def idle(node):
                                machine=node.health.get('machine_id',node.client.url)
                                return not any(n.reserved or n.external for n in self.nodes
                                    if n.health.get('machine_id',n.client.url)==machine)
                            ready=[n for n in ready if idle(n)] or ready
                        pressures = {n.name: self._cpu_pressure(n, cpus) for n in ready}
                        least = min(ready, key=lambda n: pressures[n.name])
                        logical = lambda n: n.health.get('logical_cpus') or n.capacity
                        # CPU load has priority. Below one logical thread of
                        # difference, retain speed learning and operator weights.
                        close = [n for n in ready if pressures[n.name] - pressures[least.name]
                                 < max(1 / logical(n), 1 / logical(least))]
                        priors = self._speed_priors(close)
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
                    if task.get('accelerator')is not None:
                        requirement=node.execution_accelerator(task,available=True)
                        if node.reserve_accelerator(requirement,reservation_id)is None:continue
                        task['accelerator']=requirement
                    node.reserved += cpus
                    node.reserved_memory_mb += memory_mb
                    if executable_ram_mb:
                        node.ram_holds[reservation_id] = executable_ram_mb
                    if job_id:
                        node.known_jobs.add(job_id)
                        node.live_jobs.add(job_id)
                        if affinity:
                            node.artifact_claims[job_id] = affinity
                    return node
                if not block:
                    return None
                self.condition.wait(0.2)

    def _speed_priors(self, nodes):
        """Balance measured serial latency and throughput per CPU."""
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
        return {n.name: 1 / ((total / bulk_mean)**0.5 * (single / serial_mean)**0.5)
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
        available=physical-unallocated
        committed=[n for n in sampled if n.health.get('host_commit_available_mb')is not None]
        if committed:
            from .memory import COMMIT_EMERGENCY_MB
            # Physical reserve and commit availability are separate resources.
            commit=min(n.health['host_commit_available_mb']-n.health.get('memory_commit_headroom_mb',COMMIT_EMERGENCY_MB)for n in committed)
            unallocated_commit=sum(n.memory_unreported+n.health.get('commit_unallocated_mb',
                max(0,n.health.get('memory_reserved_mb',0)-n.health.get('private_mb',n.health.get('rss_mb',0))))for n in siblings)
            available=min(available,commit-unallocated_commit)
        return available

    def _release(self, node, task, result=None, *, job_id=None):
        label = task.get("label", "")
        with self.condition:
            node.release_accelerator(job_id or f'local:{id(task)}')
            node.reserved -= task.get("cpus", 1)
            node.reserved_memory_mb -= task.get("memory_mb", 256)
            node.live_jobs.discard(job_id)
            node.artifact_claims.pop(job_id, None)
            node.memory_holds.pop(job_id,None)
            node.ram_holds.pop(job_id or f'local:{id(task)}', None)
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
