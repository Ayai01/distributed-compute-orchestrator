"""Independent, bounded, read-only publisher for the private Nurikoi display."""
import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import gzip
import io
import json
import math
import os
from pathlib import Path
import signal
import subprocess
import sys
import threading
import time
from urllib.error import HTTPError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

from .bundle import atomic_json
from .runtimes import EXECUTION_CAPABILITIES
from .startup import Cancelled, single_instance
from .liveboard_wire import encode as encode_cbor
from .progress import SCHEMA, normalize as normalize_progress, diagnose

ROOT = Path(__file__).resolve().parent.parent
CONFIG = ROOT / '.local/liveboard.local.json'
MAX_BYTES = 65536
MAX_SOURCE_BYTES = 2 * 1024 * 1024
PUBLISHER_VERSION = '1.0.10'
ACTIVE_SECONDS, IDLE_SECONDS, HEARTBEAT_SECONDS = 15, 60, 60
_network_lock = threading.Lock()
_network_call = None


def stamp():
    return datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')


def encode(value):
    return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(',', ':')).encode('utf8')


def number(value):
    return value if type(value) in (int, float) and math.isfinite(value) else None


def text(value, limit=64):
    return ''.join(c for c in str(value or '') if c.isprintable())[:limit]


def reported_memory(job):
    phase=job.get('memory_phase')or job.get('memory_phase_model')or{}
    if phase.get('memory_mb')is not None:return number(phase['memory_mb'])
    base,per=number(phase.get('base_memory_mb')),number(phase.get('memory_per_cpu_mb'))
    if base is not None and per is not None:return base+per*max(1,job.get('cpus')or 1)
    return number(job.get('memory_mb'))


def project_sample(jobs, limit):
    """Fill a bounded activity snapshot one job per project per pass."""
    if len(jobs) <= limit:
        return jobs
    groups = {}
    for job in jobs:
        groups.setdefault(job['project'], []).append(job)
    streams = [iter(group) for group in groups.values()]
    selected = []
    while streams and len(selected) < limit:
        next_pass = []
        for stream in streams:
            job = next(stream, None)
            if job is not None:
                selected.append(job)
                if len(selected) == limit:
                    break
                next_pass.append(stream)
        streams = next_pass
    return selected


def _display_capabilities(node):
    tags = set(node.get('tags') or ())
    packages = node.get('packages')
    chisel = 'rtl' in packages if packages is not None else bool(tags & {'rtl', 'chisel'})
    tags.difference_update({'rtl', 'chisel'})
    if chisel:
        tags.add('chisel')
    return [name for name in EXECUTION_CAPABILITIES if name in tags]


def snapshot(health, nodes, *, now=None):
    """Allowlist fields; never copy source dictionaries or error messages."""
    now = time.time() if now is None else now
    rows, jobs, projects, machines, machine_rows, alerts = [], [], {}, {}, {}, []
    # Disabled historical registrations still appear in the resource control API.
    nodes = [n for n in nodes if n.get('enabled', True) or n.get('reserved')
             or n.get('external_cpus') or (n.get('memory_reserved_mb') or 0) > 0
             or any(j.get('status') in ('running', 'queued', 'dispatching') for j in n.get('active_jobs') or ())]
    for n in nodes:
        identity = text(n.get('name'), 48)
        name = text(n.get('display_name') or n.get('name'), 48)
        status = ('offline' if n.get('error') else 'disabled' if not n.get('enabled', True)
                  else 'paused' if n.get('paused') else 'online')
        total, free = number(n.get('memory_total_mb')), number(n.get('host_memory_available_mb'))
        used = max(0, total-free) if total is not None and free is not None else None
        base,factor=number(n.get('base_capacity')),number(n.get('cpu_overcommit_factor'))
        active=[j for j in n.get('active_jobs')or()if j.get('status')in('running','dispatching')]
        reported=[reported_memory(j)for j in active]
        rows.append(dict(id=identity, name=name, status=status,
                         cpu_used=number(n.get('reserved', 0)+n.get('external_cpus', 0)),
                         cpu_capacity=number(n.get('capacity')), cpu_base=number(n.get('base_capacity')),
                         cpu_total=number(n.get('logical_cpus')),
                         cpu_overcommit_factor=factor,
                         cpu_max_capacity=math.floor(base*factor)if base is not None and factor is not None else None,
                         cpu_percent=number(n.get('cpu_percent')), memory_used_mb=used,
                         memory_total_mb=total,
                         host_memory_total_mb=total,host_memory_used_mb=used,
                         memory_actual_mb=number(n.get('rss_mb')),
                         memory_reported_mb=sum(reported)if all(v is not None for v in reported)else None,
                         memory_reserved_mb=number(n.get('memory_reserved_mb')),
                         memory_reserve_mb=number(n.get('memory_reserve_mb')),
                         memory_headroom_mb=number(n.get('memory_headroom_mb')),
                         host_memory_floor_mb=number((n.get('memory_pressure_state')or{}).get('physical_threshold_mb')),
                         capabilities=_display_capabilities(n)))
        if status == 'offline':
            alerts.append(dict(code='node_offline', level='warning', node=name, text='设备离线'))
        if status != 'offline':
            machines.setdefault(n.get('machine_id') or name, n)
            machine_rows.setdefault(n.get('machine_id')or name,rows[-1])
        for j in n.get('active_jobs') or ():
            if j.get('status') not in ('running', 'queued', 'dispatching'):
                continue
            project = text(j.get('project') or 'default', 80)
            p = projects.setdefault(project, dict(name=project, running=0, queued=None,
                                                   cpu_used=0, completed=None, failed=None))
            p['running'] += int(j.get('status') == 'running')
            p['cpu_used'] += number(j.get('cpus')) or 0
            progress = display_progress(j)
            started = number(j.get('started'))
            jobs.append(dict(id=text(j.get('id'), 64), identity_scope=j.get('identity_scope','worker_attempt'),
                             tag=text(j.get('label'), 96), project=project, node=name,
                             status=text(j.get('status'), 16), cpus=number(j.get('cpus')),
                             max_cpus=number(j.get('max_cpus')), rss_mb=number(j.get('rss_mb')),
                             memory_reported_mb=reported_memory(j),
                             memory_reserved_mb=number(j.get('memory_reservation_mb')),
                             elapsed_seconds=int(max(0, now-started)) if started is not None else None,
                             progress=progress,diagnostic=diagnose(j,now=now,offline=status=='offline')))
            if j.get('observe_only'):
                if j.get('phase'):
                    jobs[-1]['phase'] = text(j['phase'], 80)
                for key in ('measured_cpus', 'cpu_seconds'):
                    value = number(j.get(key))
                    if value is not None:
                        jobs[-1][key] = round(value, 2)
    for j in health.get('queued_jobs')or():
        project=text(j.get('project')or'default',80)
        projects.setdefault(project,dict(name=project,running=0,queued=None,cpu_used=0,completed=None,failed=None))
        jobs.append(dict(id=text(j.get('id'),64),identity_scope='task',tag=text(j.get('label'),96),
            project=project,node=None,status='queued',cpus=0,max_cpus=number(j.get('max_cpus')),
            progress=None,diagnostic=diagnose(dict(j,status='queued'),now=now)))
    for name,p in projects.items():
        resource=(health.get('projects')or{}).get(name,{})
        p['queued']=number(resource.get('queued'))
        p['resource_share']=number(resource.get('resource_share'))
    if health.get('paused'):
        alerts.append(dict(code='dispatch_paused', level='info', text='新派发已暂停'))
    if not health.get('accepting', True):
        alerts.append(dict(code='admission_closed', level='info', text='新任务受理已关闭'))
    physical = list(machines.values())
    weighted = [(number(n.get('cpu_percent')), number(n.get('logical_cpus'))) for n in physical]
    weighted = [(p, c) for p, c in weighted if p is not None and c is not None and c > 0]
    def memory_sum(key):
        values = [number(n.get(key)) for n in physical]
        return sum(values) if values and all(v is not None for v in values) else None
    def display_memory_sum(key):
        values=[row[key]for row in machine_rows.values()]
        return sum(values)if values and all(v is not None for v in values)else None
    total, free = memory_sum('memory_total_mb'), memory_sum('host_memory_available_mb')
    mode = 'hybrid' if any(str(n.get('url', '')).startswith('local://') for n in nodes) else 'scheduler'
    all_jobs = len(jobs)
    running=sorted((j for j in jobs if j['status']in('running','dispatching')),key=lambda j:(-(j['cpus']or 0),j['id']))
    queued=[j for j in jobs if j['status']=='queued']
    display_jobs=project_sample(running,32-min(8,len(queued)))+project_sample(queued,8)
    from .progress import LABELS
    for job in jobs:
        d=job.get('diagnostic',{})
        if d.get('level') in ('warning','error'):
            alerts.append(dict(code=d['code'],level=d['level'],job_id=job['id'],
                               text=LABELS.get(d['code'],d['code'])+' · '+job['id'][:8]))
    result = dict(schema='dco.live.v1', sampled_at=stamp(),
                  source=dict(name='DCO', version=text(health.get('version'), 24), mode=mode,
                              uptime_seconds=None, status='online'),
                  summary=dict(running=sum(j['status'] == 'running' for j in jobs),
                               queued=number(health.get('queued')), completed=None, failed=None,
                               cpu_used=number(health.get('used_cpus')),
                               cpu_capacity=number(health.get('cpus')),
                               cpu_base=sum(r['cpu_base'] or 0 for r in rows if r['status'] in ('online', 'paused')),
                               cpu_total=memory_sum('logical_cpus'),
                               cpu_max_capacity=sum(r['cpu_max_capacity']or 0 for r in rows if r['status']in('online','paused')),
                               cpu_percent=round(sum(p*c for p,c in weighted)/sum(c for _,c in weighted), 1) if weighted else None,
                               memory_used_mb=max(0, total-free) if total is not None and free is not None else None,
                               memory_total_mb=total,
                               memory_actual_mb=display_memory_sum('memory_actual_mb'),
                               memory_reported_mb=display_memory_sum('memory_reported_mb'),
                               memory_reserved_mb=display_memory_sum('memory_reserved_mb'),
                               memory_reserve_mb=display_memory_sum('memory_reserve_mb'),
                               host_memory_floor_mb=display_memory_sum('host_memory_floor_mb')),
                  nodes=rows, projects=sorted(projects.values(), key=lambda p: (-p['cpu_used'], p['name']))[:16],
                  jobs=display_jobs[:32],
                  alerts=alerts[:8],
                  coverage=dict(nodes_total=len(rows), projects_active_total=len(projects), jobs_active_total=all_jobs,
                                queued_job_details=bool(queued), history=False))
    # Bound the complete upload, including its JSON envelope and UTF-8 labels.
    for field in ('jobs', 'projects', 'nodes'):
        while len(encode(dict(data=result))) > MAX_BYTES and result[field]:
            result[field].pop()
    if len(encode(dict(data=result))) > MAX_BYTES:
        raise ValueError('Display snapshot exceeds size budget')
    return result


def display_progress(job):
    p=job.get('progress')
    if isinstance(p,dict)and p.get('schema')==SCHEMA:
        try:return normalize_progress(p)
        except ValueError:return None
    done,count=number(job.get('completed')),number(job.get('total'))
    return (dict(completed=done,total=count,ratio=done/count)
            if done is not None and count is not None and 0<=done<=count and count>0 else None)


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


def read_json(opener, request, timeout, limit):
    # One unsettled daemon call bounds even OS DNS/connect stalls. A timeout
    # never adds another call or an offline queue while that request remains.
    global _network_call
    deadline = time.monotonic()+timeout
    with _network_lock:
        if _network_call is not None and not _network_call['done'].is_set():
            raise TimeoutError('Previous display request remains unsettled')
        call = dict(done=threading.Event())
        _network_call = call

        def receive():
            try:
                with opener.open(request, timeout=timeout) as response:
                    data = bytearray()
                    while True:
                        remaining = deadline-time.monotonic()
                        if remaining <= 0:
                            raise TimeoutError('Display response deadline reached')
                        sock = getattr(getattr(getattr(response, 'fp', None), 'raw', None), '_sock', None)
                        if sock is not None:
                            sock.settimeout(remaining)
                        part = response.read1(min(8192, limit+1-len(data)))
                        if not part:
                            break
                        data.extend(part)
                        if len(data) > limit:
                            raise ValueError('Response exceeds display budget')
                if time.monotonic() > deadline:
                    raise TimeoutError('Display response deadline reached')
                call['value'] = json.loads(data)
            except Exception as error:
                if isinstance(error, HTTPError):
                    error.close()
                call['error'] = error
            finally:
                call['done'].set()

        threading.Thread(target=receive, name='dco-display-io', daemon=True).start()
    if not call['done'].wait(max(0, deadline-time.monotonic())):
        raise TimeoutError('Display request deadline reached')
    if 'error' in call:
        raise call['error']
    return call['value']


def collect(config):
    url = config.get('source_url', 'http://127.0.0.1:26133').rstrip('/')
    target = urlsplit(url)
    if (target.scheme != 'http' or target.hostname not in ('127.0.0.1', 'localhost', '::1')
            or target.username or target.password or target.query or target.fragment or target.path):
        raise ValueError('Display source must be a loopback DCO endpoint')
    token = Path(config['operator_token_file']).read_text(encoding='utf8').strip()
    opener = build_opener(ProxyHandler({}), NoRedirect())
    started = time.perf_counter()
    # This existing endpoint waits behind scheduler planning. Allow the normal
    # loaded response while keeping the wait isolated in this bounded sidecar.
    health = read_json(opener, Request(url+'/health',headers={
        'Authorization':'Bearer '+token,'X-DCO-Role':'operator','X-DCO-Health-Display':'1'}), 5, MAX_SOURCE_BYTES)
    if not isinstance(health, dict) or not isinstance(health.get('nodes'), list):
        raise ValueError('Invalid display source response')
    if not all(isinstance(n, dict) for n in health['nodes']):
        raise ValueError('Invalid display source nodes')
    value = snapshot(health, health['nodes'])
    if number(health.get('health_sampled_at')) is not None:
        value['sampled_at'] = datetime.fromtimestamp(health['health_sampled_at'],timezone.utc).isoformat().replace('+00:00','Z')
    return value, dict(collect_ms=round((time.perf_counter()-started)*1000, 3),
                       source_bytes=len(encode(health)),source_queries=1)


def fingerprint(value):
    return hashlib.sha256(encode({k:v for k,v in value.items() if k != 'sampled_at'})).digest()


def due(signature, previous, now, last):
    return signature != previous or now-last >= HEARTBEAT_SECONDS


def backoff(failures):
    return min(300, 5 * 2**min(max(0, failures-1), 6))


def compact_update(previous, value, sequence):
    if previous.keys() != value.keys() or value.get('schema') != 'dco.live.v1':
        return None
    fields, maps, rows = {}, {}, {}
    for key, item in value.items():
        before = previous[key]
        if item == before:
            continue
        if key in ('summary', 'source', 'coverage') and item.keys() == before.keys():
            maps[key] = {name: field for name, field in item.items() if field != before[name]}
        elif key in ('nodes', 'projects', 'jobs') and len(item) == len(before) and all(
                old.keys() == new.keys() for old, new in zip(before, item)):
            rows[key] = {str(i): {name: field for name, field in new.items() if field != old[name]}
                         for i, (old, new) in enumerate(zip(before, item)) if old != new}
        else:
            fields[key] = item
    return {'patch': {'base': sequence, 'set': fields, 'maps': maps, 'rows': rows}}


def upload_body(envelope, transport):
    if transport == 'json':
        return encode(envelope), {'Content-Type': 'application/json'}
    stream = io.BytesIO()
    with gzip.GzipFile(fileobj=stream, mode='wb', compresslevel=3, mtime=0) as compressor:
        compressor.write(encode_cbor(envelope))
    return stream.getvalue(), {'Content-Type': 'application/cbor', 'Content-Encoding': 'gzip'}


def upload(config, value, *, baseline=None):
    private = json.loads(Path(config['upload_file']).read_text(encoding='utf-8-sig'))
    url, token = private['url'], private['token']
    target = urlsplit(url)
    if (target.scheme != 'https' or target.username or target.password or target.query or target.fragment
            or not target.path.startswith('/manage/live/') or not target.path.endswith('/update/')
            or not isinstance(token, str) or not 32 <= len(token) <= 128 or '\n' in token or '\r' in token):
        raise ValueError('Invalid display upload configuration')
    envelope=dict(data=value)
    if len(encode(envelope)) > MAX_BYTES:
        raise ValueError('Display upload exceeds size budget')
    transport=config.get('transport','json')
    if transport not in ('json','cbor-gzip'):raise ValueError('Unsupported display transport')
    full = upload_body(envelope, transport)
    selected, kind = full, 'full'
    destination = (url, token)
    if baseline and baseline.get('destination') == destination and type(baseline.get('sequence')) is int:
        delta = compact_update(baseline['value'], value, baseline['sequence'])
        if delta is not None:
            plain = upload_body(delta, 'json')
            compressed = upload_body(delta, transport)
            candidate = min((plain, compressed), key=lambda variant: len(variant[0]))
            if len(candidate[0]) < len(full[0]):
                selected = candidate
                kind = 'heartbeat' if baseline['signature'] == fingerprint(value) else 'delta'
    opener = build_opener(ProxyHandler({}), NoRedirect())

    def send(variant):
        data, headers = variant
        request = Request(url, data=data, headers={**headers, 'Authorization': 'Bearer '+token,
                          'X-DCO-Liveboard-Compact': '1'}, method='POST')
        return read_json(opener, request, 4, 16384)

    size = len(selected[0])
    try:
        reply = send(selected)
    except HTTPError as error:
        if kind == 'full' or error.code not in (400, 409):
            raise
        error.close()
        # A lost reply, a restarted server, or another publisher can invalidate
        # the base. Re-send the complete current sample once without a backlog.
        reply = send(full)
        size += len(full[0])
        kind = 'resync'
    if not isinstance(reply, dict) or not isinstance(reply.get('instance'), dict):
        raise ValueError('Invalid display upload response')
    sequence = number(reply['instance'].get('sequence'))
    if baseline is not None:
        baseline.clear()
        baseline.update(kind=kind)
        if reply.get('compact') == 1 and type(sequence) is int:
            baseline.update(destination=destination, sequence=sequence, value=value, signature=fingerprint(value))
    return size, sequence, reply.get('viewer_active') is True


class Owner:
    """One OS handle keeps ownership stable even if a PID is later reused."""
    def __init__(self, pid):
        self.pid, self.handle = pid, None
        if pid and os.name == 'nt':
            import ctypes
            from ctypes import wintypes
            self.kernel = ctypes.WinDLL('kernel32', use_last_error=True)
            self.kernel.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
            self.kernel.OpenProcess.restype = wintypes.HANDLE
            self.kernel.WaitForSingleObject.argtypes = [wintypes.HANDLE, wintypes.DWORD]
            self.kernel.CloseHandle.argtypes = [wintypes.HANDLE]
            self.handle = self.kernel.OpenProcess(0x100000, False, pid)

    def alive(self):
        if not self.pid:
            return True
        if os.name == 'nt':
            return bool(self.handle) and self.kernel.WaitForSingleObject(self.handle, 0) == 258
        try:
            os.kill(self.pid, 0)
            return True
        except ProcessLookupError:
            return False

    def close(self):
        if self.handle:
            self.kernel.CloseHandle(self.handle)


def run(config_path, *, owner_pid=None, once=False, send=True):
    config_path = Path(config_path).resolve()
    owner, stopped = Owner(owner_pid), threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stopped.set())
    state, previous, last = dict(publisher_version=PUBLISHER_VERSION, pid=os.getpid(), owner_pid=owner_pid, posts=0, failures=0), None, float('-inf')
    baseline = {}
    try:
        with single_instance('liveboard:'+str(config_path), wait=5):
            while owner.alive() and not stopped.is_set():
                config = json.loads(config_path.read_text(encoding='utf-8-sig'))
                state_path = Path(config['state_file'])
                if config.get('enabled') is not True:
                    break
                # These are current runtime outputs, including on a fresh migration.
                for key in ('state_file', 'sample_file'):
                    Path(config[key]).parent.mkdir(parents=True, exist_ok=True)
                wait = IDLE_SECONDS
                try:
                    value, metrics = collect(config)
                    state.update(metrics, sampled_at=value['sampled_at'], snapshot_bytes=len(encode(dict(data=value))),
                                 transport=config.get('transport','json'))
                    atomic_json(config['sample_file'], value, private=True)
                    signature = fingerprint(value)
                    if not owner.alive() or stopped.is_set():
                        break
                    wait = ACTIVE_SECONDS if state.get('viewer_active') else IDLE_SECONDS
                    if send and due(signature, previous, time.monotonic(), last):
                        started = time.perf_counter()
                        size, sequence, viewer_active = upload(config, value, baseline=baseline)
                        state.update(posts=state['posts']+1, last_uploaded_at=stamp(), upload_bytes=size,
                                     upload_ms=round((time.perf_counter()-started)*1000, 3), server_sequence=sequence,
                                     viewer_active=viewer_active, upload_kind=baseline.get('kind'))
                        wait = ACTIVE_SECONDS if viewer_active else IDLE_SECONDS
                        previous, last = signature, time.monotonic()
                    state.update(status='online', failures=0, error=None)
                except (OSError, ValueError, KeyError) as error:
                    state['failures'] += 1
                    wait = backoff(state['failures'])
                    # Exception strings may contain URLs or private token paths.
                    state.update(status='retrying', error='http_'+str(error.code) if isinstance(error, HTTPError) else type(error).__name__)
                state.update(updated_at=stamp(), next_interval_seconds=wait)
                atomic_json(state_path, state, private=True)
                if once:
                    return state
                until = time.monotonic()+wait
                while time.monotonic() < until and owner.alive() and not stopped.wait(min(1, max(0, until-time.monotonic()))):
                    if json.loads(config_path.read_text(encoding='utf-8-sig')).get('enabled') is not True:
                        stopped.set()
            if 'state_path' in locals():
                state.update(status='stopped', updated_at=stamp())
                atomic_json(state_path, state, private=True)
    finally:
        owner.close()


@contextmanager
def supervised_publisher(root):
    """Optional sidecar; its startup and failures never gate DCO readiness."""
    config = Path(root)/'.local/liveboard.local.json'
    process = None
    try:
        settings = json.loads(config.read_text(encoding='utf-8-sig')) if config.is_file() else None
        if isinstance(settings, dict) and settings.get('enabled') is True:
            process = subprocess.Popen([sys.executable, '-B', '-m', 'remote_compute.liveboard',
                '--config', str(config), '--owner-pid', str(os.getpid())], cwd=root,
                stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
    except (OSError, ValueError):
        pass
    try:
        yield
    finally:
        if process and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, default=CONFIG)
    parser.add_argument('--owner-pid', type=int)
    parser.add_argument('--once', action='store_true')
    parser.add_argument('--no-upload', action='store_true')
    parser.add_argument('--stop', action='store_true')
    args = parser.parse_args()
    if args.stop:
        config = json.loads(args.config.read_text(encoding='utf-8-sig'))
        atomic_json(args.config, dict(config, enabled=False), private=True)
        return 0
    try:
        state = run(args.config, owner_pid=args.owner_pid, once=args.once, send=not args.no_upload)
        if args.once:
            print(json.dumps(state, ensure_ascii=False))
        return 0
    except Cancelled:
        return 0


if __name__ == '__main__':
    raise SystemExit(main())
