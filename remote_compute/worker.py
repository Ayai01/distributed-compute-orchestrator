"""One HTTP worker, one CPU budget, and one private directory per job."""

from collections import deque
from contextlib import nullcontext
import hashlib
import gzip
import hmac
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
import json
import math
import os
from pathlib import Path
import re
import shutil
import socket
import select
import ssl
import sys
import threading
import tempfile
import time
import weakref
import zipfile
from urllib.parse import parse_qs, unquote, urlsplit

from .bundle import (MAX_BODY, PROTOCOL, TERMINAL, atomic_json, inside, json_bytes, pack,native_path,
                     tree, unpack, progress, progress_fields)
from .runtimes import (EXECUTION_CAPABILITIES, CHISEL_TOOLS, LAYOUT_TOOLS,
                       python_backend, python_accelerator, supports_python, uses_python, execution_tool)
from . import __version__
from .process import Process, low_integrity_directory
from .resources import (Host, CpuAccounting, process_usage, cpu_benchmark, service_usage,
                        DEFAULT_MEMORY_RESERVE_MB, DEFAULT_MEMORY_HEADROOM_MB)
from .telemetry import Activity
from .client import RemoteError
from .cleanup import AssetCache, Reclaimer, remove_owned
from .records import Job, JobRecords, active_items, active_values, archive_job, asset_references, load_job
from .compat import unlink
from .receipts import read_receipt
from .paths import cache_environment
from .allocation import plan_resize,cpu_capacity,validate_cpu_overcommit
from .memory import MemoryObservation, reservation, startup_memory, calibrated_model, plan_memory, pressure_victim, same_phase, PressureGuard
from .policy import (MAX_PRIORITY, MIN_PRIORITY, MAX_REQUIREMENTS, validate_status_ids,
                     MAX_REQUIREMENT_LENGTH, MAX_RESOURCE_SHARE,
                     MAX_SINGLE_THREAD_WEIGHT, normalize_accelerator_requirement, select_accelerator_units,
                     normalize_accelerators, aggregate_accelerators, canonical_capabilities,
                     canonical_capability, clean_tool_snapshot, validate_platform)

_ID = re.compile(r"[a-f0-9]{32}\Z")
_ASSET = re.compile(r"[a-f0-9]{64}\Z")
_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_-]*\Z")
_VAR = re.compile(r"\$\{(work|cpus|exe|dco|tool:[A-Za-z_][A-Za-z0-9_+.-]*|asset:[\w-]+|shared:[\w-]+)\}")


class Conflict(ValueError):
    pass


def missing_task_assets(task):
    """Find undeclared assets in fields the worker resolves before admission."""
    def strings(value):
        if isinstance(value,str):yield value
        elif isinstance(value,dict):
            for item in value.values():yield from strings(item)
        elif isinstance(value,(list,tuple)):
            for item in value:yield from strings(item)
    fields=[task.get('cwd',''),task.get('steps',[]),task.get('env',{}),task.get('execution',{})]
    return sorted({match[1][6:] for value in strings(fields)for match in _VAR.finditer(value)
                   if match[1].startswith('asset:')and match[1][6:]not in task.get('assets',{})})


def asset_handoff(service, identity, assets=None):
    """Hold an original request's inputs across a slow resynchronization."""
    if not _ID.fullmatch(identity):
        raise ValueError('Job id requires 32 lowercase hex characters')
    with service.asset_cache.lock, service.condition:
        if assets is None:
            service.asset_handoffs.pop(identity,None)
        else:
            if service.stopping:
                raise Conflict('Worker is stopping')
            if not isinstance(assets,list) or any(not isinstance(a,str) or not _ASSET.fullmatch(a) for a in assets):
                raise ValueError('Asset handoff requires synchronized SHA256 ids')
            # This is only a cache reference, never a task or CPU grant.
            # Abandoned transfers expire; each input check/upload renews it.
            now=time.monotonic()
            service.asset_handoffs={key:value for key,value in service.asset_handoffs.items() if value[1]>now}
            if identity not in service.jobs:
                service.asset_handoffs[identity]=(frozenset(assets),now+3600)
        return {'id':identity}


def retire_asset(service, identity):
    """The operator releases one input after its producers and receipts close."""
    if not isinstance(identity,str)or not _ASSET.fullmatch(identity):
        raise ValueError('Asset id requires SHA256')
    with service.asset_cache.lock, service.condition:
        if service.stopping:raise Conflict('Service is stopping')
        references=asset_references(service.jobs)
        references.update(getattr(service,'accepting_assets',()))
        now=time.monotonic()
        for assets,expires in service.asset_handoffs.values():
            if expires>now:references.update(assets)
        transfer=service.asset_writes.get(identity)
        if identity in references or transfer is not None:
            raise Conflict('Asset is still referenced by a task, producer or upload')
        result=service.asset_cache.retire(identity)
        # Legacy uploads have no original-ID handoff. Only this explicit
        # operator release removes their warm-input ownership hint.
        if hasattr(service,'legacy_assets'):service.legacy_assets.discard(identity)
        return result


def lock_root(root, role):
    root.mkdir(parents=True, exist_ok=True)
    handle = (root / (role + ".lock")).open("a+b")
    try:
        if os.name == "nt":
            import msvcrt
            if handle.tell() == 0:
                handle.write(b"0")
                handle.flush()
            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        handle.close()
        raise Conflict("Another " + role + " is using " + str(root)) from None
    return handle


def cpu_budget(value=0):
    count = os.cpu_count() or 1
    limit = max(1, count)
    if int(value) < 0 or int(value) > limit:
        raise ValueError(f'CPU budget must be in 1..{limit}, or 0/auto')
    return int(value) or limit


def validate_task(task, capacity, *, allow_auto=False):
    if not isinstance(task, dict):
        raise ValueError("Task must be an object")
    unknown = set(task) - {"steps", "cpus", "max_cpus", "execution", "memory_model", "timeout", "outputs", "env", "assets", "render", "label", "priority", "requires", "node", "memory_mb", "cwd", "stdin", "project", "resource_share", "single_thread_weight", "retries", "accelerator", "platform", "cache_ref"}
    if unknown:
        raise ValueError("Unknown task fields: " + ", ".join(sorted(unknown)))
    maximum=task.get('max_cpus',task.get('cpus') or 1024)
    task = {"cpus": 0 if allow_auto else 1, "max_cpus": maximum, "timeout": 300, "outputs": [], "env": {}, "assets": {},
            "render": [], "label": "", "priority": 0, "requires": [], "node": None, "memory_mb": 256,
            "accelerator": None, **task}
    cpus, timeout = task["cpus"], task["timeout"]
    if task.get('cache_ref')is not None:
        from .cache_refs import reference_id
        reference_id(task['cache_ref'])
    if 'execution' in task:
        from .execution import validate_execution
        validate_execution(task['execution'])
    if 'memory_model'in task:
        model=task['memory_model']
        if not isinstance(model,dict)or set(model)-{'base_memory_mb','memory_per_cpu_mb','memory_limit_mb'}:
            raise ValueError('memory_model requires base/per-CPU memory and optional limit')
        base,per=model.get('base_memory_mb',0),model.get('memory_per_cpu_mb',0)
        width,required=plan_resize(1,None,base,per,model.get('memory_limit_mb'),task['memory_mb'])
        if width!=1 or required>task['memory_mb']:raise ValueError('Initial memory must fit one execution slot')
    if type(task.get('retries',2)) is not int or not 0<=task.get('retries',2)<=10:
        raise ValueError('retries must be an integer in 0..10')
    if not isinstance(task.get('project', ''), str):
        raise ValueError('project must be a string')
    share = task.get('resource_share', 1.0)
    if (isinstance(share, bool) or not isinstance(share, (int, float))
            or not math.isfinite(share) or not 0 < share <= MAX_RESOURCE_SHARE):
        raise ValueError(f'resource_share must be in (0, {MAX_RESOURCE_SHARE:g}]')
    serial = task.get('single_thread_weight', 1.0)
    if (isinstance(serial, bool) or not isinstance(serial, (int, float))
            or not math.isfinite(serial) or not 0 < serial <= MAX_SINGLE_THREAD_WEIGHT):
        raise ValueError(f'single_thread_weight must be in (0, {MAX_SINGLE_THREAD_WEIGHT:g}]')
    task['accelerator'] = normalize_accelerator_requirement(task.get('accelerator'))
    task['requires'] = canonical_capabilities(task.get('requires', ()))
    validate_platform(task.get('platform'))
    if type(cpus) is not int or not (0 if allow_auto else 1) <= cpus <= capacity:
        raise ValueError(f"Task cpus must be in 1..{capacity}")
    if 'max_cpus' in task and (type(task['max_cpus']) is not int or task['max_cpus']<1 or cpus>task['max_cpus']):
        raise ValueError('max_cpus must be a positive integer no smaller than cpus')
    if type(task["memory_mb"]) is not int or task["memory_mb"] < 1:
        raise ValueError("memory_mb must be a positive integer in MiB")
    if (isinstance(timeout, bool) or not isinstance(timeout, (float, int))
            or not math.isfinite(timeout) or timeout <= 0):
        raise ValueError("Task timeout must be a positive number")
    steps = task.get("steps")
    if (not isinstance(steps, list) or not steps or any(
            not isinstance(argv, list) or not argv or any(not isinstance(x, str) or "\0" in x for x in argv)
            for argv in steps)):
        raise ValueError("steps must be a nonempty list of argv lists")
    for key in ("outputs", "render"):
        if not isinstance(task[key], list) or any(not isinstance(x, str) for x in task[key]):
            raise ValueError(key + " must be a list of relative file patterns")
        for name in task[key]:
            inside(Path.cwd(), name)
    if not isinstance(task["env"], dict) or any(
            not isinstance(k, str) or not k or "=" in k or "\0" in k
            or not isinstance(v, str) or "\0" in v for k, v in task["env"].items()):
        raise ValueError("env must contain string names and values")
    if not isinstance(task["assets"], dict) or any(
            not isinstance(k, str) or not _NAME.fullmatch(k) or not isinstance(v, str)
            or not _ASSET.fullmatch(v) for k, v in task["assets"].items()):
        raise ValueError("assets must map names to synchronized SHA256 ids")
    if not isinstance(task["label"], str):
        raise ValueError("label must be a string")
    if (type(task["priority"]) is not int
            or not MIN_PRIORITY <= task["priority"] <= MAX_PRIORITY):
        raise ValueError(f"priority must be an integer in {MIN_PRIORITY}..{MAX_PRIORITY}")
    if (not isinstance(task["requires"], list)
            or len(task["requires"]) > MAX_REQUIREMENTS
            or any(not isinstance(x, str) or not x or len(x) > MAX_REQUIREMENT_LENGTH
                   for x in task["requires"])):
        raise ValueError(f"requires must contain at most {MAX_REQUIREMENTS} short capability tags")
    if task["node"] is not None and not isinstance(task["node"], str):
        raise ValueError("node must be a configured node name")
    if "cwd" in task and (not isinstance(task["cwd"], str) or not task["cwd"] or "\0" in task["cwd"]):
        raise ValueError("cwd must be an absolute worker path or ~ / a shared path variable")
    if "stdin" in task:
        if not isinstance(task["stdin"], str):
            raise ValueError("stdin must name an input file")
        inside(Path.cwd(), task["stdin"])
    return task


class Worker:
    def __init__(self, root, cpus=0, *, tools=None, shared=None, env=None, token="", tags=(), memory_mb=0, memory_reserve_mb=DEFAULT_MEMORY_RESERVE_MB, memory_headroom_mb=DEFAULT_MEMORY_HEADROOM_MB, cpu_limit=None, cpu_overcommit_factor=1.5, temp_root=None, cache_root=None, accelerators=None, python_runtimes=None):
        started=time.monotonic()
        self.startup={}
        self.root = Path(root).expanduser().resolve()
        self.cache_root = Path(cache_root).expanduser().resolve() if cache_root else self.root / 'cache'
        self.temp_root=Path(temp_root).expanduser().resolve() if temp_root else None
        if self.temp_root:
            self.temp_root/=hashlib.sha256(str(self.root).encode()).hexdigest()[:12]
            self.temp_root.mkdir(parents=True,exist_ok=True,mode=0o700)
        self.callback = {}
        physical_limit = max(1, os.cpu_count() or 1)
        if cpu_limit is not None and (type(cpu_limit) is not int or not 1 <= cpu_limit <= physical_limit):
            raise ValueError(f'cpu_limit must be in 1..{physical_limit}')
        self.cpu_limit = physical_limit if cpu_limit is None else cpu_limit
        self.capacity = cpu_budget(cpus) if cpus else self.cpu_limit
        if self.capacity > self.cpu_limit:
            raise ValueError(f'CPU budget exceeds configured cpu_limit={self.cpu_limit}')
        self.cpu_overcommit_factor=validate_cpu_overcommit(cpu_overcommit_factor)
        self.activity = Activity()
        self.save_settings = None
        self.configuration_revision = 0
        self.token = token
        self.accelerators = normalize_accelerators(accelerators)
        for profile in self.accelerators:
            profile['tags'] = canonical_capabilities(profile['tags'] + ['gpu:shared'])
        self.accelerator_reservations = {}
        self.configured_tags = set(canonical_capabilities(tags))
        self.paused = False
        self.host = Host()
        self.pressure_guard=PressureGuard()
        self.memory_wait_file=self.root/'memory-pressure.wait'
        unlink(self.memory_wait_file)
        self.memory_wait_signaled=False
        total_mb = self.host.sample()["memory_total_mb"]
        if any(type(value) is not int or value<0 for value in (memory_mb,memory_reserve_mb,memory_headroom_mb)):
            raise ValueError("Memory budget and headroom must be nonnegative integer MiB")
        self.memory_budget = memory_mb
        self.memory_capacity = memory_mb or max(0, total_mb - memory_reserve_mb)
        self.memory_reserve = memory_reserve_mb
        self.memory_headroom = memory_headroom_mb
        self.env = dict(env or {})
        self.shared, self.unavailable_shared = {}, {}
        for name, value in (shared or {}).items():
            path = Path(value).expanduser().absolute().resolve()
            (self.shared if path.is_dir() else self.unavailable_shared)[name] = path.as_posix()
        self.restart_available = False
        self.tools = {"python": [sys.executable], "ngspice": ["ngspice"],
                      "cxx": [shutil.which('c++') or shutil.which('g++') or 'c++'],
                      "verilator": [sys.executable, str(Path(__file__).with_name("rtl.py"))]}
        for name, value in (tools or {}).items():
            self.tools[name] = [value] if isinstance(value, str) else list(value)
        self.tools,self.tool_info=self._probe_tools(self.tools)
        runtimes=python_runtimes or {}
        if not isinstance(runtimes,dict) or set(runtimes)-{'cpu','ml','cuda','rocm','xpu'}:
            raise ValueError('python_runtimes maps cpu/ml/cuda/rocm/xpu to interpreter argv')
        self.python_runtimes={}
        for backend,argv in runtimes.items():
            resolved,info=self._probe_tools({'python':argv})
            if not info['python']['available']:raise ValueError('Python backend executable not found: '+backend)
            self.python_runtimes[backend]=resolved['python']
        if 'cpu' in self.python_runtimes:
            self.tools['python']=self.python_runtimes['cpu']
            self.tool_info['python']=dict(argv=self.tools['python'],available=True)
        else:self.python_runtimes['cpu']=self.tools['python']
        self.tool_tags = set(canonical_capabilities(self.tools)) | set(EXECUTION_CAPABILITIES)
        self._refresh_tags()
        # The lock is released by the OS on exit; no stale PID files to repair.
        self.root_lock = lock_root(self.root, "worker")
        self.temp_reclaimer=Reclaimer(self.temp_root/'.trash')if self.temp_root else None
        benchmark_python=self.tools['python'][0]
        benchmark_env=tuple((name,self.env[name]) for name in ('PATH','LD_LIBRARY_PATH','OPENBLAS_CORETYPE','MKL_ENABLE_INSTRUCTIONS')
                            if name in self.env)
        self.benchmark_python = benchmark_python
        self.benchmark_env = benchmark_env
        self.benchmark = dict(cpu_benchmark(self.capacity,benchmark_python,benchmark_env))
        self.benchmark_state = 'ready' if self.benchmark.get('available') else 'unavailable'
        self.benchmark_thread = None
        (self.root / "jobs").mkdir(exist_ok=True)
        (self.root / "assets").mkdir(exist_ok=True)
        self.condition = threading.Condition(threading.RLock())
        self.start_lock = threading.Lock()
        self.next_start = 0.0
        self.start_interval = .5
        # Keep one bounded history index per worker root.  Sharing the parent
        # index would mix local-fresh (and any future worker roots) and force
        # startup to reopen unrelated job directories.
        self.jobs = JobRecords(self.root / 'jobs', index_path=self.root / 'history.index.jsonl')
        self.queue = deque()
        self.used = 0
        self.stopping = False
        self.threads = set()
        self.cleanup_pending = set()
        self.work_reclaimer = Reclaimer(self.root/'jobs'/'.trash',source_root=self.root/'jobs')
        self.asset_handoffs = {}
        self.asset_writes = weakref.WeakValueDictionary()
        self.startup['environment_seconds']=round(time.monotonic()-started,4)
        phase=time.monotonic();indexed = self.jobs.load_index()
        self.startup['index_seconds']=round(time.monotonic()-phase,4)
        phase=time.monotonic()
        candidates = tuple(self.jobs.pending) if indexed else None
        paths = ((self.root / 'jobs' / identity / 'result.json') for identity in candidates) if candidates is not None else (self.root / "jobs").glob("*/result.json")
        self.startup_recoveries = []
        for path in paths:
            saved = read_receipt(path, assets=self.jobs.pending_assets.get(path.parent.name, ()))
            if clean_tool_snapshot(saved):
                atomic_json(path, saved)
            interrupted = saved["status"] not in TERMINAL
            if interrupted:
                saved.update(status="cancelled" if saved.get('cancel_requested') else "interrupted", finished=time.time(), cost_incomplete=True,
                             error="Worker stopped before completion")
                atomic_json(path, saved)
            identity = saved["id"]
            job = Job(result=saved, cancel=threading.Event(), directory=path.parent)
            if saved.get('cancel_requested'):job['cancel'].set()
            self.jobs.active[identity] = job
            if saved.get('receipt_recovery'):
                self.startup_recoveries.append(identity)
                self.activity.notify('原回执损坏，保留任务目录并隔离恢复 ' + identity, 'local')
            else:
                self._clean_temp(job)
                if interrupted or saved.get('input_cleanup_error'):
                    self._clean_work(job)
            archive_job(self.jobs, job, startup=True)
        if not indexed:
            self.jobs.write_index()
        self.jobs.compact_index()
        if self.temp_reclaimer:
            for path in self.temp_root.iterdir():
                if _ID.fullmatch(path.name)and path.name not in self.startup_recoveries:
                    self.temp_reclaimer.defer(path)
        self.startup['recovery_seconds']=round(time.monotonic()-phase,4)
        phase=time.monotonic()
        self.asset_cache = AssetCache(self.root / 'assets')
        self.asset_cache.reclaim_uploads()
        self._prune_assets()
        self.startup.update(asset_seconds=round(time.monotonic()-phase,4),
                            seconds=round(time.monotonic()-started,4),pid=os.getpid(),version=__version__)
        atomic_json(self.root/'startup-current.json',self.startup)
        self.scheduler = threading.Thread(target=self._schedule, name="compute-scheduler", daemon=True)
        self.scheduler.start()

    def health(self, *, brief=False, sample=False):
        identity=dict(protocol=PROTOCOL,node=socket.gethostname(),role='worker',version=__version__,
                      capabilities=['machine','resize','resize_memory','resize_auto','resize_memory_model','resize_active_cpus','max_cpus','lease_polling','job_identity','health_sample','batch_status','job_file_version','accelerators','asset_handoff','cpu_accounting','adaptive_memory']+(['restart'] if self.restart_available else []))
        if brief:return identity
        with self.condition:
            resources = self._memory_state()
            disk = shutil.disk_usage(self.root)
            value = {**identity, "platform": sys.platform,
                    "role": "worker", "benchmark": self.benchmark, "benchmark_state": self.benchmark_state, **resources,
                    "cpus": self._cpu_capacity(resources), "base_cpus":self.capacity,
                    "cpu_overcommit_factor":self.cpu_overcommit_factor,
                    "cpu_limit": self.cpu_limit, "used_cpus": self.used, "queued": len(self.queue),
                    "tools": self.tool_info, "python_backends": sorted(self.python_runtimes), "shared": sorted(self.shared), "tags": self.tags, "paused": self.paused,
                    "accelerators": self.accelerators,
                    "accelerator_used": len({unit[:2] for items in self.accelerator_reservations.values() for unit in items}),
                    "accelerator_jobs": sum(bool(items) for items in self.accelerator_reservations.values()),
                    "pid": os.getpid(), "version": __version__, "home": Path.home().as_posix(),
                    "memory_pressure_state":dict(self.pressure_guard.state),
                    "memory_wait_signal_error":getattr(self,'memory_wait_signal_error',None),
                    "configuration_revision": self.configuration_revision,
                    "startup_recoveries": self.startup_recoveries,
                    "asset_revision": self.asset_cache.revision, "asset_cache": self.asset_cache.snapshot(),
                    "startup":self.startup,"temp_reclaim":self.temp_reclaimer.snapshot()if self.temp_reclaimer else None,
                    "work_reclaim":self.work_reclaimer.snapshot(),
                    "service": service_usage(),
                    "scratch_disk": dict(total=disk.total, used=disk.used, free=disk.free),
                    "tool_root": Path(__file__).resolve().parent.parent.as_posix(), "shared_paths": self.shared,
                    "unavailable_shared": self.unavailable_shared, "root": self.root.as_posix(),
                    "cache_root": self.cache_root.as_posix(),
                    "temp_root": self.temp_root.as_posix() if self.temp_root else None,
                    "active_jobs": [{**{k: j['result'].get(k) for k in ('id', 'label', 'project', 'status', 'cpus', 'active_cpus', 'max_cpus', 'memory_mb', 'memory_phase', 'started', 'accelerator')},
                                     'rss_mb':round(j.get('rss_mb',0),2),'private_mb':j.get('private_mb'),
                                     'memory_modeled':bool(j.get('memory_phase_model',j['task'].get('memory_model'))),
                                     'memory_phase_model':j.get('memory_phase_model',j['task'].get('memory_model')),
                                     'memory_adaptive_model':j.get('memory_adaptive_model'),
                                     'memory_width_samples':j.get('memory_width_samples'),
                                     'memory_sample_cpus':getattr(j.get('memory_observation'),'active_cpus',0),
                                     'memory_calibrated':bool(j.get('memory_observation')and j['memory_observation'].envelope is not None),
                                     'memory_reservation_mb':reservation(j)[0],
                                     'memory_commit_reservation_mb':reservation(j)[1], **progress_fields(j['result']),
                                     **({'resource_request':j['result']['resource_request']} if 'resource_request' in j['result'] else {})}
                                    for j in active_values(self.jobs) if j['result']['status'] not in TERMINAL],
                    **self.activity.snapshot()}
            if sample:
                for name in ('benchmark','tools','shared','tags','home','tool_root','shared_paths','unavailable_shared','root','cache_root','temp_root'):
                    value.pop(name,None)
            return value

    def _accelerator_units(self):
        units=[]
        for profile_index, profile in enumerate(self.accelerators):
            ids=profile.get('ids') or [str(index) for index in range(profile['count'])]
            for index in range(profile['count']):
                units.append((profile_index, index, profile['kind'], profile['memory_mb'], tuple(profile['tags']), ids[index]))
        return units

    def _match_accelerator(self, requirement):
        used=[unit for items in self.accelerator_reservations.values() for unit in items]
        return select_accelerator_units(self._accelerator_units(), requirement, used)

    def _accelerator_environment(self, job_id, environment):
        # Only reservations grant access; inherited or task-supplied values
        # must not make a CPU job appear to own an accelerator.
        denied = dict(DCO_ACCELERATOR_KIND='', DCO_ACCELERATOR_COUNT='0',
                      DCO_ACCELERATOR_MEMORY_MB='0', DCO_ACCELERATOR_IDS='', DCO_ACCELERATOR_SHARING='',
                      CUDA_VISIBLE_DEVICES='-1')
        # Windows treats environment names case-insensitively, even when
        # the Python input mapping contains both upper- and lower-case keys.
        for key in list(environment):
            if key.upper() in denied:
                environment.pop(key)
        environment.update(denied)
        units=self.accelerator_reservations.get(job_id, [])
        if not units:
            return
        kind=units[0][2]
        ids=[unit[5] for unit in units]
        environment.update(DCO_ACCELERATOR_KIND=kind,
                           DCO_ACCELERATOR_COUNT=str(len(units)),
                           DCO_ACCELERATOR_MEMORY_MB=str(min(unit[6]['memory_mb'] if len(unit)>6 else unit[3] for unit in units)),
                           DCO_ACCELERATOR_SHARING='memory' if all(len(unit)>6 for unit in units) else 'exclusive',
                           DCO_ACCELERATOR_IDS=','.join(ids))
        if kind == 'cuda':
            environment['CUDA_VISIBLE_DEVICES']=','.join(ids)

    def _refresh_tags(self):
        # Tool and shared-path labels follow the current probes, including
        # removals; a stale configured label must not restore a missing tool.
        tags = {tag for tag in self.configured_tags - self.tool_tags
                if not tag.startswith('shared:')}
        tags.update('shared:'+name for name in self.shared)
        available=set(canonical_capabilities(name for name,item in self.tool_info.items()if item['available']))
        tags.update(available)
        if CHISEL_TOOLS <= available:tags.add('chisel')
        else:tags.discard('chisel')
        if not LAYOUT_TOOLS <= available:tags.discard('layout')
        if self.tool_info.get('verilator', {}).get('available') or self.tool_info.get('iverilog', {}).get('available'):
            tags.add('rtl')
        if self.accelerators:
            tags.update({'gpu', *{tag for item in self.accelerators for tag in item['tags']}})
            tags.update(item['kind']for item in self.accelerators)
        if 'ml' in self.python_runtimes:tags.add('ml')
        else:tags.discard('ml')
        if not any(item['kind']=='cuda'for item in self.accelerators):tags.discard('cuda')
        self.tags = sorted(tags)

    def _probe_tools(self, tools):
        resolved,info={},{}
        search_path=self.env.get('PATH',os.environ.get('PATH',''))
        for name,argv in tools.items():
            if not isinstance(name,str) or not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_+.-]*',name):raise ValueError('Invalid tool name')
            canonical_capability(name, registration=True)
            if not isinstance(argv,(str,list,tuple)):raise ValueError('Tool requires an argv prefix')
            argv=[argv] if isinstance(argv,str) else list(argv)
            if not argv or any(not isinstance(x,str) for x in argv):raise ValueError('Tool requires an argv prefix: '+name)
            executable=shutil.which(argv[0],path=search_path)
            # Resolving a venv's Python symlink would select the base interpreter.
            if executable:argv[0]=str(Path(executable).absolute())
            resolved[name]=argv
            info[name]=dict(argv=argv,available=executable is not None)
            if name=='verilator' and argv[1:]==[str(Path(__file__).with_name('rtl.py'))]:
                if os.name == 'nt':
                    msys=self.env.get('MSYS2_ROOT',os.environ.get('MSYS2_ROOT'))
                    bash=str(Path(msys)/'usr/bin/bash.exe') if msys else shutil.which('bash.exe',path=search_path)
                    base=Path(bash).resolve().parents[2] if bash and Path(bash).is_file() else None
                    engine=str(base/'ucrt64/bin/verilator_bin.exe') if base else None
                else:
                    configured=self.env.get('VERILATOR',os.environ.get('VERILATOR','verilator'))
                    engine=shutil.which(configured,path=search_path)
                ready=bool(executable and engine and Path(engine).is_file() and Path(argv[1]).is_file())
                info[name].update(available=ready,engine=engine)
        return resolved,info

    def _calibrate(self):
        """Re-measure the fixed native mix without occupying a task slot."""
        try:
            result = dict(cpu_benchmark(self.capacity, self.benchmark_python, self.benchmark_env, force=True))
            with self.condition:
                self.benchmark = result
                self.benchmark_state = 'ready' if result.get('available') else 'unavailable'
                self.benchmark_thread = None
                self.condition.notify_all()
        except BaseException as error:
            with self.condition:
                self.benchmark_state = 'error: ' + str(error)
                self.benchmark_thread = None
                self.condition.notify_all()

    def calibrate(self):
        with self.condition:
            if self.benchmark_thread and self.benchmark_thread.is_alive():
                return {'status': 'running', 'benchmark': self.benchmark, 'benchmark_state': self.benchmark_state}
            active = any(job['result']['status'] not in TERMINAL for job in active_values(self.jobs))
            self.benchmark_state = 'queued' if active else 'running'
            def run():
                with self.condition:
                    while active and not self.stopping and any(job['result']['status'] not in TERMINAL for job in active_values(self.jobs)):
                        self.condition.wait(0.5)
                    if self.stopping:
                        self.benchmark_thread = None
                        return
                self._calibrate()
            self.benchmark_thread = threading.Thread(target=run, name='native-calibration', daemon=True)
            self.benchmark_thread.start()
            return {'status': self.benchmark_state, 'benchmark': self.benchmark, 'benchmark_state': self.benchmark_state}

    def control(self, value):
        if isinstance(value,dict)and set(value)=={'retire_asset'}:
            return retire_asset(self,value['retire_asset'])
        if not isinstance(value, dict) or set(value) - {"cpus", "cpu_overcommit_factor", "paused", "memory_mb", "memory_reserve_mb", "memory_headroom_mb", "tools", "benchmark"}:
            raise ValueError("Control accepts cpus, cpu_overcommit_factor, memory_mb, memory_reserve_mb, memory_headroom_mb, paused, tools and benchmark")
        if value.get('benchmark'):
            return self.calibrate()
        if "cpus" in value and (type(value["cpus"]) is not int or not 0 <= value["cpus"] <= self.cpu_limit):
            raise ValueError(f"cpus must be in 0..{self.cpu_limit} (0 = automatic)")
        if "paused" in value and not isinstance(value["paused"], bool):
            raise ValueError("paused must be true or false")
        if 'cpu_overcommit_factor'in value:validate_cpu_overcommit(value['cpu_overcommit_factor'])
        for key in ('memory_mb','memory_reserve_mb','memory_headroom_mb'):
            if key in value and (type(value[key]) is not int or value[key]<0):
                raise ValueError(key+' must be a nonnegative integer')
        tools=value.get('tools',{})
        if not isinstance(tools,dict):raise ValueError('tools must map names to argv or null')
        removed={name for name,argv in tools.items() if argv is None}
        if any(not isinstance(name,str) or not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_+.-]*',name) for name in removed):raise ValueError('Invalid tool name')
        additions,info=self._probe_tools({name:argv for name,argv in tools.items() if argv is not None})
        if any(not item['available'] for item in info.values()):raise ValueError('Tool executable or engine not found')
        with self.condition:
            if tools:
                # Accepted commands and their recorded tool dictionaries retain
                # their identities while future jobs see the new aliases.
                self.tools={k:v for k,v in self.tools.items() if k not in removed}
                self.tool_info={k:v for k,v in self.tool_info.items() if k not in removed}
                self.tools.update(additions);self.tool_info.update(info)
                if 'python' in additions:self.python_runtimes['cpu']=additions['python']
                if 'python' in removed:self.python_runtimes.pop('cpu',None)
                self.tool_tags.update(canonical_capabilities(tools))
                self._refresh_tags()
                self.configuration_revision += 1
            if "cpus" in value:
                self.capacity = value["cpus"] or self.cpu_limit
            if 'cpu_overcommit_factor'in value:self.cpu_overcommit_factor=float(value['cpu_overcommit_factor'])
            if "paused" in value:
                self.paused = value["paused"]
            if "memory_mb" in value:
                self.memory_budget = value["memory_mb"]
            if "memory_reserve_mb" in value:self.memory_reserve=value['memory_reserve_mb']
            if "memory_headroom_mb" in value:self.memory_headroom=value['memory_headroom_mb']
            if any(key in value for key in ('memory_mb','memory_reserve_mb')):
                self.memory_capacity=self.memory_budget or max(0,self.host.sample()['memory_total_mb']-self.memory_reserve)
            if self.save_settings:
                self.save_settings({key: value[key] for key in ('cpus','cpu_overcommit_factor','memory_mb','memory_reserve_mb','memory_headroom_mb') if key in value})
            self.activity.notify('Resources ' + json.dumps(value, ensure_ascii=False))
            self.condition.notify_all()
            return self.health()

    def _cpu_capacity(self,host=None):
        if self.cpu_overcommit_factor==1:return self.capacity
        return cpu_capacity(self.capacity,self.used,(self.host.sample()if host is None else host).get('cpu_percent'),
                            self.cpu_overcommit_factor)

    def _memory_state(self):
        host = self.host.sample()
        running = [j for j in active_values(self.jobs) if j["result"]["status"] == "running"]
        quotes=[reservation(j) for j in running]
        reservations=[quote[0] for quote in quotes]
        reserved=sum(reservations)
        unallocated=sum(max(0,amount-j.get('rss_mb',0))for j,amount in zip(running,reservations))
        private=sum(j.get('private_mb',j.get('rss_mb',0))for j in running)
        commit_unallocated=sum(max(0,quote[1]-j.get('private_mb',j.get('rss_mb',0)))
                               for j,quote in zip(running,quotes))
        limits=[self.memory_capacity-reserved,host['host_memory_available_mb']-self.memory_headroom-unallocated]
        if 'host_commit_available_mb'in host:
            limits.append(host['host_commit_available_mb']-self.memory_headroom-commit_unallocated)
        available=max(0,min(limits))
        return {**host, "memory_capacity_mb": self.memory_capacity, "memory_reserved_mb": round(reserved, 2),
                "memory_available_mb": int(available), "memory_reserve_mb": self.memory_reserve,
                "memory_headroom_mb": self.memory_headroom, "memory_budget_mb":self.memory_budget,
                "memory_effective_mb":int(min(self.memory_capacity,reserved+available)),
                'private_mb':round(private,2),'memory_unallocated_mb':round(unallocated,2),
                'commit_unallocated_mb':round(commit_unallocated,2),
                "memory_accounting":"rss", "memory_requested_mb":sum(j['task']['memory_mb'] for j in running),
                "rss_mb": round(sum(j.get("rss_mb", 0) for j in running), 2)}

    def _save(self, job):
        atomic_json(job["directory"] / "result.json", job["result"])

    def _tool_argv(self, name, task=None):
        """Resolve one semantic capability for commands and path variables."""
        canonical = execution_tool(name)
        if canonical=='python' and task is not None:
            backend=python_backend(task)
            if backend not in self.python_runtimes:raise ValueError('Python backend is not configured: '+backend)
            return self.python_runtimes[backend]
        names = [key for key in dict.fromkeys((name,canonical)) if key in self.tools]
        names += [alias for alias in self.tools if alias not in names and canonical_capability(alias) == canonical]
        for alias in names:
            if self.tool_info[alias]['available']:
                return self.tools[alias]
        if names:
            return self.tools[names[0]]
        raise KeyError(name)

    def _expand(self, text, task, work, *, dynamic_cpus=False):
        def substitute(match):
            key = match[1]
            if key == "work":
                return work.as_posix()
            if key == "cpus":
                return '${cpus}' if dynamic_cpus else str(task["cpus"])
            if key == "exe":
                return ".exe" if os.name == "nt" else ""
            if key == 'dco':
                return Path(__file__).resolve().parent.parent.as_posix()
            kind, name = key.split(":", 1)
            if kind == "shared":
                if name in self.unavailable_shared:
                    raise ValueError("Shared directory is unavailable on this worker: " + name)
                return self.shared[name]
            if kind == "asset":
                return (self.root / "assets" / task["assets"][name]).as_posix()
            prefix = self._tool_argv(name, task)
            if len(prefix) != 1:
                raise ValueError("Multi-argument tools belong at argv[0]")
            return Path(prefix[0]).as_posix()
        return _VAR.sub(substitute, text)

    def resize(self, identity, cpus, *, active_cpus=None, memory_mb=None, base_memory_mb=None, memory_per_cpu_mb=None, memory_limit_mb=None, memory_budget_mb=None):
        if type(cpus) is not int or cpus<0:
            raise ValueError('resize cpus must be nonnegative; 0 uses the current worker budget')
        if memory_mb is not None and (type(memory_mb) is not int or memory_mb<1):
            raise ValueError('resize memory_mb must be a positive integer in MiB')
        if memory_budget_mb is not None and (type(memory_budget_mb)is not int or memory_budget_mb<1):
            raise ValueError('resize memory_budget_mb must be a positive integer in MiB')
        if active_cpus is not None and (type(active_cpus) is not int or active_cpus<0):
            raise ValueError('active_cpus must count nonnegative in-flight threads')
        with self.condition:
            job=self.jobs[identity];task,result=job['task'],job['result']
            if self.stopping or job['cancel'].is_set() or result['status']!='running':
                raise Conflict('Only a running task can change its CPU grant')
            old=task['cpus']
            old_memory=task['memory_mb']
            if all(value is None for value in (memory_mb,base_memory_mb,memory_per_cpu_mb,memory_limit_mb)):
                current=job.get('memory_phase_model',task.get('memory_model'))or result.get('memory_phase')or dict(memory_mb=old_memory)
                if 'base_memory_mb' in current:
                    base_memory_mb=current['base_memory_mb'];memory_per_cpu_mb=current['memory_per_cpu_mb']
                    memory_limit_mb=current.get('memory_limit_mb')
                else:memory_mb=current['memory_mb']
            phase_model=(dict(base_memory_mb=base_memory_mb,memory_per_cpu_mb=memory_per_cpu_mb,
                              memory_limit_mb=memory_limit_mb) if base_memory_mb is not None or memory_per_cpu_mb is not None else
                         None if memory_mb is not None else job.get('memory_phase_model',task.get('memory_model')))
            requested_cpus=cpus
            capacity=self._cpu_capacity()
            cpus=min(cpus or capacity,task.get('max_cpus',capacity))
            floor=max(1,active_cpus or 0)
            if active_cpus is not None and (active_cpus>old or requested_cpus and active_cpus>cpus):
                raise ValueError('active_cpus must count threads within the existing grant and requested width')
            cpus=max(cpus,floor)
            granted=max(floor,min(cpus,max(0,capacity-self.used+old)))
            if self.pressure_guard.state['limited']:granted=min(granted,floor)
            cpu_ready_limit=granted
            changes_memory=(memory_mb is not None and memory_mb!=old_memory) or base_memory_mb is not None or memory_per_cpu_mb is not None
            credit=min(reservation(job)) if changes_memory else old_memory
            model=dict(memory_mb=memory_mb,base_memory_mb=base_memory_mb,memory_per_cpu_mb=memory_per_cpu_mb,memory_limit_mb=memory_limit_mb)
            requested_memory={k:v for k,v in model.items()if v is not None}
            previous_memory=result.get('memory_phase')or job.get('memory_phase_model',task.get('memory_model'))
            if not requested_memory:requested_memory=previous_memory or {}
            physical,commit=reservation(job)
            observation=job.get('memory_observation')
            observed_cpus=getattr(observation,'active_cpus',0)or result.get('active_cpus',0)
            calibrated=bool(observation and observation.envelope is not None)
            samples=job.get('memory_width_samples',[])
            sampled_at=job.get('memory_width_sampled_at',time.monotonic())
            if time.monotonic()-sampled_at>=10:samples=[];sampled_at=time.monotonic()
            if calibrated and observed_cpus and same_phase(requested_memory,previous_memory):
                points=dict(samples);width=observed_cpus
                points.pop(width,None);points[width]=max(physical,commit)
                samples=list(points.items())[-8:]
            model=calibrated_model(model,previous_memory,observed_cpus,physical,commit,
                                   calibrated,job.get('memory_adaptive_model'),samples,cold_probe=not observed_cpus)
            adaptive_model=model
            available=min(self.memory_capacity,credit+max(0,self._memory_state()['memory_available_mb']))
            if memory_budget_mb is not None:available=min(available,memory_budget_mb)
            if memory_budget_mb is not None and calibrated and same_phase(requested_memory,previous_memory):
                field='memory_mb'if model.get('memory_mb')is not None else'base_memory_mb'if model.get('memory_per_cpu_mb')==0 else None
                actual=max(job.get('rss_mb',0),job.get('private_mb')or 0)
                if actual>available:
                    raise Conflict('Insufficient RAM for active workers; existing grant retained')
                # This width-independent phase already has funded growth room.
                # A newer padded sample must not charge that margin twice.
                if field and 0<actual<=available:
                    model={**model,field:min(model[field],int(available))}
            granted,memory=plan_memory(granted,available,floor=floor,**model)
            if granted<floor:raise Conflict('Insufficient RAM for active workers; existing grant retained')
            memory=old_memory if memory is None else memory
            extra=max(0,memory-credit)
            if memory>self.memory_capacity or (extra and extra>self._memory_state()['memory_available_mb']):
                raise Conflict('Insufficient memory for resource resize; existing grant retained')
            active_changed=result.get('active_cpus',0)!=(active_cpus or 0)
            result.update(active_cpus=active_cpus or 0,requested_cpus=requested_cpus)
            result['memory_phase']=requested_memory
            phase_changed=not same_phase(phase_model,job.get('memory_phase_model',task.get('memory_model')))
            phase_changed=phase_changed or memory_mb is not None and not same_phase(requested_memory,previous_memory)
            if phase_changed:
                job.pop('memory_adaptive_model',None);job.pop('memory_width_samples',None);job.pop('memory_width_sampled_at',None)
            elif calibrated and (phase_model or memory_mb is not None):
                job['memory_adaptive_model']=dict(adaptive_model)
                if samples:job.update(memory_width_samples=samples,memory_width_sampled_at=sampled_at)
            if phase_changed or granted!=old:
                job['memory_observation']=MemoryObservation()
                job.pop('startup_memory_mb',None)
            job['memory_phase_model']=phase_model
            if granted!=old or memory!=old_memory or active_changed or phase_changed:
                self.used+=granted-old
                task['cpus']=result['cpus']=granted
                task['memory_mb']=result['memory_mb']=memory
                result['peak_cpus']=max(result.get('peak_cpus',old),granted)
                result['resize_count']=result.get('resize_count',0)+1
                self._save(job)
                self.condition.notify_all()
            memory_limited=granted<cpu_ready_limit
            return dict(id=identity,node=socket.gethostname(),cpus=granted,memory_mb=memory,requested_cpus=requested_cpus,memory_phase=requested_memory,
                        cpu_ready_limit=cpu_ready_limit,cpu_available=max(floor,capacity-self.used+granted),
                        memory_calibrated=calibrated and not phase_changed,memory_guard_limited=self.pressure_guard.state['limited'],
                        memory_calibrating=memory_limited and not(calibrated and not phase_changed)and not self.pressure_guard.state['limited'],
                        memory_calibration=(dict(phase=requested_memory,model=adaptive_model,physical_mb=physical,commit_mb=commit,sample_cpus=observed_cpus)
                                            if calibrated and not phase_changed else None))

    def lease(self, identity, request):
        """A task asks locally; the center answers through its existing poll."""
        with self.condition:
            job=self.jobs[identity]
            if 'request_id' in request:
                # An answer may arrive after the requesting process finished.
                # Acknowledge it without replacing that task's real outcome.
                current=job['result'].get('resource_request')
                if job['result']['status']=='running' and current and current['request_id']==request['request_id']:
                    job['resource_reply']=request
                    self.condition.notify_all()
                return {'accepted':True}
            if job['result']['status']!='running':raise Conflict('Only a running task can request resources')
            if job.get('callback',{}).get('mode')!='poll':raise Conflict('Task is not owned by a polling center')
            if 'resource_request' in job['result']:raise Conflict('A resource request is already pending')
            serial=job['lease_serial']=job.get('lease_serial',0)+1
            job['result']['resource_request']={**request,'request_id':serial}
            deadline=time.monotonic()+120
            try:
                while 'resource_reply' not in job:
                    if self.stopping or job['cancel'].is_set():raise Conflict('Worker is stopping')
                    if job['result']['status']!='running':raise Conflict('Task finished before resource reply')
                    remaining=deadline-time.monotonic()
                    if remaining<=0:raise Conflict('Center resource reply timed out; existing grant retained')
                    self.condition.wait(min(remaining,.5))
                reply=job.pop('resource_reply')
                if reply.get('error'):raise Conflict(reply['error'])
                return reply['grant']
            finally:
                job['result'].pop('resource_request',None)

    def submit(self, identity, payload, *, callback=None):
        if not _ID.fullmatch(identity):
            raise ValueError("Job id requires 32 lowercase hex characters")
        digest = hashlib.sha256(payload).hexdigest()
        with self.asset_cache.lock:
            with self.condition:
                if self.stopping:
                    raise Conflict("Worker is stopping")
                existing = identity in self.jobs
            if existing:
                job = load_job(self.jobs, self.condition, identity)
                with self.condition:
                    result = job["result"]
                    if result["request_sha256"] != digest:
                        raise Conflict("Job id already contains a different request")
                    return dict(result)
            with zipfile.ZipFile(io.BytesIO(payload)) as archive:
                task = validate_task(json.loads(archive.read("task.json")), int(self.capacity*self.cpu_overcommit_factor))
                if task.get('cache_ref'):
                    raise ValueError('cache_ref requires the scheduling center')
                if task.get('platform') is not None and task['platform'] != sys.platform:
                    raise ValueError('Task platform differs from worker sys.platform')
                with self.condition:
                    task['accelerator']=python_accelerator(task,self.python_runtimes,self._accelerator_units(),
                        [unit for items in self.accelerator_reservations.values()for unit in items])
                if startup_memory(task) > self.memory_capacity:
                    raise ValueError("Task memory_mb exceeds worker memory budget")
                if not supports_python(task,self.python_runtimes):
                    raise ValueError('Requested Python backend is not configured')
                if task.get('accelerator') is not None and not self._match_accelerator(task['accelerator']):
                    raise ValueError('Task accelerator requirement is not provided by this worker')
                for item in archive.infolist():
                    inside(self.root, item.filename)
                    if item.filename != "task.json" and not item.filename.startswith("input/"):
                        raise ValueError("Job bundle contains a file outside input/")
            for asset in task["assets"].values():
                if not (self.root / "assets" / asset).is_dir():
                    raise ValueError("Synchronize missing asset first: " + asset)
                self.asset_cache.touch(asset)
            # Resolve commands and references before admission, so typos do not occupy slots.
            work = self.root / "jobs" / identity / "input"
            cwd = Path(self._expand(task["cwd"], task, work)).expanduser() if "cwd" in task else work
            if not cwd.is_absolute():
                raise ValueError("cwd must resolve to an absolute worker path")
            cwd = cwd.resolve()
            commands = []
            for argv in task["steps"]:
                match = _VAR.fullmatch(argv[0])
                prefix = (self._tool_argv(match[1][5:], task) if match and match[1].startswith('tool:')
                          else [self._expand(argv[0], task, work,dynamic_cpus=True)])
                commands.append([*prefix, *[self._expand(x, task, work,dynamic_cpus=True) for x in argv[1:]]])
            for value in task["env"].values():
                self._expand(value, task, work)
            directory = work.parent
            try:
                directory.mkdir()
            except FileExistsError:
                raise Conflict('Original job directory already exists; retain it and recover the original id') from None
            try:
                low_integrity_directory(directory)
                unpack(payload, directory)
                work.mkdir(exist_ok=True)
                if "stdin" in task and not inside(work, task["stdin"]).is_file():
                    raise ValueError("Missing stdin input file: " + task["stdin"])
                templates={name:self._expand(path.read_text(encoding='utf8'),task,work,dynamic_cpus=True)
                           for name,path in tree(work,task['render']).items()}
                resolved_env={key:self._expand(value,task,work,dynamic_cpus=True)for key,value in task['env'].items()}
                def freeze(value):
                    if isinstance(value,str):return self._expand(value,task,work,dynamic_cpus=True)
                    if isinstance(value,list):return [freeze(item)for item in value]
                    if isinstance(value,dict):return {key:freeze(item)for key,item in value.items()}
                    return value
                execution=freeze(task['execution'])if 'execution'in task else None
                result = {"id": identity, "status": "queued", "label": task["label"], 'project': task['project'], "cpus": task["cpus"], 'max_cpus':task['max_cpus'],
                          "memory_mb": task["memory_mb"], "peak_rss_mb": 0, "cpu_seconds": 0,
                          'initial_cpus':task['cpus'],'peak_cpus':task['cpus'],'resize_count':0,
                          "submitted": time.time(), "started": None, "finished": None,
                          "request_sha256": digest, "node": socket.gethostname(), "steps": [],
                          "total_steps": len(task["steps"]),
                          "stdout": "", "stderr": "", "tools": ({**self.tool_info,
                              'python':dict(argv=self._tool_argv('python',task),available=True)}
                              if uses_python(task) else self.tool_info),
                          "assets": task["assets"], "cwd": cwd.as_posix(),
                          "accelerator": task.get('accelerator')}
                job = Job({"result": result, "cancel": threading.Event(), "task": task,
                       "commands": commands, "directory": directory, "cwd": cwd,
                       'callback':dict(self.callback if callback is None else callback),
                       'templates':templates,'resolved_env':resolved_env,'execution':execution,
                       'python_argv':self._tool_argv('python',task)if uses_python(task)else []})
                # This small immutable admission record survives damage to a
                # later progress receipt. Never snapshot inputs or full tools.
                admission = {key: result[key] for key in (
                    'id', 'request_sha256', 'submitted', 'label', 'project',
                    'initial_cpus', 'memory_mb', 'assets', 'total_steps')}
                atomic_json(directory / 'admission.json', admission)
                self._save(job)
                with self.condition:
                    if self.stopping:
                        raise Conflict("Worker is stopping")
                    if task['cpus'] > int(self.capacity*self.cpu_overcommit_factor) or startup_memory(task) > self.memory_capacity:
                        raise Conflict('Worker budget changed during submission; retry the original id')
                    self.jobs[identity] = job
                    self.asset_handoffs.pop(identity,None)
                    self.queue.append(identity)
                    self.queue = deque(sorted(self.queue, key=lambda key: -self.jobs[key]["task"]["priority"]))
                    self.condition.notify_all()
            except BaseException:
                if directory.exists():
                    shutil.rmtree(directory)
                raise
            return dict(result)

    def _schedule(self):
        with self.condition:
            while not self.stopping:
                host=self.host.sample()
                pressure=self.pressure_guard.update(host)
                if pressure['waiting']!=self.memory_wait_signaled:
                    try:
                        if pressure['waiting']:self.memory_wait_file.touch()
                        else:unlink(self.memory_wait_file)
                    except OSError as error:
                        self.memory_wait_signal_error=str(error)
                    else:
                        self.memory_wait_signaled=pressure['waiting']
                        self.memory_wait_signal_error=None
                if pressure['limited']:
                    if pressure['terminate']and time.monotonic()>=getattr(self,'next_memory_pressure',0):
                        victim=pressure_victim(active_values(self.jobs),host)
                        if victim is not None:
                            victim['result'].update(error_kind='memory_pressure',error='Task terminated under critical host RAM/commit pressure',
                                memory_pressure={**host,'guard':dict(pressure),'victim_priority':victim['task'].get('priority',0)})
                            victim['memory_pressure']=True;victim['cancel'].set()
                            self.next_memory_pressure=time.monotonic()+2
                    self.condition.wait(.25);continue
                # FIFO prevents a continuous stream of small jobs starving a larger one.
                if self.queue and not self.paused:
                    job = self.jobs[self.queue[0]]
                    if (self.used + job["task"]["cpus"] <= self._cpu_capacity(host)
                            and startup_memory(job['task']) <= self._memory_state()["memory_available_mb"]
                            and (job['task'].get('accelerator') is None or self._match_accelerator(job['task']['accelerator']))):
                        self.queue.popleft()
                        job['cpu_overcommit']=self.used+job['task']['cpus']>self.capacity
                        self.used += job["task"]["cpus"]
                        job['startup_memory_mb']=startup_memory(job['task'])
                        self.accelerator_reservations[job['result']['id']]=self._match_accelerator(job['task'].get('accelerator')) or []
                        job["result"].update(status="running", started=time.time(),cpu_overcommit=job['cpu_overcommit'])
                        self._save(job)
                        thread = threading.Thread(target=self._execute, args=(job,), name="compute-job", daemon=True)
                        self.threads.add(thread)
                        thread.start()
                        continue
                self.condition.wait(0.5)

    def _start_process(self, job, *args, **kwargs):
        # Serialize only initial OS process creation, outside the scheduler
        # condition. Cancellation wakes the pacing wait without starting it.
        with self.start_lock:
            interval=1. if job.get('cpu_overcommit')else .5
            deadline=self.next_start+max(0,interval-self.start_interval)
            if self.stopping or job['cancel'].wait(max(0,deadline-time.monotonic())):
                return None
            if self.stopping or job['cancel'].is_set():
                return None
            try:
                return Process(*args, **kwargs)
            finally:
                self.next_start = time.monotonic() + interval
                self.start_interval=interval

    def _execute(self, job):
        task, result, directory = job["task"], job["result"], job["directory"]
        job['parent_memory_mb']=task['memory_mb']
        work = directory / "input"
        deadline = time.monotonic() + task["timeout"]
        result['deadline']=time.time()+max(0,deadline-time.monotonic())
        state, error = "succeeded", None
        cpu_seconds = 0.0
        accounting = None
        accounting_saved = time.monotonic()
        accounting_saved_cpu = -1.0
        try:
            if task['env'].get('DCO_CPU_ACCOUNTING') == '1':
                accounting = CpuAccounting()
            environment = dict(os.environ, **self.env)
            # MSYS2 compiler frontends (cc1plus, clang and their runtime DLLs)
            # are launched as children of the configured tool.  Keep the
            # matching UCRT64 and MSYS runtime directories on PATH so a
            # configured C++ alias is executable in a clean worker process,
            # without requiring a machine-global PATH mutation.
            if os.name == 'nt' and self.env.get('MSYS2_ROOT'):
                msys_root = Path(self.env['MSYS2_ROOT'])
                msys_paths = [msys_root / 'ucrt64' / 'bin', msys_root / 'usr' / 'bin']
                inherited = environment.get('PATH', '')
                environment['PATH'] = os.pathsep.join(
                    [str(path) for path in msys_paths if path.is_dir()] + ([inherited] if inherited else []))
            environment.update({key:value.replace('${cpus}',str(task['cpus']))for key,value in job.get('resolved_env',{}).items()})
            environment = cache_environment(environment, self.cache_root)
            from .paths import build_environment
            environment = build_environment(environment,self.tools)
            # Default libraries to the grant; callers with outer parallelism
            # can explicitly keep each numerical kernel single-threaded.
            for key in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS", "NUMBA_NUM_THREADS"):
                if key not in task['env']:
                    environment[key] = str(task["cpus"])
            temporary = self._temporary(job)
            temporary.mkdir()
            low_integrity_directory(temporary)
            environment.update(TMP=str(temporary), TEMP=str(temporary), TMPDIR=str(temporary),
                               PYTHONDONTWRITEBYTECODE="1", PYTHONUTF8="1", REMOTE_COMPUTE_JOB_DIR=str(directory),
                               REMOTE_COMPUTE_JOB_TEMP=str(temporary),
                               REMOTE_COMPUTE_WORKER='1', REMOTE_COMPUTE_JOB_ID=result['id'],
                               REMOTE_COMPUTE_DEADLINE=str(deadline))
            callback=job.get('callback',{})
            polling=callback.get('mode')=='poll'
            if polling:callback={**self.callback,**callback}
            environment.update(REMOTE_COMPUTE_URL=callback.get('url',''),
                               REMOTE_COMPUTE_JOB_ID=callback.get('job_id',result['id']),
                               REMOTE_COMPUTE_WORKER_JOB_ID=result['id'],
                               REMOTE_COMPUTE_TOKEN=callback.get('token',''),
                               REMOTE_COMPUTE_TIMEOUT=str(callback.get('timeout',10)),
                               REMOTE_COMPUTE_RESIZE='1' if callback.get('url') else '0',
                               REMOTE_COMPUTE_LEASE='1' if polling else '0',
                               REMOTE_COMPUTE_CA=callback.get('ca_file') or '',
                               REMOTE_COMPUTE_PYTHON_ARGV=json.dumps(job.get('python_argv',[])),
                               REMOTE_COMPUTE_MEMORY_WAIT=str(self.memory_wait_file),
                               REMOTE_COMPUTE_HOME=str(Path(__file__).resolve().parent.parent))
            self._accelerator_environment(result['id'], environment)
            with (directory / "stdout.log").open("wb") as stdout, (directory / "stderr.log").open("wb") as stderr:
                templates=job.get('templates',{})
                for frozen in job['commands']:
                    with self.condition:
                        while (self.pressure_guard.state['waiting'] and not self.stopping
                               and not job['cancel'].is_set() and time.monotonic()<deadline):
                            self.condition.wait(.1)
                    if job["cancel"].is_set():
                        state = "cancelled"
                        break
                    if time.monotonic()>=deadline:
                        state='timed_out';break
                    with self.condition:
                        job['rss_sampled']=False;job['rss_mb']=0
                        for key in ('private_mb','memory_adaptive_model','memory_width_samples','memory_width_sampled_at'):job.pop(key,None)
                        job['memory_observation']=MemoryObservation()
                        result['memory_phase']=None
                    self._automatic_grant(job)
                    environment.update({key:value.replace('${cpus}',str(task['cpus']))for key,value in job.get('resolved_env',{}).items()})
                    self._accelerator_environment(result['id'],environment)
                    for key in ('OMP_NUM_THREADS','OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','NUMEXPR_NUM_THREADS','NUMBA_NUM_THREADS'):
                        if key not in task['env']:environment[key]=str(task['cpus'])
                    environment.update(REMOTE_COMPUTE_CPUS=str(task['cpus']),
                        REMOTE_COMPUTE_MAX_CPUS=str(task['max_cpus']),
                        REMOTE_COMPUTE_MEMORY_MODEL=json.dumps(task.get('memory_model')or dict(memory_mb=job['parent_memory_mb'])))
                    for name,text in templates.items():
                        inside(work,name).write_text(text.replace('${cpus}',str(task['cpus'])),encoding='utf8')
                    if 'execution'in task:
                        def expand(value):
                            if isinstance(value,str):return value.replace('${cpus}',str(task['cpus']))
                            if isinstance(value,list):return [expand(v)for v in value]
                            if isinstance(value,dict):return {k:expand(v)for k,v in value.items()}
                            return value
                        atomic_json(work/'.dco-execution.json',expand(job['execution']))
                    argv=[value.replace('${cpus}',str(task['cpus']))for value in frozen]
                    if time.monotonic()>=deadline:
                        state='timed_out'
                        break
                    step = {"argv": argv, "started": time.time(), "returncode": None}
                    with self.condition:
                        result["steps"].append(step)
                        self._save(job)
                    source = inside(work, task["stdin"]).open("rb") if "stdin" in task else nullcontext(None)
                    with source as stdin:
                        if len(result['steps']) == 1:
                            process = self._start_process(job, argv, job["cwd"], environment, stdout, stderr, stdin=stdin)
                        else:
                            process = Process(argv, job["cwd"], environment, stdout, stderr, stdin=stdin)
                    if process is None:
                        state = 'cancelled'
                        break
                    with self.condition:
                        job['process'] = process
                        # Cancellation may arrive between Popen and registration.
                        if job['cancel'].is_set():process.kill()
                    try:
                        while process.child.poll() is None:
                            usage = process_usage(process)
                            total_cpu = max(result["cpu_seconds"], cpu_seconds + usage["cpu_seconds"])
                            measured = accounting.update(total_cpu) if accounting else None
                            with self.condition:
                                # A cached process scan may predate this child.
                                if usage['rss_mb'] > 0:
                                    job['rss_mb'] = usage['rss_mb']
                                    job['rss_sampled'] = True
                                    if 'private_mb'in usage:
                                        job['private_mb']=usage['private_mb']
                                        result['peak_private_mb']=max(result.get('peak_private_mb',0),usage['private_mb'])
                                    job['memory_observation'].observe(usage['rss_mb'],usage.get('private_mb'),active_cpus=result.get('active_cpus',0))
                                    physical,commit=reservation(job)
                                    result.update(memory_reservation_mb=physical,memory_commit_reservation_mb=commit)
                                result["peak_rss_mb"] = max(result["peak_rss_mb"], usage["rss_mb"])
                                result["cpu_seconds"] = total_cpu
                                if measured:
                                    result.update(measured)
                                    if time.monotonic()-accounting_saved >= 1 and total_cpu != accounting_saved_cpu:
                                        try:
                                            self._save(job)
                                        except OSError:
                                            pass  # A Windows snapshot reader may briefly hold the old file open.
                                        else:
                                            accounting_saved_cpu = total_cpu
                                        accounting_saved = time.monotonic()
                            if job["cancel"].is_set() or time.monotonic() >= deadline:
                                state = "failed"if job.get('memory_pressure')else "cancelled" if job["cancel"].is_set() else "timed_out"
                                process.stop()
                                break
                            job["cancel"].wait(0.05)
                        step.update(returncode=process.child.wait(), finished=time.time())
                        usage = process_usage(process)
                        cpu_seconds = max(result["cpu_seconds"], cpu_seconds + usage["cpu_seconds"])
                        measured = accounting.update(cpu_seconds) if accounting else None
                        with self.condition:
                            result["cpu_seconds"] = cpu_seconds
                            if measured:
                                result.update(measured)
                    finally:
                        with self.condition:
                            job.pop('process', None)
                        process.close()
                    if state != "succeeded":
                        break
                    if step["returncode"] != 0:
                        state, error = "failed", f"Command exited with {step['returncode']}"
                        break
        except Exception as exc:
            state, error = "failed", f"{type(exc).__name__}: {exc}"
        finally:
            waiting=work/'.dco-memory-waits'
            if (state!='succeeded'and not job['cancel'].is_set() and
                    (waiting.is_dir()and any(waiting.iterdir()) or
                     self.pressure_guard.state['waiting']and time.monotonic()>=deadline)):
                job['memory_pressure']=True
                result.update(error_kind='memory_pressure',error='Task deadline reached while waiting for host memory recovery',
                              memory_pressure={**self.host.sample(),'guard':dict(self.pressure_guard.state)})
            self._clean_temp(job)
            self._clean_work(job)
            while True:
                if job.get('memory_pressure'):
                    state,error='failed',result['error']
                if job["cancel"].is_set() and not job.get('memory_pressure'):
                    state = "cancelled"
                if 'execution'in task:
                    try:
                        from .execution import finish_execution
                        finish_execution(work/'.dco-results.json',state,error,spec=job['execution'])
                    except Exception as exc:
                        result['execution_state_error']=str(exc)
                with self.condition:
                    # A cancellation can arrive while the manifest is written
                    # outside the scheduler lock. Repeat that one transition
                    # before making the terminal receipt available to fetch.
                    if job['cancel'].is_set()and state=='succeeded':
                        state='cancelled';continue
                    self.accelerator_reservations.pop(result['id'], None)
                    result.update(status=state, finished=time.time())
                    job["rss_mb"] = 0
                    if error:
                        result["error"] = error
                    for name in ("stdout", "stderr"):
                        path = directory / (name + ".log")
                        if path.exists():
                            with path.open("rb") as stream:
                                stream.seek(max(0, path.stat().st_size - 16384))
                                result[name] = stream.read().decode("utf-8", errors="replace")
                    result["elapsed"] = result["finished"] - result["started"]
                    self._save(job)
                    self.used -= task["cpus"]
                    self.threads.discard(threading.current_thread())
                    archive_job(self.jobs, job)
                    self.condition.notify_all()
                    break

    def _automatic_grant(self,job):
        """Prepare a real kernel grant before expanding its argv and thread env."""
        task=job['task'];maximum=task['max_cpus']
        if task.get('execution',{}).get('mode')=='entry':maximum=1
        if task.get('execution',{}).get('mode')=='map':
            maximum=min(maximum,max(1,len(task['execution']['items'])))
        if maximum==task['cpus']==1 and not job['result']['steps']:return
        callback=job.get('callback',{})
        request=dict(cpus=maximum,active_cpus=0,**task.get('memory_model',{}))
        if not task.get('memory_model'):request['memory_mb']=job.get('parent_memory_mb',task['memory_mb'])
        try:
            if callback.get('mode')=='poll':
                self.lease(job['result']['id'],request)
            elif callback.get('url'):
                from .client import Client
                client=Client(callback['url'],callback.get('token',''),ca_file=callback.get('ca_file'))
                client.resize(callback.get('job_id',job['result']['id']),**request)
            else:self.resize(job['result']['id'],**request)
        except Exception as error:
            # Every command keeps this task's funded slot. Both admission
            # refusal paths retain it; neither failure cancels prepared work.
            retained=('Insufficient memory for resource resize; existing grant retained',
                      'Insufficient RAM for active workers; existing grant retained')
            if (not any(message in str(error)for message in retained)
                    or not(isinstance(error,Conflict)or getattr(error,'status_code',None)==409)):
                raise

    def _clean_work(self, job, *, discard=False):
        """Keep declared results; consumed input decks/builds are no longer artifacts."""
        work = job['directory']/'input'
        try:
            cancelled = job['cancel'].is_set
            if cancelled():return  # Retain all partial files until collection/delete.
            if work.resolve() != work:
                raise ValueError('Job input directory became a link')
            work=native_path(work,force=True)
            task = job.get('task') or json.loads((job['directory']/'task.json').read_text(encoding='utf-8'))
            if not task['outputs'] and not discard:
                return  # Generic job-file callers collect arbitrary files by name.
            if work.is_dir():
                with self.condition:lock=job.setdefault('artifact_lock',threading.RLock())
                with lock:
                    keep=tree(work,task['outputs'],cancel=cancelled)
                    with tempfile.TemporaryDirectory(prefix='.dco-output-',dir=native_path(job['directory'],force=True))as folder:
                        mirror=Path(folder)
                        for name,path in keep.items():
                            if cancelled():raise InterruptedError('Job cleanup cancelled')
                            target=mirror/name;target.parent.mkdir(parents=True,exist_ok=True)
                            try:os.link(path,target)
                            except OSError:shutil.copy2(path,target)
                        if cancelled():raise InterruptedError('Job cleanup cancelled')
                        self.work_reclaimer.defer(work,after_move=lambda:mirror.rename(work))
        except InterruptedError:
            return
        except (OSError, ValueError) as error:
            job['result']['input_cleanup_error'] = str(error)
            self.cleanup_pending.add(job['result']['id'])
            self.activity.notify('输入临时物待清理 '+str(work), 'local')
        else:
            job['result'].pop('input_cleanup_error', None)
            if not job['result'].get('cleanup_error'):
                self.cleanup_pending.discard(job['result']['id'])

    def _prune_assets(self, *, recent=True):
        # A check/upload and the following submit are separate LAN requests.
        # Keep their short handoff intact even if active assets exceed the LRU target.
        with self.asset_cache.lock:
            with self.condition:
                now = time.time()
                protected = {identity for identity, (_size, used) in self.asset_cache.entries.items() if recent and now-used<5}
                for identity,(assets,expires) in tuple(self.asset_handoffs.items()):
                    if time.monotonic()>=expires:
                        self.asset_handoffs.pop(identity,None)
                    else:
                        protected.update(assets)
                protected.update(asset_references(self.jobs))
            for error in self.asset_cache.prune(protected):
                self.activity.notify('资产缓存待清理 '+error, 'local')

    def _temporary(self, job):
        base=getattr(self,'temp_root',None)
        return base/job['result']['id'] if base else job['directory']/'tmp'

    def _clean_temp(self, job):
        """Process TEMP is scratch; keep result inputs/outputs until collection."""
        issue=None
        for temporary in {self._temporary(job),job['directory']/'tmp'}:
            try:
                # Scratch has no declared scientific outputs. Retire its name
                # before finalizing; physical deletion never holds CPU slots.
                reclaimer=self.temp_reclaimer if self.temp_reclaimer and temporary.parent==self.temp_root else self.work_reclaimer
                reclaimer.defer(temporary)
            except InterruptedError:
                self.cleanup_pending.add(job['result']['id'])
                continue
            except FileNotFoundError:
                pass
            except OSError as error:
                issue=str(error)
        with self.condition:
            if issue:
                job['result']['cleanup_error']=issue
                self.cleanup_pending.add(job['result']['id'])
                self.activity.notify('临时目录待清理 '+str(temporary), 'local')
            else:
                job['result'].pop('cleanup_error',None)
                if not job['result'].get('input_cleanup_error'):
                    self.cleanup_pending.discard(job['result']['id'])

    def get(self, identity):
        job = load_job(self.jobs, self.condition, identity)
        with self.condition:
            return json.loads(json_bytes(job["result"]))

    def job_file(self, identity, name, data=None):
        job = load_job(self.jobs, self.condition, identity)
        with self.condition:
            result_lock = job.setdefault('artifact_lock', threading.RLock())
        with result_lock:
            with self.condition:
                if self.jobs.get(identity) is not job:
                    raise KeyError(identity)
            path = inside(job['directory'] / 'input', name)
            if data is None:
                return path.read_bytes()
            path.parent.mkdir(parents=True, exist_ok=True)
            temporary = path.with_name(path.name + '.control-upload')
            temporary.write_bytes(data)
            temporary.replace(path)
            return {'written': name, 'bytes': len(data)}

    def job_file_version(self, identity, name, version=None):
        job = load_job(self.jobs, self.condition, identity)
        with self.condition:
            result_lock = job.setdefault('artifact_lock', threading.RLock())
        with result_lock:
            with self.condition:
                if self.jobs.get(identity) is not job:
                    raise KeyError(identity)
            path=inside(job['directory']/'input',name)
            with path.open('rb') as stream:
                stat=os.fstat(stream.fileno())
                current='"'+str(stat.st_ino)+'-'+str(stat.st_mtime_ns)+'-'+str(stat.st_size)+'"'
                return dict(version=current,changed=current!=version,data=stream.read() if current!=version else None)

    def logs(self, identity):
        job = load_job(self.jobs, self.condition, identity)
        with self.condition:
            result_lock = job.setdefault('artifact_lock', threading.RLock())
        with result_lock:
            with self.condition:
                if self.jobs.get(identity) is not job:
                    raise KeyError(identity)
            directory = job["directory"]
            result = {}
            for name in ("stdout", "stderr"):
                path = directory / (name + ".log")
                if path.exists():
                    with path.open("rb") as stream:
                        stream.seek(max(0, path.stat().st_size - 16384))
                        result[name] = stream.read().decode("utf-8", errors="replace")
                else:
                    result[name] = ""
            return result

    def cancel(self, identity):
        job = load_job(self.jobs, self.condition, identity)
        queued = False
        with self.condition:
            if job["result"]["status"] not in TERMINAL:
                job["cancel"].set()
                job['result']['cancel_requested'] = True
                process = job.get('process')
                if process is not None:
                    process.kill()
                    job['result']['kill_signal_sent'] = True
                if job["result"]["status"] == "queued" and identity in self.queue:
                    self.queue.remove(identity)
                    queued = True
                self._save(job)
                self.condition.notify_all()
        if queued:
            with self.condition:
                job["result"].update(status="cancelled", finished=time.time())
                self._save(job)
                archive_job(self.jobs, job)
                self.condition.notify_all()
        return self.get(identity)

    def artifacts(self, identity, *, result=None):
        job = load_job(self.jobs, self.condition, identity)
        with self.condition:
            if job["result"]["status"] not in TERMINAL:
                raise Conflict("Artifacts are available when the job has finished")
            result_lock = job.setdefault('artifact_lock', threading.RLock())
        with result_lock:
            with self.condition:
                if self.jobs.get(identity) is not job:
                    raise KeyError(identity)
                receipt = json_bytes(job["result"] if result is None else result)
            directory = job["directory"]
            task = job.get("task") or json.loads((directory / "task.json").read_text(encoding="utf-8"))
            files = {"result.json": receipt}
            files.update({"files/" + name: path for name, path in tree(directory / "input", task["outputs"]).items()})
            for name in ("stdout.log", "stderr.log"):
                if (directory / name).exists():
                    files[name] = directory / name
            if job['result'].get('receipt_recovery'):
                for name in ('receipt-recovery.json', job['result']['receipt_recovery'].get('damaged_file')):
                    if name and inside(directory, name).is_file():
                        files['recovery/' + name] = inside(directory, name)
            return pack(files)

    def delete(self, identity):
        job = load_job(self.jobs, self.condition, identity)
        with self.condition:
            if job["result"]["status"] not in TERMINAL:
                raise Conflict("Cancel and wait for the job before deleting it")
            result_lock = job.setdefault('artifact_lock', threading.RLock())
        with result_lock:
            with self.condition:
                if self.jobs.get(identity) is not job:
                    return {"deleted": identity}
            self.work_reclaimer.defer(job['directory'])
            with self.condition:
                del self.jobs[identity]
                self.cleanup_pending.discard(identity)
        self._prune_assets()
        return {"deleted": identity}

    def clean(self):
        from .operations import clean_pending
        def discard(job):
            self._clean_temp(job)
            if job['result'].get('cleanup_error'):raise OSError(job['result']['cleanup_error'])
        return clean_pending(self,discard=discard)

    def asset_handoff(self, identity, assets=None):
        return asset_handoff(self, identity, assets)

    def asset(self, identity, payload=None, *, job_id=None):
        if not _ASSET.fullmatch(identity):
            raise ValueError("Asset id requires SHA256")
        if job_id is not None and not _ID.fullmatch(job_id):
            raise ValueError('Job id requires 32 lowercase hex characters')
        directory = self.root / "assets" / identity
        started = time.monotonic()
        def published():
            exists = directory.is_dir()
            if exists:
                self.asset_cache.touch(identity)
            with self.condition:
                now=time.monotonic()
                for key,(assets,expires) in tuple(self.asset_handoffs.items()):
                    if identity in assets and expires>started:
                        self.asset_handoffs[key]=(assets,now+3600)
                if job_id is not None:
                    held = self.asset_handoffs.get(job_id, (set(), 0))[0]
                    self.asset_handoff(job_id, list(set(held) | {identity}))
            return {"id": identity, "exists": exists}
        if payload is None:
            with self.asset_cache.lock:
                return published()
        with self.asset_cache.lock:
            transfer = self.asset_writes.setdefault(identity, threading.Lock())
        # Slow unpacking owns only this content ID, not every asset or job submit.
        with transfer:
            with self.asset_cache.lock:
                if directory.is_dir():
                    return published()
            if hashlib.sha256(payload).hexdigest() != identity:
                raise ValueError("Asset digest does not match bundle")
            temporary = directory.with_suffix(".upload")
            try:
                temporary.mkdir(exist_ok=True)
                unpack(payload, temporary)
                with self.asset_cache.lock:
                    if self.stopping:
                        raise Conflict('Worker is stopping')
                    temporary.replace(directory)
                    return published()
            finally:
                if temporary.exists():
                    remove_owned(temporary)

    def close(self):
        with self.condition:
            self.stopping = True
            pending = list(active_items(self.jobs))
            self.condition.notify_all()
        for identity, _job in pending:
            self.cancel(identity)
        self.scheduler.join()
        for thread in list(self.threads):
            thread.join()
        # Calibration runs in a daemon thread and observes ``stopping`` while
        # queued.  A running native helper has its own bounded timeout; do not
        # hold service shutdown open just to wait for that optional probe.
        for identity in list(self.cleanup_pending):
            job = self.jobs[identity]
            self._clean_temp(job)
            self._clean_work(job)
        self.asset_handoffs.clear()
        self._prune_assets(recent=False)
        self.asset_cache.reclaimer.close()
        self.work_reclaimer.close()
        if self.temp_reclaimer:self.temp_reclaimer.close()
        unlink(self.memory_wait_file)
        if self.temp_root:
            try:
                self.temp_root.rmdir()
            except OSError:
                pass  # Keep any unfinished files; remove only our empty namespace.
        self.root_lock.close()


class Server(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 128

    def __init__(self, address, worker, *, restart=None, certfile=None, keyfile=None, trusted_networks=None):
        from .auth import trusted_networks as parse_networks
        self.trusted_networks = parse_networks(trusted_networks)
        self.worker = worker
        self.restart = restart
        self.connections = set()
        self.connections_lock = threading.Lock()
        super().__init__(address, Handler)
        if certfile:
            context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            context.load_cert_chain(certfile,keyfile)
            # Handshake in the timed request thread, so an incomplete peer
            # cannot hold the accept loop and block every new connection.
            self.socket=context.wrap_socket(self.socket,server_side=True,do_handshake_on_connect=False)
        if hasattr(worker,'restart_node'):
            worker.callback_address=self.server_address
        else:
            worker.callback=dict(url=f'{"https" if certfile else "http"}://127.0.0.1:{self.server_port}',token=worker.token,ca_file=certfile)

    def get_request(self):
        connection, address = super().get_request()
        with self.connections_lock:
            self.connections.add(connection)
        return connection, address

    def shutdown_request(self, request):
        with self.connections_lock:
            self.connections.discard(request)
        super().shutdown_request(request)

    def server_close(self):
        super().server_close()
        with self.connections_lock:
            connections = list(self.connections)
        for connection in connections:
            try:
                connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            connection.close()


class Handler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'
    idle_timeout = 10

    def handle_one_request(self):
        # Expire idle keep-alive waits, while active transfers may be slow.
        self.connection.settimeout(self.idle_timeout)
        return BaseHTTPRequestHandler.handle_one_request(self)

    def handle(self):
        try:
            super().handle()
        except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError, ssl.SSLError):
            self.close_connection = True

    def setup(self):
        super().setup()
        self.connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    def _connected(self):
        try:
            return (not select.select([self.connection],[],[],0)[0]
                    or bool(self.connection.recv(1,socket.MSG_PEEK)))
        except OSError:
            return False
    def log_message(self, *_args):
        pass

    def _reply(self, value, status=200, *, etag=None):
        binary = isinstance(value, bytes)
        body = value if binary else json_bytes(value)
        total, offset = len(body), 0
        if binary and self.command == 'GET' and status == 200:
            etag = etag or '"' + hashlib.sha256(body).hexdigest() + '"'
            match = re.fullmatch(r'bytes=(\d+)-', self.headers.get('Range', ''))
            if match and self.headers.get('If-Range') == etag:
                offset = int(match[1])
                if 0 < offset < total:
                    body, status = body[offset:], 206
                else:
                    offset = 0
        compressed = not binary and len(body) >= 1024 and 'gzip' in self.headers.get('Accept-Encoding', '')
        if compressed:
            body = gzip.compress(body, compresslevel=1)
        try:
            self.send_response(status)
            self.send_header("Content-Type", "application/zip" if binary else "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            if etag:
                self.send_header('Accept-Ranges', 'bytes')
                self.send_header('ETag', etag)
                if status == 206:
                    self.send_header('Content-Range', f'bytes {offset}-{total-1}/{total}')
            if compressed:
                self.send_header('Content-Encoding', 'gzip')
                self.send_header('Vary', 'Accept-Encoding')
            if self.headers.get('X-DCO-Keepalive') != '1':
                # Older clients expect POST connections to stay live forever.
                # Announce closure instead of surprising them on their next POST.
                self.send_header('Connection','close')
                self.close_connection=True
            self.end_headers()
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            self.close_connection = True

    def _handle(self):
        self.connection.settimeout(None)
        worker = self.server.worker
        authorization = self.headers.get("Authorization", "")
        operator_secret = getattr(worker, 'operator_token', None)
        operator_auth = bool(operator_secret and hmac.compare_digest(authorization, "Bearer " + operator_secret))
        from .auth import in_networks
        lan = in_networks(self.client_address[0])
        if not lan and not isinstance(self.connection, ssl.SSLSocket):
            return self._reply({"error": "Non-LAN connections require HTTPS"}, 403)
        trusted = lan and in_networks(self.client_address[0], self.server.trusted_networks)
        token_auth = bool(worker.token and hmac.compare_digest(authorization, "Bearer " + worker.token))
        if not trusted and not (token_auth or operator_auth):
            return self._reply({"error": "Token required"}, 401)
        # A scheduler has two views: project clients receive aggregate
        # capabilities and redacted job state, while the visible DCO console
        # carries the explicit operator role for placement and machine data.
        scheduler = hasattr(worker, 'nodes') and hasattr(worker, 'admission')
        operator = self.headers.get('X-DCO-Role') == 'operator' and (operator_auth or not scheduler)
        if scheduler and self.headers.get('X-DCO-Role') == 'operator' and not operator_auth:
            return self._reply({"error": "Operator token required"}, 403)
        worker.activity.access(self.client_address[0], self.connection.getsockname()[0])
        parts = urlsplit(self.path).path.strip("/").split("/")
        try:
            body = None
            if self.command == "POST":
                length = int(self.headers.get("Content-Length", 0))
                if not 0 <= length <= MAX_BODY:
                    return self._reply({"error": "Bundle exceeds 512 MiB; use a shared root for large inputs"}, 413)
                body = self.rfile.read(length)
                if len(body) != length:
                    raise ValueError("Incomplete request body")
            if parts == ["health"] and self.command == "GET":
                if scheduler and not operator:
                    value = worker.public_health(brief=self.headers.get("X-DCO-Brief")=="1")
                else:
                    value = worker.health(brief=self.headers.get("X-DCO-Brief")=="1",
                                          sample=self.headers.get("X-DCO-Sample")=="1")
            elif parts == ['capabilities'] and self.command == 'GET' and scheduler:
                query = parse_qs(urlsplit(self.path).query, keep_blank_values=True)
                values = query.get('platform', [])
                if len(values) > 1:
                    raise ValueError('capabilities accepts one platform')
                value = worker.capabilities(platform=values[0] if values else None,operator=operator)
            elif parts == ["nodes"] and self.command == "GET" and hasattr(worker, "nodes"):
                if not operator:
                    return self._reply({"error": "Device list is operator-only; use /capabilities"}, 403)
                value = worker.nodes()
            elif parts == ['admission'] and self.command == 'POST' and hasattr(worker, 'admission'):
                value = worker.admission(json.loads(body), operator=operator)
            elif parts == ["control"] and self.command == "POST":
                if scheduler and not operator:
                    return self._reply({"error": "Scheduler control is operator-only"}, 403)
                value = worker.control(json.loads(body))
                worker.activity.notify('Control ' + body.decode('utf-8'), self.client_address[0])
            elif parts == ['clean'] and self.command == 'POST':
                if not operator:
                    return self._reply({'error':'Queue cleanup is operator-only'},403)
                value=worker.clean()
            elif parts == ["restart"] and self.command == "POST":
                if scheduler and not operator:
                    return self._reply({"error": "Scheduler restart is operator-only"}, 403)
                request = json.loads(body)
                if self.server.restart and (not hasattr(worker, 'restart_node') or request.get('node') in (None,socket.gethostname())):
                    self.server.restart(check=True)
                    self._reply({"restarting": True, "pid": os.getpid()}, 202)
                    self.server.restart()
                    return
                elif hasattr(worker, "restart_node") and request.get('node'):
                    value = worker.restart_node(request['node'])
                else:
                    raise ValueError("Restart requires a supervised service")
            elif len(parts)==2 and parts[0]=='cache-references' and scheduler and self.command in ('GET','POST'):
                request=json.loads(body)if self.command=='POST'else None
                value=worker.cache_reference(parts[1],request)
            elif parts == ['jobs','status'] and self.command == 'POST':
                request = json.loads(body)
                identities = validate_status_ids(request.get('ids') if isinstance(request, dict) else None)
                value = {}
                for identity in identities:
                    try:
                        result = worker.public_get(identity) if scheduler and not operator else worker.get(identity)
                    except (KeyError, FileNotFoundError):
                        value[identity] = dict(id=identity, status='unknown', status_code=404)
                    else:
                        value[identity] = progress(result)
            elif parts == ["jobs"] and self.command == "GET":
                query = parse_qs(urlsplit(self.path).query)
                if 'limit' in query or 'offset' in query:
                    try:
                        limit = max(1, min(512, int(query.get('limit', ['256'])[0])))
                        offset = max(0, int(query.get('offset', ['0'])[0]))
                    except (TypeError, ValueError):
                        raise ValueError('jobs page requires integer limit and offset')
                    with worker.condition:
                        if hasattr(worker.jobs, 'page'):
                            identities, total = worker.jobs.page(limit=limit, offset=offset)
                        else:
                            all_ids = tuple(worker.jobs)
                            total = len(all_ids)
                            identities = all_ids[offset:offset + limit]
                    items = []
                    for identity in identities:
                        try:
                            result = worker.public_get(identity) if scheduler and not operator else worker.get(identity)
                        except KeyError:
                            continue
                        items.append(result)
                    value = dict(items=items, total=total, offset=offset,
                                 limit=limit, has_more=offset + len(items) < total)
                else:
                    with worker.condition:
                        identities = tuple(worker.jobs)
                    # Legacy callers retain the full-list contract. New
                    # operator views use the bounded page above.
                    value = []
                    for identity in identities:
                        try:
                            result = worker.public_get(identity) if scheduler and not operator else worker.get(identity)
                        except KeyError:
                            continue
                        value.append(result)
            elif len(parts) == 2 and parts[0] == "assets" and self.command in {"GET", "POST"}:
                job_id = self.headers.get('X-DCO-Asset-Job')
                value = worker.asset(parts[1], body, **({'job_id':job_id} if job_id else {}))
            elif len(parts) >= 2 and parts[0] == "jobs":
                identity = parts[1]
                if len(parts) == 2 and self.command == "POST":
                    callback=self.headers.get('X-DCO-Callback')
                    if scheduler:
                        value = worker.submit(identity,body,operator=operator)
                    else:
                        if not operator:
                            with zipfile.ZipFile(io.BytesIO(body)) as archive:
                                from .policy import validate_operator_fields
                                validate_operator_fields(json.loads(archive.read('task.json')),False)
                        value = worker.submit(identity,body,callback=json.loads(callback)) if callback else worker.submit(identity,body)
                    if scheduler and not operator and isinstance(value, dict):
                        value = worker.public_result(value)
                    worker.activity.submitted(value, self.client_address[0])
                    return self._reply(value, 202)
                if len(parts) == 2 and self.command == "GET":
                    value = worker.public_get(identity) if scheduler and not operator else worker.get(identity)
                elif len(parts) == 2 and self.command == "DELETE":
                    value = worker.delete(identity)
                elif parts[2:] == ["cancel"] and self.command == "POST":
                    value = worker.cancel(identity)
                    if scheduler and not operator and isinstance(value, dict):
                        value = worker.public_result(value)
                elif parts[2:] == ['resize'] and self.command == 'POST':
                    request = json.loads(body)
                    options={k:request[k] for k in ('active_cpus','memory_mb','base_memory_mb','memory_per_cpu_mb','memory_limit_mb') if k in request}
                    if not scheduler and 'memory_budget_mb'in request:options['memory_budget_mb']=request['memory_budget_mb']
                    value = worker.resize(identity, request['cpus'], **options)
                    if scheduler and not operator and isinstance(value, dict):
                        value = worker.public_result(value)
                elif parts[2:] == ['lease'] and self.command == 'POST' and hasattr(worker,'lease'):
                    value=worker.lease(identity,json.loads(body))
                elif parts[2:] == ['asset-handoff'] and self.command in {'POST','DELETE'}:
                    value=worker.asset_handoff(identity,json.loads(body)['assets'] if self.command=='POST' else None)
                elif parts[2:] == ["artifacts"] and self.command == "GET":
                    if (scheduler and self.headers.get('X-DCO-Artifact-Poll') == '1'
                            and not worker.prepare_artifacts(identity)):
                        return self._reply({'id': identity, 'status': 'preparing'}, 202)
                    value = (worker.artifacts(identity,is_connected=self._connected,public=not operator) if hasattr(worker,'pool')
                             else worker.artifacts(identity))
                elif parts[2:] == ["logs"] and self.command == "GET":
                    value = worker.logs(identity)
                elif len(parts) >= 4 and parts[2] == 'files' and self.command in {'GET', 'POST'}:
                    name = unquote('/'.join(parts[3:]))
                    if self.command=='GET' and self.headers.get('X-DCO-File-Version')=='1':
                        version=worker.job_file_version(identity,name,self.headers.get('If-None-Match'))
                        return self._reply(version['data'] or b'',200 if version['changed'] else 304,etag=version['version'])
                    value = worker.job_file(identity, name, body if self.command == 'POST' else None)
                    if self.command == 'POST':
                        worker.activity.notify('Job file ' + name + ' [' + identity[:8] + ']', self.client_address[0])
                else:
                    return self._reply({"error": "Unknown route"}, 404)
            else:
                return self._reply({"error": "Unknown route"}, 404)
            if self.command == 'GET' and self.headers.get('X-DCO-Brief') == '1':
                if parts == ['health']:
                    value={k:value[k] for k in ('protocol','node','role','version','capabilities') if k in value}
                elif parts == ['jobs']:
                    # Paged job views are mappings; the legacy view remains a
                    # list.  Normalize only the rows so X-DCO-Brief cannot
                    # iterate pagination keys (``items``, ``total``...) as if
                    # they were result objects.
                    if isinstance(value, dict) and 'items' in value:
                        value = {**value, 'items': [progress(row) for row in value['items']]}
                    else:
                        value = [progress(row) for row in value]
                elif len(parts) == 2 and parts[0] == 'jobs':
                    value = progress(value)
            self._reply(value)
        except KeyError as exc:
            self._reply({"error": "Unknown job, tool or path variable: " + str(exc)}, 404)
        except Conflict as exc:
            self._reply({"error": str(exc)}, 409)
        except RemoteError as exc:
            self._reply({"error": str(exc)}, 502)
        except FileNotFoundError as exc:
            self._reply({"error": str(exc)}, 404)
        except (ValueError, OSError, zipfile.BadZipFile) as exc:
            self._reply({"error": str(exc)}, 400)

    do_GET = do_POST = do_DELETE = _handle
