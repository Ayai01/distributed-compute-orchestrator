"""A central HTTP endpoint forwarding accepted jobs to the shared worker pool."""

import hashlib
from bisect import insort_right
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, contextmanager, nullcontext
from copy import copy, deepcopy
import io
import json
from pathlib import Path, PurePosixPath
import shutil
import threading
import time
import weakref
import zipfile

from .bundle import PROTOCOL, TERMINAL, atomic_json, batch_status, cancellation_needs_reconnect, json_bytes, pack, progress_fields
from . import client as transport
from . import build_refs
from .client import PoolBusy, RemoteError, RequestCancelled, _check_cancelled
from .worker import (Conflict, QueueFull, _ASSET, _ID, lock_root, validate_task, asset_handoff,
                     handoff_references, retire_asset, missing_task_assets)
from .policy import (MIN_PRIORITY, FRACTIONAL_PRIORITY, MAX_QUEUE_RATIO, MAX_REQUIREMENTS, MAX_READY_TASKS, READY_QUEUE_FLOOR,
                     MAX_REQUIREMENT_LENGTH,
                     MAX_RESOURCE_SHARE, priority_weight, validate_priority,
                     public_capabilities, normalize_accelerator_requirement,
                     aggregate_accelerators, canonical_capabilities, clean_tool_snapshot, validate_ready_tasks, validate_platform,
                     RESOURCE_CONTROL_FIELDS, validate_resource_controls)
from .telemetry import Activity
from .allocation import (ACTIVE_GRANT_CONFLICT, STARTUP_CPU_FLOOR,
                         plan_resize, elastic_cpus, reject_startup_cpu_floor, startup_cpus)
from .memory import plan_memory, startup_memory, same_phase
from .placement import PlacementPlanning, PlacementSnapshot, _elastic_job, _matching_pass
from .recovery import RESOURCE_RECOVERY_DELAYS, reentrant_computation, resource_recovery, resume_map, carry_outputs, restore_map_results, BusinessItemFailure
from .compat import unlink
from .cleanup import (AssetCache, AssetMaintenance, TransportRetirement, UnregisteredRetirementProvider,
                      prune_asset_caches, remove_owned, retire_job)
from .acceptance import SubmissionAssets, SubmissionLocks
from .asset_upload import ASSET_RESUME_CAPABILITY, ASSET_RESUME_MIN_BYTES
from .asset_variants import bind_asset_variants, freeze_asset_variants, load_asset_variants
from .records import Job, JobRecords, active_items, active_values, archive_job, asset_references, load_job
from .observations import Observations, observation_batch, terminal_observation
from .receipts import read_center_receipt as read_receipt
import socket
import math
import os
import re
import secrets

from urllib.parse import urlsplit
from .runtimes import EXECUTION_CAPABILITIES, execution_capabilities, supports_python
from . import __version__
from .policy import validate_operator_fields
from .ramdisk import validate_executable_ram_mb
from .packages import (DEFAULT_PACKAGE, PACKAGES, RETAINED_NAMES, package_names, package_task,
                       project_package, provider_environment, provider_has_package)


RESOURCE_TRANSPORT_RETAINED='Center resource transport interrupted; existing grant retained'
TRANSIENT_WORKER_HTTP = (429, 502, 503, 504)


class _PreparationWorkerLost(Exception):
    """The selected worker disconnected before submission began."""

# These observations describe one worker attempt, rather than the frozen job.
ATTEMPT_OBSERVATIONS = ('runtime','progress','progress_error','diagnostic','phase','stage','unit',
    'completed','total','current','target','nfe','budget','active_cpus','requested_cpus',
    'min_cpus','memory_phase','memory_reservation_mb','memory_commit_reservation_mb','deadline','resource_request','execution_timing',
    'task_process_started','preparation_stage','preparation_seconds','worker_sampled_at',
    'worker_preparation_elapsed_seconds','execution_started','bundle_unpack_seconds','worker_admission_seconds',
    'dispatch_stage','dispatch_stage_started_at','handoff_seconds','handoff_acknowledged_at','handoff_transport')


def _restore_phase_demand(job):
    """A useful ceiling is not the serial entry's current ready width."""
    if 'desired_cpus' in job:return
    requested=job['result'].get('requested_cpus')
    if job['result'].get('status')!='queued' and type(requested)is int and requested>=0:
        if requested==0:
            node=job.get('node')
            if node is None:return  # Automatic width needs the reattached provider.
            requested=node.capacity
        job['desired_cpus']=min(requested,job['task']['max_cpus'])
    elif job['task'].get('execution',{}).get('mode')=='entry':
        # Nested map/call requests publish their useful widths at phase boundaries.
        job['desired_cpus']=startup_cpus(job['task'])


def _infrastructure_interruption(job, outcome):
    # Named-machine operations may change external state and retain their
    # explicit retry allowance. Ordinary frozen jobs run in fresh workdirs.
    return not job['task'].get('cwd') and (outcome.get('status') == 'interrupted'
        or outcome.get('error_kind') == 'memory_pressure'
        or (outcome.get('error_kind') == 'scratch_unavailable' and outcome.get('task_process_started') is False
            and outcome.get('cpu_seconds') == 0))


class Coordinator(PlacementPlanning):
    max_queued = 16384
    max_queued_per_project = 12000
    shared_asset_visibility_seconds = 150
    observation_poll_seconds = 5
    observation_grace_seconds = 10
    display_health_interval = .75

    def __init__(self, pool, root, *, token="", operator_token=None, paused=False):
        started=time.monotonic();self.startup={}
        self.pool, self.token = pool, token
        self.activity = Activity()
        self.root = Path(root).resolve()
        self.asset_variants = load_asset_variants(self.root / 'asset-variants.json')
        self.root_lock = lock_root(self.root, "scheduler")
        (self.root / "jobs").mkdir(parents=True, exist_ok=True)
        (self.root / "assets").mkdir(exist_ok=True)
        self.operator_token = self._load_operator_token(operator_token)
        self.condition = threading.Condition(threading.RLock())
        self.cpu_suspension_path = self.root / 'cpu-suspended-nodes.json'
        self.cpu_suspended = set(json.loads(self.cpu_suspension_path.read_bytes())) if self.cpu_suspension_path.is_file() else set()
        with self.pool.condition:
            for node in self.pool.nodes:
                node.cpu_suspended = node.name in self.cpu_suspended
        self.submission_lock = SubmissionLocks()
        self.accepting_assets = SubmissionAssets()
        self.asset_writes = weakref.WeakValueDictionary()
        self.asset_flight_lock = threading.Lock()
        self.asset_flights = {}
        self.shared_asset_publications = {}
        self.jobs, self.queue, self.threads = JobRecords(
            self.root / 'jobs', index_path=self.root / 'history.index.jsonl',receipt_reader=read_receipt), [], set()
        self.project_leaders = {}
        self.cancel_snapshot_path = self.root / 'cancel-snapshot.json'
        saved_cancellations = set(json.loads(self.cancel_snapshot_path.read_bytes())) if self.cancel_snapshot_path.is_file() else set()
        self.cancel_snapshot = None
        self.cancel_snapshot_thread = None
        self.retirement = TransportRetirement(self.root / 'retirements', self._reclaim_retired)
        self.observations = Observations(self.root / 'observed-jobs.json')
        self.observation_due = 0
        self.observation_retries = {}
        self.observation_probe = None
        self.observation_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix='observed-receipts')
        self.asset_handoffs = {}
        self.legacy_assets = set()
        self.stopping = False
        self.paused = bool(paused)
        self.accepting = True
        admission_policy=self.root/'admission-control.json'
        if admission_policy.is_file():
            saved=json.loads(admission_policy.read_bytes())['accepting']
            if type(saved)is not bool:raise ValueError('Saved accepting policy requires a boolean')
            self.accepting=saved
        self.restart_available = False
        self.callback_address = None
        phase=time.monotonic();indexed = self.jobs.load_index()
        self.retirement.restore(self.jobs)
        self.startup['index_seconds']=round(time.monotonic()-phase,4);phase=time.monotonic()

        def restore_live(path, value):
            if clean_tool_snapshot(value):
                atomic_json(path, value)
            job = Job(result=value, read_result=deepcopy(value), directory=path.parent,
                      cancel=threading.Event(), node=None)
            # The index already accounts for terminal receipts.  Only live
            # entries need objects in memory after a fast restart.
            self.jobs.active[value["id"]] = job
            with zipfile.ZipFile(path.parent/'request.zip') as archive:
                task=json.loads(archive.read('task.json'))
            task=self._restored_task(task, value)
            value['priority'] = task['priority']
            job['read_result']['priority'] = task['priority']
            self._restore_accelerator(task, value)
            job.update(task=task,auto_cpus=task['cpus']==0,elastic_cpus=elastic_cpus(task))
            _restore_phase_demand(job)
            if value['id'] in saved_cancellations:
                value['cancel_requested'] = True
            if value.get('cancel_requested'):job['cancel'].set()
            value.setdefault('max_retries',task.get('retries',2))
            if value['status']=='queued' or not value.get('worker_url'):
                if job['cancel'].is_set():
                    value.update(status='cancelled',finished=time.time())
                    self._save(job);archive_job(self.jobs,job,startup=True)
                    return
                if value.get('worker_id'):job['worker_id']=value['worker_id']
                value.update(status='queued')
                self._enqueue(value['id'])
            else:
                job['recover_submission']=value['status']=='dispatching' and value.get('started')is None
                job['recover']=True
                if not cancellation_needs_reconnect(value):
                    value.update(status='dispatching')
            if not cancellation_needs_reconnect(value):
                value.pop('error',None)
            self._save(job)

        if indexed:
            # A bounded startup touches only the small set of non-terminal
            # jobs recorded in the index.  Terminal receipts stay addressable
            # by ID through their original directories.
            for identity in tuple(self.jobs.pending):
                if identity in self.jobs.deleted:
                    continue
                path = self.root / 'jobs' / identity / 'result.json'
                value = read_receipt(path, assets=self.jobs.pending_assets.get(identity, ()))
                if value.get('status') in TERMINAL and not (value.get('status') == 'interrupted' and value.get('finished') is None):
                    job = Job(result=value, directory=path.parent, cancel=threading.Event(), node=None)
                    self.jobs.active[identity] = job
                    archive_job(self.jobs, job, startup=True)
                else:
                    restore_live(path, value)
        else:
            # One-time migration path for an older state root.  It preserves
            # the established recovery semantics, then writes the compact
            # index so later starts do not rescan every terminal receipt.
            for path in (self.root / "jobs").glob("*/result.json"):
                if path.parent.name in self.jobs.deleted:
                    continue
                value = read_receipt(path)
                if value["status"] not in TERMINAL or (value['status']=='interrupted' and value.get('finished') is None):
                    restore_live(path, value)
                else:
                    job = Job(result=value, directory=path.parent, cancel=threading.Event(), node=None)
                    self.jobs.active[value["id"]] = job
                    archive_job(self.jobs, job, startup=True)
            self.jobs.write_index()
        if saved_cancellations:
            atomic_json(self.cancel_snapshot_path, [])
        if not self.retirement.pending:
            self.jobs.compact_index()
        self.startup['recovery_seconds']=round(time.monotonic()-phase,4);phase=time.monotonic()
        self.asset_cache = AssetCache(self.root / 'assets', archives=True)
        self.local_asset_workers = tuple(dict.fromkeys(worker for node in self.pool.nodes
                                                       if (worker := getattr(node.client, 'worker', None)) is not None))
        self.asset_cache.reclaim_uploads()
        self.startup.update(asset_seconds=round(time.monotonic()-phase,4),
                            seconds=round(time.monotonic()-started,4),pid=os.getpid(),version=__version__)
        atomic_json(self.root/'startup-current.json',self.startup)
        self.asset_maintenance = AssetMaintenance(lambda: self._maintain_assets(wait=False), self._report_asset_cleanup)
        self.scheduler = threading.Thread(target=self._schedule, name="central-dispatch", daemon=True)
        self.scheduler.start()
        self.retirement.start()
        self.asset_maintenance.request()

    def _load_operator_token(self, supplied):
        path = self.root / 'operator.token'
        token = supplied or os.environ.get('REMOTE_COMPUTE_OPERATOR_TOKEN', '')
        if not token and path.is_file():
            token = path.read_text(encoding='utf-8').strip()
        if not token:
            token = secrets.token_urlsafe(32)
        # The token is local operator state, never part of a task or report.
        if not path.is_file() or path.read_text(encoding='utf-8').strip() != token:
            path.write_text(token + '\n', encoding='utf-8')
            try:
                if os.name != 'nt':
                    path.chmod(0o600)
            except OSError:
                pass
        return token

    def nodes(self):
        return self.pool.snapshot()

    def job_observed(self, identity):
        observations = getattr(self, 'observations', None)
        return observations is not None and identity in observations.rows

    def job_identities(self):
        return (*self.observations.rows, *self.jobs)

    def jobs_page(self, *, limit, offset, prefix=None):
        active = [identity for identity in self.jobs.active if prefix is None or identity.startswith(prefix)]
        observed = sorted((row for row in self.observations.rows.values()
                           if prefix is None or row['id'].startswith(prefix)),
                          key=lambda row: (row['submitted'], row['id']), reverse=True)
        active.extend(row['id'] for row in observed if row['status'] not in TERMINAL)
        head = active + [row['id'] for row in observed if row['status'] in TERMINAL]
        selected = head[offset:offset + limit]
        slots = limit-len(selected)
        native, total = self.jobs.page(limit=max(1, slots), offset=max(0, offset-len(head)),
                                       prefix=prefix, include_active=False)
        return [*selected, *native[:slots]], len(head)+total

    def observe(self, rows):
        batch = observation_batch(rows)
        with ExitStack() as locks:
            for identity in sorted(batch):
                locks.enter_context(self.submission_lock.hold(identity))
            with self.condition, self.pool.condition:
                if self.stopping:
                    raise Conflict('Scheduler is stopping')
                nodes = {node.name for node in self.pool.nodes}
                reply = self.observations.upsert(batch, nodes, self.jobs.knows_worker)
                self.condition.notify_all()
                return reply

    def _poll_observations(self):
        """Schedule one small original-ID lookup from the existing scheduler tick."""
        now = time.monotonic()
        if not self.observations.active or now < self.observation_due or (
                self.observation_probe is not None and not self.observation_probe.done()):
            return
        self.observation_due = now + self.observation_poll_seconds
        self.observation_retries = {key: retry for key, retry in self.observation_retries.items()
                                    if key in self.observations.active}
        with self.pool.condition:
            nodes = {n.name: n for n in self.pool.nodes if n.enabled and not n.error and not n.detached
                     and 'active_jobs' in n.health and n.sampled_at and
                     now-n.sampled_at <= max(15, self.pool.sample_seconds*3)}
            sampled = {name: {item['id'] for item in (node.health['active_jobs'] or ())} for name, node in nodes.items()}
            batch, selected = {}, None
            wall = time.time()
            for identity in sorted(self.observations.active,
                                   key=lambda key: self.observation_retries.get(key, (0, 0))[0]):
                row = self.observations.rows[identity]
                node = nodes.get(row['node'])
                if node is None or (selected is not None and node is not selected):
                    continue
                if identity in sampled[node.name]:
                    self.observation_retries.pop(identity, None)
                    continue
                due, attempts = self.observation_retries.get(identity, (0, 0))
                if now < due or wall-row['submitted'] < self.observation_grace_seconds:
                    continue
                selected = node
                batch[identity] = row
                self.observation_retries[identity] = now + min(120, 10*2**min(attempts, 4)), attempts+1
                if len(batch) == 8:
                    break
            if batch:
                self.observation_probe = self.observation_executor.submit(
                    self._read_observed_receipts, selected, selected.client, selected.connection_generation, batch)

    def _read_observed_receipts(self, node, peer, generation, batch):
        client = peer if peer.url.startswith('local://') else transport.Client(
            peer.url, peer.token, timeout=min(peer.timeout, 3), ca_file=peer.ca_file,
            operator=True, operator_token=peer.operator_token, relay_url=peer.relay_url, peer_id=peer.peer_id)
        try:
            states = client.request('/jobs/status', {'ids': list(batch)})
        except RemoteError:
            return  # A failed control read leaves observations pending for the next bounded retry.
        if not isinstance(states, dict):
            return
        for identity, previous in batch.items():
            candidate = states.get(identity)
            if not isinstance(candidate, dict) or candidate.get('status') not in TERMINAL:
                continue
            try:
                if 'cpu_seconds' not in candidate:
                    # Older workers omit fees in brief state. Read only the
                    # just-confirmed terminal original, never its artifacts.
                    candidate = client.status(identity)
                update = terminal_observation(previous, candidate)
            except (RemoteError, ValueError):
                continue
            if update is None:
                continue
            with self.condition, self.pool.condition:
                if self.stopping or node.client is not peer or node.connection_generation != generation:
                    return
                if self.observations.rows.get(identity) is previous:
                    self.observations.upsert(update, {node.name}, self.jobs.knows_worker)
                    self.observation_retries.pop(identity, None)

    def admission(self, request, *, operator=False):
        project = request.get('project') or 'default'
        job_id=request.get('job_id')
        advice={}
        share = request.get('resource_share', 1.0)
        ratio = request.get('queue_ratio', 1.0)
        cpus = request.get('cpus', 1)
        maximum_cpus = request.get('max_cpus')
        ready_tasks = validate_ready_tasks(request.get('ready_tasks'))
        platform = validate_platform(request.get('platform'))
        executable_ram_mb = (validate_executable_ram_mb(request['executable_ram_mb'])
                             if 'executable_ram_mb' in request else 0)
        validate_operator_fields(request,operator)
        raw_requires = request.get('requires', [])
        raw_accelerator = request.get('accelerator')
        if operator:
            package = None
            requires = canonical_capabilities(raw_requires)
            accelerator = normalize_accelerator_requirement(raw_accelerator)
        else:
            package, accelerator = project_package(raw_requires, raw_accelerator)
            requires = PACKAGES[package].requirements
        node_name=request.get('node') if operator else None
        if (not isinstance(project, str) or isinstance(share, bool) or not isinstance(share, (int,float))
                or not math.isfinite(share) or not 0 < share <= MAX_RESOURCE_SHARE or isinstance(ratio, bool)
                or not isinstance(ratio, (int,float)) or not math.isfinite(ratio) or not 0 <= ratio <= MAX_QUEUE_RATIO
                or type(cpus) is not int or cpus < 0 or not isinstance(requires,list)
                or len(requires) > MAX_REQUIREMENTS
                or any(not isinstance(tag,str) or not tag or len(tag) > MAX_REQUIREMENT_LENGTH for tag in requires)
                or (accelerator is not None and not isinstance(accelerator, dict))
                or (node_name is not None and not isinstance(node_name,str))
                or (maximum_cpus is not None and (type(maximum_cpus) is not int or maximum_cpus<max(1,cpus)))):
            raise ValueError(f'project, resource_share <= {MAX_RESOURCE_SHARE:g}, queue_ratio <= {MAX_QUEUE_RATIO:g} and cpus required')
        providers = tuple(self.pool.nodes)
        paused={node.name for node in providers if node.health.get('paused')}
        placement = None
        if paused or job_id is not None:
            with self.condition, self.pool.condition:
                placement = PlacementSnapshot(self)
            providers = placement.pool.nodes
            paused = {node.name for node in providers if node.health.get('paused')}
        resources = placement._project_resources() if placement is not None else {}
        held = ({name: cpus for name, cpus in resources.get(project, {}).get('node_demands', {}).items()
                 if name in paused} if not executable_ram_mb else {})
        eligible=[n for n in providers if not n.error and n.enabled and not n.health.get('paused')
                  and n.capacity>=max(1,cpus)
                  and (not node_name or n.name==node_name)
                  and (platform is None or n.health.get('platform') == platform)
                  and (not executable_ram_mb or n.executable_ram_capacity >= executable_ram_mb)
                  and (provider_has_package(n.health, package) if package is not None else
                       set(requires).issubset(canonical_capabilities(n.health.get('tags',()))))
                  ]
        execution=dict(requires=requires,accelerator=accelerator)
        fallback_capable = (sum(n.capacity for n in eligible
                                if supports_python(dict(execution, accelerator=None),
                                                   n.health.get('python_backends', ['cpu'])))
                            if accelerator is not None and 'fallback_after_seconds' in accelerator else 0)
        eligible = [n for n in eligible if n.supports_accelerator(n.execution_accelerator(execution))
                    and supports_python(dict(execution,accelerator=n.execution_accelerator(execution)),n.health.get('python_backends',['cpu']))]
        capable=sum(n.capacity for n in eligible)
        target=capable+sum(held.values())
        if job_id is not None:
            job=placement.jobs.get(job_id)
            if job is None or job.get('node') is None or job['result']['status'] in TERMINAL:
                raise Conflict('admission job_id must identify a running task')
            if (job['task'].get('project') or 'default')!=project:
                raise ValueError('admission project differs from the running task')
            node=job['node'];old=job['task']['cpus'];old_memory=job['task']['memory_mb']
            advised=max(1,min(job['task'].get('max_cpus',node.capacity),node.capacity,
                              node.capacity-node.reserved-node.external+old))
            if _elastic_job(job) and not node.error and project in resources:
                advised=min(advised,placement._cooperative_cpu_target(job_id,node.capacity,resources))
            advice=dict(recommended_cpus=advised if node.enabled and not node.error and not node.cpu_suspended and not node.health.get('paused') else old,
                        current_cpus=old,current_memory_mb=old_memory,
                        available_memory_mb=old_memory+max(0,min(node.memory_free,placement.pool._host_memory_free(node))))
        ceilings=[min(n.limit or n.health.get('cpu_limit',n.capacity),n.health.get('cpu_limit',n.capacity)) for n in eligible]+list(held.values())
        maximum=sum(ceilings)
        unit = cpus or maximum_cpus
        # Grants on different devices cannot share one task. Count each
        # device's partial automatic grant instead of merging their CPUs.
        # Fixed jobs can repack the fair plan's node shares; only physical
        # device remainders impose a per-node limit on their task count.
        def slots(values):
            return sum(math.ceil(value/unit) if unit else bool(value) for value in values)
        wave = max(1,slots(node.capacity for node in eligible) if cpus==0
                   else min(math.ceil(target/unit),sum(node.capacity//unit for node in eligible)+sum(value//unit for value in held.values())))
        ready = wave
        ceiling_wave=max(1,slots(ceilings) if cpus==0 else sum(value//unit for value in ceilings))
        ceiling=ceiling_wave
        if cpus==0:
            # An automatic task can return most of its CPUs after compilation.
            # Keep those running tasks and admit another wave into the freed
            # credits; prefetch still covers only a full-width wave.
            with self.condition,self.pool.condition:
                names={node.name for node in eligible}|set(held)
                running=sum(job.get('auto_cpus',False) and (job.get('task',{}).get('project') or 'default')==project
                            and job['result'].get('assigned_node') in names
                            and job['result']['status'] not in TERMINAL for job in active_values(self.jobs))
                if running:
                    room=sum(max(0,node.capacity-node.reserved-node.external) for node in eligible)
                    free=[max(0,node.capacity-node.reserved-node.external) for node in eligible]
                    credits=[]
                    remaining=room
                    for value in free:
                        credits.append(min(value,remaining))
                        remaining-=credits[-1]
                    extra=slots(credits) or int(room>0)
                    ready=max(1,running+extra)
            ceiling=max(1,maximum)  # Every admitted task may later shrink to one CPU.
        value = dict(project=project,resource_share=share,queue_ratio=ratio,capable_cpus=capable,accepting=getattr(self,'accepting',True),
                    dispatch_paused=getattr(self, 'paused', False),
                    accelerator=accelerator,fallback_capable_cpus=fallback_capable,
                    **advice,
                    running_tasks=ready,queued_tasks=math.ceil(wave*ratio),
                    in_flight=ready+math.ceil(wave*ratio),max_in_flight=ceiling+math.ceil(ceiling_wave*ratio))
        if operator and platform is not None:
            value['platform'] = platform
        if executable_ram_mb:
            value['executable_ram_mb'] = executable_ram_mb
        if package is not None:
            value['capability_package'] = package
        if ready_tasks is None and ratio > 0:
            ready_tasks = min(MAX_READY_TASKS, max(READY_QUEUE_FLOOR, maximum + 1, value['in_flight']))
        if ready_tasks is not None:
            # Queue depth is transport preparation, not another CPU reservation.
            # All producers of a project share this outstanding-work bound.
            active = sum((job.get('task', {}).get('project') or 'default') == project
                         and job['result']['status'] not in TERMINAL
                         for job in active_values(self.jobs, snapshot=True))
            running = min(ready_tasks, value['running_tasks'])
            value.update(ready_tasks=ready_tasks, ready_source='center', queue_scope='project',
                         project_active_tasks=active, running_tasks=running,
                         queued_tasks=ready_tasks-running, in_flight=ready_tasks, max_in_flight=ready_tasks)
        queued_count = len(self.queue)
        project_queued = project_active = 0
        for job in active_values(self.jobs, snapshot=True):
            row = job.get('read_result') or job['result']
            if row.get('project') == project and row['status'] not in TERMINAL:
                project_active += 1
                project_queued += row['status'] == 'queued'
        value['submission_window'] = min(self.max_queued_per_project, ceiling + math.ceil(ceiling_wave * ratio))
        value['submission_slots'] = max(0, min(self.max_queued - queued_count,
            self.max_queued_per_project - project_queued, value['submission_window'] - project_active))
        if value['dispatch_paused'] or not value['accepting'] or not (capable or fallback_capable):
            value['submission_slots'] = 0
            value['submission_window'] = 0
        if request.get('activity') is True:
            # Telemetry reads the published worker samples and active records.
            # A CPU resize or dispatch must not hold up project producers.
            nodes = tuple(self.pool.nodes)
            online = eligible if executable_ram_mb else [node for node in nodes if node.health and node.enabled and
                                                         not node.error and not node.health.get('paused')]
            value.update(self._cpu_availability(online, operator=operator, executable_ram_mb=executable_ram_mb))
            value['activity'] = self._project_activity(project, nodes)
        if package is not None and not capable and not fallback_capable:
            value.update(running_tasks=0, queued_tasks=0, in_flight=0, max_in_flight=0)
        return value

    def _project_activity(self, project, nodes):
        """Expose each original's CPU sample without revealing its worker."""
        disconnected = {node.name for node in nodes if node.error}
        reported = {sample['id']: sample for node in nodes if not node.error
                    for sample in node.health.get('active_jobs') or ()}
        rows = []
        for job in active_values(self.jobs, snapshot=True):
            result = job['result'].copy()
            if (job['task'].get('project') or 'default') != project or result['status'] in TERMINAL:
                continue
            rows.append((result, reported.get(job.get('worker_id') or result['id'], {})))
        observed = self.observations.active.copy()
        observed_rows = self.observations.rows
        for identity in observed:
            row = observed_rows.get(identity)
            if row and row['status'] not in TERMINAL and row['project'] == project:
                rows.append((row, reported.get(row['id'], {})))
        jobs = []
        for result, sample in rows:
            runtime = sample.get('runtime') or result.get('runtime') or {}
            active = sample.get('active_cpus')
            if active is None and result.get('assigned_node', result.get('node')) not in disconnected:
                active = result.get('active_cpus')
            if type(active) is not int or active < 0:
                active = None
            jobs.append(dict(id=result['id'], status=result['status'], cpus=result.get('cpus'),
                active_cpus=active,
                cpu_seconds=runtime.get('cpu_seconds'), sampled_at=runtime.get('sampled_at'),
                process_alive=runtime.get('process_alive')))
        running = [job['active_cpus'] for job in jobs if job['status'] == 'running']
        observed = [width for width in running if width is not None]
        active = dict(lower_bound=sum(observed), observed_jobs=len(observed), running_jobs=len(running),
                      coverage=len(observed) / len(running) if running else 1.0)
        return dict(sampled_at=time.time(), jobs=jobs, active_cpus=active)

    @staticmethod
    def _cpu_availability(nodes, *, operator=False, executable_ram_mb=0):
        capacity = available = 0
        labels = {} if operator else {name: {'capacity': 0, 'available': 0} for name in PACKAGES}
        for node in nodes:
            width = node.capacity
            free = max(0, width - node.reserved - node.external)
            if executable_ram_mb and node.executable_ram_available < executable_ram_mb:
                free = 0
            capacity += width
            available += free
            tags = node.health.get('tags', ())
            names = execution_capabilities(tags) if operator else package_names(node.health)
            for label in names:
                if not operator:
                    package = PACKAGES[label]
                    if package.accelerator and not node.supports_accelerator(
                            normalize_accelerator_requirement({'kind': package.accelerator})):
                        continue
                entry = labels.setdefault(label, {'capacity': 0, 'available': 0})
                entry['capacity'] += width
                entry['available'] += free
        return {'cpu': {'capacity': capacity, 'available': available, 'unit': 'logical_threads'},
                'capability_cpus': dict(sorted(labels.items()))}

    def restart_node(self, name):
        node = next((n for n in self.pool.nodes if n.name == name), None)
        if node is None:
            raise ValueError("Unknown node: " + str(name))
        try:
            return {"node": name, **node.client.restart()}
        except RemoteError as error:
            raise Conflict(str(error)) from None

    def health(self, *, brief=False, sample=False, display=False):
        if brief:
            return dict(protocol=PROTOCOL,role='scheduler',node=socket.gethostname(),version=__version__,
                        capabilities=['resize','resize_memory','resize_auto','resize_memory_model','resize_active_cpus','max_cpus','admission_job','admission_ready_tasks','batch_status','job_prefix','job_file_version','retry_jobs','accelerators','accelerator_fallback','asset_handoff','cache_references','cpu_accounting','kill_snapshot',FRACTIONAL_PRIORITY]+(['restart'] if self.restart_available else []))
        if display:
            value = self._display_health()
        else:
            with self.condition, self.pool.condition:
                view, projects = self._health_inputs()
            value = view._health()
            value['projects'] = projects._project_resources()
        relay = transport._RELAY_BULK_BUDGET
        value['relay_bulk'] = relay.snapshot() if relay is not None else None
        return value

    def _health_inputs(self, *, display=False):
        """Capture published state; build tables and aggregate resources after releasing both locks."""
        projects = PlacementSnapshot(self,display=display)
        view = copy(self)
        view.pool = projects.pool
        view.pool.condition = nullcontext()
        view.condition = nullcontext()
        view.queue = projects.queue
        view.observations = copy(self.observations)
        if isinstance(self.jobs, JobRecords):
            view.jobs = copy(self.jobs)
            view.jobs.active = projects.jobs
            view.jobs.recent = self.jobs.recent.copy()
        else:
            view.jobs = projects.jobs
        return view, projects

    def _display_health(self):
        """Keep the display responsive and queue a fresh sample when busy."""
        cached = getattr(self, '_display_health_cache', None)
        if cached is None or time.monotonic() - cached[0] >= self.display_health_interval:
            refresh = vars(self).setdefault('_display_health_refresh', threading.Lock())
            if refresh.acquire(blocking=cached is None):
                handoff = False
                try:
                    if getattr(self, '_display_health_cache', None) is cached and (
                            not self._capture_display_health(wait=cached is None)):
                        threading.Thread(target=self._finish_display_refresh, args=(refresh,),
                                         name='dco-display-refresh', daemon=True).start()
                        handoff = True
                finally:
                    if not handoff:
                        refresh.release()
        sampled, wall, value = self._display_health_cache
        age = time.monotonic() - sampled
        return dict(value, health_sampled_at=wall, health_age_seconds=round(age, 3),
                    health_stale=age >= self.display_health_interval)

    def _capture_display_health(self, *, wait):
        if not wait:
            return False  # Return the previous sample while one background reader refreshes it.
        view, projects = self._health_inputs(display=True)
        sampled = (time.monotonic(), time.time())
        value = view._health()
        value['projects'] = projects._project_resources()
        self._display_health_cache = (*sampled, deepcopy(value))
        return True

    def _finish_display_refresh(self, refresh):
        try:
            self._capture_display_health(wait=True)
        except Exception as error:
            self.activity.notify('面板采样失败 ' + str(error), 'local')
        finally:
            refresh.release()

    def capabilities(self, *, platform=None, operator=False):
        """Return the scheduler's aggregate project-facing capability view.

        This deliberately has no node names, URLs, paths, jobs, project
        quotas, or placement policy.  The operator console continues to use
        :meth:`health` for that private view.
        """
        validate_operator_fields(dict(platform=platform),operator)
        platform = validate_platform(platform)
        online = [node for node in tuple(self.pool.nodes) if node.health and not node.error and node.enabled
                  and not node.health.get('paused') and (platform is None or node.health.get('platform') == platform)]
        memory_capacity = sum(node.memory_capacity for node in online)
        memory_available = sum(max(0, node.memory_free) for node in online)
        detail = {'default_package': DEFAULT_PACKAGE}
        if operator:
            tags = {tag for node in online for tag in execution_capabilities(node.health.get('tags', ()))}
            tags.difference_update({'python', 'ml'})
            detail = {
                'platforms': sorted({node.health.get('platform') for node in online} - {None, ''}),
                'tags': [name for name in EXECUTION_CAPABILITIES if name in tags],
                'tools': [name for name in EXECUTION_CAPABILITIES if name in tags],
                'accelerators': aggregate_accelerators(profile for node in online
                                                       for profile in node.health.get('accelerators', ())),
            }
        capacity = self._cpu_availability(online, operator=operator)
        for base in (DEFAULT_PACKAGE, 'ml'):
            capacity['capability_cpus'].pop(base, None)
        return {
            'protocol': PROTOCOL,
            'role': 'scheduler',
            'version': __version__,
            **detail,
            'capabilities': public_capabilities(),
            **capacity,
            'memory': {'capacity_mb': memory_capacity, 'available_mb': memory_available, 'unit': 'MiB'},
        }

    def public_error(self, value):
        """Return a diagnostic without exposing a provider address or path."""
        value = re.sub(r'https?://[^\s\])}]+', '<remote>', str(value))
        value = re.sub(r'(?i)(?:[A-Za-z]:[\\/]|/(?:home|tmp|var|mnt)/)[^\s\])}]+', '<remote-path>', value)
        names = {name for node in self.pool.nodes for name in
                 (node.name, node.display_name, node.health.get('node'), node.host_ip, node.health.get('host_ip'))
                 if isinstance(name, str) and len(name) > 2}
        if names:
            pattern = '|'.join(re.escape(name) for name in sorted(names, key=len, reverse=True))
            value = re.sub(r'(?<![\w.-])(?:'+pattern+r')(?![\w.-])', '<remote>', value, flags=re.I)
        return value

    def public_result(self, result):
        """Return a project result without placement or worker-local paths."""
        hidden = {'assigned_node', 'worker_url', 'worker_id', 'node', 'cwd', '_artifact_abi', 'placement_scope',
                  'tool_root', 'root', 'home', 'worker_root',
                  'shared_paths', 'unavailable_shared', 'resource_request',
                  'observe_only', 'source', 'revision', '_asset_variants'}

        def clean_mapping(mapping):
            cleaned = {key: item for key, item in mapping.items() if key not in hidden}
            package = cleaned.get('capability_package')
            if package in RETAINED_NAMES:
                cleaned['capability_package'] = RETAINED_NAMES[package]
            for key in ('error', 'connection_error', 'artifact_publication_error'):
                if isinstance(cleaned.get(key), str):
                    cleaned[key] = self.public_error(cleaned[key])
            if isinstance(mapping.get('cache_hit'), dict):
                hit = mapping['cache_hit']
                cleaned['cache_hit'] = dict(artifact=bool(hit.get('artifact')), checkpoint=bool(hit.get('checkpoint')),
                                            worker_id=result['id'])
            # Keep per-step timing and return codes without argv/path data.
            if isinstance(mapping.get('steps'), list):
                cleaned['steps'] = [{key: step[key] for key in ('started', 'finished', 'returncode', 'cpu_phase', 'cpu_seconds') if key in step}
                                   for step in mapping['steps'] if isinstance(step, dict)]
            if result.get('capability_package'):
                cleaned.pop('tools', None)
            elif isinstance(mapping.get('tools'), dict):
                cleaned['tools'] = {}
                for name, info in mapping['tools'].items():
                    if not isinstance(info, dict):
                        continue
                    cleaned['tools'][name] = {'available': bool(info.get('available'))}
            return cleaned

        value = clean_mapping(result)
        if isinstance(result.get('attempts'), list):
            value['attempts'] = [
                clean_mapping(attempt)
                for attempt in result['attempts']
            ]
        return value

    def public_health(self, *, brief=False):
        if brief:
            return dict(protocol=PROTOCOL, role='scheduler', version=__version__, capabilities=public_capabilities())
        value = self.capabilities()
        value.update({
            'used_cpus': sum(n.reserved + n.external for n in tuple(self.pool.nodes) if not n.error and n.enabled),
            'queued': len(self.queue),
            'paused': self.paused,
            'accepting': getattr(self, 'accepting', True),
        })
        return value

    def _health(self):
        snapshot = self.pool.snapshot()
        online = [n for n in self.pool.nodes if n.health and not n.error and n.enabled]
        machines = {}
        tools, tags = {}, set()
        for node in online:
            machine = node.health.get("machine_id", node.client.url)
            machines.setdefault(machine, []).append(node)
            tags.update(node.health.get("tags", ()))
            for name, value in node.health.get("tools", {}).items():
                if value.get("available"):
                    info = tools.setdefault(name, {"available": True, "variants": []})
                    info["variants"].append({"node": node.name, **value})
        with self.condition:
            active = {}
            known_ids = set()
            for job in active_values(self.jobs):
                result = job['result']
                known_ids.add(job.get('worker_id', result.get('worker_id', result['id'])))
                if result['status'] not in TERMINAL and result.get('assigned_node'):
                    item = {k: result.get(k) for k in ('id','label','status','cpus','max_cpus','memory_mb','started')}
                    item['identity_scope'] = 'task'
                    item['memory_phase']=(result.get('memory_phase')or job['task'].get('memory_model')
                                          or dict(memory_mb=job['task'].get('memory_mb')))
                    item.update(progress_fields(result))
                    item['project'] = job.get('task',{}).get('project','')
                    item.update({key:job['task'].get(key,default) for key,default in
                                 (('priority',MIN_PRIORITY),('resource_share',1.0))})
                    active.setdefault(result['assigned_node'],[]).append(item)
            observation_rows = self.observations.rows
            observed_active = {}
            for row in observation_rows.values():
                if row['status'] not in TERMINAL:
                    observed_active.setdefault(row['node'], []).append(row)
            for node in snapshot['nodes']:
                external, remaining = [], node['external_cpus']
                sampled={item['id']:item for item in node.get('active_jobs',[])}
                # Receipts retain their original assigned name across a rename.
                owned=[item for name in dict.fromkeys([node['name'], *node.get('aliases', [])])
                       for item in active.get(name, [])]
                for item in owned:
                    job=self.jobs[item['id']]
                    worker_id=job.get('worker_id',job['result'].get('worker_id',item['id']))
                    sample = sampled.get(worker_id, {})
                    item['rss_mb'] = sample.get('rss_mb')
                    item.update(progress_fields(sample))
                    if item.get('dispatch_stage') == 'awaiting_worker' and sample.get('status') in ('queued','running'):
                        item['worker_status'] = sample['status']
                    elif item.get('dispatch_stage') == 'awaiting_worker' and not node['error']:
                        item.update(progress_fields(node.get('pending_unpacks', {}).get(worker_id, {})))
                for item in node.get('active_jobs',[]):
                    known = item['id'] in known_ids or item['id'] in self.jobs
                    if isinstance(self.jobs, JobRecords):
                        known = known or self.jobs.knows_worker(item['id'])
                    if not known and 0 < (item.get('cpus') or 0) <= remaining:
                        if item['id'] not in observation_rows:
                            external.append(item)
                        remaining -= item['cpus']
                observed = [self.observations.project(row, sampled.get(row['id']) if not node['error'] else None)
                            for row in observed_active.get(node['name'], ())]
                node['active_jobs'] = owned + observed + external
            return {"protocol": PROTOCOL, "role": "scheduler",
                    "mode":"hybrid" if any(n.client.url.startswith('local://')for n in self.pool.nodes)else"scheduler",
                    "node": socket.gethostname(), "tools": tools,
                    'pid':os.getpid(), 'version':__version__, 'root':self.root.as_posix(),
                    'tool_root':Path(__file__).resolve().parent.parent.as_posix(),
                    'asset_cache': self.asset_cache.snapshot(),
                    'transport_retirement': self.retirement.status(),
                    'retained_jobs': (len(self.jobs.history) if isinstance(self.jobs, JobRecords) else 0)
                                     + sum(j['result']['status'] in TERMINAL for j in active_values(self.jobs))
                                     + sum(row['status'] in TERMINAL for row in observation_rows.values()),
                    'capabilities':['resize','resize_memory','resize_auto','resize_memory_model','resize_active_cpus','max_cpus','admission_job','admission_ready_tasks','batch_status','job_prefix','job_file_version','retry_jobs','accelerators','accelerator_fallback','asset_handoff','cache_references','cpu_accounting',FRACTIONAL_PRIORITY]+(['restart'] if self.restart_available else []),
                    "cpus": sum(n.capacity for n in online), "used_cpus": sum(n.reserved + n.external for n in online),
                    "logical_cpus": sum(group[0].health.get("logical_cpus", 0) for group in machines.values()),
                    "memory_total_mb": sum(group[0].health.get("memory_total_mb", 0) for group in machines.values()),
                    "memory_capacity_mb": sum(min(sum(n.memory_capacity for n in group),
                                                   group[0].health.get("memory_total_mb", 0)) for group in machines.values()),
                    "memory_available_mb": sum(max(0, min(self.pool._host_memory_free(group[0]),
                                                          sum(n.memory_free for n in group))) for group in machines.values()),
                    "memory_reserved_mb": sum(n.memory_accounted for n in online),
                    "memory_requested_mb": sum(n.reserved_memory_mb+n.external_memory_mb for n in online),
                    "paused": self.paused,"accepting":getattr(self,'accepting',True), "queued": len(self.queue), "tags": sorted(tags),
                    "policy": snapshot["policy"], "nodes": snapshot["nodes"],
                    'queued_jobs':[{**{key:self.jobs[identity]['result'].get(key) for key in
                        ('id','label','status','cpus','max_cpus','memory_mb','started')},
                        **{key:self.jobs[identity]['task'].get(key,default) for key,default in
                           (('project','default'),('priority',MIN_PRIORITY),('resource_share',1.0))}}
                        for identity in self.queue],
                    'recent_failures':[{**{key:job['result'].get(key)for key in
                        ('id','label','status','cpus','max_cpus','started','finished','elapsed','assigned_node')},
                        'project':job.get('task',{}).get('project',job['result'].get('project','')),
                        'identity_scope':'task',**progress_fields(job['result'])}
                        for job in list(getattr(self.jobs,'recent',{}).values())
                        if job['result']['status'] in ('failed','timed_out','interrupted')][-8:],
                    **self.activity.snapshot()}

    def project_resources(self):
        with self.condition, self.pool.condition:
            snapshot = PlacementSnapshot(self)
        return snapshot._project_resources()

    def control(self, changes):
        if isinstance(changes, dict) and set(changes) == {'reconcile_original'}:
            return self.reconcile_original(changes['reconcile_original'])
        if isinstance(changes, dict) and set(changes) == {'observations'}:
            return self.observe(changes['observations'])
        if isinstance(changes, dict) and set(changes) == {'relay_bulk'}:
            from .relay_bulk import RELAY_BULK_CONFIG, UNLIMITED_RATE, RelayBulkBudget
            config = changes['relay_bulk']
            if not isinstance(config, dict) or not {'ports', 'bytes_per_second'} <= config.keys() or (
                    config.keys() - {'ports', 'bytes_per_second', 'download_bytes_per_second'}):
                raise ValueError('Relay bulk control requires ports and validated byte rates')
            persisted = dict(config)
            for rate in ('bytes_per_second', 'download_bytes_per_second'):
                if rate in persisted and persisted[rate] is None:
                    persisted[rate] = UNLIMITED_RATE
            budget = RelayBulkBudget(**persisted)
            with self.condition:
                atomic_json(RELAY_BULK_CONFIG, persisted)
                transport._RELAY_BULK_BUDGET = budget
            return budget.snapshot()
        if isinstance(changes,dict)and set(changes)=={'retire_asset'}:
            return retire_asset(self,changes['retire_asset'])
        if isinstance(changes,dict)and 'job'in changes:
            if set(changes)!={'job','priority'}:
                raise ValueError('Queued job control accepts job and priority')
            identity,priority=changes['job'],changes['priority']
            if not isinstance(identity,str)or not _ID.fullmatch(identity):
                raise ValueError('Job id requires 32 lowercase hex characters')
            validate_priority(priority)
            if self.job_observed(identity):
                raise Conflict('Externally observed jobs are read-only')
            job=load_job(self.jobs,self.condition,identity)
            with self.condition:
                if self.stopping:raise Conflict('Scheduler is stopping')
                if job['result']['status']!='queued':
                    raise Conflict('Only queued jobs accept a dispatch priority override')
                result=job['result']
                updated=dict(result,submitted_priority=result.get('submitted_priority',job['task']['priority']),
                             dispatch_priority=priority, priority=priority)
                # No acceptance/index transition occurs. Publish the one
                # durable metadata change before it can affect dispatch.
                atomic_json(job['directory']/'result.json',updated)
                result.update(updated)
                # The normalized live descriptor drives allocation. The
                # immutable request.zip and its identity stay untouched.
                job['task']['priority']=priority
                self._publish(job)
                self.queue.sort(key=lambda key:-self.jobs[key]['task']['priority'])
                self.condition.notify_all()
                return {key:result.get(key)for key in ('id','status','submitted_priority','dispatch_priority','request_sha256')}
        if isinstance(changes, dict) and "node" in changes:
            name = changes["node"]
            node = next((n for n in self.pool.nodes if n.name == name), None)
            if node is None:
                raise ValueError('Unknown node: ' + str(name))
            if "routing" in changes:
                self.pool.configure(name, **changes["routing"])
                if {'url', 'name', 'aliases'} & changes['routing'].keys():
                    self.retirement.routes_changed()
            if "worker" in changes:
                try:
                    result=self._node_worker_control(node, changes["worker"])
                except RemoteError as error:
                    if getattr(error, 'status_code', None) == 400:
                        raise ValueError(str(error)) from None
                    raise Conflict(str(error)) from None
                if set(changes)=={'node','worker'}and set(changes['worker'])=={'retire_asset'}:
                    return dict(result,node=name)
            return self.health()
        if isinstance(changes, dict) and set(changes) == {"policy"}:
            self.pool.configure(policy=changes["policy"])
            return self.health()
        if isinstance(changes, dict) and set(changes) == {"add"}:
            self.pool.add(**{**changes['add'], 'cpu_suspended': changes['add'].get('name') in self.cpu_suspended})
            self.retirement.routes_changed()
            return self.health()
        if (not isinstance(changes,dict)or not changes or set(changes)-{'paused','accepting'}
                or any(type(value)is not bool for value in changes.values())):
            raise ValueError('Scheduler control accepts paused/accepting booleans; node budgets are set per node')
        with self.condition:
            if 'paused'in changes:self.paused=changes['paused']
            if 'accepting'in changes:
                atomic_json(self.root/'admission-control.json',dict(accepting=changes['accepting']),private=True)
                self.accepting=changes['accepting']
            self.condition.notify_all()
        return self.health()

    def _node_worker_control(self, node, changes):
        requested = dict(changes)
        has_cpus = 'cpus' in requested
        cpus = requested.get('cpus')
        if has_cpus and cpus is not None and (type(cpus) is not int or cpus < 0):
            raise ValueError('cpus requires a nonnegative integer or null for automatic')
        held = has_cpus and cpus == 0
        if held:
            requested.pop('cpus')
        elif has_cpus and cpus is None:
            requested['cpus'] = 0  # the worker wire protocol still uses 0 for automatic
        with self.submission_lock.hold('cpu-budget:' + node.name):
            result = node.client.control(**requested) if requested else {}
            if requested:
                self.pool.refresh(nodes=[node])
            if has_cpus:
                with self.condition, self.pool.condition:
                    names = (self.cpu_suspended | {node.name}) if held else (self.cpu_suspended - {node.name})
                    if names != self.cpu_suspended:
                        atomic_json(self.cpu_suspension_path, sorted(names), private=True)
                        self.cpu_suspended = names
                    node.cpu_suspended = held
                    self.condition.notify_all()
                    self.pool.condition.notify_all()
        return result

    def resource_controls(self, changes=None):
        """The website changes numeric device policy through the same owner."""
        if changes is not None:
            if not isinstance(changes,dict)or set(changes)!={'node','changes'}:
                raise ValueError('Resource control requires node and changes')
            node=next((n for n in self.pool.nodes if n.name==changes['node']),None)
            if node is None:raise ValueError('Unknown node: '+str(changes['node']))
            logical=node.health.get('logical_cpus')
            if type(logical)is not int or logical<1:raise Conflict('Device CPU limits are awaiting a health sample')
            values = changes['changes']
            numeric = {**values, 'cpus': 0} if isinstance(values, dict) and 'cpus' in values and values['cpus'] is None else values
            validate_resource_controls(numeric,logical_cpus=logical,
                cpu_limit=node.health.get('cpu_limit',logical))
            try:self._node_worker_control(node, values)
            except RemoteError as error:
                if error.status_code==400:raise ValueError(str(error))from None
                raise Conflict(str(error))from None
        snapshots={row['name']:row for row in self.pool.snapshot()['nodes']}
        nodes=[]
        for node in self.pool.nodes:
            source=snapshots[node.name];health=node.health
            fields=('name','display_name','enabled','capacity','base_capacity','cpu_suspended','logical_cpus','cpu_limit','cpu_overcommit_factor',
                'reserved','external_cpus','used_cpus','queued','cpu_percent','memory_capacity_mb','memory_available_mb',
                'memory_reserved_mb','memory_total_mb','host_memory_available_mb','host_commit_limit_mb','host_commit_available_mb',
                'memory_headroom_mb','memory_reserve_mb','memory_budget_mb','rss_mb','private_mb','paused','error')
            row={key:source.get(key)for key in fields}
            configured=dict(health.get('configured')or {})
            defaults=dict(cpus=health.get('base_cpus',row['base_capacity']),
                cpu_limit=health.get('cpu_limit'),cpu_overcommit_factor=health.get('cpu_overcommit_factor',1.),
                cpu_overcommit_utilization=health.get('cpu_overcommit_utilization',90.),
                memory_mb=health.get('memory_budget_mb',0),memory_reserve_mb=health.get('memory_reserve_mb',0),
                memory_headroom_mb=health.get('memory_headroom_mb',0),
                memory_commit_headroom_mb=health.get('memory_commit_headroom_mb',1024))
            row['configured']={key:configured.get(key,defaults[key])for key in sorted(RESOURCE_CONTROL_FIELDS)}
            row['configured']['cpus'] = 0 if node.cpu_suspended else (row['configured']['cpus'] or None)
            row['effective']=dict(cpus=row['capacity'],base_cpus=row['base_capacity'],
                max_cpus=int(row['base_capacity']*defaults['cpu_overcommit_factor']),memory_mb=row['memory_capacity_mb'],
                memory_available_mb=row['memory_available_mb'])
            row['actual']=dict(cpus=health.get('used_cpus',0),active_cpus=sum(j.get('active_cpus')or 0 for j in health.get('active_jobs',())),
                cpu_percent=health.get('cpu_percent'),rss_mb=health.get('rss_mb',0),private_mb=health.get('private_mb'))
            reasons=[]
            if node.error:reasons.append('offline')
            if health.get('paused')or not node.enabled:reasons.append('paused')
            if node.cpu_suspended:reasons.append('cpu_budget')
            if health.get('memory_pressure_state',{}).get('limited'):reasons.append('memory_pressure')
            if not node.cpu_suspended and health.get('queued',0)and health.get('used_cpus',0)>=row['capacity']:reasons.append('cpu_budget')
            if row['memory_available_mb']<=0:reasons.append('memory_budget')
            row.update(wait_reasons=reasons,restart_required=[])
            nodes.append(row)
        return dict(nodes=nodes)

    @staticmethod
    def _publish(job):
        # Replace one complete view; readers never observe an in-place update.
        if job.get('task'):job['result']['max_cpus']=job['task'].get('max_cpus',1024)
        job['read_result'] = deepcopy(job['result'])

    def _save(self, job):
        if job.get('retired'):return
        job.pop('grant_receipt_revision',None)
        terminal = job['result']['status'] in TERMINAL
        if not terminal:
            self._publish(job)
        atomic_json(job["directory"] / "result.json", job["result"])
        # Interrupted may be an internal retry transition. Archive publishes
        # its final outcome after that decision; never expose it mid-retry.
        if terminal and (job['result']['status'] != 'interrupted' or cancellation_needs_reconnect(job['result'])):
            self._publish(job)

    def _persist_grant(self, job, revision, receipt):
        """Write outside the scheduling locks; publish only the latest durable grant."""
        def publish(pending, path):
            with self.condition:
                if job.get('grant_receipt_revision') is revision and not job.get('retired'):
                    pending.replace(path)
        try:
            atomic_json(job['directory']/'result.json',receipt,publish=publish)
        except FileNotFoundError:
            with self.condition:
                if job.get('grant_receipt_revision') is revision and not job.get('retired'):
                    raise

    @staticmethod
    def _restore_accelerator(task, result):
        # The input bundle is immutable; the receipt owns a queued fallback
        # decision across restart, retry and reattachment to an accepted job.
        if 'requested_accelerator' in result:
            task['accelerator'] = result['accelerator']

    def _fallback_accelerator(self, job):
        requirement = job['task'].get('accelerator')
        result = job['result']
        if (requirement is None or 'fallback_after_seconds' not in requirement
                or result['status'] != 'queued' or result.get('attempts')
                or result.get('dispatched') is not None or job.get('node') is not None):
            return
        now = time.time()
        with self.pool.condition:
            matching = [node for node in self.pool._matching(job['task'])
                        if not node.error and node.enabled and not node.health.get('paused')]
            online = bool(matching)
            if requirement['fallback_after_seconds'] == 0 and any(
                    node.can_match_accelerator(node.execution_accelerator(job['task'],available=True)) and node.memory_free >= node.startup_memory(job['task'])
                    for node in matching):
                return
        reason = ('unavailable' if not online else
                  'queue_timeout' if now - result['submitted'] >= requirement['fallback_after_seconds'] else None)
        if reason is not None:
            job['task']['accelerator'] = None
            result.update(accelerator=None, accelerator_fallback=dict(reason=reason, at=now))
            self._save(job)

    def _cache_placement(self,task):
        if task.get('cache_ref')is None:return task
        from .cache_refs import read
        reference=read(self.root,task['cache_ref'])
        if reference['project']!=task.get('project'):
            raise ValueError('Cache reference belongs to another project')
        if task.get('node')not in (None,reference['provider']):
            raise ValueError('Cache reference conflicts with explicit placement')
        return dict(task,node=reference['provider'])

    def _restored_task(self, raw_task, result):
        task = validate_task(raw_task, max(1, raw_task.get('cpus', 1)), allow_auto=True)
        task['priority'] = priority_weight(result.get('dispatch_priority', task['priority']))
        task = build_refs.bind(self.root, task)
        package = result.get('capability_package')
        if package is None:
            return self._cache_placement(task)
        accelerator = result.get('requested_accelerator', result.get('accelerator', task['accelerator']))
        return package_task(task, package, accelerator)

    def cache_reference(self,identity,request=None):
        from .cache_refs import read,register,public,reference_id
        reference_id(identity)
        if request is None:return public(read(self.root,identity))
        if not isinstance(request,dict)or set(request)-{'content_id'}:
            raise ValueError('Cache registration accepts content_id only')
        with self.submission_lock.hold(identity):
            try:previous=read(self.root,identity)
            except ValueError:previous=None
            if previous is not None:
                if request.get('content_id')not in (None,previous.get('content_id')):
                    raise ValueError('A producer cache reference is immutable')
                return public(previous)
            job=load_job(self.jobs,self.condition,identity)
            result=dict(job['result'])
            if result.get('status')!='succeeded':
                raise ValueError('Cache registration requires a successful producer')
            provider=result.get('assigned_node')or getattr(job.get('node'),'name',None)
            record=register(self.root,identity,project=result.get('project'),provider=provider,
                content_id=request.get('content_id'))
            return public(record)

    def publish_reference(self, identity, request, *, operator=False):
        """Retain files produced by the original job, with its observed ABI."""
        if not _ID.fullmatch(identity):
            raise ValueError('Publication requires an original job ID')
        required = {'project', 'kind', 'paths', 'recipe_sha256'}
        if not isinstance(request, dict) or not required <= set(request) or set(request) - required - {'legacy_abi'}:
            raise ValueError('Publication requires project, kind, paths and recipe_sha256')
        if 'legacy_abi' in request and not operator:
            raise ValueError('Legacy ABI attestation is operator-only')
        kind = request['kind']
        spec = build_refs.declaration({key: request[key] for key in ('paths', 'recipe_sha256')}, kind=kind)
        with self.submission_lock.hold(identity):
            result = self.get(identity)
            if not result.get('project') or result['project'] != request['project']:
                raise ValueError('Publication project differs from original receipt')
            if result['status'] != 'succeeded' and not (kind == 'checkpoint' and result['status'] == 'timed_out'):
                raise ValueError('Publication requires a successful producer or a timed-out checkpoint')
            abi = result.get('_artifact_abi')
            if abi is None and 'legacy_abi' in request:
                attested = build_refs.provider_abi(dict(artifact_abi=request['legacy_abi']))
                provider = next((node for node in self.pool.nodes if node.name == result.get('assigned_node')), None)
                if attested is None or provider is None or provider.error or build_refs.provider_abi(provider.health) != attested:
                    raise ValueError('Legacy ABI attestation requires the original provider with matching live ABI')
                abi = attested
            if not abi:
                raise ValueError('Original receipt has no verified artifact ABI')
            raw = self.artifacts(identity)
            stored = self.root / 'jobs' / identity / 'worker-artifacts.zip'
            archive = build_refs.output_archive(stored.read_bytes() if stored.is_file() else raw,
                                                result.get('worker_id', identity) if stored.is_file() else identity,
                                                spec['paths'], checkpoint=kind == 'checkpoint')
            if archive is None:
                raise ValueError('Checkpoint was not committed by the original task')
            return build_refs.public(build_refs.register(self, project=result['project'], producer_id=identity,
                                                         kind=kind, spec=spec, abi=abi, archive=archive))

    def _enqueue(self, identity):
        priority = lambda key: -self.jobs[key]['task']['priority']
        if not self.queue or priority(self.queue[-1]) <= priority(identity):
            self.queue.append(identity)
        else:
            insort_right(self.queue, identity, key=priority)

    def _validate_project_share(self, task):
        """New originals keep the active project's weight; recovery keeps its request."""
        project=task.get('project')or'default'
        share=task.get('resource_share',1.0)
        job = self.jobs.active.get(self.project_leaders.get(project))
        if job is None or job['result']['status'] in TERMINAL:
            job = next((job for job in active_values(self.jobs) if job['result']['status'] not in TERMINAL
                       and (job['task'].get('project') or 'default') == project), None)
            if job is None:
                return
            self.project_leaders[project] = job['result']['id']
        current_share = job['task'].get('resource_share', 1.0)
        if current_share != share:
            raise ValueError('Project '+repr(project)+' has active tasks with resource_share='
                             +str(current_share)+'; new tasks must use the same resource_share')

    def _validate_queue_capacity(self, project):
        queued = len(self.queue)
        if queued >= self.max_queued or (queued >= self.max_queued_per_project and
                sum(self.jobs.active[key]['result']['project'] == project for key in self.queue)
                >= self.max_queued_per_project):
            raise QueueFull('DCO queue is full; retain the original ID and retry after 2 seconds')

    def submit(self, identity, payload, *, operator=False):
        if not _ID.fullmatch(identity):
            raise ValueError("Job id requires 32 lowercase hex characters")
        digest = hashlib.sha256(payload).hexdigest()
        with self.submission_lock.hold(identity):
            with self.condition:
                if self.stopping:
                    raise Conflict("Scheduler is stopping")
                if self.job_observed(identity):
                    raise Conflict('Job ID already belongs to an external observation')
                if identity in self.jobs.deleted:
                    raise Conflict('Job transport has already been retired')
                existing = identity in self.jobs
            if existing:
                job = load_job(self.jobs, self.condition, identity)
                with self.condition:
                    saved = job["result"]
                    if saved["request_sha256"] != digest:
                        raise Conflict("Job id already contains a different request")
                    return dict(saved)
            with zipfile.ZipFile(io.BytesIO(payload)) as archive:
                raw_task = json.loads(archive.read("task.json"))
                if 'cpus'in raw_task:raise ValueError('Declare max_cpus; DCO assigns the actual CPU count')
                reject_startup_cpu_floor(raw_task)
                if not self.accepting and not operator:raise Conflict('DCO maintenance: new task admissions are paused')
                validate_operator_fields(raw_task,operator)
                package = None
                if not operator:
                    package, accelerator = project_package(raw_task.get('requires', []), raw_task.get('accelerator'))
                task = validate_task(raw_task,
                                     max((n.capacity for n in self.pool.nodes), default=1), allow_auto=True)
                if scope := task.get('placement_scope'):
                    if not set(scope['nodes']) <= {node.name for node in self.pool.nodes}:
                        raise ValueError('placement_scope includes an unregistered node')
                    if task.get('node') and task['node'] not in scope['nodes']:
                        raise ValueError('Explicit node falls outside the authorized scope')
                task = build_refs.bind(self.root, task)
                task = self._cache_placement(task) if operator else package_task(task, package, accelerator)
                if not operator and (abi := task.get('_artifact_abi')) is not None:
                    portable = dict(task)
                    portable.pop('_artifact_abi')
                    pool = copy(self.pool)
                    with self.pool.condition:
                        pool.nodes = [copy(node) for node in self.pool.nodes]
                    narrows_pool = any(build_refs.provider_abi(node.health) != abi for node in pool._matching(portable))
                    if narrows_pool:
                        raise ValueError('A native artifact or checkpoint must not narrow the capability pool. '
                                         'Submit the frozen sources and resolve or build the native artifact on the assigned worker.')
            with self.condition:
                self._validate_project_share(task)
                self._validate_queue_capacity(task['project'])
            with self.asset_cache.lock:
                for asset in set(task['assets'].values()):
                    if not (self.root/'assets'/(asset+'.zip')).is_file():
                        raise ValueError('Synchronize missing asset first: '+asset)
                    self.asset_cache.touch(asset)
                self.accepting_assets.update(task['assets'].values())
            # Durable request writes must not serialize unrelated cache hits.
            # This reference belongs to acceptance, independently of the caller
            # releasing its preparation handoff before publication finishes.
            try:
                directory = self.root / "jobs" / identity
                directory.mkdir()
                (directory / "request.zip").write_bytes(payload)
                job = Job({"directory": directory, "task": task, "auto_cpus": task['cpus'] == 0,
                           "elastic_cpus": elastic_cpus(task), "cancel": threading.Event(), "node": None,
                       "result": {"id": identity, "status": "queued", "label": task["label"], "project": task['project'], "cpus": task["cpus"], 'max_cpus':task['max_cpus'],
                                  "priority": task["priority"],
                                  "accelerator": task.get('accelerator'),
                                  "max_retries": task.get('retries',2), "attempts": [],
                                  "resource_recovery_limit":len(RESOURCE_RECOVERY_DELAYS)if reentrant_computation(task)else 0,
                                  "memory_mb": task["memory_mb"], "request_sha256": digest,
                                  "submitted": time.time(), "started": None, "finished": None,
                                  "stdout": "", "stderr": "", "steps": [],
                                  "total_steps": len(task["steps"]), "assets": task["assets"]}})
                _restore_phase_demand(job)
                if 'priority' in raw_task and raw_task['priority'] != task['priority']:
                    job['result']['submitted_priority'] = raw_task['priority']
                if 'min_start_cpus' in task:
                    job['result']['min_start_cpus'] = task['min_start_cpus']
                if package is not None:
                    job['result']['capability_package'] = package
                if scope := task.get('placement_scope'):
                    job['result']['placement_scope'] = scope
                if task['accelerator'] is not None and 'fallback_after_seconds' in task['accelerator']:
                    job['result']['requested_accelerator'] = task['accelerator']
                if task.get('cache_ref'):job['result']['cache_ref']=task['cache_ref']
                if 'checkpoint_output' in task or 'timeout_grace_s' in task:
                    job['result']['partial_checkpoint_ref'] = None
                if task['assets'] and self.asset_variants:
                    variants = freeze_asset_variants(task, self.asset_variants)
                    if variants:
                        job['result']['_asset_variants'] = variants
                admission = {
                    key: job['result'][key] for key in ('id', 'status', 'request_sha256', 'assets', 'label',
                        'project', 'cpus', 'memory_mb', 'submitted', 'total_steps')}
                admission['initial_cpus'] = task['cpus']
                if package is not None:
                    admission['capability_package'] = package
                atomic_json(directory / 'admission.json', admission)
                self._save(job)
                with self.condition:
                    stopping = self.stopping
                    held_admission=not self.accepting and not operator
                    rejected=None
                    if not stopping and not held_admission:
                        # Independent request IDs can prepare concurrently.
                        # Recheck with insertion under the same lock so only
                        # one weight establishes a previously idle project.
                        try:
                            self._validate_project_share(task)
                            self._validate_queue_capacity(task['project'])
                        except ValueError as error:rejected=error
                        else:
                            self.jobs[identity] = job
                            self.asset_handoffs.pop(identity, None)
                            self.project_leaders[task['project']] = identity
                            self._enqueue(identity)
                            self.condition.notify_all()
                if stopping or held_admission or rejected is not None:
                    shutil.rmtree(directory)
                    if rejected is not None:raise rejected
                    raise Conflict('Scheduler is stopping'if stopping else'DCO maintenance: new task admissions are paused')
                return dict(job["result"])
            finally:
                # The published job now owns its assets. Releasing the
                # acceptance reference must not wait behind cache I/O.
                self.accepting_assets.clear()

    def _schedule(self):
        while True:
            with self.condition:
                if self.stopping:
                    return
                self._recover_running()
                self._poll_observations()
                if not self.paused and self.queue:
                    for identity in self.queue:
                        job = self.jobs[identity]
                        if job['task'].get('accelerator'):
                            self._fallback_accelerator(job)
                    with self.pool.condition:
                        snapshot = PlacementSnapshot(self)
                else:
                    snapshot = None
                    self._wait_for_placement()
            if snapshot is not None:
                for identity, planned, headroom in snapshot.candidates():
                    with self.condition:
                        if self.stopping or self.paused:
                            break
                        job = self.jobs.get(identity)
                        if (job is None or job['result']['status'] != 'queued' or job['cancel'].is_set()
                                or job['task'] != planned['task']
                                or job['result'].get('resource_retry_after', 0) > time.time()):
                            continue
                        node = self._dispatch_queued(identity, job, headroom)
                        if node is not None:
                            with self.pool.condition:
                                snapshot.record_dispatch(identity, job, node)
                with self.condition:
                    if not self.stopping and self.queue:
                        self._wait_for_placement()

    def _wait_for_placement(self):
        delayed = self.queue and all(self.jobs[key]['result'].get('resource_retry_after',0)>time.time()
                                     for key in self.queue)
        wait = .5 if self.paused or delayed else .05 if self.queue else .5 if self.__dict__.get('_pending_recovery') else None
        if wait is None and self.observations.active:
            wait = self.observation_poll_seconds
        self.condition.wait(wait)

    def _dispatch_queued(self, identity, job, headroom):
        """Commit one still-valid placement and its durable assignment."""
        try:
            node = self.pool._acquire(job['task'], block=False,
                cpu_limit=startup_cpus(job['task']) if job.get('auto_cpus') else None,
                partial_cpus=True,job_id=job.get('worker_id',identity),
                node_headroom=headroom,memory_floor=job['result'].get('resource_recovery_memory_mb',0),
                resource_recovery=bool(job['result'].get('resource_recovery_count')))
        except (RuntimeError, ValueError) as error:
            message = ('Cache-reference provider is unavailable or incompatible'
                       if job['task'].get('cache_ref') else str(error))
            job['result'].update(status='failed', error=message, finished=time.time())
            self._save(job)
            self.queue.remove(identity)
            archive_job(self.jobs, job)
            return None
        if node is None:
            return None
        self.queue.remove(identity)
        job['node'] = node
        with self.pool.condition:
            node.pending_dispatches.add(job.get('worker_id', identity))
        job['cpu_overcommit'] = node.reserved + node.external > node.base_capacity
        job['result'].update(status='dispatching', assigned_node=node.name, worker_url=node.client.url,
                             cpus=job['task']['cpus'], dispatch_stage='bundle', dispatch_stage_started_at=time.time())
        abi = build_refs.provider_abi(node.health)
        if abi is not None:
            job['result']['_artifact_abi'] = list(abi)
        job['result'].pop('resource_retry_after', None)
        job['result'].pop('resource_wait_reason', None)
        if job['result'].get('accelerator') != job['task'].get('accelerator'):
            job['result'].setdefault('requested_accelerator', job['result'].get('accelerator'))
            job['result']['accelerator'] = job['task'].get('accelerator')
        self._save(job)
        thread = threading.Thread(target=self._execute, args=(job,), name='central-job', daemon=True)
        self.threads.add(thread)
        thread.start()
        return node

    def _recover_running(self):
        """Reattach accepted work from fresh worker snapshots; never resubmit it."""
        with self.condition,self.pool.condition:
            pending=self.__dict__.get('_pending_recovery')
            if pending is None:
                pending=self._pending_recovery={identity for identity,job in active_items(self.jobs) if job.get('recover')}
            for identity in list(pending):
                job=self.jobs.get(identity)
                if job is None:
                    pending.discard(identity);continue
                pending_cancel = cancellation_needs_reconnect(job['result'])
                if job['result']['status'] in TERMINAL and not pending_cancel:
                    pending.discard(identity);continue
                node = self._original_node(job['result'].get('assigned_node'), job['result'].get('worker_url'))
                worker_id=job['result'].get('worker_id',identity)
                if node is None or node.error or node.detached or worker_id in node.live_jobs:continue
                if job.get('handoff_cancel_retry_at', 0) > time.monotonic():
                    continue
                remote=next((r for r in node.health.get('active_jobs') or [] if r['id']==worker_id),None)
                with zipfile.ZipFile(job['directory']/'request.zip') as archive:
                    task=json.loads(archive.read('task.json'))
                task=self._restored_task(task, job['result'])
                self._restore_accelerator(task, job['result'])
                automatic=task['cpus']==0;elastic=elastic_cpus(task)
                grant=remote or job['result']
                task.update(cpus=grant['cpus'],memory_mb=grant['memory_mb'])
                job.update(task=task,auto_cpus=automatic,elastic_cpus=elastic,node=node,reattach=True,worker_id=worker_id)
                _restore_phase_demand(job)
                was_known=worker_id in node.known_jobs
                if remote and not was_known:
                    node.external=max(0,node.external-(task['cpus'] if remote['status']=='running' else 0))
                    node.external_memory_mb=max(0,node.external_memory_mb-max(task['memory_mb'],remote.get('rss_mb',0)))
                # Treat the reattached worker job as owned before rebuilding
                # external accelerator reservations.  This prevents a GPU
                # already running after a center restart from being counted
                # twice while the original job is reconnected.
                node.known_jobs.add(worker_id)
                node.refresh_external_accelerators(node.health.get('active_jobs') or [])
                if node.reserve_accelerator(task.get('accelerator'), worker_id) is None:
                    # The worker snapshot is authoritative; retain a local
                    # external reservation until the next health sample if a
                    # provider has not exposed the same profile yet.
                    node.external_accelerator_reservations.extend(node.accelerator_units())
                node.reserved+=task['cpus'];node.reserved_memory_mb+=task['memory_mb']
                node.live_jobs.add(worker_id)
                if not pending_cancel:
                    job['result'].pop('error',None)
                    if remote:self._update(job,remote)
                job['result'].setdefault('dispatched',job['result']['submitted'])
                self._save(job)
                job.pop('recover',None)
                pending.discard(identity)
                thread=threading.Thread(target=self._execute,args=(job,),name='central-reconnect',daemon=True)
                self.threads.add(thread);thread.start()
                self.activity.notify('Reconnected original task ['+identity[:4]+']')

    def _execute(self, job):
        node, identity, task = job["node"], job.get('worker_id',job['result']['id']), job["task"]
        result = None;retry=False
        preparation_worker_lost = False
        deferred_cancel = False
        released = False
        try:
            reattach=job.pop('reattach',False)
            if reattach and job['cancel'].is_set() and (job.get('recover_submission')
                    or job['result'].get('handoff_cancel_pending')):
                result = self._settle_cancelled_handoff(job, node, identity)
                if result is not None:
                    with self.condition:
                        self._update(job, result)
                    return
                job.pop('recover_submission', None)
            if reattach and job.pop('recover_submission',False):
                # A center can stop between reserving a provider and finishing
                # its POST. Observe the same ID before replaying that pending
                # submission; an already accepted/finished body is never rerun.
                try:node.client._read_reconnecting(lambda:node.client.status(identity,brief=True))
                except RemoteError as error:
                    if getattr(error,'status_code',None)!=404:raise
                    reattach=False
                    job['result']['submission_recovered']=True
            if not reattach:
                payload = (job['directory'] / 'request.zip').read_bytes()
                missing=missing_task_assets(task)
                if missing:
                    # This accepted ID has not been submitted to any worker.
                    # The zero covers this attempt's task process tree only;
                    # _update retains earlier attempts and their paid/unknown cost.
                    result=dict(status='failed',error_kind='pre_execution',
                        error='Undeclared task asset references: '+', '.join(missing),
                        rejected_before_submit=True,task_process_started=False,
                        cpu_seconds=0,cpu_accounting_scope='task_process_tree',
                        started=None,finished=time.time(),steps=[],stdout='',stderr='')
                    with self.condition:self._update(job,result)
                    return
                variants = job['result'].get('_asset_variants', ())
                dispatch_task = bind_asset_variants(task, variants, node.health) if variants else task
                if (dispatch_task is not task or job['auto_cpus'] or 'max_cpus' in task
                        or 'retries' in task or task.get('node') is not None
                        or 'requested_accelerator' in job['result'] or 'placement_scope' in task):
                    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
                        files = {name: archive.read(name) for name in archive.namelist()}
                    worker_task = dict(dispatch_task)
                    worker_task['env'] = provider_environment(dispatch_task, node.health)
                    # Placement hints belong to the dispatcher, not the executable.
                    worker_task.pop('node',None)
                    worker_task.pop('placement_scope', None)
                    worker_task.pop('cache_ref',None)
                    worker_task.pop('native_cache',None)
                    worker_task.pop('_capability_package', None)
                    if STARTUP_CPU_FLOOR not in node.health.get('capabilities', ()):
                        worker_task.pop('min_start_cpus', None)
                    worker_task.pop('retries',None)
                    for field in (*(build_refs.REFERENCE_FIELDS - {'checkpoint_output'}),
                                  '_artifact_abi', '_artifact_mounts', '_affinity_assets'):
                        worker_task.pop(field, None)
                    if worker_task.get('accelerator') is not None:
                        worker_task['accelerator'] = {
                            key: value for key, value in worker_task['accelerator'].items()
                            if key != 'fallback_after_seconds'}
                    if 'max_cpus' not in node.health.get('capabilities',[]):
                        worker_task.pop('max_cpus',None)
                    files['task.json'] = json_bytes(worker_task)
                    recovery=job['directory']/'resource-resume.json'
                    if recovery.is_file():
                        previous=job['directory']/'attempts'/('%04d.zip'%len(job['result']['attempts']))
                        if previous.is_file():carry_outputs(files,previous.read_bytes())
                        snapshot=json.loads(recovery.read_bytes())
                        if snapshot is not None:files['input/.dco-resource-resume.json']=json_bytes(snapshot)
                    payload = pack(files)
                callback=self._callback(node) if 'resize' in node.health.get('capabilities',[]) else None
                if callback:callback={**callback,'job_id':job['result']['id']}
                job['result']['dispatch_zip_bytes'] = len(payload)
                job['result']['worker_request_sha256'] = hashlib.sha256(payload).hexdigest()
                dispatch_options = {'assets': dispatch_task['assets']} if dispatch_task is not task else {}
                try:
                    submitted = self._submit(job, identity, payload, callback, **dispatch_options)
                except RequestCancelled:
                    result = self._settle_cancelled_handoff(job, node, identity)
                    if result is not None:
                        with self.condition:
                            self._update(job, result)
                        return
                    submitted = True
                if not submitted:
                    result=dict(status='cancelled',finished=time.time(),cancelled_before_submit=True,cpu_seconds=0)
                    with self.condition:self._update(job,result)
                    return
                with self.pool.condition:
                    node.pending_dispatches.discard(identity)
                with self.condition:
                    ack = time.time()
                    job['result']['handoff_acknowledged_at'] = ack
                    if 'handoff_clock' in job:
                        job['result']['handoff_seconds'] = time.monotonic() - job.pop('handoff_clock')
                    job['result'].update(dispatch_stage='worker_accepted', dispatch_stage_started_at=ack)
                    self._publish(job)
            # Worker acceptance owns the executable copy. Recovery rereads
            # the frozen request on disk; polling needs no transport buffers.
            payload = files = None
            cancel_sent=False
            last_worker_status = None
            while True:
                try:
                    if node.detached:
                        if job['cancel'].is_set():
                            deferred_cancel = True
                            break
                        # The pool owns bounded reconnection probes. Keep this
                        # accepted ID without a separate offline polling loop.
                        time.sleep(.5)
                        continue
                    if job["cancel"].is_set() and not cancel_sent:
                        with self.condition:
                            cancellation_in_progress = job.get('cancel_sending')
                            cancel_sent = job['result'].get('cancel_acknowledged', False)
                        if not cancel_sent and cancellation_in_progress:
                            time.sleep(.1)
                            continue
                        if not cancel_sent:
                            node.client.cancel(identity)
                            cancel_sent = True
                    if not self.stopping and self.pool.needs_relay_status_sample(node):
                        # Status reads await the common sample. The stop signal
                        # uses its own control connection and takes priority.
                        time.sleep(.25)
                        continue
                    remote = node.client.status(identity, brief=True)
                    if remote.get('resource_request') and not job['cancel'].is_set():
                        self._answer_resource_request(job, identity, remote)
                    if remote['status'] in TERMINAL:
                        remote = node.client.status(identity)
                except RemoteError as error:
                    if getattr(error,'status_code',None)==404:
                        if cancellation_needs_reconnect(job['result']):
                            with self.condition:
                                job['result'].update(error='Cancellation unresolved: original worker receipt is missing',
                                                     finished=time.time())
                                self._save(job)
                            break
                        retry=True
                        result=dict(status='interrupted',error='Worker does not have the original receipt: '+str(error),
                                    finished=time.time())
                        break
                    if self.stopping or error.status_code not in (None, *TRANSIENT_WORKER_HTTP):
                        raise
                    with self.condition:
                        job['result']['connection_error']=str(error)
                        self._publish(job)
                    time.sleep(.5)
                    continue
                spec, checkpoint = task.get('produces_artifact_ref'), task.get('checkpoint_output')
                completed = remote['status'] == 'succeeded'
                publish = (spec and completed) or (checkpoint and (completed or remote['status'] == 'timed_out'))
                if publish:
                    self.pool._release(node, task, remote, job_id=identity)
                    released = True
                    try:
                        raw = self._worker_artifact_archive(node.client, identity, job['directory'] / 'worker-artifacts.zip')
                        if spec and completed:
                            remote['artifact_ref'] = self._retain_output(job['result'], raw, identity, 'artifact', spec)
                        if checkpoint:
                            remote['partial_checkpoint_ref'] = self._retain_output(job['result'], raw, identity,
                                                                                  'checkpoint', checkpoint)
                    except (OSError, ValueError, KeyError, RemoteError, zipfile.BadZipFile) as error:
                        remote['artifact_publication_error'] = str(error)
                pending_cancel = cancellation_needs_reconnect(job['result'])
                if remote['status'] in TERMINAL or (not pending_cancel and
                        (remote != last_worker_status or 'connection_error' in job['result'])):
                    last_worker_status = remote.copy()
                    with self.condition:
                        job['result'].pop('connection_error',None)
                        # Retry is one atomic state transition. A client must not
                        # collect a transient interrupted outcome mid-retry.
                        if not (remote['status']=='interrupted' or _infrastructure_interruption(job,remote)) or job['cancel'].is_set():
                            self._reconcile_startup_memory(job, identity, remote)
                            self._update(job, remote)
                if remote["status"] in TERMINAL:
                    result = remote
                    retry=remote['status']=='interrupted' or _infrastructure_interruption(job,remote)
                    break
                interval=0.15 if time.time()-job['result']['dispatched']<2 else 0.2
                if cancel_sent:
                    time.sleep(interval)
                else:
                    job['cancel'].wait(interval)
        except _PreparationWorkerLost:
            preparation_worker_lost = True
        except Exception as error:
            deferred_cancel = (job['cancel'].is_set() and job['result'].get('dispatched') is not None
                               and isinstance(error, RemoteError)
                               and error.status_code in (None, *TRANSIENT_WORKER_HTTP))
            if not deferred_cancel:
                with self.condition:
                    if not retry:
                        job["result"].update(status="interrupted", error=str(error), finished=time.time())
                        self._save(job)
        finally:
            with self.pool.condition:
                node.pending_dispatches.discard(identity)
            if not released:
                self.pool._release(node, task, result,job_id=identity)
            if retry and result and resource_recovery(job['task'],result) and not job['cancel'].is_set():
                try:self._prepare_resource_recovery(job,result)
                except Exception as error:
                    job['resource_recovery_error']=str(error)
                    job['resource_recovery_business_failure']=isinstance(error,BusinessItemFailure)
            with self.condition:
                self.threads.discard(threading.current_thread())
                job.pop('handoff_stop', None)
                if deferred_cancel:
                    if job['result'].get('handoff_cancel_pending'):
                        job['handoff_cancel_retry_at'] = time.monotonic() + 2
                    self._defer_cancellation(job)
                elif preparation_worker_lost:
                    if job['cancel'].is_set():
                        self._update(job, dict(status='cancelled', finished=time.time(),
                                               cancelled_before_submit=True, cpu_seconds=0))
                    elif not self.stopping:
                        self._requeue_unsubmitted(job)
                elif retry and not self.stopping and not job['cancel'].is_set():self._retry(job,result)
                elif retry and job['cancel'].is_set():
                    self._update(job,{**result,'status':'cancelled','finished':time.time()})
                if not deferred_cancel:
                    archive_job(self.jobs, job)
                self.condition.notify_all()

    def _answer_resource_request(self, job, worker_id, remote):
        request = remote['resource_request']
        serial = request['request_id']
        reply = job.get('lease_reply')
        if serial <= job.get('lease_answered', 0) or reply and serial < reply['request_id']:
            return
        if not reply or serial != reply['request_id']:
            options = {key: value for key, value in request.items() if key != 'request_id'}
            operation = options.pop('_operation', 'resize')
            if operation == 'resize':
                with self.condition:
                    job['result']['resource_request'] = request.copy()
            reply = {'request_id': serial}
            try:
                reply['grant'] = (self.admission(options) if operation == 'admission'
                                  else self.resize(job['result']['id'], **options))
            except (Conflict, RemoteError, ValueError) as error:
                message = str(error)
                if isinstance(error, RemoteError) and (error.transport_retryable or error.status_code in (502, 503, 504)):
                    message = RESOURCE_TRANSPORT_RETAINED + ': ' + message
                reply['error'] = message
            # The worker may already have received a reply whose ACK was lost.
            # Replaying this answer must never apply the resource change twice.
            job['lease_reply'] = reply
        grant = reply.get('grant', {})
        if 'memory_phase' in grant:
            remote['memory_phase'] = grant['memory_phase']
        if 'active_cpus' in grant:
            remote['active_cpus'] = grant['active_cpus']
        job['node'].client.request('/jobs/' + worker_id + '/lease', reply)
        job['lease_answered'] = serial

    def _prepare_resource_recovery(self,job,outcome):
        """Fetch paid checkpoints outside the scheduling lock before requeue."""
        count=sum(a.get('error_kind')=='memory_pressure'for a in job['result'].get('attempts',[]))
        if count>=len(RESOURCE_RECOVERY_DELAYS):return
        worker=job.get('worker_id',job['result']['id'])
        cached=job['directory']/'worker-artifacts.zip'
        raw=cached.read_bytes()if cached.is_file()else job['node'].client.artifacts(worker)
        with zipfile.ZipFile(io.BytesIO(raw))as archive:
            if json.loads(archive.read('result.json'))['id']!=worker:
                raise ValueError('Resource recovery artifact identity mismatch')
        if not cached.is_file():
            pending=cached.with_suffix('.partial')
            try:pending.write_bytes(raw);pending.replace(cached)
            finally:unlink(pending)
        path=job['directory']/'resource-resume.json'
        previous=json.loads(path.read_bytes())if path.is_file()else None
        snapshot=resume_map(job['task'],job['result']['id'],job['result']['request_sha256'],raw,previous)
        atomic_json(path,snapshot)

    def _retain_output(self, result, raw, identity, kind, spec):
        archive = build_refs.output_archive(raw, identity, spec['paths'], checkpoint=kind == 'checkpoint')
        if archive is not None:
            return build_refs.register(self, project=result['project'], producer_id=result['id'],
                kind=kind, spec=spec, abi=result['_artifact_abi'], archive=archive)['id']

    def _pace_submission(self, job):
        # Planning can reserve fair shares in one pass. Pace only the actual
        # POST after asset preparation, without serializing network transfers.
        node = job['node']
        interval = 1. if job.get('cpu_overcommit') else .1 if job.get('task', {}).get('short_job') else .5
        while not self.stopping and not job['cancel'].is_set():
            with node.dispatch_lock:
                now = time.monotonic()
                delay = node.next_dispatch + max(0, interval - node.dispatch_interval) - now
                if delay <= 0:
                    dispatched = time.time()
                    node.next_dispatch = time.monotonic() + interval
                    node.dispatch_interval = interval
            if delay > 0:
                job['cancel'].wait(delay)
                continue
            with self.condition:
                if self.stopping or job['cancel'].is_set():
                    return False
                job['result']['dispatched'] = dispatched
                job['result'].update(dispatch_stage='awaiting_worker', dispatch_stage_started_at=dispatched)
                job['handoff_clock'] = time.monotonic()
                self._save(job)
            return True
        return False

    def _submit(self, job, identity, payload, callback, *, assets=None):
        node, force, uncertain = job['node'], False, False
        assets = job['task']['assets'] if assets is None else assets

        def lost_before_submit():
            return (not uncertain and not job['result'].get('submission_recovered')
                    and not self.stopping and (node.error or node.detached))

        stop_post = transport.HandoffCancellation(job['cancel'])
        with self.condition:
            job['handoff_stop'] = stop_post
        asset_metrics = None
        if assets:
            asset_metrics = job['result'].setdefault('asset_transport', {'uploads': {}})
            asset_metrics['selected'] = sorted(assets)
        while True:
            if job['cancel'].is_set():
                if uncertain:
                    raise RequestCancelled('Worker POST acceptance remains uncertain')
                return False
            submitting=False
            try:
                if assets and not uncertain:
                    with self.condition:
                        job['result'].update(dispatch_stage='assets', dispatch_stage_started_at=time.time())
                        self._publish(job)
                started = time.monotonic()
                with ExitStack() as pins:
                    try:
                        pins.enter_context(self._asset_handoff(node,identity,assets.values(),enabled=bool(assets)))
                        self._ensure_assets(node, assets.values(), force=force,
                                            cancel=None if uncertain else job['cancel'].is_set,
                                            report=asset_metrics['uploads'] if asset_metrics is not None else None)
                    finally:
                        job['result']['asset_preparation_seconds'] = (
                            job['result'].get('asset_preparation_seconds', 0.0) + time.monotonic() - started)
                    if mounts := job['task'].get('_artifact_mounts'):
                        hits = asset_metrics['uploads']
                        job['result']['cache_hit'] = dict(
                            artifact=hits.get(mounts.get('dco_artifact'), {}).get('cache_hit', False),
                            checkpoint=hits.get(mounts.get('dco_checkpoint'), {}).get('cache_hit', False),
                            worker_id=node.name)
                    if not uncertain and job['cancel'].is_set():return False
                    if not uncertain:
                        with self.condition:
                            job['result'].update(dispatch_stage='pacing', dispatch_stage_started_at=time.time())
                            self._publish(job)
                        if not self._pace_submission(job):return False
                        if self.stopping or job['cancel'].is_set():return False
                    submitting=True
                    timing = dict(job['result'].get('handoff_transport', {}))
                    try:
                        with transport.measure_job_post(identity, timing):
                            node.client.submit_bundle(identity,payload,cancel=stop_post,
                                **({'callback':callback} if callback else {}))
                    finally:
                        if timing:
                            job['result']['handoff_transport'] = timing
                return True
            except RequestCancelled as error:
                if uncertain or getattr(error, 'request_may_have_been_sent', False):
                    raise
                return False
            except PoolBusy as error:
                # Nothing was sent. Keep the same reservation and ID while the
                # local transport queue drains, rather than ending the task.
                if self.stopping:raise
                if not submitting and lost_before_submit():
                    raise _PreparationWorkerLost() from error
                with self.condition:
                    job['result']['connection_error']=str(error)
                    self._publish(job)
                if uncertain:
                    time.sleep(.1)
                elif job['cancel'].wait(.1):
                    return False
            except RemoteError as error:
                if submitting and job['cancel'].is_set():
                    raise RequestCancelled('Worker POST cancelled before acknowledgement') from error
                if not force and getattr(error,'status_code',None)==400 and 'Synchronize missing asset first:' in str(error):
                    force=True
                    continue
                if (not submitting and lost_before_submit()
                        and (error.transport_retryable or error.status_code in TRANSIENT_WORKER_HTTP)):
                    raise _PreparationWorkerLost() from error
                if self.stopping or error.status_code not in (None, *TRANSIENT_WORKER_HTTP):
                    raise
                uncertain=uncertain or submitting
                if not uncertain:
                    # Asset failures precede any job POST. Cancellation can
                    # release this grant without querying/replaying a job.
                    if job['cancel'].wait(.1):return False
                    continue
                # A missing POST reply says nothing about acceptance. Keep the
                # original worker, ID and grant while querying that same job.
                while True:
                    if job['cancel'].is_set():
                        raise RequestCancelled('Worker POST acceptance remains uncertain') from error
                    with self.condition:
                        job['result']['connection_error']=str(error)
                        self._publish(job)
                    try:
                        node.client.status(identity,brief=True)
                        return True
                    except RemoteError as observed:
                        if getattr(observed,'status_code',None)==404:
                            # The first POST may still arrive later. Repeating
                            # its exact ID and payload remains worker-idempotent.
                            break
                        if self.stopping or observed.status_code not in (None, *TRANSIENT_WORKER_HTTP):
                            raise
                        error=observed
                        time.sleep(.5)

    def _requeue_unsubmitted(self, job):
        result = job['result']
        with zipfile.ZipFile(job['directory'] / 'request.zip') as archive:
            task = self._restored_task(json.loads(archive.read('task.json')), result)
        self._restore_accelerator(task, result)
        job.update(task=task, node=None, auto_cpus=task['cpus'] == 0, elastic_cpus=elastic_cpus(task))
        _restore_phase_demand(job)
        job.pop('handoff_clock', None)
        for field in ('assigned_node', 'worker_url', 'dispatched', 'dispatch_stage', 'dispatch_stage_started_at',
                      'handoff_seconds', 'handoff_acknowledged_at', 'handoff_transport', 'connection_error',
                      'asset_transport', 'cache_hit', '_artifact_abi'):
            result.pop(field, None)
        result.update(status='queued', priority=task['priority'], cpus=task['cpus'], memory_mb=task['memory_mb'],
                      resource_wait_reason='transport: waiting for a reachable worker')
        identity = result['id']
        self._enqueue(identity)
        self._save(job)

    def _retry(self, job, outcome):
        if job.get('retired'):return
        # The interrupted receipt is still a paid attempt, including when the
        # retry allowance is exhausted. Keep its measurements before resetting.
        measured={key:deepcopy(outcome.get(key,job['result'].get(key))) for key in (
            'started','elapsed','cpus','memory_mb','peak_cpus','peak_rss_mb','cpu_seconds',
            'cumulative_cpu_seconds','cpu_accounting_sampled_at','cpu_work_ghz_seconds',
            'cpu_frequency','cost_incomplete','peak_private_mb','error_kind','memory_pressure',
            'cpu_breakdown_s','compile_cpu_s','load_check_cpu_s','projection_cpu_s','sim_cpu_s',
            'handoff_seconds','handoff_acknowledged_at','handoff_transport','asset_preparation_seconds',
            'worker_admission_seconds','preparation_seconds','execution_timing')
            if key in outcome or key in job['result']}
        if 'cpu_seconds' not in outcome:
            measured['cost_incomplete']=True  # A live snapshot is only the known prefix.
        job['result'].update(measured)
        job['result'].update(status='interrupted',error=outcome.get('error','Worker stopped'),finished=outcome.get('finished',time.time()))
        self._attempt_total(job['result'])
        self._save(job)
        attempts=job['result'].setdefault('attempts',[])
        allowance=job['result'].get('max_retries',0)
        if outcome.get('error_kind')=='memory_pressure' and job['task'].get('resource_recovery',True) is False:
            self._save(job)
            return
        recovering=resource_recovery(job['task'],outcome)
        if recovering:
            used=sum(a.get('error_kind')=='memory_pressure'for a in attempts)
            allowance=len(RESOURCE_RECOVERY_DELAYS)
            job['result']['resource_recovery_limit']=allowance
            if job.get('resource_recovery_error'):
                job['result']['resource_recovery_error']=job.pop('resource_recovery_error')
                if job.pop('resource_recovery_business_failure',False):
                    job['result'].update(status='failed',error_kind='business_failure',error=job['result']['resource_recovery_error'])
                self._save(job);return
        else:used=sum(not resource_recovery(job['task'],a)for a in attempts)
        if _infrastructure_interruption(job,outcome):
            job['result']['infrastructure_retry_limit']=allowance
        if used>=allowance:
            self._save(job)
            return
        attempts.append(dict(node=job['node'].name,worker_id=job.get('worker_id',job['result']['id']),
                             worker_url=job['node'].client.url,
                             artifact=('.dco-attempts/%04d.zip'%(len(attempts)+1)) if outcome.get('id') else None,
                             status=outcome['status'],error=outcome.get('error',''),finished=outcome.get('finished'),
                             **measured))
        attempts[-1].update({key:deepcopy(outcome.get(key,job['result'].get(key)))
                            for key in ATTEMPT_OBSERVATIONS if key in outcome or key in job['result']})
        if recovering:
            peak=max(max(attempt.get('peak_rss_mb',0),attempt.get('peak_private_mb',0)) for attempt in attempts)
            reservation_floor=max(max(attempt.get('memory_reservation_mb')or 0,
                                      attempt.get('memory_commit_reservation_mb')or 0) for attempt in attempts)
            job['result'].update(resource_recovery_count=used+1,
                resource_retry_after=time.time()+RESOURCE_RECOVERY_DELAYS[used],
                resource_wait_reason='memory recovery: cooldown and measured working-set headroom',
                resource_recovery_memory_mb=max(job['result'].get('resource_recovery_memory_mb',0),
                    startup_memory({**job['task'],'cpus':1}),reservation_floor,peak*1.25,peak+32),
                resource_recovery_policy='same identity; completed map items retained; unknown running prefixes preserved')
        elif outcome.get('error_kind') == 'scratch_unavailable':
            job['result'].update(resource_retry_after=time.time()+1,
                                 resource_wait_reason='waiting for a fresh executable RAM scratch sample')
        cached=job['directory']/'worker-artifacts.zip'
        if cached.is_file():
            saved=job['directory']/'attempts'/('%04d.zip'%len(attempts));saved.parent.mkdir(exist_ok=True)
            cached.replace(saved)
        identity=job['result']['id']
        with zipfile.ZipFile(job['directory']/'request.zip') as archive:
            task=json.loads(archive.read('task.json'))
        task=self._restored_task(task, job['result'])
        self._restore_accelerator(task, job['result'])
        worker_id=hashlib.sha256((identity+':'+str(len(attempts))).encode()).hexdigest()[:32]
        job.update(task=task,auto_cpus=task['cpus']==0,elastic_cpus=elastic_cpus(task),node=None,worker_id=worker_id)
        job.pop('desired_cpus',None);job.pop('phase_memory',None);job.pop('lease_answered',None);job.pop('lease_reply',None)
        job['result'].update(status='queued',worker_id=worker_id,cpus=task['cpus'],memory_mb=task['memory_mb'],finished=None)
        for key in ('error','connection_error','assigned_node','worker_url','dispatched','elapsed','total_elapsed','peak_cpus','peak_rss_mb','cpu_seconds','total_cpu_seconds','cost_incomplete',
                    'cumulative_cpu_seconds','cpu_accounting_sampled_at','cpu_work_ghz_seconds','cpu_frequency','error_kind','memory_pressure','peak_private_mb',
            'cpu_breakdown_s','compile_cpu_s','load_check_cpu_s','projection_cpu_s','sim_cpu_s',
            'asset_preparation_seconds','asset_transport','cache_hit','_artifact_abi'):
            job['result'].pop(key,None)
        job['result'].update(started=None,stdout='',stderr='',steps=[])
        for key in ATTEMPT_OBSERVATIONS:job['result'].pop(key,None)
        _restore_phase_demand(job)
        self._enqueue(identity)
        self._save(job)
        self.activity.notify('Interrupted; retry original task ['+identity[:4]+']')

    @staticmethod
    def _attempt_total(result):
        if result.get('attempts'):
            parts=result['attempts']+[result]
            values=[part.get('cpu_seconds') for part in parts]
            result['total_cpu_seconds']=(sum(values) if all(not part.get('cost_incomplete')
                and type(value) in (int,float) and math.isfinite(value) and value>=0
                for part,value in zip(parts,values)) else None)

    @contextmanager
    def _asset_handoff(self, node, identity, assets, *, enabled):
        if not enabled or 'asset_handoff' not in node.health.get('capabilities',[]):
            yield
            return
        route='/jobs/'+identity+'/asset-handoff'
        pending=self.jobs.get(identity)if hasattr(self,'jobs')else None
        options={'cancel':pending['cancel'].is_set}if pending is not None else {}
        node.client.request(route,{'assets':sorted(set(assets))},**options)
        accepted=False
        try:
            yield
            accepted=True
        finally:
            # The sole caller completes this body only after submit succeeds.
            # Worker acceptance already removes the handoff under the same
            # lock that publishes its durable task reference. Start polling
            # leases immediately instead of queuing redundant bulk cleanup.
            pending=self.jobs.get(identity)if hasattr(self,'jobs')else None
            cancelled=pending is not None and pending['cancel'].is_set()
            if not accepted and not cancelled:
                try:
                    node.client.request(route,method='DELETE')
                except RemoteError:
                    pass  # Worker expiry also releases an unreachable sender's hold.

    def _resumable_asset_receivers(self, node, shared_cache):
        if not shared_cache:
            return []
        receivers = []
        for receiver in (node, *(peer for peer in self.pool.nodes if peer is not node)):
            health = receiver.health
            if (receiver.enabled and not receiver.error and health.get('shared_asset_cache') == shared_cache
                    and ASSET_RESUME_CAPABILITY in health.get('capabilities', ())):
                receivers.append(receiver)
        return receivers

    def _upload_shared_asset(self, receivers, asset, archive, *, cancel, report):
        for index, receiver in enumerate(receivers):
            has_peer = index + 1 < len(receivers)
            try:
                receiver.client.upload_resumable(asset, archive, cancel=cancel, report=report, fail_fast=has_peer)
            except RemoteError as error:
                if not has_peer or not error.transport_retryable:
                    raise
                report['receiver_failovers'] = report.get('receiver_failovers', 0) + 1
            else:
                return receiver

    @contextmanager
    def _asset_flight(self, key, cancel):
        """Elect one operation; waiters never hold a scheduler or transport lock."""
        with self.asset_flight_lock:
            done = self.asset_flights.get(key)
            owner = done is None
            if owner:
                done = self.asset_flights[key] = threading.Event()
        if not owner:
            while not done.wait(.1):
                _check_cancelled(cancel)
            _check_cancelled(cancel)
            yield False
            return
        try:
            yield True
        finally:
            with self.asset_flight_lock:
                del self.asset_flights[key]
            done.set()

    def _send_asset(self, node, shared_cache, asset, route, archive, *, cancel, report):
        archive_bytes = archive.stat().st_size
        receivers = self._resumable_asset_receivers(node, shared_cache) if archive_bytes >= ASSET_RESUME_MIN_BYTES else []
        started = time.monotonic()
        receiver = None
        if receivers:
            receiver = self._upload_shared_asset(receivers, asset, archive, cancel=cancel, report=report)
        elif report.get('protocol') == 'resumable':
            raise RemoteError('Waiting for the shared asset upload receiver to reconnect')
        else:
            report.update(protocol='legacy', archive_bytes=archive_bytes)
            node.client.request(route, archive.read_bytes(), cancel=cancel)
        if shared_cache:
            key = (shared_cache, asset)
            completed = time.monotonic()
            with self.asset_flight_lock:
                publications = self.shared_asset_publications
                publications.pop(key, None)
                publications[key] = ((receiver or node).name, completed, completed - started)
                if len(publications) > 1024:
                    del publications[next(iter(publications))]
        if receiver is not None and not node.client.request(route, cancel=cancel)['exists']:
            raise RemoteError('Waiting for the committed archive on the shared asset volume')

    def _ensure_shared_asset(self, node, cache, asset, route, archive, *, cancel, abandoned, report):
        key = (cache, asset)
        while True:
            with self.asset_flight_lock:
                published = self.shared_asset_publications.get(key)
            if published and published[0] != node.name:
                now = time.monotonic()
                deadline = published[1] + min(self.shared_asset_visibility_seconds, 1 + 2 * published[2])
                delay = 1
                while now < deadline:
                    until = min(deadline, now + delay)
                    while (left := until - time.monotonic()) > 0:
                        _check_cancelled(cancel)
                        time.sleep(min(.25, left))
                    if node.client.request(route, cancel=abandoned)['exists']:
                        return
                    now = time.monotonic()
                    delay = min(30, delay * 2)
            # Visibility waits never own the election. Independent targets can
            # use a mounted copy while this one waits for its own mount.
            with self._asset_flight(('shared', *key), cancel) as owner:
                _check_cancelled(cancel)
                if node.client.request(route, cancel=abandoned)['exists']:
                    return
                if owner:
                    with self.asset_flight_lock:
                        newer = self.shared_asset_publications.get(key)
                    if newer == published:
                        self._send_asset(node, cache, asset, route, archive, cancel=abandoned, report=report)
                        return

    def _ensure_assets(self, node, assets, *, force=False, cancel=None, report=None):
        assets = set(assets)
        if not assets:
            return
        cache = node.health.get('shared_asset_cache')
        with node.asset_lock:
            session = (node.health.get('pid'), node.health.get('root'), node.health.get('asset_revision'))
            if node.asset_session != session:
                node.known_assets.clear()
                node.asset_session = session
            if force:
                node.known_assets.difference_update(assets)
        for asset in assets:
            metrics = report.setdefault(asset, {}) if report is not None else {}
            route = '/assets/' + asset
            archive = self.root / 'assets' / (asset + '.zip')

            def abandoned():
                if cancel is None or not cancel():
                    return False
                # Another original can still need an upload owned by a
                # cancelled original. Read active references without the
                # dispatch lock; a later arrival can resume the same digest.
                return not any((job.get('node') is node or
                                (cache and job.get('node') is not None and
                                 job['node'].health.get('shared_asset_cache') == cache))
                               and not job['cancel'].is_set() and asset in job.get('task', {}).get('assets', {}).values()
                               for job in active_values(self.jobs, snapshot=True))

            while True:
                _check_cancelled(cancel)
                with node.asset_lock:
                    if node.asset_session == session and asset in node.known_assets:
                        metrics.setdefault('cache_hit', True)
                        break
                with self._asset_flight(('node', id(node), asset), cancel) as owner:
                    if not owner:
                        continue
                    with node.asset_lock:
                        if node.asset_session == session and asset in node.known_assets:
                            metrics.setdefault('cache_hit', True)
                            break
                    presence = node.client.request(route, cancel=abandoned)
                    exists = presence['exists']
                    metrics.setdefault('cache_hit', presence.get('cache_hit', exists and not cache))
                    if not exists:
                        if cache:
                            self._ensure_shared_asset(node, cache, asset, route, archive, cancel=cancel,
                                                      abandoned=abandoned, report=metrics)
                        else:
                            self._send_asset(node, None, asset, route, archive, cancel=abandoned, report=metrics)
                    with node.asset_lock:
                        if node.asset_session == session:
                            node.known_assets.add(asset)
                    _check_cancelled(cancel)
                    break
            if 'protocol' not in metrics:
                metrics.setdefault('wire_body_bytes', 0)

    def _reconcile_startup_memory(self, job, worker_id, remote):
        """Release a superseded center estimate once the original actually starts."""
        node, task = job['node'], job['task']
        funded = remote.get('memory_mb')
        if (remote['status'] != 'running' or not remote.get('execution_started') or
                job['result'].get('execution_started') or remote.get('resource_request') or
                remote.get('id') != worker_id or 'adaptive_memory' not in node.health.get('capabilities', ()) or
                type(funded) is not int or not 0 < funded < task['memory_mb'] or
                task['cpus'] != remote.get('cpus') or job.get('resize_uncertain') or
                job.get('grant_receipt_revision') is not None):
            return
        # The caller holds the center lock. A resize may be waiting for that
        # lock or for its worker ACK, so never wait on its lock from here.
        resize_lock = job.get('resize_lock')
        if resize_lock is not None and not resize_lock.acquire(blocking=False):
            return
        try:
            with self.pool.condition:
                if worker_id not in node.live_jobs:
                    return
                node.reserved_memory_mb -= task['memory_mb'] - funded
                task['memory_mb'] = funded
                self.pool.condition.notify_all()
                self.condition.notify_all()
        finally:
            if resize_lock is not None:
                resize_lock.release()

    def _update(self, job, remote):
        if job.get('retired'):return
        clean_tool_snapshot(remote)
        # A polling snapshot owns these fields, including their absence before
        # the replacement attempt has created its first process.
        for key in ('dispatch_stage','dispatch_stage_started_at','worker_sampled_at','worker_preparation_elapsed_seconds',
                    'runtime','progress','step_progress','progress_error','diagnostic','phase','stage','unit',
                    'completed','total','current','target','nfe','budget'):
            if key not in remote:job['result'].pop(key,None)
        if remote.get('status') in TERMINAL and remote['status']!='interrupted':
            job['result'].pop('remote_cancel_pending',None)
            job['result'].pop('cancel_error',None)
            job['result'].pop('cost_incomplete',None)
        if remote.get('status') in TERMINAL:
            for key in ('completed', 'total'):
                if key not in remote:
                    job['result'].pop(key, None)
        if isinstance(remote.get('progress'),dict) and remote['progress'].get('schema')=='dco.progress.v1':
            for key in ('completed','total','phase','unit'):
                if key not in remote['progress']:job['result'].pop(key,None)
        if 'resource_request' not in remote:job['result'].pop('resource_request',None)
        submitted = job["result"].get("center_submitted", job["result"]["submitted"])
        assigned = {key: job["result"][key] for key in ('id',"assigned_node", "worker_url", "dispatched", 'request_sha256') if key in job["result"]}
        if job['result'].get('_asset_variants'):
            assigned['assets'] = job['result']['assets']
        if remote.get('request_sha256') and remote['request_sha256']!=assigned.get('request_sha256'):
            assigned['worker_request_sha256']=remote['request_sha256']
        assigned.pop('resource_request',None)
        if remote.get('status')not in TERMINAL and 'min_cpus'in job['result']:
            assigned['min_cpus']=job['result']['min_cpus']
        if job.get('task'):
            assigned['cpus']=job['task']['cpus']
            assigned['priority']=job['task'].get('priority',MIN_PRIORITY)
            assigned['memory_mb']=job['task']['memory_mb']
            assigned['accelerator']=job['task'].get('accelerator')
        job["result"].update(remote, **assigned, center_submitted=submitted)
        # A full terminal receipt can omit the compact step counters sampled
        # while running. Refresh them from its explicit final step records.
        job['result'].update(progress_fields(remote))
        self._attempt_total(job['result'])
        if remote["status"] in TERMINAL:
            job["result"]["total_elapsed"] = time.time() - submitted
            self._save(job)
        else:
            self._publish(job)

    def _callback(self, node):
        if not node.client.url.startswith('local://'):
            return {'mode':'poll','timeout':node.client.timeout,'local_exact_activation':True}
        if not self.callback_address:return None
        host,port=self.callback_address[:2]
        host='127.0.0.1' if host=='0.0.0.0' else host
        return dict(url=f'http://{host}:{port}',token=self.token,timeout=node.client.timeout)

    @staticmethod
    def _resize_intent(job, node, cpus, active_cpus, min_cpus):
        explicit = min_cpus is not None and 'resize_min_cpus' in node.health.get('capabilities', ())
        maximum = job['task'].get('max_cpus', node.capacity)
        desired = min(cpus if min_cpus is not None else cpus or node.capacity, maximum)
        if not explicit:
            desired = max(1, desired)
        floor = (active_cpus or 0) if explicit else max(1, active_cpus or 0)
        cooperative = min_cpus is not None or cpus == 0 or _elastic_job(job)
        available = node.enabled and not node.error and not node.health.get('paused')
        fully_active = desired == job['task']['cpus'] == floor and available
        return desired, floor, explicit, cooperative, available and cooperative and not fully_active and (cpus == 0 or desired > 1)

    @staticmethod
    def _resize_memory_request(job, node, model):
        if any(value is not None for value in model.values()):
            return model
        current = job.get('phase_memory') or job['result'].get('memory_phase') or job['task'].get('memory_model') or {}
        if 'base_memory_mb' in current:
            return {key: current.get(key) for key in model}
        if 'adaptive_memory' in node.health.get('capabilities', ()):
            return dict(model, memory_mb=current.get('memory_mb', job['task']['memory_mb']))
        return model

    def _resize_fair_target(self, job, identity, cpus, active_cpus, min_cpus, model):
        node = job.get('node')
        if node is None or not self._resize_intent(job, node, cpus, active_cpus, min_cpus)[-1]:
            return None
        with self.condition, self.pool.condition:
            if job.get('node') is not node or job['result']['status'] in TERMINAL or job['cancel'].is_set():
                return None
            snapshot = PlacementSnapshot(self)
        view = snapshot.jobs[identity]
        desired, _, _, _, needs_plan = self._resize_intent(view, view['node'], cpus, active_cpus, min_cpus)
        if not needs_plan:
            return None
        view['desired_cpus'] = desired
        view['result']['min_cpus'] = min_cpus
        view['result']['active_cpus'] = active_cpus or 0
        phase = {key: value for key, value in self._resize_memory_request(view, view['node'], model).items()
                 if value is not None}
        target = snapshot._cooperative_cpu_target(identity, desired, snapshot._project_resources(), phase_model=phase)
        return node, target

    def resize(self, identity, cpus, *, active_cpus=None, min_cpus=None, memory_mb=None, base_memory_mb=None, memory_per_cpu_mb=None, memory_limit_mb=None):
        if type(cpus) is not int or cpus<0:
            raise ValueError('resize cpus must be nonnegative; 0 uses the current node budget')
        if memory_mb is not None and (type(memory_mb) is not int or memory_mb<1):
            raise ValueError('resize memory_mb must be a positive integer in MiB')
        if active_cpus is not None and (type(active_cpus) is not int or active_cpus<0):
            raise ValueError('active_cpus must count nonnegative in-flight threads')
        if min_cpus is not None and (type(min_cpus)is not int or not 0<=min_cpus<=cpus):
            raise ValueError('min_cpus must be an integer in 0..cpus')
        with self.condition:
            job = self.jobs[identity]
            resize_lock = job.get('resize_lock')
            if resize_lock is None:
                resize_lock = job['resize_lock'] = threading.Lock()
        # A slow worker must not hold up health, admission or other tasks.
        with resize_lock:
            request = dict(memory_mb=memory_mb, base_memory_mb=base_memory_mb,
                           memory_per_cpu_mb=memory_per_cpu_mb, memory_limit_mb=memory_limit_mb)
            fair = self._resize_fair_target(job, identity, cpus, active_cpus, min_cpus, request)
            with self.condition,self.pool.condition:
                job=self.jobs[identity];node=job.get('node')
                if self.stopping or job['cancel'].is_set() or node is None or job['result']['status'] in TERMINAL:
                    raise Conflict('Only a running task can change its CPU grant')
                grant_fields=('cpus','memory_mb','memory_phase','requested_cpus','min_cpus','active_cpus')
                prior_grant=tuple(job['result'].get(field) for field in grant_fields)
                old=job['task']['cpus']
                old_memory=job['task']['memory_mb']
                request = self._resize_memory_request(job, node, request)
                memory_mb = request['memory_mb']
                base_memory_mb = request['base_memory_mb']
                memory_per_cpu_mb = request['memory_per_cpu_mb']
                memory_limit_mb = request['memory_limit_mb']
                modeled=base_memory_mb is not None or memory_per_cpu_mb is not None or memory_limit_mb is not None
                capabilities=node.health.get('capabilities',[])
                if ('resize' not in capabilities or ((memory_mb is not None or modeled) and 'resize_memory' not in capabilities)
                        or active_cpus is not None and 'resize_active_cpus' not in capabilities):
                    return dict(id=identity,node=node.name,cpus=old,memory_mb=old_memory,requested_cpus=cpus,supported=False)
                maximum=job['task'].get('max_cpus',node.capacity)
                if min_cpus is not None and min_cpus>maximum:
                    raise ValueError('min_cpus exceeds task max_cpus')
                desired, active_floor, explicit, cooperative, needs_plan = self._resize_intent(
                    job, node, cpus, active_cpus, min_cpus)
                if active_cpus is not None and (active_cpus>old or active_cpus>desired):
                    raise ValueError(ACTIVE_GRANT_CONFLICT)
                requested_model={k:v for k,v in request.items()if v is not None}
                sample=next((j for j in node.health.get('active_jobs',())if j['id']==job.get('worker_id',identity)),{})
                phase_calibrated=bool(sample.get('memory_calibrated')and same_phase(requested_model,sample.get('memory_phase')or sample.get('memory_phase_model')))
                model=node.resize_memory_model(job.get('worker_id',identity),request)
                planner=(lambda width,available,**values:plan_memory(width,available,
                    floor=max(active_floor,min(min_cpus or 1,width)),**values))if 'adaptive_memory'in node.health.get('capabilities',())else lambda width,available,**values:plan_resize(width,available_mb=available,**values)
                previous_desired=job.get('desired_cpus')
                previous_memory=job.get('phase_memory')
                previous_active=job['result'].get('active_cpus',0)
                job['desired_cpus']=desired
                if min_cpus is not None:job['result']['min_cpus']=min_cpus
                else:job['result'].pop('min_cpus',None)
                job['result']['active_cpus']=active_cpus or 0
                draining=not node.enabled and not node.detached
                granted=min(desired,old)if draining else desired
                fully_active = (desired == old == active_floor and node.enabled
                                and not node.error and not node.health.get('paused'))
                if not draining and not fully_active and (desired > old or cpus == 0 or (cooperative and desired > 1)):
                    if node.error or job.get('recover'):
                        return dict(id=identity,node=node.name,cpus=old,memory_mb=old_memory,requested_cpus=cpus)
                    available = node.capacity - node.reserved - node.external + old
                    floor = active_floor if cooperative else old
                    granted = max(floor, min(desired, available)) if node.enabled and not node.health.get('paused') else active_floor if explicit else old
                    if needs_plan:
                        # A concurrent health/policy change can invalidate the
                        # opportunity, never the already held grant.
                        granted = min(granted, fair[1] if fair is not None and fair[0] is node else old)
                # The report describes work already in flight. Return only
                # unoccupied slots, then let the owner refill from the grant.
                granted=max(active_floor,granted)
                if min_cpus is not None and granted<min_cpus:granted=active_floor
                cpu_ready_limit=granted
                previous_phase=previous_memory or job['task'].get('memory_model') or dict(memory_mb=old_memory)
                cpu_return=granted<old and same_phase(requested_model or previous_phase,previous_phase)
                changes_memory=(memory_mb is not None and memory_mb!=old_memory) or modeled
                credit=node.memory_credit(job.get('worker_id',identity),old_memory) if changes_memory else old_memory
                if granted==0:memory=old_memory
                else:
                    granted,memory=planner(granted,max(credit,min(node.memory_capacity,credit+max(0,min(node.memory_free,self.pool._host_memory_free(node))))),**model)
                    if min_cpus is not None and granted<min_cpus:granted,memory=active_floor,old_memory
                memory=old_memory if memory is None else memory
                extra=max(0,memory-credit)
                if granted<active_floor or memory>node.memory_capacity and memory>old_memory or (extra and extra>min(node.memory_free,self.pool._host_memory_free(node))):
                    if not cpu_return:
                        if previous_desired is None:job.pop('desired_cpus',None)
                        else:job['desired_cpus']=previous_desired
                        raise Conflict('Insufficient memory for resource resize; existing grant retained')
                    granted,memory=cpu_ready_limit,old_memory
                job['phase_memory']=(dict(base_memory_mb=base_memory_mb,memory_per_cpu_mb=memory_per_cpu_mb,
                                         memory_limit_mb=memory_limit_mb) if modeled else
                                     dict(memory_mb=old_memory if memory_mb is None else memory_mb))
                active_changed=previous_active!=(active_cpus or 0)
                job['result'].update(active_cpus=active_cpus or 0,requested_cpus=cpus)
                phase_changed=({key:value for key,value in previous_phase.items() if value is not None} !=
                               {key:value for key,value in job['phase_memory'].items() if value is not None})
                if (granted==old and memory==old_memory and not phase_changed and not job.get('resize_uncertain') and not active_changed
                        and not(granted<cpu_ready_limit and not phase_calibrated)):
                    self._publish(job)
                    return dict(id=identity,node=node.name,cpus=old,memory_mb=old_memory,requested_cpus=cpus)
                # Reserve increases before the RPC; return decreases after its ack.
                held,held_memory=max(old,granted),max(old_memory,memory)
                node.reserved+=held-old
                node.reserved_memory_mb+=held_memory-old_memory
                worker_id=job.get('worker_id',identity)
                reclaimed=max(0,old_memory-credit) if changes_memory else 0
                if reclaimed:
                    node.memory_holds[worker_id]={'amount':max(reclaimed,node.memory_holds.get(worker_id,{}).get('amount',0)), 'after':None}
                job['task']['cpus']=job['result']['cpus']=held
                job['task']['memory_mb']=job['result']['memory_mb']=held_memory
                self._publish(job)

            def commit(actual,actual_memory,memory_phase=None):
                revision=None
                with self.condition,self.pool.condition:
                    # Completion may have released this task while the RPC waited.
                    if job.get('retired') or job['result']['status'] in TERMINAL or self.jobs.get(identity) is not job:
                        return
                    if job.get('worker_id',identity) in node.live_jobs:
                        node.reserved+=actual-job['task']['cpus']
                        node.reserved_memory_mb+=actual_memory-job['task']['memory_mb']
                    job['task']['cpus']=job['result']['cpus']=actual
                    job['task']['memory_mb']=job['result']['memory_mb']=actual_memory
                    if memory_phase is not None:job['result']['memory_phase']=memory_phase
                    self._publish(job)
                    if prior_grant!=tuple(job['result'].get(field) for field in grant_fields):
                        revision=job['grant_receipt_revision']=object()
                    receipt=job.get('read_result') or job['result'].copy()
                    hold=node.memory_holds.get(job.get('worker_id',identity))
                    if job.get('worker_id',identity)in node.live_jobs:
                        if hold is None:
                            hold=node.memory_holds[job.get('worker_id',identity)]={'amount':0}
                        hold.update(after=time.monotonic(),credit=actual_memory)
                    self.pool.condition.notify_all()
                    self.condition.notify_all()
                if revision is not None:
                    self._persist_grant(job,revision,receipt)

            try:
                worker_id=job.get('worker_id',identity)
                options={}
                if modeled:
                    options.update(base_memory_mb=base_memory_mb,memory_per_cpu_mb=memory_per_cpu_mb,memory_limit_mb=memory_limit_mb)
                elif memory_mb is not None:options['memory_mb']=memory_mb if 'adaptive_memory'in capabilities else memory
                if 'adaptive_memory'in capabilities:options['_memory_budget_mb']=memory
                if active_cpus is not None:options['active_cpus']=active_cpus
                if explicit:options['min_cpus']=min(min_cpus,granted)
                result=node.client.resize(worker_id,granted,**options)
            except RemoteError as error:
                if getattr(error,'status_code',None) is not None:
                    with self.condition:
                        if previous_desired is None:job.pop('desired_cpus',None)
                        else:job['desired_cpus']=previous_desired
                        if previous_memory is None:job.pop('phase_memory',None)
                        else:job['phase_memory']=previous_memory
                    commit(old,old_memory)
                    if error.status_code==409:raise Conflict(str(error))from None
                    raise
                try:
                    observed=node.client.status(job.get('worker_id',identity))
                except RemoteError:
                    # The worker may have accepted the increase. Keep its held
                    # budget until an acknowledged resize or normal completion.
                    with self.condition:job['resize_uncertain']=True
                    raise
                result=dict(id=identity,cpus=observed['cpus'],memory_mb=observed['memory_mb'])
                if isinstance(observed.get('memory_phase'),dict):result['memory_phase']=observed['memory_phase']
            with self.condition:job.pop('resize_uncertain',None)
            quote=result.get('memory_calibration')
            if quote and same_phase(quote['phase'],job.get('phase_memory')):
                # The resize acknowledgement owns fresher calibration than
                # the periodic device snapshot. Use it at this same boundary.
                with self.pool.condition:
                    sample=next((j for j in node.health.get('active_jobs',())if j['id']==worker_id),None)
                    if sample is not None:
                        sample.update(memory_phase=quote['phase'],memory_adaptive_model=quote['model'],memory_calibrated=True,
                                      memory_reservation_mb=quote['physical_mb'],memory_commit_reservation_mb=quote['commit_mb'],
                                      memory_observed_mb=quote.get('observed_physical_mb',quote['physical_mb']),
                                      memory_commit_observed_mb=quote.get('observed_commit_mb',quote['commit_mb']),
                                      memory_sample_cpus=quote['sample_cpus'])
            commit(result['cpus'],result.get('memory_mb',old_memory),result.get('memory_phase'))
            cpu_ready_limit=min(cpu_ready_limit,result.get('cpu_available',cpu_ready_limit))
            calibrating=(result['cpus']<cpu_ready_limit and not result.get('memory_guard_limited')
                         and (not phase_calibrated or not result.get('memory_calibrated',phase_calibrated)))
            return dict(result,id=identity,node=node.name,requested_cpus=cpus,cpu_ready_limit=cpu_ready_limit,
                        memory_calibrating=bool(result.get('memory_calibrating')or calibrating))

    def _original_node(self, name, url):
        if name:
            return next((node for node in self.pool.nodes if node.name == name or name in getattr(node, 'aliases', ())), None)
        # Older attempt records only carry an endpoint.
        return next((node for node in self.pool.nodes if node.client.url == url), None)

    def _client(self, job):
        if job.get("node"):
            return job["node"].client
        result = job['result']
        node = self._original_node(result.get('assigned_node'), result.get('worker_url'))
        return node.client if node is not None else None

    def _queued_memory_wait(self, job):
        """Explain the current admission predicate without changing a grant."""
        if self.paused or job['result'].get('resource_wait_reason'):
            return None
        task=job['task']; choices=[]
        for node in self.pool._matching(task):
            if (node.error or not node.enabled or node.health.get('paused') or node.health.get('queued',0)
                    or node.capacity-node.reserved-node.external < startup_cpus(task)
                    or task.get('accelerator') is not None and not node.can_match_accelerator(
                        node.execution_accelerator(task,available=True))):
                continue
            floor=max(job['result'].get('resource_recovery_memory_mb',0),node.startup_memory(task))
            available=max(0,min(node.memory_free,self.pool._host_memory_free(node)))
            choices.append((available-floor,available,floor))
        if not choices:return None
        margin,available,floor=max(choices)
        if margin>=0:return None
        return ('memory: declared %s MiB; startup requires %s MiB; best eligible headroom %.0f MiB'
                %(task.get('memory_mb',256),floor,available))

    def get(self, identity):
        if self.job_observed(identity):
            with self.condition, self.pool.condition:
                row = self.observations.rows[identity]
                node = next((n for n in self.pool.nodes if n.name == row['node'] and not n.error), None)
                sample = next((item for item in node.health.get('active_jobs') or ()
                               if item['id'] == identity), None) if node else None
                return self.observations.project(row, sample)
        if isinstance(self.jobs, JobRecords):
            job = self.jobs.cached(identity)
            if job is not None and 'read_result' in job:
                result=json.loads(json_bytes(job['read_result']))
                node = job.get('node')
                if result.get('dispatch_stage') == 'awaiting_worker' and node and not node.error:
                    worker_id = job.get('worker_id') or identity
                    sample = next((row for row in node.health.get('active_jobs', ())
                                   if row['id'] == worker_id), None)
                    if sample and sample.get('status') in ('queued','running'):
                        result.update(progress_fields(sample), worker_status=sample['status'])
                    else:
                        result.update(progress_fields(node.health.get('pending_unpacks', {}).get(worker_id, {})))
                if result.get('status')=='queued' and not result.get('resource_wait_reason'):
                    with self.condition,self.pool.condition:
                        if job['result'].get('status')=='queued':
                            reason=self._queued_memory_wait(job)
                            if reason:result['resource_wait_reason']=reason
                return result
            if identity in self.jobs.history:
                return self.jobs.read_result(identity)
            if job is None:
                raise KeyError(identity)
        job = load_job(self.jobs, self.condition, identity)
        with self.condition:
            self._publish(job)
            return json.loads(json_bytes(job['read_result']))

    def public_get(self, identity):
        return self.public_result(self.get(identity))

    def compact_status(self, identity):
        """Read the published state without recalculating queue diagnostics."""
        row = self.observations.rows.get(identity)
        if row is not None:
            if row['status'] == 'queued':
                node = next((n for n in self.pool.nodes if n.name == row['node'] and not n.error), None)
                if node and any(item['id'] == identity and item.get('status') == 'running'
                                for item in node.health.get('active_jobs') or ()):
                    return {'status': 'running'}
            return batch_status(row, compact=True)
        job = self.jobs.cached(identity)
        snapshot = job.get('read_result') if job is not None else None
        if snapshot is None:
            snapshot = self.jobs.read_result(identity)
        return batch_status(snapshot, compact=True)

    def logs(self, identity):
        job = load_job(self.jobs, self.condition, identity)
        with self.condition:
            client = self._client(job)
        if client:
            try:
                return client.logs(job.get('worker_id',identity))
            except RemoteError as error:
                if getattr(error, 'status_code', None) != 404 or job['result']['status'] != 'dispatching':
                    raise Conflict(str(error)) from None
        return {"stdout": "", "stderr": ""}

    @staticmethod
    def _cached_job_output(job, identity, name):
        result = job['result']
        if result['status'] not in TERMINAL:
            return None
        name = str(name).replace('\\', '/')
        path = PurePosixPath(name)
        if path.is_absolute() or '..' in path.parts or ':' in name or not path.parts:
            raise ValueError('Expected a relative file path: ' + name)
        try:
            with zipfile.ZipFile(job['directory'] / 'worker-artifacts.zip') as archive:
                if json.loads(archive.read('result.json'))['id'] != job.get('worker_id', result.get('worker_id', identity)):
                    return None
                return archive.read('files/' + path.as_posix())
        except (OSError, KeyError, ValueError, zipfile.BadZipFile):
            return None

    def job_file(self, identity, name, data=None):
        job = load_job(self.jobs, self.condition, identity)
        if data is None:
            cached = self._cached_job_output(job, identity, name)
            if cached is not None:
                return cached
        with self.condition:
            client = self._client(job)
        if not client:
            raise Conflict('Job has not been assigned yet')
        try:
            return client.read_job_file(job.get('worker_id',identity), name) if data is None else client.write_job_file(job.get('worker_id',identity), name, data)
        except RemoteError as error:
            if getattr(error, 'status_code', None) == 404:
                raise FileNotFoundError(name) from None
            raise Conflict(str(error)) from None

    def job_file_version(self, identity, name, version=None):
        job = load_job(self.jobs, self.condition, identity)
        cached = self._cached_job_output(job, identity, name)
        if cached is not None:
            etag = '"' + hashlib.sha256(cached).hexdigest() + '"'
            return dict(version=etag, changed=etag != version, data=cached if etag != version else None)
        with self.condition:
            client=self._client(job);worker_id=job.get('worker_id',identity)
        if not client:raise Conflict('Job has not been assigned yet')
        from .local import LocalClient
        if isinstance(client,LocalClient):return client.worker.job_file_version(worker_id,name,version)
        try:
            return client.read_job_file_changed(worker_id,name,version)
        except RemoteError as error:
            if getattr(error,'status_code',None)==404:raise FileNotFoundError(name) from None
            if getattr(error,'status_code',None)==409:raise Conflict(str(error)) from None
            raise

    def _settle_cancelled_handoff(self, job, node, identity):
        """Fence an unacknowledged upload or hand an admitted original to normal settlement."""
        with self.condition:
            job['result']['handoff_cancel_pending'] = True
            self._save(job)
            while job.get('cancel_sending'):
                self.condition.wait(.1)
            fenced = job['result'].get('handoff_fenced', False)
        if not fenced:
            try:
                remote = node.client.cancel_many([identity], handoff_ids=[identity]).get(identity, {})
            except OSError as error:
                raise RemoteError('Cancellation awaits an authoritative worker receipt') from error
            fenced = remote.get('handoff_fenced', False)
            if not fenced and remote.get('status', 'unknown') == 'unknown':
                raise RemoteError('Cancellation awaits an authoritative worker receipt')
            with self.condition:
                self._cancel_response(job, remote=remote)
        with self.condition:
            job['result'].pop('handoff_cancel_pending', None)
        if fenced:
            return dict(status='cancelled', finished=time.time(), cancel_requested=True,
                        cancelled_before_submit=True, handoff_fenced=True, cpu_seconds=0)
        return None

    def _cancel_response(self, job, remote=None, error=None):
        result = job['result']
        if error is not None and result['status'] not in TERMINAL and not result.get('cancel_acknowledged'):
            result.update(remote_cancel_pending=True, cancel_error=str(error))
        elif remote is not None and (remote.get('status') != 'unknown' or remote.get('cancel_requested')):
            result.pop('cancel_error', None)
            result.pop('remote_cancel_pending', None)
            result['cancel_acknowledged'] = True
            if remote.get('kill_signal_sent'):
                result['kill_signal_sent'] = True
            if remote.get('handoff_fenced'):
                result['handoff_fenced'] = True

    def _defer_cancellation(self, job):
        """Keep a cancelled intent live internally until its original node settles it."""
        job.update(recover=True, node=None)
        result = job['result']
        result.update(status='interrupted', finished=None,
                      error='Cancellation pending until the original worker reconnects',
                      cancel_requested=True, remote_cancel_pending=True, cost_incomplete=True)
        if (pending := self.__dict__.get('_pending_recovery')) is not None:
            pending.add(result['id'])
        self._save(job)

    def cancel_all(self):
        """Persist one snapshot, stop dispatch, and deliver worker controls asynchronously."""
        with self.condition:
            if self.cancel_snapshot_thread is not None and self.cancel_snapshot_thread.is_alive():
                return self.cancel_snapshot
            if self.stopping:
                raise Conflict('Scheduler is stopping')
            jobs, deferred = {}, []
            for identity, job in active_items(self.jobs):
                if job['result']['status'] not in TERMINAL:
                    jobs[identity] = job
                elif cancellation_needs_reconnect(job['result']):
                    deferred.append(identity)
            if not jobs:
                return dict(status='incomplete' if deferred else 'completed', matched=len(deferred),
                            finished=0, pending=deferred, errors={}, jobs=[])
            atomic_json(self.cancel_snapshot_path, list(jobs))
            self.queue[:] = [identity for identity in self.queue if identity not in jobs]
            routes = defaultdict(list)
            for identity, job in jobs.items():
                node = job.get('node')
                client = (node.client if node is not None and not node.detached
                          and job['result']['status'] != 'queued' else None)
                if client is not None and not job.get('cancel_sending') and not job['result'].get('cancel_acknowledged'):
                    job['cancel_sending'] = True
                    routes[client].append((job.get('worker_id', identity), job))
                job['result']['cancel_requested'] = True
                self._publish(job)
                job['cancel'].set()
                if stop_post := job.get('handoff_stop'):
                    stop_post.interrupt()
            self.cancel_snapshot = dict(status='accepted', matched=len(jobs) + len(deferred), finished=0,
                                        pending=list(jobs) + deferred, errors={}, jobs=[])
            self.cancel_snapshot_thread = threading.Thread(target=self._deliver_cancel_snapshot,
                args=(jobs, routes), name='center-cancel-snapshot', daemon=True)
            self.cancel_snapshot_thread.start()
            self.condition.notify_all()
            return self.cancel_snapshot

    def _deliver_cancel_snapshot(self, jobs, routes):
        def send(client, pairs):
            failure = None
            try:
                ids = [identity for identity, _job in pairs]
                handoffs = [identity for identity, job in pairs
                            if job['result'].get('dispatch_stage') == 'awaiting_worker']
                responses = client.cancel_many(ids, handoff_ids=handoffs) if handoffs else client.cancel_many(ids)
            except (RemoteError, OSError) as error:
                responses = {}
                failure = error
            with self.condition:
                for identity, job in pairs:
                    job.pop('cancel_sending', None)
                    self._cancel_response(job, remote=responses.get(identity), error=failure)
                    self._save(job)
                self.condition.notify_all()

        try:
            with ThreadPoolExecutor(max_workers=8, thread_name_prefix='worker-cancel') as controls:
                sends = [controls.submit(send, client, pairs) for client, pairs in routes.items()]
                for job in jobs.values():
                    with self.condition:
                        if job['result']['status'] == 'queued':
                            job['result'].update(status='cancelled', finished=time.time())
                            self._save(job)
                            archive_job(self.jobs, job)
                        elif job['result']['status'] not in TERMINAL:
                            if job.get('recover') and job.get('node') is None:
                                self._defer_cancellation(job)
                            else:
                                self._save(job)
                for sent in sends:
                    sent.result()
        except (OSError, RemoteError) as error:
            self.activity.notify('Cancellation snapshot retained for recovery: ' + str(error))
        else:
            atomic_json(self.cancel_snapshot_path, [])

    def cancel(self, identity):
        job = load_job(self.jobs, self.condition, identity)
        client = None
        with self.condition:
            if job["result"]["status"] not in TERMINAL:
                job["cancel"].set()
                if stop_post := job.get('handoff_stop'):
                    stop_post.interrupt()
                job['result']['cancel_requested'] = True
                if identity in self.queue:
                    self.queue.remove(identity)
                    job["result"].update(status="cancelled", finished=time.time())
                    self._save(job)
                    archive_job(self.jobs, job)
                elif job.get('recover') and job.get('node') is None:
                    self._defer_cancellation(job)
                else:
                    node = job.get('node')
                    if ((node is None or not node.detached) and not job.get('cancel_sending')
                            and not job['result'].get('cancel_acknowledged')):
                        client = self._client(job)
                        if client is not None:
                            job['cancel_sending'] = True
                self._save(job)
                self.condition.notify_all()
        # Do not wait for the execution poller, which may be inside a resource
        # handshake. Client.cancel owns an independent short control transport.
        if client is not None:
            try:
                remote = client.cancel(job.get('worker_id',identity))
            except (RemoteError,OSError) as error:
                with self.condition:
                    job.pop('cancel_sending', None)
                    self._cancel_response(job, error=error)
                    self._save(job)
                    self.condition.notify_all()
            else:
                with self.condition:
                    job.pop('cancel_sending', None)
                    self._cancel_response(job, remote=remote)
                    self._save(job)
                    self.condition.notify_all()
        return self.get(identity)

    def prepare_artifacts(self, identity):
        """Acknowledge slow remote preparation without tying it to an HTTP wait."""
        job = load_job(self.jobs, self.condition, identity)
        with self.condition:
            if job['result']['status'] not in TERMINAL:
                raise Conflict('Artifacts are available when the job has finished')
            client = self._client(job)
            node = job.get('node') or next((n for n in self.pool.nodes if n.client is client), None)
            if (job['result'].get('cancelled_before_submit') or
                    job['result']['status'] == 'interrupted' and node is not None and (node.detached or not node.enabled)):
                client = None
        from .local import LocalClient
        if client is None or isinstance(client, LocalClient):
            return True
        if (job['directory']/'worker-artifacts.zip').is_file():
            return True
        with self.condition:
            if self.jobs.get(identity) is not job:
                raise KeyError(identity)
            preparation = job.get('artifact_preparation')
            if preparation is not None:
                if not preparation['done'].is_set():
                    return False
                job.pop('artifact_preparation')
                if preparation.get('error') is not None:
                    # The provider client has already exhausted its bounded
                    # reconnect attempt. End this collection attempt, preserving
                    # the original ID for an explicit subsequent retry.
                    raise Conflict('Artifact preparation failed: '+str(preparation['error']))
                return True
            if self.stopping:
                raise Conflict('Scheduler is stopping')
            preparation = {'done': threading.Event()}
            job['artifact_preparation'] = preparation

            def prepare(job=job):
                try:
                    # Keep the original job resident and use the same ID/node
                    # locks as fetch/delete. Only the durable provider ZIP is
                    # prepared; no final response or scientific payload is held.
                    def download():
                        try:
                            return self.artifacts(identity, is_connected=lambda: not self.stopping, cache_only=True)
                        except RemoteError as error:
                            # This is the same upstream 502 exposed by the
                            # synchronous HTTP path. Preserve its bounded
                            # recovery before reporting a preparation failure.
                            if getattr(error, 'status_code', None) is None:
                                error.status_code = 502
                            raise
                    client._read_reconnecting(download)
                except Exception as error:
                    preparation['error'] = error
                finally:
                    with self.condition:
                        preparation['done'].set()
                        self.threads.discard(threading.current_thread())
                        self.condition.notify_all()

            thread = threading.Thread(target=prepare, name='artifact-preparation', daemon=True)
            self.threads.add(thread)
            thread.start()
            return False

    @staticmethod
    def _worker_artifact_archive(client, worker_id, cached):
        if cached.is_file():
            return cached.read_bytes()
        # Resume the original download and keep it for publication and fetch.
        temporary = cached.with_suffix('.partial')
        client.artifacts(worker_id, destination=temporary)
        raw = temporary.read_bytes()
        with zipfile.ZipFile(io.BytesIO(raw)) as archive:
            if json.loads(archive.read('result.json'))['id'] != worker_id:
                raise ValueError('Artifact task identity mismatch')
        temporary.replace(cached)
        unlink(temporary.with_name(temporary.name + '.http.json'))
        return raw

    def reconcile_original(self, identity):
        """Settle a terminal interruption from its authenticated original route."""
        if not isinstance(identity, str) or not _ID.fullmatch(identity):
            raise ValueError('Reconciliation requires an original ID')
        job = load_job(self.jobs, self.condition, identity)
        with self.condition:
            result = job['result'].copy()
            if result['status'] not in TERMINAL:
                raise Conflict('Original still has an active owner')
            if not (result.get('remote_cancel_pending') or result.get('cost_incomplete')):
                return result
            client = self._client(job)
            worker_id = result.get('worker_id', identity)
            lock = job.setdefault('artifact_lock', threading.RLock())
        if client is None:
            raise Conflict('Original worker route is unavailable')
        with lock:
            remote = client.status(worker_id)
            if remote.get('status') not in TERMINAL or remote.get('cost_incomplete'):
                raise Conflict('The original worker has not settled execution')
            if remote.get('id') != worker_id or any(remote.get(key) != result.get(key)
                    for key in ('project', 'label', 'assets')):
                raise Conflict('Worker receipt does not match the original job binding')
            expected = result.get('worker_request_sha256')
            if expected and remote.get('request_sha256') != expected:
                raise Conflict('Worker receipt has a different request digest')
            if not expected and not result.get('handoff_acknowledged_at'):
                raise Conflict('Original receipt has no verified handoff')
            if not isinstance(remote.get('cpu_seconds'), (int, float)):
                raise Conflict('Original worker CPU accounting is missing')
            directory = job['directory']
            if hashlib.sha256((directory / 'request.zip').read_bytes()).hexdigest() != result['request_sha256']:
                raise Conflict('Original request digest changed')
            if remote['status'] == 'succeeded':
                self._worker_artifact_archive(client, worker_id, directory / 'worker-artifacts.zip')
            with self.condition:
                if job.get('retired') or job['result'] != result:
                    raise Conflict('Original changed during reconciliation')
                self._update(job, remote)
                archive_job(self.jobs, job)
                return job['result'].copy()

    def artifacts(self, identity, *, is_connected=None, public=False, cache_only=False):
        job = load_job(self.jobs, self.condition, identity)
        with self.condition:
            if job["result"]["status"] not in TERMINAL:
                raise Conflict("Artifacts are available when the job has finished")
            result, client = json.loads(json_bytes(job['result'])), self._client(job)
            if result.get('cancelled_before_submit')or result.get('rejected_before_submit'):client=None
            node=job.get('node') or next((n for n in self.pool.nodes if n.client is client),None)
            cached=job['directory']/'worker-artifacts.zip'
            if (result['status']=='interrupted' and node is not None and (node.detached or not node.enabled)
                    and not cached.is_file()):
                result['cost_incomplete']=True
                client=None
            worker_id=job.get('worker_id',result.get('worker_id',identity))
            result_lock=job.setdefault('artifact_lock',threading.RLock())
        sealed = None
        if not cache_only and result['status'] == 'succeeded' and not result.get('attempts'):
            try:
                sealed = cached.read_bytes()
            except FileNotFoundError:
                pass
        # A complete archive is published atomically. A reader holding its
        # bytes need not queue behind a previous download from the worker.
        with (nullcontext() if sealed is not None else result_lock):
            with self.condition:
                if self.jobs.get(identity) is not job:
                    raise KeyError(identity)
            files = {}
            if (client is None and sealed is None and not cached.is_file() and result['status'] != 'interrupted'
                    and not (result.get('cancelled_before_submit') or result.get('rejected_before_submit'))
                    and (result['status'] == 'succeeded' or result.get('execution_started') or result.get('worker_id'))
                    and (result.get('assigned_node') or result.get('worker_url'))):
                raise Conflict('Original result archive is waiting for its worker to reconnect')
            if client or sealed is not None or cached.is_file():
                from .local import LocalClient
                if sealed is None and isinstance(client, LocalClient) and result['status'] != 'interrupted' and not result.get('attempts') and not cached.is_file():
                    return client.worker.artifacts(worker_id, result=self.public_result(result) if public else result)
                try:
                    if is_connected and not is_connected():
                        raise ConnectionAbortedError('Result download caller disconnected while queued')
                    raw = sealed if sealed is not None else self._worker_artifact_archive(client, worker_id, cached)
                except RemoteError as error:
                    if getattr(error, 'status_code', None) != 404 or result['status'] != 'interrupted':
                        raise
                else:
                    if cache_only:
                        return
                    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
                        files = {name: archive.read(name) for name in archive.namelist() if name != "result.json"}
                        if result['status']=='interrupted':
                            remote=json.loads(archive.read('result.json'))
                            if remote.get('error_kind')=='memory_pressure':remote['status']='interrupted'
                            with self.condition:
                                job['result'].pop('error',None)
                                job['result'].pop('connection_error',None)
                                self._update(job,remote)
                                result=json.loads(json_bytes(job['result']))
            for index, attempt in enumerate(result.get('attempts',()),1):
                if not attempt.get('artifact'):continue
                saved=job['directory']/'attempts'/('%04d.zip'%index)
                if not saved.is_file():
                    source = self._original_node(attempt.get('node'), attempt.get('worker_url'))
                    if source is None:raise Conflict('Interrupted attempt provider is unavailable')
                    raw=source.client.artifacts(attempt['worker_id'])
                    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
                        if json.loads(archive.read('result.json'))['id']!=attempt['worker_id']:
                            raise ValueError('Interrupted attempt identity mismatch')
                    saved.parent.mkdir(exist_ok=True)
                    pending=saved.with_suffix('.partial');pending.write_bytes(raw);pending.replace(saved)
                raw=saved.read_bytes()
                if attempt.get('error_kind')=='memory_pressure':
                    with zipfile.ZipFile(io.BytesIO(raw))as archive:
                        for name in archive.namelist():
                            if name.startswith('files/')and not name.endswith('/'):
                                files.setdefault(name,archive.read(name))
                if public:
                    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
                        nested={name:archive.read(name) for name in archive.namelist() if name!='result.json'}
                        nested['result.json']=json_bytes(self.public_result(json.loads(archive.read('result.json'))))
                    raw=pack(nested)
                files['files/'+attempt['artifact']]=raw
            recovery=job['directory']/'resource-resume.json'
            if recovery.is_file():
                restore_map_results(files,json.loads(recovery.read_bytes()),status=result['status'],error=result.get('error'))
            files["result.json"] = json_bytes(self.public_result(result) if public else result)
            return pack(files)

    def _retirement_record(self, job):
        result = job['result']
        if cancellation_needs_reconnect(result):
            raise Conflict('The original cancellation and CPU accounting are waiting for the worker to reconnect')
        targets = []
        if not (result.get('rejected_before_submit') or result.get('cancelled_before_submit')):
            client = self._client(job)
            url = client.url if client is not None else result.get('worker_url')
            if url or result.get('assigned_node'):
                targets.append(dict(url=url, node=result.get('assigned_node'),
                    id=job.get('worker_id') or result.get('worker_id') or result['id'],
                    cancel=bool(result.get('remote_cancel_pending'))))
        targets.extend(dict(url=attempt['worker_url'], node=attempt.get('node'), id=attempt['worker_id'])
                       for attempt in result.get('attempts', ())
                       if attempt.get('artifact') and attempt.get('worker_url') and attempt.get('worker_id'))
        return dict(targets=targets)

    def _reclaim_retired(self, identity, record):
        with self.jobs.record_lock(identity):
            remove_owned(self.root / 'jobs' / identity)
        for target in record['targets']:
            if self.retirement.stopping:
                raise InterruptedError('Retirement resumes on next startup')
            node = self._original_node(target.get('node'), target.get('url'))
            if node is None:
                raise UnregisteredRetirementProvider('Original retirement provider is no longer configured')
            if node.error or node.detached:
                raise OSError('Retirement provider temporarily unavailable')
            try:
                if target.get('cancel'):
                    node.client.cancel(target['id'])
                node.client.delete(target['id'])
            except RemoteError as error:
                if error.status_code != 404:
                    raise OSError(str(error)) from error
        with self.condition:
            self.jobs.record_retired(identity)
        self.asset_maintenance.request()

    def delete(self, identity):
        with self.submission_lock.hold(identity):
            return retire_job(self, identity, describe=self._retirement_record)

    def clean(self):
        from .operations import clean_pending
        return clean_pending(self)

    def _protected_assets(self, *, recent=True):
        with self.condition:
            # Compatibility clients without an original ID have the same
            # preparation window as an explicit handoff, instead of pinning forever.
            now = time.time()
            protected = {asset for asset in self.legacy_assets
                         if recent and now - self.asset_cache.entries.get(asset, (0, 0))[1] < 3600}
            self.legacy_assets.intersection_update(protected)
            protected.update(handoff_references(self))
            recent_inputs = {asset for asset, (_, used) in self.asset_cache.entries.items()
                             if now - used < self.asset_cache.max_idle}
            protected.update(asset_references(self.jobs, recent_terminal_assets=recent_inputs))
            protected.update(self.accepting_assets)
            return protected

    def _report_asset_cleanup(self, error):
        self.activity.notify('资产缓存待清理 ' + error, 'local')

    def _prune_assets(self, *, recent=True):
        with self.asset_cache.lock:
            protected = self._protected_assets(recent=recent)
            for error in self.asset_cache.prune(protected):
                self._report_asset_cleanup(error)

    def _maintain_assets(self, *, wait=True):
        for error in prune_asset_caches((self, *self.local_asset_workers), wait=wait):
            self._report_asset_cleanup(error)

    def asset_handoff(self, identity, assets=None):
        # The same original-ID handoff is understood by center and worker.
        value = asset_handoff(self, identity, assets)
        if assets is None:
            self.asset_maintenance.request()
        return value

    def asset(self, identity, payload=None, *, job_id=None):
        if not _ASSET.fullmatch(identity):
            raise ValueError("Asset id requires SHA256")
        if job_id is not None and not _ID.fullmatch(job_id):
            raise ValueError('Job id requires 32 lowercase hex characters')
        path = self.root / "assets" / (identity + ".zip")
        def published():
            exists=path.is_file()
            if exists:
                self.asset_cache.touch(identity)
                if job_id is None:
                    self.legacy_assets.add(identity)
            if job_id is not None:
                held = self.asset_handoffs.get(job_id, (set(), 0))[0]
                self.asset_handoff(job_id, list(set(held) | {identity}))
            return {"id": identity, "exists": exists}
        if payload is None:
            with self.asset_cache.lock:
                return published()
        with self.asset_cache.lock:
            transfer=self.asset_writes.setdefault(identity,threading.Lock())
        with transfer:
            with self.asset_cache.lock:
                value=published()
            if not value['exists']:
                if hashlib.sha256(payload).hexdigest() != identity:
                    raise ValueError("Asset digest does not match bundle")
                temporary=path.with_suffix('.upload')
                try:
                    temporary.write_bytes(payload)
                    with self.asset_cache.lock:
                        temporary.replace(path)
                        value=published()
                finally:
                    unlink(temporary)
                self.asset_maintenance.request()
        return value

    def close(self, *, preserve_queued=False):
        with self.condition:
            self.stopping = True
            # Native reloads preserve accepted, undispatched inputs. The
            # replacement restores the original queue from durable receipts.
            identities = [identity for identity, job in active_items(self.jobs)
                          if not preserve_queued or job['result']['status'] != 'queued']
            self.condition.notify_all()
        for identity in identities:
            try:
                self.cancel(identity)
            except KeyError:
                pass
        self.scheduler.join()
        if self.cancel_snapshot_thread is not None:
            self.cancel_snapshot_thread.join()
        self.asset_maintenance.close()
        self.observation_executor.shutdown(wait=True, cancel_futures=True)
        for thread in list(self.threads):
            thread.join()
        self.retirement.close()
        self.asset_handoffs.clear()
        self._prune_assets(recent=False)
        self.asset_cache.reclaimer.close()
        self.root_lock.close()
