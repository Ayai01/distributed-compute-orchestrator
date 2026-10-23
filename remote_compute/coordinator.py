"""A central HTTP endpoint forwarding accepted jobs to the shared worker pool."""

import hashlib
from contextlib import contextmanager
import io
import json
from pathlib import Path
import shutil
import threading
import time
import weakref
import zipfile

from .bundle import PROTOCOL, TERMINAL, atomic_json, json_bytes, pack
from .client import PoolBusy, RemoteError, RequestCancelled, _check_cancelled
from .worker import Conflict, _ASSET, _ID, lock_root, validate_task, asset_handoff
from .policy import (MAX_QUEUE_RATIO, MAX_REQUIREMENTS,
                     MAX_REQUIREMENT_LENGTH,
                     MAX_RESOURCE_SHARE, MAX_SINGLE_THREAD_WEIGHT,
                     public_capabilities, normalize_accelerator_requirement,
                     aggregate_accelerators, canonical_capabilities, clean_tool_snapshot)
from .telemetry import Activity
from .allocation import weighted_node_threads, plan_resize
from .compat import unlink
from .cleanup import AssetCache
from .records import Job, JobRecords, active_items, active_values, archive_job, asset_references
import socket
import math
import os
import re
import secrets
from urllib.parse import urlsplit
from . import __version__


class Coordinator:
    def __init__(self, pool, root, *, token="", operator_token=None):
        self.pool, self.token = pool, token
        self.activity = Activity()
        self.root = Path(root).resolve()
        self.root_lock = lock_root(self.root, "scheduler")
        (self.root / "jobs").mkdir(parents=True, exist_ok=True)
        (self.root / "assets").mkdir(exist_ok=True)
        self.operator_token = self._load_operator_token(operator_token)
        self.condition = threading.Condition(threading.RLock())
        self.jobs, self.queue, self.threads = JobRecords(self.root / 'jobs'), [], set()
        self.asset_handoffs = {}
        self.legacy_assets = set()
        self.stopping = self.paused = False
        self.restart_available = False
        self.callback_address = None
        for path in (self.root / "jobs").glob("*/result.json"):
            value = json.loads(path.read_text(encoding="utf-8"))
            if clean_tool_snapshot(value):
                atomic_json(path, value)
            job = Job(result=value, directory=path.parent,
                      cancel=threading.Event(), node=None)
            self.jobs[value["id"]] = job
            if value["status"] not in TERMINAL or (value['status']=='interrupted' and value.get('finished') is None):
                with zipfile.ZipFile(path.parent/'request.zip') as archive:
                    task=json.loads(archive.read('task.json'))
                task=validate_task(task,max(1,task.get('cpus',1)),allow_auto=True)
                self._restore_accelerator(task, value)
                job.update(task=task,auto_cpus=task['cpus']==0)
                value.setdefault('max_retries',task.get('retries',2))
                if value['status']=='queued' or not value.get('worker_url'):
                    if value.get('worker_id'):job['worker_id']=value['worker_id']
                    value.update(status='queued')
                    self.queue.append(value['id'])
                else:
                    value.update(status='dispatching')
                    job['recover']=True
                value.pop('error',None)
                atomic_json(path, value)
            else:
                archive_job(self.jobs, job, startup=True)
        self.asset_cache = AssetCache(self.root / 'assets', archives=True)
        self._prune_assets()
        self.scheduler = threading.Thread(target=self._schedule, name="central-dispatch", daemon=True)
        self.scheduler.start()

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

    def admission(self, request, *, operator=False):
        project = request.get('project') or 'default'
        job_id=request.get('job_id')
        advice={}
        if job_id is not None:
            with self.condition,self.pool.condition:
                job=self.jobs.get(job_id)
                if job is None or job.get('node') is None or job['result']['status'] in TERMINAL:
                    raise Conflict('admission job_id must identify a running task')
                if (job['task'].get('project') or 'default')!=project:
                    raise ValueError('admission project differs from the running task')
                node=job['node'];old=job['task']['cpus'];old_memory=job['task']['memory_mb']
                resource=self.project_resources()[project]
                advised=max(1,min(job['task'].get('max_cpus',node.capacity),node.capacity,
                                  resource['target']-resource['used']+old,node.capacity-node.reserved-node.external+old))
                if job.get('auto_cpus'):
                    advised=min(advised,self._cooperative_cpu_target(job_id,node.capacity,{project:resource}))
                advice=dict(recommended_cpus=advised if node.enabled and not node.health.get('paused') else old,
                            current_cpus=old,current_memory_mb=old_memory,
                            available_memory_mb=old_memory+max(0,min(node.memory_free,self.pool._host_memory_free(node))))
        share = request.get('resource_share', 1.0)
        ratio = request.get('queue_ratio', 1.0)
        cpus = request.get('cpus', 1)
        maximum_cpus = request.get('max_cpus')
        requires = canonical_capabilities(request.get('requires', []))
        accelerator = normalize_accelerator_requirement(request.get('accelerator'))
        if not operator and request.get('node') is not None:
            raise ValueError('node selection belongs to the DCO operator; use requires for capabilities')
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
        resources = self.project_resources()
        shares = {name: row['resource_share'] for name,row in resources.items()}
        demands = {name: min(row['demand'],row['capacity']) for name,row in resources.items()}
        paused={node.name for node in self.pool.nodes if node.health.get('paused')}
        held={name:cpus for name,cpus in resources.get(project,{}).get('node_demands',{}).items() if name in paused}
        eligible=[n for n in self.pool.nodes if not n.error and n.enabled and not n.health.get('paused')
                  and n.capacity>=max(1,cpus)
                  and (not node_name or n.name==node_name)
                  and set(requires).issubset(canonical_capabilities(n.health.get('tags',())))
                  ]
        fallback_capable = (sum(n.capacity for n in eligible)
                            if accelerator is not None and 'fallback_after_seconds' in accelerator else 0)
        eligible = [n for n in eligible if n.supports_accelerator(accelerator)]
        capable=sum(n.capacity for n in eligible)
        names={n.name for n in eligible}|{name for row in resources.values() for name in row['eligible_nodes']}
        shares[project], demands[project] = share, capable+sum(held.values())
        rooms={name:row['node_demands'] for name,row in resources.items()}
        rooms[project]={**held,**{node.name:node.capacity for node in eligible}}
        grants=weighted_node_threads({n.name:n.reserved if n.health.get('paused') else n.capacity for n in self.pool.nodes if n.name in names
                                     and not n.error and n.enabled},shares,demands,rooms)
        target=sum(grants[project].values())
        ceilings=[min(n.limit or n.health.get('cpu_limit',n.capacity),n.health.get('cpu_limit',n.capacity)) for n in eligible]+list(held.values())
        maximum=sum(ceilings)
        unit = cpus or maximum_cpus
        # Grants on different devices cannot share one task. Count each
        # device's partial automatic grant instead of merging their CPUs.
        # Fixed jobs can repack the fair plan's node shares; only physical
        # device remainders impose a per-node limit on their task count.
        def slots(values):
            return sum(math.ceil(value/unit) if unit else bool(value) for value in values)
        wave = max(1,slots(grants[project].values()) if cpus==0
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
                    room=max(0,target-resources.get(project,{}).get('used',0))
                    free=[max(0,node.capacity-node.reserved-node.external) for node in eligible]
                    credits=[]
                    remaining=room
                    for value in free:
                        credits.append(min(value,remaining))
                        remaining-=credits[-1]
                    extra=slots(credits) or int(room>0)
                    ready=max(1,running+extra)
            ceiling=max(1,maximum)  # Every admitted task may later shrink to one CPU.
        return dict(project=project,resource_share=share,queue_ratio=ratio,target_cpus=target,capable_cpus=capable,
                    accelerator=accelerator,fallback_capable_cpus=fallback_capable,
                    **advice,
                    running_tasks=ready,queued_tasks=math.ceil(wave*ratio),
                    in_flight=ready+math.ceil(wave*ratio),max_in_flight=ceiling+math.ceil(ceiling_wave*ratio))

    def restart_node(self, name):
        node = next((n for n in self.pool.nodes if n.name == name), None)
        if node is None:
            raise ValueError("Unknown node: " + str(name))
        try:
            return {"node": name, **node.client.restart()}
        except RemoteError as error:
            raise Conflict(str(error)) from None

    def health(self, *, brief=False, sample=False):
        if brief:
            return dict(protocol=PROTOCOL,role='scheduler',node=socket.gethostname(),version=__version__,
                        capabilities=['resize','resize_memory','resize_auto','resize_memory_model','max_cpus','single_thread_weight','admission_job','job_file_version','retry_jobs','accelerators','accelerator_fallback','asset_handoff']+(['restart'] if self.restart_available else []))
        with self.condition, self.pool.condition:
            return self._health()

    def capabilities(self):
        """Return the scheduler's aggregate project-facing capability view.

        This deliberately has no node names, URLs, paths, jobs, project
        quotas, or placement policy.  The operator console continues to use
        :meth:`health` for that private view.
        """
        with self.condition, self.pool.condition:
            online = [n for n in self.pool.nodes if n.health and not n.error and n.enabled]
            tags = set()
            tools = set()
            accelerator_profiles = []
            for node in online:
                tags.update(node.health.get('tags', ()))
                accelerator_profiles.extend(node.health.get('accelerators', ()))
                for name, info in node.health.get('tools', {}).items():
                    if not info.get('available'):
                        continue
                    tools.add(name)
            cpu_capacity = sum(n.capacity for n in online)
            cpu_available = sum(max(0, n.capacity - n.reserved - n.external) for n in online)
            memory_capacity = sum(n.memory_capacity for n in online)
            memory_available = sum(max(0, n.memory_free) for n in online)
            return {
                'protocol': PROTOCOL,
                'role': 'scheduler',
                'version': __version__,
                'capabilities': public_capabilities(),
                'tags': canonical_capabilities(tags),
                'tools': canonical_capabilities(tools),
                'accelerators': aggregate_accelerators(accelerator_profiles),
                'cpu': {'capacity': cpu_capacity, 'available': cpu_available, 'unit': 'logical_threads'},
                'memory': {'capacity_mb': memory_capacity, 'available_mb': memory_available, 'unit': 'MiB'},
            }

    @staticmethod
    def public_result(result):
        """Return a project result without placement or worker-local paths."""
        hidden = {'assigned_node', 'worker_url', 'worker_id', 'node', 'cwd',
                  'tool_root', 'root', 'home', 'worker_root',
                  'shared_paths', 'unavailable_shared', 'resource_request'}

        def clean_text(text):
            if not isinstance(text, str):
                return text
            # Errors are useful to the project, but transport URLs and local
            # worker paths are private scheduler details.
            text = re.sub(r'https?://[^\s\])}]+', '<remote>', text)
            return re.sub(r'(?i)(?:[A-Za-z]:[\\/]|/(?:home|tmp|var|mnt)/)[^\s\])}]+', '<remote-path>', text)

        def clean_mapping(mapping):
            cleaned = {key: item for key, item in mapping.items() if key not in hidden}
            for key in ('error', 'connection_error'):
                if key in cleaned:
                    cleaned[key] = clean_text(cleaned[key])
            # Keep per-step timing and return codes without argv/path data.
            if isinstance(mapping.get('steps'), list):
                cleaned['steps'] = [{key: step[key] for key in ('started', 'finished', 'returncode') if key in step}
                                   for step in mapping['steps'] if isinstance(step, dict)]
            if isinstance(mapping.get('tools'), dict):
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
        value = self.capabilities()
        if brief:
            return {key: value[key] for key in ('protocol', 'role', 'version', 'capabilities')}
        with self.condition:
            value.update({
                'used_cpus': sum(n.reserved + n.external for n in self.pool.nodes if not n.error and n.enabled),
                'queued': len(self.queue),
                'paused': self.paused,
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
                    item = {k: result.get(k) for k in ('id','label','status','cpus','memory_mb','started')}
                    item['single_thread_weight']=job.get('task',{}).get('single_thread_weight',1.0)
                    item['project'] = job.get('task',{}).get('project','')
                    item.update({key:job['task'].get(key,default) for key,default in
                                 (('priority',0),('resource_share',1.0))})
                    active.setdefault(result['assigned_node'],[]).append(item)
            for node in snapshot['nodes']:
                external, remaining = [], node['external_cpus']
                sampled={item['id']:item for item in node.get('active_jobs',[])}
                owned=active.get(node['name'],[])
                for item in owned:
                    job=self.jobs[item['id']]
                    worker_id=job.get('worker_id',job['result'].get('worker_id',item['id']))
                    item['rss_mb']=sampled.get(worker_id,{}).get('rss_mb')
                for item in node.get('active_jobs',[]):
                    known = item['id'] in known_ids or item['id'] in self.jobs
                    if isinstance(self.jobs, JobRecords):
                        known = known or self.jobs.knows_worker(item['id'])
                    if not known and 0 < item.get('cpus',0) <= remaining:
                        external.append(item)
                        remaining -= item['cpus']
                node['active_jobs'] = owned + external
            return {"protocol": PROTOCOL, "role": "scheduler", "node": socket.gethostname(), "tools": tools,
                    'pid':os.getpid(), 'version':__version__, 'root':self.root.as_posix(),
                    'tool_root':Path(__file__).resolve().parent.parent.as_posix(),
                    'asset_cache': self.asset_cache.snapshot(),
                    'retained_jobs': (len(self.jobs.history) if isinstance(self.jobs, JobRecords) else 0)
                                     + sum(j['result']['status'] in TERMINAL for j in active_values(self.jobs)),
                    'capabilities':['resize','resize_memory','resize_auto','resize_memory_model','max_cpus','single_thread_weight','admission_job','job_file_version','retry_jobs','accelerators','accelerator_fallback','asset_handoff']+(['restart'] if self.restart_available else []),
                    "cpus": sum(n.capacity for n in online), "used_cpus": sum(n.reserved + n.external for n in online),
                    "logical_cpus": sum(group[0].health.get("logical_cpus", 0) for group in machines.values()),
                    "memory_total_mb": sum(group[0].health.get("memory_total_mb", 0) for group in machines.values()),
                    "memory_capacity_mb": sum(min(sum(n.memory_capacity for n in group),
                                                   group[0].health.get("memory_total_mb", 0)) for group in machines.values()),
                    "memory_available_mb": sum(max(0, min(self.pool._host_memory_free(group[0]),
                                                          sum(n.memory_free for n in group))) for group in machines.values()),
                    "memory_reserved_mb": sum(n.memory_accounted for n in online),
                    "memory_requested_mb": sum(n.reserved_memory_mb+n.external_memory_mb for n in online),
                    "paused": self.paused, "queued": len(self.queue), "tags": sorted(tags),
                    "policy": snapshot["policy"], "nodes": snapshot["nodes"],
                    'queued_jobs':[{**{key:self.jobs[identity]['result'].get(key) for key in
                        ('id','label','status','cpus','memory_mb','started')},
                        **{key:self.jobs[identity]['task'].get(key,default) for key,default in
                           (('project','default'),('priority',0),('resource_share',1.0))}}
                        for identity in self.queue],
                    'projects': self.project_resources(), **self.activity.snapshot()}

    def project_resources(self):
        shares, demands, used, queued, rooms = {}, {}, {}, {}, {}
        online={n.name:n for n in self.pool.nodes if not n.error and n.enabled
                and (not n.health.get('paused') or n.reserved)}
        capacities={name:node.reserved if node.health.get('paused') else node.capacity for name,node in online.items()}
        largest = max((n.capacity for n in online.values() if not n.health.get('paused')), default=1)
        with self.condition,self.pool.condition:
            for job in active_values(self.jobs):
                if job['result']['status'] in TERMINAL:
                    continue
                task = job.get('task', {})
                name = task.get('project') or 'default'
                shares[name] = task.get('resource_share', 1.0)
                demand = job.get('desired_cpus',largest if job.get('auto_cpus') else task.get('cpus', 1))
                demand=min(demand,task.get('max_cpus',demand))
                held=job.get('node')
                paused=held is not None and held.health.get('paused')
                if paused:demand=task.get('cpus',0)
                demands[name] = demands.get(name, 0) + demand
                available=rooms.setdefault(name,{})
                wanted={**task,'cpus':0} if job.get('auto_cpus') else task
                if paused and held.name in online:
                    # Pausing stops new work; existing grants still occupy
                    # their project's share until their tasks release them.
                    available[held.name]=available.get(held.name,0)+demand
                else:
                    for node in self.pool._matching(wanted):
                        if node.name in online and not node.health.get('paused') and (not held or node is held):
                            available[node.name]=available.get(node.name,0)+min(demand,node.capacity)
                used[name] = used.get(name, 0) + (task.get('cpus', 0) if job.get('node') else 0)
                queued[name] = queued.get(name, 0) + int(job['result']['status'] == 'queued')
        names={node for available in rooms.values() for node in available}
        ceilings={name:sum(min(capacities[node],demand) for node,demand in rooms[name].items()) for name in shares}
        grants=weighted_node_threads({name:capacities[name] for name in names},shares,demands,rooms)
        quotas={name:sum(grants[name].values()) for name in shares}
        return {name: dict(resource_share=shares[name], target=quotas[name], demand=demands[name], used=used[name], queued=queued[name],
                           capacity=ceilings[name],eligible_nodes=sorted(rooms[name]),node_demands=rooms[name],
                           node_targets=grants[name]) for name in shares}

    def control(self, changes):
        if isinstance(changes, dict) and "node" in changes:
            name = changes["node"]
            node = next((n for n in self.pool.nodes if n.name == name), None)
            if node is None:
                raise ValueError('Unknown node: ' + str(name))
            if "routing" in changes:
                self.pool.configure(name, **changes["routing"])
            if "worker" in changes:
                try:
                    node.client.control(**changes["worker"])
                except RemoteError as error:
                    if getattr(error, 'status_code', None) == 400:
                        raise ValueError(str(error)) from None
                    raise Conflict(str(error)) from None
                self.pool.refresh(nodes=[node])
            return self.health()
        if isinstance(changes, dict) and set(changes) == {"policy"}:
            self.pool.configure(policy=changes["policy"])
            return self.health()
        if isinstance(changes, dict) and set(changes) == {"add"}:
            self.pool.add(**changes["add"])
            return self.health()
        if not isinstance(changes, dict) or set(changes) - {"paused"} or not isinstance(changes.get("paused"), bool):
            raise ValueError("Scheduler control accepts paused=true|false; node budgets are set per node")
        with self.condition:
            self.paused = changes["paused"]
            self.condition.notify_all()
        return self.health()

    def _save(self, job):
        atomic_json(job["directory"] / "result.json", job["result"])

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
            online = any(not node.error and node.enabled and not node.health.get('paused')
                         for node in self.pool._matching(job['task']))
        reason = ('unavailable' if not online else
                  'queue_timeout' if now - result['submitted'] >= requirement['fallback_after_seconds'] else None)
        if reason is not None:
            job['task']['accelerator'] = None
            result.update(accelerator=None, accelerator_fallback=dict(reason=reason, at=now))
            self._save(job)

    def submit(self, identity, payload, *, operator=False):
        if not _ID.fullmatch(identity):
            raise ValueError("Job id requires 32 lowercase hex characters")
        digest = hashlib.sha256(payload).hexdigest()
        with self.asset_cache.lock:
            with self.condition:
                if self.stopping:
                    raise Conflict("Scheduler is stopping")
                if identity in self.jobs:
                    saved = self.jobs[identity]["result"]
                    if saved["request_sha256"] != digest:
                        raise Conflict("Job id already contains a different request")
                    return dict(saved)
            with zipfile.ZipFile(io.BytesIO(payload)) as archive:
                raw_task = json.loads(archive.read("task.json"))
                if not operator and raw_task.get('node') is not None:
                    raise ValueError('node selection belongs to the DCO operator; use requires for capabilities')
                task = validate_task(raw_task,
                                     max((n.capacity for n in self.pool.nodes), default=1), allow_auto=True)
            for asset in set(task['assets'].values()):
                if not (self.root/'assets'/(asset+'.zip')).is_file():
                    raise ValueError('Synchronize missing asset first: '+asset)
                self.asset_cache.touch(asset)
            directory = self.root / "jobs" / identity
            directory.mkdir()
            (directory / "request.zip").write_bytes(payload)
            job = Job({"directory": directory, "task": task, "auto_cpus": task['cpus'] == 0, "cancel": threading.Event(), "node": None,
                   "result": {"id": identity, "status": "queued", "label": task["label"], "project": task['project'], "cpus": task["cpus"],
                              "single_thread_weight": task.get('single_thread_weight',1.0),
                              "accelerator": task.get('accelerator'),
                              "max_retries": task.get('retries',2), "attempts": [],
                              "memory_mb": task["memory_mb"], "request_sha256": digest,
                              "submitted": time.time(), "started": None, "finished": None,
                              "stdout": "", "stderr": "", "steps": [], "assets": task["assets"]}})
            if task['accelerator'] is not None and 'fallback_after_seconds' in task['accelerator']:
                job['result']['requested_accelerator'] = task['accelerator']
            self._save(job)
            with self.condition:
                stopping = self.stopping
                if not stopping:
                    self.jobs[identity] = job
                    self.asset_handoffs.pop(identity, None)
                    self.queue.append(identity)
                    self.queue.sort(key=lambda key: -self.jobs[key]["task"]["priority"])
                    self.condition.notify_all()
            if stopping:
                shutil.rmtree(directory)
                raise Conflict("Scheduler is stopping")
            return dict(job["result"])

    def _queued_candidates(self,budgets):
        """Within a priority, serve the least CPU per resource-share first."""
        from collections import deque
        counts={key:sum(not node.error and node.enabled and not node.health.get('paused')
                        for node in self.pool._matching(self.jobs[key]['task'])) for key in self.queue}
        for borrowing in (False,True):
            for priority in sorted({self.jobs[key]['task']['priority'] for key in self.queue},reverse=True):
                groups={}
                for key in sorted(self.queue,key=lambda key:(self.jobs[key]['task']['cpus']==0,counts[key])):
                    task=self.jobs[key]['task']
                    if task['priority']==priority:groups.setdefault(task.get('project') or 'default',deque()).append(key)
                while groups:
                    group=min(groups,key=lambda group:(budgets[group]['used']/budgets[group]['resource_share'],
                        self.jobs[groups[group][0]]['task']['cpus']==0,counts[groups[group][0]]))
                    key=groups[group].popleft()
                    if not groups[group]:del groups[group]
                    yield key,borrowing

    def _growth_headroom(self, task, resources):
        """Keep newly freed slots for acknowledged, unfilled phase demand.

        This holds no worker lease and never changes an active pool. Its owner
        claims the slots with the next drained resize; lowering demand or
        finishing makes them available to queued work immediately.
        """
        protected,groups={},{}
        for identity,job in sorted(active_items(self.jobs),key=lambda row:-row[1].get('task',{}).get('priority',0)):
            held=job.get('node');current=job.get('task',{})
            desired=job.get('desired_cpus',0);old=current.get('cpus',0)
            if (held is None or desired<=old
                    or job['result']['status']!='running' or job['cancel'].is_set()
                    or current.get('priority',0)<task.get('priority',0)
                    or held.error or not held.enabled or held.health.get('paused')):
                continue
            group=current.get('project') or 'default';resource=resources[group]
            room=max(0,resource['target']-resource['used']-groups.get(group,0))
            free=max(0,held.capacity-held.reserved-held.external-protected.get(held.name,0))
            if not room or not free:continue
            target=self._cooperative_cpu_target(identity,desired,resources)
            available_memory=current['memory_mb']+max(0,min(held.memory_free,self.pool._host_memory_free(held)))
            model=dict(memory_mb=None,base_memory_mb=None,memory_per_cpu_mb=None,memory_limit_mb=None)
            model.update(job.get('phase_memory',dict(memory_mb=current['memory_mb'])))
            target,memory=plan_resize(target,available_mb=available_memory,**model)
            if memory is not None and memory>available_memory:continue
            count=max(0,min(target-old,room,free))
            if count:
                protected[held.name]=protected.get(held.name,0)+count
                groups[group]=groups.get(group,0)+count
        return protected

    def _fixed_queue_claims(self, resources):
        """Let a ready fixed-width job accumulate its share on one worker."""
        claims, groups = {}, set()
        def order(identity):
            task = self.jobs[identity]['task']
            group = task.get('project') or 'default'
            return (-task['priority'], resources[group]['used']/resources[group]['resource_share'])
        with self.pool.condition:
            for identity in sorted(self.queue, key=order):
                job = self.jobs[identity]; task = job['task']
                group = task.get('project') or 'default'; resource = resources[group]
                width = task['cpus']
                if (job.get('auto_cpus') or width <= 1 or group in groups or job['cancel'].is_set()
                        or width > resource['target']-resource['used']):
                    continue
                ready = [node for node in self.pool._matching(task)
                         if node.name not in claims and not node.error and node.enabled
                         and not node.health.get('paused') and not node.health.get('queued', 0)
                         and node.capacity-node.external >= width
                         and min(node.memory_free, self.pool._host_memory_free(node)) >= task['memory_mb']
                         and node.can_match_accelerator(task.get('accelerator'))]
                if ready:
                    node = max(ready, key=lambda node: (node.capacity-node.reserved-node.external, node.name))
                    claims[node.name] = (node, task)
                    groups.add(group)
        return claims

    @staticmethod
    def _fixed_queue_headroom(task, resources, claims):
        group = task.get('project') or 'default'; current = resources[group]
        protected = {}
        for name, (node, waiting) in claims.items():
            owner = waiting.get('project') or 'default'; resource = resources[owner]
            if (owner != group and (waiting['priority'] > task['priority'] or
                    waiting['priority'] == task['priority'] and
                    resource['used']/resource['resource_share'] <= current['used']/current['resource_share'])):
                protected[name] = max(0, min(waiting['cpus'], node.capacity-node.reserved-node.external))
        return protected

    def _schedule(self):
        with self.condition:
            while not self.stopping:
                self._recover_running()
                if not self.paused:
                    for identity in self.queue:
                        self._fallback_accelerator(self.jobs[identity])
                    budgets = self.project_resources()
                    growth = {}
                    fixed = None
                    # Within the same priority and CPU mode, place constrained
                    # work first so flexible jobs retain the other devices.
                    # First honor weighted targets, then lend otherwise idle slots.
                    for identity, borrowing in self._queued_candidates(budgets):
                        if identity not in self.queue:
                            continue
                        job = self.jobs[identity]
                        group = job['task'].get('project') or 'default'
                        budget = budgets[group]
                        room = budget['target'] - budget['used']
                        if not borrowing and (room <= 0 or (job['task']['cpus'] or 1) > room):
                            continue
                        try:
                            priority=job['task']['priority']
                            if priority not in growth:
                                growth[priority]=self._growth_headroom(job['task'],budgets)
                            if fixed is None:
                                fixed = self._fixed_queue_claims(budgets)
                            headroom = dict(growth[priority])
                            for name, count in self._fixed_queue_headroom(job['task'], budgets, fixed).items():
                                headroom[name] = max(headroom.get(name, 0), count)
                            node = self.pool._acquire(job["task"], block=False,
                                cpu_limit=room if room > 0 else None,
                                partial_cpus=time.time()-job['result']['submitted']>=1,job_id=job.get('worker_id',identity),
                                node_headroom=headroom)
                        except (RuntimeError, ValueError) as error:
                            job["result"].update(status="failed", error=str(error), finished=time.time())
                            self._save(job)
                            self.queue.remove(identity)
                            archive_job(self.jobs, job)
                            continue
                        if node is None:
                            continue
                        self.queue.remove(identity)
                        job["node"] = node
                        budget['used'] += job['task']['cpus']
                        growth.clear()
                        fixed = None
                        job["result"].update(status="dispatching", assigned_node=node.name, worker_url=node.client.url,
                                             dispatched=time.time(), cpus=job['task']['cpus'])
                        self._save(job)
                        thread = threading.Thread(target=self._execute, args=(job,), name="central-job", daemon=True)
                        self.threads.add(thread)
                        thread.start()
                self.condition.wait(0.05 if self.queue else .5 if self.__dict__.get('_pending_recovery') else None)

    def _recover_running(self):
        """Reattach accepted work from fresh worker snapshots; never resubmit it."""
        with self.condition,self.pool.condition:
            pending=self.__dict__.get('_pending_recovery')
            if pending is None:
                pending=self._pending_recovery={identity for identity,job in active_items(self.jobs) if job.get('recover')}
            for identity in list(pending):
                job=self.jobs.get(identity)
                if job is None or job['result']['status'] in TERMINAL:
                    pending.discard(identity);continue
                node=next((n for n in self.pool.nodes if n.client.url==job['result']['worker_url']),None)
                worker_id=job['result'].get('worker_id',identity)
                if node is None or node.error or worker_id in node.live_jobs:continue
                remote=next((r for r in node.health.get('active_jobs') or [] if r['id']==worker_id),None)
                with zipfile.ZipFile(job['directory']/'request.zip') as archive:
                    task=json.loads(archive.read('task.json'))
                task=validate_task(task,max(1,task.get('cpus',1)),allow_auto=True)
                self._restore_accelerator(task, job['result'])
                automatic=task['cpus']==0
                grant=remote or job['result']
                task.update(cpus=grant['cpus'],memory_mb=grant['memory_mb'])
                job.update(task=task,auto_cpus=automatic,node=node,reattach=True,worker_id=worker_id)
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
                job['result'].pop('error',None)
                job['result'].setdefault('dispatched',job['result']['submitted'])
                if remote:self._update(job,remote)
                self._save(job)
                job.pop('recover',None)
                pending.discard(identity)
                thread=threading.Thread(target=self._execute,args=(job,),name='central-reconnect',daemon=True)
                self.threads.add(thread);thread.start()
                self.activity.notify('Reconnected original task ['+identity[:4]+']')

    def _execute(self, job):
        node, identity, task = job["node"], job.get('worker_id',job['result']['id']), job["task"]
        result = None;retry=False
        try:
            if not job.pop('reattach',False):
                payload = (job['directory'] / 'request.zip').read_bytes()
                if (job['auto_cpus'] or 'max_cpus' in task or 'single_thread_weight' in task
                        or 'retries' in task or task.get('node') is not None
                        or 'requested_accelerator' in job['result']):
                    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
                        files = {name: archive.read(name) for name in archive.namelist()}
                    worker_task = dict(task)
                    # Placement hints belong to the dispatcher, not the executable.
                    worker_task.pop('node',None)
                    worker_task.pop('single_thread_weight',None)
                    worker_task.pop('retries',None)
                    if worker_task.get('accelerator') is not None:
                        worker_task['accelerator'] = {
                            key: value for key, value in worker_task['accelerator'].items()
                            if key != 'fallback_after_seconds'}
                    if 'max_cpus' not in node.health.get('capabilities',[]):
                        worker_task.pop('max_cpus',None)
                    files['task.json'] = json_bytes(worker_task)
                    payload = pack(files)
                callback=self._callback(node) if 'resize' in node.health.get('capabilities',[]) else None
                if callback:callback={**callback,'job_id':job['result']['id']}
                if not self._submit(job, identity, payload, callback):
                    result=dict(status='cancelled',finished=time.time(),cancelled_before_submit=True,cpu_seconds=0)
                    with self.condition:self._update(job,result)
                    return
            cancel_sent=False
            while True:
                try:
                    if job['cancel'].is_set() and node.detached:
                        # Cancellation has no remote acknowledgement. Finish
                        # the caller's drain, keeping the original receipt for
                        # recovery rather than retrying uncertain computation.
                        with self.condition:
                            job['result'].update(status='interrupted',finished=time.time(),
                                error='Cancellation pending on a worker detached after 60 seconds offline',
                                remote_cancel_pending=True,cost_incomplete=True)
                            self._save(job)
                        break
                    if job["cancel"].is_set() and not cancel_sent:
                        node.client.cancel(identity)
                        cancel_sent=True
                    remote = node.client.status(identity, brief=True)
                    request=remote.get('resource_request')
                    if request and job.get('lease_answered')!=request['request_id']:
                        options={k:v for k,v in request.items() if k!='request_id'}
                        try:
                            operation=options.pop('_operation','resize')
                            reply={'grant':self.admission(options) if operation=='admission' else self.resize(job['result']['id'],**options)}
                        except (Conflict,RemoteError,ValueError) as error:
                            reply={'error':str(error)}
                        node.client.request('/jobs/'+identity+'/lease',{'request_id':request['request_id'],**reply})
                        job['lease_answered']=request['request_id']
                    if remote['status'] in TERMINAL:
                        remote = node.client.status(identity)
                except RemoteError as error:
                    if getattr(error,'status_code',None)==404:
                        retry=True
                        result=dict(status='interrupted',error='Worker unavailable: '+str(error),finished=time.time())
                        with self.pool.condition:node.error=str(error)
                        break
                    if self.stopping or getattr(error,'status_code',None) is not None:
                        raise
                    with self.condition:
                        job['result']['connection_error']=str(error)
                    time.sleep(.5)
                    continue
                with self.condition:
                    job['result'].pop('connection_error',None)
                    # Retry is one atomic state transition. A client must not
                    # collect/delete a transient interrupted outcome mid-retry.
                    if remote['status']!='interrupted':self._update(job, remote)
                if remote["status"] in TERMINAL:
                    result = remote
                    retry=remote['status']=='interrupted'
                    break
                interval=0.15 if time.time()-job['result']['dispatched']<2 else 0.2
                if cancel_sent:
                    time.sleep(interval)
                else:
                    job['cancel'].wait(interval)
        except Exception as error:
            with self.condition:
                if not retry:
                    job["result"].update(status="interrupted", error=str(error), finished=time.time())
                    self._save(job)
        finally:
            self.pool._release(node, task, result,job_id=identity)
            with self.condition:
                self.threads.discard(threading.current_thread())
                if retry and not self.stopping and not job['cancel'].is_set():self._retry(job,result)
                archive_job(self.jobs, job)
                self.condition.notify_all()

    def _submit(self, job, identity, payload, callback):
        node, force, uncertain = job['node'], False, False
        while True:
            if not uncertain and job['cancel'].is_set():return False
            submitting=False
            try:
                with self._asset_handoff(node,identity,job['task']['assets'].values(),enabled=force):
                    self._ensure_assets(node, job['task']['assets'].values(), force=force,
                                        cancel=None if uncertain else job['cancel'].is_set)
                    if not uncertain and job['cancel'].is_set():return False
                    submitting=True
                    node.client.submit_bundle(identity,payload,**({'callback':callback} if callback else {}))
                return True
            except RequestCancelled:
                return False
            except PoolBusy as error:
                # Nothing was sent. Keep the same reservation and ID while the
                # local transport queue drains, rather than ending the task.
                if self.stopping:raise
                with self.condition:job['result']['connection_error']=str(error)
                if uncertain:
                    time.sleep(.1)
                elif job['cancel'].wait(.1):
                    return False
            except RemoteError as error:
                if not force and getattr(error,'status_code',None)==400 and 'Synchronize missing asset first:' in str(error):
                    force=True
                    continue
                if self.stopping or getattr(error,'status_code',None) is not None:
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
                    with self.condition:job['result']['connection_error']=str(error)
                    try:
                        node.client.status(identity,brief=True)
                        return True
                    except RemoteError as observed:
                        if getattr(observed,'status_code',None)==404:
                            # The first POST may still arrive later. Repeating
                            # its exact ID and payload remains worker-idempotent.
                            break
                        if self.stopping or getattr(observed,'status_code',None) is not None:
                            raise
                        error=observed
                        time.sleep(.5)

    def _retry(self, job, outcome):
        job['result'].update(status='interrupted',error=outcome.get('error','Worker stopped'),finished=outcome.get('finished',time.time()))
        self._save(job)
        attempts=job['result'].setdefault('attempts',[])
        if len(attempts)>=job['result'].get('max_retries',0):return
        attempts.append(dict(node=job['node'].name,worker_id=job.get('worker_id',job['result']['id']),
                             status=outcome['status'],error=outcome.get('error',''),finished=outcome.get('finished')))
        identity=job['result']['id']
        with zipfile.ZipFile(job['directory']/'request.zip') as archive:
            task=json.loads(archive.read('task.json'))
        task=validate_task(task,max(1,task.get('cpus',1)),allow_auto=True)
        self._restore_accelerator(task, job['result'])
        worker_id=hashlib.sha256((identity+':'+str(len(attempts))).encode()).hexdigest()[:32]
        job.update(task=task,auto_cpus=task['cpus']==0,node=None,worker_id=worker_id)
        job.pop('desired_cpus',None);job.pop('phase_memory',None);job.pop('lease_answered',None)
        job['result'].update(status='queued',worker_id=worker_id,cpus=task['cpus'],memory_mb=task['memory_mb'],finished=None)
        for key in ('error','connection_error','assigned_node','worker_url','dispatched','elapsed','total_elapsed','peak_rss_mb','cpu_seconds'):
            job['result'].pop(key,None)
        job['result'].update(started=None,stdout='',stderr='',steps=[])
        self.queue.append(identity);self.queue.sort(key=lambda key:-self.jobs[key]['task']['priority'])
        self._save(job)
        self.activity.notify('Worker stopped; retry original task ['+identity[:4]+']')

    @contextmanager
    def _asset_handoff(self, node, identity, assets, *, enabled):
        if not enabled or 'asset_handoff' not in node.health.get('capabilities',[]):
            yield
            return
        route='/jobs/'+identity+'/asset-handoff'
        node.client.request(route,{'assets':sorted(set(assets))})
        try:
            yield
        finally:
            # Acceptance replaces the handoff with the ordinary job reference.
            # Failure/cancellation releases it without changing the original ID.
            try:
                node.client.request(route,method='DELETE')
            except RemoteError:
                pass  # Worker expiry also releases an unreachable sender's hold.

    def _ensure_assets(self, node, assets, *, force=False, cancel=None):
        # Cache changes invalidate presence, but keep same-hash uploads serialized.
        assets=set(assets)
        if not assets:return
        with node.asset_lock:
            session = (node.health.get('pid'), node.health.get('root'), node.health.get('asset_revision'))
            if node.asset_session != session:
                node.known_assets.clear()
                node.asset_session = session
            if force:node.known_assets.difference_update(assets)
            # Pending callers keep strong references; idle locks disappear.
            locks=node.__dict__.setdefault('_asset_upload_locks',weakref.WeakValueDictionary())
            pending=[(asset,locks.setdefault(asset,threading.Lock())) for asset in assets-node.known_assets]
        for asset,transfer in pending:
            _check_cancelled(cancel)
            while not transfer.acquire(timeout=.1):
                _check_cancelled(cancel)
            try:
                _check_cancelled(cancel)
                with node.asset_lock:
                    if node.asset_session==session and asset in node.known_assets:continue
                def abandoned():
                    if cancel is None or not cancel():return False
                    # Other accepted jobs can still need this same upload.
                    # Finish their shared transfer while cancelling its owner
                    # before job submission; cancelled waiters leave the lock.
                    with self.condition:
                        return not any(job.get('node') is node and not job['cancel'].is_set()
                                       and asset in job.get('task',{}).get('assets',{}).values()
                                       for job in active_values(self.jobs))
                if not node.client.request('/assets/' + asset,cancel=abandoned)['exists']:
                    node.client.request('/assets/' + asset, (self.root/'assets'/(asset+'.zip')).read_bytes(),cancel=abandoned)
                with node.asset_lock:
                    if node.asset_session==session:node.known_assets.add(asset)
                _check_cancelled(cancel)
            finally:
                transfer.release()

    def _update(self, job, remote):
        clean_tool_snapshot(remote)
        if remote.get('status') in TERMINAL and remote['status']!='interrupted':
            job['result'].pop('remote_cancel_pending',None)
            job['result'].pop('cost_incomplete',None)
        if 'resource_request' not in remote:job['result'].pop('resource_request',None)
        submitted = job["result"].get("center_submitted", job["result"]["submitted"])
        assigned = {key: job["result"][key] for key in ('id',"assigned_node", "worker_url", "dispatched", 'request_sha256') if key in job["result"]}
        assigned.pop('resource_request',None)
        if job.get('task'):
            assigned['cpus']=job['task']['cpus']
            assigned['memory_mb']=job['task']['memory_mb']
            assigned['accelerator']=job['task'].get('accelerator')
        job["result"].update(remote, **assigned, center_submitted=submitted)
        if remote["status"] in TERMINAL:
            job["result"]["total_elapsed"] = time.time() - submitted
            self._save(job)

    def _callback(self, node):
        if not node.client.url.startswith('local://'):
            return {'mode':'poll','timeout':node.client.timeout}
        if not self.callback_address:return None
        host,port=self.callback_address[:2]
        host='127.0.0.1' if host=='0.0.0.0' else host
        return dict(url=f'http://{host}:{port}',token=self.token,timeout=node.client.timeout)

    def _cooperative_cpu_target(self, identity, desired, resources):
        """Share a project's node budget at a drained automatic phase boundary.

        Running peers keep their grants until their own boundary. Fixed-width
        ready work is placed whole before dividing the remaining automatic work.
        """
        current=self.jobs[identity];group=current['task'].get('project') or 'default'
        resource=resources[group];priority=current['task'].get('priority',0)
        members={key:job for key,job in active_items(self.jobs)
                 if job['result']['status'] not in TERMINAL and not job['cancel'].is_set()
                 and (job['task'].get('project') or 'default')==group}
        nodes={node.name:node for node in self.pool.nodes
               if node.enabled and not node.error and not node.health.get('paused')}
        capacities={}
        for name,node in nodes.items():
            held=sum(job['task']['cpus'] for job in members.values() if job.get('node') is node)
            capacities[name]=max(0,min(resource['node_targets'].get(name,0),
                node.capacity-node.external-node.reserved+held))
        fixed=[];demands={};rooms={}
        for key,job in members.items():
            task=job['task'];held=job.get('node')
            automatic=key==identity or job.get('auto_cpus')
            if held is not None and (not automatic or task.get('priority',0)<priority):
                if held.name in capacities:capacities[held.name]=max(0,capacities[held.name]-task['cpus'])
                continue
            if task.get('priority',0)<priority:continue
            wanted={**task,'cpus':0} if automatic else task
            eligible=[]
            for node in self.pool._matching(wanted):
                if node.name not in capacities or (held is not None and held is not node):continue
                if held is None:
                    if task['memory_mb']>max(0,min(node.memory_free,self.pool._host_memory_free(node))):continue
                    if task.get('accelerator') and node._select_accelerator(task['accelerator']) is None:continue
                eligible.append(node.name)
            if not eligible:continue
            if not automatic:
                fixed.append((key,task,eligible,job['result'].get('submitted',0)))
                continue
            demand=desired if key==identity else job.get('desired_cpus',max(nodes[n].capacity for n in eligible))
            demands[key]=min(demand,task.get('max_cpus',demand))
            rooms[key]={name:min(demands[key],nodes[name].capacity) for name in eligible}
        for key,task,eligible,submitted in sorted(fixed,key=lambda row:(-row[1].get('priority',0),len(row[2]),row[3])):
            choices=[name for name in eligible if capacities[name]>=task['cpus']]
            if choices:
                selected=max(choices,key=lambda name:capacities[name])
                capacities[selected]-=task['cpus']
        plan=weighted_node_threads(capacities,{key:1. for key in demands},demands,rooms)
        return max(1,plan.get(identity,{}).get(current['node'].name,0))

    def resize(self, identity, cpus, *, memory_mb=None, base_memory_mb=None, memory_per_cpu_mb=None, memory_limit_mb=None):
        if type(cpus) is not int or cpus<0:
            raise ValueError('resize cpus must be nonnegative; 0 uses the current node budget')
        if memory_mb is not None and (type(memory_mb) is not int or memory_mb<1):
            raise ValueError('resize memory_mb must be a positive integer in MiB')
        with self.condition:
            resize_lock=self.jobs[identity].setdefault('resize_lock',threading.Lock())
        # A slow worker must not hold up health, admission or other tasks.
        with resize_lock:
            with self.condition,self.pool.condition:
                job=self.jobs[identity];node=job.get('node')
                if self.stopping or job['cancel'].is_set() or node is None or job['result']['status'] in TERMINAL:
                    raise Conflict('Only a running task can change its CPU grant')
                old=job['task']['cpus']
                old_memory=job['task']['memory_mb']
                modeled=base_memory_mb is not None or memory_per_cpu_mb is not None or memory_limit_mb is not None
                capabilities=node.health.get('capabilities',[])
                if 'resize' not in capabilities or ((memory_mb is not None or modeled) and 'resize_memory' not in capabilities):
                    return dict(id=identity,node=node.name,cpus=old,memory_mb=old_memory,requested_cpus=cpus,supported=False)
                desired=min(cpus or node.capacity,job['task'].get('max_cpus',node.capacity))
                desired,_=plan_resize(desired,memory_mb,base_memory_mb,memory_per_cpu_mb,memory_limit_mb,node.memory_capacity)
                if desired<1:raise Conflict('Insufficient RAM for one phase worker; existing grant retained')
                previous_desired=job.get('desired_cpus')
                previous_memory=job.get('phase_memory')
                job['desired_cpus']=desired
                granted=desired
                cooperative=cpus==0 or job.get('auto_cpus',False)
                # Automatic jobs may report a finite useful phase width.
                # Repeating that width is still a drained boundary; returning
                # to one CPU stays cheap for synchronous microkernels.
                if desired>old or cpus==0 or (cooperative and desired>1):
                    resources=self.project_resources();group=job['task'].get('project') or 'default'
                    target=resources[group]['target']
                    room=target-resources[group]['used']+old
                    available=node.capacity-node.reserved-node.external+old
                    floor=1 if cooperative else old
                    granted=max(floor,min(desired,room,available)) if node.enabled and not node.health.get('paused') else old
                    if cooperative and node.enabled and not node.health.get('paused'):
                        granted=min(granted,self._cooperative_cpu_target(identity,desired,resources))
                granted,memory=plan_resize(granted,memory_mb,base_memory_mb,memory_per_cpu_mb,memory_limit_mb,
                    old_memory+max(0,min(node.memory_free,self.pool._host_memory_free(node))))
                memory=old_memory if memory is None else memory
                extra=max(0,memory-old_memory)
                if granted<1 or memory>node.memory_capacity or (extra and extra>min(node.memory_free,self.pool._host_memory_free(node))):
                    if previous_desired is None:job.pop('desired_cpus',None)
                    else:job['desired_cpus']=previous_desired
                    raise Conflict('Insufficient memory for resource resize; existing grant retained')
                job['phase_memory']=(dict(base_memory_mb=base_memory_mb,memory_per_cpu_mb=memory_per_cpu_mb,
                                         memory_limit_mb=memory_limit_mb) if modeled else
                                     dict(memory_mb=old_memory if memory_mb is None else memory_mb))
                if granted==old and memory==old_memory and not job.get('resize_uncertain'):
                    return dict(id=identity,node=node.name,cpus=old,memory_mb=old_memory,requested_cpus=cpus)
                # Reserve increases before the RPC; return decreases after its ack.
                held,held_memory=max(old,granted),max(old_memory,memory)
                node.reserved+=held-old
                node.reserved_memory_mb+=held_memory-old_memory
                job['task']['cpus']=job['result']['cpus']=held
                job['task']['memory_mb']=job['result']['memory_mb']=held_memory

            def commit(actual,actual_memory):
                with self.condition,self.pool.condition:
                    # Completion may have released this task while the RPC waited.
                    if job.get('worker_id',identity) in node.live_jobs:
                        node.reserved+=actual-job['task']['cpus']
                        node.reserved_memory_mb+=actual_memory-job['task']['memory_mb']
                    job['task']['cpus']=job['result']['cpus']=actual
                    job['task']['memory_mb']=job['result']['memory_mb']=actual_memory
                    if self.jobs.get(identity) is job:self._save(job)
                    self.pool.condition.notify_all()
                    self.condition.notify_all()

            try:
                worker_id=job.get('worker_id',identity)
                result=(node.client.resize(worker_id,granted,memory_mb=memory) if memory_mb is not None or modeled
                        else node.client.resize(worker_id,granted))
            except RemoteError as error:
                if getattr(error,'status_code',None) is not None:
                    with self.condition:
                        if previous_desired is None:job.pop('desired_cpus',None)
                        else:job['desired_cpus']=previous_desired
                        if previous_memory is None:job.pop('phase_memory',None)
                        else:job['phase_memory']=previous_memory
                    commit(old,old_memory)
                    raise
                try:
                    observed=node.client.status(job.get('worker_id',identity))
                except RemoteError:
                    # The worker may have accepted the increase. Keep its held
                    # budget until an acknowledged resize or normal completion.
                    with self.condition:job['resize_uncertain']=True
                    raise
                result=dict(id=identity,cpus=observed['cpus'],memory_mb=observed['memory_mb'])
            with self.condition:job.pop('resize_uncertain',None)
            commit(result['cpus'],result.get('memory_mb',old_memory))
            return dict(result,id=identity,node=node.name,requested_cpus=cpus)

    def _client(self, job):
        if job.get("node"):
            return job["node"].client
        assigned = job["result"].get("assigned_node")
        return next(n.client for n in self.pool.nodes if n.name == assigned or assigned in getattr(n,'aliases',())
                    or n.client.url == job['result'].get('worker_url')) if assigned else None

    def get(self, identity):
        with self.condition:
            job = self.jobs[identity]
            return json.loads(json_bytes(job["result"]))

    def public_get(self, identity):
        return self.public_result(self.get(identity))

    def logs(self, identity):
        with self.condition:
            job = self.jobs[identity]
            client = self._client(job)
        if client:
            try:
                return client.logs(job.get('worker_id',identity))
            except RemoteError as error:
                if getattr(error, 'status_code', None) != 404 or job['result']['status'] != 'dispatching':
                    raise Conflict(str(error)) from None
        return {"stdout": "", "stderr": ""}

    def job_file(self, identity, name, data=None):
        with self.condition:
            job=self.jobs[identity];client = self._client(job)
        if not client:
            raise Conflict('Job has not been assigned yet')
        try:
            return client.read_job_file(job.get('worker_id',identity), name) if data is None else client.write_job_file(job.get('worker_id',identity), name, data)
        except RemoteError as error:
            if getattr(error, 'status_code', None) == 404:
                raise FileNotFoundError(name) from None
            raise Conflict(str(error)) from None

    def job_file_version(self, identity, name, version=None):
        with self.condition:
            job=self.jobs[identity];client=self._client(job);worker_id=job.get('worker_id',identity)
        if not client:raise Conflict('Job has not been assigned yet')
        from .local import LocalClient
        if isinstance(client,LocalClient):return client.worker.job_file_version(worker_id,name,version)
        try:
            return client.read_job_file_changed(worker_id,name,version)
        except RemoteError as error:
            if getattr(error,'status_code',None)==404:raise FileNotFoundError(name) from None
            if getattr(error,'status_code',None)==409:raise Conflict(str(error)) from None
            raise

    def cancel(self, identity):
        with self.condition:
            job = self.jobs[identity]
            if job["result"]["status"] not in TERMINAL:
                job["cancel"].set()
                if identity in self.queue:
                    self.queue.remove(identity)
                    job["result"].update(status="cancelled", finished=time.time())
                    self._save(job)
                    archive_job(self.jobs, job)
                elif job.get('recover') and job.get('node') is None:
                    # No worker acknowledgement has been recovered. Keep its
                    # original ID and cancellation intent for later cleanup.
                    job['result'].update(status='interrupted',finished=time.time(),
                        error='Cancellation pending before original worker reattachment',
                        remote_cancel_pending=True,cost_incomplete=True)
                    self._save(job)
                    archive_job(self.jobs,job)
                self.condition.notify_all()
            return self.get(identity)

    def artifacts(self, identity, *, is_connected=None, public=False):
        with self.condition:
            job = self.jobs[identity]
            if job["result"]["status"] not in TERMINAL:
                raise Conflict("Artifacts are available when the job has finished")
            result, client = self.get(identity), self._client(job)
            if result.get('cancelled_before_submit'):client=None
            node=job.get('node') or next((n for n in self.pool.nodes if n.client is client),None)
            if result['status']=='interrupted' and node is not None and (node.detached or not node.enabled):
                result['cost_incomplete']=True
                client=None
            worker_id=job.get('worker_id',result.get('worker_id',identity))
            result_lock=job.setdefault('artifact_lock',threading.RLock())
            transfers=self.__dict__.setdefault('_artifact_transfers',{})
            transfer_lock=transfers.setdefault(client.url,threading.Lock()) if client else None
        with result_lock:
            with self.condition:
                if self.jobs.get(identity) is not job:
                    raise KeyError(identity)
            files = {}
            if client:
                from .local import LocalClient
                if isinstance(client, LocalClient) and result['status'] != 'interrupted':
                    return client.worker.artifacts(worker_id, result=self.public_result(result) if public else result)
                try:
                    if is_connected and not is_connected():
                        raise ConnectionAbortedError('Result download caller disconnected while queued')
                    cached=job['directory']/'worker-artifacts.zip'
                    if cached.is_file():
                        raw=cached.read_bytes()
                    else:
                        while not transfer_lock.acquire(timeout=.2):
                            if is_connected and not is_connected():
                                raise ConnectionAbortedError('Result download caller disconnected while queued')
                        try:
                            if is_connected and not is_connected():
                                raise ConnectionAbortedError('Result download caller disconnected while queued')
                            raw=client.artifacts(worker_id)
                        finally:
                            transfer_lock.release()
                        with zipfile.ZipFile(io.BytesIO(raw)) as archive:
                            if json.loads(archive.read('result.json'))['id']!=worker_id:
                                raise ValueError('Artifact task identity mismatch')
                        temporary=cached.with_suffix('.partial')
                        try:
                            temporary.write_bytes(raw)
                            temporary.replace(cached)
                        finally:
                            unlink(temporary)
                except RemoteError as error:
                    if getattr(error, 'status_code', None) != 404 or result['status'] != 'interrupted':
                        raise
                else:
                    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
                        files = {name: archive.read(name) for name in archive.namelist() if name != "result.json"}
                        if result['status']=='interrupted':
                            remote=json.loads(archive.read('result.json'))
                            with self.condition:
                                job['result'].pop('error',None)
                                job['result'].pop('connection_error',None)
                                self._update(job,remote)
                                result=self.get(identity)
            files["result.json"] = json_bytes(self.public_result(result) if public else result)
            return pack(files)

    def delete(self, identity):
        with self.condition:
            job = self.jobs[identity]
            if job["result"]["status"] not in TERMINAL:
                raise Conflict("Cancel and wait before deleting the job")
            client = self._client(job)
            result_lock=job.setdefault('artifact_lock',threading.RLock())
        with result_lock:
            with self.condition:
                if self.jobs.get(identity) is not job:
                    return {"deleted": identity}
            if client:
                try:
                    if job['result'].get('remote_cancel_pending'):
                        client.cancel(job.get('worker_id',job['result'].get('worker_id',identity)))
                    client.delete(job.get('worker_id',job['result'].get('worker_id',identity)))
                except RemoteError as error:
                    if getattr(error, 'status_code', None) != 404:
                        raise
            shutil.rmtree(job["directory"])
            with self.condition:
                del self.jobs[identity]
        self._prune_assets()
        return {"deleted": identity}

    def _prune_assets(self, *, recent=True):
        with self.asset_cache.lock:
            with self.condition:
                # Legacy callers have no producer identity. Keep their uploads
                # until shutdown; an arbitrary delay is not a transfer of ownership.
                protected = set(self.legacy_assets) if recent else set()
                now = time.monotonic()
                for identity, (assets, expires) in tuple(self.asset_handoffs.items()):
                    if now >= expires:
                        self.asset_handoffs.pop(identity, None)
                    else:
                        protected.update(assets)
                protected.update(asset_references(self.jobs))
            for error in self.asset_cache.prune(protected):
                self.activity.notify('资产缓存待清理 '+error, 'local')

    def asset_handoff(self, identity, assets=None):
        # The same original-ID handoff is understood by center and worker.
        value = asset_handoff(self, identity, assets)
        if assets is None:
            self._prune_assets()
        return value

    def asset(self, identity, payload=None, *, job_id=None):
        if not _ASSET.fullmatch(identity):
            raise ValueError("Asset id requires SHA256")
        if job_id is not None and not _ID.fullmatch(job_id):
            raise ValueError('Job id requires 32 lowercase hex characters')
        path = self.root / "assets" / (identity + ".zip")
        with self.asset_cache.lock:
            if payload is not None and not path.is_file():
                if hashlib.sha256(payload).hexdigest() != identity:
                    raise ValueError("Asset digest does not match bundle")
                temporary = path.with_suffix(".upload")
                try:
                    temporary.write_bytes(payload)
                    temporary.replace(path)
                finally:
                    unlink(temporary)
            if path.is_file():
                self.asset_cache.touch(identity)
                if job_id is None:
                    self.legacy_assets.add(identity)
            if job_id is not None:
                with self.condition:
                    held = self.asset_handoffs.get(job_id, (set(), 0))[0]
                self.asset_handoff(job_id, list(set(held) | {identity}))
            if payload is not None:
                self._prune_assets()
            return {"id": identity, "exists": path.is_file()}

    def close(self):
        with self.condition:
            self.stopping = True
            for identity, _job in list(active_items(self.jobs)):
                self.cancel(identity)
            self.condition.notify_all()
        self.scheduler.join()
        for thread in list(self.threads):
            thread.join()
        self.asset_handoffs.clear()
        self._prune_assets(recent=False)
        self.root_lock.close()
