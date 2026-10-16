"""A central HTTP endpoint forwarding accepted jobs to the shared worker pool."""

import hashlib
import io
import json
from pathlib import Path
import shutil
import threading
import time
import zipfile

from .bundle import PROTOCOL, TERMINAL, atomic_json, json_bytes, pack
from .client import RemoteError
from .worker import Conflict, _ASSET, _ID, lock_root, validate_task
from .telemetry import Activity
from .allocation import weighted_threads
import socket
import math


class Coordinator:
    def __init__(self, pool, root, *, token=""):
        self.pool, self.token = pool, token
        self.activity = Activity()
        self.root = Path(root).resolve()
        self.root_lock = lock_root(self.root, "scheduler")
        (self.root / "jobs").mkdir(parents=True, exist_ok=True)
        (self.root / "assets").mkdir(exist_ok=True)
        self.condition = threading.Condition(threading.RLock())
        self.jobs, self.queue, self.threads = {}, [], set()
        self.stopping = self.paused = False
        for path in (self.root / "jobs").glob("*/result.json"):
            value = json.loads(path.read_text(encoding="utf-8"))
            if value["status"] not in TERMINAL:
                value.update(status="interrupted", error="Coordinator restarted; original worker reference retained")
                atomic_json(path, value)
            self.jobs[value["id"]] = {"result": value, "directory": path.parent,
                                      "cancel": threading.Event(), "node": None}
        self.scheduler = threading.Thread(target=self._schedule, name="central-dispatch", daemon=True)
        self.scheduler.start()

    def nodes(self):
        return self.pool.snapshot()

    def admission(self, request):
        project = request.get('project') or 'default'
        share = request.get('resource_share', 1.0)
        ratio = request.get('queue_ratio', 1.0)
        cpus = request.get('cpus', 1)
        requires = request.get('requires', [])
        if (not isinstance(project, str) or isinstance(share, bool) or not isinstance(share, (int,float))
                or not math.isfinite(share) or share <= 0 or isinstance(ratio, bool)
                or not isinstance(ratio, (int,float)) or not math.isfinite(ratio) or ratio < 0
                or type(cpus) is not int or cpus < 0 or not isinstance(requires,list)
                or any(not isinstance(tag,str) for tag in requires)):
            raise ValueError('project, positive resource_share, nonnegative queue_ratio and cpus required')
        resources = self.project_resources()
        capacity = sum(n.capacity for n in self.pool.nodes if not n.error and n.enabled)
        shares = {name: row['resource_share'] for name,row in resources.items()}
        demands = {name: row['demand'] for name,row in resources.items()}
        shares[project], demands[project] = share, capacity
        target = weighted_threads(capacity, shares, demands)[project]
        capable = sum(n.capacity for n in self.pool.nodes if not n.error and n.enabled
                      and set(requires).issubset(n.health.get('tags',())))
        target = min(target, capable)
        ready = max(1, math.ceil(target / max(1, cpus))) if cpus else 1
        return dict(project=project,resource_share=share,queue_ratio=ratio,target_cpus=target,
                    running_tasks=ready,queued_tasks=math.ceil(ready*ratio),
                    in_flight=ready+math.ceil(ready*ratio))

    def restart_node(self, name):
        node = next((n for n in self.pool.nodes if n.name == name), None)
        if node is None:
            raise ValueError("Unknown node: " + str(name))
        try:
            return {"node": name, **node.client.restart()}
        except RemoteError as error:
            raise Conflict(str(error)) from None

    def health(self):
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
            return {"protocol": PROTOCOL, "role": "scheduler", "node": socket.gethostname(), "tools": tools,
                    "cpus": sum(n.capacity for n in online), "used_cpus": sum(n.reserved + n.external for n in online),
                    "logical_cpus": sum(group[0].health.get("logical_cpus", 0) for group in machines.values()),
                    "memory_total_mb": sum(group[0].health.get("memory_total_mb", 0) for group in machines.values()),
                    "memory_capacity_mb": sum(min(sum(n.memory_capacity for n in group),
                                                   group[0].health.get("memory_total_mb", 0)) for group in machines.values()),
                    "memory_available_mb": sum(max(0, min(self.pool._host_memory_free(group[0]),
                                                          sum(n.memory_free for n in group))) for group in machines.values()),
                    "memory_reserved_mb": sum(n.reserved_memory_mb + n.external_memory_mb for n in online),
                    "paused": self.paused, "queued": len(self.queue), "tags": sorted(tags),
                    "policy": snapshot["policy"], "nodes": snapshot["nodes"],
                    'projects': self.project_resources(), **self.activity.snapshot()}

    def project_resources(self):
        shares, demands, used, queued = {}, {}, {}, {}
        capacity = sum(n.capacity for n in self.pool.nodes if not n.error and n.enabled)
        largest = max((n.capacity for n in self.pool.nodes if not n.error and n.enabled), default=1)
        with self.condition:
            for job in self.jobs.values():
                if job['result']['status'] in TERMINAL:
                    continue
                task = job.get('task', {})
                name = task.get('project') or 'default'
                shares[name] = task.get('resource_share', 1.0)
                demands[name] = demands.get(name, 0) + (largest if job.get('auto_cpus') else task.get('cpus', 1))
                used[name] = used.get(name, 0) + (task.get('cpus', 0) if job.get('node') else 0)
                queued[name] = queued.get(name, 0) + int(job['result']['status'] == 'queued')
        quotas = weighted_threads(capacity, shares, demands)
        return {name: dict(resource_share=shares[name], target=quotas[name], demand=demands[name], used=used[name], queued=queued[name]) for name in shares}

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
                self.pool.refresh()
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

    def submit(self, identity, payload):
        if not _ID.fullmatch(identity):
            raise ValueError("Job id requires 32 lowercase hex characters")
        digest = hashlib.sha256(payload).hexdigest()
        with self.condition:
            if self.stopping:
                raise Conflict("Scheduler is stopping")
            if identity in self.jobs:
                saved = self.jobs[identity]["result"]
                if saved["request_sha256"] != digest:
                    raise Conflict("Job id already contains a different request")
                return dict(saved)
            with zipfile.ZipFile(io.BytesIO(payload)) as archive:
                task = validate_task(json.loads(archive.read("task.json")),
                                     max((n.capacity for n in self.pool.nodes), default=1), allow_auto=True)
            directory = self.root / "jobs" / identity
            directory.mkdir()
            (directory / "request.zip").write_bytes(payload)
            job = {"directory": directory, "task": task, "auto_cpus": task['cpus'] == 0, "cancel": threading.Event(), "node": None,
                   "result": {"id": identity, "status": "queued", "label": task["label"], "cpus": task["cpus"],
                              "memory_mb": task["memory_mb"], "request_sha256": digest,
                              "submitted": time.time(), "started": None, "finished": None,
                              "stdout": "", "stderr": "", "steps": [], "assets": task["assets"]}}
            self._save(job)
            self.jobs[identity] = job
            self.queue.append(identity)
            self.queue.sort(key=lambda key: -self.jobs[key]["task"]["priority"])
            self.condition.notify_all()
            return dict(job["result"])

    def _schedule(self):
        with self.condition:
            while not self.stopping:
                if not self.paused:
                    budgets = self.project_resources()
                    reservations = {}
                    ordered = sorted(self.queue, key=lambda key: (-self.jobs[key]['task']['priority'],
                                     self.jobs[key]['task']['cpus'] != 0))
                    # First honor weighted targets, then lend otherwise idle slots.
                    for identity, borrowing in [(key, False) for key in ordered] + [(key, True) for key in ordered]:
                        if identity not in self.queue:
                            continue
                        job = self.jobs[identity]
                        group = job['task'].get('project') or 'default'
                        budget = budgets[group]
                        room = budget['target'] - budget['used']
                        if not borrowing and (room <= 0 or (job['task']['cpus'] or 1) > room):
                            continue
                        if borrowing and job['auto_cpus'] and room <= 0 and reservations:
                            continue
                        try:
                            node = self.pool._acquire(job["task"], block=False,
                                cpu_limit=room if room > 0 else None, reservations=reservations)
                        except (RuntimeError, ValueError) as error:
                            job["result"].update(status="failed", error=str(error), finished=time.time())
                            self._save(job)
                            self.queue.remove(identity)
                            continue
                        if node is None:
                            continue
                        self.queue.remove(identity)
                        job["node"] = node
                        budget['used'] += job['task']['cpus']
                        job["result"].update(status="dispatching", assigned_node=node.name, worker_url=node.client.url,
                                             dispatched=time.time(), cpus=job['task']['cpus'])
                        self._save(job)
                        thread = threading.Thread(target=self._execute, args=(job,), name="central-job", daemon=True)
                        self.threads.add(thread)
                        thread.start()
                self.condition.wait(0.05)

    def _execute(self, job):
        node, identity, task = job["node"], job["result"]["id"], job["task"]
        result = None
        try:
            for asset in task["assets"].values():
                if not node.client.request("/assets/" + asset)["exists"]:
                    node.client.request("/assets/" + asset, (self.root / "assets" / (asset + ".zip")).read_bytes())
            with zipfile.ZipFile(job['directory'] / 'request.zip') as archive:
                payload = {name: archive.read(name) for name in archive.namelist()}
            payload['task.json'] = json_bytes(task)
            node.client.submit_bundle(identity, pack(payload))
            while True:
                if job["cancel"].is_set():
                    node.client.cancel(identity)
                remote = node.client.status(identity)
                with self.condition:
                    self._update(job, remote)
                if remote["status"] in TERMINAL:
                    result = remote
                    break
                job["cancel"].wait(0.15 if time.time() - job["result"]["dispatched"] < 2 else 0.2)
        except Exception as error:
            with self.condition:
                job["result"].update(status="interrupted", error=str(error), finished=time.time())
                self._save(job)
        finally:
            self.pool._release(node, task, result)
            with self.condition:
                self.threads.discard(threading.current_thread())
                self.condition.notify_all()

    def _update(self, job, remote):
        submitted = job["result"].get("center_submitted", job["result"]["submitted"])
        assigned = {key: job["result"][key] for key in ("assigned_node", "worker_url", "dispatched") if key in job["result"]}
        job["result"].update(remote, **assigned, center_submitted=submitted)
        if remote["status"] in TERMINAL:
            job["result"]["total_elapsed"] = time.time() - submitted
            self._save(job)

    def _client(self, job):
        if job.get("node"):
            return job["node"].client
        assigned = job["result"].get("assigned_node")
        return next(n.client for n in self.pool.nodes if n.name == assigned
                    or n.client.url == job['result'].get('worker_url')) if assigned else None

    def get(self, identity):
        with self.condition:
            job = self.jobs[identity]
            return json.loads(json_bytes(job["result"]))

    def logs(self, identity):
        with self.condition:
            job = self.jobs[identity]
            client = self._client(job)
        if client:
            try:
                return client.logs(identity)
            except RemoteError as error:
                if getattr(error, 'status_code', None) != 404 or job['result']['status'] != 'dispatching':
                    raise Conflict(str(error)) from None
        return {"stdout": "", "stderr": ""}

    def job_file(self, identity, name, data=None):
        with self.condition:
            client = self._client(self.jobs[identity])
        if not client:
            raise Conflict('Job has not been assigned yet')
        try:
            return client.read_job_file(identity, name) if data is None else client.write_job_file(identity, name, data)
        except RemoteError as error:
            if getattr(error, 'status_code', None) == 404:
                raise FileNotFoundError(name) from None
            raise Conflict(str(error)) from None

    def cancel(self, identity):
        with self.condition:
            job = self.jobs[identity]
            if job["result"]["status"] not in TERMINAL:
                job["cancel"].set()
                if identity in self.queue:
                    self.queue.remove(identity)
                    job["result"].update(status="cancelled", finished=time.time())
                    self._save(job)
                self.condition.notify_all()
            return self.get(identity)

    def artifacts(self, identity):
        with self.condition:
            job = self.jobs[identity]
            if job["result"]["status"] not in TERMINAL:
                raise Conflict("Artifacts are available when the job has finished")
            result, client = self.get(identity), self._client(job)
        files = {}
        if client:
            try:
                raw = client.request("/jobs/" + identity + "/artifacts", binary=True)
            except RemoteError as error:
                if getattr(error, 'status_code', None) != 404 or result['status'] != 'interrupted':
                    raise
            else:
                with zipfile.ZipFile(io.BytesIO(raw)) as archive:
                    files = {name: archive.read(name) for name in archive.namelist() if name != "result.json"}
        files["result.json"] = json_bytes(result)
        return pack(files)

    def delete(self, identity):
        with self.condition:
            job = self.jobs[identity]
            if job["result"]["status"] not in TERMINAL:
                raise Conflict("Cancel and wait before deleting the job")
            client = self._client(job)
            if client:
                try:
                    client.delete(identity)
                except RemoteError as error:
                    if getattr(error, 'status_code', None) != 404:
                        raise
            shutil.rmtree(job["directory"])
            del self.jobs[identity]
            return {"deleted": identity}

    def asset(self, identity, payload=None):
        if not _ASSET.fullmatch(identity):
            raise ValueError("Asset id requires SHA256")
        path = self.root / "assets" / (identity + ".zip")
        with self.condition:
            if payload is not None and not path.is_file():
                if hashlib.sha256(payload).hexdigest() != identity:
                    raise ValueError("Asset digest does not match bundle")
                temporary = path.with_suffix(".upload")
                temporary.write_bytes(payload)
                temporary.replace(path)
            return {"id": identity, "exists": path.is_file()}

    def close(self):
        with self.condition:
            self.stopping = True
            for identity in list(self.jobs):
                self.cancel(identity)
            self.condition.notify_all()
        self.scheduler.join()
        for thread in list(self.threads):
            thread.join()
        self.root_lock.close()
