"""HTTP client and weighted, adaptive dispatch across independent workers."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
import hashlib
import gzip
import http.client
import io
import json
from pathlib import Path
import re
import threading
import time
from urllib.parse import quote
from urllib.parse import urlsplit
import uuid
import zipfile

from .bundle import PROTOCOL, TERMINAL, json_bytes, pack, task_bundle, tree, unpack


class RemoteError(RuntimeError):
    pass


class _ConnectionOwner:
    def __init__(self, connection):
        self.connection = connection

    def __del__(self):
        self.connection.close()


class Client:
    def __init__(self, url="http://127.0.0.1:26133", token="", *, timeout=10):
        self.url, self.token, self.timeout = url.rstrip("/"), token, timeout
        self.endpoint = urlsplit(self.url)
        self.connections = threading.local()
        self._capabilities = None

    def request(self, path, data=None, *, method=None, binary=False, brief=False, extra_headers=None):
        if self.endpoint.scheme not in {'http', 'https'}:
            raise RemoteError('Private worker is available only inside its owning hybrid scheduler')
        headers = {"Content-Type": "application/zip" if isinstance(data, bytes) else "application/json",
                   'Accept-Encoding': 'gzip'}
        if self.token:
            headers["Authorization"] = "Bearer " + self.token
        if brief:
            headers['X-Remote-Compute-Brief'] = '1'
        headers.update(extra_headers or {})
        if data is not None and not isinstance(data, bytes):
            data = json_bytes(data)
        method = method or ('POST' if data is not None else 'GET')
        for attempt in range(2):
            connection = getattr(self.connections, 'connection', None)
            if connection is None:
                factory = http.client.HTTPSConnection if self.endpoint.scheme == 'https' else http.client.HTTPConnection
                connection = factory(self.endpoint.hostname, self.endpoint.port, timeout=self.timeout)
                self.connections.connection = connection
                self.connections.owner = _ConnectionOwner(connection)
            connection.timeout = self.timeout
            if connection.sock is not None:
                connection.sock.settimeout(self.timeout)
            try:
                connection.request(method, self.endpoint.path.rstrip('/') + path, body=data, headers=headers)
                response = connection.getresponse()
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
                if method != 'GET' or attempt:
                    raise RemoteError(f'{self.url}{path}: {exc}') from None
                time.sleep(.1)
        return raw if binary else json.loads(raw)

    def health(self):
        result = self.request("/health")
        if result.get("protocol") != PROTOCOL:
            raise RemoteError("Worker protocol differs: " + self.url)
        self._capabilities = result.get('capabilities',[])
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
                                extra_headers={'X-Remote-Compute-Callback':json.dumps(callback)})
        return self.request("/jobs/" + identity, payload)

    def resize(self, identity, cpus, *, memory_mb=None):
        """Borrow/return resources at a safe boundary; use the actual returned grant."""
        if memory_mb is not None:
            if self._capabilities is None:
                self.health()
            if 'resize_memory' not in self._capabilities:
                state=self.status(identity,brief=True)
                return dict(id=identity,node=state.get('assigned_node',state.get('node')),
                            cpus=state['cpus'],memory_mb=state['memory_mb'],requested_cpus=cpus,supported=False)
        request = dict(cpus=cpus)
        if memory_mb is not None:
            request['memory_mb'] = memory_mb
        return self.request('/jobs/'+identity+'/resize', request)

    def nodes(self):
        return self.request("/nodes")

    def admission(self, project, *, resource_share=1.0, queue_ratio=1.0, cpus=1, requires=(), node=None, max_cpus=None):
        """Center-owned transport window; execution slots stay in the central queue."""
        request=dict(project=project,resource_share=resource_share,queue_ratio=queue_ratio,cpus=cpus,requires=list(requires))
        if node:
            request['node']=node
        if max_cpus is not None:
            request['max_cpus'] = max_cpus
        return self.request('/admission',request)

    def window(self, project, **options):
        from .admission import AdmissionWindow
        return AdmissionWindow(self,project,**options)

    def machine(self, node, *, cwd="~"):
        """Operate on a named worker through this same center connection."""
        from .machine import Machine
        return Machine(self, node, cwd=cwd)

    def status(self, identity=None, *, brief=False):
        return self.request("/jobs" + ("/" + identity if identity else ""), brief=brief)

    def cancel(self, identity):
        return self.request("/jobs/" + identity + "/cancel", {})

    def logs(self, identity):
        return self.request("/jobs/" + identity + "/logs")

    def read_job_file(self, identity, path):
        return self.request('/jobs/' + identity + '/files/' + quote(str(path), safe='/'), binary=True)

    def write_job_file(self, identity, path, data):
        if isinstance(data, str):
            data = data.encode('utf-8')
        return self.request('/jobs/' + identity + '/files/' + quote(str(path), safe='/'), data)

    def delete(self, identity):
        return self.request("/jobs/" + identity, method="DELETE")

    def wait(self, identity, *, poll=0.15, wait_timeout=None):
        started = time.monotonic()
        while True:
            result = self.status(identity, brief=True)
            if result["status"] in TERMINAL:
                result = self.status(identity)
                result["url"] = self.url
                return result
            if wait_timeout is not None and time.monotonic() - started >= wait_timeout:
                raise RemoteError(f"Wait limit reached; query {self.url}/jobs/{identity}")
            time.sleep(poll)

    def fetch(self, identity, output_dir=None, *, delete=True):
        raw = self.request("/jobs/" + identity + "/artifacts", binary=True)
        with zipfile.ZipFile(io.BytesIO(raw)) as archive:
            result = json.loads(archive.read("result.json"))
            result["files"] = {name[6:]: archive.read(name) for name in archive.namelist() if name.startswith("files/")}
        if output_dir is not None:
            unpack(raw, Path(output_dir))
            result["files"] = sorted(result["files"])
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

    def sync_files(self, files):
        payload = pack(files)
        identity = hashlib.sha256(payload).hexdigest()
        if not self.request("/assets/" + identity)["exists"]:
            self.request("/assets/" + identity, payload)
        return identity


@dataclass
class Node:
    name: str
    client: Client
    weight: float = 1.0
    limit: int = 0
    enabled: bool = True
    health: dict = field(default_factory=dict)
    error: str = ""
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
        unreported = max(0, self.reserved_memory_mb - self.health.get("memory_reserved_mb", 0))
        return min(self.memory_capacity - self.reserved_memory_mb - self.external_memory_mb,
                   self.health.get("memory_available_mb", 0) - unreported)


class Pool:
    """Shared admission across concurrent batches; workers enforce the final budget.

    CPU pressure chooses the least-busy host; adaptive uses same-label elapsed
    time for near ties. round_robin gives predictable rotation. Accepted work stays put.
    """
    POLICIES = {"adaptive", "balanced", "round_robin"}

    def __init__(self, nodes, *, policy="adaptive", refresh=2):
        self.condition = threading.Condition(threading.RLock())
        self.refresh_lock = threading.Lock()
        self.nodes = []
        self.policy = policy
        self.refresh_seconds = refresh
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
                            entry.get('client') or Client(entry["url"], entry.get("token", "")),
                            weight=entry.get("weight", 1.0), limit=entry.get("limit", 0),
                            memory_limit_mb=entry.get("memory_limit_mb", 0),
                            enabled=entry.get("enabled", True))
            self._validate_node(node)
            self.nodes.append(node)
        if not self.nodes or len({x.name for x in self.nodes}) != len(self.nodes):
            raise ValueError("Configure nodes with unique names")
        if len({x.client.url for x in self.nodes}) != len(self.nodes):
            raise ValueError("Configure each worker URL once")
        self.refresh()
        self.monitor = threading.Thread(target=self._monitor, name="compute-nodes", daemon=True)
        self.monitor.start()

    @staticmethod
    def _validate_node(node):
        if (not isinstance(node.weight, (int, float)) or not 0 < node.weight < float("inf")
                or type(node.limit) is not int or node.limit < 0 or not isinstance(node.enabled, bool)
                or type(node.memory_limit_mb) is not int or node.memory_limit_mb < 0):
            raise ValueError("Node weight must be positive; limit is 0/auto or a positive integer")

    def refresh(self):
        def probe(node):
            try:
                value, error = node.client.health(), ""
            except RemoteError as exc:
                value, error = {}, str(exc)
            with self.condition:
                node.error = error
                if error:
                    self.condition.notify_all()
                    return
                node.health = value
                jobs = value.get('active_jobs')
                if jobs is None:
                    node.external = max(0, value.get("used_cpus", 0) - node.reserved)
                    node.external_memory_mb = max(0, value.get("memory_reserved_mb", 0) - node.reserved_memory_mb)
                else:
                    running = [job for job in jobs if job['status']=='running']
                    node.external = sum(job['cpus'] for job in running if job['id'] not in node.known_jobs)
                    owned_memory = sum(max(job['memory_mb'],job.get('rss_mb',0)) for job in running
                                       if job['id'] in node.known_jobs)
                    node.external_memory_mb = max(0,value.get('memory_reserved_mb',0)-owned_memory)
                    # Keep completed identities until a fresh worker snapshot
                    # drops them; a slow probe must not resurrect our released work.
                    node.known_jobs.intersection_update({job['id'] for job in jobs} | node.live_jobs)
                self.condition.notify_all()
        with self.refresh_lock, ThreadPoolExecutor(max_workers=len(self.nodes)) as executor:
            list(executor.map(probe, self.nodes))

    def _monitor(self):
        while not self.stop_event.wait(self.refresh_seconds):
            self.refresh()

    def configure(self, name=None, *, policy=None, **changes):
        with self.condition:
            if policy is not None:
                if policy not in self.POLICIES:
                    raise ValueError("Policy: adaptive, balanced, round_robin")
                self.policy = policy
            if name is not None:
                node = next(x for x in self.nodes if x.name == name)
                if set(changes) - {"weight", "limit", "enabled", "memory_limit_mb"}:
                    raise ValueError("Node settings: weight, limit, enabled, memory_limit_mb")
                proposed = Node(node.name, node.client, **{
                    key: changes.get(key, getattr(node, key)) for key in ("weight", "limit", "enabled", "memory_limit_mb")})
                self._validate_node(proposed)
                for key, value in changes.items():
                    setattr(node, key, value)
            self.condition.notify_all()

    def add(self, name, url, *, token="", weight=1.0, limit=0):
        node = Node(name, Client(url, token), weight=weight, limit=limit)
        self._validate_node(node)
        with self.condition:
            if any(n.name == name or n.client.url == node.client.url for n in self.nodes):
                raise ValueError("Node name or URL already exists")
            self.nodes.append(node)
        self.refresh()

    def snapshot(self):
        with self.condition:
            return {"policy": self.policy, "nodes": [
                {"name": n.name, "url": n.client.url, "enabled": n.enabled,
                 "capacity": n.capacity, "reserved": n.reserved, "external_cpus": n.external,
                 "used_cpus": n.health.get("used_cpus", 0), "queued": n.health.get("queued", 0),
                 "cpu_percent": n.health.get("cpu_percent"), "logical_cpus": n.health.get("logical_cpus"),
                 "benchmark": n.health.get("benchmark"), "asset_cache": n.health.get("asset_cache"),
                 "service": n.health.get("service"), "scratch_disk": n.health.get("scratch_disk"),
                 "memory_capacity_mb": n.memory_capacity, "memory_available_mb": max(0, int(n.memory_free)),
                 "memory_reserved_mb": n.health.get("memory_reserved_mb", 0),
                 "reserved_memory_mb": n.reserved_memory_mb, "memory_limit_mb": n.memory_limit_mb,
                 "memory_total_mb": n.health.get("memory_total_mb"),
                 "host_memory_available_mb": n.health.get("host_memory_available_mb"),
                 "machine_id": n.health.get("machine_id", n.client.url), "rss_mb": n.health.get("rss_mb", 0),
                 "paused": n.health.get("paused", False), "tags": n.health.get("tags", []),
                 "capabilities": n.health.get("capabilities", []), "home": n.health.get("home"),
                 "shared_paths": n.health.get("shared_paths", {}),
                 "active_jobs": n.health.get("active_jobs") or [],
                 "unavailable_shared": n.health.get("unavailable_shared", {}),
                 "tool_root": n.health.get("tool_root"),
                 **{key: n.health.get(key) for key in ('node', 'cpu_limit', 'temperature_c', 'host_ip', 'peers', 'events', 'root', 'version')},
                 "weight": n.weight, "limit": n.limit, "completed": n.completed, "failed": n.failed,
                 "seconds_by_label": dict(n.durations), "error": n.error} for n in self.nodes]}

    def _acquire(self, task, *, block=True, cancel_event=None, cpu_limit=None, partial_cpus=False, job_id=None):
        if not isinstance(task, dict) or not isinstance(task.get("steps"), list) or any(
                not isinstance(argv, list) or not argv for argv in task["steps"]):
            raise ValueError("Task requires nonempty argv steps")
        automatic = task.get('cpus') == 0
        cpus = task.get("cpus", 1) or 1
        memory_mb = task.get("memory_mb", 256)
        label = task.get("label", "")
        required_tools = {m[1] for argv in task.get("steps", ())
                          for m in [re.fullmatch(r"\$\{tool:([\w-]+)\}", argv[0])] if m}
        texts = [*task.get("env", {}).values(), task.get("cwd", ""),
                 *(arg for argv in task["steps"] for arg in argv)]
        required_shared = {name for text in texts for name in re.findall(r"\$\{shared:([\w-]+)\}", text)}
        with self.condition:
            while True:
                if self.closed:
                    raise RemoteError("Pool has closed")
                if cancel_event is not None and cancel_event.is_set():
                    return None
                matching = [n for n in self.nodes if (not task.get("node") or n.name == task["node"])
                            and set(task.get("requires", ())).issubset(n.health.get("tags", ()))
                            and all(n.health.get("tools", {}).get(tool, {}).get("available") for tool in required_tools)
                            and required_shared.issubset(n.health.get("shared", ()))
                            and n.capacity >= cpus and n.memory_capacity >= memory_mb]
                if not matching:
                    raise RemoteError("No matching online node for task CPU count, tools, shared paths, node name and tags")
                def needed(node):
                    # Automatic jobs can start with a partial grant. Waiting for
                    # an entire target reserved idle CPUs behind long-running jobs.
                    return min(cpu_limit,node.capacity,task.get('max_cpus',node.capacity)) if automatic and cpu_limit and not partial_cpus else cpus
                ready = [n for n in matching if not n.error and n.enabled and not n.health.get("paused")
                         and n.capacity - n.reserved - n.external >= needed(n)
                         and n.memory_free >= memory_mb and self._host_memory_free(n) >= memory_mb
                         and n.health.get("queued", 0) == 0]
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
                        observations = [n.durations[label] for n in matching if label in n.durations]
                        typical = sum(observations) / len(observations) if observations else 1.0
                        def score(n):
                            speed = n.durations.get(label, typical * 0.8) / typical if self.policy == "adaptive" else 1.0
                            load = n.reserved + n.external
                            return ((load + cpus) * speed / (n.capacity * n.weight), pressures[n.name])
                        unexplored = [n for n in close if label not in n.durations] if self.policy == "adaptive" else []
                        node = min(unexplored or close, key=score)
                    if automatic:
                        cpus = node.capacity - node.reserved - node.external
                        if cpu_limit is not None:
                            cpus = min(cpus, cpu_limit)
                        cpus = min(cpus, task.get('max_cpus',node.capacity))
                        task['cpus'] = cpus
                    node.reserved += cpus
                    node.reserved_memory_mb += memory_mb
                    if job_id:
                        node.known_jobs.add(job_id)
                        node.live_jobs.add(job_id)
                    return node
                if not block:
                    return None
                self.condition.wait(0.2)

    def _cpu_pressure(self, node, cpus):
        machine = node.health.get('machine_id', node.client.url)
        siblings = [n for n in self.nodes if not n.error
                    and n.health.get('machine_id', n.client.url) == machine]
        logical = max(n.health.get('logical_cpus') or n.capacity for n in siblings)
        sampled = max(n.health.get('cpu_percent') or 0 for n in siblings) * logical / 100
        reported = sum(n.health.get('used_cpus', 0) for n in siblings)
        committed = sum(n.reserved + n.external for n in siblings)
        # The sample already includes running work. Replace its reported grants
        # with today's ledger, including dispatches and resizes not sampled yet.
        return (max(0, sampled - reported) + committed + cpus) / logical

    def _host_memory_free(self, node):
        machine = node.health.get("machine_id", node.client.url)
        siblings = [n for n in self.nodes if n.health.get("machine_id", n.client.url) == machine and not n.error]
        # One physical memory pool per machine, even with several worker ports.
        physical = min(n.health.get("host_memory_available_mb", 0) - n.health.get("memory_reserve_mb", 0) for n in siblings)
        unallocated = sum(max(0, n.reserved_memory_mb + n.external_memory_mb - n.health.get("rss_mb", 0)) for n in siblings)
        return physical - unallocated

    def _release(self, node, task, result=None, *, job_id=None):
        label = task.get("label", "")
        with self.condition:
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
                    node.durations[label] = 0.7 * node.durations.get(label, measured) + 0.3 * measured
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

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.close()
