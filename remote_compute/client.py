"""HTTP client and weighted, adaptive dispatch across independent workers."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
import hashlib
import io
import json
from pathlib import Path
import re
import threading
import time
from urllib.error import HTTPError, URLError
from urllib.request import ProxyHandler, Request, build_opener
import uuid
import zipfile

from .bundle import PROTOCOL, TERMINAL, json_bytes, pack, task_bundle, tree, unpack


class RemoteError(RuntimeError):
    pass


class Client:
    def __init__(self, url="http://127.0.0.1:8765", token="", *, timeout=10):
        self.url, self.token, self.timeout = url.rstrip("/"), token, timeout
        # LAN traffic goes directly to the worker, independent of system proxy setup.
        self.http = build_opener(ProxyHandler({}))

    def request(self, path, data=None, *, method=None, binary=False):
        headers = {"Content-Type": "application/zip" if isinstance(data, bytes) else "application/json"}
        if self.token:
            headers["Authorization"] = "Bearer " + self.token
        if data is not None and not isinstance(data, bytes):
            data = json_bytes(data)
        request = Request(self.url + path, data=data, headers=headers, method=method)
        try:
            with self.http.open(request, timeout=self.timeout) as response:
                raw = response.read()
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            exc.close()
            error = RemoteError(f"{self.url}{path}: HTTP {exc.code}: {detail}")
            error.status_code = exc.code
            raise error from None
        except (URLError, OSError) as exc:
            raise RemoteError(f"{self.url}{path}: {exc}") from None
        return raw if binary else json.loads(raw)

    def health(self):
        result = self.request("/health")
        if result.get("protocol") != PROTOCOL:
            raise RemoteError("Worker protocol differs: " + self.url)
        return result

    def control(self, **changes):
        return self.request("/control", changes)

    def submit(self, task, *, inputs=None, files=None, job_id=None):
        identity = job_id or uuid.uuid4().hex
        # A stable id can be queried or resubmitted to THIS node after a lost reply.
        return self.submit_bundle(identity, task_bundle(task, inputs, files))

    def submit_bundle(self, identity, payload):
        return self.request("/jobs/" + identity, payload)

    def nodes(self):
        return self.request("/nodes")

    def machine(self, node, *, cwd="~"):
        """Operate on a named worker through this same center connection."""
        from .machine import Machine
        return Machine(self, node, cwd=cwd)

    def status(self, identity=None):
        return self.request("/jobs" + ("/" + identity if identity else ""))

    def cancel(self, identity):
        return self.request("/jobs/" + identity + "/cancel", {})

    def logs(self, identity):
        return self.request("/jobs/" + identity + "/logs")

    def delete(self, identity):
        return self.request("/jobs/" + identity, method="DELETE")

    def wait(self, identity, *, poll=0.15, wait_timeout=None):
        started = time.monotonic()
        while True:
            result = self.status(identity)
            if result["status"] in TERMINAL:
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

    def run(self, task, *, inputs=None, files=None, output_dir=None, keep=False, on_submit=None):
        identity = uuid.uuid4().hex
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

    balanced uses relative CPU load; adaptive adds an EMA of same-label elapsed
    time; round_robin gives predictable rotation. No accepted task is moved.
    """
    POLICIES = {"adaptive", "balanced", "round_robin"}

    def __init__(self, nodes, *, policy="adaptive", refresh=2):
        self.condition = threading.Condition(threading.RLock())
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
                            Client(entry["url"], entry.get("token", "")),
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
                node.health, node.error = value, error
                node.external = max(0, value.get("used_cpus", 0) - node.reserved)
                node.external_memory_mb = max(0, value.get("memory_reserved_mb", 0) - node.reserved_memory_mb)
                self.condition.notify_all()
        with ThreadPoolExecutor(max_workers=len(self.nodes)) as executor:
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
                 "memory_capacity_mb": n.memory_capacity, "memory_available_mb": max(0, int(n.memory_free)),
                 "memory_reserved_mb": n.health.get("memory_reserved_mb", 0),
                 "reserved_memory_mb": n.reserved_memory_mb, "memory_limit_mb": n.memory_limit_mb,
                 "memory_total_mb": n.health.get("memory_total_mb"),
                 "host_memory_available_mb": n.health.get("host_memory_available_mb"),
                 "machine_id": n.health.get("machine_id", n.client.url), "rss_mb": n.health.get("rss_mb", 0),
                 "paused": n.health.get("paused", False), "tags": n.health.get("tags", []),
                 "capabilities": n.health.get("capabilities", []), "home": n.health.get("home"),
                 "shared_paths": n.health.get("shared_paths", {}),
                 "weight": n.weight, "limit": n.limit, "completed": n.completed, "failed": n.failed,
                 "seconds_by_label": dict(n.durations), "error": n.error} for n in self.nodes]}

    def _acquire(self, task, *, block=True, cancel_event=None):
        if not isinstance(task, dict) or not isinstance(task.get("steps"), list) or any(
                not isinstance(argv, list) or not argv for argv in task["steps"]):
            raise ValueError("Task requires nonempty argv steps")
        cpus = task.get("cpus", 1)
        memory_mb = task.get("memory_mb", 256)
        label = task.get("label", "")
        required_tools = {m[1] for argv in task.get("steps", ())
                          if (m := re.fullmatch(r"\$\{tool:([\w-]+)\}", argv[0]))}
        with self.condition:
            while True:
                if self.closed:
                    raise RemoteError("Pool has closed")
                if cancel_event is not None and cancel_event.is_set():
                    return None
                matching = [n for n in self.nodes if (not task.get("node") or n.name == task["node"])
                            and set(task.get("requires", ())).issubset(n.health.get("tags", ()))
                            and all(n.health.get("tools", {}).get(tool, {}).get("available") for tool in required_tools)
                            and n.capacity >= cpus and n.memory_capacity >= memory_mb and not n.error]
                if not matching:
                    raise RemoteError("No matching online node for task CPU count, tools, node name and tags")
                ready = [n for n in matching if n.enabled and not n.health.get("paused")
                         and n.capacity - n.reserved - n.external >= cpus
                         and n.memory_free >= memory_mb and self._host_memory_free(n) >= memory_mb
                         and n.health.get("queued", 0) == 0]
                if ready:
                    if self.policy == "round_robin":
                        node = next((n for n in self.nodes[self.rotation:] + self.nodes[:self.rotation] if n in ready))
                        self.rotation = (self.nodes.index(node) + 1) % len(self.nodes)
                    else:
                        observations = [n.durations[label] for n in matching if label in n.durations]
                        typical = sum(observations) / len(observations) if observations else 1.0
                        def score(n):
                            speed = n.durations.get(label, typical * 0.8) / typical if self.policy == "adaptive" else 1.0
                            load = n.reserved + n.external
                            pressure = (n.health.get("cpu_percent") or 0) / 100
                            return (load + cpus) * speed * (1 + pressure) / (n.capacity * n.weight)
                        unexplored = [n for n in ready if label not in n.durations] if self.policy == "adaptive" else []
                        node = min(unexplored or ready, key=score)
                    node.reserved += cpus
                    node.reserved_memory_mb += memory_mb
                    return node
                if not block:
                    return None
                self.condition.wait(0.2)

    def _host_memory_free(self, node):
        machine = node.health.get("machine_id", node.client.url)
        siblings = [n for n in self.nodes if n.health.get("machine_id", n.client.url) == machine and not n.error]
        # One physical memory pool per machine, even with several worker ports.
        physical = min(n.health.get("host_memory_available_mb", 0) - n.health.get("memory_reserve_mb", 0) for n in siblings)
        unallocated = sum(max(0, n.reserved_memory_mb + n.external_memory_mb - n.health.get("rss_mb", 0)) for n in siblings)
        return physical - unallocated

    def _release(self, node, task, result=None):
        label = task.get("label", "")
        with self.condition:
            node.reserved -= task.get("cpus", 1)
            node.reserved_memory_mb -= task.get("memory_mb", 256)
            if result is not None:
                node.completed += 1
                if result["status"] == "succeeded":
                    measured = result["elapsed"]
                    node.durations[label] = 0.7 * node.durations.get(label, measured) + 0.3 * measured
                else:
                    node.failed += 1
            self.condition.notify_all()

    def run(self, task, *, inputs=None, files=None, output_dir=None, keep=False, on_submit=None):
        node = self._acquire(task)
        cpus, label = task.get("cpus", 1), task.get("label", "")
        identity = None
        def submitted(client, job_id):
            nonlocal identity
            identity = job_id
            with self.condition:
                self.active[(node.name, identity)] = client
            if on_submit:
                on_submit(client, job_id)
        result = None
        try:
            result = node.client.run(task, inputs=inputs, files=files, output_dir=output_dir,
                                     keep=keep, on_submit=submitted)
            result["assigned_node"] = node.name
            return result
        finally:
            with self.condition:
                if identity:
                    self.active.pop((node.name, identity), None)
                self._release(node, task, result)

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
