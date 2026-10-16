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
            raise RemoteError(f"{self.url}{path}: HTTP {exc.code}: {detail}") from None
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
        return self.request("/jobs/" + identity, task_bundle(task, inputs, files))

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
        payload = pack(tree(root, patterns))
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
    durations: dict = field(default_factory=dict)
    completed: int = 0
    failed: int = 0

    @property
    def capacity(self):
        available = self.health.get("cpus", 0)
        return min(available, self.limit) if self.limit else available


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
                or type(node.limit) is not int or node.limit < 0 or not isinstance(node.enabled, bool)):
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
                if set(changes) - {"weight", "limit", "enabled"}:
                    raise ValueError("Node settings: weight, limit, enabled")
                proposed = Node(node.name, node.client, **{
                    key: changes.get(key, getattr(node, key)) for key in ("weight", "limit", "enabled")})
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
                 "paused": n.health.get("paused", False), "tags": n.health.get("tags", []),
                 "weight": n.weight, "limit": n.limit, "completed": n.completed, "failed": n.failed,
                 "seconds_by_label": dict(n.durations), "error": n.error} for n in self.nodes]}

    def _acquire(self, task):
        cpus = task.get("cpus", 1)
        label = task.get("label", "")
        required_tools = {m[1] for argv in task.get("steps", ())
                          if (m := re.fullmatch(r"\$\{tool:([\w-]+)\}", argv[0]))}
        with self.condition:
            while True:
                if self.closed:
                    raise RemoteError("Pool has closed")
                matching = [n for n in self.nodes if (not task.get("node") or n.name == task["node"])
                            and set(task.get("requires", ())).issubset(n.health.get("tags", ()))
                            and all(n.health.get("tools", {}).get(tool, {}).get("available") for tool in required_tools)
                            and n.capacity >= cpus and not n.error]
                if not matching:
                    raise RemoteError("No matching online node for task CPU count, tools, node name and tags")
                ready = [n for n in matching if n.enabled and not n.health.get("paused")
                         and n.capacity - n.reserved - n.external >= cpus
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
                            return (load + cpus) * speed / (n.capacity * n.weight)
                        node = min(ready, key=score)
                    node.reserved += cpus
                    return node
                self.condition.wait(0.2)

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
                node.reserved -= cpus
                if result is not None:
                    node.completed += 1
                    if result["status"] == "succeeded":
                        measured = result["elapsed"]
                        node.durations[label] = 0.7 * node.durations.get(label, measured) + 0.3 * measured
                    else:
                        node.failed += 1
                self.condition.notify_all()

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
