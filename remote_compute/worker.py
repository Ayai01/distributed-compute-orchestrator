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
import sys
import threading
import time
import zipfile
from urllib.parse import unquote

from .bundle import MAX_BODY, PROTOCOL, TERMINAL, atomic_json, inside, json_bytes, pack, tree, unpack, progress
from . import __version__
from .process import Process
from .resources import Host, process_usage
from .telemetry import Activity

_ID = re.compile(r"[a-f0-9]{32}\Z")
_ASSET = re.compile(r"[a-f0-9]{64}\Z")
_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_-]*\Z")
_VAR = re.compile(r"\$\{(work|cpus|exe|tool:[\w-]+|asset:[\w-]+|shared:[\w-]+)\}")


class Conflict(ValueError):
    pass


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
    limit = max(1, count - 1)
    if int(value) < 0 or int(value) > limit:
        raise ValueError(f'CPU budget must be in 1..{limit}, or 0/auto')
    return int(value) or limit


def file_hash(path):
    try:
        with open(path, "rb") as stream:
            return hashlib.file_digest(stream, "sha256").hexdigest()
    except OSError:
        # Windows execution aliases can launch a tool without exposing readable file bytes.
        return None


def validate_task(task, capacity, *, allow_auto=False):
    if not isinstance(task, dict):
        raise ValueError("Task must be an object")
    unknown = set(task) - {"steps", "cpus", "timeout", "outputs", "env", "assets", "render", "label", "priority", "requires", "node", "memory_mb", "cwd", "stdin", "project", "resource_share"}
    if unknown:
        raise ValueError("Unknown task fields: " + ", ".join(sorted(unknown)))
    task = {"cpus": 1, "timeout": 300, "outputs": [], "env": {}, "assets": {},
            "render": [], "label": "", "priority": 0, "requires": [], "node": None, "memory_mb": 256, **task}
    cpus, timeout = task["cpus"], task["timeout"]
    if not isinstance(task.get('project', ''), str):
        raise ValueError('project must be a string')
    share = task.get('resource_share', 1.0)
    if isinstance(share, bool) or not isinstance(share, (int, float)) or not math.isfinite(share) or share <= 0:
        raise ValueError('resource_share must be positive and finite')
    if type(cpus) is not int or not (0 if allow_auto else 1) <= cpus <= capacity:
        raise ValueError(f"Task cpus must be in 1..{capacity}")
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
    if type(task["priority"]) is not int:
        raise ValueError("priority must be an integer; larger values run first")
    if not isinstance(task["requires"], list) or any(not isinstance(x, str) for x in task["requires"]):
        raise ValueError("requires must be a list of node tags")
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
    def __init__(self, root, cpus=0, *, tools=None, shared=None, env=None, token="", tags=(), memory_mb=0, memory_reserve_mb=512, cpu_limit=None):
        self.root = Path(root).expanduser().resolve()
        self.callback = {}
        physical_limit = max(1, (os.cpu_count() or 1) - 1)
        if cpu_limit is not None and (type(cpu_limit) is not int or not 1 <= cpu_limit <= physical_limit):
            raise ValueError(f'cpu_limit must be in 1..{physical_limit}')
        self.cpu_limit = physical_limit if cpu_limit is None else cpu_limit
        self.capacity = cpu_budget(cpus) if cpus else self.cpu_limit
        if self.capacity > self.cpu_limit:
            raise ValueError(f'CPU budget exceeds configured cpu_limit={self.cpu_limit}')
        self.activity = Activity()
        self.save_settings = None
        self.token = token
        self.tags = list(tags)
        self.paused = False
        self.host = Host()
        total_mb = self.host.sample()["memory_total_mb"]
        if type(memory_mb) is not int or memory_mb < 0 or type(memory_reserve_mb) is not int or memory_reserve_mb < 0:
            raise ValueError("Memory budget and headroom must be nonnegative integer MiB")
        self.memory_capacity = memory_mb or max(1, total_mb - memory_reserve_mb)
        self.memory_reserve = memory_reserve_mb
        self.env = dict(env or {})
        self.shared, self.unavailable_shared = {}, {}
        for name, value in (shared or {}).items():
            path = Path(value).expanduser().absolute().resolve()
            (self.shared if path.is_dir() else self.unavailable_shared)[name] = path.as_posix()
        self.restart_available = False
        self.tools = {"python": [sys.executable], "ngspice": ["ngspice"],
                      "verilator": [sys.executable, str(Path(__file__).with_name("rtl.py"))]}
        for name, value in (tools or {}).items():
            self.tools[name] = [value] if isinstance(value, str) else list(value)
        self.tool_info = {}
        search_path = self.env.get("PATH", os.environ.get("PATH", ""))
        for name, argv in self.tools.items():
            if not argv or any(not isinstance(x, str) for x in argv):
                raise ValueError("Tool requires an executable or argv prefix: " + name)
            executable = shutil.which(argv[0], path=search_path)
            if executable:
                argv[0] = str(Path(executable).resolve())
            self.tool_info[name] = {"argv": argv, "available": executable is not None,
                                    "sha256": file_hash(executable) if executable else None}
            supporting = {str(Path(x).resolve()): file_hash(x) for x in argv[1:] if Path(x).is_file()}
            if supporting:
                self.tool_info[name]["supporting_sha256"] = supporting
        if self.tools["verilator"][1:] == [str(Path(__file__).with_name("rtl.py"))]:
            msys = self.env.get("MSYS2_ROOT", os.environ.get("MSYS2_ROOT"))
            engine = (str(Path(msys) / "ucrt64/bin/verilator_bin.exe") if os.name == "nt" and msys
                      else shutil.which("verilator", path=search_path))
            ready = bool(engine and Path(engine).is_file())
            self.tool_info["verilator"].update(available=ready, engine=engine,
                                               engine_sha256=file_hash(engine) if ready else None)
        self.tags = sorted(set(self.tags) | {sys.platform} | {k for k, v in self.tool_info.items() if v["available"]})
        if self.tool_info['verilator']['available']:
            self.tags = sorted(set(self.tags) | {'rtl'})
        # The lock is released by the OS on exit; no stale PID files to repair.
        self.root_lock = lock_root(self.root, "worker")
        (self.root / "jobs").mkdir(exist_ok=True)
        (self.root / "assets").mkdir(exist_ok=True)
        self.condition = threading.Condition(threading.RLock())
        self.jobs = {}
        self.queue = deque()
        self.used = 0
        self.stopping = False
        self.threads = set()
        for path in (self.root / "jobs").glob("*/result.json"):
            saved = json.loads(path.read_text(encoding="utf-8"))
            if saved["status"] not in TERMINAL:
                saved.update(status="interrupted", finished=time.time(), error="Worker stopped before completion")
                atomic_json(path, saved)
            self.jobs[saved["id"]] = {"result": saved, "cancel": threading.Event(), "directory": path.parent}
        self.scheduler = threading.Thread(target=self._schedule, name="compute-scheduler", daemon=True)
        self.scheduler.start()

    def health(self):
        with self.condition:
            resources = self._memory_state()
            return {"protocol": PROTOCOL, "node": socket.gethostname(), "platform": sys.platform,
                    "role": "worker", **resources,
                    "cpus": self.capacity, "cpu_limit": self.cpu_limit, "used_cpus": self.used, "queued": len(self.queue),
                    "tools": self.tool_info, "shared": sorted(self.shared), "tags": self.tags, "paused": self.paused,
                    "capabilities": ["machine", "resize"] + (["restart"] if self.restart_available else []),
                    "pid": os.getpid(), "version": __version__, "home": Path.home().as_posix(),
                    "tool_root": Path(__file__).resolve().parent.parent.as_posix(), "shared_paths": self.shared,
                    "unavailable_shared": self.unavailable_shared, "root": self.root.as_posix(),
                    "active_jobs": [{k: j['result'].get(k) for k in ('id', 'label', 'project', 'status', 'cpus', 'memory_mb')}
                                    for j in self.jobs.values() if j['result']['status'] not in TERMINAL],
                    **self.activity.snapshot()}

    def control(self, value):
        if not isinstance(value, dict) or set(value) - {"cpus", "paused", "memory_mb"}:
            raise ValueError("Control accepts cpus, memory_mb and paused")
        if "cpus" in value and (type(value["cpus"]) is not int or not 1 <= value["cpus"] <= self.cpu_limit):
            raise ValueError(f"cpus must be in 1..{self.cpu_limit}")
        if "paused" in value and not isinstance(value["paused"], bool):
            raise ValueError("paused must be true or false")
        if "memory_mb" in value and (type(value["memory_mb"]) is not int or value["memory_mb"] < 1):
            raise ValueError("memory_mb must be a positive integer")
        with self.condition:
            if "cpus" in value:
                self.capacity = value["cpus"]
            if "paused" in value:
                self.paused = value["paused"]
            if "memory_mb" in value:
                self.memory_capacity = value["memory_mb"]
            if self.save_settings:
                self.save_settings({key: value[key] for key in ('cpus', 'memory_mb') if key in value})
            self.activity.notify('Resources ' + json.dumps(value, ensure_ascii=False))
            self.condition.notify_all()
            return self.health()

    def _memory_state(self):
        host = self.host.sample()
        running = [j for j in self.jobs.values() if j["result"]["status"] == "running"]
        reserved = sum(max(j["task"]["memory_mb"], j.get("rss_mb", 0)) for j in running)
        unallocated = sum(max(0, j["task"]["memory_mb"] - j.get("rss_mb", 0)) for j in running)
        available = max(0, min(self.memory_capacity - reserved,
                               host["host_memory_available_mb"] - self.memory_reserve - unallocated))
        return {**host, "memory_capacity_mb": self.memory_capacity, "memory_reserved_mb": round(reserved, 2),
                "memory_available_mb": int(available), "memory_reserve_mb": self.memory_reserve,
                "rss_mb": round(sum(j.get("rss_mb", 0) for j in running), 2)}

    def _save(self, job):
        atomic_json(job["directory"] / "result.json", job["result"])

    def _expand(self, text, task, work):
        def substitute(match):
            key = match[1]
            if key == "work":
                return work.as_posix()
            if key == "cpus":
                return str(task["cpus"])
            if key == "exe":
                return ".exe" if os.name == "nt" else ""
            kind, name = key.split(":", 1)
            if kind == "shared":
                if name in self.unavailable_shared:
                    raise ValueError("Shared directory is unavailable on this worker: " + name)
                return self.shared[name]
            if kind == "asset":
                return (self.root / "assets" / task["assets"][name]).as_posix()
            if len(self.tools[name]) != 1:
                raise ValueError("Multi-argument tools belong at argv[0]")
            return Path(self.tools[name][0]).as_posix()
        return _VAR.sub(substitute, text)

    def resize(self, identity, cpus):
        if type(cpus) is not int or cpus<1:
            raise ValueError('resize cpus must be a positive integer')
        with self.condition:
            job=self.jobs[identity];task,result=job['task'],job['result']
            if self.stopping or job['cancel'].is_set() or result['status']!='running':
                raise Conflict('Only a running task can change its CPU grant')
            old=task['cpus']
            granted=min(cpus,old+max(0,self.capacity-self.used)) if cpus>old else cpus
            if granted!=old:
                self.used+=granted-old
                task['cpus']=result['cpus']=granted
                result['peak_cpus']=max(result.get('peak_cpus',old),granted)
                result['resize_count']=result.get('resize_count',0)+1
                self._save(job)
                self.condition.notify_all()
            return dict(id=identity,node=socket.gethostname(),cpus=granted,requested_cpus=cpus)

    def submit(self, identity, payload, *, callback=None):
        if not _ID.fullmatch(identity):
            raise ValueError("Job id requires 32 lowercase hex characters")
        digest = hashlib.sha256(payload).hexdigest()
        with self.condition:
            if self.stopping:
                raise Conflict("Worker is stopping")
            if identity in self.jobs:
                result = self.jobs[identity]["result"]
                if result["request_sha256"] != digest:
                    raise Conflict("Job id already contains a different request")
                return dict(result)
            with zipfile.ZipFile(io.BytesIO(payload)) as archive:
                task = validate_task(json.loads(archive.read("task.json")), self.capacity)
                if task["memory_mb"] > self.memory_capacity:
                    raise ValueError("Task memory_mb exceeds worker memory budget")
                for item in archive.infolist():
                    inside(self.root, item.filename)
                    if item.filename != "task.json" and not item.filename.startswith("input/"):
                        raise ValueError("Job bundle contains a file outside input/")
            for asset in task["assets"].values():
                if not (self.root / "assets" / asset).is_dir():
                    raise ValueError("Synchronize missing asset first: " + asset)
            # Resolve commands and references before admission, so typos do not occupy slots.
            work = self.root / "jobs" / identity / "input"
            cwd = Path(self._expand(task["cwd"], task, work)).expanduser() if "cwd" in task else work
            if not cwd.is_absolute():
                raise ValueError("cwd must resolve to an absolute worker path")
            cwd = cwd.resolve()
            commands = []
            for argv in task["steps"]:
                match = re.fullmatch(r"\$\{tool:([\w-]+)\}", argv[0])
                prefix = self.tools[match[1]] if match else [self._expand(argv[0], task, work)]
                commands.append([*prefix, *[self._expand(x, task, work) for x in argv[1:]]])
            for value in task["env"].values():
                self._expand(value, task, work)
            directory = work.parent
            try:
                directory.mkdir()
                unpack(payload, directory)
                work.mkdir(exist_ok=True)
                if "stdin" in task and not inside(work, task["stdin"]).is_file():
                    raise ValueError("Missing stdin input file: " + task["stdin"])
                for name, path in tree(work, task["render"]).items():
                    path.write_text(self._expand(path.read_text(encoding="utf-8"), task, work), encoding="utf-8")
                result = {"id": identity, "status": "queued", "label": task["label"], 'project': task['project'], "cpus": task["cpus"],
                          "memory_mb": task["memory_mb"], "peak_rss_mb": 0, "cpu_seconds": 0,
                          'initial_cpus':task['cpus'],'peak_cpus':task['cpus'],'resize_count':0,
                          "submitted": time.time(), "started": None, "finished": None,
                          "request_sha256": digest, "node": socket.gethostname(), "steps": [],
                          "stdout": "", "stderr": "", "tools": self.tool_info,
                          "assets": task["assets"], "cwd": cwd.as_posix()}
                job = {"result": result, "cancel": threading.Event(), "task": task,
                       "commands": commands, "directory": directory, "cwd": cwd,
                       'callback':dict(self.callback if callback is None else callback)}
                self._save(job)
            except BaseException:
                if directory.exists():
                    shutil.rmtree(directory)
                raise
            self.jobs[identity] = job
            self.queue.append(identity)
            self.queue = deque(sorted(self.queue, key=lambda key: -self.jobs[key]["task"]["priority"]))
            self.condition.notify_all()
            return dict(result)

    def _schedule(self):
        with self.condition:
            while not self.stopping:
                # FIFO prevents a continuous stream of small jobs starving a larger one.
                if self.queue and not self.paused:
                    job = self.jobs[self.queue[0]]
                    if (self.used + job["task"]["cpus"] <= self.capacity
                            and job["task"]["memory_mb"] <= self._memory_state()["memory_available_mb"]):
                        self.queue.popleft()
                        self.used += job["task"]["cpus"]
                        job["result"].update(status="running", started=time.time())
                        self._save(job)
                        thread = threading.Thread(target=self._execute, args=(job,), name="compute-job", daemon=True)
                        self.threads.add(thread)
                        thread.start()
                        continue
                self.condition.wait(0.5)

    def _execute(self, job):
        task, result, directory = job["task"], job["result"], job["directory"]
        work = directory / "input"
        deadline = time.monotonic() + task["timeout"]
        state, error = "succeeded", None
        cpu_seconds = 0.0
        try:
            environment = dict(os.environ, **self.env)
            environment.update({key: self._expand(value, task, work) for key, value in task["env"].items()})
            # One admission charge covers task processes AND their numerical threads.
            for key in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS"):
                environment[key] = str(task["cpus"])
            temporary = directory / "tmp"
            temporary.mkdir()
            environment.update(TMP=str(temporary), TEMP=str(temporary), TMPDIR=str(temporary),
                               PYTHONDONTWRITEBYTECODE="1", PYTHONUTF8="1", REMOTE_COMPUTE_JOB_DIR=str(directory),
                               REMOTE_COMPUTE_WORKER='1', REMOTE_COMPUTE_JOB_ID=result['id'])
            callback=job.get('callback',{})
            environment.update(REMOTE_COMPUTE_URL=callback.get('url',''),
                               REMOTE_COMPUTE_TOKEN=callback.get('token',''),
                               REMOTE_COMPUTE_RESIZE='1' if callback.get('url') else '0',
                               REMOTE_COMPUTE_HOME=str(Path(__file__).resolve().parent.parent))
            with (directory / "stdout.log").open("wb") as stdout, (directory / "stderr.log").open("wb") as stderr:
                for argv in job["commands"]:
                    if job["cancel"].is_set():
                        state = "cancelled"
                        break
                    step = {"argv": argv, "started": time.time(), "returncode": None}
                    with self.condition:
                        result["steps"].append(step)
                        self._save(job)
                    source = inside(work, task["stdin"]).open("rb") if "stdin" in task else nullcontext(None)
                    with source as stdin:
                        process = Process(argv, job["cwd"], environment, stdout, stderr, stdin=stdin)
                    try:
                        while process.child.poll() is None:
                            usage = process_usage(process)
                            with self.condition:
                                job["rss_mb"] = usage["rss_mb"]
                                result["peak_rss_mb"] = max(result["peak_rss_mb"], usage["rss_mb"])
                                result["cpu_seconds"] = max(result["cpu_seconds"], cpu_seconds + usage["cpu_seconds"])
                            if job["cancel"].is_set() or time.monotonic() >= deadline:
                                state = "cancelled" if job["cancel"].is_set() else "timed_out"
                                process.stop()
                                break
                            job["cancel"].wait(0.05)
                        step.update(returncode=process.child.wait(), finished=time.time())
                        usage = process_usage(process)
                        cpu_seconds = max(result["cpu_seconds"], cpu_seconds + usage["cpu_seconds"])
                        result["cpu_seconds"] = cpu_seconds
                    finally:
                        process.close()
                    if state != "succeeded":
                        break
                    if step["returncode"] != 0:
                        state, error = "failed", f"Command exited with {step['returncode']}"
                        break
        except Exception as exc:
            state, error = "failed", f"{type(exc).__name__}: {exc}"
        finally:
            with self.condition:
                if job["cancel"].is_set() and state == "succeeded":
                    state = "cancelled"
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
                self.condition.notify_all()

    def get(self, identity):
        with self.condition:
            return json.loads(json_bytes(self.jobs[identity]["result"]))

    def job_file(self, identity, name, data=None):
        with self.condition:
            path = inside(self.jobs[identity]['directory'] / 'input', name)
            if data is None:
                return path.read_bytes()
            path.parent.mkdir(parents=True, exist_ok=True)
            temporary = path.with_name(path.name + '.control-upload')
            temporary.write_bytes(data)
            temporary.replace(path)
            return {'written': name, 'bytes': len(data)}

    def logs(self, identity):
        with self.condition:
            directory = self.jobs[identity]["directory"]
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
        with self.condition:
            job = self.jobs[identity]
            if job["result"]["status"] not in TERMINAL:
                job["cancel"].set()
                if job["result"]["status"] == "queued":
                    self.queue.remove(identity)
                    job["result"].update(status="cancelled", finished=time.time())
                    self._save(job)
                self.condition.notify_all()
            return self.get(identity)

    def artifacts(self, identity, *, result=None):
        with self.condition:
            job = self.jobs[identity]
            if job["result"]["status"] not in TERMINAL:
                raise Conflict("Artifacts are available when the job has finished")
            directory = job["directory"]
            task = job.get("task") or json.loads((directory / "task.json").read_text(encoding="utf-8"))
            files = {"result.json": json_bytes(job["result"] if result is None else result)}
            files.update({"files/" + name: path for name, path in tree(directory / "input", task["outputs"]).items()})
            for name in ("stdout.log", "stderr.log"):
                if (directory / name).exists():
                    files[name] = directory / name
            return pack(files)

    def delete(self, identity):
        with self.condition:
            job = self.jobs[identity]
            if job["result"]["status"] not in TERMINAL:
                raise Conflict("Cancel and wait for the job before deleting it")
            shutil.rmtree(job["directory"])
            del self.jobs[identity]
            return {"deleted": identity}

    def asset(self, identity, payload=None):
        if not _ASSET.fullmatch(identity):
            raise ValueError("Asset id requires SHA256")
        directory = self.root / "assets" / identity
        with self.condition:
            if payload is not None and not directory.is_dir():
                if hashlib.sha256(payload).hexdigest() != identity:
                    raise ValueError("Asset digest does not match bundle")
                temporary = directory.with_suffix(".upload")
                try:
                    temporary.mkdir(exist_ok=True)
                    unpack(payload, temporary)
                    temporary.replace(directory)
                finally:
                    if temporary.exists():
                        shutil.rmtree(temporary)
            return {"id": identity, "exists": directory.is_dir()}

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


class Server(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 128

    def __init__(self, address, worker, *, restart=None):
        self.worker = worker
        self.restart = restart
        self.connections = set()
        self.connections_lock = threading.Lock()
        super().__init__(address, Handler)
        if hasattr(worker,'restart_node'):
            worker.callback_address=self.server_address
        else:
            worker.callback=dict(url=f'http://127.0.0.1:{self.server_port}',token=worker.token)

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

    def setup(self):
        super().setup()
        self.connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    def log_message(self, *_args):
        pass

    def _reply(self, value, status=200):
        binary = isinstance(value, bytes)
        body = value if binary else json_bytes(value)
        compressed = not binary and len(body) >= 1024 and 'gzip' in self.headers.get('Accept-Encoding', '')
        if compressed:
            body = gzip.compress(body, compresslevel=1)
        self.send_response(status)
        self.send_header("Content-Type", "application/zip" if binary else "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        if compressed:
            self.send_header('Content-Encoding', 'gzip')
            self.send_header('Vary', 'Accept-Encoding')
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def _handle(self):
        worker = self.server.worker
        if worker.token and not hmac.compare_digest(self.headers.get("Authorization", ""), "Bearer " + worker.token):
            return self._reply({"error": "Token required"}, 401)
        worker.activity.access(self.client_address[0])
        parts = self.path.strip("/").split("/")
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
                value = worker.health()
            elif parts == ["nodes"] and self.command == "GET" and hasattr(worker, "nodes"):
                value = worker.nodes()
            elif parts == ['admission'] and self.command == 'POST' and hasattr(worker, 'admission'):
                value = worker.admission(json.loads(body))
            elif parts == ["control"] and self.command == "POST":
                value = worker.control(json.loads(body))
                worker.activity.notify('Control ' + body.decode('utf-8'), self.client_address[0])
            elif parts == ["restart"] and self.command == "POST":
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
            elif parts == ["jobs"] and self.command == "GET":
                with worker.condition:
                    value = [worker.get(identity) for identity in worker.jobs]
            elif len(parts) == 2 and parts[0] == "assets" and self.command in {"GET", "POST"}:
                value = worker.asset(parts[1], body)
            elif len(parts) >= 2 and parts[0] == "jobs":
                identity = parts[1]
                if len(parts) == 2 and self.command == "POST":
                    callback=self.headers.get('X-Remote-Compute-Callback')
                    value = worker.submit(identity,body,callback=json.loads(callback)) if callback else worker.submit(identity,body)
                    worker.activity.submitted(value, worker.jobs[identity].get('task', {}), self.client_address[0])
                    return self._reply(value, 202)
                if len(parts) == 2 and self.command == "GET":
                    value = worker.get(identity)
                elif len(parts) == 2 and self.command == "DELETE":
                    value = worker.delete(identity)
                elif parts[2:] == ["cancel"] and self.command == "POST":
                    value = worker.cancel(identity)
                elif parts[2:] == ['resize'] and self.command == 'POST':
                    value = worker.resize(identity,json.loads(body)['cpus'])
                elif parts[2:] == ["artifacts"] and self.command == "GET":
                    value = worker.artifacts(identity)
                elif parts[2:] == ["logs"] and self.command == "GET":
                    value = worker.logs(identity)
                elif len(parts) >= 4 and parts[2] == 'files' and self.command in {'GET', 'POST'}:
                    name = unquote('/'.join(parts[3:]))
                    value = worker.job_file(identity, name, body if self.command == 'POST' else None)
                    if self.command == 'POST':
                        worker.activity.notify('Job file ' + name + ' [' + identity[:8] + ']', self.client_address[0])
                else:
                    return self._reply({"error": "Unknown route"}, 404)
            else:
                return self._reply({"error": "Unknown route"}, 404)
            if self.command == 'GET' and parts[0] == 'jobs' and self.headers.get('X-Remote-Compute-Brief') == '1':
                if len(parts) == 1:
                    value = [progress(row) for row in value]
                elif len(parts) == 2:
                    value = progress(value)
            self._reply(value)
        except KeyError as exc:
            self._reply({"error": "Unknown job, tool or path variable: " + str(exc)}, 404)
        except Conflict as exc:
            self._reply({"error": str(exc)}, 409)
        except FileNotFoundError as exc:
            self._reply({"error": str(exc)}, 404)
        except (ValueError, OSError, zipfile.BadZipFile) as exc:
            self._reply({"error": str(exc)}, 400)

    do_GET = do_POST = do_DELETE = _handle
