"""The unified Python entry point and its operator consoles."""

import argparse
import cmd
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import tempfile
import threading
import time
import uuid
import socket

from .client import Client, Pool, RemoteError
from .coordinator import Coordinator
from .tasks import ngspice, verilator
from .bundle import TERMINAL, atomic_json
from .worker import Conflict, Server, Worker
from .startup import Cancelled, resolve

ROOT = Path(__file__).resolve().parent.parent


def read_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))


def words(line):
    lexer = shlex.shlex(line, posix=True)
    lexer.whitespace_split = True
    lexer.commenters = ""
    if os.name == "nt":
        lexer.escape = ""
    return list(lexer)


def display(value):
    print(json.dumps(value, indent=2, ensure_ascii=False))


def table(rows, columns):
    rows = [[str(row.get(key, "")) for key in columns] for row in rows]
    widths = [max([len(columns[i]), *(len(row[i]) for row in rows)]) for i in range(len(columns))]
    print("  ".join(key.ljust(widths[i]) for i, key in enumerate(columns)))
    for row in rows:
        print("  ".join(value.ljust(widths[i]) for i, value in enumerate(row)))


def submission_parser(kind):
    parser = argparse.ArgumentParser(prog=kind)
    parser.add_argument("source", help="任务 JSON、SPICE 文件或 RTL 目录")
    parser.add_argument("--input", help="要传输的任务输入目录")
    parser.add_argument("--output", help="结果保存目录")
    parser.add_argument("--node", help="固定分配给指定节点")
    parser.add_argument("--cpus", type=int, dest="task_cpus")
    parser.add_argument("--memory-mb", type=int, help="任务内存预留，MiB")
    parser.add_argument("--priority", type=int)
    parser.add_argument("--requires", help="节点标签，以逗号分隔")
    parser.add_argument("--keep", action="store_true", help="回收后保留节点任务目录")
    if kind == "rtl":
        parser.add_argument("--top", default="tb")
        parser.add_argument("--sources", nargs="+", default=["tb.sv"])
        parser.add_argument("--flags", default="")
    return parser


def prepare(kind, args):
    source = Path(args.source).resolve()
    inputs = args.input
    if kind == "run":
        task = read_json(source)
    elif kind == "spice":
        task = ngspice(source.name)
        inputs = inputs or source.parent
    else:
        task = verilator(args.sources, args.top, flags=words(args.flags))
        inputs = inputs or source
    for key in ("cpus", "priority", "node"):
        value = getattr(args, "task_cpus" if key == "cpus" else key)
        if value is not None:
            task[key] = value
    if args.requires:
        task["requires"] = args.requires.split(",")
    if args.memory_mb is not None:
        task["memory_mb"] = args.memory_mb
    output = Path(args.output) if args.output else ROOT / "results" / (time.strftime("%Y%m%d-%H%M%S-") + uuid.uuid4().hex[:8])
    return task, {"inputs": inputs, "output_dir": output, "keep": args.keep}


MACHINE_ACTIONS = {"exec", "python", "ls", "read", "put", "get"}


def machine_parser(action):
    parser = argparse.ArgumentParser(prog=action)
    parser.add_argument("--node", required=True, help="操作的目标节点")
    parser.add_argument("--cwd", default="~", help="目标机工程目录，默认目标用户目录")
    if action in {"exec", "python"}:
        parser.add_argument("--cpus", type=int, default=1, dest="task_cpus")
        parser.add_argument("--memory-mb", type=int, default=256)
        parser.add_argument("--timeout", type=float, default=3600)
        parser.add_argument("--priority", type=int, default=0)
        parser.add_argument("--env", action="append", default=[], metavar="NAME=VALUE")
        parser.add_argument("--stdin", help="将本地文件发送为标准输入")
        parser.add_argument("--output", help="保存完整日志和结果")
        parser.add_argument("--detach", action="store_true", help="返回任务ID，任务在现有服务中继续运行")
        parser.add_argument("--keep", action="store_true")
        if action == "python":
            parser.add_argument("--tool", default="python", help="目标机的 Python 工具别名")
            parser.add_argument("source", help="发送并执行的本地 Python 脚本")
        parser.add_argument("argv", nargs=argparse.REMAINDER, help="-- 后为命令参数；python 模式下为脚本参数")
    else:
        parser.add_argument("source", help="ls/read/get 为目标机路径；put 为本地路径")
        if action in {"put", "get"}:
            parser.add_argument("destination")
            parser.add_argument("--pattern", action="append", help="目录传输的文件模式，可重复设置")
        if action == "read":
            parser.add_argument("--encoding", default="utf-8-sig")
    return parser


def machine_action(client, args, *, on_submit=None):
    machine = client.machine(args.node, cwd=args.cwd)
    if args.action in {"exec", "python"}:
        options = {"cpus": args.task_cpus, "memory_mb": args.memory_mb, "timeout": args.timeout,
                   "priority": args.priority, "env": dict(item.split("=", 1) for item in args.env),
                   "stdin": Path(args.stdin) if args.stdin else None, "wait": not args.detach,
                   "output_dir": args.output, "keep": args.keep, "on_submit": on_submit}
        argv = args.argv[1:] if args.argv[:1] == ["--"] else args.argv
        if args.action == "python":
            return machine.python(script=args.source, args=argv, tool=args.tool, **options)
        if not argv:
            raise ValueError("exec --node 节点 [选项] -- 程序 参数...")
        return machine.execute(argv, **options)
    if args.action == "ls":
        return machine.listdir(args.source)
    if args.action == "read":
        return machine.read_text(args.source, encoding=args.encoding)
    operation = machine.upload if args.action == "put" else machine.download
    return operation(args.source, args.destination, patterns=args.pattern or ("**/*",))


def batch(pool, source, output, *, on_submit=None, stop_event=None):
    source = Path(source).resolve()
    entries = read_json(source)
    if not isinstance(entries, list):
        raise ValueError("Batch JSON must be a list of {task, input} entries")
    prepared = []
    for item in entries:
        task = read_json(source.parent / item["task"]) if isinstance(item["task"], str) else item["task"]
        inputs = source.parent / item["input"] if item.get("input") else None
        prepared.append((task, inputs))
    results = [None] * len(prepared)
    def run(index):
        if stop_event is not None and stop_event.is_set():
            raise RemoteError("Console is closing")
        task, inputs = prepared[index]
        results[index] = pool.run(task, inputs=inputs, output_dir=Path(output) / str(index), on_submit=on_submit)
    order = sorted(range(len(entries)), key=lambda i: -prepared[i][0].get("priority", 0))
    capacity = sum(n.capacity for n in pool.nodes) if isinstance(pool, Pool) else pool.health()["cpus"]
    with ThreadPoolExecutor(max_workers=max(1, min(len(entries), capacity or 1))) as executor:
        list(executor.map(run, order))
    return results


class Shell(cmd.Cmd):
    intro = "输入 help 查看命令。空行保持当前状态，Ctrl+C 返回提示符，quit 退出。"

    def emptyline(self):
        pass

    def onecmd(self, line):
        try:
            return super().onecmd(line)
        except SystemExit:
            return False  # argparse --help and syntax errors keep the console open.
        except (ValueError, OSError, RuntimeError, KeyError, StopIteration) as error:
            print("操作失败:", error)

    def loop(self, health=None, url='', alive=lambda: True):
        if health and os.name == 'nt' and sys.stdin.isatty() and sys.stdout.isatty():
            from .console import Dashboard
            if Dashboard(self, health, url, alive).run():
                return
        while True:
            try:
                self.cmdloop()
                return
            except KeyboardInterrupt:
                print("\n已返回操作终端。")
                self.intro = None

    def do_quit(self, _line):
        """退出终端，并结束本入口持有的任务。"""
        return True

    do_exit = do_quit

    def do_EOF(self, line):
        print()
        return self.do_quit(line)


class SchedulerShell(Shell):
    prompt = "调度> "

    def __init__(self, pool, center=None):
        super().__init__()
        self.pool = pool
        self.center = center
        self.dispatch = center or pool
        self.handles = {}
        self.closing = threading.Event()
        self.executor = ThreadPoolExecutor(max_workers=8, thread_name_prefix="console-submit")
        self.futures = []

    def _node(self, name):
        return next(n for n in self.pool.nodes if n.name == name)

    def _start(self, function, *args, show_logs=False, **kwargs):
        future = self.executor.submit(function, *args, **kwargs)
        self.futures.append(future)
        number = len(self.futures)
        print(f"已提交操作 #{number}；终端可继续使用。")
        def finished(done):
            try:
                result = done.result()
                results = result if isinstance(result, list) else [result]
                for item in results:
                    print(f"\n#{number} {item['status']}  {item.get('assigned_node', '')}  {item['id']}  {item.get('output_dir', '')}")
                    if show_logs:
                        for stream in ("stdout", "stderr"):
                            if item.get(stream):
                                print(stream + ":\n" + item[stream])
            except Exception as error:
                print(f"\n#{number} 操作失败: {error}")
        future.add_done_callback(finished)

    def do_nodes(self, _line):
        """查看节点预算、占用、权重和分配状态。"""
        if self.center:
            value = self.center.nodes()
        else:
            self.pool.refresh()
            value = self.pool.snapshot()
        print("分配策略:", value["policy"])
        table(value["nodes"], ["name", "capacity", "used_cpus", "cpu_percent", "memory_available_mb", "memory_reserved_mb", "weight", "enabled", "paused"])

    def do_status(self, _line):
        """查看中心和每台设备的实时资源、标签、访问IP和任务。"""
        from .console import format_status
        print('\n'.join(format_status(self.center.health(), self.center.url)))

    def do_jobs(self, line):
        """jobs [节点]：查看任务；id 可用于 logs/cancel/fetch。"""
        rows = []
        if self.center:
            rows = [{**item, "node": item.get("assigned_node", "queued")} for item in self.center.status()]
            table(rows, ["node", "id", "status", "label", "cpus", "memory_mb"])
            return
        for node in self.pool.nodes:
            if line.strip() and node.name != line.strip():
                continue
            try:
                rows.extend({**item, "node": node.name} for item in node.client.status())
            except RemoteError as error:
                print(error)
        table(rows, ["node", "id", "status", "label", "cpus"])
        print("本终端操作:", "  ".join(f"#{i+1} {'已结束' if f.done() else '进行中'}" for i, f in enumerate(self.futures)))

    def _submit(self, kind, line):
        task, options = prepare(kind, submission_parser(kind).parse_args(words(line)))
        self._start(self.dispatch.run, task, **options, on_submit=self._accepted)

    def _accepted(self, client, identity):
        self.handles[identity] = client
        if self.closing.is_set():
            client.cancel(identity)
        print(f"\n已接单 {client.url}  {identity}")

    def do_run(self, line):
        """run 任务.json [--input 目录] [--output 目录] [--node 节点] [--cpus N] [--priority N]"""
        self._submit("run", line)

    def do_spice(self, line):
        """spice 网表.spice [--cpus N] [--node 节点] [--output 目录]"""
        self._submit("spice", line)

    def do_rtl(self, line):
        """rtl 目录 --top tb --sources tb.sv [--cpus N] [--output 目录]"""
        self._submit("rtl", line)

    def _machine(self, action, line):
        args = machine_parser(action).parse_args(words(line))
        args.action = action
        client = self.center or self._node(args.node).client
        if action in {"exec", "python"}:
            if args.detach:
                display(machine_action(client, args))
            else:
                self._start(machine_action, client, args, on_submit=self._accepted, show_logs=True)
        else:
            value = machine_action(client, args)
            print(value) if isinstance(value, str) else display(value)

    def do_exec(self, line):
        """exec --node 节点 --cwd 工程目录 -- 程序 参数...：在目标机执行命令。"""
        self._machine("exec", line)

    def do_python(self, line):
        """python --node 节点 --cwd 工程目录 本地脚本.py -- 参数...：发送并执行 Python。"""
        self._machine("python", line)

    def do_ls(self, line):
        """ls --node 节点 远程目录：查看目标机目录。"""
        self._machine("ls", line)

    def do_read(self, line):
        """read --node 节点 远程文件：读取文本文件。"""
        self._machine("read", line)

    def do_put(self, line):
        """put --node 节点 本地路径 远程路径 [--pattern 模式]：上传或更新文件。"""
        self._machine("put", line)

    def do_get(self, line):
        """get --node 节点 远程路径 本地路径 [--pattern 模式]：下载文件或目录。"""
        self._machine("get", line)

    def do_batch(self, line):
        """batch 清单.json 结果目录：按优先级和可用算力并行分配。"""
        source, output = words(line)
        self._start(batch, self.dispatch, source, output, on_submit=self._accepted, stop_event=self.closing)

    def do_policy(self, line):
        """policy adaptive|balanced|round_robin：切换自适应、均衡或轮询。"""
        if self.center:
            self.center.control(policy=line.strip())
        else:
            self.pool.configure(policy=line.strip())
        print("分配策略:", line.strip())

    def do_set(self, line):
        """set 节点 weight=2 limit=4 enabled=true：设置本调度器的权重、CPU上限和开关。"""
        name, *settings = words(line)
        changes = {}
        for setting in settings:
            key, value = setting.split("=", 1)
            if key == "enabled":
                if value.lower() not in {"true", "false"}:
                    raise ValueError("enabled=true|false")
                value = value.lower() == "true"
            else:
                value = int(value) if key in {"limit", "memory_limit_mb"} else float(value)
            changes[key] = value
        if self.center:
            self.center.control(node=name, routing=changes)
        else:
            self.pool.configure(name, **changes)
        self.do_nodes("")

    def do_add(self, line):
        """add http://设备IP:26133：以设备实际名称加入节点。"""
        url = words(line)[-1]
        name = Client(url, os.environ.get('REMOTE_COMPUTE_TOKEN', '')).health()['node']
        if self.center:
            self.center.control(add={"name": name, "url": url, "token": os.environ.get("REMOTE_COMPUTE_TOKEN", "")})
        else:
            self.pool.add(name, url, token=os.environ.get("REMOTE_COMPUTE_TOKEN", ""))
        self.do_nodes("")
        self.do_save('')

    def do_save(self, _line):
        """保存当前节点和策略到 nodes.local.json，下次启动自动加载。"""
        snapshot = self.center.nodes() if self.center else self.pool.snapshot()
        tokens = {n.client.url: n.client.token for n in self.pool.nodes}
        value = {"policy": snapshot["policy"], "nodes": []}
        for row in snapshot["nodes"]:
            node = {key: row[key] for key in ("name", "url", "weight", "limit", "memory_limit_mb", "enabled")}
            token = tokens.get(row["url"], self.center.token if self.center else "")
            if token:
                node["token"] = token
            value["nodes"].append(node)
        path = ROOT / "nodes.local.json"
        path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print("已保存:", path)

    def do_pause(self, line):
        """pause 节点：暂停该提供者的新任务，已运行任务继续。"""
        if self.center:
            self.center.control(node=line.strip(), worker={"paused": True})
        else:
            self._node(line.strip()).client.control(paused=True)
        self.pool.refresh()

    def do_resume(self, line):
        """resume 节点：恢复该提供者接单。"""
        if self.center:
            self.center.control(node=line.strip(), worker={"paused": False})
        else:
            self._node(line.strip()).client.control(paused=False)
        self.pool.refresh()

    def do_cpus(self, line):
        """cpus 节点 N：调整提供者的总CPU预算，现有任务自然收尾。"""
        name, count = words(line)
        if self.center:
            self.center.control(node=name, worker={"cpus": int(count)})
        else:
            self._node(name).client.control(cpus=int(count))
        self.pool.refresh()

    def do_memory(self, line):
        """memory 节点 MiB：调整提供者的任务内存总预算。"""
        name, amount = words(line)
        if self.center:
            self.center.control(node=name, worker={"memory_mb": int(amount)})
        else:
            self._node(name).client.control(memory_mb=int(amount))
        self.do_nodes("")

    def do_cancel(self, line):
        """cancel 节点 任务ID：取消指定任务及其子进程。"""
        name, identity = words(line)
        display((self.center or self._node(name).client).cancel(identity))

    def do_logs(self, line):
        """logs 节点 任务ID：读取实时日志末尾。"""
        name, identity = words(line)
        for key, value in (self.center or self._node(name).client).logs(identity).items():
            print(key + ":\n" + value)

    def do_fetch(self, line):
        """fetch 节点 任务ID 结果目录：回收结果并清理节点任务目录。"""
        name, identity, output = words(line)
        result = (self.center or self._node(name).client).fetch(identity, output)
        print(result["status"], result["output_dir"])

    def do_sync(self, line):
        """sync 目录：将模型/只读文件同步到在线节点，返回内容ID。"""
        print(self.dispatch.sync(words(line)[0]))

    def do_restart(self, line):
        """restart 节点：空闲时重新加载远程 worker 的源码和配置。"""
        name = words(line)[0]
        display(self.center.restart(name) if self.center else self._node(name).client.restart())

    def do_wait(self, _line):
        """等待本终端已提交操作结束；Ctrl+C 返回提示符。"""
        for future in self.futures:
            future.result()

    def close(self):
        self.closing.set()
        if not self.center:
            self.pool.close(cancel=True)
        for identity, client in list(self.handles.items()):
            try:
                client.cancel(identity)
            except RemoteError:
                pass  # completed tasks have already been collected and removed
        self.executor.shutdown(wait=True, cancel_futures=True)


class WorkerShell(Shell):
    prompt = "算力> "

    def __init__(self, worker):
        super().__init__()
        self.worker = worker

    def do_status(self, _line):
        """查看本节点CPU预算和实时占用。"""
        value = self.worker.health()
        display({k: value[k] for k in ("node", "platform", "cpus", "logical_cpus", "used_cpus", "cpu_percent", "queued",
                                      "memory_total_mb", "memory_available_mb", "memory_reserved_mb", "rss_mb", "paused", "tags")})

    def do_memory(self, line):
        """memory MiB：调整本机任务内存总预算。"""
        self.worker.control({"memory_mb": int(line)})
        self.do_status("")

    def do_jobs(self, _line):
        """列出本节点任务。"""
        with self.worker.condition:
            table([j["result"] for j in self.worker.jobs.values()], ["id", "status", "label", "cpus"])

    def do_cpus(self, line):
        """cpus N：调整总CPU预算。"""
        self.worker.control({"cpus": int(line)})
        self.do_status("")

    def do_pause(self, _line):
        """暂停启动新任务，当前任务继续。"""
        self.worker.control({"paused": True})

    def do_resume(self, _line):
        """恢复启动新任务。"""
        self.worker.control({"paused": False})

    def do_cancel(self, line):
        """cancel 任务ID|all：取消任务及其子进程。"""
        with self.worker.condition:
            identities = list(self.worker.jobs) if line.strip() == "all" else [line.strip()]
        for identity in identities:
            self.worker.cancel(identity)
        print("已发出取消。")

    def do_logs(self, line):
        """logs 任务ID：读取实时日志末尾。"""
        for key, value in self.worker.logs(line.strip()).items():
            print(key + ":\n" + value)

    def do_delete(self, line):
        """delete 任务ID：删除已经结束且不再需要的任务文件。"""
        display(self.worker.delete(line.strip()))


def load_nodes(path=None, url=None):
    if url:
        token = os.environ.get('REMOTE_COMPUTE_TOKEN', '')
        return {"nodes": [{"name": Client(url, token).health()['node'], "url": url, "token": token}]}
    path = Path(path) if path else ROOT / "nodes.local.json"
    value = read_json(path) if path.exists() else {"nodes": [{"name": socket.gethostname(), "url": "http://127.0.0.1:26134"}]}
    for node in value["nodes"]:
        node.setdefault("token", os.environ.get("REMOTE_COMPUTE_TOKEN", ""))
        try:
            node['name'] = Client(node['url'], node['token'], timeout=2).health()['node']
        except RemoteError:
            pass
    return value


def run_scheduler(args, local_url=None):
    config = load_nodes(args.nodes, args.url)
    if local_url:
        if not args.nodes and not args.url and not (ROOT / "nodes.local.json").exists():
            config["nodes"] = []
        if not any(n["url"].rstrip("/") == local_url for n in config["nodes"]):
            name = socket.gethostname()
            existing = next((n for n in config['nodes'] if n['name'] == name), None)
            if existing is not None:
                existing['url'] = local_url
            else:
                config["nodes"].append({"name": name, "url": local_url, "token": args.local_token})
    pool = Pool(config["nodes"], policy=args.policy or config.get("policy", "adaptive"))
    coordinator = api = api_thread = None
    dispatch = None
    try:
        if args.url:
            dispatch = Client(args.url, os.environ.get("REMOTE_COMPUTE_TOKEN", ""))
            if dispatch.health().get("role") != "scheduler":
                dispatch = None  # existing direct-worker CLI calls retain their route
        else:
            from .ramdisk import scratch
            root = Path(args.root) / "scheduler" if args.root else scratch('Remote-Compute') / 'scheduler'
            coordinator = Coordinator(pool, root, token=args.local_token)
            port = args.center_port if args.center_port is not None else (0 if args.port == 0 else 26133)
            if args.mode == "scheduler" and args.port is not None and args.center_port is None:
                port = args.port
            api = Server((args.host or "127.0.0.1", port), coordinator)
            api_thread = threading.Thread(target=api.serve_forever, kwargs={"poll_interval": 0.1}, daemon=True)
            api_thread.start()
            print(f"调度中心 http://{args.host or '127.0.0.1'}:{api.server_port}", flush=True)
            dispatch = Client(f"http://127.0.0.1:{api.server_port}", args.local_token)
        if args.action is None:
            if args.headless:
                try:
                    while api_thread is None or api_thread.is_alive():
                        time.sleep(0.5)
                except KeyboardInterrupt:
                    return 0
            shell = SchedulerShell(pool, dispatch)
            try:
                shell.do_nodes("")
                shell.loop(dispatch.health if dispatch else None, dispatch.url if dispatch else '')
            finally:
                shell.close()
            return 0
        if args.action in {"run", "spice", "rtl"}:
            task, options = prepare(args.action, args)
            result = (dispatch or pool).run(task, **options)
            display(result)
            return 0 if result["status"] == "succeeded" else 1
        if args.action == "batch":
            results = batch(dispatch or pool, args.source, args.output)
            display([{k: x.get(k) for k in ("id", "status", "assigned_node", "elapsed", "output_dir")} for x in results])
            return 0 if all(x["status"] == "succeeded" for x in results) else 1
        if args.action == "nodes":
            display(dispatch.nodes() if dispatch else pool.snapshot())
        elif args.action == "jobs":
            display(dispatch.status() if dispatch else {n.name: n.client.status() for n in pool.nodes if not n.error})
        elif args.action == "sync":
            print((dispatch or pool).sync(args.source))
        elif args.action in MACHINE_ACTIONS:
            client = dispatch or next(n.client for n in pool.nodes if n.name == args.node)
            value = machine_action(client, args)
            print(value) if isinstance(value, str) else display(value)
            return 1 if isinstance(value, dict) and value.get("status") in {"failed", "cancelled", "timed_out", "interrupted"} else 0
        elif args.action == "restart":
            display(dispatch.restart(args.node) if dispatch else next(n.client for n in pool.nodes if n.name == args.node).restart())
        elif args.action == 'control':
            changes = {key: getattr(args, key) for key in ('cpus', 'memory_mb', 'paused') if getattr(args, key) is not None}
            display(dispatch.control(node=args.node, worker=changes) if dispatch else next(n.client for n in pool.nodes if n.name == args.node).control(**changes))
        else:
            client = dispatch or next(n.client for n in pool.nodes if n.name == args.node)
            method = getattr(client, args.action)
            display(method(args.identity, args.output) if args.action == "fetch" else method(args.identity))
        return 0
    except KeyboardInterrupt:
        print("\n正在取消本调度器持有的任务。")
        pool.close(cancel=True)
        return 130
    finally:
        if coordinator:
            coordinator.close()
        if api:
            api.shutdown()
            api_thread.join()
            api.server_close()
        if not pool.closed:
            pool.close()


def check_tools(config):
    env = dict(os.environ, **config.get("env", {}))
    tools = {"python": [sys.executable], "ngspice": ["ngspice"],
             "verilator": [sys.executable, str(ROOT / "remote_compute/rtl.py")]}
    tools.update(config.get("tools", {}))
    result = []
    for name, prefix in tools.items():
        prefix = [prefix] if isinstance(prefix, str) else prefix
        try:
            run = subprocess.run([*prefix, "--version"], env=env, capture_output=True,
                                 text=True, errors="replace", timeout=10)
            result.append({"tool": name, "ready": run.returncode == 0, "version": (run.stdout + run.stderr).strip()[:500]})
        except (OSError, subprocess.TimeoutExpired) as error:
            result.append({"tool": name, "ready": False, "error": str(error)})
    display(result)
    return 0 if all(x["ready"] for x in result) else 1


def supervise(argv):
    """Keep a headless worker launch alive across explicit remote reloads."""
    while True:
        process = subprocess.Popen([sys.executable, "-B", str(ROOT / "main.py"), "--service-child", *argv])
        while True:
            try:
                code = process.wait()
                break
            except KeyboardInterrupt:
                continue  # the child receives the same console signal
        if code != 75:
            return code
        # Live controls are saved to the device config. Replaying the original
        # launch override would silently undo the operator's latest selection.
        cleaned, skip = [], False
        for arg in argv:
            if skip:
                skip = False
            elif arg in {'--cpus', '--memory-mb'}:
                skip = True
            elif not arg.startswith(('--cpus=', '--memory-mb=')):
                cleaned.append(arg)
        argv = cleaned
        print("正在重新加载 worker 源码和配置。", flush=True)


def main(argv=None):
    invocation = list(sys.argv[1:] if argv is None else argv)
    parser = argparse.ArgumentParser(description="统一入口：总调度器、本机调度+算力、远程算力提供者")
    parser.add_argument("--mode", choices=("scheduler", "hybrid", "worker"), help="默认 hybrid；--url 连接现有服务时默认 scheduler")
    parser.add_argument("--config", help="本机工具链/共享目录配置 JSON")
    parser.add_argument("--nodes", help="调度器节点配置 JSON")
    parser.add_argument("--url", help="连接现有调度中心或直接连接算力提供者")
    parser.add_argument("--policy", choices=sorted(Pool.POLICIES))
    parser.add_argument("--host")
    parser.add_argument("--port", type=int)
    parser.add_argument("--center-port", type=int, help="调度中心HTTP端口，默认26133")
    parser.add_argument("--cpus", type=lambda v: 0 if v == "auto" else int(v), help="本机算力预算")
    parser.add_argument("--memory-mb", type=int, dest="worker_memory_mb", help="本机提供者的任务内存总预算，MiB")
    parser.add_argument("--root", help="节点任务/资产目录；默认使用现有内存盘或系统临时目录")
    parser.add_argument("--headless", action="store_true", help="启动后台服务，不打开交互终端；console=true可启用设备终端")
    parser.add_argument("--service-child", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument('--on-port-conflict', choices=('ask', 'reuse', 'kill', 'admin-kill', 'cancel'),
                        default='ask', help='端口冲突处理；交互终端默认询问，admin-kill请求管理员权限')
    parser.add_argument("--check", action="store_true", help="检查本机仿真工具")
    parser.add_argument("--test", action="store_true", help="运行核心回归")
    parser.add_argument("--test-native", action="store_true", help="同时验证真实ngspice和Verilator")
    sub = parser.add_subparsers(dest="action")
    sub.add_parser("nodes")
    sub.add_parser("jobs")
    item = sub.add_parser('ramdisk', help='检查/创建每设备唯一8GiB共享盘')
    item.add_argument('--install', action='store_true', help='管理员安装已放入.tools/imdisk的签名驱动')
    item = sub.add_parser('update', help='从统一GitHub仓库快进更新当前工程')
    item.add_argument('--check', action='store_true', dest='update_check')
    item = sub.add_parser('setup', help='安装本工程内的科学运行时；工具存放于.tools，不上传仓库')
    item.add_argument('--ngspice-source', help='复用已有ngspice目录，省去下载')
    item.add_argument('--rtl-source', help='提取已有MSYS2的GCC/Verilator闭包')
    item = sub.add_parser('control', help='在线调整设备资源，保存到设备配置')
    item.add_argument('--node', default=socket.gethostname())
    item.add_argument('--cpus', type=int)
    item.add_argument('--memory-mb', type=int)
    item.add_argument('--paused', choices=(True, False), type=lambda x: x == 'true')
    item = sub.add_parser("restart")
    item.add_argument("--node", default=socket.gethostname())
    for kind in ("run", "spice", "rtl"):
        sub.add_parser(kind, parents=[submission_parser(kind)], add_help=False)
    for action in sorted(MACHINE_ACTIONS):
        sub.add_parser(action, parents=[machine_parser(action)], add_help=False)
    item = sub.add_parser("batch")
    item.add_argument("source")
    item.add_argument("output")
    item = sub.add_parser("sync")
    item.add_argument("source")
    for action in ("cancel", "logs", "fetch", "delete"):
        item = sub.add_parser(action)
        item.add_argument("identity")
        item.add_argument("--node", default=socket.gethostname())
        if action == "fetch":
            item.add_argument("--output", required=True)
    args = parser.parse_args(invocation)
    worker = server = thread = None
    restarting = threading.Event()
    try:
        path = Path(args.config) if args.config else ROOT / "worker.local.json"
        if args.config and not path.exists():
            raise ValueError("Missing worker config: " + str(path))
        config = read_json(path) if path.exists() else {}
        args.mode = args.mode or ('scheduler' if args.url else config.get('mode', 'hybrid'))
        if args.mode not in {'scheduler', 'hybrid', 'worker'}:
            raise ValueError('mode must be scheduler, hybrid or worker')
        if args.action == 'ramdisk':
            from .ramdisk import ensure_shared, install_driver
            print(install_driver() if args.install else ensure_shared())
            return 0
        if args.action == 'update':
            from .repository import update
            display(update(check=args.update_check))
            return 0
        if args.action == 'setup':
            from .setup_tools import install, copy_rtl
            result = install(ngspice_source=args.ngspice_source)
            if args.rtl_source:
                result['env'].update(copy_rtl(args.rtl_source)['env'])
            saved = {**config, 'tools': {**config.get('tools', {}), **result['tools']},
                     'env': {**config.get('env', {}), **result['env']}}
            atomic_json(path, saved)
            display(result)
            return 0
        if args.test or args.test_native:
            import unittest
            if args.test_native:
                os.environ["REMOTE_COMPUTE_NATIVE"] = "1"
                if path.exists():
                    os.environ["REMOTE_COMPUTE_TEST_CONFIG"] = str(path.resolve())
            suite = unittest.defaultTestLoader.discover(str(ROOT / "tests"))
            return 0 if unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful() else 1
        if args.check:
            return check_tools(config)
        if args.mode == "worker" and not args.service_child:
            return supervise(invocation)
        if config.get('console') and sys.stdin.isatty():
            args.headless = False
        if getattr(args, "detach", False) and not args.url:
            raise ValueError("--detach 使用 --url 连接持续运行的中心或提供者")
        args.local_token = os.environ.get("REMOTE_COMPUTE_TOKEN", config.get("token", ""))
        if args.mode == "worker" and args.action:
            raise ValueError("worker 模式使用本地操作终端；提交任务使用 scheduler/hybrid 模式")
        existing_worker = None
        if not args.url:
            if args.mode != 'worker':
                center_port = args.center_port if args.center_port is not None else (0 if args.port == 0 else 26133)
                if args.mode == 'scheduler' and args.port is not None and args.center_port is None:
                    center_port = args.port
                existing = resolve(args.host or '127.0.0.1', center_port, policy=args.on_port_conflict,
                                   token=args.local_token, reuse_role='scheduler')
                if existing:
                    args.mode, args.url = 'scheduler', existing
            if args.mode != 'scheduler':
                existing_worker = resolve('127.0.0.1' if args.mode == 'hybrid' else args.host or config.get('host', '127.0.0.1'),
                                          args.port if args.port is not None else config.get('port', 26134 if args.mode == 'hybrid' else 26133),
                                          policy=args.on_port_conflict, token=args.local_token,
                                          reuse_role='worker' if args.mode == 'hybrid' else None)
        if args.mode != "scheduler" and not existing_worker:
            port = args.port if args.port is not None else config.get("port", 26134 if args.mode == "hybrid" else 26133)
            from .ramdisk import scratch
            root = args.root or config.get("root") or scratch('Remote-Compute') / str(port)
            options = {key: config[key] for key in ("tools", "shared", "env", "tags", "memory_mb", "memory_reserve_mb", "cpu_limit") if key in config}
            if args.worker_memory_mb is not None:
                options["memory_mb"] = args.worker_memory_mb
            args.local_token = os.environ.get("REMOTE_COMPUTE_TOKEN", config.get("token", ""))
            options["token"] = args.local_token
            worker = Worker(root, args.cpus if args.cpus is not None else config.get("cpus", 0), **options)
            def save_settings(changes):
                if changes:
                    saved = read_json(path) if path.exists() else {}
                    atomic_json(path, {**saved, **changes})
            if args.config or port != 0:
                worker.save_settings = save_settings
                initial = {}
                if args.cpus is not None:
                    initial['cpus'] = worker.capacity
                if args.worker_memory_mb is not None:
                    initial['memory_mb'] = worker.memory_capacity
                save_settings(initial)
            def restart_worker(*, check=False):
                if check:
                    with worker.condition:
                        if worker.stopping or any(j["result"]["status"] not in TERMINAL for j in worker.jobs.values()):
                            raise Conflict("Worker has active jobs; wait for them to finish before restarting")
                        worker.stopping = True
                        worker.condition.notify_all()
                else:
                    restarting.set()
                    threading.Thread(target=server.shutdown, daemon=True).start()
            host = '127.0.0.1' if args.mode == 'hybrid' else args.host or config.get("host", "127.0.0.1")
            reloadable = args.mode == "worker" and args.service_child
            worker.restart_available = reloadable
            server = Server((host, port), worker, restart=restart_worker if reloadable else None)
            print(f"算力提供者 http://{host}:{server.server_port}  CPU={worker.capacity}  工作目录={worker.root}", flush=True)
            thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.1}, daemon=True)
            thread.start()
        if args.mode == "worker":
            if args.headless:
                try:
                    while thread.is_alive():
                        thread.join(0.5)
                except KeyboardInterrupt:
                    pass
            else:
                WorkerShell(worker).loop(worker.health, f'http://{host}:{server.server_port}', thread.is_alive)
            return 75 if restarting.is_set() else 0
        return run_scheduler(args, f"http://127.0.0.1:{server.server_port}" if server else existing_worker)
    except Cancelled as error:
        print(error)
        return 0
    except (ValueError, OSError, RuntimeError, StopIteration) as error:
        print("操作失败:", error, file=sys.stderr)
        if args.url and ('10061' in str(error) or 'Connection refused' in str(error)):
            print('调度中心未启动。本机请运行 start.cmd（Windows）、./start.sh（Linux）或 python main.py；统一连接地址 http://127.0.0.1:26133。', file=sys.stderr)
        return 2
    finally:
        if server:
            server.shutdown()
            if thread:
                thread.join()
            server.server_close()
        if worker:
            worker.close()
