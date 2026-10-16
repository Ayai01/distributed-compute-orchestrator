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

from .client import Pool, RemoteError
from .tasks import ngspice, verilator
from .worker import Server, Worker

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
    output = Path(args.output) if args.output else ROOT / "results" / (time.strftime("%Y%m%d-%H%M%S-") + uuid.uuid4().hex[:8])
    return task, {"inputs": inputs, "output_dir": output, "keep": args.keep}


def batch(pool, source, output):
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
        task, inputs = prepared[index]
        results[index] = pool.run(task, inputs=inputs, output_dir=Path(output) / str(index))
    order = sorted(range(len(entries)), key=lambda i: -prepared[i][0].get("priority", 0))
    with ThreadPoolExecutor(max_workers=max(1, min(len(entries), sum(n.capacity for n in pool.nodes) or 1))) as executor:
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

    def loop(self):
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

    def __init__(self, pool):
        super().__init__()
        self.pool = pool
        self.executor = ThreadPoolExecutor(max_workers=8, thread_name_prefix="console-submit")
        self.futures = []

    def _node(self, name):
        return next(n for n in self.pool.nodes if n.name == name)

    def _start(self, function, *args, **kwargs):
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
            except Exception as error:
                print(f"\n#{number} 操作失败: {error}")
        future.add_done_callback(finished)

    def do_nodes(self, _line):
        """查看节点预算、占用、权重和分配状态。"""
        self.pool.refresh()
        value = self.pool.snapshot()
        print("分配策略:", value["policy"])
        table(value["nodes"], ["name", "url", "capacity", "reserved", "used_cpus", "weight", "enabled", "paused", "error"])

    def do_jobs(self, line):
        """jobs [节点]：查看任务；id 可用于 logs/cancel/fetch。"""
        rows = []
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
        def accepted(client, identity):
            print(f"\n已接单 {client.url}  {identity}")
        self._start(self.pool.run, task, **options, on_submit=accepted)

    def do_run(self, line):
        """run 任务.json [--input 目录] [--output 目录] [--node 节点] [--cpus N] [--priority N]"""
        self._submit("run", line)

    def do_spice(self, line):
        """spice 网表.spice [--cpus N] [--node 节点] [--output 目录]"""
        self._submit("spice", line)

    def do_rtl(self, line):
        """rtl 目录 --top tb --sources tb.sv [--cpus N] [--output 目录]"""
        self._submit("rtl", line)

    def do_batch(self, line):
        """batch 清单.json 结果目录：按优先级和可用算力并行分配。"""
        source, output = words(line)
        self._start(batch, self.pool, source, output)

    def do_policy(self, line):
        """policy adaptive|balanced|round_robin：切换自适应、均衡或轮询。"""
        self.pool.configure(policy=line.strip())
        print("分配策略:", self.pool.policy)

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
                value = int(value) if key == "limit" else float(value)
            changes[key] = value
        self.pool.configure(name, **changes)
        self.do_nodes("")

    def do_add(self, line):
        """add 名称 http://设备IP:8765：加入节点，token 从当前环境变量读取。"""
        name, url = words(line)
        self.pool.add(name, url, token=os.environ.get("REMOTE_COMPUTE_TOKEN", ""))
        self.do_nodes("")

    def do_save(self, _line):
        """保存当前节点和策略到 nodes.local.json，下次启动自动加载。"""
        value = {"policy": self.pool.policy, "nodes": [
            {"name": n.name, "url": n.client.url, "weight": n.weight, "limit": n.limit,
             "enabled": n.enabled, **({"token": n.client.token} if n.client.token else {})} for n in self.pool.nodes]}
        path = ROOT / "nodes.local.json"
        path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print("已保存:", path)

    def do_pause(self, line):
        """pause 节点：暂停该提供者的新任务，已运行任务继续。"""
        self._node(line.strip()).client.control(paused=True)
        self.pool.refresh()

    def do_resume(self, line):
        """resume 节点：恢复该提供者接单。"""
        self._node(line.strip()).client.control(paused=False)
        self.pool.refresh()

    def do_cpus(self, line):
        """cpus 节点 N：调整提供者的总CPU预算，现有任务自然收尾。"""
        name, count = words(line)
        self._node(name).client.control(cpus=int(count))
        self.pool.refresh()

    def do_cancel(self, line):
        """cancel 节点 任务ID：取消指定任务及其子进程。"""
        name, identity = words(line)
        display(self._node(name).client.cancel(identity))

    def do_logs(self, line):
        """logs 节点 任务ID：读取实时日志末尾。"""
        name, identity = words(line)
        for key, value in self._node(name).client.logs(identity).items():
            print(key + ":\n" + value)

    def do_fetch(self, line):
        """fetch 节点 任务ID 结果目录：回收结果并清理节点任务目录。"""
        name, identity, output = words(line)
        result = self._node(name).client.fetch(identity, output)
        print(result["status"], result["output_dir"])

    def do_sync(self, line):
        """sync 目录：将模型/只读文件同步到在线节点，返回内容ID。"""
        print(self.pool.sync(words(line)[0]))

    def do_wait(self, _line):
        """等待本终端已提交操作结束；Ctrl+C 返回提示符。"""
        for future in self.futures:
            future.result()

    def close(self):
        self.pool.close(cancel=True)
        self.executor.shutdown(wait=True, cancel_futures=True)


class WorkerShell(Shell):
    prompt = "算力> "

    def __init__(self, worker):
        super().__init__()
        self.worker = worker

    def do_status(self, _line):
        """查看本节点CPU预算和实时占用。"""
        value = self.worker.health()
        display({k: value[k] for k in ("node", "platform", "cpus", "used_cpus", "queued", "paused", "tags")})

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
        return {"nodes": [{"name": "local", "url": url, "token": os.environ.get("REMOTE_COMPUTE_TOKEN", "")}]}
    path = Path(path) if path else ROOT / "nodes.local.json"
    value = read_json(path) if path.exists() else {"nodes": [{"name": "local", "url": "http://127.0.0.1:8765"}]}
    for node in value["nodes"]:
        node.setdefault("token", os.environ.get("REMOTE_COMPUTE_TOKEN", ""))
    return value


def run_scheduler(args, local_url=None):
    config = load_nodes(args.nodes, args.url)
    if local_url:
        if not args.nodes and not args.url and not (ROOT / "nodes.local.json").exists():
            config["nodes"] = []
        if not any(n["url"].rstrip("/") == local_url for n in config["nodes"]):
            name = "local" if all(n["name"] != "local" for n in config["nodes"]) else "this-device"
            config["nodes"].append({"name": name, "url": local_url,
                                     "token": args.local_token})
    pool = Pool(config["nodes"], policy=args.policy or config.get("policy", "adaptive"))
    try:
        if args.action is None:
            shell = SchedulerShell(pool)
            try:
                shell.do_nodes("")
                shell.loop()
            finally:
                shell.close()
            return 0
        if args.action in {"run", "spice", "rtl"}:
            task, options = prepare(args.action, args)
            result = pool.run(task, **options)
            display(result)
            return 0 if result["status"] == "succeeded" else 1
        if args.action == "batch":
            results = batch(pool, args.source, args.output)
            display([{k: x.get(k) for k in ("id", "status", "assigned_node", "elapsed", "output_dir")} for x in results])
            return 0 if all(x["status"] == "succeeded" for x in results) else 1
        if args.action == "nodes":
            display(pool.snapshot())
        elif args.action == "jobs":
            display({n.name: n.client.status() for n in pool.nodes if not n.error})
        elif args.action == "sync":
            print(pool.sync(args.source))
        else:
            client = next(n.client for n in pool.nodes if n.name == args.node)
            method = getattr(client, args.action)
            display(method(args.identity, args.output) if args.action == "fetch" else method(args.identity))
        return 0
    except KeyboardInterrupt:
        print("\n正在取消本调度器持有的任务。")
        pool.close(cancel=True)
        return 130
    finally:
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


def main(argv=None):
    parser = argparse.ArgumentParser(description="统一入口：总调度器、本机调度+算力、远程算力提供者")
    parser.add_argument("--mode", choices=("scheduler", "hybrid", "worker"), default="hybrid")
    parser.add_argument("--config", help="本机工具链/共享目录配置 JSON")
    parser.add_argument("--nodes", help="调度器节点配置 JSON")
    parser.add_argument("--url", help="直接连接一个算力提供者")
    parser.add_argument("--policy", choices=sorted(Pool.POLICIES))
    parser.add_argument("--host")
    parser.add_argument("--port", type=int)
    parser.add_argument("--cpus", type=lambda v: 0 if v == "auto" else int(v), help="本机算力预算")
    parser.add_argument("--root", help="节点任务/资产目录；默认使用现有内存盘或系统临时目录")
    parser.add_argument("--headless", action="store_true", help="算力提供者不打开交互终端")
    parser.add_argument("--check", action="store_true", help="检查本机仿真工具")
    parser.add_argument("--test", action="store_true", help="运行核心回归")
    parser.add_argument("--test-native", action="store_true", help="同时验证真实ngspice和Verilator")
    sub = parser.add_subparsers(dest="action")
    sub.add_parser("nodes")
    sub.add_parser("jobs")
    for kind in ("run", "spice", "rtl"):
        sub.add_parser(kind, parents=[submission_parser(kind)], add_help=False)
    item = sub.add_parser("batch")
    item.add_argument("source")
    item.add_argument("output")
    item = sub.add_parser("sync")
    item.add_argument("source")
    for action in ("cancel", "logs", "fetch", "delete"):
        item = sub.add_parser(action)
        item.add_argument("identity")
        item.add_argument("--node", default="local")
        if action == "fetch":
            item.add_argument("--output", required=True)
    args = parser.parse_args(argv)
    worker = server = thread = None
    try:
        path = Path(args.config) if args.config else ROOT / "worker.local.json"
        if args.config and not path.exists():
            raise ValueError("Missing worker config: " + str(path))
        config = read_json(path) if path.exists() else {}
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
        if args.mode != "worker" and args.headless and args.action is None:
            raise ValueError("--headless 调度模式请指定 run/batch 等命令")
        if args.mode == "worker" and args.action:
            raise ValueError("worker 模式使用本地操作终端；提交任务使用 scheduler/hybrid 模式")
        if args.mode != "scheduler":
            port = args.port if args.port is not None else config.get("port", 8765)
            temporary = Path("Z:/") if os.name == "nt" and Path("Z:/").is_dir() else Path(tempfile.gettempdir())
            root = args.root or config.get("root") or temporary / "Remote-Compute" / str(port)
            options = {key: config[key] for key in ("tools", "shared", "env", "tags") if key in config}
            args.local_token = os.environ.get("REMOTE_COMPUTE_TOKEN", config.get("token", ""))
            options["token"] = args.local_token
            worker = Worker(root, args.cpus if args.cpus is not None else config.get("cpus", 0), **options)
            host = args.host or config.get("host", "127.0.0.1")
            server = Server((host, port), worker)
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
                WorkerShell(worker).loop()
            return 0
        return run_scheduler(args, f"http://127.0.0.1:{server.server_port}" if server else None)
    except (ValueError, OSError, RuntimeError, StopIteration) as error:
        print("操作失败:", error, file=sys.stderr)
        return 2
    finally:
        if server:
            server.shutdown()
            if thread:
                thread.join()
            server.server_close()
        if worker:
            worker.close()
