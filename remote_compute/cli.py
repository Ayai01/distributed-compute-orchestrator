"""The unified Python entry point and its operator consoles."""

import argparse
from contextlib import ExitStack
import os
from pathlib import Path
import subprocess
import sys
import threading
import time
import socket

from .client import Client, Pool, RemoteError
from .coordinator import Coordinator
from .bundle import TERMINAL, atomic_json
from .worker import Conflict, Server, Worker
from .startup import Cancelled, resolve
from .process import keep_awake

ROOT = Path(__file__).resolve().parent.parent


# Preserve the existing Python imports while command ownership lives in commands.
from .commands import (Shell, SchedulerShell, WorkerShell, MACHINE_ACTIONS, read_json,
                       words, display, table, submission_parser, prepare, machine_parser,
                       machine_action, batch)


def load_nodes(path=None, url=None, local_client=None):
    if url:
        token = os.environ.get('REMOTE_COMPUTE_TOKEN', '')
        return {"nodes": [{"name": Client(url, token).health()['node'], "url": url, "token": token}]}
    path = Path(path) if path else ROOT / "nodes.local.json"
    value = read_json(path) if path.exists() else {"nodes": [{"name": socket.gethostname(), "url": 'local://' + socket.gethostname()}]}
    for node in value["nodes"]:
        node.setdefault("token", os.environ.get("REMOTE_COMPUTE_TOKEN", ""))
        if local_client and node.get('name') == socket.gethostname():
            node.update(url=local_client.url, client=local_client)
            continue
        try:
            node['name'] = Client(node['url'], node['token'], timeout=2,ca_file=node.get('ca_file')).health()['node']
        except RemoteError:
            pass
    return value


def run_scheduler(args, local_client=None, restarting=None):
    restarting = restarting or threading.Event()
    config = load_nodes(args.nodes, args.url, local_client)
    if local_client:
        if not args.nodes and not args.url and not (ROOT / "nodes.local.json").exists():
            config["nodes"] = []
        if not any(n["url"].rstrip("/") == local_client.url for n in config["nodes"]):
            name = socket.gethostname()
            existing = next((n for n in config['nodes'] if n['name'] == name), None)
            if existing is not None:
                existing.update(url=local_client.url,client=local_client)
            else:
                config["nodes"].append({"name": name, "url": local_client.url, 'client': local_client})
    pool = Pool(config["nodes"], policy=args.policy or config.get("policy", "adaptive"))
    coordinator = api = api_thread = None
    dispatch = None
    try:
        if args.url:
            dispatch = Client(args.url, os.environ.get("REMOTE_COMPUTE_TOKEN", ""))
            if dispatch.health().get("role") != "scheduler":
                dispatch = None  # existing direct-worker CLI calls retain their route
        else:
            root = Path(args.root) / "scheduler" if args.root else ROOT / '.local/state/scheduler'
            coordinator = Coordinator(pool, root, token=args.local_token)
            port = args.center_port if args.center_port is not None else (args.port if args.port is not None else 26133)
            if args.mode == "scheduler" and args.port is not None and args.center_port is None:
                port = args.port
            def restart_center(*, check=False):
                if check:
                    with coordinator.condition:
                        if coordinator.stopping or any(j['result']['status'] not in TERMINAL for j in coordinator.jobs.values()):
                            raise Conflict('Scheduler has active jobs; wait for them to finish before restarting')
                        coordinator.stopping = True
                        coordinator.condition.notify_all()
                else:
                    restarting.set()
                    threading.Thread(target=api.shutdown, daemon=True).start()
            coordinator.restart_available = args.service_child
            api = Server((args.host or "0.0.0.0", port), coordinator,
                         restart=restart_center if args.service_child else None)
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
                return 75 if restarting.is_set() else 0
            shell = SchedulerShell(pool, dispatch)
            try:
                shell.do_nodes("")
                shell.loop(dispatch.health if dispatch else None, dispatch.url if dispatch else '',
                           api_thread.is_alive if api_thread else lambda: True)
            finally:
                shell.close()
            return 75 if restarting.is_set() else 0
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
            client = dispatch or next(n.client for n in pool.nodes if args.node is None or n.name == args.node)
            display(client.restart(args.node if dispatch else None, wait=args.wait))
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
            version_flag = '-V' if name == 'iverilog' else '--version'
            run = subprocess.run([*prefix, version_flag], env=env, capture_output=True,
                                 text=True, errors="replace", timeout=10)
            result.append({"tool": name, "ready": run.returncode == 0, "version": (run.stdout + run.stderr).strip()[:500]})
        except (OSError, subprocess.TimeoutExpired) as error:
            result.append({"tool": name, "ready": False, "error": str(error)})
    display(result)
    return 0 if all(x["ready"] for x in result) else 1


def supervise(argv):
    """Keep the service terminal alive across requested code/config reloads."""
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
        print("正在重新加载服务源码和配置。", flush=True)


def handoff_python(invocation):
    """Let a reloaded child replace its old supervisor through a normal exit."""
    from .setup_tools import preferred_python
    executable=preferred_python()
    if executable.resolve()==Path(sys.executable).resolve():
        return False
    environment=dict(os.environ,PYTHONUTF8='1',PYTHONDONTWRITEBYTECODE='1')
    environment.pop('PYTHONHOME',None)
    environment['PATH']=str(executable.parent)+os.pathsep+str(executable.parent/'Scripts')+os.pathsep+environment.get('PATH','')
    args=[arg for arg in invocation if arg!='--service-child']
    subprocess.Popen([str(executable),'-B',str(ROOT/'main.py'),*args],cwd=ROOT,env=environment)
    return True


def main(argv=None):
    invocation = list(sys.argv[1:] if argv is None else argv)
    parser = argparse.ArgumentParser(description="Distributed Compute Orchestrator (DCO)：调度器、本机调度+算力、独立worker")
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
    parser.add_argument("--root", help="持久任务/资产目录；默认工程内 .local/state，临时物单独使用RAM盘")
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
    item = sub.add_parser('setup', help='复用系统Python安装科学包；原生工具存放于.tools')
    item.add_argument('--ngspice-source', help='复用已有ngspice目录，省去下载')
    item.add_argument('--rtl-source', help='提取已有MSYS2的GCC/Verilator闭包')
    item = sub.add_parser('control', help='在线调整设备资源，保存到设备配置')
    item.add_argument('--node', default=socket.gethostname())
    item.add_argument('--cpus', type=int)
    item.add_argument('--memory-mb', type=int)
    item.add_argument('--paused', choices=(True, False), type=lambda x: x == 'true')
    item = sub.add_parser("restart")
    item.add_argument("--node", help='省略时重载所连接的中心或worker')
    item.add_argument('--wait', action='store_true', help='等待在途任务完成后重载')
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
    lifetime = ExitStack()
    restarting = threading.Event()
    try:
        if (args.service_child and not args.url and args.action is None and
                not (args.test or args.test_native or args.check) and handoff_python(invocation)):
            return 0
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
            from test_compute import calibration_environment
            with calibration_environment():
                return 0 if unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful() else 1
        if args.check:
            return check_tools(config)
        if not args.url and args.action is None:
            # Both the supervisor and its service child own a request: reloads
            # stay covered, and a directly launched child works independently.
            lifetime.enter_context(keep_awake())
        if not args.service_child and not args.url and args.action is None:
            return supervise(invocation)
        if config.get('console') and sys.stdin.isatty():
            args.headless = False
        if getattr(args, "detach", False) and not args.url:
            raise ValueError("--detach 使用 --url 连接持续运行的中心或提供者")
        args.local_token = os.environ.get("REMOTE_COMPUTE_TOKEN", config.get("token", ""))
        if args.url and args.mode=='worker' and args.action is None:
            from .commands import RemoteWorker
            client=Client(args.url,args.local_token,ca_file=config.get('ca_file') or config.get('certfile'))
            if client.health().get('role')!='worker':raise ValueError('Worker console requires a worker URL')
            WorkerShell(RemoteWorker(client)).loop(client.health,client.url)
            return 0
        if args.mode == "worker" and args.action:
            raise ValueError("worker 模式使用本地操作终端；提交任务使用 scheduler/hybrid 模式")
        existing_worker = None
        if not args.url:
            if args.mode != 'worker':
                center_port = args.center_port if args.center_port is not None else (args.port if args.port is not None else 26133)
                if args.mode == 'scheduler' and args.port is not None and args.center_port is None:
                    center_port = args.port
                existing = resolve(args.host or '127.0.0.1', center_port, policy=args.on_port_conflict,
                                   token=args.local_token, reuse_role='scheduler')
                if existing:
                    args.mode, args.url = 'scheduler', existing
            if args.mode == 'worker':
                existing_worker = resolve(args.host or config.get('host', '127.0.0.1'),
                                          args.port if args.port is not None else config.get('port', 26133),
                                          policy=args.on_port_conflict, token=args.local_token,
                                          reuse_role='worker',ca_file=config.get('ca_file') or config.get('certfile'),
                                          tls=bool(config.get('certfile')))
                if existing_worker:
                    from .commands import RemoteWorker
                    client=Client(existing_worker,args.local_token,ca_file=config.get('ca_file') or config.get('certfile'))
                    WorkerShell(RemoteWorker(client)).loop(client.health,client.url)
                    return 0
        if args.mode != "scheduler" and not existing_worker:
            port = args.port if args.port is not None else config.get("port", 26133)
            from .ramdisk import scratch
            root = args.root or config.get("root") or ROOT / '.local/state' / ('local' if args.mode == 'hybrid' else str(port))
            options = {key: config[key] for key in ("tools", "shared", "env", "tags", "memory_mb", "memory_reserve_mb", "cpu_limit", "temp_root") if key in config}
            options.setdefault('temp_root', scratch('Remote-Compute') / 'tmp')
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
            if args.mode == 'worker':
                host = args.host or config.get("host", "127.0.0.1")
                worker.restart_available = args.service_child
                server = Server((host, port), worker, restart=restart_worker if args.service_child else None,
                                certfile=config.get('certfile'),keyfile=config.get('keyfile'))
                worker_url=f'{"https" if config.get("certfile") else "http"}://{host}:{server.server_port}'
                print(f"算力提供者 {worker_url}  CPU={worker.capacity}  工作目录={worker.root}", flush=True)
                thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.1}, daemon=True)
                thread.start()
            else:
                print(f"本机算力（进程内）CPU={worker.capacity}  工作目录={worker.root}", flush=True)
        if args.mode == "worker":
            if args.headless:
                try:
                    while thread.is_alive():
                        thread.join(0.5)
                except KeyboardInterrupt:
                    pass
            else:
                WorkerShell(worker).loop(worker.health, worker_url, thread.is_alive)
            return 75 if restarting.is_set() else 0
        if worker:
            from .local import LocalClient
        return run_scheduler(args, LocalClient(worker) if worker else None, restarting)
    except Cancelled as error:
        print(error)
        return 0
    except (ValueError, OSError, RuntimeError, StopIteration) as error:
        print("操作失败:", error, file=sys.stderr)
        if args.url and ('10061' in str(error) or 'Connection refused' in str(error)):
            print('调度中心未启动。本机请运行 start.cmd（Windows）、./start.sh（Linux）或 python main.py；统一连接地址 http://127.0.0.1:26133。', file=sys.stderr)
        return 2
    finally:
        try:
            if server:
                server.shutdown()
                if thread:
                    thread.join()
                server.server_close()
            if worker:
                worker.close()
        finally:
            lifetime.close()
