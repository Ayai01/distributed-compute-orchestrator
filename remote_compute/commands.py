"""Operator commands and task argument parsing; no service startup or rendering."""

import argparse
import cmd
from concurrent.futures import ThreadPoolExecutor
import json
import os
import shutil
from pathlib import Path
import shlex
import sys
import threading
import time
import uuid

from .client import Client, Pool, RemoteError
from .tasks import ngspice, verilator

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
    from .view import _width, _cell, wrap_lines
    names=dict(name='设备',node='设备',capacity='预算',used_cpus='分配',cpu_percent='CPU%',
               memory_available_mb='可用RAM',memory_reserved_mb='预留RAM',memory_mb='RAM',
               weight='权重',enabled='启用',paused='暂停',status='状态',cpus='线程',label='标签',id='ID')
    headers=[names.get(key,key) for key in columns]
    rows = [[str(row.get(key, "")) for key in columns] for row in rows]
    widths = [max([_width(headers[i]), *(_width(row[i]) for row in rows)]) for i in range(len(columns))]
    width = getattr(sys.stdout, 'output_width', shutil.get_terminal_size((110,32)).columns-1)
    available=max(1,width-2*(len(columns)-1))
    while sum(widths)>available:
        candidates=[i for i,size in enumerate(widths) if size>max({'id':32,'label':15}.get(columns[i],4),_width(headers[i]))]
        if not candidates:
            break
        index=max(candidates,key=lambda i: widths[i])
        widths[index]-=1
    if sum(widths)>available:
        for row in rows:
            for key, value in zip(headers, row):
                print('\n'.join(wrap_lines(key+': '+value, width)))
            print()
        return
    print("  ".join(_cell(key,widths[i]) for i,key in enumerate(headers)))
    for row in rows:
        print("  ".join(_cell(value,widths[i]) for i,value in enumerate(row)))


def submission_parser(kind):
    parser = argparse.ArgumentParser(prog=kind)
    parser.add_argument("source", help="任务 JSON、SPICE 文件或 RTL 目录")
    parser.add_argument("--input", help="要传输的任务输入目录")
    parser.add_argument("--output", help="结果保存目录")
    parser.add_argument("--node", help="固定分配给指定节点")
    parser.add_argument("--cpus", type=int, dest="task_cpus")
    parser.add_argument("--max-cpus", type=int, help="任务线程需求硬上界，包含后续借核")
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
    for key in ("cpus", "max_cpus", "priority", "node"):
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
    command_groups = (('终端','help quit'),)

    def do_help(self, line):
        """help [命令]：按用途查看操作，或查看单条命令用法。"""
        if line.strip():
            return super().do_help(line)
        for title, commands in self.command_groups:
            print(title + '  ' + commands)
        print('输入 help 命令 查看参数；PgUp/PgDn 翻阅输出；↑/↓ 选择历史命令。')

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
                if health and os.name != 'nt' and sys.stdin.isatty():
                    self._poll_input(alive)
                else:
                    self.cmdloop()
                return
            except KeyboardInterrupt:
                print("\n已返回操作终端。")
                self.intro = None

    def _poll_input(self, alive):
        """POSIX canonical input stays interruptible while a service reloads."""
        import select
        if self.intro:
            print(self.intro)
        while alive():
            self.stdout.write(self.prompt)
            self.stdout.flush()
            while alive():
                if select.select([sys.stdin],[],[],.2)[0]:
                    line=sys.stdin.readline()
                    break
            else:
                return
            if self.postcmd(self.onecmd(self.precmd(line.rstrip('\r\n') if line else 'EOF')),line):
                return

    def do_quit(self, _line):
        """退出终端，并结束本入口持有的任务。"""
        return True

    do_exit = do_quit

    def do_EOF(self, line):
        print()
        return self.do_quit(line)


class SchedulerShell(Shell):
    prompt = "调度> "
    command_groups = (('资源','status nodes cpus memory pause resume policy set add save'),
                      ('任务','run spice rtl batch jobs logs cancel fetch wait'),
                      ('机器','exec python ls read put get restart'),('终端','help quit'))

    def __init__(self, pool, center=None):
        super().__init__()
        self.pool = pool
        self.center = center
        self.dispatch = center or pool
        self.handles = {}
        self.closing = threading.Event()
        self.executor = ThreadPoolExecutor(max_workers=8, thread_name_prefix="console-submit")
        self.futures = {}
        self.operation_number = 0

    def _node(self, name):
        return next(n for n in self.pool.nodes if n.name == name)

    def _start(self, function, *args, show_logs=False, **kwargs):
        future = self.executor.submit(function, *args, **kwargs)
        self.operation_number += 1
        number = self.operation_number
        self.futures[number] = future
        print(f"已提交操作 #{number}；终端可继续使用。")
        def finished(done):
            try:
                result = done.result()
                results = result if isinstance(result, list) else [result]
                if not all(isinstance(item,dict) and 'status' in item and 'id' in item for item in results):
                    print(f'\n#{number} 已完成')
                    print(result) if isinstance(result,str) else display(result)
                    return
                for item in results:
                    self.handles.pop(item['id'],None)
                    print(f"\n#{number} {item['status']}  {item.get('assigned_node', '')}  {item['id']}  {item.get('output_dir', '')}")
                    if show_logs:
                        for stream in ("stdout", "stderr"):
                            if item.get(stream):
                                print(stream + ":\n" + item[stream])
            except Exception as error:
                print(f"\n#{number} 操作失败: {error}")
            finally:
                self.futures.pop(number,None)
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
        """jobs [节点|任务ID]：支持4位以上ID前缀，唯一匹配时查看完整记录。"""
        rows = []
        if self.center:
            identity=line.strip()
            if 4<=len(identity)<=32 and all(c in '0123456789abcdef' for c in identity):
                if len(identity)<32:
                    matches=[j for j in self.center.status(brief=True) if j['id'].startswith(identity)]
                    if len(matches)!=1:
                        if not matches:
                            raise ValueError('未找到任务ID: '+identity)
                        table(matches,['id','status','label','cpus'])
                        print('ID前缀匹配多个任务，请增加位数。')
                        return
                    identity=matches[0]['id']
                from .view import wrap_lines
                width=max(20,shutil.get_terminal_size((110,32)).columns-5)
                for row in json.dumps(self.center.status(identity),ensure_ascii=False,indent=2).splitlines():
                    for part in wrap_lines(row,width):
                        print(part)
                return
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
        print("本终端操作:", "  ".join(f"#{i} 进行中" for i in list(self.futures)))

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
            self._start(machine_action,client,args)

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
        """restart [节点] [--wait]：重载中心或节点；--wait等待在途任务完成。"""
        args=words(line)
        wait='--wait' in args
        args=[arg for arg in args if arg!='--wait']
        if len(args)>1 or (not self.center and not args):
            raise ValueError('restart [节点] [--wait]')
        name=args[0] if args else None
        client=self.center or self._node(name).client
        self._start(client.restart,name if self.center else None,wait=wait)

    def do_wait(self, _line):
        """等待本终端已提交操作结束；Ctrl+C 返回提示符。"""
        for future in list(self.futures.values()):
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
        for future in list(self.futures.values()):
            future.cancel()
        self.executor.shutdown(wait=True)


class WorkerShell(Shell):
    prompt = "算力> "
    command_groups = (('资源','status cpus memory pause resume'),
                      ('任务','jobs logs cancel delete'),('终端','help quit'))

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

    def do_jobs(self, line):
        """jobs [任务ID前缀]：列出本节点任务或查看完整记录。"""
        with self.worker.condition:
            rows=[j['result'].copy() for j in self.worker.jobs.values()
                  if not line.strip() or j['result']['id'].startswith(line.strip())]
        if line.strip() and len(rows)==1:
            from .view import wrap_lines
            width=max(20,shutil.get_terminal_size((110,32)).columns-5)
            for row in json.dumps(rows[0],ensure_ascii=False,indent=2).splitlines():
                for part in wrap_lines(row,width):
                    print(part)
        else:
            table(rows,["id", "status", "label", "cpus"])

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
