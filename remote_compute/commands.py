"""Operator commands and task argument parsing; no service startup or rendering."""

import argparse
import cmd
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
import json
import math
import os
import shutil
from pathlib import Path
import shlex
import socket
import sys
import threading
import time
import uuid
from urllib.parse import urlsplit

from .client import Client, Pool, RemoteError
from .bundle import atomic_json
from .tasks import ngspice, verilator
from .records import active_items
from .auth import node_url
from .startup import single_instance, Cancelled

ROOT = Path(__file__).resolve().parent.parent


def read_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))


@contextmanager
def node_configuration(path):
    """One read/merge/write across consoles and the background node monitor."""
    try:
        with single_instance('node-config:'+str(Path(path).resolve()),wait=10):
            value=read_json(path) if Path(path).exists() else {}
            yield value
            atomic_json(path,value,private=True)
    except Cancelled as error:
        raise OSError('Node configuration is busy') from error


def save_detached_node(path, node):
    """Persist automatic connection state; keep budgets, credentials and peers."""
    if not Path(path).exists():return
    with node_configuration(path) as value:
        for row in value.get('nodes',[]):
            if row.get('name')==node.name or row.get('url','').rstrip('/')==node.client.url:
                row.update(enabled=node.enabled,url=node.client.url)
                if getattr(node.client,'ca_file',None):row['ca_file']=node.client.ca_file
                else:row.pop('ca_file',None)
                if node.detached:
                    row['reconnect']={'attempts':node.reconnect_attempts,'after':node.reconnect_after}
                else:
                    row.pop('reconnect',None)


def scan_parser():
    parser=argparse.ArgumentParser(prog='scan')
    parser.add_argument('networks',nargs='*',metavar='IPv4-CIDR')
    parser.add_argument('--port',dest='scan_port',type=int,default=26133)
    parser.add_argument('--timeout',dest='scan_timeout',type=float,default=.7)
    parser.add_argument('--add',dest='scan_add',action='store_true',help='接入本次发现的可用DCO')
    return parser


def scan_devices(args):
    from .discovery import scan
    path=Path(getattr(args,'nodes',None) or ROOT/'nodes.local.json')
    known=read_json(path).get('nodes',[]) if path.exists() else []
    value=scan(args.networks,port=args.scan_port,timeout=args.scan_timeout,known=known)
    print('扫描网段: '+', '.join(value['networks']))
    print('可用DCO: '+str(len(value['nodes'])))
    table(value['nodes'],['name','url'])
    return value['nodes']


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
    parser.add_argument("--cpus", type=int, dest="task_cpus")
    parser.add_argument("--max-cpus", type=int, help="任务线程需求硬上界，包含后续借核")
    parser.add_argument("--memory-mb", type=int, help="任务内存预留，MiB")
    parser.add_argument("--priority", type=int)
    parser.add_argument("--single-thread-weight", type=float, help="单线程偏好，默认1；较高偏单线程、较低偏总吞吐")
    parser.add_argument("--requires", help="节点标签，以逗号分隔")
    parser.add_argument("--accelerator", help='加速器需求 JSON，例如 {"kind":"cuda","memory_mb":8192,"requires":["gpu:training"]}')
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
    for key in ("cpus", "max_cpus", "priority", "single_thread_weight"):
        value = getattr(args, "task_cpus" if key == "cpus" else key)
        if value is not None:
            task[key] = value
    if args.requires:
        task["requires"] = args.requires.split(",")
    if args.accelerator:
        task["accelerator"] = json.loads(args.accelerator)
    if args.memory_mb is not None:
        task["memory_mb"] = args.memory_mb
    output = Path(args.output) if args.output else ROOT / "results" / (time.strftime("%Y%m%d-%H%M%S-") + uuid.uuid4().hex[:8])
    return task, {"inputs": inputs, "output_dir": output, "keep": args.keep}


def cache_parser():
    parser = argparse.ArgumentParser(prog="sync_cache")
    aliases = {'aop_v2': 'aop', 'rgcn-v2': 'rgcn', 'arh-dse': 'arh',
               'arh-dse-v2': 'arh', 'analog-opt-bench-v2': 'bench'}
    parser.add_argument("project", choices=('all', 'aop', 'rgcn', 'arh', 'bench'),
                        type=lambda value: aliases.get(value.casefold(), value.casefold()),
                        help="同步全部工程或指定工程")
    parser.add_argument("--kind", choices=('all', 'research', 'assets'), default='all',
                        help="all同时同步科研缓存和内容资产；research仅科研缓存，assets仅源码/模型")
    return parser


MACHINE_ACTIONS = {"exec", "python", "ls", "read", "put", "get"}

COMMAND_TREE = (
    ('查看', ('status', 'nodes', 'jobs')),
    ('资源', ('cpus', 'memory', 'calibrate', 'pause', 'resume', 'policy', 'set')),
    ('任务', ('run', 'spice', 'rtl', 'batch', 'sync_cache', 'logs', 'cancel', 'fetch', 'wait')),
    ('设备与文件', ('add', 'scan', 'save', 'exec', 'python', 'ls', 'read', 'put', 'get', 'restart')),
    ('终端', ('help', 'quit')),
)


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


def ram_mib(value):
    text = str(value).strip().upper()
    for suffix in ('GIB', 'GB', 'G'):
        if text.endswith(suffix):
            amount = float(text[:-len(suffix)]) * 1024
            if not math.isfinite(amount) or amount < 0 or not amount.is_integer():
                raise ValueError('RAM需要非负整数MiB或G容量，例如8192或8G')
            return int(amount)
    amount = int(text)
    if amount < 0:
        raise ValueError('RAM预算不能为负数；0为自动')
    return amount


def job_identity(rows, prefix):
    if len(prefix) < 4:
        raise ValueError('请输入至少4位任务ID；输入 jobs 查看ID')
    matches = [row['id'] for row in rows if row['id'].startswith(prefix)]
    if len(matches) != 1:
        raise ValueError('任务ID未找到' if not matches else 'ID匹配多个任务，请增加位数：'+', '.join(matches))
    return matches[0]


class Shell(cmd.Cmd):
    intro = "输入 help 查看命令。空行保持当前状态，Ctrl+C 返回提示符，quit 退出。"
    command_groups = (('终端','help quit'),)

    def suggest(self, line, cursor, snapshot):
        """Complete from command definitions and the already displayed snapshot."""
        before = line[:cursor]
        tokens = before.split()
        prefix = tokens[-1] if before and not before[-1].isspace() else ''
        index = len(tokens) - bool(prefix)
        previous = tokens[index-1] if index else ''
        start = cursor - len(prefix)
        end = cursor
        while end < len(line) and not line[end].isspace():
            end += 1
        commands = [name[3:] for name in self.get_names() if name.startswith('do_') and name != 'do_EOF']
        command = tokens[0] if tokens else ''
        method = getattr(self, 'do_' + command, None)
        usage = ((method.__doc__ or '').strip().splitlines() or [command])[0] if method else ''
        nodes = snapshot.get('nodes') or []
        names = [n.get('name') or n.get('node') for n in nodes]
        jobs = [(n.get('name') or n.get('node'), j) for n in nodes for j in n.get('active_jobs') or []]
        jobs.extend((snapshot.get('node'), j) for j in snapshot.get('active_jobs') or [])
        scheduler = hasattr(self, 'pool')
        choices = []
        if index == 0 or command == 'help' and index == 1:
            choices = commands
        elif previous == '--node':
            choices = names
        elif command == 'policy' and index == 1:
            choices = ['adaptive', 'balanced', 'round_robin']
        elif command == 'sync_cache' and previous == '--kind':
            choices = ['all', 'research', 'assets']
        elif command == 'sync_cache' and index == 1:
            choices = ['all', 'aop', 'rgcn', 'arh', 'bench']
        elif command == 'set' and index >= 2:
            choices = ['weight=', 'limit=', 'memory_limit_mb=', 'enabled=true', 'enabled=false']
        elif scheduler and index == 1 and command in {'cpus','memory','calibrate','set','pause','resume','jobs','restart','logs','cancel','fetch'}:
            choices = names + ([j.get('id') for _, j in jobs] if command in {'jobs','logs','cancel','fetch'} else [])
        elif command in {'jobs','logs','cancel','delete','fetch'} and index == (2 if scheduler else 1):
            choices = [j.get('id') for node, j in jobs if not scheduler or node == tokens[1]]
            if not scheduler and command == 'cancel':
                choices.append('all')
        if prefix.startswith('-') or not prefix:
            parser = (submission_parser(command) if command in {'run','spice','rtl'} else
                      machine_parser(command) if command in MACHINE_ACTIONS else
                      cache_parser() if command == 'sync_cache' else None)
            if command=='scan':parser=scan_parser()
            if parser:
                action = parser._option_string_actions.get(previous)
                if action is None or action.nargs == 0:
                    choices += list(parser._option_string_actions)
            elif command == 'restart':
                choices.append('--wait')
        matches = sorted({value for value in choices if value and value.casefold().startswith(prefix.casefold())})
        return dict(start=start, end=end, prefix=prefix, matches=matches, usage=usage)

    def do_help(self, line):
        """help [命令]：按用途查看操作，或查看单条命令用法。"""
        all_commands = line.strip() == 'all'
        if line.strip() and not all_commands:
            return super().do_help(line)
        print('命令树（Tab 补全命令，Ctrl+Tab 翻当前任务）：')
        allowed = {command for _title, commands in self.command_groups for command in commands.split()}
        for title, tree_commands in COMMAND_TREE:
            selected = [command for command in tree_commands if command in allowed and (all_commands or command in {
                'status','jobs','cpus','memory','calibrate','pause','resume','logs','cancel','fetch','sync_cache','restart','help','quit'})]
            if not selected:
                continue
            print('\n'+title)
            for command in selected:
                method = getattr(self, 'do_'+command, None)
                description = (method.__doc__ or '').strip().splitlines()[0] if method else command
                if not description.startswith(command):
                    description = command+'：'+description
                print('  ├─ '+description)
        print('\n常用例子：')
        if hasattr(self, 'pool'):
            print('  status                  查看全部设备资源')
            print('  cpus 16                 将本机预算设为16线程（自动保存）')
            print('  cpus '+socket.gethostname()+' 16   指定设备；其他设备名称可按 Tab 选择')
            print('  memory 8G               将本机任务 RAM 预算设为8 GiB')
            print('  logs ab12               查看4位任务ID的日志')
            print('  fetch ab12 "D:/结果"     保存结果并清理该任务')
            print('  sync_cache all          手动同步所有工程的科研缓存和源码/模型')
            print('  restart --wait          空闲后重新加载本机中心')
        else:
            print('  cpus 16                 将本设备预算设为16线程')
            print('  logs ab12               查看4位任务ID的日志')
        print('  help cpus               查看单条指令的参数和例子')
        print('help all 查看其余指令。Tab 补全；Ctrl+Tab/Shift+Tab 翻任务页；PgUp/PgDn 翻输出；↑/↓ 历史。')

    def default(self, line):
        import difflib
        command = line.split()[0] if line.split() else ''
        commands = [name[3:] for name in self.get_names() if name.startswith('do_')]
        near = difflib.get_close_matches(command, commands, n=1)
        print('未知指令：'+command+'。'+('你可能要输入 '+near[0]+'；' if near else '')+'输入 help 查看操作示例。')

    def emptyline(self):
        pass

    def onecmd(self, line):
        try:
            return super().onecmd(line)
        except SystemExit:
            return False  # argparse --help and syntax errors keep the console open.
        except (ValueError, OSError, RuntimeError, KeyError, StopIteration) as error:
            print("操作失败:", error)
            method = getattr(self, 'do_'+line.split()[0], None) if line.split() else None
            if method and method.__doc__:
                print('用法：'+method.__doc__.strip())

    def loop(self, health=None, url='', alive=lambda: True):
        if health and sys.stdin.isatty() and sys.stdout.isatty():
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
    command_groups = (('查看','status nodes jobs'),('调资源','cpus memory calibrate pause resume'),
                      ('任务','logs cancel fetch wait run spice rtl batch sync_cache'),
                      ('维护','restart policy set add save exec python ls read put get'),('帮助','help quit'))

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
        text = str(name).strip()
        nodes = getattr(self.pool, 'nodes', ())
        if not isinstance(nodes, (list, tuple)):
            # Lightweight command tests and remote-only shells can resolve
            # names at the center; a real Pool still validates IDs locally.
            return type('NodeRef', (), {'name': text})()
        if text.startswith('#') or text.isdigit():
            index = int(text.lstrip('#'))
            node = nodes[index-1] if 1 <= index <= len(nodes) else None
        else:
            node = next((n for n in nodes if n.name.casefold() == text.casefold()), None)
        if node is None:
            raise ValueError('未找到设备 '+name+'；可用设备：'+', '.join(n.name for n in nodes))
        return node

    def _job(self, line, *, output=False):
        args = words(line)
        count = 2 if output else 1
        if self.center and len(args) in (count, count+1):
            if len(args) == count+1:
                args = args[1:]  # Keep the old "node id" spelling available.
            client = self.center
        elif not self.center and len(args) in (count,count+1):
            client = self._node(args.pop(0) if len(args)==count+1 else socket.gethostname()).client
        else:
            raise ValueError('需要任务ID'+('和结果目录' if output else '')+'；例如 '+('fetch ab12 "D:/结果"' if output else 'logs ab12'))
        identity = job_identity(client.status(brief=True), args[0])
        return client, identity, args[1:]

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
        """run 任务.json [--input 目录] [--output 目录] [--cpus N] [--priority N]"""
        self._submit("run", line)

    def do_spice(self, line):
        """spice 网表.spice [--cpus N] [--output 目录]"""
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

    def do_sync_cache(self, line):
        """sync_cache all|aop|rgcn|arh|bench [--kind all|research|assets]：手动同步工程缓存。"""
        args = cache_parser().parse_args(words(line))
        if self.center is None:
            raise ValueError("sync_cache 需要连接调度中心；使用 main.py --url 中心地址 sync_cache all")
        self._start(self._sync_cache, args.project, args.kind)

    def _sync_cache(self, project, kind):
        from .cache_sync import sync_cache
        owned = set()
        def accepted(client, identity):
            owned.add(identity)
            self._accepted(client, identity)
        try:
            return sync_cache(self.center, project, kind=kind,
                              on_submit=accepted, stop_event=self.closing)
        finally:
            for identity in owned:
                self.handles.pop(identity, None)

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
        name = self._node(name).name
        changes = {}
        for setting in settings:
            key, value = setting.split("=", 1)
            if key == "enabled":
                if value.lower() not in {"true", "false"}:
                    raise ValueError("enabled=true|false")
                value = value.lower() == "true"
            elif key not in ('url','ca_file','host_ip'):
                value = int(value) if key in {"limit", "memory_limit_mb"} else float(value)
            changes[key] = value
        if self.center:
            self.center.control(node=name, routing=changes)
        else:
            self.pool.configure(name, **changes)
        if set(changes)&{'enabled','url','ca_file'}:self.do_save('')
        self.do_nodes("")

    def do_add(self, line):
        """add 设备IP[:端口]：默认26133；公网自动HTTPS并复用已保存证书和凭据。"""
        args=words(line)
        if len(args)!=1:raise ValueError('add 设备IP[:端口] 或 http(s)://设备IP[:端口]')
        address=args[0]
        path = ROOT / 'nodes.local.json'
        previous = read_json(path) if path.exists() else {}
        saved = next((row for row in previous.get('nodes', [])
                      if address == row['name'] or address in row.get('aliases',[])), {})
        url=node_url(saved['url'] if saved else address)
        if not saved:
            saved = next((row for row in previous.get('nodes', []) if node_url(row['url'])==url
                          or '://' not in address and urlsplit(node_url(row['url'])).netloc==urlsplit(url).netloc), {})
        if saved and '://' not in address:url=node_url(saved['url'])
        token = os.environ.get('REMOTE_COMPUTE_TOKEN', saved.get('token', ''))
        ca_file=saved.get('ca_file')
        timeout=saved.get('timeout',10)
        name = Client(url, token, ca_file=ca_file, timeout=timeout, operator=True).health()['node']
        snapshot = self.center.nodes() if self.center else self.pool.snapshot()
        existing = next((row for row in snapshot['nodes'] if row['name'] == name
                         or row['url'].rstrip('/') == url.rstrip('/')), None)
        if existing:
            routing = dict(url=url, enabled=True,ca_file=ca_file,timeout=timeout)
            if self.center:
                self.center.control(node=existing['name'], routing=routing)
            else:
                self.pool.configure(existing['name'], **routing)
        elif self.center:
            node = dict(name=name, url=url, token=token)
            if ca_file:node['ca_file'] = ca_file
            node['timeout']=timeout
            self.center.control(add=node)
        else:
            self.pool.add(name, url, token=token, ca_file=ca_file,timeout=timeout)
        # Remote center snapshots intentionally omit worker secrets. Remember
        # only the verified endpoint's credentials before saving its routing.
        with node_configuration(path) as current:
            profile=next((row for row in current.get('nodes',[]) if row['name']==name or row['url']==url),None)
            if profile is None:
                profile=dict(name=name,url=url);current.setdefault('nodes',[]).append(profile)
            profile.update(url=url,token=token,enabled=True)
            if ca_file:profile['ca_file']=ca_file
            profile.pop('reconnect',None)
        self.do_nodes("")
        self.do_save('')

    def do_scan(self, line):
        """scan [IPv4-CIDR] [--add]：扫描局域网；--add接入可用DCO。"""
        self.scan(scan_parser().parse_args(words(line)))

    def scan(self,args):
        found=scan_devices(args)
        if args.scan_add:
            for node in found:self.do_add(node['url'])

    def do_save(self, _line):
        """保存当前节点和策略到 nodes.local.json，下次启动自动加载。"""
        path = ROOT / "nodes.local.json"
        with node_configuration(path) as previous:
            snapshot = self.center.nodes() if self.center else self.pool.snapshot()
            tokens = {n['url']: n.get('token', '') for n in previous.get('nodes', [])}
            tokens.update({n.client.url: n.client.token for n in self.pool.nodes})
            addresses = {n['name']: n.get('host_ip','') for n in previous.get('nodes', [])}
            addresses.update({n.name:getattr(n,'host_ip','') for n in self.pool.nodes})
            value = {"policy": snapshot["policy"], "nodes": []}
            for row in snapshot["nodes"]:
                node = {key: row[key] for key in ("name", "url", "weight", "limit", "memory_limit_mb", "enabled")}
                if addresses.get(row['name']):
                    node['host_ip'] = addresses[row['name']]
                if row.get('timeout',10)!=10:
                    node['timeout']=row['timeout']
                for key in ('ca_file','aliases','reconnect'):
                    if row.get(key):node[key]=row[key]
                token = tokens.get(row["url"], "")
                if token:
                    node["token"] = token
                value["nodes"].append(node)
            previous.update(value)
        print("已保存:", path)

    def do_pause(self, line):
        """pause [节点]：暂停接新任务；省略节点即本机，当前计算继续。"""
        name = socket.gethostname() if not line.strip() else self._node(line.strip()).name
        if self.center:
            self.center.control(node=name, worker={"paused": True})
        else:
            self._node(name).client.control(paused=True)
        self.pool.refresh()

    def do_resume(self, line):
        """resume [节点]：恢复接单；省略节点即本机。"""
        name = socket.gethostname() if not line.strip() else self._node(line.strip()).name
        if self.center:
            self.center.control(node=name, worker={"paused": False})
        else:
            self._node(name).client.control(paused=False)
        self.pool.refresh()

    def do_cpus(self, line):
        """cpus [节点] N：设置CPU预算；省略节点即本机，0为自动，现有任务自然收尾。
        例：cpus 16；cpus Aya1-TX 30。设置自动保存，下次启动继续使用。
        """
        args = words(line)
        if not args:
            self.do_nodes(''); print(self.do_cpus.__doc__.strip()); return
        if len(args) not in (1,2):
            raise ValueError('需要线程数，可在前面指定设备')
        name, count = (socket.gethostname(),args[0]) if len(args)==1 else (self._node(args[0]).name,args[1])
        if self.center:
            self.center.control(node=name, worker={"cpus": int(count)})
        else:
            self._node(name).client.control(cpus=int(count))
        self.pool.refresh()
        print(name+' CPU预算已设为 '+str(count)+'；配置已保存。')

    def do_memory(self, line):
        """memory [节点] RAM：设置任务RAM预算；8G=8GiB，无单位按MiB，0为自动。
        例：memory 8G；memory Aya1-TX 16384。省略节点即本机，设置自动保存。
        """
        args = words(line)
        if not args:
            self.do_nodes(''); print(self.do_memory.__doc__.strip()); return
        if len(args) not in (1,2):
            raise ValueError('需要RAM容量，可在前面指定设备')
        name, amount = (socket.gethostname(),args[0]) if len(args)==1 else (self._node(args[0]).name,args[1])
        amount = ram_mib(amount)
        if self.center:
            self.center.control(node=name, worker={"memory_mb": int(amount)})
        else:
            self._node(name).client.control(memory_mb=int(amount))
        self.do_nodes("")

    def do_calibrate(self, line):
        """calibrate [设备]：在调度中心触发 native-mixed-v4 算力校准，空闲后自动运行。"""
        names = words(line)
        if len(names) > 1:
            raise ValueError('calibrate [设备]')
        targets = names or [n.name for n in self.pool.nodes]
        for name in targets:
            node = self._node(name)
            result = (self.center.control(node=node.name, worker={'benchmark': True})
                      if self.center else node.client.control(benchmark=True))
            state = result.get('benchmark_state', result.get('status', 'started')) if isinstance(result, dict) else result
            print(f'{node.name} 算力校准: {state}')
        self.pool.refresh()

    def do_cancel(self, line):
        """cancel 任务ID：取消任务及其子进程；可用唯一4位ID，例如 cancel ab12。"""
        client, identity, _ = self._job(line)
        display(client.cancel(identity))

    def do_logs(self, line):
        """logs 任务ID：查看日志末尾；可用唯一4位ID，例如 logs ab12。"""
        client, identity, _ = self._job(line)
        for key, value in client.logs(identity).items():
            print(key + ":\n" + value)

    def do_fetch(self, line):
        """fetch 任务ID 结果目录：保存结果并清理任务；例如 fetch ab12 "D:/结果"。"""
        client, identity, (output,) = self._job(line, output=True)
        result = client.fetch(identity, output)
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
        name=self._node(args[0]).name if args else None
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


class RemoteWorker:
    """WorkerShell's small API over the same authenticated node connection."""
    def __init__(self,client):
        self.client=client;self.condition=threading.RLock()
    @property
    def jobs(self):return {row['id']:{'result':row} for row in self.client.status()}
    def control(self,changes):return self.client.control(**changes)
    def health(self):return self.client.health()
    def cancel(self,identity):return self.client.cancel(identity)
    def logs(self,identity):return self.client.logs(identity)
    def delete(self,identity):return self.client.delete(identity)


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
        """memory RAM：设置本设备任务RAM预算；例如 memory 8G，0为自动。"""
        self.worker.control({"memory_mb": ram_mib(line)})
        self.do_status("")

    def do_jobs(self, line):
        """jobs [任务ID前缀]：列出本节点任务或查看完整记录。"""
        with self.worker.condition:
            identities=[identity for identity in self.worker.jobs if identity.startswith(line.strip())]
        rows=[self.worker.get(identity) for identity in identities]
        if line.strip() and len(rows)==1:
            from .view import wrap_lines
            width=max(20,shutil.get_terminal_size((110,32)).columns-5)
            for row in json.dumps(rows[0],ensure_ascii=False,indent=2).splitlines():
                for part in wrap_lines(row,width):
                    print(part)
        else:
            table(rows,["id", "status", "label", "cpus"])

    def do_cpus(self, line):
        """cpus N：设置本设备CPU预算并保存；例如 cpus 16，0为自动。"""
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
            rows = [{'id':identity} for identity in self.worker.jobs]
            identities = [identity for identity,_ in active_items(self.worker.jobs)] if line.strip() == "all" else [job_identity(rows,line.strip())]
        for identity in identities:
            self.worker.cancel(identity)
        print("已发出取消。")

    def do_logs(self, line):
        """logs 任务ID：查看日志；支持唯一4位ID，例如 logs ab12。"""
        with self.worker.condition:
            identity = job_identity([{'id':identity} for identity in self.worker.jobs],line.strip())
        for key, value in self.worker.logs(identity).items():
            print(key + ":\n" + value)

    def do_delete(self, line):
        """delete 任务ID：删除已经结束且不再需要的任务文件。"""
        with self.worker.condition:
            identity = job_identity([{'id':identity} for identity in self.worker.jobs],line.strip())
        display(self.worker.delete(identity))
