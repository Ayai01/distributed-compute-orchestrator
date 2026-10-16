"""Local listener checks and explicit operator choices before opening services."""
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import time

from .client import Client, RemoteError


class Cancelled(RuntimeError):
    pass


def available(host, port):
    if port == 0:
        return True
    family = socket.AF_INET6 if ':' in host else socket.AF_INET
    with socket.socket(family) as probe:
        if os.name == 'nt':
            probe.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
        else:
            probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            probe.bind((host, port))
            return True
        except OSError:
            return False


def owners(port):
    if os.name == 'nt':
        script = (
            f"Get-NetTCPConnection -State Listen -LocalPort {int(port)} -ErrorAction SilentlyContinue | "
            "Select-Object -ExpandProperty OwningProcess -Unique | ForEach-Object { "
            "Get-CimInstance Win32_Process -Filter ('ProcessId=' + $_) | "
            "Select-Object @{n='pid';e={$_.ProcessId}},Name,CommandLine } | ConvertTo-Json -Compress"
        )
        result = subprocess.run(['powershell', '-NoProfile', '-Command', script],
                                capture_output=True, text=True, errors='replace')
        rows = json.loads(result.stdout.strip() or '[]')
        return rows if isinstance(rows, list) else [rows]
    inodes = set()
    for name in ('tcp', 'tcp6'):
        for line in Path('/proc/net', name).read_text().splitlines()[1:]:
            fields = line.split()
            if int(fields[1].split(':')[1], 16) == port and fields[3] == '0A':
                inodes.add('socket:[' + fields[9] + ']')
    result = []
    for process in Path('/proc').iterdir():
        if not process.name.isdigit():
            continue
        try:
            if any(os.readlink(fd) in inodes for fd in (process / 'fd').iterdir()):
                result.append({'pid': int(process.name), 'CommandLine':
                               (process / 'cmdline').read_bytes().replace(b'\0', b' ').decode(errors='replace')})
        except (PermissionError, FileNotFoundError, ProcessLookupError):
            continue
    return result


def terminate(pids, *, admin=False):
    pids = sorted({int(pid) for pid in pids})
    if not pids or any(pid <= 1 or pid == os.getpid() for pid in pids):
        raise ValueError('未找到可结束的监听进程，请按显示的端口检查进程。')
    if os.name == 'nt':
        argv = ['/T', '/F', *[value for pid in pids for value in ('/PID', str(pid))]]
        if admin:
            arguments = ','.join("'" + arg + "'" for arg in argv)
            script = ("$ErrorActionPreference='Stop'; try { $p=Start-Process "
                      "-FilePath \"$env:SystemRoot\\System32\\taskkill.exe\" "
                      f"-ArgumentList {arguments} -Verb RunAs -WindowStyle Hidden -Wait -PassThru; "
                      "exit $p.ExitCode } catch { Write-Error $_; exit 1 }")
            command = ['powershell', '-NoProfile', '-Command', script]
        else:
            command = ['taskkill', *argv]
    else:
        # Include descendants so interrupted compilers cannot keep working alone.
        family = set(pids)
        parents = {}
        for path in Path('/proc').glob('[0-9]*/stat'):
            try:
                parents[int(path.parent.name)] = int(path.read_text().rsplit(')', 1)[1].split()[1])
            except (OSError, ValueError, IndexError):
                continue
        while True:
            children = {pid for pid, parent in parents.items() if parent in family}
            if children <= family:
                break
            family.update(children)
        command = [*(['sudo'] if admin else []), 'kill', '-KILL', *map(str, sorted(family))]
    result = subprocess.run(command, capture_output=True, text=True, errors='replace')
    if result.returncode:
        raise RuntimeError((result.stderr or result.stdout).strip() + '\n可选择 admin-kill 以管理员权限结束进程。')
    print((result.stdout or '已结束端口占用进程。').strip())


def resolve(host, port, *, policy='ask', token='', reuse_role=None):
    """Return a reusable local service URL, or None when the port is free."""
    if available(host, port):
        return None
    local_host = '127.0.0.1' if host in {'0.0.0.0', ''} else '::1' if host == '::' else host
    url = f"http://{'[' + local_host + ']' if ':' in local_host else local_host}:{port}"
    reusable = False
    if reuse_role:
        try:
            reusable = Client(url, token, timeout=1).health().get('role') == reuse_role
        except RemoteError:
            pass
    print(f'端口冲突：{host}:{port}')
    processes = owners(port)
    for process in processes:
        print(f"  PID {process['pid']}  {process.get('CommandLine') or process.get('Name', '')}")
    choice = policy
    if choice == 'ask':
        if not sys.stdin.isatty():
            raise RuntimeError('非交互启动已取消；使用 --on-port-conflict reuse|kill|admin-kill|cancel 选择处理方式。')
        options = '[r] reuse 复用已有服务 / ' if reusable else ''
        try:
            choice = input(options + '[k] kill / [a] admin-kill 管理员结束 / [c] cancel 取消：').strip().lower()
        except (EOFError, KeyboardInterrupt):
            raise Cancelled('已取消启动，已有进程继续运行。') from None
        choice = {'r': 'reuse', 'k': 'kill', 'a': 'admin-kill', 'c': 'cancel',
                  '': 'reuse' if reusable else 'cancel'}.get(choice, choice)
    if choice == 'reuse' and reusable:
        print('复用已有服务：' + url)
        return url
    if choice not in {'kill', 'admin-kill'}:
        raise Cancelled('已取消启动，已有进程继续运行。')
    terminate([p['pid'] for p in processes], admin=choice == 'admin-kill')
    deadline = time.monotonic() + 5
    while not available(host, port):
        if time.monotonic() >= deadline:
            raise RuntimeError(f'端口 {port} 仍被占用，请重新启动并查看当前进程。')
        time.sleep(.1)
    return None
