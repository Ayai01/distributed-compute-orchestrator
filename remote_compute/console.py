"""Live stdlib terminal: resource dashboard with an editable command line."""
from collections import deque
import shutil
import sys
import threading
import time
import unicodedata
import os
import subprocess


def _width(text):
    return sum(0 if ord(char) < 32 or unicodedata.combining(char) else
               2 if unicodedata.east_asian_width(char) in 'WF' else 1 for char in text)


def _clip(text, width):
    result, size = '', 0
    for char in str(text).replace('\n', ' ').replace('\r', ''):
        n = _width(char)
        if size + n > width:
            break
        if ord(char) >= 32:
            result += char
            size += n
    return result


def _input_view(prompt, line, cursor, width):
    """Keep the editable cursor visible even when a command exceeds one row."""
    prompt = _clip(prompt, max(0, width // 3))
    room = max(1, width - _width(prompt))
    cursor = min(max(0, cursor), len(line))
    start, before = 0, _width(line[:cursor])
    while start < cursor and before >= room - int(start > 0 and room > 1):
        before -= _width(line[start])
        start += 1
    marker = '‹' if start and room > 1 else ''
    visible = _clip(line[start:], room - len(marker))
    return prompt + marker + visible, _width(prompt + marker) + before


def _layout(status, events, output, height):
    """Reserve the lower rows for newest notices and command feedback."""
    budget = max(1, height - 2)
    sections = int(bool(events)) + int(bool(output))
    count = min(6, max(1, (budget - 3 - sections) // sections)) if sections else 0
    if sections and isinstance(status,StatusRows):
        resource_rows = max(3,min(22,budget-4))
        count = min(count,max(1,(budget-resource_rows-sections)//sections))
    footer = (['最近操作（最多6条）:'] + events[-count:] if events else [])
    footer += (['终端输出（PgUp/PgDn翻页）:'] + output[-count:] if output else [])
    remaining = max(0, budget - len(footer))
    head = status.fit(remaining) if isinstance(status,StatusRows) else status[:remaining]
    if not isinstance(status,StatusRows) and len(status) > remaining and head:
        head[-1] = '… status 查看完整资源；help 查看操作'
    return (head + footer)[-budget:]


def _enable_ansi():
    """Return a restoration callback, or None for a plain console/redirect."""
    import ctypes
    from ctypes import wintypes
    kernel = ctypes.WinDLL('kernel32', use_last_error=True)
    kernel.GetStdHandle.argtypes = [wintypes.DWORD]
    kernel.GetStdHandle.restype = wintypes.HANDLE
    kernel.GetConsoleMode.argtypes = [wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD)]
    kernel.GetConsoleMode.restype = wintypes.BOOL
    kernel.SetConsoleMode.argtypes = [wintypes.HANDLE, wintypes.DWORD]
    kernel.SetConsoleMode.restype = wintypes.BOOL
    handle, mode = kernel.GetStdHandle(-11 & 0xffffffff), wintypes.DWORD()
    if not kernel.GetConsoleMode(handle, ctypes.byref(mode)):
        return None
    if not kernel.SetConsoleMode(handle, mode.value | 4):
        return None
    return lambda: kernel.SetConsoleMode(handle, mode.value)


def _gb(value):
    return f'{(value or 0) / 1024:.1f}'


def _focus_probe():
    import ctypes
    from ctypes import wintypes
    user = ctypes.WinDLL('user32', use_last_error=True)
    kernel = ctypes.WinDLL('kernel32', use_last_error=True)
    kernel.GetConsoleWindow.restype = wintypes.HWND
    user.GetForegroundWindow.restype = wintypes.HWND
    user.IsWindowVisible.argtypes = [wintypes.HWND]
    user.GetWindowThreadProcessId.argtypes = [wintypes.HWND, ctypes.POINTER(wintypes.DWORD)]
    console = kernel.GetConsoleWindow()
    if not console:
        return lambda: True
    if user.IsWindowVisible(console):
        return lambda: user.GetForegroundWindow() == console
    # ConPTY consoles are hidden; match the foreground editor/terminal host.
    # Resolve its process ancestors once, never on the refresh path.
    script = (f'$nextId={os.getpid()}; $seen=@{{}}; while ($nextId -gt 0 -and !$seen.ContainsKey($nextId)) {{ '
              '$seen[$nextId]=$true; $p=Get-CimInstance Win32_Process -Filter ("ProcessId="+$nextId); '
              'if (!$p -or $p.Name -eq "explorer.exe") { break }; $nextId; $nextId=$p.ParentProcessId }')
    result = subprocess.run(['powershell','-NoProfile','-Command',script], capture_output=True, text=True,
                            creationflags=subprocess.CREATE_NO_WINDOW)
    hosts = {int(line) for line in result.stdout.splitlines() if line.strip().isdigit()}
    def focused():
        process = wintypes.DWORD()
        user.GetWindowThreadProcessId(user.GetForegroundWindow(), ctypes.byref(process))
        return process.value in hosts
    return focused


class FocusRefresh:
    def __init__(self):
        self.last = None
        self.focused = False

    def due(self, now, focused, *, force=False):
        refresh = force or self.last is None or (focused and not self.focused) or now-self.last >= (1 if focused else 30)
        self.focused = focused
        if refresh:
            self.last = now
        return refresh


class StatusRows(list):
    def __init__(self):
        super().__init__()
        self.priorities = []

    def add(self, text, priority=1):
        self.append(text)
        self.priorities.append(priority)

    def fit(self, count):
        if count >= len(self):
            return list(self)
        if count <= 0:
            return []
        chosen = sorted(range(len(self)), key=lambda index: (self.priorities[index],index))[:max(0,count-1)]
        return [self[index] for index in sorted(chosen)] + ['… status 查看完整资源；jobs 查看全部任务']


def format_status(value, url=''):
    center = value.get('role') == 'scheduler'
    rows = StatusRows()
    rows.add(f"Remote Compute | {value.get('node', '')} | {'调度中心' if center else '算力提供者'} | {url}",0)
    rows.add(f"总览  分配 {value.get('used_cpus',0)}/{value.get('cpus',0)} 线程 | 排队 {value.get('queued',0)} | 内存预留 {_gb(value.get('memory_reserved_mb'))} GiB" + (f" | 策略 {value.get('policy')}" if center else ''),0)
    nodes = value.get('nodes',[]) if center else [{**value,'name':value.get('node'),'capacity':value.get('cpus')}]
    rows.add('[设备资源]  设备 / 线程 / CPU / 温度',0)
    jobs = []
    for node in nodes:
        name = node.get('node') or node.get('name')
        total,available = node.get('memory_total_mb') or 0,node.get('host_memory_available_mb') or 0
        temp = node.get('temperature_c')
        state = '离线' if node.get('error') else '暂停' if node.get('paused') or not node.get('enabled',True) else '在线'
        allocated = max(node.get('used_cpus',0),node.get('reserved',0)+node.get('external_cpus',0))
        rows.add(f"{name}  {state} | 已分配 {allocated}/{node.get('capacity',0)} | CPU {node.get('cpu_percent') or 0:.1f}% | 设备 {node.get('logical_cpus','?')} 线程 | 温度 {str(temp)+'°C' if temp is not None else '未提供'}",0)
        peers = ', '.join('本机进程' if item['ip']=='local' else item['ip'] for item in node.get('peers') or []) or '等待连接'
        rows.add(f"  内存 {_gb(total-available)}/{_gb(total)} GiB | RSS {_gb(node.get('rss_mb'))} | 预留 {_gb(node.get('memory_reserved_mb'))} | 可分配 {_gb(node.get('memory_available_mb'))} | 访问IP {peers}",1)
        address = node.get('url',url)
        if address.startswith('local://'):
            address = '本机内置 worker'
        rows.add('  '+address+' | 能力 '+', '.join(node.get('tags') or []),3)
        if node.get('error'):
            rows.add('  '+node['error'],1)
        jobs.extend((name,job) for job in node.get('active_jobs') or [])
    if center:
        rows.add('[工程份额]  工程 / 权重 / 已分配 / 目标 / 排队',0)
        for name,allocation in value.get('projects',{}).items():
            rows.add(f"{name} | 权重 {allocation['resource_share']:g} | {allocation['used']}/{allocation['target']} 线程 | 排队 {allocation['queued']}",1)
        if not value.get('projects'):
            rows.add('  等待工程任务',2)
    rows.add(f'[当前任务]  共 {len(jobs)} 个；jobs 查看完整列表',0)
    # Interleave devices so a busy first device cannot hide every remote task.
    groups = [[item for item in jobs if item[0]==(node.get('node') or node.get('name'))] for node in nodes]
    for index in range(max((len(group) for group in groups),default=0)):
        for group in groups:
            if index < len(group):
                name,job=group[index]
                rows.add(f"{name} | {job['cpus']}线程 {_gb(job.get('memory_mb'))}GiB | {job['status']} | {job['label']} [{job['id'][:8]}]",1 if index==0 else 2)
    if not jobs:
        rows.add('  空闲',2)
    rows.add('操作  help | status | jobs | cpus '+('设备名 N' if center else 'N')+' | memory | pause | resume | quit',2)
    return rows


class Dashboard:
    def __init__(self, shell, health, url='', alive=lambda: True):
        self.shell, self.health, self.url, self.alive = shell, health, url, alive
        self.output = deque(maxlen=200)
        self.output_lock = threading.RLock()
        self.pending = ''
        self.output_revision = 0
        self.stream = sys.stdout
        self.encoding = getattr(self.stream, 'encoding', 'utf-8')

    def write(self, text):
        with self.output_lock:
            self.pending += text
            self.output_revision += 1
            while '\n' in self.pending:
                line, self.pending = self.pending.split('\n', 1)
                if line.strip():
                    self.output.append(line)
        return len(text)

    def flush(self):
        pass

    def isatty(self):
        return True

    def run(self):
        import msvcrt
        restore = _enable_ansi()
        if restore is None:
            return False
        focus, refresh = _focus_probe(), FocusRefresh()
        line, cursor, history, selected = '', 0, [], 0
        last, offset, last_frame = 0, 0, ''
        rows, events = [], []
        revision, cached_key, body = 0, None, ''
        stdout, stderr, shell_stdout = sys.stdout, sys.stderr, self.shell.stdout
        try:
            sys.stdout = sys.stderr = self.shell.stdout = self
            self.stream.write('\x1b[?1049h\x1b[2J')
            while True:
                try:
                    if not self.alive():
                        break
                    now = time.monotonic()
                    if refresh.due(now, focus(), force=last == 0):
                        try:
                            value = self.health()
                            rows = format_status(value, self.url)
                            notices = list(value.get('events', []))
                            for node in value.get('nodes', []):
                                notices.extend(node.get('events') or [])
                            events = [time.strftime('%H:%M:%S', time.localtime(e['time'])) + ' ' + e['ip'] + ' ' + e['message']
                                      for e in sorted(notices, key=lambda x: x['time'])[-6:]]
                        except (OSError, RuntimeError, ValueError) as error:
                            rows = ['连接状态: ' + str(error)]
                        revision += 1
                        last = now
                    columns, height = shutil.get_terminal_size((110, 32))
                    with self.output_lock:
                        key = (revision,self.output_revision,offset,columns,height)
                        if key != cached_key:
                            output = list(self.output) + ([self.pending] if self.pending else [])
                            offset = min(offset,max(0,len(output)-1))
                            if offset:
                                output = output[:-offset]
                            visible = _layout(rows,events,output,height)
                            visible += [''] * max(0,height-2-len(visible))
                            body = '\n'.join(_clip(row,columns-1)+'\x1b[K' for row in visible)
                            body += '\n'+'─'*max(1,columns-1)
                            cached_key = (revision,self.output_revision,offset,columns,height)
                    prompt, column = _input_view(self.shell.prompt, line, cursor, max(1, columns - 1))
                    frame = '\x1b[H' + body + '\n\x1b[J' + prompt + f'\r\x1b[{column+1}G'
                    if frame != last_frame:
                        self.stream.write(frame)
                        self.stream.flush()
                        last_frame = frame
                    while msvcrt.kbhit():
                        key = msvcrt.getwch()
                        if key in ('\x00', '\xe0'):
                            key = msvcrt.getwch()
                            if key == 'K': cursor = max(0, cursor - 1)
                            elif key == 'M': cursor = min(len(line), cursor + 1)
                            elif key == 'G': cursor = 0
                            elif key == 'O': cursor = len(line)
                            elif key == 'S': line = line[:cursor] + line[cursor + 1:]
                            elif key == 'I': offset += 6
                            elif key == 'Q': offset = max(0, offset - 6)
                            elif key in ('H', 'P') and history:
                                selected = max(0, min(len(history), selected + (-1 if key == 'H' else 1)))
                                line = history[selected] if selected < len(history) else ''
                                cursor = len(line)
                        elif key == '\r':
                            if line.strip():
                                self.write(self.shell.prompt + line + '\n')
                                history.append(line)
                                history = history[-50:]
                                selected = len(history)
                                if self.shell.onecmd(line):
                                    return True
                            line, cursor, last, offset = '', 0, 0, 0
                        elif key in ('\x03', '\x1b'):
                            line, cursor = '', 0
                        elif key == '\x08':
                            if cursor: line, cursor = line[:cursor-1] + line[cursor:], cursor - 1
                        elif key == '\x04':
                            return True
                        elif key.isprintable():
                            line, cursor = line[:cursor] + key + line[cursor:], cursor + 1
                    time.sleep(0.08)
                except KeyboardInterrupt:
                    line, cursor, last, offset = '', 0, 0, 0
                    self.write('已返回操作终端。\n')
            return True
        finally:
            sys.stdout, sys.stderr, self.shell.stdout = stdout, stderr, shell_stdout
            try:
                self.stream.write('\x1b[?1049l')
                self.stream.flush()
            finally:
                restore()
