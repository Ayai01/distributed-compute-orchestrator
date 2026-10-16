"""Live stdlib terminal: resource dashboard with an editable command line."""
from collections import deque
import shutil
import sys
import threading
import time
import os
import subprocess

from .view import _width, _clip, _layout, StatusRows, format_status


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

    @property
    def output_width(self):
        return max(1, shutil.get_terminal_size((110, 32)).columns - 5)

    def run(self):
        import msvcrt
        restore = _enable_ansi()
        if restore is None:
            return False
        focus, refresh = _focus_probe(), FocusRefresh()
        line, cursor, history, selected = '', 0, [], 0
        last, offset, last_frame, page_size = 0, 0, '', 6
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
                            if isinstance(rows,StatusRows):
                                rows.resize(columns-1)
                            visible = _layout(rows,events,output,height)
                            for index,text in enumerate(visible):
                                if text.startswith('+ 命令输出'):
                                    page_size=max(1,len(visible)-index-2)
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
                            elif key == 'I': offset += page_size
                            elif key == 'Q': offset = max(0, offset - page_size)
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
