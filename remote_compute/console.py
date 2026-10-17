"""Live stdlib terminal: resource dashboard with an editable command line."""
from collections import deque
import shutil
import sys
import threading
import time
import os
import subprocess

from .view import _width, _clip, _layout, StatusRows, format_status, format_notices, set_width_probe


def _input_view(prompt, line, cursor, width):
    """Keep the editable cursor visible even when a command exceeds one row."""
    prompt = _clip(prompt, max(0, width // 3))
    room = max(1, width - _width(prompt))
    cursor = min(max(0, cursor), len(line))
    start, before = 0, _width(line[:cursor])
    while start < cursor and before >= room - int(start > 0 and room > 1):
        before -= _width(line[start])
        start += 1
    marker = '<' if start and room > 1 else ''
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


def _console_width_probe():
    """Measure ambiguous Unicode in an inactive buffer using the console font."""
    import ctypes as c
    from ctypes import wintypes as w
    class Coord(c.Structure):
        _fields_=[('x',w.SHORT),('y',w.SHORT)]
    class Rect(c.Structure):
        _fields_=[(name,w.SHORT) for name in ('left','top','right','bottom')]
    class Info(c.Structure):
        _fields_=[('size',Coord),('cursor',Coord),('attributes',w.WORD),('window',Rect),('maximum',Coord)]
    kernel=c.WinDLL('kernel32',use_last_error=True)
    kernel.CreateConsoleScreenBuffer.argtypes=[w.DWORD,w.DWORD,c.c_void_p,w.DWORD,c.c_void_p]
    kernel.CreateConsoleScreenBuffer.restype=w.HANDLE
    kernel.SetConsoleCursorPosition.argtypes=[w.HANDLE,Coord]
    kernel.WriteConsoleW.argtypes=[w.HANDLE,w.LPCWSTR,w.DWORD,c.POINTER(w.DWORD),c.c_void_p]
    kernel.GetConsoleScreenBufferInfo.argtypes=[w.HANDLE,c.POINTER(Info)]
    kernel.CloseHandle.argtypes=[w.HANDLE]
    handle=kernel.CreateConsoleScreenBuffer(0xc0000000,3,None,1,None)
    if not handle or handle==w.HANDLE(-1).value:
        return None,lambda: None
    def measure(char):
        written,info=w.DWORD(),Info()
        kernel.SetConsoleCursorPosition(handle,Coord(0,0))
        if (kernel.WriteConsoleW(handle,char,len(char.encode('utf-16-le'))//2,c.byref(written),None)
                and kernel.GetConsoleScreenBufferInfo(handle,c.byref(info))):
            return max(1,info.cursor.x)
        return 1
    return measure,lambda: kernel.CloseHandle(handle)


class RowPainter:
    """Paint changed rows with absolute addresses, without scrolling the buffer."""
    def __init__(self):
        self.rows=()
        self.size=None
        self.cursor=None

    def frame(self, rows, columns, height, cursor):
        rows=tuple(_clip(row,max(0,columns-1)) for row in rows[:height])
        rows+=('',)*max(0,height-len(rows))
        previous=self.rows if self.size==(columns,height) else ()
        changes=[f'\x1b[{i+1};1H'+row+'\x1b[K' for i,row in enumerate(rows)
                 if i>=len(previous) or row!=previous[i]]
        position=(height,min(max(0,cursor),max(0,columns-2))+1)
        result=''
        if changes:
            result='\x1b[?25l'+''.join(changes)
        if changes or position!=self.cursor:
            result+=f'\x1b[{position[0]};{position[1]}H'
        if changes:
            result+='\x1b[?25h'
        self.rows,self.size,self.cursor=rows,(columns,height),position
        return result


def _initial_size(columns=160, rows=50):
    """Grow a classic Windows console once, within its current screen/font limit."""
    import ctypes as c
    from ctypes import wintypes as w
    class Coord(c.Structure):
        _fields_=[('x',w.SHORT),('y',w.SHORT)]
    class Rect(c.Structure):
        _fields_=[(name,w.SHORT) for name in ('left','top','right','bottom')]
    class Info(c.Structure):
        _fields_=[('size',Coord),('cursor',Coord),('attributes',w.WORD),('window',Rect),('maximum',Coord)]
    kernel=c.WinDLL('kernel32')
    user=c.WinDLL('user32')
    kernel.GetConsoleWindow.restype=w.HWND
    user.IsWindowVisible.argtypes=[w.HWND]
    if not user.IsWindowVisible(kernel.GetConsoleWindow()):
        return False  # ConPTY/editor panes are sized by their host.
    kernel.GetStdHandle.argtypes=[w.DWORD]
    kernel.GetStdHandle.restype=w.HANDLE
    kernel.GetConsoleScreenBufferInfo.argtypes=[w.HANDLE,c.POINTER(Info)]
    kernel.GetLargestConsoleWindowSize.argtypes=[w.HANDLE]
    kernel.GetLargestConsoleWindowSize.restype=Coord
    kernel.SetConsoleScreenBufferSize.argtypes=[w.HANDLE,Coord]
    kernel.SetConsoleWindowInfo.argtypes=[w.HANDLE,w.BOOL,c.POINTER(Rect)]
    handle=kernel.GetStdHandle(-11 & 0xffffffff)
    info=Info()
    if not kernel.GetConsoleScreenBufferInfo(handle,c.byref(info)):
        return False
    maximum=kernel.GetLargestConsoleWindowSize(handle)
    width=max(info.window.right-info.window.left+1,min(columns,maximum.x))
    height=max(info.window.bottom-info.window.top+1,min(rows,maximum.y))
    if (width,height)==(info.window.right-info.window.left+1,info.window.bottom-info.window.top+1):
        return True
    if not kernel.SetConsoleScreenBufferSize(handle,Coord(max(info.size.x,width),max(info.size.y,height))):
        return False
    return bool(kernel.SetConsoleWindowInfo(handle,True,c.byref(Rect(0,0,width-1,height-1))))


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
        refresh = force or self.last is None or (focused and not self.focused) or now-self.last >= (1 if focused else 5)
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
        if self.stream.isatty():
            _initial_size()
        width_probe,close_probe=_console_width_probe() if self.stream.isatty() else (None,lambda: None)
        set_width_probe(width_probe)
        focus, refresh = _focus_probe(), FocusRefresh()
        line, cursor, history, selected = '', 0, [], 0
        last, offset, page_size, task_page = 0, 0, 6, 0
        painter=RowPainter()
        rows, events = [], []
        revision, cached_key, body = 0, None, []
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
                            events = format_notices(value)
                        except (OSError, RuntimeError, ValueError) as error:
                            rows = ['连接状态: ' + str(error)]
                        revision += 1
                        last = now
                    columns, height = shutil.get_terminal_size((110, 32))
                    with self.output_lock:
                        key = (revision,self.output_revision,offset,task_page,columns,height)
                        if key != cached_key:
                            output = list(self.output) + ([self.pending] if self.pending else [])
                            offset = min(offset,max(0,len(output)-1))
                            if offset:
                                output = output[:-offset]
                            if isinstance(rows,StatusRows):
                                rows.resize(columns-1)
                                rows.task_page=task_page
                            visible = _layout(rows,events,output,height)
                            if isinstance(rows,StatusRows):
                                task_page=rows.task_page
                            for index,text in enumerate(visible):
                                if text.startswith('+ 命令输出'):
                                    page_size=max(1,len(visible)-index-2)
                            visible += [''] * max(0,height-2-len(visible))
                            body = visible+['-'*max(1,columns-1)]
                            cached_key = (revision,self.output_revision,offset,task_page,columns,height)
                    prompt, column = _input_view(self.shell.prompt, line, cursor, max(1, columns - 1))
                    frame = painter.frame(body+[prompt],columns,height,column)
                    if frame:
                        self.stream.write(frame)
                        self.stream.flush()
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
                            elif key == '\x0f': task_page -= 1
                            elif key in ('H', 'P') and history:
                                selected = max(0, min(len(history), selected + (-1 if key == 'H' else 1)))
                                line = history[selected] if selected < len(history) else ''
                                cursor = len(line)
                        elif key == '\t':
                            task_page += 1
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
                set_width_probe(None)
                close_probe()
                restore()
