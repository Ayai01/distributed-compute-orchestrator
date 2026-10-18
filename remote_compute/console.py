"""Live stdlib terminal: resource dashboard with an editable command line."""
from collections import deque
import shutil
import sys
import threading
import time
import os
import subprocess
import re

from .view import _width, _clip, _rule, _line, _layout, StatusRows, format_status, format_notices, set_width_probe


_COLOR_ENABLED = False
_RESET = '\x1b[0m'


def _paint_row(row):
    """Add restrained ANSI hierarchy after width calculation.

    Rows remain plain strings in the painter cache, so ANSI bytes never affect
    clipping, Unicode width, or change detection.  The same SGR sequences are
    supported by Windows 10+ virtual terminals and POSIX terminals.
    """
    if not _COLOR_ENABLED or not row:
        return row
    text = row.strip()
    if not text:
        return row
    # Keep prose at the terminal's normal white.  Panel structure and the
    # command prompt may span a whole row; task state/value colors below are
    # applied to individual tokens so a right-hand task never tints a left
    # resource panel in the two-column layout.
    if text.startswith(('调度>', '算力>', 'worker>', 'scheduler>')):
        color = '\x1b[36;1m'          # command input
    elif text.startswith('+ ') and any(char.isalpha() or '\u4e00' <= char <= '\u9fff' for char in text):
        color = '\x1b[34;1m'          # panel/section title
    elif set(text) <= set('+-|='):
        color = '\x1b[90m'            # separators
    else:
        hash_pattern = re.compile(r'(?<![\w])(?:[0-9a-f]{8}|[0-9a-f]{32}|[0-9a-f]{40}|[0-9a-f]{64}|(?=[0-9a-f]{0,3}[a-f][0-9a-f]{0,3}(?![0-9a-f]))[0-9a-f]{4})(?![\w])', re.I)
        number_pattern = re.compile(r'(?<![\w.])(?:\d+(?:\.\d+)?(?:/\d+(?:\.\d+)?)?)(?![\w.])')
        state_pattern = re.compile(r'运行(?!秒)|排队|等待|暂停|派发|完成|成功|校准中|失败|错误|异常|blocked|error|failed|succeeded|online', re.I)
        state_colors = {
            '运行':'\x1b[32m', '完成':'\x1b[32m', '成功':'\x1b[32m', 'succeeded':'\x1b[32m', 'online':'\x1b[32m',
            '排队':'\x1b[33m', '等待':'\x1b[33m', '暂停':'\x1b[33m',
            '派发':'\x1b[36m', '校准中':'\x1b[36m',
            '失败':'\x1b[31;1m', '错误':'\x1b[31;1m', '异常':'\x1b[31;1m',
            'blocked':'\x1b[31;1m', 'error':'\x1b[31;1m', 'failed':'\x1b[31;1m'}
        result, position = [], 0
        matches = [*hash_pattern.finditer(row), *number_pattern.finditer(row), *state_pattern.finditer(row)]
        for match in sorted(matches, key=lambda m: (m.start(), -m.end())):
            if match.start() < position:
                continue
            result.append(row[position:match.start()])
            token = match.group()
            token_color = ('\x1b[36m' if hash_pattern.fullmatch(token) else
                           state_colors.get(token.casefold(), '\x1b[33m'))
            result.extend((token_color, match.group(), _RESET))
            position = match.end()
        result.append(row[position:])
        return ''.join(result)
    return color + row + _RESET


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


class CommandCompletion:
    """Cycle a local candidate list without changing the command's other words."""
    def __init__(self):
        self.last = None
        self.matches = []
        self.index = -1

    def apply(self, line, cursor, context, *, reverse=False):
        if self.last == (line, cursor):
            self.index = (self.index + (-1 if reverse else 1)) % len(self.matches)
            text = self.matches[self.index]
        else:
            self.matches = context['matches']
            if not self.matches:
                return line, cursor
            self.before, self.after = line[:context['start']], line[context['end']:]
            common = os.path.commonprefix(self.matches)
            self.index = -1
            if not reverse and len(self.matches) > 1 and len(common) > len(context['prefix']):
                text = common
            else:
                self.index = len(self.matches)-1 if reverse else 0
                text = self.matches[self.index]
        if len(self.matches) == 1 and not self.after and not text.endswith('='):
            text += ' '
        result = self.before + text + self.after
        position = len(self.before + text)
        self.last = (result, position) if len(self.matches) > 1 else None
        return result, position


def _command_rows(shell, line, cursor, width, context, completion):
    usage = context['usage'] or ('Tab 补全命令 | Ctrl+Tab 翻任务 | Enter 执行 | Esc 清空 | help 命令'
                                 if line.strip() else
                                 'Tab 补全命令 | Ctrl+Tab 翻任务 | PgUp/PgDn 输出 | 上下键 历史 | help 命令')
    matches = completion.matches if completion.last == (line, cursor) else context['matches']
    selected = completion.index if completion.last == (line, cursor) else -1
    candidates = []
    order = list(range(len(matches)))
    if selected >= 0:
        order = order[selected:] + order[:selected]
    for position, index in enumerate(order):
        match = matches[index]
        label = match[:4] if len(match) == 32 and all(c in '0123456789abcdef' for c in match) else match
        if index == selected:
            label = '[' + label + ']'
        suffix = ' ... +' + str(len(matches)-position)
        if candidates and _width('候选: ' + '  '.join(candidates + [label]) + suffix) > width-4:
            candidates.append(suffix.strip())
            break
        candidates.append(label)
    choices = '候选: ' + '  '.join(candidates) if candidates else 'Tab 补全命令 | Ctrl+Tab 翻任务 | Enter 执行 | Esc 清空'
    prompt, column = _input_view(shell.prompt, line, cursor, width)
    return [_rule('用法: ' + usage, width), _line(choices, width), _rule('', width), prompt], column


def _console_size_probe():
    """Read the visible viewport, independently of buffer rows and LINES/COLUMNS."""
    import ctypes as c
    from ctypes import wintypes as w
    class Coord(c.Structure):
        _fields_ = [('x', w.SHORT), ('y', w.SHORT)]
    class Rect(c.Structure):
        _fields_ = [(name, w.SHORT) for name in ('left', 'top', 'right', 'bottom')]
    class Info(c.Structure):
        _fields_ = [('size', Coord), ('cursor', Coord), ('attributes', w.WORD), ('window', Rect), ('maximum', Coord)]
    kernel = c.WinDLL('kernel32')
    kernel.GetStdHandle.argtypes = [w.DWORD]
    kernel.GetStdHandle.restype = w.HANDLE
    kernel.GetConsoleScreenBufferInfo.argtypes = [w.HANDLE, c.POINTER(Info)]
    handle = kernel.GetStdHandle(-11 & 0xffffffff)
    def size():
        info = Info()
        if kernel.GetConsoleScreenBufferInfo(handle, c.byref(info)):
            return os.terminal_size((max(1, info.window.right-info.window.left+1),
                                     max(1, info.window.bottom-info.window.top+1)))
        return shutil.get_terminal_size((110, 32))
    return size


def _enable_ansi():
    """Return a restoration callback, or None for a plain console/redirect."""
    global _COLOR_ENABLED
    allowed = not (os.environ.get('NO_COLOR') is not None
                   or os.environ.get('DCO_NO_COLOR') is not None
                   or os.environ.get('TERM','').lower() == 'dumb')
    if os.name!='nt':
        if not sys.stdout.isatty():
            return None
        _COLOR_ENABLED = allowed
        def restore_posix():
            global _COLOR_ENABLED
            _COLOR_ENABLED = False
        return restore_posix
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
    _COLOR_ENABLED = allowed
    def restore_windows():
        global _COLOR_ENABLED
        _COLOR_ENABLED = False
        kernel.SetConsoleMode(handle, mode.value)
    return restore_windows


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
        changes=[f'\x1b[{i+1};1H'+_paint_row(row)+'\x1b[K' for i,row in enumerate(rows)
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
    if os.name!='nt':return lambda:True
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


class _PosixKeyboard:
    """Translate ordinary terminal keys to the dashboard's existing key codes."""
    def __init__(self):
        import codecs,termios,tty
        self.fd=sys.stdin.fileno();self.termios=termios
        self.previous=termios.tcgetattr(self.fd);tty.setcbreak(self.fd)
        self.decoder=codecs.getincrementaldecoder('utf-8')();self.buffer=''
        self.keys={'\x1b[A':'H','\x1b[B':'P','\x1b[C':'M','\x1b[D':'K','\x1b[H':'G','\x1b[F':'O',
                   '\x1b[3~':'S','\x1b[5~':'I','\x1b[6~':'Q','\x1b[Z':'\x0f',
                   # Terminals differ in how they encode Ctrl+Tab.  Normalize
                   # the common xterm/kitty forms to the Windows-style code
                   # consumed by the dashboard loop.
                   '\x1b[27;5;9~':'\x94','\x1b[9;5~':'\x94',
                   '\x1b[9;5u':'\x94','\x1b[1;5I':'\x94'}
    def kbhit(self):
        import select
        if select.select([self.fd],[],[],0)[0]:self.buffer+=self.decoder.decode(os.read(self.fd,128))
        return bool(self.buffer)
    def getwch(self):
        if self.buffer.startswith('\x1b['):
            for sequence,key in self.keys.items():
                if self.buffer.startswith(sequence):
                    self.buffer='\x00'+key+self.buffer[len(sequence):];break
        key,self.buffer=self.buffer[0],self.buffer[1:]
        return '\x08' if key=='\x7f' else '\r' if key=='\n' else key
    def close(self):
        self.termios.tcsetattr(self.fd,self.termios.TCSADRAIN,self.previous)


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
        return max(1, getattr(self, 'columns', shutil.get_terminal_size((110, 32)).columns) - 5)

    def run(self, _state=None):
        restore = _enable_ansi()
        if restore is None:
            return False
        if os.name=='nt':
            import msvcrt
        else:msvcrt=_PosixKeyboard()
        if os.name=='nt' and self.stream.isatty() and _state is None:
            _initial_size()
        width_probe,close_probe=_console_width_probe() if os.name=='nt' and self.stream.isatty() else (None,lambda: None)
        set_width_probe(width_probe)
        focus, refresh = _focus_probe(), FocusRefresh()
        state = _state or {}
        line, cursor = state.get('line', ''), state.get('cursor', 0)
        history, selected = state.get('history', []), state.get('selected', 0)
        last, offset, page_size, task_page = 0, state.get('offset', 0), 6, state.get('task_page', 0)
        empty_tab = False
        completion = CommandCompletion()
        painter=state.get('painter') or RowPainter()
        rows, events, value = [], [], {}
        revision, cached_key, body = 0, None, []
        context_key, context = None, None
        stdout, stderr, shell_stdout = sys.stdout, sys.stderr, self.shell.stdout
        try:
            sys.stdout = sys.stderr = self.shell.stdout = self
            if _state is None:
                self.stream.write('\x1b[?1049h\x1b[2J')
                self.stream.flush()
            size = _console_size_probe() if os.name=='nt' and self.stream.isatty() else lambda: shutil.get_terminal_size((110, 32))
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
                    columns, height = size()
                    self.columns = columns
                    # The command footer owns the last five rows.  Keeping a
                    # spare row above it prevents a refresh from painting over
                    # the prompt in short Windows/ConPTY viewports.
                    reserved = min(5, height)
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
                            visible = _layout(rows,events,output,height,reserved=reserved)
                            if isinstance(rows,StatusRows):
                                task_page=rows.task_page
                            for index,text in enumerate(visible):
                                if text.startswith('+ 命令输出'):
                                    page_size=max(1,len(visible)-index-2)
                            body_height=max(0, height-reserved)
                            body = visible[-body_height:] if body_height else []
                            body = ([''] * max(0, body_height-len(body))) + body
                            cached_key = (revision,self.output_revision,offset,task_page,columns,height)
                    if context_key != (line, cursor, revision):
                        context = self.shell.suggest(line, cursor, value)
                        context_key = (line, cursor, revision)
                    footer, column = _command_rows(self.shell, line, cursor, max(1,columns-1), context, completion)
                    # The footer is an independent fixed-height viewport.  Do
                    # not let a status/output refresh consume its last row.
                    footer = ([''] * max(0, reserved-len(footer))) + footer[-reserved:]
                    frame = painter.frame(body+footer,columns,height,column)
                    if frame:
                        self.stream.write(frame)
                        self.stream.flush()
                    while msvcrt.kbhit():
                        key = msvcrt.getwch()
                        if key not in ('\t', '\x00', '\xe0'):
                            empty_tab = False
                        if key not in ('\t', '\x00', '\xe0'):
                            completion.last = None
                        if key in ('\x00', '\xe0'):
                            key = msvcrt.getwch()
                            if key not in ('\x0f','\x94'):
                                completion.last = None
                            if key == 'K': cursor = max(0, cursor - 1)
                            elif key == 'M': cursor = min(len(line), cursor + 1)
                            elif key == 'G': cursor = 0
                            elif key == 'O': cursor = len(line)
                            elif key == 'S': line = line[:cursor] + line[cursor + 1:]
                            elif key == 'I': offset += page_size
                            elif key == 'Q': offset = max(0, offset - page_size)
                            elif key == '\x94':
                                task_page += 1
                                empty_tab = False
                            elif key == '\x0f':
                                if line.strip():
                                    line, cursor = completion.apply(line,cursor,self.shell.suggest(line,cursor,value),reverse=True)
                                elif empty_tab:
                                    # Older Windows console hosts report
                                    # Ctrl+Tab as the same extended code as
                                    # Shift+Tab.  A preceding root Tab makes
                                    # that intent unambiguous while Tab itself
                                    # still only exposes command candidates.
                                    task_page += 1
                                    empty_tab = False
                                else:
                                    # Shift+Tab is a convenient reverse page
                                    # gesture when the command line is empty.
                                    task_page -= 1
                            elif key in ('H', 'P') and history:
                                selected = max(0, min(len(history), selected + (-1 if key == 'H' else 1)))
                                line = history[selected] if selected < len(history) else ''
                                cursor = len(line)
                        elif key == '\t':
                            if line.strip():
                                line, cursor = completion.apply(line,cursor,self.shell.suggest(line,cursor,value))
                            else:
                                # An empty line is already the command root;
                                # keep it empty and expose the root candidates
                                # in the footer instead of inserting an
                                # arbitrary command.
                                completion.last = None
                                empty_tab = True
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
                if _state is None:
                    self.stream.write('\x1b[?1049l')
                    self.stream.flush()
            finally:
                set_width_probe(None)
                close_probe()
                if os.name!='nt':msvcrt.close()
                restore()
