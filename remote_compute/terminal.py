"""Require a visible, foreground terminal before starting an operator service."""

import os
import socket
import subprocess
import sys


def _windows_processes():
    """Read the local process ancestry without a WMI provider round trip."""
    import ctypes as c
    from ctypes import wintypes as w
    kernel = c.WinDLL('kernel32', use_last_error=True)
    class Entry(c.Structure):
        _fields_ = [('size', w.DWORD), ('usage', w.DWORD), ('pid', w.DWORD),
                    ('heap', c.c_size_t), ('module', w.DWORD), ('threads', w.DWORD),
                    ('parent', w.DWORD), ('priority', w.LONG), ('flags', w.DWORD),
                    ('exe', w.WCHAR * 260)]
    kernel.CreateToolhelp32Snapshot.argtypes = [w.DWORD, w.DWORD]
    kernel.CreateToolhelp32Snapshot.restype = w.HANDLE
    kernel.Process32FirstW.argtypes = kernel.Process32NextW.argtypes = [w.HANDLE, c.POINTER(Entry)]
    kernel.CloseHandle.argtypes = [w.HANDLE]
    snapshot = kernel.CreateToolhelp32Snapshot(2, 0)
    if snapshot == w.HANDLE(-1).value:
        raise c.WinError(c.get_last_error())
    processes, entry = {}, Entry()
    entry.size = c.sizeof(entry)
    try:
        more = kernel.Process32FirstW(snapshot, c.byref(entry))
        while more:
            processes[entry.pid] = (entry.parent, entry.exe)
            more = kernel.Process32NextW(snapshot, c.byref(entry))
    finally:
        kernel.CloseHandle(snapshot)
    return processes


def _windows_command_line(pid):
    """Read a process command line without entering a WMI provider."""
    import ctypes as c
    from ctypes import wintypes as w
    kernel=c.WinDLL('kernel32',use_last_error=True)
    kernel.OpenProcess.argtypes=[w.DWORD,w.BOOL,w.DWORD];kernel.OpenProcess.restype=w.HANDLE
    kernel.CloseHandle.argtypes=[w.HANDLE]
    handle=kernel.OpenProcess(0x1000,False,pid)
    if not handle:return ''
    try:
        query=c.WinDLL('ntdll').NtQueryInformationProcess
        query.argtypes=[w.HANDLE,w.ULONG,c.c_void_p,w.ULONG,c.POINTER(w.ULONG)]
        class String(c.Structure):
            _fields_=[('length',w.USHORT),('maximum',w.USHORT),('buffer',c.c_void_p)]
        size=w.ULONG();query(handle,60,None,0,c.byref(size))
        if not size.value:return ''
        buffer=c.create_string_buffer(size.value)
        if query(handle,60,buffer,size,c.byref(size))!=0:return ''
        value=String.from_buffer(buffer)
        return c.wstring_at(value.buffer,value.length//2) if value.length else ''
    finally:kernel.CloseHandle(handle)


def _windows_visible():
    import ctypes as c
    from ctypes import wintypes as w

    kernel = c.WinDLL('kernel32', use_last_error=True)
    user = c.WinDLL('user32', use_last_error=True)
    kernel.GetConsoleWindow.restype = w.HWND
    user.IsWindowVisible.argtypes = [w.HWND]
    user.GetClassNameW.argtypes = [w.HWND, w.LPWSTR, c.c_int]
    window = kernel.GetConsoleWindow()
    if not window:
        return False
    name = c.create_unicode_buffer(128)
    user.GetClassNameW(window, name, len(name))
    if name.value != 'PseudoConsoleWindow':
        return bool(user.IsWindowVisible(window))

    # ConPTY's own HWND is hidden. A visible chat/editor ancestor does not
    # prove that its terminal pane is visible; require an actual terminal host.
    parents = {pid: parent for pid, (parent, _) in _windows_processes().items()}
    user.GetWindowThreadProcessId.argtypes = [w.HWND, c.POINTER(w.DWORD)]
    owner = w.DWORD()
    user.GetWindowThreadProcessId(window, c.byref(owner))
    ancestors = set()
    for pid in (os.getpid(), owner.value):
        while pid and pid not in ancestors:
            ancestors.add(pid)
            pid = parents.get(pid, 0)
    user.GetShellWindow.restype = w.HWND
    user.GetWindowThreadProcessId(user.GetShellWindow(), c.byref(owner))
    ancestors.discard(owner.value)  # Explorer launching an app is not its terminal.
    found = []
    callback_type = c.WINFUNCTYPE(w.BOOL, w.HWND, w.LPARAM)

    @callback_type
    def visit(hwnd, _):
        pid = w.DWORD()
        user.GetWindowThreadProcessId(hwnd, c.byref(pid))
        if pid.value in ancestors and user.IsWindowVisible(hwnd):
            host_class = c.create_unicode_buffer(128)
            user.GetClassNameW(hwnd, host_class, len(host_class))
            if host_class.value in {'ConsoleWindowClass', 'CASCADIA_HOSTING_WINDOW_CLASS'}:
                found.append(True)
        return True

    user.EnumWindows.argtypes = [callback_type, w.LPARAM]
    user.EnumWindows(visit, 0)
    return bool(found)


def _posix_visible():
    if os.tcgetpgrp(sys.stdin.fileno()) != os.getpgrp():
        return False
    if os.environ.get('TMUX'):
        pane = os.environ.get('TMUX_PANE')
        if not pane:
            return False
        result = subprocess.run(['tmux', 'display-message', '-p', '-t', pane,
                                 '#{session_attached}'], capture_output=True, text=True, timeout=2)
        return result.returncode == 0 and int(result.stdout.strip()) > 0
    if os.environ.get('STY'):
        result = subprocess.run(['screen', '-ls'], capture_output=True, text=True, timeout=2)
        return any(line.strip().startswith(os.environ['STY'] + '\t') and '(Attached)' in line
                   for line in result.stdout.splitlines())
    return True


def require_visible():
    """No listener, calibration, state mutation or supervisor precedes this check."""
    try:
        visible = (sys.stdin.isatty() and sys.stdout.isatty() and
                   (_windows_visible() if os.name == 'nt' else _posix_visible()))
    except (OSError, ValueError, AttributeError, subprocess.SubprocessError):
        visible = False
    if not visible:
        raise RuntimeError('DCO需要可见终端并在前台运行。请在终端运行 start.cmd（Windows）、'
                           './start.sh（Linux/macOS）或 python main.py；'
                           '不要隐藏窗口、重定向输入输出或使用无终端的后台服务。')
    if os.name == 'nt':
        import ctypes
        title = ctypes.WinDLL('kernel32').SetConsoleTitleW
        title.argtypes = [ctypes.c_wchar_p]
        title('DCO | ' + socket.gethostname())
