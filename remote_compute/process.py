"""Own service power requests and launched process trees."""

from contextlib import contextmanager
import os
from pathlib import Path
import signal
import shutil
import subprocess
import sys
import threading
import time


_proc_lock = threading.Lock()
_proc_sample = (0.0, {})


def _proc_rows(fresh=False):
    """Share scans, including overlapping termination requests."""
    global _proc_sample
    requested = time.monotonic()
    with _proc_lock:
        if _proc_sample[0] > requested or (not fresh and time.monotonic()-_proc_sample[0]<.25):
            return _proc_sample[1]
        # A cleanup may reuse a scan that began after it requested freshness.
        # Otherwise many simultaneous exits serialize two full /proc scans each.
        started = time.monotonic()
        rows={}
        for entry in Path('/proc').iterdir():
            if not entry.name.isdigit():continue
            try:rows[int(entry.name)]=(entry/'stat').read_text().rsplit(')',1)[1].split()
            except (OSError,ValueError):continue
        _proc_sample=(started,rows)
        return rows


@contextmanager
def keep_awake():
    """Own a sleep inhibitor for the service lifetime, including reloads."""
    if os.name != 'nt':
        inhibitor = shutil.which('systemd-inhibit') if sys.platform.startswith('linux') else None
        if inhibitor:
            import select
            command = [inhibitor, '--what=idle', '--mode=block', '--who=DCO',
                       '--why=DCO compute service is running', sys.executable, '-B', '-u', '-c',
                       'import sys;print("DCO_POWER_READY",flush=True);sys.stdin.buffer.read()']
            with subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE) as helper:
                try:
                    ready = select.select([helper.stdout], [], [], 5)[0]
                    if not ready or helper.stdout.readline() != b'DCO_POWER_READY\n':
                        raise OSError('DCO could not acquire the Linux idle inhibitor')
                    yield
                finally:
                    helper.stdin.close()
                    try:
                        helper.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        helper.terminate()
                        helper.wait(timeout=5)
            return
        yield
        return
    import ctypes as c
    from ctypes import wintypes as w

    class Detailed(c.Structure):
        _fields_ = [('module', w.HMODULE), ('reason', w.ULONG),
                    ('count', w.ULONG), ('strings', c.POINTER(w.LPWSTR))]
    class Reason(c.Union):
        _fields_ = [('text', w.LPWSTR), ('details', Detailed)]
    class Context(c.Structure):
        _anonymous_ = ('reason',)
        _fields_ = [('version', w.ULONG), ('flags', w.DWORD), ('reason', Reason)]
    kernel = c.WinDLL('kernel32', use_last_error=True)
    kernel.PowerCreateRequest.argtypes = [c.POINTER(Context)]
    kernel.PowerCreateRequest.restype = w.HANDLE
    kernel.PowerSetRequest.argtypes = kernel.PowerClearRequest.argtypes = [w.HANDLE, c.c_int]
    kernel.PowerSetRequest.restype = kernel.PowerClearRequest.restype = w.BOOL
    kernel.CloseHandle.argtypes = [w.HANDLE]
    reason = Context()
    reason.flags = 1  # POWER_REQUEST_CONTEXT_SIMPLE_STRING
    reason.text = 'DCO compute service is running'
    handle = kernel.PowerCreateRequest(c.byref(reason))
    if not handle or handle == w.HANDLE(-1).value:
        raise c.WinError(c.get_last_error())
    requests = []
    try:
        # SystemRequired and ExecutionRequired cover idle sleep and execution
        # suspension. Neither requests DisplayRequired, so monitors may sleep.
        for kind in (1, 3):
            if not kernel.PowerSetRequest(handle, kind):
                raise c.WinError(c.get_last_error())
            requests.append(kind)
        yield
    finally:
        for kind in reversed(requests):
            kernel.PowerClearRequest(handle, kind)
        kernel.CloseHandle(handle)


def low_integrity_directory(path):
    """Let existing Low native tools write inside a newly owned job directory."""
    if os.name != 'nt':
        return
    import ctypes as c
    from ctypes import wintypes as w

    security=c.WinDLL('advapi32',use_last_error=True)
    kernel=c.WinDLL('kernel32',use_last_error=True)
    security.ConvertStringSecurityDescriptorToSecurityDescriptorW.argtypes=[w.LPCWSTR,w.DWORD,c.POINTER(c.c_void_p),c.POINTER(w.DWORD)]
    security.ConvertStringSecurityDescriptorToSecurityDescriptorW.restype=w.BOOL
    security.SetFileSecurityW.argtypes=[w.LPCWSTR,w.DWORD,c.c_void_p]
    security.SetFileSecurityW.restype=w.BOOL
    kernel.LocalFree.argtypes=[c.c_void_p]
    kernel.LocalFree.restype=c.c_void_p
    descriptor=c.c_void_p()
    if not security.ConvertStringSecurityDescriptorToSecurityDescriptorW('S:(ML;OICI;NW;;;LW)',1,c.byref(descriptor),None):
        raise c.WinError(c.get_last_error())
    try:
        # These directories are newly owned and empty. Set only the label:
        # SetNamedSecurityInfo also normalizes inherited DACL ACE flags here.
        # Future children inherit OICI; existing dependency trees are untouched.
        if not security.SetFileSecurityW(str(path),0x10,descriptor):
            error=c.get_last_error()
            if error in (5,1314):return False
            raise c.WinError(error)
    finally:
        kernel.LocalFree(descriptor)


class Process:
    def __init__(self, argv, cwd, env, stdout, stderr, *, stdin=None):
        self.job = None
        options = {"start_new_session": True} if os.name != "nt" else {
            "creationflags": subprocess.CREATE_NO_WINDOW | 4,  # CREATE_SUSPENDED
        }
        self.child = subprocess.Popen(argv, cwd=cwd, env=env, stdin=stdin if stdin is not None else subprocess.DEVNULL,
                                      stdout=stdout, stderr=stderr, **options)
        if os.name == "nt":
            try:
                self._windows_job()
            except BaseException:
                self.child.kill()
                self.child.wait()
                self.close()
                raise

    def _windows_job(self):
        import ctypes as c
        from ctypes import wintypes as w

        class Basic(c.Structure):
            _fields_ = [("process_time", c.c_int64), ("job_time", c.c_int64),
                        ("flags", w.DWORD), ("min_working", c.c_size_t),
                        ("max_working", c.c_size_t), ("active", w.DWORD),
                        ("affinity", c.c_size_t), ("priority", w.DWORD), ("scheduling", w.DWORD)]

        class IO(c.Structure):
            _fields_ = [(name, c.c_uint64) for name in ("read_ops", "write_ops", "other_ops",
                                                      "read_bytes", "write_bytes", "other_bytes")]

        class Extended(c.Structure):
            _fields_ = [("basic", Basic), ("io", IO), ("process_mem", c.c_size_t),
                        ("job_mem", c.c_size_t), ("peak_process", c.c_size_t), ("peak_job", c.c_size_t)]

        self.kernel = c.WinDLL("kernel32", use_last_error=True)
        self.kernel.CreateJobObjectW.argtypes = [c.c_void_p, w.LPCWSTR]
        self.kernel.CreateJobObjectW.restype = w.HANDLE
        self.kernel.SetInformationJobObject.argtypes = [w.HANDLE, c.c_int, c.c_void_p, w.DWORD]
        self.kernel.AssignProcessToJobObject.argtypes = [w.HANDLE, w.HANDLE]
        self.kernel.TerminateJobObject.argtypes = [w.HANDLE, w.UINT]
        self.kernel.CloseHandle.argtypes = [w.HANDLE]
        self.kernel.QueryInformationJobObject.argtypes = [w.HANDLE,c.c_int,c.c_void_p,w.DWORD,c.c_void_p]
        self.kernel.OpenProcess.argtypes = [w.DWORD,w.BOOL,w.DWORD]
        self.kernel.OpenProcess.restype = w.HANDLE
        self.kernel.WaitForSingleObject.argtypes = [w.HANDLE,w.DWORD]
        self.job = self.kernel.CreateJobObjectW(None, None)
        info = Extended()
        info.basic.flags = 0x2000  # KILL_ON_JOB_CLOSE, including worker termination
        if (not self.job or not self.kernel.SetInformationJobObject(self.job, 9, c.byref(info), c.sizeof(info))
                or not self.kernel.AssignProcessToJobObject(self.job, int(self.child._handle))):
            raise c.WinError(c.get_last_error())
        native = c.WinDLL("ntdll")
        native.NtResumeProcess.argtypes = [w.HANDLE]
        native.NtResumeProcess.restype = c.c_long
        if native.NtResumeProcess(int(self.child._handle)) != 0:
            raise OSError("Could not resume owned process")

    def _process_handles(self):
        """Retain waitable handles before job accounting drops exiting children."""
        import ctypes as c
        count=32
        while True:
            buffer=c.create_string_buffer(8+c.sizeof(c.c_size_t)*count)
            if self.kernel.QueryInformationJobObject(self.job,3,buffer,len(buffer),None):
                found=c.c_uint32.from_buffer(buffer,4).value
                identities=(c.c_size_t*found).from_buffer(buffer,8)
                handles = [self.kernel.OpenProcess(0x100000,False,identity) for identity in identities]
                return [handle for handle in handles if handle]
            if c.get_last_error()!=234:  # ERROR_MORE_DATA
                raise c.WinError(c.get_last_error())
            count=max(count*2,c.c_uint32.from_buffer(buffer).value)

    def _posix_members(self, *, fresh=True):
        """Keep ancestry and birth times, including children that call setsid."""
        rows=_proc_rows(fresh)
        known=self.__dict__.setdefault('_posix_known',{})
        owned={pid for pid,born in known.items() if pid in rows and rows[pid][19]==born}
        if self.child.pid in rows and (self.child.pid not in known or self.child.pid in owned):owned.add(self.child.pid)
        while True:
            children={pid for pid,parts in rows.items() if int(parts[1]) in owned or int(parts[2])==self.child.pid}
            if children<=owned:break
            owned.update(children)
        for pid in owned:known[pid]=rows[pid][19]
        return {pid:rows[pid] for pid in owned}

    def _stop_posix(self):
        members=self._posix_members()
        for pid in members:
            try:os.kill(pid,signal.SIGKILL)
            except ProcessLookupError:pass
        deadline=time.monotonic()+5
        while any(parts[0]!='Z' for parts in self._posix_members().values()):
            if time.monotonic()>=deadline:raise RuntimeError('Cancelled child process did not finish exiting')
            time.sleep(.01)

    def stop(self):
        handles=self._process_handles() if os.name=='nt' and self.job else []
        try:
            self._stop(handles)
        finally:
            for handle in handles:
                self.kernel.CloseHandle(handle)

    def _stop(self, handles):
        if os.name == "nt":
            if self.job:
                self.kernel.TerminateJobObject(self.job, 1)
        else:
            self._stop_posix()
        self.child.wait()
        if os.name == 'nt' and self.job:
            # TerminateJobObject starts termination of the whole tree. Wait for
            # grandchildren too before their scratch/lock files are reclaimed.
            import ctypes as c
            from ctypes import wintypes as w
            class Accounting(c.Structure):
                _fields_ = [(name, c.c_int64) for name in ('user', 'kernel', 'period_user', 'period_kernel')] + [
                    (name, w.DWORD) for name in ('faults', 'total', 'active', 'terminated')]
            self.kernel.QueryInformationJobObject.argtypes = [w.HANDLE, c.c_int, c.c_void_p, w.DWORD, c.c_void_p]
            info = Accounting()
            deadline = time.monotonic() + 5
            for handle in handles:
                remaining=max(1,int((deadline-time.monotonic())*1000))
                if self.kernel.WaitForSingleObject(handle,remaining)!=0:
                    raise RuntimeError('Cancelled child process did not finish exiting')
            while self.kernel.QueryInformationJobObject(self.job, 1, c.byref(info), c.sizeof(info), None) and info.active:
                if time.monotonic() >= deadline:
                    raise RuntimeError('Cancelled process tree did not finish terminating')
                time.sleep(.01)

    def close(self):
        if self.job:
            self.kernel.CloseHandle(self.job)
            self.job = None
        elif os.name != "nt":
            self._stop_posix()
