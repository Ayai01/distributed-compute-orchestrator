"""Own the launched process tree so cancellation also stops compilers."""

import os
import signal
import subprocess


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

    def stop(self):
        if os.name == "nt":
            if self.job:
                self.kernel.TerminateJobObject(self.job, 1)
        else:
            try:
                os.killpg(self.child.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        self.child.wait()

    def close(self):
        if self.job:
            self.kernel.CloseHandle(self.job)
            self.job = None
        elif os.name != "nt":
            try:
                os.killpg(self.child.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
