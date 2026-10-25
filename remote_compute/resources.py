"""CPU and physical-memory observations without third-party dependencies."""

import ctypes as c
import atexit
from functools import lru_cache
import os
from pathlib import Path
import socket
import threading
import uuid
import json
import math
import subprocess
import time

MIB = 1024 * 1024
DEFAULT_MEMORY_RESERVE_MB = 16 * 1024
DEFAULT_MEMORY_HEADROOM_MB = 8 * 1024
MACHINE_ID = socket.gethostname() + ":" + f"{uuid.getnode():012x}"
_frequency_lock = threading.Lock()
_frequency_sample = (-math.inf, None)


class _WindowsFrequency:
    def __init__(self):
        from ctypes import wintypes as w
        class Value(c.Structure):
            _fields_ = [('status', w.DWORD), ('value', c.c_double)]
        self.Value = Value
        self.api = c.WinDLL('pdh', use_last_error=True)
        self.api.PdhOpenQueryW.argtypes = [w.LPCWSTR, c.c_size_t, c.POINTER(w.HANDLE)]
        self.api.PdhAddEnglishCounterW.argtypes = [w.HANDLE, w.LPCWSTR, c.c_size_t, c.POINTER(w.HANDLE)]
        self.api.PdhCollectQueryData.argtypes = [w.HANDLE]
        self.api.PdhGetFormattedCounterValue.argtypes = [w.HANDLE, w.DWORD, c.c_void_p, c.POINTER(Value)]
        self.api.PdhCloseQuery.argtypes = [w.HANDLE]
        self.query, self.counter = w.HANDLE(), w.HANDLE()
        if self.api.PdhOpenQueryW(None, 0, c.byref(self.query)):
            raise OSError('PDH frequency query unavailable')
        if self.api.PdhAddEnglishCounterW(self.query,
                r'\Processor Information(_Total)\Actual Frequency', 0, c.byref(self.counter)):
            self.close()
            raise OSError('PDH Actual Frequency unavailable')
        self.api.PdhCollectQueryData(self.query)
        self.started = time.monotonic()
        atexit.register(self.close)

    def close(self):
        if self.query:
            self.api.PdhCloseQuery(self.query)
            self.query = None

    def __call__(self):
        if time.monotonic()-self.started < 1:
            return None  # A rate counter needs a full initial sampling interval.
        value = self.Value()
        if (self.api.PdhCollectQueryData(self.query) or
                self.api.PdhGetFormattedCounterValue(self.counter, 0x200 | 0x8000, None, c.byref(value)) or
                value.status not in (0, 1)):
            return None
        return dict(ghz=value.value / 1000, source='windows.pdh.actual_frequency',
                    scope='host_active_average_estimate')


def _linux_frequency():
    values = []
    for policy in Path('/sys/devices/system/cpu/cpufreq').glob('policy*'):
        for name in ('cpuinfo_avg_freq', 'cpuinfo_cur_freq'):
            try:
                value = float((policy / name).read_text()) / 1e6
            except (OSError, ValueError):
                continue
            if value > 0 and math.isfinite(value):
                values.append(value)
                break
    return dict(ghz=sum(values)/len(values), source='linux.cpufreq.hardware_frequency',
                scope='host_reported_policy_average_estimate') if values else None


@lru_cache(maxsize=1)
def _frequency_reader():
    try:
        return _WindowsFrequency() if os.name == 'nt' else _linux_frequency
    except OSError:
        return None


def cpu_frequency():
    """One host observation per second; never substitute a nominal clock."""
    global _frequency_sample
    with _frequency_lock:
        now = time.monotonic()
        if now - _frequency_sample[0] >= 1:
            reader = _frequency_reader()
            try:
                value = reader() if reader else None
            except OSError:
                value = None
            if value and (not math.isfinite(value['ghz']) or value['ghz'] <= 0):
                value = None
            _frequency_sample = (now, value)
        return _frequency_sample[1]


class CpuAccounting:
    """Integrate task CPU-time deltas using the reported host active clock."""
    def __init__(self):
        self.seconds = self.covered = self.work = 0.0
        self.source = None
        self.scope = None
        cpu_frequency()  # Prime the shared counter before launching the child.

    def update(self, seconds):
        seconds = max(self.seconds, seconds)
        delta = seconds - self.seconds
        frequency = cpu_frequency()
        if delta and frequency:
            self.covered += delta
            self.work += delta * frequency['ghz']
            self.source = frequency['source']
            self.scope = frequency.get('scope', 'host_active_average_estimate')
        self.seconds = seconds
        return dict(cumulative_cpu_seconds=seconds,
                    cpu_accounting_sampled_at=time.time(),
                    cpu_work_ghz_seconds=self.work if seconds-self.covered <= 1e-9 else None,
                    cpu_frequency=dict(source=self.source, scope=self.scope,
                        covered_cpu_seconds=self.covered, observed_ghz_seconds=self.work,
                        weighted_mean_ghz=self.work/self.covered if self.covered else None))
def cpu_benchmark(cpus, executable=None, environment=(), *, force=False):
    """Measure a fixed circuit/RTL/scientific mix at the worker CPU budget."""
    if type(cpus) is not int or cpus < 1:
        raise ValueError('Benchmark cpus must be a positive integer')
    if force:
        from .benchmark import calibrated
        _cached_cpu_benchmark.cache_clear()
        return calibrated(cpus, executable=executable, environment=environment, force=True)
    return _cached_cpu_benchmark(cpus, executable, environment)


@lru_cache(maxsize=16)
def _cached_cpu_benchmark(cpus, executable, environment):
    from .benchmark import calibrated
    return calibrated(cpus, executable=executable, environment=environment)


cpu_benchmark.cache_clear = _cached_cpu_benchmark.cache_clear


def memory():
    if os.name == "nt":
        class Memory(c.Structure):
            _fields_ = [("length", c.c_ulong), ("load", c.c_ulong),
                        *[(name, c.c_uint64) for name in ("total", "available", "page_total", "page_available",
                                                         "virtual_total", "virtual_available", "extended")]]
        value = Memory()
        value.length = c.sizeof(value)
        if not c.windll.kernel32.GlobalMemoryStatusEx(c.byref(value)):
            raise c.WinError()
        return value.total // MIB, value.available // MIB
    values = {}
    for line in Path("/proc/meminfo").read_text().splitlines():
        key, value = line.split(":", 1)
        values[key] = int(value.split()[0]) // 1024
    return values["MemTotal"], values["MemAvailable"]


@lru_cache(maxsize=1)
def _service_api():
    from ctypes import wintypes as w
    class Counters(c.Structure):
        _fields_ = [('size', w.DWORD), ('faults', w.DWORD),
                    *[(name, c.c_size_t) for name in ('peak_rss', 'rss', 'peak_paged', 'paged',
                       'peak_nonpaged', 'nonpaged', 'pagefile', 'peak_pagefile', 'private')]]
    kernel = c.WinDLL('kernel32', use_last_error=True)
    psapi = c.WinDLL('psapi', use_last_error=True)
    psapi.GetProcessMemoryInfo.argtypes = [w.HANDLE, c.POINTER(Counters), w.DWORD]
    kernel.GetProcessHandleCount.argtypes = [w.HANDLE, c.POINTER(w.DWORD)]
    return Counters, psapi.GetProcessMemoryInfo, kernel.GetProcessHandleCount


@lru_cache(maxsize=1)
def _private_resident_mb(second):
    try:
        fields = dict(line.split(':', 1) for line in Path('/proc/self/smaps_rollup').read_text().splitlines() if ':' in line)
        return sum(int(fields[key].split()[0]) for key in ('Private_Clean', 'Private_Dirty')) / 1024
    except (OSError, KeyError, ValueError):
        return None


def service_usage():
    """Current service footprint, separate from the admitted child processes."""
    value = dict(pid=os.getpid(), threads=threading.active_count())
    if os.name == 'nt':
        from ctypes import wintypes as w
        Counters, memory_info, handle_count = _service_api()
        info = Counters(); info.size = c.sizeof(info)
        handles = w.DWORD()
        if memory_info(w.HANDLE(-1), c.byref(info), info.size):
            value.update(rss_mb=round(info.rss/MIB, 2), private_mb=round(info.private/MIB, 2))
        if handle_count(w.HANDLE(-1), c.byref(handles)):
            value['handles'] = handles.value
    else:
        entries = dict(line.split(':', 1) for line in Path('/proc/self/status').read_text().splitlines())
        value.update(rss_mb=int(entries['VmRSS'].split()[0])/1024,
                     handles=len(list(Path('/proc/self/fd').iterdir())))
        private = _private_resident_mb(int(time.monotonic()))
        if private is not None:
            value.update(private_mb=private, private_kind='resident')
    return value


def cpu_ticks():
    if os.name == "nt":
        idle, kernel, user = c.c_uint64(), c.c_uint64(), c.c_uint64()
        if not c.windll.kernel32.GetSystemTimes(c.byref(idle), c.byref(kernel), c.byref(user)):
            raise c.WinError()
        return kernel.value + user.value, idle.value
    values = [int(x) for x in Path("/proc/stat").read_text().splitlines()[0].split()[1:]]
    return sum(values[:8]), values[3] + values[4]


class Host:
    def __init__(self):
        self.previous = cpu_ticks()
        self.sampled = 0.0
        self.cpu_percent = None
        self.lock = threading.Lock()
    def sample(self):
        total_mb, available_mb = memory()
        with self.lock:
            if time.monotonic() - self.sampled >= 0.5:
                current = cpu_ticks()
                elapsed, idle = current[0] - self.previous[0], current[1] - self.previous[1]
                if elapsed:
                    self.cpu_percent = round(max(0, min(100, 100 * (1 - idle / elapsed))), 2)
                self.previous, self.sampled = current, time.monotonic()
        return {"machine_id": MACHINE_ID, "logical_cpus": os.cpu_count() or 1,
                "cpu_percent": self.cpu_percent, "temperature_c": None,
                "memory_total_mb": total_mb, "host_memory_available_mb": available_mb}


def process_usage(process):
    """Sum the owned job/process group; CPU time includes completed Windows children."""
    if os.name != "nt":
        rss, seconds = 0, 0.0
        totals=process.__dict__.setdefault('_posix_cpu',{})
        for pid,parts in process._posix_members(fresh=False).items():
            totals[(pid,parts[19])]=(int(parts[11])+int(parts[12]))/os.sysconf('SC_CLK_TCK')
            rss+=int(parts[21])*os.sysconf('SC_PAGE_SIZE')
        seconds=sum(totals.values())
        return {"rss_mb": rss / MIB, "cpu_seconds": seconds}

    from ctypes import wintypes as w

    class Accounting(c.Structure):
        _fields_ = [(name, c.c_int64) for name in ("user", "kernel", "period_user", "period_kernel")] + [
            (name, w.DWORD) for name in ("faults", "total", "active", "terminated")]

    class MemoryInfo(c.Structure):
        _fields_ = [("size", w.DWORD), ("faults", w.DWORD)] + [
            (name, c.c_size_t) for name in ("peak_rss", "rss", "peak_paged", "paged", "peak_nonpaged",
                                           "nonpaged", "pagefile", "peak_pagefile", "private")]

    query = process.kernel.QueryInformationJobObject
    query.argtypes = [w.HANDLE, c.c_int, c.c_void_p, w.DWORD, c.c_void_p]
    accounting = Accounting()
    if not query(process.job, 1, c.byref(accounting), c.sizeof(accounting), None):
        raise c.WinError(c.get_last_error())
    count = 64
    while True:
        class Pids(c.Structure):
            _fields_ = [("assigned", w.DWORD), ("count", w.DWORD), ("ids", c.c_size_t * count)]
        pids = Pids()
        if query(process.job, 3, c.byref(pids), c.sizeof(pids), None):
            break
        if c.get_last_error() != 234:  # ERROR_MORE_DATA
            raise c.WinError(c.get_last_error())
        count = max(count * 2, pids.assigned)
    kernel, psapi = process.kernel, c.WinDLL("psapi", use_last_error=True)
    kernel.OpenProcess.argtypes = [w.DWORD, w.BOOL, w.DWORD]
    kernel.OpenProcess.restype = w.HANDLE
    psapi.GetProcessMemoryInfo.argtypes = [w.HANDLE, c.c_void_p, w.DWORD]
    rss = 0
    for pid in pids.ids[:pids.count]:
        handle = kernel.OpenProcess(0x1010, False, pid)
        if handle:
            try:
                info = MemoryInfo()
                info.size = c.sizeof(info)
                if psapi.GetProcessMemoryInfo(handle, c.byref(info), c.sizeof(info)):
                    rss += info.rss
            finally:
                kernel.CloseHandle(handle)
    return {"rss_mb": rss / MIB, "cpu_seconds": (accounting.user + accounting.kernel) / 1e7}
