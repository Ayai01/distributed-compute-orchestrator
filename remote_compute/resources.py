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
import sys
import time

MIB = 1024 * 1024
DEFAULT_MEMORY_RESERVE_MB = 16 * 1024
DEFAULT_MEMORY_HEADROOM_MB = 8 * 1024
MACHINE_ID = socket.gethostname() + ":" + f"{uuid.getnode():012x}"
CPU_FREQUENCY_INTERVAL = 1.0
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
        self.collected_at = time.monotonic()
        atexit.register(self.close)

    def close(self):
        if self.query:
            self.api.PdhCloseQuery(self.query)
            self.query = None

    def __call__(self):
        elapsed = time.monotonic()-self.collected_at
        if elapsed < CPU_FREQUENCY_INTERVAL:
            return None  # A rate counter needs a full initial sampling interval.
        value = self.Value()
        status = self.api.PdhCollectQueryData(self.query)
        collected = time.monotonic()
        elapsed = collected-self.collected_at
        self.collected_at = collected
        # After a quiet gap this rate counter needs a new baseline. On a real
        # Intel host, formatting the long first window reported a false 41 GHz.
        if status or elapsed > 2*CPU_FREQUENCY_INTERVAL:
            return None
        if (self.api.PdhGetFormattedCounterValue(self.counter, 0x200 | 0x8000, None, c.byref(value)) or
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
        if now - _frequency_sample[0] >= CPU_FREQUENCY_INTERVAL:
            reader = _frequency_reader()
            try:
                value = reader() if reader else None
            except OSError:
                value = None
            if value and (not math.isfinite(value['ghz']) or value['ghz'] <= 0):
                value = None
            _frequency_sample = (time.monotonic(), value)
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
    """Read a cached score; force=True is reserved for manual calibration."""
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


# cgroup mount and membership files are read on demand, since a running
# container can be moved or its limits changed without restarting the worker.
_CGROUP_MEMBERSHIP = Path('/proc/self/cgroup')
_CGROUP_MOUNTINFO = Path('/proc/self/mountinfo')


def _cgroup_locations():
    """Map controllers to (process directory, visible mount root, version)."""
    try:
        memberships = _CGROUP_MEMBERSHIP.read_text().splitlines()
        mounts = _CGROUP_MOUNTINFO.read_text().splitlines()
    except OSError:
        return {}
    groups = {}
    for line in memberships:
        parts = line.split(':', 2)
        if len(parts) == 3:
            for controller in parts[1].split(','):
                groups[controller] = parts[2]
    locations = {}
    for line in mounts:
        fields = line.split()
        try:
            separator = fields.index('-')
            kind = fields[separator + 1]
            if kind not in ('cgroup', 'cgroup2'):
                continue
            # mountinfo escapes spaces, tabs, newlines and backslashes as octal.
            def unescape(value):
                import re
                return re.sub(r'\\([0-7]{3})', lambda m: chr(int(m.group(1), 8)), value)
            root, mount = unescape(fields[3]), Path(unescape(fields[4]))
            controllers = [''] if kind == 'cgroup2' else fields[separator + 3].split(',')
            for controller in controllers:
                group = groups.get(controller)
                if group is None:
                    continue
                base = root.rstrip('/') or '/'
                if base != '/' and group != base and not group.startswith(base + '/'):
                    continue
                relative = group[len(base):].lstrip('/') if base != '/' else group.lstrip('/')
                locations[controller] = (mount / relative, mount, 2 if kind == 'cgroup2' else 1)
        except (ValueError, IndexError):
            continue
    return locations


def _ancestors(location):
    current, mount, version = location
    while True:
        yield current, version
        if current == mount:
            break
        parent = current.parent
        if parent == current:
            break
        current = parent


def _read(path):
    try:
        return path.read_text().strip()
    except OSError:
        return ''


def _cpu_set_count(value):
    cpus = set()
    try:
        for item in value.split(','):
            parts = item.split('-')
            first, last = int(parts[0]), int(parts[-1])
            if len(parts) > 2 or first < 0 or last < first or last > 1000000:
                return None
            cpus.update(range(first, last + 1))
    except ValueError:
        return None
    return len(cpus) or None


def _effective_cpu_capacity(locations=None):
    """Effective CPU budget before rounding it to whole scheduling slots."""
    capacity = max(1, os.cpu_count() or 1)
    if os.name == 'nt' or not sys.platform.startswith('linux'):
        return capacity
    try:
        capacity = min(capacity, max(1, len(os.sched_getaffinity(0))))
    except (AttributeError, OSError):
        pass
    if locations is None:
        locations = _cgroup_locations()
    for controller in ('', 'cpu'):
        if controller not in locations:
            continue
        for path, version in _ancestors(locations[controller]):
            try:
                if version == 2:
                    quota, period = _read(path / 'cpu.max').split()
                    if quota == 'max':
                        continue
                else:
                    quota, period = _read(path / 'cpu.cfs_quota_us'), _read(path / 'cpu.cfs_period_us')
                quota, period = int(quota), int(period)
                if quota > 0 and period > 0:
                    capacity = min(capacity, quota / period)
            except (ValueError, TypeError):
                pass
    for controller in ('', 'cpuset'):
        if controller not in locations:
            continue
        for path, version in _ancestors(locations[controller]):
            names = ('cpuset.cpus.effective', 'cpuset.cpus') if version == 2 else ('cpuset.effective_cpus', 'cpuset.cpus')
            count = next((n for n in (_cpu_set_count(_read(path / name)) for name in names) if n), None)
            if count:
                capacity = min(capacity, count)
    return capacity


def cpu_capacity():
    """Effective integer CPU capacity, respecting Linux cgroup and affinity."""
    return max(1, int(_effective_cpu_capacity()))


def _cgroup_memory(total, available):
    locations = _cgroup_locations()
    for controller in ('', 'memory'):
        if controller not in locations:
            continue
        for path, version in _ancestors(locations[controller]):
            limit_name, usage_name = (('memory.max', 'memory.current') if version == 2
                                      else ('memory.limit_in_bytes', 'memory.usage_in_bytes'))
            try:
                limit = int(_read(path / limit_name))
                # v1 represents unlimited with a near-LONG_MAX sentinel.
                if limit <= 0 or limit >= (1 << 60):
                    continue
                total = min(total, limit // MIB)
                available = min(available, total)
                usage = int(_read(path / usage_name))
                available = min(available, max(0, limit - usage) // MIB)
            except ValueError:
                continue
    return total, available


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
    total, available = values["MemTotal"], values["MemAvailable"]
    return _cgroup_memory(total, available) if sys.platform.startswith("linux") else (total, available)


def commit_memory():
    """Windows allocation headroom can run out while physical RAM is free."""
    if os.name != "nt":
        return None
    class Memory(c.Structure):
        _fields_ = [("length", c.c_ulong), ("load", c.c_ulong),
                    *[(name, c.c_uint64) for name in ("total", "available", "page_total", "page_available",
                                                     "virtual_total", "virtual_available", "extended")]]
    value = Memory()
    value.length = c.sizeof(value)
    if not c.windll.kernel32.GlobalMemoryStatusEx(c.byref(value)):
        raise c.WinError()
    return value.page_total // MIB, value.page_available // MIB


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


def _cgroup_cpu_counter(locations):
    """Read the cumulative CPU time charged to the worker's group."""
    if '' in locations:
        path = locations[''][0] / 'cpu.stat'
        for line in _read(path).splitlines():
            name, separator, value = line.partition(' ')
            if separator and name == 'usage_usec' and value.strip().isdigit():
                return path, int(value) * 1000
    if 'cpuacct' in locations:
        path = locations['cpuacct'][0] / 'cpuacct.usage'
        value = _read(path)
        if value.isdigit():
            return path, int(value)
    return None


class Host:
    def __init__(self):
        self.previous = cpu_ticks()
        self.sampled = 0.0
        self.cpu_percent = None
        self.host_cpu_percent = None
        self.cpu_scope = 'host'
        self.group_cpu_sample = None
        self.lock = threading.Lock()

    def sample(self):
        total_mb, available_mb = memory()
        commit = commit_memory()
        locations = _cgroup_locations() if sys.platform.startswith('linux') else {}
        capacity = _effective_cpu_capacity(locations)
        with self.lock:
            now = time.monotonic()
            if now - self.sampled >= 0.5:
                current = cpu_ticks()
                elapsed, idle = current[0] - self.previous[0], current[1] - self.previous[1]
                if elapsed:
                    self.host_cpu_percent = round(max(0, min(100, 100 * (1 - idle / elapsed))), 2)
                self.previous, self.sampled = current, now
                group = _cgroup_cpu_counter(locations) if capacity < (os.cpu_count() or 1) else None
                if group is None:
                    self.group_cpu_sample = None
                    self.cpu_scope, self.cpu_percent = 'host', self.host_cpu_percent
                else:
                    source, counter = group
                    previous = self.group_cpu_sample
                    self.group_cpu_sample = (source, capacity, counter, now)
                    self.cpu_scope, self.cpu_percent = 'cgroup', None
                    if previous and previous[:2] == (source, capacity) and counter >= previous[2]:
                        cpu_seconds = (counter - previous[2]) / 1e9
                        self.cpu_percent = round(min(100, 100 * cpu_seconds / ((now - previous[3]) * capacity)), 2)
            result = {"machine_id": MACHINE_ID, "logical_cpus": max(1, int(capacity)),
                      "cpu_percent": self.cpu_percent, "cpu_scope": self.cpu_scope,
                      "host_cpu_percent": self.host_cpu_percent, "temperature_c": None,
                      "memory_total_mb": total_mb, "host_memory_available_mb": available_mb}
        if commit is not None:
            result.update(host_commit_limit_mb=commit[0], host_commit_available_mb=commit[1])
        return result


CPU_STAGES = ('compile', 'load_check', 'projection', 'sim')


def process_usage(process):
    """Sum the owned job/process group; CPU time includes completed Windows children."""
    if os.name != "nt":
        rss, seconds = 0, 0.0
        totals=process.__dict__.setdefault('_posix_cpu',{})
        for pid,parts in process._posix_members(fresh=False).items():
            totals[(pid,parts[19])]=(int(parts[11])+int(parts[12]))/os.sysconf('SC_CLK_TCK')
            rss+=int(parts[21])*os.sysconf('SC_PAGE_SIZE')
        seconds=max(sum(totals.values()), getattr(process.child, 'reaped_cpu_seconds', 0.0))
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
    rss = private = 0
    for pid in pids.ids[:pids.count]:
        handle = kernel.OpenProcess(0x1010, False, pid)
        if handle:
            try:
                info = MemoryInfo()
                info.size = c.sizeof(info)
                if psapi.GetProcessMemoryInfo(handle, c.byref(info), c.sizeof(info)):
                    rss += info.rss
                    private += info.private
            finally:
                kernel.CloseHandle(handle)
    return {"rss_mb": rss / MIB, 'private_mb':private/MIB, "cpu_seconds": (accounting.user + accounting.kernel) / 1e7}
