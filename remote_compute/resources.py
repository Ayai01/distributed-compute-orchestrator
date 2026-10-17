"""CPU and physical-memory observations without third-party dependencies."""

import ctypes as c
from concurrent.futures import ThreadPoolExecutor
from functools import cache
import hashlib
import os
from pathlib import Path
import socket
import ssl
import threading
import uuid
import json
import subprocess
import time

MIB = 1024 * 1024
MACHINE_ID = socket.gethostname() + ":" + f"{uuid.getnode():012x}"
_temperature = None
_sensor_lock = threading.Lock()
_sensor_started = False


@cache
def cpu_benchmark(cpus):
    """Measure total native throughput at the configured worker thread budget."""
    if type(cpus) is not int or cpus < 1:
        raise ValueError('Benchmark cpus must be a positive integer')
    password,salt=b'Remote Compute CPU benchmark v1',b'fixed benchmark input'
    chunk=16000
    started=0.0
    def start():
        nonlocal started
        started=time.perf_counter()
    ready=threading.Barrier(cpus,action=start)
    def measure(_index):
        hashlib.pbkdf2_hmac('sha256',password,salt,chunk,dklen=32)
        ready.wait()
        iterations=0
        while time.perf_counter()-started<2.0:
            hashlib.pbkdf2_hmac('sha256',password,salt,chunk,dklen=32)
            iterations+=chunk
        return iterations,time.perf_counter()
    with ThreadPoolExecutor(max_workers=cpus,thread_name_prefix='cpu-benchmark') as executor:
        samples=list(executor.map(measure,range(cpus)))
    iterations=sum(count for count,_ in samples)
    elapsed=max(finished for _,finished in samples)-started
    return dict(name='pbkdf2-sha256-v1',score=round(iterations/elapsed/1000,1),unit='kiter/s',
                threads=cpus,seconds=round(elapsed,3),iterations=iterations,openssl=ssl.OPENSSL_VERSION)


def _sensors():
    global _temperature
    while True:
        try:
            if os.name == 'nt':
                script = ("$v=@(); foreach($n in 'root/LibreHardwareMonitor','root/OpenHardwareMonitor') { "
                          "$v+=Get-CimInstance -Namespace $n -ClassName Sensor -ErrorAction SilentlyContinue | "
                          "Where-Object { $_.SensorType -eq 'Temperature' -and $_.Parent -match 'cpu' } | "
                          "Select-Object -ExpandProperty Value }; $v | ConvertTo-Json -Compress")
                run = subprocess.run(['powershell.exe', '-NoProfile', '-Command', script], capture_output=True,
                                     text=True, errors='replace', timeout=8,
                                     creationflags=subprocess.CREATE_NO_WINDOW)
                value = json.loads(run.stdout or 'null')
                values = value if isinstance(value, list) else [value]
            else:
                values = []
                for base in Path('/sys/class/hwmon').glob('*'):
                    if (base / 'name').read_text().strip() in {'coretemp', 'k10temp', 'zenpower'}:
                        values.extend(float(p.read_text()) / 1000 for p in base.glob('temp*_input'))
            valid = [x for x in values if isinstance(x, (int, float)) and 0 < x < 130]
            _temperature = round(max(valid), 1) if valid else None
        except (OSError, ValueError, subprocess.TimeoutExpired):
            _temperature = None
        time.sleep(30 if _temperature is not None else 120)


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
        global _sensor_started
        self.previous = cpu_ticks()
        self.sampled = 0.0
        self.cpu_percent = None
        self.lock = threading.Lock()
        with _sensor_lock:
            if not _sensor_started:
                _sensor_started = True
                threading.Thread(target=_sensors, name='cpu-temperature', daemon=True).start()

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
                "cpu_percent": self.cpu_percent, "temperature_c": _temperature,
                "memory_total_mb": total_mb, "host_memory_available_mb": available_mb}


def process_usage(process):
    """Sum the owned job/process group; CPU time includes completed Windows children."""
    if os.name != "nt":
        rss, seconds = 0, 0.0
        for entry in Path("/proc").iterdir():
            if not entry.name.isdigit():
                continue
            try:
                parts = (entry / "stat").read_text().rsplit(")", 1)[1].split()
                if int(parts[2]) == process.child.pid:  # pgrp, after removing pid/comm
                    seconds += (int(parts[11]) + int(parts[12])) / os.sysconf("SC_CLK_TCK")
                    rss += int(parts[21]) * os.sysconf("SC_PAGE_SIZE")
            except (OSError, ValueError, IndexError):
                continue  # a child exited between enumeration and observation
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
