"""CPU and physical-memory observations without third-party dependencies."""

import ctypes as c
import os
from pathlib import Path
import socket
import threading
import uuid

MIB = 1024 * 1024
MACHINE_ID = socket.gethostname() + ":" + f"{uuid.getnode():012x}"


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
        self.previous = cpu_ticks()
        self.lock = threading.Lock()

    def sample(self):
        total_mb, available_mb = memory()
        with self.lock:
            current = cpu_ticks()
            elapsed, idle = current[0] - self.previous[0], current[1] - self.previous[1]
            self.previous = current
        return {"machine_id": MACHINE_ID, "logical_cpus": os.cpu_count() or 1,
                "cpu_percent": round(max(0, min(100, 100 * (1 - idle / elapsed))), 2) if elapsed else None,
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
