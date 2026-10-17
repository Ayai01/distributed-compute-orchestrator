"""One shared 8 GiB scratch volume per host, coordinated across all projects."""
from contextlib import contextmanager
import ctypes as c
from ctypes import wintypes as w
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import re
import sys

MARKER = '.codex-shared-ramdisk.json'
SIZE = 8 * 1024**3
ROOT = Path(__file__).resolve().parent.parent


def _run(argv):
    result = subprocess.run([str(x) for x in argv], capture_output=True, text=True,
                            errors='replace', timeout=60, creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
    if result.returncode:
        raise OSError((result.stdout + result.stderr).strip())
    return result.stdout


def driver():
    for path in (Path(os.environ.get('SystemRoot', 'C:/Windows')) / 'System32/imdisk.exe',
                 ROOT / '.tools/imdisk/imdisk.exe'):
        if path.is_file():
            return str(path)
    return shutil.which('imdisk')


@contextmanager
def creation_lock():
    kernel = c.WinDLL('kernel32', use_last_error=True)
    kernel.CreateMutexW.argtypes = [c.c_void_p, w.BOOL, w.LPCWSTR]
    kernel.CreateMutexW.restype = w.HANDLE
    kernel.WaitForSingleObject.argtypes = [w.HANDLE, w.DWORD]
    kernel.ReleaseMutex.argtypes = kernel.CloseHandle.argtypes = [w.HANDLE]
    handle = kernel.CreateMutexW(None, False, 'Global\\CodexSharedRamdiskV1')
    if not handle:
        raise c.WinError(c.get_last_error())
    acquired = False
    try:
        acquired = kernel.WaitForSingleObject(handle, 60000) in (0, 0x80)
        if not acquired:
            raise TimeoutError('Shared RAM disk creation lock')
        yield
    finally:
        if acquired:
            kernel.ReleaseMutex(handle)
        kernel.CloseHandle(handle)


def volumes():
    if os.name != 'nt':
        return []
    mask = c.windll.kernel32.GetLogicalDrives()
    return [Path(chr(65 + i) + ':/') for i in range(25, 2, -1) if mask & (1 << i)]


def is_ram(root):
    kernel = c.windll.kernel32
    kernel.GetDriveTypeW.argtypes = [w.LPCWSTR]
    if kernel.GetDriveTypeW(str(root)) == 6:
        return True
    executable = driver()
    if executable:
        try:
            value = _run([executable, '-l', '-m', str(root)[:2]])
            return 'No image file.' in value and ('Virtual Memory' in value or 'Proxy' in value)
        except OSError:
            pass
    return False


def shared_volume():
    for root in volumes():
        try:
            info = json.loads((root / MARKER).read_text(encoding='utf-8-sig'))
            if info.get('kind') == 'ramdisk' and info.get('shared') is True and is_ram(root):
                return root
        except (OSError, ValueError):
            continue
    return None


def ensure_shared(size_gib=8):
    """Reuse/extend the marked volume; never create a second disk after a failed resize."""
    if size_gib != 8:
        raise ValueError('Shared RAM disk capacity is fixed at 8 GiB')
    if os.name != 'nt':
        if os.environ.get('REMOTE_COMPUTE_WORKER')=='1':
            return None  # Linux jobs follow their configured ordinary-disk TMPDIR.
        root = Path('/dev/shm')
        return root if root.is_dir() else None
    with creation_lock():
        root = shared_volume()
        executable = driver()
        if root:
            if shutil.disk_usage(root).total > SIZE + 32 * 1024**2:
                raise OSError('Shared RAM disk exceeds 8 GiB; preserve its files and resize it during maintenance')
            if shutil.disk_usage(root).total < SIZE - 32 * 1024**2:
                if not executable:
                    raise OSError('Install ImDisk to extend the existing shared RAM disk to 8 GiB')
                details = _run([executable, '-l', '-m', str(root)[:2]])
                current = int(re.search(r'Size:\s*(\d+) bytes', details)[1])
                _run([executable, '-e', '-m', str(root)[:2], '-s', str(SIZE - current)])
            if shutil.disk_usage(root).total < SIZE - 32 * 1024**2:
                raise OSError('Existing shared RAM disk still needs extension to 8 GiB')
        else:
            if not executable:
                return None
            occupied = {str(p)[0].upper() for p in volumes()}
            letter = next(x for x in 'ZYXWVUTSRQPONMLKJIHGFED' if x not in occupied)
            root = Path(letter + ':/')
            _run([executable, '-a', '-t', 'vm', '-s', '8G', '-m', letter + ':',
                  '-p', '/fs:ntfs /q /y /v:SHARED_RAM'])
        (root / MARKER).write_text(json.dumps(dict(version=1, kind='ramdisk', shared=True,
            capacity_bytes=SIZE, purpose='Shared simulation scratch for all local projects'), indent=2), encoding='utf-8')
        return root


def scratch(project):
    volume = ensure_shared()
    root = (volume or Path(tempfile.gettempdir())) / project
    root.mkdir(parents=True, exist_ok=True)
    return root


def install_driver():
    """Install a signed ImDisk driver staged in ignored .tools/imdisk (Windows admin)."""
    if os.name != 'nt':
        raise ValueError('ImDisk installation is for Windows')
    if not c.windll.shell32.IsUserAnAdmin():
        raise PermissionError('Run the RAM disk setup once as administrator')
    if not (Path(os.environ['SystemRoot']) / 'System32/drivers/imdisk.sys').exists():
        source = ROOT / '.tools/imdisk'
        for name, target in [('imdisk.sys', 'System32/drivers/imdisk.sys'),
                             ('imdisk.exe', 'System32/imdisk.exe'), ('imdisk.cpl', 'System32/imdisk.cpl')]:
            shutil.copy2(source / name, Path(os.environ['SystemRoot']) / target)
        _run(['sc.exe', 'create', 'imdisk', 'type=', 'kernel', 'start=', 'demand',
              'binPath=', str(Path(os.environ['SystemRoot']) / 'System32/drivers/imdisk.sys')])
    # Windows recreates volatile storage after a reboot. All project launches
    # still enter the same mutex, so a concurrent launch reuses this one disk.
    command = subprocess.list2cmdline([sys.executable, '-B', str(ROOT / 'main.py'), 'ramdisk'])
    _run(['schtasks.exe', '/Create', '/TN', 'RemoteCompute Shared RAM', '/SC', 'ONSTART',
          '/RU', 'SYSTEM', '/RL', 'HIGHEST', '/TR', command, '/F'])
    return ensure_shared()
