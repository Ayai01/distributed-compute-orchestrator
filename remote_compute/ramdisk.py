"""One configured shared scratch volume per host, coordinated across projects."""
from contextlib import contextmanager
import ctypes as c
from ctypes import wintypes as w
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import tempfile
import re
import sys

MARKER = '.codex-shared-ramdisk.json'
SIZE = 8 * 1024**3
ROOT = Path(__file__).resolve().parent.parent
MIB = 1024**2
EXECUTABLE_RAM_UNAVAILABLE = 'Executable RAM scratch is unavailable'


def _run(argv, *, timeout=60):
    result = subprocess.run([str(x) for x in argv], capture_output=True, text=True,
                            errors='replace', timeout=timeout, creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
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
    kernel.CreateMutexExW.argtypes = [c.c_void_p, w.LPCWSTR, w.DWORD, w.DWORD]
    kernel.CreateMutexExW.restype = w.HANDLE
    kernel.WaitForSingleObject.argtypes = [w.HANDLE, w.DWORD]
    kernel.ReleaseMutex.argtypes = kernel.CloseHandle.argtypes = [w.HANDLE]
    # Waiting/releasing needs SYNCHRONIZE; CreateMutexW demands ALL_ACCESS
    # when another project already created this shared object.
    handle = kernel.CreateMutexExW(None, 'Global\\CodexSharedRamdiskV1', 0, 0x100000)
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


def is_ram(root, *, timeout=60, allow_proxy=True):
    kernel = c.windll.kernel32
    kernel.GetDriveTypeW.argtypes = [w.LPCWSTR]
    if kernel.GetDriveTypeW(str(root)) == 6:
        return True
    executable = driver()
    if executable:
        try:
            value = _run([executable, '-l', '-m', str(root)[:2]], timeout=timeout)
            return 'No image file.' in value and ('Virtual Memory' in value or allow_proxy and 'Proxy' in value)
        except (OSError, subprocess.TimeoutExpired):
            pass
    return False


def validate_executable_ram_mb(value):
    if type(value) is not int or value < 1:
        raise ValueError('executable_ram_mb must be a positive integer in MiB')
    return value


def _linux_memory_mount(path, device):
    match = f'{os.major(device)}:{os.minor(device)}'
    with Path('/proc/self/mountinfo').open(encoding='utf-8', errors='surrogateescape') as mounts:
        for line in mounts:
            before, _, after = line.partition(' - ')
            fields = before.split()
            if len(fields) < 5 or fields[2] != match or not after:
                continue
            mount = Path(re.sub(r'\\([0-7]{3})', lambda m: chr(int(m[1], 8)), fields[4]))
            if (path == mount or mount in path.parents) and after.split()[0] in ('tmpfs', 'ramfs'):
                return True
    return False


class ExecutableScratch:
    """An existing RAM filesystem on which the worker has launched a native executable."""

    def __init__(self, root):
        self.root = root
        self.identity = self._identity()

    def _identity(self):
        current = self.root.stat()
        return current.st_dev, current.st_ino

    def space(self):
        """Return capacity and free MiB. A changed mount needs a fresh probe."""
        try:
            if self._identity() != self.identity:
                return 0, 0
            if os.name == 'nt':
                usage = shutil.disk_usage(self.root)
                return usage.total // MIB, usage.free // MIB
            state = os.statvfs(self.root)
            if state.f_flag & (os.ST_NOEXEC | os.ST_RDONLY):
                return 0, 0
            return state.f_blocks * state.f_frsize // MIB, state.f_bavail * state.f_frsize // MIB
        except OSError:
            return 0, 0


def probe_executable_scratch(configured, namespace):
    candidates = [(configured, False)] if configured else []
    if sys.platform.startswith('linux'):
        runtime = os.environ.get('XDG_RUNTIME_DIR')
        if runtime:
            candidates.append((Path(runtime), True))
        candidates.extend((path, True) for path in (Path(f'/run/user/{os.getuid()}'), Path('/dev/shm')))
    elif os.name != 'nt':
        return None
    seen = set()
    for candidate, separate in candidates:
        try:
            try:
                base = candidate.resolve(strict=True)
            except OSError as error:
                # ImDisk can report ERROR_INVALID_FUNCTION for handle-based
                # path resolution despite supporting ordinary file operations.
                if os.name != 'nt' or getattr(error, 'winerror', None) != 1:
                    continue
                base = candidate.absolute()
                if any(path.lstat().st_file_attributes & stat.FILE_ATTRIBUTE_REPARSE_POINT
                       for path in (base, *base.parents)):
                    continue
            if base in seen or not base.is_dir():
                continue
            seen.add(base)
            if os.name == 'nt':
                if not is_ram(Path(base.anchor), timeout=2, allow_proxy=False):
                    continue
                source = Path(os.environ.get('SystemRoot', 'C:/Windows')) / 'System32/cmd.exe'
                args = ['/d', '/c', 'exit', '0']
            else:
                if not _linux_memory_mount(base, base.stat().st_dev):
                    continue
                if os.statvfs(base).f_flag & (os.ST_NOEXEC | os.ST_RDONLY):
                    continue
                source, args = Path('/bin/true'), []
            root = base / ('dco-ram-' + namespace) if separate else base
            root.mkdir(mode=0o700, exist_ok=True)
            if separate and root.resolve(strict=True) != root:
                continue
            scratch = ExecutableScratch(root)
            if scratch.identity[0] != base.stat().st_dev or not scratch.space()[0]:
                continue
            with tempfile.TemporaryDirectory(prefix='dco-ram-probe-', dir=root) as probe:
                from .process import low_integrity_directory
                low_integrity_directory(probe)
                executable = Path(probe) / source.name
                shutil.copy2(source, executable)
                subprocess.run([str(executable), *args], cwd=probe, check=True, timeout=2,
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                               creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
            return scratch
        except (OSError, RuntimeError, subprocess.SubprocessError):
            continue
    return None


def shared_volume():
    for root in volumes():
        try:
            info = json.loads((root / MARKER).read_text(encoding='utf-8-sig'))
            if info.get('kind') == 'ramdisk' and info.get('shared') is True and is_ram(root):
                return root
        except (OSError, ValueError):
            continue
    return None


def _mark_shared(root, size):
    path=root/MARKER
    expected=dict(version=1,kind='ramdisk',shared=True,capacity_bytes=size)
    try:
        current=json.loads(path.read_text(encoding='utf-8-sig'))
    except (FileNotFoundError,ValueError):
        pass
    else:
        if all(current.get(key)==value for key,value in expected.items()):
            return  # Readers need neither write access nor repeated metadata I/O.
    path.write_text(json.dumps(dict(expected,purpose='Shared simulation scratch for all local projects'),indent=2),encoding='utf-8')


def ensure_shared(size_gib=None, *, create=False, reuse_larger=False):
    """Reuse/extend the marked volume; never create a second disk after a failed resize."""
    settings=json.loads((ROOT/'.local/worker.json').read_text(encoding='utf-8-sig')) if (ROOT/'.local/worker.json').is_file() else {}
    # Capacity belongs to this host's operator, including for older callers passing 8.
    size_gib=settings.get('ramdisk_gib',8 if size_gib is None else size_gib)
    if type(size_gib) is not int or size_gib not in (4,8,16):
        raise ValueError('Shared RAM disk capacity must be 4, 8 or 16 GiB')
    size=size_gib*1024**3
    if os.name != 'nt':
        if os.environ.get('REMOTE_COMPUTE_WORKER')=='1':
            return None  # Linux jobs follow their configured ordinary-disk TMPDIR.
        root=Path(settings.get('ramdisk_root',ROOT/'.local/ramdisk')).expanduser().absolute()
        if not os.path.ismount(root):
            if not create:return None
            root.mkdir(parents=True,exist_ok=True)
            _run(['mount','-t','tmpfs','-o',f'size={size_gib}G,mode=1777','tmpfs',root])
        entries=[line.split() for line in Path('/proc/mounts').read_text().splitlines()]
        if not any(row[1]==root.as_posix() and row[2]=='tmpfs' for row in entries):
            raise OSError('Configured shared RAM root must be a dedicated tmpfs mount')
        if abs(shutil.disk_usage(root).total-size)>32*1024**2:
            if not create:raise OSError('Shared tmpfs size differs from configured capacity; run ramdisk during maintenance')
            _run(['mount','-o',f'remount,size={size_gib}G',root])
        _mark_shared(root,size)
        return root
    with creation_lock():
        root = shared_volume()
        executable = driver()
        if root:
            if shutil.disk_usage(root).total > size + 32 * 1024**2:
                if reuse_larger:
                    return root
                raise OSError(f'Shared RAM disk exceeds {size_gib} GiB; preserve its files and resize it during maintenance')
            if shutil.disk_usage(root).total < size - 32 * 1024**2:
                if not executable:
                    raise OSError(f'Install ImDisk to extend the existing shared RAM disk to {size_gib} GiB')
                details = _run([executable, '-l', '-m', str(root)[:2]])
                current = int(re.search(r'Size:\s*(\d+) bytes', details)[1])
                _run([executable, '-e', '-m', str(root)[:2], '-s', str(size - current)])
            if shutil.disk_usage(root).total < size - 32 * 1024**2:
                raise OSError(f'Existing shared RAM disk still needs extension to {size_gib} GiB')
        else:
            if not executable:
                return None
            occupied = {str(p)[0].upper() for p in volumes()}
            letter = next(x for x in 'ZYXWVUTSRQPONMLKJIHGFED' if x not in occupied)
            root = Path(letter + ':/')
            _run([executable, '-a', '-t', 'vm', '-s', f'{size_gib}G', '-m', letter + ':',
                  '-p', '/fs:ntfs /q /y /v:SHARED_RAM'])
        _mark_shared(root,size)
        return root


def scratch(project):
    volume = ensure_shared(reuse_larger=True)
    root = (volume / project) if volume else ROOT / '.local/tmp' / project
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
