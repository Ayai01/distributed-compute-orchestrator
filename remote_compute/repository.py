"""Update Git or ZIP deployments; device configuration and runtimes stay local."""
import io
from pathlib import Path, PurePosixPath
import os
import shutil
import subprocess
import tempfile
from urllib.request import urlopen
import zipfile

ROOT = Path(__file__).resolve().parent.parent
URL = 'https://github.com/Ayai01/distributed-compute-orchestrator.git'
ARCHIVE_URL = 'https://codeload.github.com/Ayai01/distributed-compute-orchestrator/zip/refs/heads/main'


def _archive_update(check=False):
    # ZIP installs have no Git history. Overlay only published source files;
    # private device configuration and ignored execution directories stay local.
    with urlopen(ARCHIVE_URL, timeout=60) as response:
        contents = response.read()
    files = []
    root = ROOT.resolve()
    private = {'.git', '.local', '.tools', '.projects', '.runtime', 'worker.local.json', 'nodes.local.json'}
    with zipfile.ZipFile(io.BytesIO(contents)) as archive:
        revision = archive.comment.decode('ascii', 'replace').strip()
        prefix = archive.infolist()[0].filename.split('/', 1)[0]
        for entry in archive.infolist():
            parts = PurePosixPath(entry.filename).parts
            if not parts or parts[0] != prefix or '..' in parts:
                raise ValueError('Unexpected source archive path: ' + entry.filename)
            if entry.is_dir() or len(parts) < 2 or parts[1] in private:
                continue
            path = root.joinpath(*parts[1:])
            path.resolve().relative_to(root)
            files.append((path, archive.read(entry)))
    if not any(path == root / 'main.py' for path, _ in files):
        raise ValueError('Source archive does not contain main.py')
    changed = [(path, data) for path, data in files if not path.is_file() or path.read_bytes() != data]
    if not check:
        for path, data in changed:
            path.parent.mkdir(parents=True, exist_ok=True)
            descriptor, name = tempfile.mkstemp(prefix=path.name + '.', suffix='.tmp', dir=str(path.parent))
            pending = Path(name)
            try:
                with os.fdopen(descriptor, 'wb') as output:
                    output.write(data)
                pending.replace(path)
            finally:
                if pending.exists():pending.unlink()
    return dict(repository=URL, deployment='archive', available=revision,
                updated=bool(changed) and not check, changed_files=len(changed),
                restart_required=bool(changed) and not check)


def git(*args):
    options = ['-c', 'http.sslBackend=openssl'] if os.name == 'nt' else []
    executable = shutil.which('git')
    portable = ROOT / '.tools' / 'git' / 'cmd' / 'git.exe'
    if not executable and portable.is_file():
        executable = str(portable)
    run = subprocess.run([executable or 'git', '-c', 'safe.directory=' + ROOT.as_posix(), *options, *args],
                         cwd=ROOT, capture_output=True, text=True, encoding='utf-8', errors='replace', timeout=60)
    if run.returncode:
        raise RuntimeError(run.stderr.strip() or run.stdout.strip())
    return run.stdout.strip()


def update(*, check=False):
    if not (ROOT / '.git').exists():
        return _archive_update(check)
    before = git('rev-parse', 'HEAD')
    remote = git('remote', 'get-url', 'origin')
    if remote.rstrip('/') != URL:
        raise ValueError('origin must point to ' + URL)
    git('fetch', 'origin', 'main')
    target = git('rev-parse', 'origin/main')
    if not check and before != target:
        if git('status', '--porcelain', '--untracked-files=no'):
            raise ValueError('Commit local source changes before updating')
        if git('merge-base', before, target) != before:
            raise ValueError('Local commits are ahead of or diverge from origin/main; '
                             'push them or reconcile the branch before updating')
        git('merge', '--ff-only', 'origin/main')
    current = git('rev-parse', 'HEAD')
    return dict(repository=URL, current=current, available=target,
                updated=current != before, restart_required=current != before)
