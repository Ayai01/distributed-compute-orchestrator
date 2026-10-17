"""Fast-forward updates from the shared source repository; local runtimes stay local."""
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parent.parent
URL = 'https://github.com/Ayai01/distributed-compute-orchestrator.git'


def git(*args):
    run = subprocess.run(['git', '-c', 'safe.directory=' + ROOT.as_posix(), '-c', 'http.sslBackend=openssl', *args],
                         cwd=ROOT, capture_output=True, text=True, encoding='utf-8', errors='replace', timeout=60)
    if run.returncode:
        raise RuntimeError(run.stderr.strip() or run.stdout.strip())
    return run.stdout.strip()


def update(*, check=False):
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
