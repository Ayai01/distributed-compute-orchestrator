"""Defaults for worker-created caches; explicit environment paths keep priority."""
from pathlib import Path
import os


def cache_environment(environment, root):
    environment = dict(environment)
    root = Path(root).resolve()
    root.mkdir(parents=True, exist_ok=True)
    home = Path.home()
    old_cache = Path(environment.get('XDG_CACHE_HOME', home / '.cache'))
    local = Path(environment.get('LOCALAPPDATA', home / 'AppData/Local'))

    # Reuse installed dependency/model caches in place. Newly created caches
    # and compiled numerical intermediates default to this DCO-owned directory.
    def dependency(key, name, candidates):
        if key in environment:
            return
        for candidate in candidates:
            try:
                if candidate.is_dir() and next(candidate.iterdir(), None) is not None:
                    environment[key] = str(candidate)
                    return
            except OSError:
                continue
        environment[key] = str(root / name)

    dependency('COURSIER_CACHE', 'coursier',
               [local / 'Coursier/Cache/v1', local / 'Coursier/cache/v1', old_cache / 'coursier/v1'])
    dependency('TORCH_HOME', 'torch', [old_cache / 'torch'])
    dependency('HF_HOME', 'huggingface', [old_cache / 'huggingface'])
    for key, name in (('XDG_CACHE_HOME', ''), ('NUMBA_CACHE_DIR', 'numba'),
                      ('MPLCONFIGDIR', 'matplotlib'), ('CUDA_CACHE_PATH', 'cuda'),
                      ('CCACHE_DIR', 'ccache')):
        environment.setdefault(key, str(root / name))
    environment.setdefault('HUGGINGFACE_HUB_CACHE', str(Path(environment['HF_HOME']) / 'hub'))
    environment.setdefault('REMOTE_COMPUTE_CACHE_DIR', str(root))
    return environment


def migrate_worker_state(source, destination, workspace):
    """Move an explicitly configured, stopped worker on the same filesystem."""
    from .worker import lock_root
    source = Path(source).expanduser().absolute()
    destination = Path(destination).expanduser().resolve()
    managed = Path(workspace).resolve() / '.local'
    destination.relative_to(managed)
    if source == destination or source in destination.parents or destination in source.parents:
        raise ValueError('State migration requires separate source and destination directories')
    if not source.exists() and (destination / 'worker.lock').is_file() and (destination / 'jobs').is_dir():
        return  # Rename completed before an interrupted configuration write.
    info = source.lstat()
    if source.is_symlink() or getattr(info, 'st_file_attributes', 0) & 1024:
        raise ValueError('State migration source must be the actual worker directory')
    if not (source / 'worker.lock').is_file() or not (source / 'jobs').is_dir():
        raise ValueError('State migration source is not a DCO worker state root')
    if destination.exists():
        raise ValueError('State migration destination already exists; no state merging is performed')
    # A running worker owns this lock. The supervisor reaches this function
    # only after its old service child exits; existing jobs/receipts move intact.
    with lock_root(source, 'worker'):
        pass
    destination.parent.mkdir(parents=True, exist_ok=True)
    os.rename(str(source), str(destination))
