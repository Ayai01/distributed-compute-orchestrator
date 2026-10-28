"""Deterministic file bundles shared by the client and worker."""

import io
import json
import math
import os
from pathlib import Path, PurePosixPath
import stat
import tempfile
import zipfile

from .compat import is_relative_to

PROTOCOL = 1
TERMINAL = frozenset({"succeeded", "failed", "cancelled", "timed_out", "interrupted"})
MAX_BODY = 512 * 1024 * 1024


def progress_fields(value):
    """Return cheap progress fields already present in a task snapshot."""
    fields = {}
    progress = value.get('progress')
    if isinstance(progress, (int, float)) and math.isfinite(float(progress)):
        fields['progress'] = progress
    elif isinstance(progress, dict):
        compact = {key: progress[key] for key in ('ratio', 'fraction', 'percent')
                   if isinstance(progress.get(key), (int, float))
                   and math.isfinite(float(progress[key]))}
        if compact:
            fields['progress'] = compact
    for key in ('phase', 'stage'):
        if isinstance(value.get(key), str) and value[key]:
            fields[key] = value[key][:64]
    for key in ('completed', 'total', 'current', 'target', 'nfe', 'budget'):
        candidate = value.get(key)
        if isinstance(candidate, (int, float)) and math.isfinite(float(candidate)):
            fields[key] = candidate
    if not any(key in fields for key in ('progress', 'completed', 'current', 'nfe')):
        total = value.get('total_steps')
        if isinstance(total, int) and total > 0:
            steps = value.get('steps') or ()
            completed = sum(1 for step in steps
                            if isinstance(step, dict)
                            and (step.get('finished') is not None
                                 or step.get('returncode') is not None))
            fields.update(completed=min(completed, total), total=total)
    return fields


def progress(value):
    """Small polling view; full provenance remains in the final result."""
    result = {key: value[key] for key in ('id','status','label','cpus','memory_mb','submitted','started',
             'finished','elapsed','assigned_node','worker_url','center_submitted','dispatched','resource_request') if key in value}
    result.update(progress_fields(value))
    return result


def json_bytes(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, allow_nan=False).encode("utf-8")


def inside(root, name):
    name = str(name).replace("\\", "/")
    parts = PurePosixPath(name)
    if parts.is_absolute() or ".." in parts.parts or ":" in name or not parts.parts:
        raise ValueError("Expected a relative file path: " + name)
    root = Path(root).resolve()
    result = (root / name).resolve()
    if not is_relative_to(result, root):
        raise ValueError("File path leaves its directory: " + name)
    return result


def pack(files):
    """files maps relative names to bytes or Paths; stable bytes permit reuse."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED, compresslevel=1) as archive:
        for name, source in sorted(files.items()):
            inside(Path.cwd(), name)
            info = zipfile.ZipInfo(str(name).replace("\\", "/"), (1980, 1, 1, 0, 0, 0))
            info.compress_type = (zipfile.ZIP_STORED if str(name).lower().endswith(('.gz','.npz','.zip','.7z'))
                                  else zipfile.ZIP_DEFLATED)
            mode = source.stat().st_mode if isinstance(source, Path) else 0o644
            info.external_attr = (stat.S_IFREG | stat.S_IMODE(mode)) << 16
            archive.writestr(info, source.read_bytes() if isinstance(source, Path) else source,
                            compresslevel=1)
    return buffer.getvalue()


def tree(root, patterns=("**/*",)):
    root = Path(root).resolve()
    files = {}
    for pattern in patterns:
        inside(root, pattern)
        for path in sorted(root.glob(pattern)):
            if path.is_file():
                if path.is_symlink() or not is_relative_to(path.resolve(), root):
                    raise ValueError("Bundle requires regular files: " + str(path))
                files[path.relative_to(root).as_posix()] = path
    return files


def unpack(data, root):
    root = Path(root).resolve()
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        entries = archive.infolist()
        if sum(item.file_size for item in entries) > 2 * 1024**3:
            raise ValueError("Expanded bundle exceeds 2 GiB; predeploy large assets with shared roots")
        for item in entries:
            target = inside(root, item.filename)
            mode = item.external_attr >> 16
            if stat.S_ISLNK(mode):
                raise ValueError("Bundle requires regular files")
            if item.is_dir():
                target.mkdir(parents=True, exist_ok=True)
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(archive.read(item))
            if os.name != "nt":
                target.chmod(stat.S_IMODE(mode) or 0o644)


def task_bundle(task, inputs=None, files=None):
    contents = {"task.json": json_bytes(task)}
    if inputs is not None:
        contents.update({"input/" + k: v for k, v in tree(inputs).items()})
    for name, value in (files or {}).items():
        if isinstance(value, str):
            value = value.encode("utf-8")
        contents["input/" + name] = value
    return pack(contents)


def atomic_json(path, value, *, private=False):
    """Publish JSON atomically; private configs use an owner-only temporary file."""
    path = Path(path)
    data = json_bytes(value)
    if private:
        descriptor, name = tempfile.mkstemp(prefix=path.name + '.', suffix='.tmp', dir=path.parent)
        pending = Path(name)
        try:
            with os.fdopen(descriptor, 'wb') as output:
                output.write(data)
            pending.replace(path)
        finally:
            if pending.exists():
                pending.unlink()
        return
    pending = path.with_suffix(".tmp")
    pending.write_bytes(data)
    pending.replace(path)
