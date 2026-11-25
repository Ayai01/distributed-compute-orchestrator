"""Deterministic file bundles shared by the client and worker."""

import io
import json
import math
import os
from pathlib import Path, PurePosixPath
import shutil
import stat
import tempfile
import time
import zipfile

from .compat import is_relative_to

PROTOCOL = 1
TERMINAL = frozenset({"succeeded", "failed", "cancelled", "timed_out", "interrupted"})
MAX_BODY = 512 * 1024 * 1024
_PACK_BLOCK = 4 * 1024 * 1024


def cancellation_needs_reconnect(result):
    """A locally interrupted original still owns its remote cancellation."""
    return (result.get('status') == 'interrupted' and result.get('finished') is None
            and result.get('remote_cancel_pending'))


def native_path(value,*,force=False):
    """Use the Windows extended namespace for owned filesystem I/O."""
    # Keep a link's own identity; containment checks resolve links in inside().
    path=Path(os.path.abspath(value))
    text=str(path)
    if os.name=='nt'and (force or len(text)>=248)and not text.startswith('\\\\?\\'):
        text=('\\\\?\\UNC\\'+text[2:])if text.startswith('\\\\')else'\\\\?\\'+text
        return Path(text)
    return path


def command_step_progress(value):
    """The command runner owns this denominator independently of business progress."""
    saved = value.get('step_progress')
    if isinstance(saved, dict):
        done, total = saved.get('completed'), saved.get('total')
        if type(total) is int and type(done) is int and 0 <= done <= total and total > 0:
            return {'completed': done, 'total': total}
    total, steps = value.get('total_steps'), value.get('steps')
    if type(total) is not int or total <= 0 or not isinstance(steps, (list, tuple)) or not 0 < len(steps) <= total:
        return None
    done = 0
    for step in steps:
        if not isinstance(step, dict):
            return None
        started, finished, code = step.get('started'), step.get('finished'), step.get('returncode')
        for stamp in (started, finished):
            if stamp is not None and (type(stamp) not in (int,float) or not math.isfinite(stamp) or stamp < 0):
                return None
        if code is not None and type(code) is not int:
            return None
        if started is None and finished is None and code is None:
            return None
        done += finished is not None or code is not None
    return {'completed': done, 'total': total}


def progress_fields(value):
    """Return cheap progress fields already present in a task snapshot."""
    fields = {key: value[key] for key in (
        'dispatch_stage', 'dispatch_stage_started_at', 'asset_preparation_seconds','worker_status',
        'handoff_seconds', 'handoff_acknowledged_at', 'handoff_transport', 'worker_admission_seconds',
        'bundle_unpack_seconds', 'preparation_stage', 'worker_sampled_at',
        'worker_preparation_elapsed_seconds', 'execution_started', 'preparation_seconds',
        'worker_receive_stage', 'worker_unpack_elapsed_seconds'
    ) if key in value}
    progress = value.get('progress')
    from .progress import SCHEMA, normalize, diagnose
    business = isinstance(progress,dict) and progress.get('schema')==SCHEMA
    if business:
        fields['progress'] = normalize(progress)
        fields.update({key:progress[key]for key in ('phase','completed','total','unit')if key in progress})
        if 'ratio' not in fields['progress']:
            steps = command_step_progress(value)
            if steps is not None:
                fields['step_progress'] = steps
    elif isinstance(progress, (int, float)) and math.isfinite(float(progress)):
        fields['progress'] = progress
    elif isinstance(progress, dict):
        compact = {key: progress[key] for key in ('ratio', 'fraction', 'percent')
                   if isinstance(progress.get(key), (int, float))
                   and math.isfinite(float(progress[key]))}
        if compact:
            fields['progress'] = compact
    for key in ('phase', 'stage'):
        if business:continue
        if isinstance(value.get(key), str) and value[key]:
            fields[key] = value[key][:64]
    for key in ('completed', 'total', 'current', 'target', 'nfe', 'budget'):
        if business:continue
        candidate = value.get(key)
        if isinstance(candidate, (int, float)) and math.isfinite(float(candidate)):
            fields[key] = candidate
    if not any(key in fields for key in ('progress', 'completed', 'current', 'nfe')):
        steps = command_step_progress(value)
        if steps is not None:
            fields.update(steps)
            fields['step_progress'] = steps
    elif 'step_progress' in value and not business:
        steps = command_step_progress(value)
        if steps is not None:
            fields['step_progress'] = steps
    if isinstance(value.get('runtime'),dict):
        fields['runtime'] = {key:value['runtime'][key]for key in
            ('sampled_at','process_alive','returncode','cpu_seconds','last_cpu_advanced')if key in value['runtime']}
    if business or 'runtime' in fields or value.get('resource_request'):
        fields['diagnostic'] = diagnose(value)
    return fields


def progress(value):
    """Small polling view; full provenance remains in the final result."""
    result = {key: value[key] for key in ('id','status','label','priority','cpus', 'active_cpus', 'min_cpus', 'requested_cpus','memory_mb','memory_phase','submitted','started',
             'finished','elapsed','assigned_node','worker_url','center_submitted','dispatched','resource_request','resource_wait_reason') if key in value}
    result.update(progress_fields(value))
    if value.get('status') in TERMINAL:
        result.update({key: value[key] for key in ('cpu_seconds', 'cumulative_cpu_seconds', 'total_cpu_seconds') if key in value})
    if value.get('observe_only'):
        result.update({key: value[key] for key in ('observe_only', 'source', 'revision', 'project', 'phase',
                       'cpu_seconds', 'cumulative_cpu_seconds', 'cpu_sampled_at', 'measured_cpus') if key in value})
    return result


def batch_status(value, compact=False):
    if compact:
        return {key: value[key] for key in ('status', 'status_code') if key in value}
    return value if value.get('status') == 'unknown' else progress(value)


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
            metadata = source.stat() if isinstance(source, Path) else None
            mode = stat.S_IMODE(metadata.st_mode) if metadata is not None else 0o644
            info.external_attr = (stat.S_IFREG | mode) << 16
            if (metadata is not None and metadata.st_size > _PACK_BLOCK
                    and info.compress_type == zipfile.ZIP_DEFLATED):
                info.file_size = metadata.st_size
                # Use the same compressor and ZIP headers as writestr().
                if hasattr(info, 'compress_level'):
                    info.compress_level = 1
                else:
                    info._compresslevel = 1
                with source.open('rb') as stream, archive.open(info, 'w') as entry:
                    shutil.copyfileobj(stream, entry, _PACK_BLOCK)
            else:
                archive.writestr(info, source.read_bytes() if metadata is not None else source,
                                compresslevel=1)
    return buffer.getvalue()


def tree(root, patterns=("**/*",), *, cancel=None):
    root = native_path(Path(root).resolve(),force=True)
    files = {}
    for pattern in patterns:
        inside(root, pattern)
        paths = root.glob(pattern)
        for path in paths if cancel else sorted(paths):
            if cancel and cancel():raise InterruptedError('Job cleanup cancelled')
            if path.is_file():
                if path.is_symlink() or not is_relative_to(path.resolve(), root):
                    raise ValueError("Bundle requires regular files: " + str(path))
                files[path.relative_to(root).as_posix()] = path
    return files


def _archive_members(archive, root):
    entries = archive.infolist()
    if sum(item.file_size for item in entries) > 2 * 1024**3:
        raise ValueError("Expanded bundle exceeds 2 GiB; predeploy large assets with shared roots")
    for item in entries:
        target = inside(root, item.filename)
        mode = item.external_attr >> 16
        if stat.S_ISLNK(mode):
            raise ValueError("Bundle requires regular files")
        yield item, target, mode


def validate_archive(path, root):
    with zipfile.ZipFile(path) as archive:
        for _ in _archive_members(archive, native_path(root, force=True)):
            pass


def unpack(data, root):
    root = native_path(root,force=True)
    sizes = {}
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        for item, target, mode in _archive_members(archive, root):
            if item.is_dir():
                target.mkdir(parents=True, exist_ok=True)
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(archive.read(item))
            sizes[target] = item.file_size
            if os.name != "nt":
                target.chmod(stat.S_IMODE(mode) or 0o644)
    return sum(sizes.values())


def task_bundle(task, inputs=None, files=None):
    contents = {"task.json": json_bytes(task)}
    if inputs is not None:
        contents.update({"input/" + k: v for k, v in tree(inputs).items()})
    for name, value in (files or {}).items():
        if isinstance(value, str):
            value = value.encode("utf-8")
        contents["input/" + name] = value
    return pack(contents)


def atomic_json(path, value, *, private=False, publish=None):
    """Publish flushed JSON through a unique, owner-only sibling file.

    Concurrent progress/final receipt writers must never share a temporary
    filename. Flush before replacement so a crash cannot publish an empty
    receipt in place of the last complete JSON document.
    """
    path = native_path(path)
    data = json_bytes(value)
    descriptor, name = tempfile.mkstemp(prefix=path.name + '.', suffix='.tmp', dir=path.parent)
    pending = Path(name)
    try:
        with os.fdopen(descriptor, 'wb') as output:
            output.write(data)
            output.flush()
            os.fsync(output.fileno())
        for attempt in range(7):
            try:
                if publish is None:
                    pending.replace(path)
                else:
                    publish(pending,path)
                break
            except PermissionError:
                # Windows readers/antivirus may briefly hold the old file.
                # Retry only publication, never truncate the destination.
                if attempt == 6:
                    raise
                time.sleep(.01 * 2 ** attempt)
    finally:
        if pending.exists():
            pending.unlink()
