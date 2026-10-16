"""Deterministic file bundles shared by the client and worker."""

import io
import json
import os
from pathlib import Path, PurePosixPath
import stat
import zipfile

PROTOCOL = 1
TERMINAL = frozenset({"succeeded", "failed", "cancelled", "timed_out", "interrupted"})
MAX_BODY = 512 * 1024 * 1024


def json_bytes(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, allow_nan=False).encode("utf-8")


def inside(root, name):
    name = str(name).replace("\\", "/")
    parts = PurePosixPath(name)
    if parts.is_absolute() or ".." in parts.parts or ":" in name or not parts.parts:
        raise ValueError("Expected a relative file path: " + name)
    root = Path(root).resolve()
    result = (root / name).resolve()
    if not result.is_relative_to(root):
        raise ValueError("File path leaves its directory: " + name)
    return result


def pack(files):
    """files maps relative names to bytes or Paths; stable bytes permit reuse."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED, compresslevel=1) as archive:
        for name, source in sorted(files.items()):
            inside(Path.cwd(), name)
            info = zipfile.ZipInfo(str(name).replace("\\", "/"), (1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            mode = source.stat().st_mode if isinstance(source, Path) else 0o644
            info.external_attr = (stat.S_IFREG | stat.S_IMODE(mode)) << 16
            archive.writestr(info, source.read_bytes() if isinstance(source, Path) else source)
    return buffer.getvalue()


def tree(root, patterns=("**/*",)):
    root = Path(root).resolve()
    files = {}
    for pattern in patterns:
        inside(root, pattern)
        for path in sorted(root.glob(pattern)):
            if path.is_file():
                if path.is_symlink() or not path.resolve().is_relative_to(root):
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


def atomic_json(path, value):
    path = Path(path)
    pending = path.with_suffix(".tmp")
    pending.write_bytes(json_bytes(value))
    pending.replace(path)
