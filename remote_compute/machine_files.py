"""Self-contained file operations shipped with a machine job; Python stdlib only."""

import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import tempfile
import zipfile


def replace_file(source, target):
    target.parent.mkdir(parents=True, exist_ok=True)
    mode = target.stat().st_mode if target.exists() else source.stat().st_mode
    with tempfile.NamedTemporaryFile(dir=target.parent, prefix="." + target.name + ".", delete=False) as stream:
        pending = Path(stream.name)
    try:
        shutil.copyfile(source, pending)
        pending.chmod(mode)
        os.replace(pending, target)
    finally:
        pending.unlink(missing_ok=True)


def selected(root, patterns):
    files = {}
    for pattern in patterns:
        relative = PurePosixPath(pattern.replace("\\", "/"))
        if relative.is_absolute() or ".." in relative.parts or ":" in pattern:
            raise ValueError("File pattern must stay inside the selected directory")
        for path in root.glob(pattern):
            if path.is_file():
                if path.is_symlink() or not path.resolve().is_relative_to(root):
                    raise ValueError("Select regular files: " + str(path))
                files[path.relative_to(root).as_posix()] = path
    return sorted(files.items())


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("list", "read", "write", "upload", "download"))
    parser.add_argument("path")
    parser.add_argument("--work", required=True)
    parser.add_argument("--patterns", required=True)
    args = parser.parse_args()
    # Make relative paths absolute before resolving (also works on Windows RAM disks).
    path, work = Path(args.path).expanduser().absolute().resolve(), Path(args.work)
    output = work / "result"
    output.mkdir()
    manifest = {"path": str(path)}
    if args.action == "list":
        manifest["entries"] = [{"name": p.name, "kind": "directory" if p.is_dir() else "file",
                                "size": p.stat().st_size, "modified": p.stat().st_mtime}
                               for p in sorted(path.iterdir(), key=lambda p: (not p.is_dir(), p.name.casefold()))]
    elif args.action == "read":
        shutil.copyfile(path, output / "data")
    elif args.action == "write":
        replace_file(work / "data", path)
        with path.open("rb") as stream:
            manifest.update(size=path.stat().st_size, sha256=hashlib.file_digest(stream, "sha256").hexdigest())
    elif args.action == "upload":
        path.mkdir(parents=True, exist_ok=True)
        names = []
        for name, source in selected(work / "upload", ["**/*"]):
            replace_file(source, path / name)
            names.append(name)
        manifest["files"] = names
    elif path.is_file():
        shutil.copyfile(path, output / "data")
        manifest.update(kind="file", name=path.name, size=path.stat().st_size)
    else:
        if not path.is_dir():
            raise FileNotFoundError(path)
        entries = selected(path, json.loads(args.patterns))
        with zipfile.ZipFile(output / "transfer.zip", "w", zipfile.ZIP_DEFLATED, compresslevel=1) as archive:
            for name, source in entries:
                archive.write(source, name)
        manifest.update(kind="directory", files=[name for name, _ in entries])
    (output / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False), encoding="utf-8")


if __name__ == "__main__":
    main()
