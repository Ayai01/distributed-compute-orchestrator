"""Reclaim owned job files and bounded, reconstructible worker assets."""

import os
from pathlib import Path
import re
import stat
import time


def remove_owned(path):
    """Remove an owned entry without traversing symlinks or Windows junctions."""
    try:
        info = path.lstat()
    except FileNotFoundError:
        return
    if stat.S_ISDIR(info.st_mode):
        linked = stat.S_ISLNK(info.st_mode) or bool(getattr(info, 'st_file_attributes', 0) & 1024)
        if not linked:
            for child in path.iterdir():
                remove_owned(child)
        path.rmdir()
    else:
        path.unlink()


class AssetCache:
    """LRU metadata is collected once; ordinary health polls never scan files."""
    def __init__(self, root, limit=512 * 1024**2):
        self.root, self.limit = Path(root), limit
        self.entries = {}
        self.revision = 0
        for path in self.root.iterdir():
            if re.fullmatch(r'[a-f0-9]{64}', path.name) and path.is_dir() and path.resolve() == path:
                self._add(path)

    def _add(self, path):
        size = 0
        for folder, directories, files in os.walk(path, followlinks=False):
            directories[:] = [name for name in directories if not self._linked(Path(folder)/name)]
            size += sum((Path(folder)/name).stat().st_size for name in files
                        if not self._linked(Path(folder)/name))
        self.entries[path.name] = (size, path.stat().st_mtime)

    @staticmethod
    def _linked(path):
        info = path.lstat()
        return stat.S_ISLNK(info.st_mode) or bool(getattr(info, 'st_file_attributes', 0) & 1024)

    def touch(self, identity):
        path = self.root/identity
        if identity not in self.entries:
            self._add(path)
        size, _ = self.entries[identity]
        self.entries[identity] = (size, time.time())
        os.utime(path, None)

    def snapshot(self):
        return dict(bytes=sum(size for size, _ in self.entries.values()),
                    limit_bytes=self.limit, count=len(self.entries))

    def prune(self, protected):
        total = self.snapshot()['bytes']
        errors = []
        for identity, (size, _used) in sorted(self.entries.items(), key=lambda item: item[1][1]):
            if total <= self.limit:
                break
            if identity in protected:
                continue
            try:
                remove_owned(self.root/identity)
            except OSError as error:
                errors.append(str(error))
            else:
                self.entries.pop(identity)
                self.revision += 1
                total -= size
        return errors
