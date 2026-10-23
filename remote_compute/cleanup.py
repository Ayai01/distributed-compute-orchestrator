"""Reclaim owned job files and bounded, reconstructible worker assets."""

import os
from pathlib import Path
import re
import stat
import threading
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
    def __init__(self, root, limit=512 * 1024**2, *, archives=False):
        self.root, self.limit = Path(root), limit
        self.archives = archives
        # File operations share this lock; health reads only the short metadata lock.
        self.lock = threading.RLock()
        self.metadata_lock = threading.Lock()
        self.entries = {}
        self.revision = 0
        for path in self.root.iterdir():
            identity = path.stem if archives else path.name
            valid = path.suffix == '.zip' and path.is_file() if archives else path.is_dir()
            if re.fullmatch(r'[a-f0-9]{64}', identity) and valid and not self._linked(path):
                self._add(path)

    def _add(self, path):
        size = path.stat().st_size if self.archives else 0
        if not self.archives:
            for folder, directories, files in os.walk(path, followlinks=False):
                directories[:] = [name for name in directories if not self._linked(Path(folder)/name)]
                size += sum((Path(folder)/name).stat().st_size for name in files
                            if not self._linked(Path(folder)/name))
        used = path.stat().st_mtime
        with self.metadata_lock:
            self.entries[path.stem if self.archives else path.name] = (size, used)

    def _path(self, identity):
        return self.root / (identity + '.zip' if self.archives else identity)

    @staticmethod
    def _linked(path):
        info = path.lstat()
        return stat.S_ISLNK(info.st_mode) or bool(getattr(info, 'st_file_attributes', 0) & 1024)

    def touch(self, identity):
        with self.lock:
            path = self._path(identity)
            if identity not in self.entries:
                self._add(path)
            os.utime(path, None)
            with self.metadata_lock:
                size, _ = self.entries[identity]
                self.entries[identity] = (size, time.time())

    def snapshot(self):
        with self.metadata_lock:
            return dict(bytes=sum(size for size, _ in self.entries.values()),
                        limit_bytes=self.limit, count=len(self.entries))

    def prune(self, protected):
        with self.lock:
            return self._prune(protected)

    def _prune(self, protected):
        total = self.snapshot()['bytes']
        errors = []
        for identity, (size, _used) in sorted(self.entries.items(), key=lambda item: item[1][1]):
            if total <= self.limit:
                break
            if identity in protected:
                continue
            try:
                remove_owned(self._path(identity))
            except OSError as error:
                errors.append(str(error))
            else:
                with self.metadata_lock:
                    self.entries.pop(identity)
                    self.revision += 1
                total -= size
        return errors
