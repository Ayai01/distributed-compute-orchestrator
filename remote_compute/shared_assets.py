"""Keep immutable asset ZIPs on an opt-in volume shared by workers."""

import hashlib
import os
from pathlib import Path
import re
import stat
import tempfile
import time

from .cleanup import ASSET_CACHE_BYTES, ASSET_CACHE_IDLE_SECONDS, ASSET_SWEEP_SECONDS, evict_unused


class SharedAssets:
    def __init__(self, root):
        self.root = Path(root).resolve()
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        if os.name != 'nt' and self.root.stat().st_mode & 0o077:
            raise ValueError('Shared asset cache must be private to the worker user')
        self.identity = hashlib.sha256(os.fsencode(self.root)).hexdigest()

    def read(self, identity):
        path = self.root / (identity + '.zip')
        try:
            with path.open('rb') as stream:
                payload = stream.read()
                if hashlib.sha256(payload).hexdigest() != identity:
                    return None
                try:
                    os.utime(stream.fileno() if os.utime in os.supports_fd else path, None)
                except OSError:
                    pass  # Updating cache age cannot prevent reading a valid archive.
                return payload
        except OSError:
            return None

    def publish(self, identity, payload):
        """Publish a verified ZIP without exposing an incomplete file."""
        path = self.root / (identity + '.zip')
        descriptor, name = tempfile.mkstemp(dir=self.root, prefix='.upload-')
        pending = Path(name)
        try:
            with os.fdopen(descriptor, 'wb') as stream:
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
            # Concurrent publishers verified this same digest before writing.
            pending.replace(path)
        finally:
            pending.unlink(missing_ok=True)

    def prune(self):
        """One worker sweeps the redundant archive pool for this mount."""
        from .worker import Conflict, lock_root

        stamp = self.root / '.last-prune'
        now = time.time()

        def due():
            try:
                return now - stamp.stat().st_mtime >= ASSET_SWEEP_SECONDS
            except FileNotFoundError:
                return True

        if not due():
            return []
        try:
            lock = lock_root(self.root, '.asset-prune')
        except Conflict:
            return []
        with lock:
            if not due():
                return []
            entries, original = {}, {}
            for path in self.root.iterdir():
                if not re.fullmatch(r'[a-f0-9]{64}\.zip|\.upload-[a-z0-9_]+', path.name):
                    continue
                try:
                    info = path.lstat()
                except FileNotFoundError:
                    continue
                if stat.S_ISREG(info.st_mode) and not getattr(info, 'st_file_attributes', 0) & 1024:
                    if path.suffix == '.zip' or now - info.st_mtime > ASSET_CACHE_IDLE_SECONDS:
                        entries[path] = (info.st_size, info.st_mtime)
                        original[path] = (info.st_ino, info.st_mtime_ns)

            def discard(path):
                try:
                    info = path.lstat()
                    if (info.st_ino, info.st_mtime_ns) != original[path]:
                        return False
                    path.unlink()
                except FileNotFoundError:
                    pass
                return True

            errors = evict_unused(entries, set(), ASSET_CACHE_BYTES, ASSET_CACHE_IDLE_SECONDS,
                                  discard, now=now)
            stamp.touch()
            return errors
