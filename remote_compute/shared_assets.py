"""Keep immutable asset ZIPs on an opt-in volume shared by workers."""

import hashlib
import os
from pathlib import Path
import tempfile


class SharedAssets:
    def __init__(self, root):
        self.root = Path(root).resolve()
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        if os.name != 'nt' and self.root.stat().st_mode & 0o077:
            raise ValueError('Shared asset cache must be private to the worker user')
        self.identity = hashlib.sha256(os.fsencode(self.root)).hexdigest()

    def read(self, identity):
        try:
            return (self.root / (identity + '.zip')).read_bytes()
        except FileNotFoundError:
            return None

    def publish(self, identity, payload):
        """Publish a verified ZIP without exposing an incomplete file."""
        path = self.root / (identity + '.zip')
        if path.is_file():
            return
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
