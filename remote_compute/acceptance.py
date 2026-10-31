"""Serialize one request ID while independent durable acceptances overlap."""
from contextlib import contextmanager
import threading


class SubmissionLocks:
    def __init__(self, legacy=None, legacy_ids=()):
        self.lock = threading.Lock()
        self.entries = {}
        self.legacy = legacy if legacy is not None else threading.RLock()
        self.legacy_ids = set(legacy_ids)

    @contextmanager
    def hold(self, identity):
        # A rolling control update can still have old submit frames waiting on
        # the previous lock. Their original IDs keep that same lock until drain.
        if identity in self.legacy_ids:
            with self.legacy:
                yield
            return
        with self.lock:
            entry = self.entries.setdefault(identity, [threading.RLock(), 0])
            entry[1] += 1
        try:
            with entry[0]:
                yield
        finally:
            with self.lock:
                entry[1] -= 1
                if not entry[1]:
                    self.entries.pop(identity)

    def __enter__(self):
        return self.legacy.__enter__()

    def __exit__(self, *error):
        return self.legacy.__exit__(*error)


class SubmissionAssets:
    def __init__(self, legacy=()):
        self.lock = threading.Lock()
        self.owners = {}
        self.legacy = set(legacy)

    def update(self, assets):
        with self.lock:
            self.owners.setdefault(threading.get_ident(), set()).update(assets)

    def clear(self):
        # Each acceptance's finally releases only that sender's source hold.
        with self.lock:
            self.owners.pop(threading.get_ident(), None)

    def __iter__(self):
        with self.lock:
            held = set(self.legacy)
            for assets in self.owners.values():
                held.update(assets)
        return iter(held)

    def __len__(self):
        return len(set(iter(self)))
