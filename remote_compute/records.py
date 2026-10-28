"""Keep live tasks in memory and retained receipts on their original disk paths."""
from collections import Counter, OrderedDict
from collections.abc import MutableMapping
from copy import deepcopy
from itertools import islice
import json
from pathlib import Path
import threading
import weakref

from .bundle import TERMINAL


class Job(dict):
    """Weak references preserve one object during concurrent result collection."""


class JobRecords(MutableMapping):
    def __init__(self, root, cache_size=64):
        self.root = Path(root)
        self.active = {}
        self.history = {}
        self.recent = OrderedDict()
        self.live = weakref.WeakValueDictionary()
        self.cache_size = cache_size
        self.assets = Counter()
        self.workers = Counter()

    def __getitem__(self, identity):
        if identity in self.active:
            return self.active[identity]
        self.history[identity]  # A deleted ID must remain a KeyError.
        job = self.live.get(identity)
        if job is None:
            directory = self.root / identity
            result = json.loads((directory / 'result.json').read_text(encoding='utf-8'))
            job = Job(result=result, directory=directory, cancel=threading.Event(), node=None)
        self._cache(identity, job)
        return job

    def _cache(self, identity, job):
        self.live[identity] = job
        self.recent[identity] = job
        self.recent.move_to_end(identity)
        while len(self.recent) > self.cache_size:
            self.recent.popitem(last=False)

    def cached(self, identity):
        """Read an already resident record without disk I/O or LRU mutation."""
        job = self.active.get(identity)
        return job if job is not None else self.live.get(identity)

    def __setitem__(self, identity, job):
        if identity in self:
            del self[identity]
        self.active[identity] = job

    def __delitem__(self, identity):
        if identity in self.active:
            del self.active[identity]
        else:
            assets, worker = self.history.pop(identity)
            self._drop(self.assets, assets)
            self._drop(self.workers, (worker,))
        self.recent.pop(identity, None)
        self.live.pop(identity, None)

    def __contains__(self, identity):
        return identity in self.active or identity in self.history

    def __iter__(self):
        yield from self.active
        yield from self.history

    def __len__(self):
        return len(self.active) + len(self.history)

    def page(self, *, limit=256, offset=0):
        """Return a bounded recent-first identity page without loading results.

        The complete receipt history stays on disk and remains addressable by
        original ID.  Operator views must not materialize every receipt just
        to paint a task table, especially after a long research campaign.
        Callers hold the worker condition while taking this identity snapshot.
        """
        limit = max(1, min(int(limit), 512))
        offset = max(0, int(offset))
        active = tuple(self.active)
        total = len(active) + len(self.history)
        ids = []
        if offset < len(active):
            take = min(limit, len(active) - offset)
            ids.extend(active[offset:offset + take])
        remaining = limit - len(ids)
        if remaining:
            history_offset = max(0, offset - len(active))
            ids.extend(islice(reversed(self.history), history_offset,
                              history_offset + remaining))
        return ids, total

    @staticmethod
    def _drop(counter, keys):
        for key in keys:
            counter[key] -= 1
            if not counter[key]:
                del counter[key]

    def archive(self, job, *, startup=False):
        result = job['result']
        identity = result['id']
        if result['status'] not in TERMINAL or identity not in self:
            return
        if self.active.get(identity) is not job and self.live.get(identity) is not job:
            return
        if 'read_result' in job:
            job['read_result'] = deepcopy(result)
        assets = tuple(set(result.get('assets', {}).values()))
        worker = job.get('worker_id', result.get('worker_id', identity))
        if identity in self.history:
            old_assets, old_worker = self.history[identity]
            self._drop(self.assets, old_assets)
            self._drop(self.workers, (old_worker,))
        self.history[identity] = (assets, worker)
        self.assets.update(assets)
        self.workers.update((worker,))
        self.active.pop(identity, None)
        if not startup:
            self._cache(identity, job)

    def knows_worker(self, identity):
        return identity in self or identity in self.workers or any(
            job.get('worker_id', job['result'].get('worker_id')) == identity
            for job in self.active.values())


def active_items(records):
    # Small isolated allocation fixtures also use ordinary dictionaries.
    return (records.active if isinstance(records, JobRecords) else records).items()


def active_values(records):
    return (records.active if isinstance(records, JobRecords) else records).values()


def archive_job(records, job, *, startup=False):
    if isinstance(records, JobRecords):
        records.archive(job, startup=startup)


def asset_references(records):
    referenced = set(records.assets) if isinstance(records, JobRecords) else set()
    for job in active_values(records):
        referenced.update(job['result'].get('assets', {}).values())
    return referenced
