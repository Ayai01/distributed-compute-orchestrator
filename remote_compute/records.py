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
    def __init__(self, root, cache_size=64, *, index_path=None):
        self.root = Path(root)
        self.active = {}
        self.history = {}
        self.recent = OrderedDict()
        self.live = weakref.WeakValueDictionary()
        self.cache_size = cache_size
        self.assets = Counter()
        self.workers = Counter()
        # Terminal receipts remain on disk.  The append-only index lets a
        # restart rebuild the cheap identity/asset view without opening every
        # result.json in a long campaign.  A missing index keeps the legacy
        # scan path and is rebuilt after that scan.
        self.index_path = Path(index_path) if index_path is not None else self.root.parent / 'history.index.jsonl'
        self.index_loaded = False
        self.pending = []
        self.deleted = set()

    def _append_index(self, record):
        if not self.index_loaded:
            return
        self.index_path.parent.mkdir(parents=True, exist_ok=True)
        with self.index_path.open('a', encoding='utf-8', newline='\n') as stream:
            stream.write(json.dumps(record, ensure_ascii=False, separators=(',', ':')) + '\n')

    def load_index(self):
        """Load the durable identity index, returning whether it was usable."""
        if not self.index_path.is_file():
            return False
        entries = {}
        order = []
        self.deleted = set()
        try:
            with self.index_path.open('r', encoding='utf-8') as stream:
                for line in stream:
                    try:
                        row = json.loads(line)
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        # A torn final append is harmless; the result file is
                        # still the source of truth for direct ID recovery.
                        continue
                    identity = row.get('id')
                    if not isinstance(identity, str) or not identity:
                        continue
                    if row.get('op') == 'delete':
                        entries.pop(identity, None)
                        self.deleted.add(identity)
                        continue
                    if identity not in entries:
                        order.append(identity)
                    entries[identity] = row
        except OSError:
            return False
        self.history.clear()
        self.assets.clear()
        self.workers.clear()
        self.pending = []
        for identity in order:
            row = entries.get(identity)
            if row is None:
                continue
            if row.get('terminal', True):
                assets = tuple(x for x in row.get('assets', ()) if isinstance(x, str))
                worker = row.get('worker') or identity
                self.history[identity] = (assets, worker)
                self.assets.update(assets)
                self.workers.update((worker,))
            else:
                self.pending.append(identity)
        self.index_loaded = True
        return True

    def write_index(self):
        """Atomically materialize the current compact index after migration."""
        self.index_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.index_path.with_suffix(self.index_path.suffix + '.tmp')
        with temporary.open('w', encoding='utf-8', newline='\n') as stream:
            for identity, (assets, worker) in self.history.items():
                stream.write(json.dumps({'id': identity, 'terminal': True,
                                         'assets': list(assets), 'worker': worker},
                                        ensure_ascii=False, separators=(',', ':')) + '\n')
            for identity, job in self.active.items():
                result = job.get('result', {})
                stream.write(json.dumps({'id': identity, 'terminal': False,
                                         'assets': list(set(result.get('assets', {}).values())),
                                         'worker': job.get('worker_id', result.get('worker_id', identity))},
                                        ensure_ascii=False, separators=(',', ':')) + '\n')
        temporary.replace(self.index_path)
        self.index_loaded = True

    def __getitem__(self, identity):
        if identity in self.active:
            return self.active[identity]
        if identity in self.deleted:
            raise KeyError(identity)
        job = self.live.get(identity)
        if job is None:
            if identity not in self.history:
                directory = self.root / identity
                result_path = directory / 'result.json'
                if not result_path.is_file():
                    raise KeyError(identity)
                result = json.loads(result_path.read_text(encoding='utf-8'))
                if result.get('status') not in TERMINAL:
                    raise KeyError(identity)
                job = Job(result=result, directory=directory, cancel=threading.Event(), node=None)
                assets = tuple(set(result.get('assets', {}).values()))
                worker = result.get('worker_id', identity)
                self.history[identity] = (assets, worker)
                self.assets.update(assets)
                self.workers.update((worker,))
                self._append_index({'id': identity, 'terminal': True,
                                    'assets': list(assets), 'worker': worker})
            else:
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
        self.deleted.discard(identity)
        self.active[identity] = job
        result = job.get('result', {})
        self._append_index({'id': identity, 'terminal': False,
                            'assets': list(set(result.get('assets', {}).values())),
                            'worker': job.get('worker_id', result.get('worker_id', identity))})

    def __delitem__(self, identity):
        if identity in self.active:
            del self.active[identity]
        else:
            assets, worker = self.history.pop(identity)
            self._drop(self.assets, assets)
            self._drop(self.workers, (worker,))
        self.recent.pop(identity, None)
        self.live.pop(identity, None)
        self.deleted.add(identity)
        self._append_index({'id': identity, 'op': 'delete'})

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
        self.deleted.discard(identity)
        self.assets.update(assets)
        self.workers.update((worker,))
        self.active.pop(identity, None)
        self._append_index({'id': identity, 'terminal': True,
                            'assets': list(assets), 'worker': worker})
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
