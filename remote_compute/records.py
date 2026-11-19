"""Keep live tasks in memory and retained receipts on their original disk paths."""
from collections import Counter, OrderedDict
from collections.abc import MutableMapping
from copy import deepcopy
from itertools import chain, islice
import json
import os
from pathlib import Path
import threading
import weakref

from .bundle import TERMINAL
from .receipts import read_receipt


class Job(dict):
    """Weak references preserve one object during concurrent result collection."""


class JobRecords(MutableMapping):
    def __init__(self, root, cache_size=64, *, index_path=None, receipt_reader=read_receipt):
        self.root = Path(root)
        self.receipt_reader=receipt_reader
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
        self.pending_assets = {}
        self.deleted = set()
        self.loading = weakref.WeakValueDictionary()
        self.loading_lock = threading.Lock()

    def _append_index(self, record):
        if not self.index_loaded:
            return
        self.index_path.parent.mkdir(parents=True, exist_ok=True)
        with self.index_path.open('a+b') as stream:
            stream.seek(0, os.SEEK_END)
            if stream.tell():
                stream.seek(-1, os.SEEK_END)
                if stream.read(1) != b'\n':
                    stream.write(b'\n')
            stream.write((json.dumps(record, ensure_ascii=False, separators=(',', ':')) + '\n').encode('utf-8'))
            # An acknowledged admission or deletion must survive a restart.
            # A terminal update can be recovered from its durable live entry.
            if not record.get('terminal', True) or record.get('op') == 'delete':
                stream.flush()
                os.fsync(stream.fileno())

    def load_index(self):
        """Load the durable identity index, returning whether it was usable."""
        if not self.index_path.is_file():
            return False
        entries = {};asset_names={}
        self.deleted = set()
        try:
            with self.index_path.open('rb') as stream:
                for line in stream:
                    try:
                        row = json.loads(line)
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        # A torn final append is harmless; the result file is
                        # still the source of truth for direct ID recovery.
                        continue
                    if not isinstance(row, dict):
                        continue
                    identity = row.get('id')
                    if not isinstance(identity, str) or not identity:
                        continue
                    if row.get('op') == 'delete':
                        entries.pop(identity, None)
                        self.deleted.add(identity)
                        continue
                    self.deleted.discard(identity)
                    # Keep compact identity metadata rather than every parsed
                    # JSON dictionary/list while rebuilding a large history.
                    assets=tuple(asset_names.setdefault(x,x)for x in row.get('assets',())if isinstance(x,str))
                    worker=row.get('worker')or identity
                    entries[identity]=(row.get('terminal',True),assets,identity if worker==identity else worker)
        except OSError:
            return False
        self.history.clear()
        self.assets.clear()
        self.workers.clear()
        self.pending = []
        self.pending_assets = {}
        for identity, (terminal,assets,worker) in entries.items():
            if terminal:
                self.history[identity] = (assets, worker)
                self.assets.update(assets)
                self.workers.update((worker,))
            else:
                self.pending.append(identity)
                self.pending_assets[identity] = assets
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
            stream.flush()
            os.fsync(stream.fileno())
        temporary.replace(self.index_path)
        self.index_loaded = True

    def compact_index(self):
        """Compact superseded identities before the service starts its threads."""
        if len(self.deleted) <= max(1024, len(self)):
            return False
        if any(identity not in self for identity in self.pending):
            return False  # Startup still has original live entries to recover.
        self.write_index()
        self.deleted.clear()
        return True

    def __getitem__(self, identity):
        if identity in self.active:
            return self.active[identity]
        if identity in self.deleted:
            raise KeyError(identity)
        job = self.live.get(identity)
        if job is None:
            job = self._read_job(identity, self.history.get(identity))
            return self._remember(identity, job)
        self._cache(identity, job)
        return job

    def _read_job(self, identity, known):
        directory = self.root / identity
        path = directory / 'result.json'
        if known is None and not path.is_file():
            raise KeyError(identity)
        result = self.receipt_reader(path, assets=known[0] if known is not None else ())
        if known is None and result.get('status') not in TERMINAL:
            raise KeyError(identity)
        return Job(result=result, read_result=deepcopy(result), directory=directory,
                   cancel=threading.Event(), node=None, artifact_lock=self.record_lock(identity))

    def record_lock(self, identity):
        with self.loading_lock:
            return self.loading.setdefault(identity, threading.RLock())

    def read_result(self, identity):
        """Read indexed terminal receipts without taking the scheduler lock."""
        with self.record_lock(identity):
            if identity in self.deleted or identity not in self.history:
                raise KeyError(identity)
            return self.receipt_reader(self.root / identity / 'result.json',
                                assets=self.history[identity][0])

    def _remember(self, identity, job):
        if identity not in self.history:
            result = job['result']
            assets = tuple(set(result.get('assets', {}).values()))
            worker = result.get('worker_id', identity)
            self.history[identity] = (assets, worker)
            self.assets.update(assets)
            self.workers.update((worker,))
            self._append_index({'id': identity, 'terminal': True,
                                'assets': list(assets), 'worker': worker})
        self._cache(identity, job)
        return job

    def load(self, identity, condition):
        """Read cold receipts under one ID lock, outside live control locks."""
        with condition:
            job = self.cached(identity)
            if job is not None:
                return job
            if identity in self.deleted:
                raise KeyError(identity)
            lock = self.record_lock(identity)
        with lock:
            with condition:
                job = self.cached(identity)
                if job is not None:
                    return job
                if identity in self.deleted:
                    raise KeyError(identity)
                known = self.history.get(identity)
            job = self._read_job(identity, known)
            with condition:
                if identity in self.deleted:
                    raise KeyError(identity)
                current = self.cached(identity)
                return current if current is not None else self._remember(identity, job)

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
        job.setdefault('artifact_lock', self.record_lock(identity))
        self.active[identity] = job
        result = job.get('result', {})
        self._append_index({'id': identity, 'terminal': False,
                            'assets': list(set(result.get('assets', {}).values())),
                            'worker': job.get('worker_id', result.get('worker_id', identity))})

    def __delitem__(self, identity):
        self.forget(identity)

    def forget(self, identity, *, journal=True):
        if identity in self.active:
            del self.active[identity]
        else:
            assets, worker = self.history.pop(identity)
            self._drop(self.assets, assets)
            self._drop(self.workers, (worker,))
        self.recent.pop(identity, None)
        self.live.pop(identity, None)
        self.deleted.add(identity)
        if journal:
            self.record_retired(identity)

    def record_retired(self, identity):
        self._append_index({'id': identity, 'op': 'delete'})

    def __contains__(self, identity):
        return identity in self.active or identity in self.history

    def __iter__(self):
        yield from self.active
        yield from self.history

    def __len__(self):
        return len(self.active) + len(self.history)

    def page(self, *, limit=256, offset=0, prefix=None, include_active=True):
        """Return a bounded recent-first identity page without loading results.

        The complete receipt history stays on disk and remains addressable by
        original ID.  Operator views must not materialize every receipt just
        to paint a task table, especially after a long research campaign.
        Callers hold the worker condition while taking this identity snapshot.
        """
        limit = max(1, min(int(limit), 512))
        offset = max(0, int(offset))
        if prefix is not None:
            identities = [identity for identity in chain(self.active if include_active else (), reversed(self.history))
                          if identity.startswith(prefix)]
            return identities[offset:offset + limit], len(identities)
        active = tuple(self.active) if include_active else ()
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


def load_job(records, condition, identity):
    if isinstance(records, JobRecords):
        return records.load(identity, condition)
    with condition:
        return records[identity]


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
