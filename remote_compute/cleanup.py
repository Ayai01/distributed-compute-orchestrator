"""Reclaim owned job files and bounded, reconstructible worker assets."""

from contextlib import ExitStack
import os
import json
import heapq
from collections import deque
from pathlib import Path
import re
import stat
import threading
import time
import uuid
from .bundle import TERMINAL, atomic_json, native_path
from .acceptance import SubmissionLocks


ASSET_CACHE_BYTES = 64 * 1024**3
ASSET_CACHE_IDLE_SECONDS = 7 * 24 * 3600
ASSET_SWEEP_SECONDS = 600


class AssetMaintenance:
    """Run owned cache cleanup outside scheduling and HTTP request threads."""

    def __init__(self, sweep, report_error, *, interval=ASSET_SWEEP_SECONDS):
        self.stop = threading.Event()
        self.pending = threading.Event()

        def run():
            while not self.stop.is_set():
                self.pending.wait(interval)
                self.pending.clear()
                if self.stop.is_set():
                    break
                try:
                    sweep()
                except (OSError, ValueError) as error:
                    report_error(str(error))

        self.thread = threading.Thread(target=run, name='dco-asset-maintenance', daemon=True)
        self.thread.start()

    def request(self):
        self.pending.set()

    def close(self):
        self.stop.set()
        self.pending.set()
        self.thread.join()


def evict_unused(entries, protected, limit, max_idle, discard, *, now):
    """Remove expired entries first, then the least recently hit until within budget."""
    total = sum(size for size, _ in entries.values())
    errors = []
    for key in sorted(entries.keys() - protected, key=lambda key: entries[key][1]):
        size, used = entries[key]
        if now - used <= max_idle and total <= limit:
            break
        try:
            if discard(key) is not False:
                total -= size
        except OSError as error:
            errors.append(str(error))
    return errors


def prune_asset_caches(owners, *, wait=True):
    """Apply one quota to DCO asset stores owned by the same hybrid service."""
    caches = [owner.asset_cache for owner in owners]
    with ExitStack() as locks:
        for cache in caches:
            locks.enter_context(cache.lock)
        entries, protected = {}, set()
        with ExitStack() as state_locks:
            for owner in owners:
                if not owner.condition.acquire(blocking=wait):
                    # Leave the cache lock available to producer lookups.
                    return []
                state_locks.callback(owner.condition.release)
            for owner, cache in zip(owners, caches):
                entries.update({(cache, key): value for key, value in cache.entries.items()})
                protected.update((cache, key) for key in owner._protected_assets())
        return evict_unused(entries, protected, caches[0].limit,
                            caches[0].max_idle, lambda item: item[0].retire(item[1]),
                            now=time.time())


def remove_owned(path, *, cancel=None):
    """Remove an owned entry without traversing symlinks or Windows junctions."""
    def remove(entry):
        if cancel and cancel():raise InterruptedError('Job cleanup cancelled')
        try:info=os.lstat(entry)
        except FileNotFoundError:return
        if stat.S_ISDIR(info.st_mode):
            linked=stat.S_ISLNK(info.st_mode)or bool(getattr(info,'st_file_attributes',0)&1024)
            if not linked:
                with os.scandir(entry)as children:
                    for child in children:remove(child.path)
            os.rmdir(entry)
        else:os.unlink(entry)
    remove(os.fspath(native_path(path,force=True)))


class Reclaimer:
    """Retire owned names atomically; reclaim their bytes outside control locks."""
    def __init__(self, root, *, source_root=None):
        self.root=Path(root)
        self.source_root=Path(source_root)if source_root is not None else self.root.parent
        self.root.mkdir(exist_ok=True)
        if self.root.is_symlink() or getattr(self.root.lstat(),'st_file_attributes',0)&1024:
            raise ValueError('Cleanup namespace must be an owned directory')
        self.lock=threading.Lock();self.pending=deque(self.root.iterdir())
        self.thread=None;self.active=False;self.cleaned=0;self.errors=deque(maxlen=8)
        with self.lock:self._start()

    def _start(self):
        if self.pending and self.thread is None:
            self.thread=threading.Thread(target=self._run,name='dco-reclaim',daemon=True)
            self.thread.start()

    def defer(self, path, *, after_move=None):
        path=native_path(path,force=True)
        scope=native_path(self.source_root,force=True)
        root=native_path(self.root,force=True)
        if (not scope in path.parents or path==root or root in path.parents):
            raise ValueError('Cleanup source must belong to its managed namespace')
        target=root/uuid.uuid4().hex
        try:path.rename(target)
        except FileNotFoundError:return
        if after_move is not None:
            try:after_move()
            except BaseException:
                target.rename(path)
                raise
        with self.lock:
            self.pending.append(target);self._start()
        return target

    def _run(self):
        while True:
            with self.lock:
                if not self.pending:
                    self.active=False;self.thread=None;return
                path=self.pending.popleft();self.active=True
            try:remove_owned(path)
            except OSError as error:
                with self.lock:self.errors.append(str(error))
            else:
                with self.lock:self.cleaned+=1

    def snapshot(self):
        with self.lock:
            return dict(pending=len(self.pending)+int(self.active),cleaned=self.cleaned,errors=list(self.errors))

    def close(self):
        with self.lock:thread=self.thread
        if thread:thread.join(.25)
        try:self.root.rmdir()
        except OSError:pass  # Retired bytes resume in this namespace next start.


class TransportRetirement:
    """Persist DELETE intent before a background thread touches a transport copy."""

    def __init__(self, root, reclaim):
        self.root = Path(root)
        self.root.mkdir(exist_ok=True)
        if self.root.is_symlink() or getattr(self.root.lstat(), 'st_file_attributes', 0) & 1024:
            raise ValueError('Retirement namespace must be an owned directory')
        self.reclaim = reclaim
        self.pending = [(0, path.stem) for path in self.root.glob('*.json')
                        if re.fullmatch('[a-f0-9]{32}', path.stem)]
        heapq.heapify(self.pending)
        self.locks = SubmissionLocks()
        self.condition = threading.Condition()
        self.stopping = False
        self.thread = None

    def restore(self, records):
        for _, identity in self.pending:
            if identity in records:
                records.forget(identity, journal=False)
            else:
                records.deleted.add(identity)

    def mark(self, identity, record):
        atomic_json(self.root / (identity + '.json'), record, private=True)

    def schedule(self, identity):
        with self.condition:
            heapq.heappush(self.pending, (0, identity))
            self._start()
            self.condition.notify()

    def start(self):
        with self.condition:
            self._start()

    def _start(self):
        if self.pending and self.thread is None and not self.stopping:
            self.thread = threading.Thread(target=self._run, name='transport-retirement', daemon=True)
            self.thread.start()

    def _run(self):
        while True:
            with self.condition:
                if self.stopping or not self.pending:
                    self.thread = None
                    return
                due, identity = self.pending[0]
                remaining = due - time.monotonic()
                if remaining > 0:
                    self.condition.wait(remaining)
                    continue
                heapq.heappop(self.pending)
            path = self.root / (identity + '.json')
            try:
                self.reclaim(identity, json.loads(path.read_bytes()))
                path.unlink()
            except (OSError, ValueError):
                # The intent still owns both the local name and any worker
                # copies. An unavailable provider is retried independently.
                with self.condition:
                    heapq.heappush(self.pending, (time.monotonic() + 5, identity))

    def close(self):
        with self.condition:
            self.stopping = True
            thread = self.thread
            self.condition.notify_all()
        if thread:
            thread.join()


def retire_job(service, identity, describe=None):
    """Acknowledge one terminal original without waiting for artifact I/O."""
    from .records import load_job
    from .worker import Conflict
    if not re.fullmatch('[a-f0-9]{32}', identity):
        raise ValueError('Job id requires 32 lowercase hex characters')
    retired = {'deleted': identity}
    with service.retirement.locks.hold(identity):
        try:
            job = load_job(service.jobs, service.condition, identity)
        except KeyError:
            return retired
        with service.condition:
            if job['result']['status'] not in TERMINAL:
                raise Conflict('Cancel and wait for the job before deleting it')
            record = describe(job) if describe else {}
        service.retirement.mark(identity, record)
        with service.condition:
            job['retired'] = True
            service.jobs.forget(identity, journal=False)
            if hasattr(service, 'cleanup_pending'):
                service.cleanup_pending.discard(identity)
        service.retirement.schedule(identity)
    return retired


class AssetCache:
    """LRU metadata is collected once; ordinary health polls never scan files."""
    def __init__(self, root, limit=ASSET_CACHE_BYTES, *, archives=False,
                 max_idle=ASSET_CACHE_IDLE_SECONDS):
        self.root, self.limit = Path(root).absolute(), limit
        self.archives = archives
        self.max_idle = max_idle
        # File operations share this lock; health reads only the short metadata lock.
        self.lock = threading.RLock()
        self.metadata_lock = threading.Lock()
        self.entries = {}
        self.revision = 0
        self.size_index=self.root.parent/(self.root.name+'.sizes.jsonl')
        sizes={};self.index_records=0
        if not archives and self.size_index.is_file():
            with self.size_index.open('rb')as stream:
                for line in stream:
                    try:
                        row=json.loads(line)
                        if (isinstance(row,dict)and isinstance(row.get('id'),str)and re.fullmatch(r'[a-f0-9]{64}',row['id'])
                                and type(row.get('bytes'))is int and row['bytes']>=0):
                            sizes[row['id']]=row['bytes'];self.index_records+=1
                    except (ValueError,UnicodeError):pass
        self.reclaimer=Reclaimer(self.root/'.trash')
        for path in self.root.iterdir():
            identity = path.stem if archives else path.name
            valid = path.suffix == '.zip' and path.is_file() if archives else path.is_dir()
            if re.fullmatch(r'[a-f0-9]{64}', identity) and valid and not self._linked(path):
                self._add(path, sizes.get(identity))
        self._compact_sizes()

    def _add(self, path, known_size=None):
        # Assets can unpack successfully into the extended namespace while
        # their deep children still exceed Win32 paths during cache indexing.
        path = native_path(path,force=True)
        size = path.stat().st_size if self.archives else known_size or 0
        if not self.archives and known_size is None:
            for folder, directories, files in os.walk(path, followlinks=False):
                directories[:] = [name for name in directories if not self._linked(Path(folder)/name)]
                size += sum((Path(folder)/name).stat().st_size for name in files
                            if not self._linked(Path(folder)/name))
        used = path.stat().st_mtime
        with self.metadata_lock:
            self.entries[path.stem if self.archives else path.name] = (size, used)
        if not self.archives and known_size is None:
            with self.size_index.open('a+b')as stream:
                stream.seek(0,os.SEEK_END)
                if stream.tell():
                    stream.seek(-1,os.SEEK_END)
                    if stream.read(1)!=b'\n':stream.write(b'\n')
                stream.write((json.dumps(dict(id=path.name,bytes=size),separators=(',',':'))+'\n').encode('utf8'))
            self.index_records+=1

    def _compact_sizes(self):
        if self.archives or self.index_records<=max(1024,4*len(self.entries)):return
        pending=self.size_index.with_suffix('.tmp')
        try:
            with pending.open('w',encoding='utf8')as stream:
                for identity,(size,_)in self.entries.items():
                    stream.write(json.dumps(dict(id=identity,bytes=size),separators=(',',':'))+'\n')
                stream.flush();os.fsync(stream.fileno())
            pending.replace(self.size_index);self.index_records=len(self.entries)
        finally:
            if pending.exists():pending.unlink()

    def reclaim_uploads(self):
        """The service calls this only after acquiring its exclusive state lock."""
        for path in self.root.iterdir():
            if re.fullmatch(r'[a-f0-9]{64}\.upload',path.name):self.reclaimer.defer(path)

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
                        limit_bytes=self.limit, max_idle_seconds=self.max_idle,
                        count=len(self.entries), reclaim=self.reclaimer.snapshot())

    def retire(self, identity):
        """Retire one explicitly released asset without evicting its neighbours."""
        if not isinstance(identity,str)or not re.fullmatch(r'[a-f0-9]{64}',identity):
            raise ValueError('Asset id requires SHA256')
        with self.lock:
            path=self._path(identity)
            present=path.exists()
            if present and self._linked(path):
                raise ValueError('Asset path became a link')
            size=self.entries.get(identity,(0,0))[0] if present else 0
            self.reclaimer.defer(path)
            with self.metadata_lock:
                if self.entries.pop(identity,None) is not None:self.revision+=1
            return dict(retired_asset=identity,retired=present,bytes=size)

    def prune(self, protected):
        with self.lock:
            errors = evict_unused(self.entries, protected, self.limit, self.max_idle,
                                  self.retire, now=time.time())
            self._compact_sizes()
            return errors
