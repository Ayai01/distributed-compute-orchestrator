"""Reclaim owned job files and bounded, reconstructible worker assets."""

import os
import json
from collections import deque
from pathlib import Path
import re
import stat
import threading
import time
import uuid
from .bundle import native_path


def remove_owned(path, *, cancel=None):
    """Remove an owned entry without traversing symlinks or Windows junctions."""
    if cancel and cancel():raise InterruptedError('Job cleanup cancelled')
    path=native_path(path,force=True)
    try:
        info = path.lstat()
    except FileNotFoundError:
        return
    if stat.S_ISDIR(info.st_mode):
        linked = stat.S_ISLNK(info.st_mode) or bool(getattr(info, 'st_file_attributes', 0) & 1024)
        if not linked:
            for child in path.iterdir():
                if cancel:remove_owned(child, cancel=cancel)
                else:remove_owned(child)
        path.rmdir()
    else:
        path.unlink()


class Reclaimer:
    """Retire owned names atomically; reclaim their bytes outside control locks."""
    def __init__(self, root):
        self.root=Path(root)
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

    def defer(self, path):
        path=Path(path).absolute()
        if path.parent!=self.root.absolute().parent:
            raise ValueError('Cleanup source must belong to its managed namespace')
        target=self.root/uuid.uuid4().hex
        try:path.rename(target)
        except FileNotFoundError:return
        with self.lock:
            self.pending.append(target);self._start()

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


class AssetCache:
    """LRU metadata is collected once; ordinary health polls never scan files."""
    def __init__(self, root, limit=512 * 1024**2, *, archives=False):
        self.root, self.limit = Path(root).absolute(), limit
        self.archives = archives
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
                        limit_bytes=self.limit, count=len(self.entries),reclaim=self.reclaimer.snapshot())

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
            return self._prune(protected)

    def _prune(self, protected):
        # Live inputs do not evict the bounded warm set needed by the next stage.
        idle = [(identity, size, used) for identity, (size, used) in self.entries.items()
                if identity not in protected]
        total = sum(size for _identity, size, _used in idle)
        errors = []
        for identity, size, _used in sorted(idle, key=lambda item: item[2]):
            if total <= self.limit:
                break
            try:
                self.reclaimer.defer(self._path(identity))
            except OSError as error:
                errors.append(str(error))
            else:
                with self.metadata_lock:
                    self.entries.pop(identity)
                    self.revision += 1
                total -= size
        self._compact_sizes()
        return errors
