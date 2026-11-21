"""Checkpoint large immutable uploads on the workers' shared asset volume."""

from hashlib import sha256
import json
import os
from pathlib import Path
import re

from .bundle import MAX_BODY, atomic_json


ASSET_RESUME_CAPABILITY = 'resumable_assets'
ASSET_CHUNK_BYTES = 2 * 1024**2
ASSET_RESUME_MIN_BYTES = 1024**2
_DIGEST = r'[a-f0-9]{64}'
_PART = re.compile(r'\.upload-(' + _DIGEST + r')(_meta)?\Z')
_RANGE = re.compile(r'bytes ([0-9]{1,9})-([0-9]{1,9})/([0-9]{1,9})\Z')


class AssetUploads:
    def __init__(self, root):
        self.root = Path(root)

    def _paths(self, identity):
        if not re.fullmatch(_DIGEST, identity):
            raise ValueError('Asset id requires SHA256')
        part = self.root / ('.upload-' + identity)
        return self.root / (identity + '.zip'), part, part.with_name(part.name + '_meta')

    def _guard(self, identity):
        from .worker import lock_root

        return lock_root(self.root / '.upload-locks', identity[:2])

    def status(self, identity):
        archive, part, meta = self._paths(identity)
        try:
            size = archive.stat().st_size
        except FileNotFoundError:
            try:
                size = part.stat().st_size if meta.is_file() else 0
                checkpoint = json.loads(meta.read_bytes()) if size else {}
            except FileNotFoundError:
                checkpoint = {}
            if not isinstance(checkpoint, dict):
                raise ValueError('Invalid asset upload checkpoint')
            total, offset = checkpoint.get('total'), checkpoint.get('offset')
            if type(total) is int and type(offset) is int and total <= MAX_BODY and 0 < offset <= min(size, total):
                return dict(id=identity, complete=False, total=total, offset=offset)
            # Publication can rename the part between the two observations.
            try:
                size = archive.stat().st_size
            except FileNotFoundError:
                return dict(id=identity, complete=False, total=None, offset=0)
        return dict(id=identity, complete=True, total=size, offset=size)

    def receive(self, identity, body=None, content_range=None):
        if body is None:
            return self.status(identity)
        archive, part, meta = self._paths(identity)
        if content_range is None:
            commit = json.loads(body)
            if not isinstance(commit, dict) or set(commit) != {'total'} or type(commit['total']) is not int:
                raise ValueError('Asset upload commit requires total bytes')
            total = commit['total']
        else:
            match = _RANGE.fullmatch(content_range)
            if not match:
                raise ValueError('Asset upload requires a valid Content-Range')
            offset, end, total = map(int, match.groups())
            if not 0 <= offset <= end < total or end - offset + 1 != len(body) or len(body) > ASSET_CHUNK_BYTES:
                raise ValueError('Asset chunk does not match its range')
        if not 0 < total <= MAX_BODY:
            raise ValueError('Asset upload exceeds the archive size limit')
        with self._guard(identity):
            current = self.status(identity)
            if current['total'] not in (None, total):
                raise ValueError('Asset upload total changed')
            confirmed = current['offset']
            if current['complete']:
                return current
            if content_range is not None:
                if offset != confirmed:
                    return current
                with part.open('r+b' if confirmed else 'wb') as stream:
                    stream.seek(confirmed)
                    stream.write(body)
                    stream.truncate()
                    stream.flush()
                    os.fsync(stream.fileno())
                current.update(total=total, offset=end + 1)
                atomic_json(meta, current, private=True)
                return current
            if confirmed != total:
                return current
            digest = sha256()
            with part.open('rb') as stream:
                for block in iter(lambda: stream.read(1024**2), b''):
                    digest.update(block)
            if digest.hexdigest() != identity:
                meta.unlink(missing_ok=True)
                part.unlink(missing_ok=True)
                raise ValueError('Asset digest does not match bundle')
            from .bundle import validate_archive

            validate_archive(part, self.root / identity)
            part.replace(archive)
            meta.unlink(missing_ok=True)
            return dict(id=identity, complete=True, total=total, offset=total)

    def discard(self, path, stamp):
        from .worker import Conflict

        match = _PART.fullmatch(path.name)
        identity, metadata = match.groups()
        try:
            guard = self._guard(identity)
        except Conflict:
            return False
        with guard:
            try:
                info = path.lstat()
            except FileNotFoundError:
                return True
            if (info.st_ino, info.st_mtime_ns) != stamp:
                return False
            _, part, meta = self._paths(identity)
            if not metadata:
                meta.unlink(missing_ok=True)
            elif part.exists():
                return False
            path.unlink(missing_ok=True)
            return True


def is_resumable_part(path):
    match = _PART.fullmatch(path.name)
    return bool(match and not match.group(2))


def is_resumable_stage(path):
    return bool(_PART.fullmatch(path.name))
