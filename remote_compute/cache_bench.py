"""Sync complete Bench reference snapshots without changing scientific fronts."""

import base64
import hashlib
import os
from pathlib import Path
import re
import tempfile

from .bundle import inside


def _path(root, key):
    if (not isinstance(key, str) or not re.fullmatch(r'reference/[A-Za-z0-9][A-Za-z0-9_.-]*\.npz', key)
            or key.endswith('.tmp.npz')):
        raise ValueError('Expected a Bench reference snapshot')
    return inside(root, key)


def _metadata(path):
    digest, size = hashlib.sha256(), 0
    with path.open('rb') as source:
        while True:
            block = source.read(1024*1024)
            if not block:
                break
            digest.update(block)
            size += len(block)
    return dict(digest=digest.hexdigest(), bytes=size)


def manifest(root):
    root = Path(root).resolve()
    directory = inside(root, 'reference')
    return {key: _metadata(_path(root, key)) for key in
            sorted('reference/'+path.name for path in directory.glob('*.npz')
                   if path.is_file() and not path.name.endswith('.tmp.npz'))}


def export(root, keys):
    result = {}
    for key in keys:
        path = _path(root, key)
        if not path.is_file():
            continue
        data = path.read_bytes()
        result[key] = dict(digest=hashlib.sha256(data).hexdigest(),
                           data=base64.b64encode(data).decode('ascii'))
    return result


def merge(root, records):
    result = dict(added=0, updated=0, unchanged=0, conflicts=0, rejected=0, digests={})
    for key, record in records.items():
        try:
            path = _path(root, key)
            data = base64.b64decode(record['data'], validate=True)
            digest = hashlib.sha256(data).hexdigest()
            if digest != record['digest']:
                raise ValueError('Bench reference digest mismatch')
        except (ValueError, TypeError, KeyError):
            result['rejected'] += 1
            continue
        if path.exists():
            current = _metadata(path)['digest']
            result['unchanged' if current == digest else 'conflicts'] += 1
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            descriptor, name = tempfile.mkstemp(prefix='.dco-reference-', suffix='.tmp', dir=path.parent)
            pending = Path(name)
            try:
                with os.fdopen(descriptor, 'wb') as output:
                    output.write(data)
                    output.flush()
                    os.fsync(output.fileno())
                # Atomic create-if-absent: a concurrent Bench publication wins.
                try:
                    os.link(pending, path)
                    result['added'] += 1
                    current = digest
                except FileExistsError:
                    current = _metadata(path)['digest']
                    result['unchanged' if current == digest else 'conflicts'] += 1
            finally:
                pending.unlink()
        result['digests'][key] = current
    return result
