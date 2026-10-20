"""Exchange AOP's raw v5 corner records; dependent aggregates stay local."""

import base64
import hashlib
import importlib.util
import math
import os
from pathlib import Path
import re
import struct
import zlib

_MAGIC = b'AOPFP32\x05'
_HEADER = struct.Struct('<8sI')
_FRAME = struct.Struct('<II')
_MAX_RECORD = 64 * 1024 * 1024
_NAMESPACE = re.compile(r'fp32-v5-[0-9a-f]{64}\Z')
_KEY = re.compile(r'(fp32-v5-[0-9a-f]{64})/(0)/([0-9a-f]{64})\Z')


def _directory(root):
    # This is AOP config.get_cache_root's existing environment contract.
    path = Path(os.environ.get('AOP_CACHE_DIR') or Path(root) / '.aop_cache').expanduser()
    return _plain(path).resolve()


def _plain(path):
    try:
        info = path.lstat()
    except FileNotFoundError:
        return path
    if path.is_symlink() or getattr(info, 'st_file_attributes', 0) & 0x400:
        raise ValueError('AOP cache path is a link: ' + str(path))
    return path


def _coordination(root, directory):
    path = Path(root) / 'utils' / 'compat.py'
    spec = importlib.util.spec_from_file_location('_dco_aop_compat', str(path))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.CoordinationStore(directory)


def _validate(payload, width):
    fixed = 33 + width * 4
    if not 0 <= width <= (_MAX_RECORD - 33) // 4 or not fixed <= len(payload) <= _MAX_RECORD:
        raise ValueError('Invalid AOP record width or length')
    if payload[0] not in (0, 1):
        raise ValueError('Invalid AOP record kind')
    if any(math.isinf(value[0]) for value in struct.iter_unpack('<f', payload[33:fixed])):
        raise ValueError('Invalid infinite AOP result')


def _scan(path):
    """Match CornerCache replay: last valid frame wins; partial tail is ignored."""
    _plain(path.parent)
    _plain(path)
    records = {}
    with path.open('rb') as stream:
        size = os.fstat(stream.fileno()).st_size
        header = stream.read(_HEADER.size)
        if len(header) != _HEADER.size:
            raise ValueError('Invalid AOP cache header')
        magic, width = _HEADER.unpack(header)
        if magic != _MAGIC:
            raise ValueError('Unsupported AOP cache version')
        end = _HEADER.size
        while end < size:
            frame = stream.read(_FRAME.size)
            if len(frame) < _FRAME.size:
                break
            length, checksum = _FRAME.unpack(frame)
            if not 1 <= length <= _MAX_RECORD:
                raise ValueError('Corrupt AOP cache record size')
            payload = stream.read(length)
            if len(payload) < length:
                break
            if zlib.crc32(payload) != checksum:
                if end + _FRAME.size + length != size:
                    raise ValueError('Corrupt AOP cache record')
                break
            if payload == b'\x02':
                records = {key: value for key, value in records.items() if key[0] == 0}
            else:
                _validate(payload, width)
                records[(payload[0], payload[1:33].hex())] = payload
            end = stream.tell()
    return width, records, end


def _digest(width, payload):
    return hashlib.sha256(struct.pack('<I', width) + payload).hexdigest()


def _record(width, payload):
    return {'width': width, 'payload': base64.b64encode(payload).decode('ascii'),
            'digest': _digest(width, payload)}


def _key(namespace, payload):
    return namespace + '/' + str(payload[0]) + '/' + payload[1:33].hex()


def manifest(root):
    result = {}
    directory = _directory(root)
    for path in sorted(directory.glob('fp32-v5-*/records.bin')):
        if not _NAMESPACE.fullmatch(path.parent.name):
            continue
        width, records, _ = _scan(path)
        for payload in records.values():
            # Aggregate keys do not record corner dependencies. Importing an old
            # aggregate could undo a target's explicit refresh/invalidation.
            if payload[0] != 0:
                continue
            result[_key(path.parent.name, payload)] = {
                'digest': _digest(width, payload),
                'bytes': (len(payload) + 2) // 3 * 4 + 64}
    return result


def export(root, keys):
    selected = {}
    for key in keys:
        match = _KEY.fullmatch(key)
        if match is None:
            raise ValueError('Invalid AOP cache key')
        selected.setdefault(match[1], set()).add(key)
    result = {}
    for namespace, wanted in sorted(selected.items()):
        path = _plain(_directory(root) / namespace) / 'records.bin'
        if not path.exists():
            continue
        width, records, _ = _scan(path)
        for payload in records.values():
            key = _key(namespace, payload)
            if key in wanted:
                result[key] = _record(width, payload)
    return result


def merge(root, records):
    result = dict(added=0, updated=0, unchanged=0, conflicts=0, rejected=0, digests={})
    groups = {}
    for key, record in sorted(records.items()):
        try:
            match = _KEY.fullmatch(key)
            if match is None:
                raise ValueError('Invalid AOP cache key')
            width = record['width']
            if isinstance(width, bool) or not isinstance(width, int):
                raise ValueError('Invalid AOP width')
            payload = base64.b64decode(record['payload'], validate=True)
            _validate(payload, width)
            if _key(match[1], payload) != key:
                raise ValueError('AOP key does not match payload')
            if record['digest'] != _digest(width, payload):
                raise ValueError('AOP record digest mismatch')
            groups.setdefault(match[1], []).append((key, width, payload))
        except (ValueError, KeyError, TypeError):
            result['rejected'] += 1
    if not groups:
        return result
    directory = _directory(root)
    _plain(directory)
    store = _coordination(root, directory)
    for namespace, rows in sorted(groups.items()):
        unit = _plain(directory / namespace)
        unit_key = 'cache-unit:' + os.path.normcase(str(unit))
        # Quota retirement and appenders use these same locks and lease key.
        with store.lock(unit_key, timeout=1):
            unit.mkdir(parents=True, exist_ok=True)
            token = store.pin(unit_key)
        try:
            with store.lock('append:' + os.path.normcase(str(unit)), timeout=1):
                path = _plain(unit / 'records.bin')
                if path.exists():
                    width, current, end = _scan(path)
                else:
                    width, current, end = rows[0][1], {}, _HEADER.size
                pending = []
                for key, incoming_width, payload in rows:
                    identity = (payload[0], payload[1:33].hex())
                    old = current.get(identity)
                    if incoming_width != width or old is not None and old != payload:
                        result['conflicts'] += 1
                    elif old is not None:
                        result['unchanged'] += 1
                    else:
                        pending.append(payload)
                        current[identity] = payload
                        result['added'] += 1
                    if identity in current:
                        result['digests'][key] = _digest(width, current[identity])
                if pending:
                    # Same partial-tail recovery as CornerCache._sync(repair=True).
                    with path.open('r+b' if path.exists() else 'w+b') as stream:
                        if stream.seek(0, 2) == 0:
                            stream.write(_HEADER.pack(_MAGIC, width))
                        stream.seek(end)
                        stream.truncate()
                        for payload in pending:
                            stream.write(_FRAME.pack(len(payload), zlib.crc32(payload)) + payload)
                        stream.flush()
                        os.fsync(stream.fileno())
        finally:
            store.unpin(token)
    return result
