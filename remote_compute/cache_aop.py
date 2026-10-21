"""Exchange raw v5 corners under AOP's readable execution contracts."""

import base64
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import re
import struct
import uuid
import zlib

from .bundle import atomic_json
from .policy import canonical_capability

_MAGIC = b'AOPFP32\x05'
_HEADER = struct.Struct('<8sI')
_FRAME = struct.Struct('<II')
_MAX_RECORD = 64 * 1024 * 1024
_FIELDS = ('engine_family', 'major_version', 'execution_protocol',
           'measurement_protocol', 'tolerance_profile')
_NAME = re.compile(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,191}\Z')
_DATA_ID = re.compile(r'[0-9a-f]{64}\Z')
_UNIT = re.compile(r'[0-9a-f]{32}\Z')


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


def _contract(value):
    if not isinstance(value, dict) or set(value) != {'environment', 'scientific_namespace'}:
        raise ValueError('Invalid AOP cache contract')
    environment, namespace = value['environment'], value['scientific_namespace']
    if not isinstance(namespace, str) or not _NAME.fullmatch(namespace):
        raise ValueError('Invalid AOP scientific namespace')
    if not isinstance(environment, dict) or set(environment) != set(_FIELDS):
        raise ValueError('AOP cache needs its complete semantic environment')
    if environment['engine_family'] not in ('ngspice', 'hspice', 'geometry'):
        raise ValueError('Invalid AOP engine family')
    for item in environment.values():
        if isinstance(item, bool) or not isinstance(item, (str, int)) or not _NAME.fullmatch(str(item)):
            raise ValueError('Invalid AOP semantic environment field')
        canonical_capability(str(item), registration=True)
    return {'environment': dict(environment), 'scientific_namespace': namespace}


def _reusable(contract):
    environment = contract['environment']
    return (str(environment['major_version']) != 'unknown' and
            not any(str(environment[field]).startswith(('legacy', 'unknown'))
                    for field in ('execution_protocol', 'measurement_protocol', 'tolerance_profile')))


def _contract_key(contract):
    # This is plain metadata, never a digest of the engine or its semantics.
    return json.dumps(contract, sort_keys=True, separators=(',', ':'))


def _key(contract, payload):
    return json.dumps(['aop-corner-v1', contract['scientific_namespace'],
                       contract['environment'], payload[0], payload[1:33].hex()],
                      sort_keys=True, separators=(',', ':'))


def _identity(key):
    parts = json.loads(key)
    if (not isinstance(parts, list) or len(parts) != 5 or parts[0] != 'aop-corner-v1'
            or type(parts[3]) is not int or parts[3] != 0
            or not isinstance(parts[4], str) or not _DATA_ID.fullmatch(parts[4])):
        raise ValueError('Invalid AOP corner key')
    contract = _contract(dict(environment=parts[2], scientific_namespace=parts[1]))
    if not _reusable(contract) or _key(contract, b'\0' + bytes.fromhex(parts[4])) != key:
        raise ValueError('AOP corner key needs a reusable semantic contract')
    return contract


def _units(root):
    directory = _directory(root)
    for path in sorted(directory.glob('semantic-v1/*/*/contract.json')):
        for parent in (path.parent.parent.parent, path.parent.parent, path.parent, path):
            _plain(parent)
        contract = _contract(json.loads(path.read_text(encoding='utf-8')))
        environment = contract['environment']
        if (not _UNIT.fullmatch(path.parent.name) or
                path.parent.parent.name != str(environment['engine_family']) + '-' + str(environment['major_version'])):
            raise ValueError('AOP cache directory differs from its semantic contract')
        if _reusable(contract):
            yield path.parent, contract


def _target_unit(directory, store, contract):
    environment = contract['environment']
    semantic = json.dumps(environment, sort_keys=True)
    with store.transaction() as connection:
        connection.execute('CREATE TABLE IF NOT EXISTS semantic_cache_units '
                           '(namespace TEXT,environment TEXT,unit TEXT,PRIMARY KEY(namespace,environment))')
        row = connection.execute('SELECT unit FROM semantic_cache_units WHERE namespace=? AND environment=?',
                                 (contract['scientific_namespace'], semantic)).fetchone()
        unit = row[0] if row else uuid.uuid4().hex
        if not isinstance(unit, str) or not _UNIT.fullmatch(unit):
            raise ValueError('Invalid AOP local cache unit')
        if row is None:
            connection.execute('INSERT INTO semantic_cache_units VALUES (?,?,?)',
                               (contract['scientific_namespace'], semantic, unit))
    base = _plain(directory / 'semantic-v1')
    family = _plain(base / (str(environment['engine_family']) + '-' + str(environment['major_version'])))
    return _plain(family / unit)


def manifest(root):
    result = {}
    for unit, contract in _units(root):
        path = unit / 'records.bin'
        if not path.is_file():
            continue
        width, records, _ = _scan(path)
        for payload in records.values():
            # Aggregate keys do not record corner dependencies. Importing an old
            # aggregate could undo a target's explicit refresh/invalidation.
            if payload[0] != 0:
                continue
            key = _key(contract, payload)
            result[key] = {
                'digest': _digest(width, payload),
                'bytes': (len(payload) + 2) // 3 * 4 + len(key.encode('utf-8')) + 64}
    return result


def export(root, keys):
    selected = {}
    for key in keys:
        contract = _identity(key)
        selected.setdefault(_contract_key(contract), set()).add(key)
    result = {}
    for unit, contract in _units(root):
        wanted = selected.get(_contract_key(contract))
        path = unit / 'records.bin'
        if not wanted or not path.is_file():
            continue
        width, records, _ = _scan(path)
        for payload in records.values():
            key = _key(contract, payload)
            if key in wanted:
                result[key] = _record(width, payload)
    return result


def merge(root, records):
    result = dict(added=0, updated=0, unchanged=0, conflicts=0, rejected=0, digests={})
    groups = {}
    for key, record in sorted(records.items()):
        try:
            contract = _identity(key)
            width = record['width']
            if isinstance(width, bool) or not isinstance(width, int):
                raise ValueError('Invalid AOP width')
            payload = base64.b64decode(record['payload'], validate=True)
            _validate(payload, width)
            if _key(contract, payload) != key:
                raise ValueError('AOP key does not match payload')
            if record['digest'] != _digest(width, payload):
                raise ValueError('AOP record digest mismatch')
            groups.setdefault(_contract_key(contract), (contract, []))[1].append((key, width, payload))
        except (ValueError, KeyError, TypeError):
            result['rejected'] += 1
    if not groups:
        return result
    directory = _directory(root)
    _plain(directory)
    store = _coordination(root, directory)
    for contract, rows in groups.values():
        unit = _target_unit(directory, store, contract)
        unit_key = 'cache-unit:' + os.path.normcase(str(unit))
        # Quota retirement and appenders use these same locks and lease key.
        with store.lock(unit_key, timeout=1):
            unit.mkdir(parents=True, exist_ok=True)
            descriptor = _plain(unit / 'contract.json')
            if descriptor.exists():
                if _contract(json.loads(descriptor.read_text(encoding='utf-8'))) != contract:
                    raise ValueError('AOP local cache unit contract differs')
            else:
                atomic_json(descriptor, contract)
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
