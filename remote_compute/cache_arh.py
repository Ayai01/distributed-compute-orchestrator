"""Exchange ARH observations by their producer keys, never SQLite files."""

import base64
import hashlib
import importlib
import json
import math
from pathlib import Path
import sqlite3
import sys
import zlib

from .bundle import inside


# These are the existing producer tables, including Conv's resumable prefixes.
STORES = {
    'mac': ('build/evaluations.sqlite3', 'mac_evaluations', ('cache_key',)),
    'conv': ('build/conv_pool/evaluations.sqlite3', 'evaluations', ('key',)),
    'conv-prefix': ('build/conv_pool/evaluations.sqlite3', 'prefixes', ('key',)),
    'riscv': ('build/riscv_core_evaluations.sqlite3', 'riscv_evaluations', ('key',)),
    'neural': ('build/workbench/evaluations.sqlite3', 'neural_evaluations', ('project', 'key')),
}


def _columns(store):
    return (*STORES[store][2], *(('trials',) if store == 'conv-prefix' else ()),
            'value' if store == 'neural' else 'data', 'touched')


def _key(store, row):
    return json.dumps([store, *(row[name] for name in STORES[store][2])],
                      ensure_ascii=False, separators=(',', ':'))


def _identity(key):
    parts = json.loads(key)
    if (not isinstance(parts, list) or not parts or parts[0] not in STORES
            or len(parts) != len(STORES[parts[0]][2])+1
            or any(not isinstance(value, str) or not value for value in parts[1:])
            or json.dumps(parts, ensure_ascii=False, separators=(',', ':')) != key):
        raise ValueError('Invalid ARH observation key')
    return parts[0], parts[1:]


def _digest(store, row):
    value = [row['value' if store == 'neural' else 'data']]
    if store == 'conv-prefix':
        value.append(row['trials'])
    if store == 'neural':
        value[0] = base64.b64encode(value[0]).decode('ascii')
    raw = json.dumps(value, ensure_ascii=False, separators=(',', ':'), allow_nan=False).encode()
    return hashlib.sha256(raw).hexdigest()


def _record(store, row):
    row = dict(row)
    digest = _digest(store, row)
    if store == 'neural':
        row['value'] = base64.b64encode(row['value']).decode('ascii')
    return dict(row, digest=digest)


def _read(root, keys=None):
    root = Path(root).resolve()
    requested = None if keys is None else set(keys)
    for store, (relative, table, primary) in STORES.items():
        path = inside(root, relative)
        if not path.is_file():
            continue
        connection = sqlite3.connect(path.as_uri()+'?mode=ro', uri=True, timeout=30)
        connection.row_factory = sqlite3.Row
        try:
            if not connection.execute('SELECT 1 FROM sqlite_master WHERE type=? AND name=?',
                                      ('table', table)).fetchone():
                continue
            columns = ','.join(_columns(store))
            if requested is None:
                rows = connection.execute('SELECT '+columns+' FROM '+table)
                for row in rows:
                    yield _key(store, row), store, row
            else:
                for key in requested:
                    kind, values = _identity(key)
                    if kind != store:
                        continue
                    row = connection.execute('SELECT '+columns+' FROM '+table+' WHERE '+
                        ' AND '.join(name+'=?' for name in primary), values).fetchone()
                    if row is not None:
                        yield key, store, row
        finally:
            connection.close()


def manifest(root):
    return {key: dict(digest=_digest(store, row),
                      bytes=len(row['value' if store == 'neural' else 'data'].encode('utf-8'))
                      if store != 'neural' else len(row['value']))
            for key, store, row in _read(root)}


def export(root, keys):
    return {key: _record(store, row) for key, store, row in _read(root, keys)}


def _open_store(root, path, schema):
    """Let the installed ARH producer create/verify this machine's ownership."""
    sys.path.insert(0, str(root))
    try:
        storage = importlib.import_module('framework.storage')
        if Path(storage.__file__).resolve() != (root/'framework/storage.py').resolve():
            raise ValueError('Loaded ARH storage belongs to a different project root')
        connection = storage.open_owned_sqlite(root, path, legacy_schema=schema)
        try:
            # Existing unknown databases may be opened by the producer without
            # adoption. Only its verified owned stores participate in exchange.
            storage._owned_sqlite(root, path)
        except BaseException:
            connection.close()
            raise
        return connection
    finally:
        sys.path.remove(str(root))


def _validated(key, record):
    store, values = _identity(key)
    row = dict(record)
    if set(row) != set(_columns(store)) | {'digest'}:
        raise ValueError('ARH observation columns differ from producer schema')
    if any(row[name] != value for name, value in zip(STORES[store][2], values)):
        raise ValueError('ARH observation identity mismatch')
    if isinstance(row['touched'], bool) or not isinstance(row['touched'], (int, float)) or not math.isfinite(row['touched']):
        raise ValueError('Invalid ARH observation time')
    if store == 'conv-prefix' and (type(row['trials']) is not int or row['trials'] < 0):
        raise ValueError('Invalid Conv prefix depth')
    if store == 'neural':
        row['value'] = base64.b64decode(row['value'], validate=True)
        # The producer stores its packed JSON in a bounded zlib record.
        decoder = zlib.decompressobj()
        decoded = decoder.decompress(row['value'], 1024*1024+1)
        if len(decoded) > 1024*1024 or not decoder.eof or decoder.unused_data:
            raise ValueError('Invalid ARH packed observation')
        json.loads(decoded)
    else:
        if not isinstance(row['data'], str):
            raise ValueError('ARH observation must retain producer JSON')
        decoded = json.loads(row['data'])
        if store == 'conv-prefix' and decoded.get('trials') != row['trials']:
            raise ValueError('Conv prefix depth differs from its payload')
    if _digest(store, row) != row['digest']:
        raise ValueError('ARH observation digest mismatch')
    return store, row


def _create(connection, store):
    table, primary = STORES[store][1:]
    definitions = [name+' TEXT NOT NULL' for name in primary]
    if store == 'conv-prefix':
        definitions.append('trials INTEGER NOT NULL')
    definitions.extend(('value BLOB NOT NULL' if store == 'neural' else 'data TEXT NOT NULL',
                        'touched REAL NOT NULL', 'PRIMARY KEY('+','.join(primary)+')'))
    connection.execute('CREATE TABLE IF NOT EXISTS '+table+' ('+','.join(definitions)+')')
    if tuple(row[1] for row in connection.execute('PRAGMA table_info('+table+')')) != _columns(store):
        raise ValueError('ARH cache schema differs from the installed producer')


def merge(root, records):
    root = Path(root).resolve()
    result = dict(added=0, updated=0, unchanged=0, conflicts=0, rejected=0, digests={})
    groups = {}
    for key, record in records.items():
        try:
            store, row = _validated(key, record)
        except (ValueError, TypeError, KeyError, AttributeError, zlib.error):
            result['rejected'] += 1
            continue
        groups.setdefault(STORES[store][0], []).append((key, store, row))
    for relative, rows in groups.items():
        path = inside(root, relative)
        path.parent.mkdir(parents=True, exist_ok=True)
        members = [store for store, spec in STORES.items() if spec[0] == relative]
        schema = {STORES[store][1]: _columns(store) for store in members}
        connection = _open_store(root, path, schema)
        connection.row_factory = sqlite3.Row
        try:
            with connection:
                connection.execute('BEGIN IMMEDIATE')
                for store in members:
                    _create(connection, store)
                for key, store, row in rows:
                    table, primary = STORES[store][1:]
                    where = ' AND '.join(name+'=?' for name in primary)
                    values = [row[name] for name in primary]
                    current = connection.execute('SELECT * FROM '+table+' WHERE '+where, values).fetchone()
                    digest = _digest(store, current) if current is not None else None
                    if (digest == row['digest'] or current is not None
                            and store == 'conv-prefix' and row['trials'] < current['trials']):
                        result['unchanged'] += 1
                    elif current is None or store == 'conv-prefix' and row['trials'] > current['trials']:
                        columns = _columns(store)
                        connection.execute('INSERT OR REPLACE INTO '+table+' ('+','.join(columns)+') VALUES ('+
                                           ','.join('?' for _ in columns)+')', [row[name] for name in columns])
                        result['added' if current is None else 'updated'] += 1
                        digest = row['digest']
                    else:
                        result['conflicts'] += 1
                    result['digests'][key] = digest
        finally:
            connection.close()
    return result
