"""Lossless logical exchange of RGCN observations, never its WAL or job ledger."""

import base64
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import zlib

_CIRCUIT = r'[A-Za-z0-9][A-Za-z0-9_-]*'
_META = re.compile(r'(' + _CIRCUIT + r')/0-metadata\Z')
_VIEW = re.compile(r'(' + _CIRCUIT + r')/1-view/([0-9a-f]{64})/([0-9a-f]{64})\Z')
_VERSION = 'native-circuit-dataset-v1'


def _api(root):
    spec = importlib.util.spec_from_file_location(
        '_dco_rgcn_dataset_store', str(Path(root) / 'src' / 'dataset_store.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _plain(path):
    try:
        info = path.lstat()
    except FileNotFoundError:
        return path
    if path.is_symlink() or getattr(info, 'st_file_attributes', 0) & 0x400:
        raise ValueError('RGCN dataset path is a link: ' + str(path))
    return path


def _path(root, circuit):
    if not re.fullmatch(_CIRCUIT, circuit):
        raise ValueError('Invalid RGCN circuit name')
    directory = _plain(Path(root).resolve() / 'datasets')
    return _plain(_plain(directory / circuit) / 'observations.sqlite3')


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)


def _digest(record):
    # Compression parameters and database row creation time are not scientific identity.
    data = dict(record)
    data.pop('digest', None)
    if data.get('kind') == 'view':
        data['payload'] = json.loads(zlib.decompress(base64.b64decode(data['payload'], validate=True)))
        data['design'] = dict(data['design'])
        data['design'].pop('created', None)
        data['view'] = dict(data['view'])
        data['view'].pop('created', None)
    return hashlib.sha256(_canonical(data).encode('utf8')).hexdigest()


def _metadata(store):
    # Acquisition identities, frozen-domain evidence and signature history stay intact.
    return {key: json.loads(value) for key, value in store.connection().execute(
        'SELECT key,value FROM metadata WHERE key != ?', ('updated',))}


def _signatures(metadata):
    signatures = {metadata['signature']}
    for transition in metadata.get('signature_history', []):
        if transition.get('policy') == 'append-only-compatible-transition':
            signatures.update((transition['from'], transition['to']))
    return signatures


def _signature(api, metadata, view):
    for signature in sorted(_signatures(metadata)):
        if api.digest([signature, view['design_id'], view['pvt']]) == view['key']:
            return signature
    raise ValueError('RGCN view key is outside the recorded physical-signature history')


def _view_record(store, row):
    connection = store.connection()
    design = dict(connection.execute('SELECT * FROM designs WHERE id=?', (row['design_id'],)).fetchone())
    provenance = [dict(item) for item in connection.execute(
        'SELECT * FROM provenance WHERE view_key=? ORDER BY source,record_id,stratum', (row['key'],))]
    for item in provenance:
        item['details'] = json.loads(item['details'])
    view = dict(row)
    payload = view.pop('payload')
    design['candidate'] = json.loads(design['candidate'])
    return {'kind': 'view', 'design': design, 'view': view, 'provenance': provenance,
            'payload': base64.b64encode(payload).decode('ascii')}


def _walk(root, wanted=None):
    api = _api(root)
    directory = Path(root).resolve() / 'datasets'
    for path in sorted(directory.glob('*/observations.sqlite3')):
        circuit = path.parent.name
        if not re.fullmatch(_CIRCUIT, circuit):
            continue
        if wanted is not None and not any(key.startswith(circuit + '/') for key in wanted):
            continue
        store = api.DatasetStore(_path(root, circuit), readonly=True)
        try:
            with store.snapshot():
                metadata = _metadata(store)
                key = circuit + '/0-metadata'
                if wanted is None or key in wanted:
                    yield key, {'kind': 'metadata', 'metadata': metadata}
                if wanted is None:
                    rows = store.connection().execute('SELECT * FROM views ORDER BY key')
                else:
                    identities = [match[3] for key in sorted(wanted)
                                  for match in [_VIEW.fullmatch(key)] if match and match[1] == circuit]
                    # Requested keys are a bounded transport batch.
                    rows = (store.connection().execute('SELECT * FROM views WHERE key=?', (key,)).fetchone()
                            for key in identities)
                for row in rows:
                    if row is None:
                        continue
                    signature = _signature(api, metadata, row)
                    key = circuit + '/1-view/' + signature + '/' + row['key']
                    if wanted is None or key in wanted:
                        yield key, _view_record(store, row)
        finally:
            store.close()


def manifest(root):
    return {key: {'digest': _digest(record), 'bytes': len(_canonical(record).encode('utf8'))}
            for key, record in _walk(root)}


def export(root, keys):
    wanted = set(keys)
    if any(_META.fullmatch(key) is None and _VIEW.fullmatch(key) is None for key in wanted):
        raise ValueError('Invalid RGCN cache key')
    return {key: dict(record, digest=_digest(record)) for key, record in _walk(root, wanted)}


def _validate_metadata(record):
    metadata = record['metadata']
    if record['kind'] != 'metadata' or metadata['version'] != _VERSION:
        raise ValueError('Unsupported RGCN dataset')
    if not isinstance(metadata['config']['circuit']['simulation'], dict):
        raise ValueError('Invalid RGCN dataset config')
    for signature in _signatures(metadata):
        if not isinstance(signature, str) or not re.fullmatch('[0-9a-f]{64}', signature):
            raise ValueError('Invalid RGCN physical signature')
    _canonical(metadata)
    if record['digest'] != _digest(record):
        raise ValueError('RGCN metadata digest mismatch')
    return metadata


def _validate_view(api, key, record, metadata):
    match = _VIEW.fullmatch(key)
    if match is None or record['kind'] != 'view':
        raise ValueError('Invalid RGCN view key')
    view, design = record['view'], record['design']
    payload = base64.b64decode(record['payload'], validate=True)
    observation = json.loads(zlib.decompress(payload))
    if (design['role'] not in ('train', 'validation', 'test')
            or not isinstance(design['family'], str)
            or api.design_key(design['candidate']) != design['id']
            or api.design_key(observation['candidate']) != design['id']
            or view['design_id'] != design['id'] or view['key'] != match[3]
            or observation['dataset_identity'] != view['key']
            or observation['design_id'] != design['id']
            or observation['dataset_status'] != view['status']
            or api.pvt_key(observation) != view['pvt']
            or _signature(api, metadata, view) != match[2]):
        raise ValueError('RGCN observation identity or partition mismatch')
    for item in record['provenance']:
        if item['view_key'] != view['key']:
            raise ValueError('RGCN provenance identity mismatch')
        _canonical(item['details'])
    _canonical(observation)
    _canonical(record)
    if record['digest'] != _digest(record):
        raise ValueError('RGCN observation digest mismatch')
    return payload


def _merge_view(api, store, key, record, result):
    connection = store.connection()
    payload = _validate_view(api, key, record, _metadata(store))
    design, view = record['design'], record['view']
    connection.execute('BEGIN IMMEDIATE')
    try:
        old_design = connection.execute('SELECT * FROM designs WHERE id=?', (design['id'],)).fetchone()
        old = connection.execute('SELECT * FROM views WHERE key=?', (view['key'],)).fetchone()
        if old_design is not None and (old_design['role'] != design['role'] or old_design['family'] != design['family']):
            result['conflicts'] += 1
        elif old is not None and _content(_view_record(store, old)) != _content(record):
            result['conflicts'] += 1
        else:
            connection.execute('INSERT OR IGNORE INTO designs VALUES (?,?,?,?,?)', (
                design['id'], _canonical(design['candidate']), design['family'], design['role'], design['created']))
            added = connection.execute('INSERT OR IGNORE INTO views VALUES (?,?,?,?,?,?,?,?)', (
                view['key'], view['design_id'], view['pvt'], view['status'], view['stratum'],
                view['seconds'], payload, view['created'])).rowcount
            changed, conflict = False, False
            for item in record['provenance']:
                identity = tuple(item[field] for field in ('view_key', 'source', 'record_id', 'stratum'))
                previous = connection.execute(
                    'SELECT details FROM provenance WHERE view_key=? AND source=? AND record_id=? AND stratum=?',
                    identity).fetchone()
                if previous is not None and json.loads(previous[0]) != item['details']:
                    conflict = True
                changed |= bool(connection.execute('INSERT OR IGNORE INTO provenance VALUES (?,?,?,?,?)',
                                                   identity + (_canonical(item['details']),)).rowcount)
            result['added' if added else 'updated' if changed else 'conflicts' if conflict else 'unchanged'] += 1
            if conflict and (added or changed):
                result['conflicts'] += 1
        connection.execute('COMMIT')
    except BaseException:
        connection.execute('ROLLBACK')
        raise
    actual = connection.execute('SELECT * FROM views WHERE key=?', (view['key'],)).fetchone()
    if actual is not None:
        result['digests'][key] = _digest(_view_record(store, actual))


def _content(record):
    data = dict(record)
    data['provenance'] = []
    return _digest(data)


def merge(root, records):
    api = _api(root)
    result = dict(added=0, updated=0, unchanged=0, conflicts=0, rejected=0, digests={})
    groups = {}
    for key, record in sorted(records.items()):
        match = _META.fullmatch(key) or _VIEW.fullmatch(key)
        if match is None:
            result['rejected'] += 1
        else:
            groups.setdefault(match[1], []).append((key, record))
    for circuit, entries in sorted(groups.items()):
        path = _path(root, circuit)
        meta_key = circuit + '/0-metadata'
        created = False
        if not path.exists():
            initial = next((record for key, record in entries if key == meta_key), None)
            try:
                metadata = _validate_metadata(initial)
            except (ValueError, KeyError, TypeError):
                result['rejected'] += len(entries)
                continue
            store = api.DatasetStore(path, config=metadata['config'], signature=metadata['signature'])
            created = True
        else:
            # No config/signature arguments: do not migrate an existing database.
            store = api.DatasetStore(path)
        try:
            with store.writer():
                for key, record in entries:
                    try:
                        if key == meta_key:
                            incoming = _validate_metadata(record)
                            current = _metadata(store)
                            if created:
                                connection = store.connection()
                                connection.execute('BEGIN IMMEDIATE')
                                try:
                                    for name, value in incoming.items():
                                        store.set_meta(name, value)
                                    connection.execute('COMMIT')
                                except BaseException:
                                    connection.execute('ROLLBACK')
                                    raise
                                result['added'] += 1
                            elif current == incoming:
                                result['unchanged'] += 1
                            else:
                                # Metadata controls physical compatibility; preserve the target boundary.
                                result['conflicts'] += 1
                            result['digests'][key] = _digest({'kind': 'metadata', 'metadata': _metadata(store)})
                        else:
                            _merge_view(api, store, key, record, result)
                    except (ValueError, KeyError, TypeError, zlib.error):
                        result['rejected'] += 1
                        match = _VIEW.fullmatch(key)
                        if match:
                            actual = store.connection().execute(
                                'SELECT * FROM views WHERE key=?', (match[3],)).fetchone()
                            if actual is not None:
                                result['digests'][key] = _digest(_view_record(store, actual))
        finally:
            store.close()
    return result
