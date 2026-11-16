"""Scientific cache exchange preserves producer identities and observations."""

import base64
from contextlib import closing
import hashlib
import io
import json
import os
from pathlib import Path
import sqlite3
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import zipfile
import zlib

from remote_compute.cache_assets import load_adapter
PROJECTS = Path(os.environ.get('REMOTE_COMPUTE_TEST_PROJECTS_ROOT', Path(__file__).resolve().parents[2]))
arh = load_adapter(PROJECTS/'ARH-DSE-v3', 'framework/cache_exchange.py')
dcbfs = load_adapter(PROJECTS/'DCBFS-MO', 'src/dcbfs/_runtime/cache_exchange.py')
from test_compute import scratch


def observation(store, identity, data, trials=None, touched=1):
    primary = arh.STORES[store][2]
    row = dict(zip(primary, identity))
    if trials is not None:
        row['trials'] = trials
    row['value' if store == 'neural' else 'data'] = data
    row['touched'] = touched
    return arh._key(store, row), arh._record(store, row)


def seed(root, store, identity, data, trials=None, touched=1):
    key, record = observation(store, identity, data, trials, touched)
    relative, table, unused = arh.STORES[store]
    path = root/relative
    path.parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(path)) as connection, connection:
        # Conv owns these two tables in a single transaction/database.
        for member, spec in arh.STORES.items():
            if spec[0] == relative:
                arh._create(connection, member)
        row = arh._validated(key, record)[1]
        columns = arh._columns(store)
        connection.execute('INSERT OR REPLACE INTO '+table+' VALUES ('+
                           ','.join('?' for unused in columns)+')',
                           [row[name] for name in columns])
    return key


def local_producer(root, path, schema):
    """Small producer stand-in; ownership stays at each local database."""
    connection = sqlite3.connect(path)
    connection.execute('CREATE TABLE IF NOT EXISTS _arh_cache_owner (token TEXT)')
    if not connection.execute('SELECT token FROM _arh_cache_owner').fetchone():
        connection.execute('INSERT INTO _arh_cache_owner VALUES (?)', (root.name,))
    connection.commit()
    path.with_name(path.name+'.arh-cache-owner.json').write_text(
        json.dumps(dict(token=root.name)), encoding='utf-8')
    return connection


def storage_fixture(root, *, legacy=True, wal=False):
    """Old/new producer signatures with real local ownership and SQLite."""
    storage = SimpleNamespace(__file__=str(root/'framework/storage.py'),
                              SQLITE_MARKER_SUFFIX='.arh-cache-owner.json')
    def cache_manifest(project, marker, path, kind):
        data = json.loads(marker.read_text())
        if (data['owner'] != 'arh-dse-regenerable-cache' or data['version'] != 1
                or data['kind'] != kind or data['unit'] != path.relative_to(project).as_posix()):
            raise RuntimeError('Cache ownership does not match its declared unit')
        return data
    def register(project, path, connection):
        token = hashlib.sha256(str(project).encode()).hexdigest()[:32]
        connection.execute('CREATE TABLE _arh_cache_owner (id INTEGER PRIMARY KEY, token TEXT NOT NULL)')
        connection.execute('INSERT INTO _arh_cache_owner VALUES (1,?)', (token,))
        connection.commit()
        path.with_name(path.name+storage.SQLITE_MARKER_SUFFIX).write_text(json.dumps(dict(
            owner='arh-dse-regenerable-cache', version=1, kind='sqlite',
            unit=path.relative_to(project).as_posix(), token=token)))
    def old_open(project, path):
        existed = path.exists()
        connection = sqlite3.connect(path)
        marker = path.with_name(path.name+storage.SQLITE_MARKER_SUFFIX)
        if not existed:
            register(project, path, connection)
        elif marker.exists():
            data = cache_manifest(project, marker, path, 'sqlite')
            if connection.execute('SELECT token FROM _arh_cache_owner WHERE id=1').fetchone() != (data['token'],):
                connection.close()
                raise RuntimeError('SQLite cache ownership token mismatch')
        # The old producer preserves unrecognized unmarked stores.
        return connection
    def new_open(project, path, *, legacy_schema=None):
        storage.received_schema = legacy_schema
        connection = old_open(project, path)
        marker = path.with_name(path.name+storage.SQLITE_MARKER_SUFFIX)
        if not marker.exists():
            tables = {row[0] for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")}
            exact = tables == set(legacy_schema or {})
            for table, columns in (legacy_schema or {}).items():
                exact &= tuple(row[1] for row in connection.execute('PRAGMA table_info('+table+')')) == columns
            if exact:
                if wal:
                    connection.execute('PRAGMA journal_mode=WAL')
                    connection.execute('PRAGMA wal_autocheckpoint=0')
                register(project, path, connection)
                if wal:
                    # The ownership row exists only in WAL until this writer closes.
                    with closing(sqlite3.connect(path.as_uri()+'?mode=ro&immutable=1', uri=True)) as old:
                        storage.immutable_owner_visible = bool(old.execute(
                            "SELECT 1 FROM sqlite_master WHERE name='_arh_cache_owner'").fetchone())
        return connection
    storage.open_owned_sqlite = old_open if legacy else new_open
    storage._cache_manifest = cache_manifest
    return storage


def snapshot(root, name, value):
    output = io.BytesIO()
    with zipfile.ZipFile(output, 'w') as archive:
        for member, data in [('X.npy', value), ('hv_reference_F.npy', b'frozen-front'),
                             ('metadata.npy', b'physical-identity')]:
            entry = zipfile.ZipInfo(member, date_time=(2020,1,1,0,0,0))
            archive.writestr(entry, data)
    data = output.getvalue()
    key = 'data/reference/' + hashlib.sha256(data).hexdigest() + '.npz'
    path = root/key
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return key


class ArhCacheTests(unittest.TestCase):
    def test_manifest_retains_full_keys_and_excludes_touched(self):
        with scratch() as directory:
            root = Path(directory)
            identities = ('{"design":[1,2],"seed":17,"engine":"abc"}',
                          '{"design":[1,2],"seed":18,"engine":"abc"}')
            keys = [seed(root, 'mac', (key,), '{"score":1}') for key in identities]
            before = arh.manifest(root)
            with closing(sqlite3.connect(root/arh.STORES['mac'][0])) as connection, connection:
                connection.execute('UPDATE mac_evaluations SET touched=987')
            self.assertEqual(before, arh.manifest(root))
            self.assertEqual(set(before), set(keys))
            self.assertEqual([json.loads(key)[1] for key in keys], list(identities))
            self.assertTrue(all(item['bytes'] > 0 for item in before.values()))

    def test_export_selected_rows_json_roundtrip_and_local_ownership(self):
        with scratch() as directory:
            source, target = Path(directory)/'source', Path(directory)/'target'
            mac = seed(source, 'mac', ('original-scientific-key',), '{"score":2}')
            neural_data = zlib.compress(b'{"tuple":{"__tuple__":[1,2]}}')
            neural = seed(source, 'neural', ('network-a', 'full-key'), neural_data)
            seed(source, 'riscv', ('not-requested',), '{"cycles":23}')
            for store in ('mac', 'neural'):
                path = source/arh.STORES[store][0]
                local_producer(source, path, {}).close()
            records = json.loads(json.dumps(arh.export(source, [mac, neural])))
            self.assertEqual(set(records), {mac, neural})
            self.assertNotIn('source', json.dumps(records))
            with patch.object(arh, '_open_store', side_effect=local_producer):
                outcome = arh.merge(target, records)
            self.assertEqual(outcome['added'], 2)
            self.assertEqual(outcome['digests'], {key: row['digest'] for key, row in records.items()})
            self.assertEqual(arh.export(target, records), records)
            with closing(sqlite3.connect(target/arh.STORES['neural'][0])) as connection:
                self.assertEqual(connection.execute('SELECT value FROM neural_evaluations').fetchone()[0],
                                 neural_data)
                self.assertEqual(connection.execute('SELECT token FROM _arh_cache_owner').fetchone()[0],
                                 'target')
            marker = target/(arh.STORES['mac'][0]+'.arh-cache-owner.json')
            self.assertEqual(json.loads(marker.read_text())['token'], 'target')
            self.assertFalse((target/arh.STORES['riscv'][0]).exists())
            self.assertFalse(list(target.rglob('*-wal')))

    def test_existing_observation_conflicts_keep_target_and_same_skips(self):
        with scratch() as directory:
            source, target = Path(directory)/'source', Path(directory)/'target'
            key = seed(source, 'mac', ('key',), '{"score":2}')
            seed(target, 'mac', ('key',), '{"score":1}', touched=100)
            before = arh.manifest(target)
            with patch.object(arh, '_open_store', side_effect=local_producer):
                outcome = arh.merge(target, arh.export(source, [key]))
                same = arh.merge(target, arh.export(target, [key]))
            self.assertEqual((outcome['conflicts'], outcome['updated']), (1, 0))
            self.assertEqual(outcome['digests'][key], before[key]['digest'])
            self.assertEqual(same['unchanged'], 1)
            self.assertEqual(arh.manifest(target), before)

    def test_conv_prefers_deeper_trials_and_keeps_equal_depth_conflict(self):
        with scratch() as directory:
            target = Path(directory)
            key = seed(target, 'conv-prefix', ('frozen-sampling-key',),
                       '{"trials":4,"_prefix_state":[1]}', trials=4)
            deeper = observation('conv-prefix', ('frozen-sampling-key',),
                                 '{"trials":8,"_prefix_state":[1,2]}', trials=8)
            with patch.object(arh, '_open_store', side_effect=local_producer):
                outcome = arh.merge(target, dict([deeper]))
                self.assertEqual(outcome['updated'], 1)
                self.assertEqual(arh.export(target, [key])[key]['trials'], 8)
                for trials in (4, 8):
                    conflict = observation('conv-prefix', ('frozen-sampling-key',),
                                           json.dumps(dict(trials=trials, _prefix_state=['different'])),
                                           trials=trials)
                    outcome = arh.merge(target, dict([conflict]))
                    self.assertEqual(outcome['unchanged' if trials == 4 else 'conflicts'], 1)
                    self.assertEqual(outcome['digests'][key], deeper[1]['digest'])

    def test_failed_batch_rolls_back_both_conv_tables(self):
        with scratch() as directory:
            target = Path(directory)
            seed(target, 'conv', ('existing',), '{"score":1}')
            path = target/arh.STORES['conv'][0]
            with closing(sqlite3.connect(path)) as connection, connection:
                connection.execute("CREATE TRIGGER fail_prefix BEFORE INSERT ON prefixes "
                                   "BEGIN SELECT RAISE(ABORT, 'fixture failure'); END")
            records = dict([observation('conv', ('new',), '{"score":2}'),
                            observation('conv-prefix', ('prefix',),
                                        '{"trials":2,"_prefix_state":[]}', trials=2)])
            before = arh.manifest(target)
            with patch.object(arh, '_open_store', side_effect=local_producer):
                with self.assertRaises(sqlite3.IntegrityError):
                    arh.merge(target, records)
            self.assertEqual(arh.manifest(target), before)

    def test_invalid_records_never_create_cache(self):
        with scratch() as directory:
            target = Path(directory)/'new'
            key, row = observation('mac', ('key',), '{"score":1}')
            bad_digest = dict(row, digest='0'*64)
            wrong_key, wrong_prefix = observation('conv-prefix', ('prefix',),
                                                  '{"trials":4}', trials=8)
            neural_key, neural = observation('neural', ('p', 'k'), b'not-zlib')
            records = {key: bad_digest, wrong_key: wrong_prefix, neural_key: neural,
                       '["../outside","key"]': row, '[[],"key"]': row}
            with patch.object(arh, '_open_store') as opened:
                outcome = arh.merge(target, records)
            self.assertEqual(outcome['rejected'], 5)
            opened.assert_not_called()
            self.assertFalse(target.exists())

    def test_producer_open_uses_target_root_and_exact_schema(self):
        with scratch() as directory:
            target = Path(directory).resolve()
            key, row = observation('riscv', ('full-key',), '{"cycles":10}')
            producer = storage_fixture(target, legacy=False)
            with patch.object(arh.importlib, 'import_module', return_value=producer):
                self.assertEqual(arh.merge(target, {key: row})['added'], 1)
                self.assertEqual(producer.received_schema,
                                 {'riscv_evaluations': ('key', 'data', 'touched')})

    def test_old_signature_creates_owned_store_and_preserves_existing_science(self):
        with scratch() as directory:
            target = Path(directory).resolve()
            producer = storage_fixture(target)
            key, row = observation('mac', ('full-key',), '{"score":1}')
            with patch.object(arh.importlib, 'import_module', return_value=producer):
                self.assertEqual(arh.merge(target, {key: row})['added'], 1)
                self.assertEqual(arh.merge(target, {key: row})['unchanged'], 1)
                other = observation('mac', ('full-key',), '{"score":2}')[1]
                self.assertEqual(arh.merge(target, {key: other})['conflicts'], 1)
            self.assertEqual(arh.export(target, [key])[key], row)

    def test_old_signature_does_not_adopt_unknown_store_or_replace_owner(self):
        with scratch() as directory:
            target = Path(directory).resolve()
            producer = storage_fixture(target)
            key = seed(target, 'mac', ('full-key',), '{"score":1}')
            records = arh.export(target, [key])
            with patch.object(arh.importlib, 'import_module', return_value=producer):
                with self.assertRaises(FileNotFoundError):
                    arh.merge(target, records)
            self.assertEqual(arh.export(target, [key]), records)
            self.assertFalse((target/(arh.STORES['mac'][0]+producer.SQLITE_MARKER_SUFFIX)).exists())

    def test_new_signature_verifies_adopted_owner_using_live_wal_connection(self):
        with scratch() as directory:
            target = Path(directory).resolve()
            key = seed(target, 'mac', ('full-key',), '{"score":1}')
            records = arh.export(target, [key])
            producer = storage_fixture(target, legacy=False, wal=True)
            with patch.object(arh.importlib, 'import_module', return_value=producer):
                self.assertEqual(arh.merge(target, records)['unchanged'], 1)
            self.assertFalse(producer.immutable_owner_visible)
            self.assertEqual(arh.export(target, [key]), records)

    def test_old_signature_rejects_conflicting_local_ownership(self):
        with scratch() as directory:
            target = Path(directory).resolve()
            producer = storage_fixture(target)
            key, row = observation('mac', ('full-key',), '{"score":1}')
            with patch.object(arh.importlib, 'import_module', return_value=producer):
                arh.merge(target, {key: row})
                marker = target/(arh.STORES['mac'][0]+producer.SQLITE_MARKER_SUFFIX)
                conflicting = json.loads(marker.read_text())
                conflicting['token'] = 'f'*32
                marker.write_text(json.dumps(conflicting))
                with self.assertRaisesRegex(RuntimeError, 'ownership token mismatch'):
                    arh.merge(target, {key: row})
            self.assertEqual(arh.export(target, [key])[key], row)
            self.assertEqual(json.loads(marker.read_text())['token'], 'f'*32)

    def test_cache_only_root_uses_separate_read_only_producer(self):
        with scratch() as directory:
            target = Path(directory).resolve()/'shared-cache'
            producer = Path(directory).resolve()/'source-asset'
            package = producer/'framework'
            package.mkdir(parents=True)
            (package/'__init__.py').write_text('')
            source = package/'storage.py'
            source.write_text(
                "from pathlib import Path\n"
                "from test_cache_arh_bench import storage_fixture\n"
                "_producer=storage_fixture(Path(__file__).resolve().parents[1])\n"
                "open_owned_sqlite=_producer.open_owned_sqlite\n"
                "_cache_manifest=_producer._cache_manifest\n"
                "SQLITE_MARKER_SUFFIX=_producer.SQLITE_MARKER_SUFFIX\n")
            before = source.read_bytes()
            source.chmod(0o444)
            (package/'__init__.py').chmod(0o444)
            package.chmod(0o555)
            producer.chmod(0o555)
            key, row = observation('mac', ('full-key',), '{"score":1}')
            with patch.dict(sys.modules), patch.object(sys, 'dont_write_bytecode', True):
                for name in list(sys.modules):
                    if name == 'framework' or name.startswith('framework.'):
                        del sys.modules[name]
                self.assertEqual(arh.merge(target, {key: row}, producer_root=producer)['added'], 1)
            self.assertEqual(arh.export(target, [key])[key], row)
            self.assertFalse((target/'framework').exists())
            self.assertEqual(source.read_bytes(), before)
            self.assertEqual(sorted(path.relative_to(producer).as_posix() for path in producer.rglob('*')
                                    if path.is_file()), ['framework/__init__.py', 'framework/storage.py'])

    def test_unowned_store_is_closed_before_merge(self):
        with scratch() as directory:
            target = Path(directory).resolve()
            path = target/'store.sqlite3'
            connection = sqlite3.connect(path)
            producer = SimpleNamespace(__file__=str(target/'framework/storage.py'),
                                       SQLITE_MARKER_SUFFIX='.arh-cache-owner.json',
                                       open_owned_sqlite=lambda *args, **kwargs: connection)
            with patch.object(producer, '_cache_manifest', create=True,
                              side_effect=RuntimeError('unregistered database')):
                with patch.object(arh.importlib, 'import_module', return_value=producer):
                    with self.assertRaisesRegex(RuntimeError, 'unregistered database'):
                        arh._open_store(target, path, {})
            with self.assertRaises(sqlite3.ProgrammingError):
                connection.execute('SELECT 1')


class DcbfsCacheTests(unittest.TestCase):
    def test_complete_reference_roundtrip_is_atomic_and_idempotent(self):
        with scratch() as directory:
            source, target = Path(directory)/'source', Path(directory)/'target'
            key = snapshot(source, 'front', b'paid-points')
            records = json.loads(json.dumps(dcbfs.export(source, [key])))
            self.assertEqual(dcbfs.merge(target, records)['added'], 1)
            self.assertEqual((target/key).read_bytes(), (source/key).read_bytes())
            self.assertEqual(dcbfs.merge(target, records)['unchanged'], 1)
            self.assertEqual(dcbfs.manifest(source), dcbfs.manifest(target))
            self.assertEqual(list((target/'data/reference').glob('.dco-*')), [])

    def test_hash_filename_and_payload_must_agree(self):
        with scratch() as directory:
            source, target = Path(directory)/'source', Path(directory)/'target'
            key = snapshot(source, 'front', b'paid-points')
            row = dcbfs.export(source, [key])[key]
            bad = 'data/reference/' + '0'*64 + '.npz'
            self.assertEqual(dcbfs.merge(target, {bad:row, '../escape':row})['rejected'], 2)
            self.assertFalse(target.exists())
            (source/key).write_bytes(b'corrupted')
            with self.assertRaisesRegex(ValueError, 'filename'):
                dcbfs.manifest(source)

    def test_concurrent_publication_wins_without_replacement(self):
        with scratch() as directory:
            source, target = Path(directory)/'source', Path(directory)/'target'
            key = snapshot(source, 'front', b'paid-points')
            data = (source/key).read_bytes()
            def publish(pending, destination):
                destination.write_bytes(data)
                raise FileExistsError(str(destination))
            with patch.object(dcbfs.os, 'link', side_effect=publish):
                result = dcbfs.merge(target, dcbfs.export(source, [key]))
            self.assertEqual(result['unchanged'], 1)
            self.assertEqual((target/key).read_bytes(), data)
            self.assertEqual(list((target/'data/reference').glob('.dco-*')), [])

    def test_publication_failure_reclaims_own_pending_file(self):
        with scratch() as directory:
            source, target = Path(directory)/'source', Path(directory)/'target'
            key = snapshot(source, 'front', b'paid-points')
            with patch.object(dcbfs.os, 'link', side_effect=OSError('fixture write failure')):
                with self.assertRaises(OSError):
                    dcbfs.merge(target, dcbfs.export(source, [key]))
            self.assertFalse((target/key).exists())
            self.assertEqual(list((target/'data/reference').glob('.dco-*')), [])

    def test_temporary_snapshots_are_not_exported(self):
        with scratch() as directory:
            root = Path(directory)
            key = snapshot(root, 'front', b'paid-points')
            (root/'data/reference/partial.tmp.npz').write_bytes(b'partial')
            self.assertEqual(set(dcbfs.manifest(root)), {key})
            with self.assertRaises(ValueError):
                dcbfs.export(root, ['data/reference/partial.tmp.npz'])

    def test_empty_cache_is_read_only(self):
        with scratch() as directory:
            root = Path(directory)/'absent'
            self.assertEqual(dcbfs.manifest(root), {})
            self.assertEqual(dcbfs.export(root, []), {})
            self.assertEqual(arh.manifest(root), {})
            self.assertFalse(root.exists())


if __name__ == '__main__':
    unittest.main()
