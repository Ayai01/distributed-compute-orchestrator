"""Scientific cache exchange preserves producer identities and observations."""

import base64
from contextlib import closing
import hashlib
import io
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import zipfile
import zlib

from remote_compute import cache_arh as arh, cache_bench as bench
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


def snapshot(root, name, value):
    path = root/'reference'/name
    path.parent.mkdir(parents=True, exist_ok=True)
    # The adapter transfers the complete archive, including auxiliary arrays.
    output = io.BytesIO()
    with zipfile.ZipFile(output, 'w') as archive:
        archive.writestr('X.npy', value)
        archive.writestr('hv_reference_F.npy', b'frozen-front')
        archive.writestr('metadata.npy', b'physical-identity')
    path.write_bytes(output.getvalue())
    return 'reference/'+name


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
            producer = SimpleNamespace(__file__=str(target/'framework/storage.py'),
                                       _owned_sqlite=lambda root, path: None)
            with patch.object(arh.importlib, 'import_module', return_value=producer):
                with patch.object(producer, 'open_owned_sqlite', create=True,
                                  side_effect=lambda root, path, legacy_schema:
                                  local_producer(root, path, legacy_schema)) as opened:
                    self.assertEqual(arh.merge(target, {key: row})['added'], 1)
                    args, kwargs = opened.call_args
                    self.assertEqual(args[0], target)
                    self.assertEqual(kwargs['legacy_schema'],
                                     {'riscv_evaluations': ('key', 'data', 'touched')})

    def test_unowned_store_is_closed_before_merge(self):
        with scratch() as directory:
            target = Path(directory).resolve()
            path = target/'store.sqlite3'
            connection = sqlite3.connect(path)
            producer = SimpleNamespace(__file__=str(target/'framework/storage.py'),
                                       open_owned_sqlite=lambda *args, **kwargs: connection)
            with patch.object(producer, '_owned_sqlite', create=True,
                              side_effect=RuntimeError('unregistered database')):
                with patch.object(arh.importlib, 'import_module', return_value=producer):
                    with self.assertRaisesRegex(RuntimeError, 'unregistered database'):
                        arh._open_store(target, path, {})
            with self.assertRaises(sqlite3.ProgrammingError):
                connection.execute('SELECT 1')


class BenchCacheTests(unittest.TestCase):
    def test_selected_complete_snapshot_missing_same_and_conflict(self):
        with scratch() as directory:
            source, target = Path(directory)/'source', Path(directory)/'target'
            key = snapshot(source, 'class-ab.npz', b'measured-designs')
            snapshot(source, 'other.npz', b'other')
            manifest = bench.manifest(source)
            records = json.loads(json.dumps(bench.export(source, [key])))
            self.assertEqual(set(records), {key})
            source_bytes = (source/key).read_bytes()
            self.assertEqual(manifest[key]['bytes'], len(source_bytes))
            first = bench.merge(target, records)
            self.assertEqual(first['added'], 1)
            self.assertEqual((target/key).read_bytes(), source_bytes)
            before = (target/key).stat().st_mtime_ns
            self.assertEqual(bench.merge(target, records)['unchanged'], 1)
            snapshot(source, 'class-ab.npz', b'different-measurements')
            conflict = bench.merge(target, bench.export(source, [key]))
            self.assertEqual((conflict['conflicts'], conflict['updated']), (1, 0))
            self.assertEqual(conflict['digests'], first['digests'])
            self.assertEqual((target/key).stat().st_mtime_ns, before)
            self.assertEqual((target/key).read_bytes(), source_bytes)
            self.assertFalse((target/'reference/other.npz').exists())
            self.assertEqual(list((target/'reference').glob('.dco-*')), [])

    def test_concurrent_publication_wins_without_replacing_target(self):
        with scratch() as directory:
            source, target = Path(directory)/'source', Path(directory)/'target'
            key = snapshot(source, 'class-ab.npz', b'source-measurement')
            def publish(pending, destination):
                destination.write_bytes(b'concurrent-bench-publication')
                raise FileExistsError(str(destination))
            with patch.object(bench.os, 'link', side_effect=publish):
                result = bench.merge(target, bench.export(source, [key]))
            self.assertEqual(result['conflicts'], 1)
            self.assertEqual((target/key).read_bytes(), b'concurrent-bench-publication')
            self.assertEqual(result['digests'][key],
                             hashlib.sha256(b'concurrent-bench-publication').hexdigest())
            self.assertEqual(list((target/'reference').glob('.dco-*')), [])

    def test_publication_failure_cleans_only_own_pending_file(self):
        with scratch() as directory:
            source, target = Path(directory)/'source', Path(directory)/'target'
            key = snapshot(source, 'class-ab.npz', b'source')
            retained = snapshot(target, 'retained.npz', b'existing')
            before = (target/retained).read_bytes()
            with patch.object(bench.os, 'link', side_effect=OSError('fixture write failure')):
                with self.assertRaises(OSError):
                    bench.merge(target, bench.export(source, [key]))
            self.assertEqual((target/retained).read_bytes(), before)
            self.assertFalse((target/key).exists())
            self.assertEqual(list((target/'reference').glob('.dco-*')), [])

    def test_invalid_snapshot_path_or_digest_is_rejected(self):
        with scratch() as directory:
            target = Path(directory)/'new'
            record = dict(digest='bad-digest', data=base64.b64encode(b'data').decode())
            records = {key: record for key in ('reference/../outside.npz',
                       'reference/valid.npz', '/reference/file.npz')}
            result = bench.merge(target, records)
            self.assertEqual(result['rejected'], 3)
            self.assertFalse(target.exists())

    def test_publisher_temporary_snapshot_is_not_exported(self):
        with scratch() as directory:
            root = Path(directory)
            key = snapshot(root, 'class-ab.npz', b'published')
            snapshot(root, 'class-ab.tmp.npz', b'partial-next-publication')
            (root/'reference/.dco-reference-pending.tmp').write_bytes(b'partial-transfer')
            self.assertEqual(set(bench.manifest(root)), {key})
            with self.assertRaises(ValueError):
                bench.export(root, ['reference/class-ab.tmp.npz'])

    def test_empty_projects_are_read_only(self):
        with scratch() as directory:
            root = Path(directory)/'absent'
            self.assertEqual(bench.manifest(root), {})
            self.assertEqual(arh.manifest(root), {})
            self.assertEqual(bench.export(root, []), {})
            self.assertEqual(arh.export(root, []), {})
            self.assertFalse(root.exists())


if __name__ == '__main__':
    unittest.main()
