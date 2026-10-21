"""Scientific-cache contracts exercised with the installed project implementations."""

import base64
import copy
import importlib.util
import json
import os
from pathlib import Path
import shutil
import struct
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
import zlib

from remote_compute import cache_aop, cache_rgcn


PROJECTS = Path(__file__).resolve().parents[2]
AOP = PROJECTS / 'AOP_v2'
RGCN = PROJECTS / 'RGCN-v2'


def scratch():
    base = Path('Z:/DCO-verify') if os.name == 'nt' and Path('Z:/').exists() else None
    if base:
        base.mkdir(exist_ok=True)
    return tempfile.TemporaryDirectory(prefix='cache-adapters-', dir=base)


class Fixture(unittest.TestCase):
    def setUp(self):
        self.temp = scratch()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.source, self.target = self.base / 'source', self.base / 'target'
        for root in (self.source, self.target):
            root.mkdir()


@unittest.skipUnless((AOP / 'analog' / 'cache.py').is_file() and importlib.util.find_spec('numpy'),
                     'AOP checkout and its NumPy runtime are optional')
class AopCacheTests(Fixture):
    environment = dict(engine_family='ngspice', major_version=47,
                       execution_protocol='ng47-transistor-v1',
                       measurement_protocol='measurements-v1', tolerance_profile='ng47-numeric-v1')

    def setUp(self):
        super().setUp()
        for root in (self.source, self.target):
            (root / 'utils').mkdir()
            shutil.copyfile(str(AOP / 'utils' / 'compat.py'), str(root / 'utils' / 'compat.py'))
        self.env = patch.dict(os.environ)
        self.env.start()
        self.addCleanup(self.env.stop)
        os.environ.pop('AOP_CACHE_DIR', None)

    def native(self, root, body, environment=None):
        env = dict(os.environ, AOP_CACHE_DIR=str(root / '.aop_cache'),
                   AOP_RECYCLE_DIR=str(root / 'recycled'), PYTHONDONTWRITEBYTECODE='1')
        code = ("from analog.cache import CornerCache\n"
                "from config import get_cache_root\n"
                "cache=CornerCache(get_cache_root(), 'science-v1', 2, memory_mb=1,environment=" +
                repr(self.environment if environment is None else environment) + ")\n"
                "try:\n" + '\n'.join('    ' + line for line in body.splitlines()) +
                "\nfinally:\n    cache.close()\n")
        return subprocess.check_output([sys.executable, '-B', '-c', code], cwd=str(AOP), env=env,
                                       text=True, stderr=subprocess.STDOUT, timeout=20).strip()

    def seed(self, root, values='[1., 2.]', environment=None):
        self.native(root, "cache.put_many([('d1','tt', " + values + ", True, {'wave':[2.,3.]})])", environment)

    def journal(self, root):
        return next((root / '.aop_cache').glob('semantic-v1/*/*/records.bin'))

    def test_native_roundtrip_retains_payload_namespace_and_samples(self):
        self.seed(self.source)
        source = cache_aop.manifest(self.source)
        self.assertEqual(len(source), 1)
        records = cache_aop.export(self.source, source)
        result = cache_aop.merge(self.target, records)
        self.assertEqual(result['added'], 1)
        self.assertEqual(cache_aop.manifest(self.target), source)
        self.assertNotEqual(self.journal(self.source).parent.name, self.journal(self.target).parent.name)
        self.assertEqual(json.loads(next(iter(source)))[2], self.environment)
        actual = json.loads(self.native(self.target,
            "import json\nv,s=cache.get('d1','tt',with_samples=True)\nprint(json.dumps([v.tolist(),s]))"))
        self.assertEqual(actual, [[1., 2.], {'wave': [2., 3.]}])

    def test_idempotence_and_conflict_preserve_valid_target(self):
        self.seed(self.source)
        self.seed(self.target, '[4., 5.]')
        before = cache_aop.manifest(self.target)
        result = cache_aop.merge(self.target, cache_aop.export(self.source, cache_aop.manifest(self.source)))
        self.assertEqual(result['conflicts'], 1)
        self.assertEqual(cache_aop.manifest(self.target), before)
        self.assertEqual(result['digests'], {key: value['digest'] for key, value in before.items()})
        same = cache_aop.merge(self.target, cache_aop.export(self.target, before))
        self.assertEqual(same['unchanged'], 1)

    def test_invalidated_aggregates_and_unfinished_tail_are_not_exported(self):
        self.native(self.source,
                    "cache.put_evaluation([b'e'*32],[3.,4.])\ncache.invalidate_evaluations()\n"
                    "cache.put_many([('d1','tt',[1.,2.],True,None)])")
        path = self.journal(self.source)
        with path.open('ab') as stream:
            stream.write(b'\x04\x00')
        before = path.read_bytes()
        records = cache_aop.export(self.source, cache_aop.manifest(self.source))
        self.assertEqual(len(records), 1)
        self.assertTrue(all(json.loads(key)[3] == 0 for key in records))
        self.assertEqual(path.read_bytes(), before)

    def test_merge_recovers_only_partial_tail_under_project_lock(self):
        self.seed(self.source)
        self.native(self.target, 'pass')
        path = self.journal(self.target)
        with path.open('ab') as stream:
            stream.write(b'\x04\x00')
        result = cache_aop.merge(self.target, cache_aop.export(self.source, cache_aop.manifest(self.source)))
        self.assertEqual(result['added'], 1)
        self.assertEqual(json.loads(self.native(self.target,
            "import json\nprint(json.dumps(cache.get('d1','tt').tolist()))")), [1., 2.])

    def test_rejects_path_escape_width_mismatch_and_infinity(self):
        self.seed(self.source)
        records = cache_aop.export(self.source, cache_aop.manifest(self.source))
        key, record = next(iter(records.items()))
        invalid = copy.deepcopy(record)
        raw = bytearray(base64.b64decode(invalid['payload']))
        raw[33:37] = struct.pack('<f', float('inf'))
        invalid['payload'] = base64.b64encode(raw).decode()
        result = cache_aop.merge(self.target, {'../escape': record, key: invalid})
        self.assertEqual(result['rejected'], 2)
        self.assertFalse((self.base / 'escape').exists())
        self.seed(self.target)
        invalid = copy.deepcopy(record)
        invalid['width'] = 1
        invalid['digest'] = cache_aop._digest(1, base64.b64decode(invalid['payload']))
        before = cache_aop.manifest(self.target)
        result = cache_aop.merge(self.target, {key: invalid})
        self.assertEqual(result['conflicts'], 1)
        self.assertEqual(cache_aop.manifest(self.target), before)

    def test_changed_payload_digest_is_rejected(self):
        self.seed(self.source)
        records = cache_aop.export(self.source, cache_aop.manifest(self.source))
        key, record = next(iter(records.items()))
        raw = bytearray(base64.b64decode(record['payload']))
        raw[33:37] = struct.pack('<f', 99.)
        record['payload'] = base64.b64encode(raw).decode()
        result = cache_aop.merge(self.target, {key: record})
        self.assertEqual(result['rejected'], 1)
        self.assertEqual(cache_aop.manifest(self.target), {})

    def test_existing_project_append_lock_is_respected(self):
        self.seed(self.source)
        self.seed(self.target, '[4.,5.]')
        directory = self.target / '.aop_cache'
        unit = self.journal(self.target).parent
        coordination = cache_aop._coordination(self.target, directory)
        records = cache_aop.export(self.source, cache_aop.manifest(self.source))
        before = cache_aop.manifest(self.target)
        with coordination.lock('append:' + os.path.normcase(str(unit.resolve()))):
            with self.assertRaises(TimeoutError):
                cache_aop.merge(self.target, records)
        self.assertEqual(cache_aop.manifest(self.target), before)

    def test_dependent_aggregate_stays_local_after_corner_refresh(self):
        self.native(self.source,
                    "cache.put_many([('d1','tt',[1.,2.],True,None)])\n"
                    "cache.put_evaluation([b'e'*32],[3.,4.])")
        self.native(self.target,
                    "cache.put_many([('d1','tt',[5.,6.],True,None)])\n"
                    "cache.put_evaluation([b'e'*32],[9.,10.])\ncache.invalidate_evaluations()")
        source = cache_aop.manifest(self.source)
        self.assertEqual(len(source), 1)
        self.assertTrue(all(json.loads(key)[3] == 0 for key in source))
        result = cache_aop.merge(self.target, cache_aop.export(self.source, source))
        self.assertEqual(result['conflicts'], 1)
        self.assertEqual(self.native(self.target, "print(cache.get_evaluation(b'e'*32))"), 'None')
        aggregate = b'\x01' + b'e' * 32 + struct.pack('<ff', 3., 4.)
        contract = cache_aop._identity(next(iter(source)))
        result = cache_aop.merge(self.target, {
            cache_aop._key(contract, aggregate): cache_aop._record(2, aggregate)})
        self.assertEqual(result['rejected'], 1)

    def test_readable_protocols_keep_distinct_units_and_only_data_is_digested(self):
        changed = dict(self.environment, measurement_protocol='measurements-v2')
        self.seed(self.source)
        self.seed(self.source, '[5., 6.]', changed)
        with patch.object(cache_aop.hashlib, 'sha256', wraps=cache_aop.hashlib.sha256) as digest:
            source = cache_aop.manifest(self.source)
            records = cache_aop.export(self.source, source)
            self.assertEqual(len(source), 2)
            self.assertTrue(all(json.loads(key)[2] in (self.environment, changed) for key in source))
            for call in digest.call_args_list:
                self.assertTrue(call.args[0].startswith(struct.pack('<I', 2) + b'\0'))
        result = cache_aop.merge(self.target, records)
        self.assertEqual(result['added'], 2)
        self.assertEqual(cache_aop.manifest(self.target), source)
        self.assertEqual(json.loads(self.native(self.target,
            "import json\nprint(json.dumps(cache.get('d1','tt').tolist()))", changed)), [5., 6.])

    def test_unknown_and_unvalidated_legacy_observations_are_not_reusable(self):
        for environment in (dict(self.environment, major_version='unknown'),
                            dict(self.environment, measurement_protocol='legacy-unvalidated')):
            self.seed(self.source, environment=environment)
        self.assertEqual(cache_aop.manifest(self.source), {})
        legacy = self.source / '.aop_cache' / ('fp32-v5-' + 'a' * 64)
        legacy.mkdir()
        shutil.copyfile(self.journal(self.source), legacy / 'records.bin')
        self.assertEqual(cache_aop.manifest(self.source), {})

    def test_engine_hash_and_platform_environment_fields_are_rejected(self):
        self.seed(self.source)
        records = cache_aop.export(self.source, cache_aop.manifest(self.source))
        key, record = next(iter(records.items()))
        for value in ('sha256-' + 'a' * 64, 'a' * 64, 'linux-native-v1'):
            parts = json.loads(key)
            parts[2]['execution_protocol'] = value
            invalid = json.dumps(parts, sort_keys=True, separators=(',', ':'))
            result = cache_aop.merge(self.target, {invalid: record})
            self.assertEqual(result['rejected'], 1)
        self.assertEqual(cache_aop.manifest(self.target), {})

    def test_semantic_directory_must_match_descriptor(self):
        self.seed(self.source)
        descriptor = self.journal(self.source).parent / 'contract.json'
        value = json.loads(descriptor.read_text(encoding='utf-8'))
        value['environment']['major_version'] = 48
        descriptor.write_text(json.dumps(value), encoding='utf-8')
        with self.assertRaisesRegex(ValueError, 'directory differs'):
            cache_aop.manifest(self.source)


@unittest.skipUnless((RGCN / 'src' / 'dataset_store.py').is_file(), 'RGCN checkout is optional')
class RgcnCacheTests(Fixture):
    def setUp(self):
        super().setUp()
        for root in (self.source, self.target):
            (root / 'src').mkdir()
            for name in ('dataset_store.py','dataset_payload.py'):
                if (RGCN/'src'/name).is_file():
                    shutil.copyfile(str(RGCN/'src'/name),str(root/'src'/name))
        self.api = cache_rgcn._api(self.source)
        self.signature = 'a' * 64
        self.legacy = 'b' * 64
        self.config = {'circuit': {'simulation': {'corner': 'tt', 'temperature_c': 27, 'vdd': 1.8},
                                   'objectives': [{'name': 'gain'}], 'metric_policy': []}}

    def store(self, root, signature=None):
        return self.api.DatasetStore(root / 'datasets' / 'toy' / 'observations.sqlite3',
                                     config=self.config, signature=signature or self.signature)

    def seed(self, root, x=1, role='test', source='original', gain=20, signature=None):
        store = self.store(root, signature)
        try:
            row = {'id': 'original-record-' + str(x), 'candidate': {'w': x}, 'corner': {'corner': 'tt'},
                   'metrics': {'gain': gain}, 'analyses': {'summary': {'status': 'complete'}},
                   'seconds': .1, 'fidelity': 'pre'}
            store.add(row, role=role, family='family-' + str(x), stratum='physical',
                      source=source, details={'geometry': 'retained'})
        finally:
            store.close()

    def transfer(self):
        source = cache_rgcn.manifest(self.source)
        return cache_rgcn.merge(self.target, cache_rgcn.export(self.source, source))

    def test_shared_layout_payload_transfers_without_layout_database(self):
        store = self.store(self.source)
        try:
            store.payload_codec = 'shared-json-layout-v1'
            store.set_meta('payload_codec', store.payload_codec)
            row = {'candidate': {'w': 1}, 'metrics': {'gain': 20},
                   'analyses': {'summary': {'status': 'complete'},
                       'dense': {'long_device_name_' + str(j): {
                           'long_parameter_' + str(k): (j + 1) * (k + 1) * .123456789012345
                           for k in range(27)} for j in range(50)}}}
            store.add(row, role='test', family='held-out', source='native-original')
            self.assertTrue(any(row[0].startswith(self.api.dataset_payload.MAGIC)
                                for row in store.connection().execute('SELECT payload FROM views')))
            before = list(store.rows())
        finally:
            store.close()
        self.assertEqual(self.transfer()['added'], 2)
        target = self.api.DatasetStore(cache_rgcn._path(self.target, 'toy'), readonly=True)
        try:
            self.assertEqual(list(target.rows()), before)
            self.assertEqual(target.connection().execute('SELECT count(*) FROM payload_layouts').fetchone()[0], 0)
        finally:
            target.close()
        self.assertEqual(self.transfer()['unchanged'], 2)

    def test_migrated_unknown_protocol_observation_keeps_identity_without_cache_hit(self):
        self.seed(self.source)
        subprocess.check_output([sys.executable, '-B', '-c',
            'import sys;from src.native_metadata_migration import migrate_store;migrate_store(sys.argv[1])',
            str(cache_rgcn._path(self.source, 'toy'))], cwd=RGCN, stderr=subprocess.STDOUT, timeout=30)
        before = cache_rgcn.manifest(self.source)
        original_key = next(key for key in before if '/1-view/' in key)
        self.assertIn('/legacy-observation-', original_key)
        result = self.transfer()
        self.assertEqual(result['added'], 2)
        self.assertEqual(cache_rgcn.manifest(self.target), before)
        target = self.api.DatasetStore(cache_rgcn._path(self.target, 'toy'), readonly=True)
        try:
            row = next(target.rows())
            self.assertEqual(row['id'], row['dataset_identity'])
            self.assertEqual(row['id'], row['observation_id'])
            self.assertEqual(row['dataset_role'], 'test')
            self.assertEqual(row['protocol_status'], 'legacy_unknown_protocol')
            self.assertIsNone(target.cached(row['id']))
            self.assertIsNone(target.find(row['candidate'], row['corner']))
            self.assertEqual(target.signature, self.signature)
        finally:
            target.close()

    def test_unknown_observation_keeps_original_signature_after_compatible_upgrade(self):
        self.seed(self.source, signature=self.legacy)
        subprocess.check_output([sys.executable, '-B', '-c',
            'import sys;from src.native_metadata_migration import migrate_store;migrate_store(sys.argv[1])',
            str(cache_rgcn._path(self.source, 'toy'))], cwd=RGCN, stderr=subprocess.STDOUT, timeout=30)
        before = cache_rgcn.manifest(self.source)
        key = next(key for key in before if '/1-view/' in key)
        store = self.store(self.source)
        store.close()
        self.assertIn('/' + self.legacy + '/legacy-observation-', key)
        self.assertIn(key, cache_rgcn.manifest(self.source))
        result = self.transfer()
        self.assertEqual(result['added'], 2)
        self.assertIn(key, cache_rgcn.manifest(self.target))

    def test_native_roundtrip_preserves_historical_key_and_test_role(self):
        self.seed(self.source, signature=self.legacy)
        store = self.store(self.source)
        try:
            store.set_meta('acquisition_physical_identity', {'engine': 'ngspice47', 'contract': 'old-domain'})
        finally:
            store.close()
        before = cache_rgcn.manifest(self.source)
        result = self.transfer()
        self.assertEqual(result['added'], 2)
        self.assertEqual(cache_rgcn.manifest(self.target), before)
        store = self.api.DatasetStore(self.target / 'datasets' / 'toy' / 'observations.sqlite3', readonly=True)
        try:
            row = next(store.rows())
            self.assertEqual(row['dataset_role'], 'test')
            self.assertEqual(row['dataset_family'], 'family-1')
            self.assertIn('/' + self.legacy + '/', next(key for key in before if '/1-view/' in key))
            self.assertEqual(store.connection().execute('SELECT count(*) FROM provenance').fetchone()[0], 1)
        finally:
            store.close()

    def test_incremental_export_and_repeat_transfer(self):
        self.seed(self.source)
        self.transfer()
        self.seed(self.source, 2, role='validation')
        before, after = cache_rgcn.manifest(self.target), cache_rgcn.manifest(self.source)
        keys = [key for key in after if key not in before or before[key]['digest'] != after[key]['digest']]
        self.assertEqual(len(keys), 1)
        result = cache_rgcn.merge(self.target, cache_rgcn.export(self.source, keys))
        self.assertEqual(result['added'], 1)
        self.assertEqual(self.transfer()['unchanged'], 3)

    def test_conflicting_scientific_result_keeps_target(self):
        self.seed(self.source, gain=20)
        self.seed(self.target, gain=99)
        before = cache_rgcn.manifest(self.target)
        result = self.transfer()
        self.assertEqual(result['conflicts'], 1)
        self.assertEqual(cache_rgcn.manifest(self.target), before)
        self.assertEqual(result['digests'], {key: value['digest'] for key, value in before.items()})

    def test_same_design_role_conflict_does_not_relabel(self):
        self.seed(self.source, role='train')
        self.seed(self.target, role='test')
        before = cache_rgcn.manifest(self.target)
        self.assertEqual(self.transfer()['conflicts'], 1)
        self.assertEqual(cache_rgcn.manifest(self.target), before)

    def test_provenance_union_preserves_existing_details(self):
        self.seed(self.source)
        self.transfer()
        self.seed(self.source, source='second-source')
        result = self.transfer()
        self.assertEqual(result['updated'], 1)
        self.assertEqual(cache_rgcn.manifest(self.target), cache_rgcn.manifest(self.source))
        records = cache_rgcn.export(self.source, cache_rgcn.manifest(self.source))
        key = next(key for key in records if '/1-view/' in key)
        records[key]['provenance'][0]['details']['geometry'] = 'conflicting'
        records[key]['digest'] = cache_rgcn._digest(records[key])
        result = cache_rgcn.merge(self.target, {key: records[key]})
        self.assertEqual(result['conflicts'], 1)

    def test_active_writer_is_respected(self):
        self.seed(self.source)
        self.transfer()
        store = self.store(self.target)
        try:
            with store.writer():
                with self.assertRaisesRegex(RuntimeError, 'active writer'):
                    self.transfer()
        finally:
            store.close()

    def test_metadata_mismatch_does_not_expand_signature_boundary(self):
        self.seed(self.source, signature=self.legacy)
        self.seed(self.target)
        before = cache_rgcn.manifest(self.target)
        result = self.transfer()
        self.assertEqual(result['conflicts'], 1)
        self.assertEqual(result['rejected'], 1)
        self.assertEqual(cache_rgcn.manifest(self.target), before)
        self.assertEqual(set(result['digests']), {'toy/0-metadata'})

    def test_claims_batches_and_timestamps_are_not_synchronized(self):
        self.seed(self.source)
        store = self.store(self.source)
        try:
            store.connection().execute("INSERT INTO batches VALUES ('keep-local','{}','{}',1)")
            store.claim('private-claim')
        finally:
            store.close()
        before = cache_rgcn.manifest(self.source)
        self.transfer()
        store = self.store(self.target)
        try:
            self.assertEqual(store.connection().execute('SELECT count(*) FROM batches').fetchone()[0], 0)
            self.assertEqual(store.connection().execute('SELECT count(*) FROM claims').fetchone()[0], 0)
            store.connection().execute('UPDATE views SET created=created+123')
            store.connection().execute('UPDATE designs SET created=created+456')
        finally:
            store.close()
        self.assertEqual({k: v['digest'] for k, v in cache_rgcn.manifest(self.target).items()},
                         {k: v['digest'] for k, v in before.items()})

    def test_invalid_identity_and_path_escape_are_rejected(self):
        self.seed(self.source)
        self.transfer()
        records = cache_rgcn.export(self.source, cache_rgcn.manifest(self.source))
        key = next(key for key in records if '/1-view/' in key)
        records[key]['design']['candidate'] = {'w': 999}
        before = cache_rgcn.manifest(self.target)
        result = cache_rgcn.merge(self.target, {key: records[key], '../escape': {}})
        self.assertEqual(result['rejected'], 2)
        self.assertEqual(cache_rgcn.manifest(self.target), before)
        self.assertFalse((self.base / 'escape').exists())

    def test_changed_payload_digest_is_rejected(self):
        self.seed(self.source)
        self.transfer()
        records = cache_rgcn.export(self.source, cache_rgcn.manifest(self.source))
        key = next(key for key in records if '/1-view/' in key)
        records[key]['provenance'][0]['details']['geometry'] = 'tampered'
        result = cache_rgcn.merge(self.target, {key: records[key]})
        self.assertEqual(result['rejected'], 1)


if __name__ == '__main__':
    unittest.main()
