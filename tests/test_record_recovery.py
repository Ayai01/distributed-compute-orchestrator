"""Retained task reads and crash recovery preserve live control traffic."""
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import threading
import unittest
from unittest.mock import patch

from remote_compute import Pool
from remote_compute.bundle import atomic_json
from remote_compute.receipts import read_receipt
from remote_compute.records import Job, JobRecords
from test_center import center
from test_compute import provider, scratch


def retained(root, identity, *, terminal=True, damaged=False):
    directory = Path(root) / 'jobs' / identity
    directory.mkdir(parents=True)
    receipt = dict(id=identity, status='succeeded' if terminal else 'queued',
                   request_sha256='b' * 64, assets={'source': 'a' * 64},
                   project='fixture', label='original', cpu_seconds=2.5)
    atomic_json(directory / 'admission.json', receipt)
    if damaged:
        (directory / 'result.json').write_bytes(b'\x00broken receipt')
    else:
        atomic_json(directory / 'result.json', receipt)
    (directory / 'original.ckpt').write_bytes(b'original paid checkpoint')
    (Path(root) / 'history.index.jsonl').write_text(json.dumps(dict(
        id=identity, terminal=terminal, assets=['a' * 64])) + '\n', encoding='utf-8')
    return directory


class RecordRecoveryTests(unittest.TestCase):
    def test_center_isolates_damaged_pending_receipt_on_restart(self):
        with scratch() as folder, provider(Path(folder) / 'worker') as (_, worker):
            root = Path(folder) / 'center'
            identity = '1' * 32
            directory = retained(root, identity, terminal=False, damaged=True)
            with Pool([worker]) as pool, center(pool, root) as (_, client):
                result = client.status(identity)
                self.assertEqual(result['error_kind'], 'receipt_corrupt')
                self.assertEqual(result['request_sha256'], 'b' * 64)
                self.assertIsNone(result['cpu_seconds'])
                self.assertEqual((directory / 'original.ckpt').read_bytes(), b'original paid checkpoint')

    def test_center_cold_status_recovers_original_damaged_receipt_once(self):
        with scratch() as folder, provider(Path(folder) / 'worker') as (_, worker):
            root = Path(folder) / 'center'
            identity = '2' * 32
            directory = retained(root, identity, damaged=True)
            with Pool([worker]) as pool, center(pool, root) as (_, client):
                result = client.status(identity)
                self.assertEqual(result['error_kind'], 'receipt_corrupt')
                self.assertTrue(result['cost_incomplete'])
                self.assertIsNone(result['cpu_seconds'])
                self.assertEqual(client.status(identity), result)
                self.assertEqual(len(list(directory.glob('result.corrupt-*.json'))), 1)

    def test_worker_cold_status_does_not_hold_global_control_lock(self):
        entered, release = threading.Event(), threading.Event()
        with scratch() as folder:
            identity = '3' * 32
            retained(folder, identity)
            def slow_read(path, **options):
                if Path(path).parent.name == identity:
                    entered.set()
                    release.wait(3)
                return read_receipt(path, **options)
            with provider(folder) as (worker, client), \
                    patch('remote_compute.records.read_receipt', side_effect=slow_read), \
                    ThreadPoolExecutor(max_workers=1) as executor:
                reading = executor.submit(client.status, identity)
                try:
                    self.assertTrue(entered.wait(2))
                    available = worker.condition.acquire(blocking=False)
                    if available:
                        worker.condition.release()
                    self.assertTrue(available, 'Cold receipt I/O held the worker control lock')
                finally:
                    release.set()
                self.assertEqual(reading.result(timeout=3)['id'], identity)

    def test_append_after_torn_index_keeps_new_accepted_identity(self):
        for tail in [b'{torn', b'\xff']:
            with self.subTest(tail=tail), scratch() as folder:
                root = Path(folder)
                index = root / 'history.index.jsonl'
                index.write_bytes(json.dumps(dict(id='old', terminal=True)).encode() + b'\n' + tail)
                records = JobRecords(root / 'jobs')
                self.assertTrue(records.load_index())
                records['accepted'] = Job(result=dict(id='accepted', status='queued', assets={}))
                restored = JobRecords(root / 'jobs')
                self.assertTrue(restored.load_index())
                self.assertIn('accepted', restored.pending)
                self.assertIn('old', restored.history)

    def test_latest_reused_identity_has_one_asset_reference_after_restart(self):
        with scratch() as folder:
            identity = '4' * 32
            directory = retained(folder, identity)
            records = JobRecords(Path(folder) / 'jobs')
            self.assertTrue(records.load_index())
            del records[identity]
            result = dict(id=identity, status='succeeded', cpu_seconds=9.25,
                          assets={'source': 'c' * 64})
            atomic_json(directory / 'result.json', result)
            job = Job(result=result, directory=directory, cancel=threading.Event())
            records[identity] = job
            records.archive(job)
            restored = JobRecords(Path(folder) / 'jobs')
            self.assertTrue(restored.load_index())
            self.assertEqual(restored[identity]['result']['cpu_seconds'], 9.25)
            self.assertEqual(dict(restored.assets), {'c' * 64: 1})
            self.assertEqual(len(restored), 1)

    def test_center_status_during_delete_does_not_recreate_receipt(self):
        entered, release = threading.Event(), threading.Event()
        with scratch() as folder, provider(Path(folder) / 'worker') as (_, worker):
            root = Path(folder) / 'center'
            identity = '5' * 32
            directory = retained(root, identity)
            from remote_compute.cleanup import remove_owned
            def slow_remove(path):
                remove_owned(path)
                entered.set()
                release.wait(3)
            with Pool([worker]) as pool, center(pool, root) as (_, client), \
                    patch('remote_compute.coordinator.remove_owned', side_effect=slow_remove), \
                    ThreadPoolExecutor(max_workers=1) as executor:
                deleting = executor.submit(client.delete, identity)
                try:
                    self.assertTrue(entered.wait(2))
                    self.assertEqual(client.status(identity)['status'], 'succeeded')
                    self.assertFalse(directory.exists())
                finally:
                    release.set()
                self.assertEqual(deleting.result(timeout=3), {'deleted': identity})

    def test_startup_compaction_preserves_receipts_and_original_ids(self):
        with scratch() as folder:
            identity = '6' * 32
            directory = retained(folder, identity)
            original = (directory / 'result.json').read_bytes()
            index = Path(folder) / 'history.index.jsonl'
            with index.open('a', encoding='utf-8') as stream:
                for i in range(2048):
                    stream.write(json.dumps(dict(id='deleted-' + str(i), op='delete')) + '\n')
            before = index.stat().st_size
            records = JobRecords(Path(folder) / 'jobs')
            self.assertTrue(records.load_index())
            self.assertTrue(records.compact_index())
            self.assertLess(index.stat().st_size, before // 10)
            self.assertEqual((directory / 'result.json').read_bytes(), original)
            self.assertFalse(records.deleted)
            restored = JobRecords(Path(folder) / 'jobs')
            self.assertTrue(restored.load_index())
            self.assertEqual(set(restored), {identity})
            self.assertEqual(dict(restored.assets), {'a' * 64: 1})
            with self.assertRaises(KeyError):
                restored['deleted-0']


if __name__ == '__main__':
    unittest.main()
