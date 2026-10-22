"""Historical receipts stay intact without entering live scheduling scans."""
import gc
import json
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

from remote_compute.bundle import atomic_json
from remote_compute.records import Job, JobRecords, active_values, asset_references
from remote_compute.resources import _private_resident_mb


class RecordTests(unittest.TestCase):
    def test_linux_private_uses_resident_pages_and_reuses_one_second_sample(self):
        _private_resident_mb.cache_clear()
        with patch.object(Path, 'read_text', return_value='Private_Clean: 2048 kB\nPrivate_Dirty: 1024 kB\nShared_Clean: 999999 kB\n') as read:
            self.assertEqual(_private_resident_mb(1), 3)
            self.assertEqual(_private_resident_mb(1), 3)
            self.assertEqual(read.call_count, 1)
        _private_resident_mb.cache_clear()

    def test_history_cache_is_bounded_and_original_receipts_are_unchanged(self):
        with tempfile.TemporaryDirectory() as folder:
            store = JobRecords(folder, cache_size=2)
            for index in range(30):
                identity = '%032x' % index
                directory = Path(folder) / identity
                directory.mkdir()
                value = dict(id=identity, status='succeeded', cpu_seconds=index + .25,
                             assets={'source': 'a' * 64}, stdout='receipt-' + identity)
                atomic_json(directory / 'result.json', value)
                job = Job(result=value, directory=directory, cancel=threading.Event())
                store[identity] = job
                store.archive(job)
            self.assertEqual(len(store), 30)
            self.assertEqual(list(active_values(store)), [])
            self.assertEqual(len(store.recent), 2)
            self.assertEqual(asset_references(store), {'a' * 64})
            identity = '%032x' % 0
            before = (Path(folder) / identity / 'result.json').read_bytes()
            loaded = store[identity]
            self.assertEqual(loaded['result']['cpu_seconds'], .25)
            self.assertEqual((Path(folder) / identity / 'result.json').read_bytes(), before)
            # Evict the cache while a collector still owns the loaded object.
            store['%032x' % 1]; store['%032x' % 2]
            gc.collect()
            self.assertIs(store[identity], loaded)
            del store[identity]
            with self.assertRaises(KeyError):
                store[identity]
            store.archive(loaded)  # A late execution finalizer cannot resurrect it.
            self.assertNotIn(identity, store)

    def test_startup_history_and_active_iteration_need_no_lazy_disk_reads(self):
        store = JobRecords('unused')
        for index in range(10000):
            identity = str(index)
            job = Job(result=dict(id=identity, status='succeeded', assets={}, worker_id='w' + identity))
            store[identity] = job
            store.archive(job, startup=True)
        live = Job(result=dict(id='live', status='running', assets={'model': 'b' * 64}))
        store['live'] = live
        with patch.object(Path, 'read_text', side_effect=AssertionError('history read in hot path')):
            self.assertEqual(list(active_values(store)), [live])
            self.assertTrue(store.knows_worker('w1234'))
            self.assertEqual(asset_references(store), {'b' * 64})
        self.assertEqual(len(store.recent), 0)


if __name__ == '__main__':
    unittest.main()
