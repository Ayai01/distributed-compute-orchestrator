"""Concurrent durable admission retains source holds and stable IDs."""
from concurrent.futures import ThreadPoolExecutor
import hashlib
from pathlib import Path
import tempfile
import threading
import unittest
import uuid

from remote_compute.acceptance import SubmissionAssets, SubmissionLocks
from remote_compute.bundle import pack, task_bundle
from remote_compute import command
from remote_compute.cleanup import AssetCache
from remote_compute.coordinator import Coordinator


class AcceptanceTests(unittest.TestCase):
    def make_center(self, root):
        center = object.__new__(Coordinator)
        center.root = root
        (root/'jobs').mkdir()
        (root/'assets').mkdir()
        center.pool = type('Pool', (), dict(nodes=[type('Node', (), dict(capacity=8))()]))()
        center.condition = threading.Condition(threading.RLock())
        center.submission_lock = SubmissionLocks()
        center.stopping = False
        center.accepting_assets = SubmissionAssets()
        center.asset_handoffs = {}
        center.jobs, center.queue = {}, []
        center.asset_cache = AssetCache(root/'assets', archives=True)
        return center

    def bundle(self, center, content):
        raw = pack({'input.txt': content})
        identity = hashlib.sha256(raw).hexdigest()
        (center.root/'assets'/(identity+'.zip')).write_bytes(raw)
        task = command(['python', '-c', 'pass'], cpus=1, memory_mb=128,
                       assets=dict(source=identity), project='test', outputs=['output.txt'])
        return task_bundle(task), identity

    def test_independent_receipt_writes_overlap_and_source_holds_survive(self):
        with tempfile.TemporaryDirectory() as name:
            center = self.make_center(Path(name))
            first, a = self.bundle(center, 'a')
            second, b = self.bundle(center, 'b')
            entered = [threading.Event(), threading.Event()]
            release = [threading.Event(), threading.Event()]
            identities = [uuid.uuid4().hex, uuid.uuid4().hex]
            original = center._save
            def delayed(job):
                index = identities.index(job['result']['id'])
                entered[index].set()
                if not release[index].wait(3):
                    raise AssertionError('An independent acceptance serialized the durable write')
                original(job)
            center._save = delayed
            with ThreadPoolExecutor(max_workers=2) as pool:
                futures = [pool.submit(center.submit, i, raw) for i, raw in zip(identities, [first, second])]
                try:
                    self.assertTrue(all(event.wait(1) for event in entered))
                    self.assertEqual(set(center.accepting_assets), {a, b})
                    release[0].set()
                    self.assertEqual(futures[0].result(timeout=1)['id'], identities[0])
                    self.assertEqual(set(center.accepting_assets), {b})
                finally:
                    for event in release:
                        event.set()
                self.assertEqual(futures[1].result(timeout=1)['id'], identities[1])
            self.assertFalse(center.accepting_assets)
            self.assertEqual(set(center.jobs), set(identities))
            self.assertEqual(len(center.queue), 2)

    def test_same_ID_is_persisted_once_and_different_payload_rejected(self):
        with tempfile.TemporaryDirectory() as name:
            center = self.make_center(Path(name))
            payload, _ = self.bundle(center, 'same')
            other, _ = self.bundle(center, 'other')
            identity = uuid.uuid4().hex
            with ThreadPoolExecutor(max_workers=16) as pool:
                results = list(pool.map(lambda _: center.submit(identity, payload), range(32)))
            self.assertTrue(all(row['id'] == identity for row in results))
            self.assertEqual(center.queue, [identity])
            self.assertEqual((center.root/'jobs'/identity/'request.zip').read_bytes(), payload)
            with self.assertRaises(ValueError):
                center.submit(identity, other)
            self.assertFalse(center.accepting_assets)
            self.assertFalse(center.submission_lock.entries)

    def test_failed_durable_write_releases_its_own_asset_hold(self):
        with tempfile.TemporaryDirectory() as name:
            center = self.make_center(Path(name))
            payload, _ = self.bundle(center, 'failure')
            def fail(job):
                raise OSError('fixture disk failure')
            center._save = fail
            with self.assertRaises(OSError):
                center.submit(uuid.uuid4().hex, payload)
            self.assertFalse(center.accepting_assets)
            self.assertFalse(center.submission_lock.entries)
            self.assertFalse(center.queue)

    def test_rolling_update_keeps_legacy_ID_on_the_original_lock(self):
        legacy = threading.RLock()
        locks = SubmissionLocks(legacy=legacy, legacy_ids=['already-posted'])
        entered_old, entered_new = threading.Event(), threading.Event()
        def take(identity, signal):
            with locks.hold(identity):
                signal.set()
        legacy.acquire()
        try:
            old = threading.Thread(target=take, args=('already-posted', entered_old))
            new = threading.Thread(target=take, args=('new-original-ID', entered_new))
            old.start()
            new.start()
            self.assertTrue(entered_new.wait(1))
            self.assertFalse(entered_old.is_set())
        finally:
            legacy.release()
        old.join(1)
        new.join(1)
        self.assertTrue(entered_old.is_set())


if __name__ == '__main__':
    unittest.main()
