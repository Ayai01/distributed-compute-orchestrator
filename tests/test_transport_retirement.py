"""Retirement commits the original ID while reclamation runs independently."""

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import threading
import unittest
from unittest.mock import patch

from remote_compute import Pool, RemoteError
from remote_compute import coordinator
from test_center import center
from test_compute import provider, python, scratch, until


class TransportRetirementTests(unittest.TestCase):
    def test_fetch_keeps_result_when_the_retirement_marker_cannot_be_written(self):
        with scratch() as folder, provider(Path(folder) / 'worker') as (worker, client):
            task = python("from pathlib import Path; Path('answer').write_text('ready')", outputs=['answer'])
            identity = client.submit(task)['id']
            client.wait(identity)
            with patch.object(worker.retirement, 'mark', side_effect=OSError('Storage temporarily unavailable')):
                result = client.fetch(identity)
                self.assertEqual(result['files']['answer'], b'ready')
                self.assertTrue(result['retirement_pending'])
                self.assertEqual(client.status(identity)['status'], 'succeeded')
            self.assertEqual(client.delete(identity), {'deleted': identity})

    def test_concurrent_fetch_and_duplicate_retirements_while_disk_is_blocked(self):
        with scratch() as folder:
            root = Path(folder)
            with provider(root / 'worker', cpus=4) as (worker, remote), Pool([remote]) as pool, \
                    center(pool, root / 'center') as (state, client):
                task = python("from pathlib import Path; Path('answer').write_bytes(b'paid result')",
                              outputs=['answer'])
                identities = [client.submit(task)['id'] for _ in range(8)]
                for identity in identities:
                    self.assertEqual(client.wait(identity, wait_timeout=15)['status'], 'succeeded')
                entered, release = threading.Event(), threading.Event()
                original = coordinator.remove_owned

                def slow_remove(path):
                    if path.parent == state.jobs.root and path.name in identities:
                        entered.set()
                        if not release.wait(15):
                            raise OSError('Test did not release disk cleanup')
                    return original(path)

                def fetch(identity):
                    receipt = client.fetch(identity)
                    return receipt, client.last_fetch_timing()['delete_seconds']

                with patch.object(coordinator, 'remove_owned', side_effect=slow_remove), \
                        ThreadPoolExecutor(max_workers=8) as callers:
                    downloads = [callers.submit(fetch, identity) for identity in identities]
                    try:
                        self.assertTrue(entered.wait(5))
                        for identity, download in zip(identities, downloads):
                            receipt, seconds = download.result(timeout=5)
                            self.assertEqual(receipt['id'], identity)
                            self.assertEqual(receipt['files']['answer'], b'paid result')
                            self.assertLess(seconds, 5)
                            self.assertNotIn('retirement_pending', receipt)
                            self.assertTrue((state.retirement.root / (identity + '.json')).is_file())
                            self.assertTrue((state.jobs.root / identity).is_dir())
                        duplicates = identities * 4 + ['a' * 32, 'b' * 32]
                        results = callers.map(client.delete, duplicates, timeout=5)
                        self.assertEqual(list(results), [{'deleted': i} for i in duplicates])
                        self.assertEqual(callers.submit(client.status).result(timeout=3), [])
                        short = client.submit(python("import time; time.sleep(.5); print('dispatched')"))['id']
                        with self.assertRaises(RemoteError) as active:
                            client.delete(short)
                        self.assertEqual(active.exception.status_code, 409)
                        self.assertEqual(client.wait(short, wait_timeout=5)['stdout'].strip(), 'dispatched')
                        with self.assertRaisesRegex(ValueError, '32 lowercase hex'):
                            client.delete('invalid')
                        with self.assertRaises(RemoteError) as resubmitted:
                            client.submit(task, job_id=identities[0])
                        self.assertEqual(resubmitted.exception.status_code, 409)
                    finally:
                        release.set()
                    until(lambda: all(not (root / 'center/jobs' / i).exists()
                                      and not (worker.jobs.root / i).exists() for i in identities), seconds=8)

    def test_worker_resumes_retirement_after_restart(self):
        with scratch() as folder:
            root = Path(folder) / 'worker'
            with provider(root) as (worker, client):
                task = python("print('once')")
                identity = client.submit(task)['id']
                self.assertEqual(client.wait(identity)['status'], 'succeeded')
                attempted = threading.Event()

                def unavailable(*args):
                    attempted.set()
                    raise OSError('Cleanup storage temporarily unavailable')

                with patch.object(worker.retirement, 'reclaim', side_effect=unavailable):
                    self.assertEqual(client.delete(identity), {'deleted': identity})
                    self.assertTrue(attempted.wait(3))
                    self.assertEqual(client.delete(identity), {'deleted': identity})
                    self.assertTrue((worker.jobs.root / identity).exists())
                    self.assertTrue((worker.retirement.root / (identity + '.json')).is_file())
            with provider(root) as (worker, client):
                with self.assertRaises(RemoteError) as missing:
                    client.status(identity)
                self.assertEqual(missing.exception.status_code, 404)
                self.assertEqual(client.delete(identity), {'deleted': identity})
                until(lambda: not (worker.retirement.root / (identity + '.json')).exists())
                self.assertFalse((worker.jobs.root / identity).exists())
