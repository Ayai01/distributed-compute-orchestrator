"""Scheduler grant writes must not hold the scheduling locks or revive an original."""
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import threading
import unittest
from unittest.mock import patch

from remote_compute import Client, Pool, coordinator
from remote_compute.view import StatusRows
from test_center import center
from test_compute import provider, python, scratch, until


HOLD = """from pathlib import Path
from time import sleep
while not Path('release').exists():
    sleep(.02)
print('original paid result')
"""


class GrantPersistenceTests(unittest.TestCase):
    def test_grant_fsync_keeps_control_available_and_cannot_overwrite_completion(self):
        self.check_stale_grant(retire=False)

    def test_stale_grant_cannot_recreate_a_retired_original(self):
        self.check_stale_grant(retire=True)

    def test_reconfirmed_identical_grant_only_updates_the_live_snapshot(self):
        with scratch() as folder:
            root = Path(folder)
            with provider(root / 'worker', cpus=2) as (_, worker), Pool([worker]) as pool, \
                    center(pool, root / 'center') as (state, client):
                original = client.submit(python(HOLD, max_cpus=2, memory_mb=64))['id']
                try:
                    until(lambda: client.status(original)['status'] == 'running', seconds=10)
                    self.assertEqual(client.resize(original, 1)['cpus'], 1)
                    with state.condition:
                        state.jobs[original]['resize_uncertain'] = True
                    self.assertEqual(client.resize(original, 1)['cpus'], 1)
                    receipt = state.jobs.root / original / 'result.json'
                    before = receipt.read_bytes()
                    with state.condition:
                        # An uncertain earlier reply normally forces one worker reconciliation.
                        state.jobs[original]['resize_uncertain'] = True
                        peer = state.jobs[original]['node'].client
                    with patch.object(peer, 'resize', wraps=peer.resize) as reconcile, \
                            patch.object(state, '_persist_grant', wraps=state._persist_grant) as persist:
                        self.assertEqual(client.resize(original, 1)['cpus'], 1)
                        self.assertEqual(client.status(original, brief=True)['cpus'], 1)
                        reconcile.assert_called_once()
                        self.assertEqual(persist.call_count, 0)
                    self.assertEqual(receipt.read_bytes(), before)
                    with state.condition:
                        self.assertNotIn('resize_uncertain', state.jobs[original])
                finally:
                    client.write_job_file(original, 'release', 'finish')
                self.assertEqual(client.wait(original, wait_timeout=6)['status'], 'succeeded')

    def test_full_and_display_health_assemble_outside_control_locks(self):
        with scratch() as folder:
            root = Path(folder)
            with provider(root / 'worker') as (_, worker), Pool([worker]) as pool, \
                    center(pool, root / 'center') as (state, operator):
                public = Client(operator.url, state.token, state_dir=root / 'public-client')
                original = operator.submit(python(HOLD, project='fixture'))['id']
                try:
                    until(lambda: operator.status(original)['status'] == 'running', seconds=10)
                    queued = operator.submit(python('pass', project='fixture'))['id']
                    until(lambda: operator.status(queued)['status'] == 'queued')
                    summarize = type(state)._health

                    def inspect_control():
                        for condition in (state.condition, pool.condition):
                            available = condition.acquire(timeout=1)
                            if available:
                                condition.release()
                            self.assertTrue(available, 'Health aggregation held a global condition lock')
                        self.assertEqual(operator.status(original, brief=True)['status'], 'running')
                        return public.health()

                    for display in (False, True):
                        with self.subTest(display=display):
                            entered, release = threading.Event(), threading.Event()

                            def slow_health(view):
                                self.assertIsNot(view, state)
                                entered.set()
                                if not release.wait(8):
                                    raise AssertionError('Test did not release health aggregation')
                                return summarize(view)

                            with patch.object(type(state), '_health', slow_health), \
                                    ThreadPoolExecutor(max_workers=2) as calls:
                                result = calls.submit(operator.health, display=display)
                                try:
                                    self.assertTrue(entered.wait(4))
                                    aggregate = calls.submit(inspect_control).result(timeout=3)
                                    self.assertEqual((aggregate['used_cpus'], aggregate['queued']), (1, 1))
                                    self.assertNotIn('nodes', aggregate)
                                    self.assertNotIn('projects', aggregate)
                                finally:
                                    release.set()
                                health = result.result(timeout=5)
                            self.assertEqual((len(health['nodes']), health['used_cpus'], health['queued']), (1, 1, 1))
                            self.assertEqual(set(health['projects']), {'fixture'})
                            project = health['projects']['fixture']
                            self.assertEqual((project['used'], project['queued']), (1, 1))
                            rows = StatusRows(health, width=140)
                            self.assertEqual([row[0] for row in rows.projects], ['fixture'])
                            self.assertCountEqual([row[0] for row in rows.records], [original[:4], queued[:4]])
                finally:
                    operator.write_job_file(original, 'release', 'finish')
                self.assertEqual(operator.wait(original, wait_timeout=6)['status'], 'succeeded')
                self.assertEqual(operator.wait(queued, wait_timeout=6)['status'], 'succeeded')

    def check_stale_grant(self, *, retire):
        with scratch() as folder:
            root = Path(folder)
            with provider(root / 'worker', cpus=2) as (_, worker), Pool([worker]) as pool, \
                    center(pool, root / 'center') as (state, client):
                original = client.submit(python(HOLD, max_cpus=2, memory_mb=64))['id']
                until(lambda: (current := client.status(original))['status'] == 'running'
                      and current['cpus'] == 2, seconds=10)
                receipt = state.jobs.root / original / 'result.json'
                marker = state.retirement.root / (original + '.json')
                entered, release = threading.Event(), threading.Event()
                writing = threading.local()
                write_json = coordinator.atomic_json
                from remote_compute import bundle
                fsync = bundle.os.fsync

                def observe(path, value, **options):
                    writing.grant = (Path(path) == receipt and value.get('status') == 'running'
                                     and value.get('cpus') == 1 and options.get('publish') is not None)
                    try:
                        return write_json(path, value, **options)
                    finally:
                        writing.grant = False

                def slow_fsync(fd):
                    if getattr(writing, 'grant', False):
                        entered.set()
                        if not release.wait(12):
                            raise AssertionError('Test did not release grant fsync')
                    return fsync(fd)

                def read():
                    for condition in (state.condition, pool.condition):
                        available = condition.acquire(timeout=1)
                        if available:
                            condition.release()
                        self.assertTrue(available, 'Grant fsync held a global condition lock')
                    self.assertEqual(client.health(brief=True)['role'], 'scheduler')
                    return client.status(original, brief=True)

                try:
                    with patch.object(coordinator, 'atomic_json', side_effect=observe), \
                            patch.object(bundle.os, 'fsync', side_effect=slow_fsync), \
                            ThreadPoolExecutor(max_workers=2) as calls:
                        resize = calls.submit(client.resize, original, 1)
                        try:
                            self.assertTrue(entered.wait(5))
                            live = calls.submit(read).result(timeout=3)
                            self.assertEqual((live['status'], live['cpus']), ('running', 1))
                            client.write_job_file(original, 'release', 'finish')
                            outcome = client.wait(original, wait_timeout=6)
                            self.assertEqual(outcome['status'], 'succeeded')
                            terminal = receipt.read_bytes()
                            self.assertEqual(json.loads(terminal)['status'], 'succeeded')
                            if retire:
                                self.assertEqual(client.delete(original), {'deleted': original})
                                self.assertEqual(client.statuses([original])[original]['status'], 'unknown')
                        finally:
                            release.set()
                        self.assertEqual(resize.result(timeout=5)['cpus'], 1)
                    if retire:
                        until(lambda: not receipt.parent.exists() and not marker.exists(), seconds=8)
                        self.assertEqual(client.statuses([original])[original]['status'], 'unknown')
                    else:
                        self.assertEqual(receipt.read_bytes(), terminal)
                        self.assertEqual(client.fetch(original)['stdout'].strip(), 'original paid result')
                finally:
                    if client.statuses([original])[original].get('status') == 'running':
                        client.write_job_file(original, 'release', 'finish')


if __name__ == '__main__':
    unittest.main()
