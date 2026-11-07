"""Slow provider preparation must not exhaust the caller's header deadline."""
from concurrent.futures import ThreadPoolExecutor
import gc
import json
from pathlib import Path
import threading
import time
import unittest
from unittest.mock import patch

from remote_compute import Client, Pool, RemoteError
from remote_compute.worker import Handler
from test_center import center
from test_compute import provider, python, scratch


class ArtifactPreparationTests(unittest.TestCase):
    def test_lru_eviction_before_background_load_keeps_original_preparation(self):
        with scratch() as root, provider(Path(root)/'worker') as (_, node):
            with Pool([node]) as pool, center(pool, Path(root)/'center') as (state, client):
                identities = [client.submit(python('pass'))['id'] for _ in range(2)]
                for identity in identities:
                    client.wait(identity)
                identity, other = identities
                # Exercise the same bounded LRU with one slot, using two real
                # completed jobs instead of creating 65 test transports.
                state.jobs.cache_size = 1
                entered, release = threading.Event(), threading.Event()
                original = state.artifacts
                preparations = []
                def before_load(job_id, **options):
                    if options.get('cache_only'):
                        preparations.append(job_id)
                        entered.set()
                        release.wait(5)
                    return original(job_id, **options)
                with patch.object(state, 'artifacts', side_effect=before_load):
                    client.request('/jobs/'+identity+'/artifacts', binary=True,
                                   extra_headers={'X-DCO-Artifact-Poll':'1'})
                    try:
                        self.assertTrue(entered.wait(2))
                        with state.condition:
                            state.jobs[other]
                        self.assertNotIn(identity, state.jobs.recent)
                        gc.collect()
                        client.request('/jobs/'+identity+'/artifacts', binary=True,
                                       extra_headers={'X-DCO-Artifact-Poll':'1'})
                        self.assertEqual(client.connections.response_status, 202)
                    finally:
                        release.set()
                    self.assertEqual(client.fetch(identity)['status'], 'succeeded')
                self.assertEqual(preparations, [identity])
                client.fetch(other)

    def test_poll_ack_survives_header_timeout_and_renews_reconnect_window(self):
        with scratch() as root, provider(Path(root)/'worker') as (worker, node):
            with Pool([node]) as pool, center(pool, Path(root)/'center') as (state, client):
                identity = client.submit(python("print('original result')"))['id']
                client.wait(identity)
                entered, release = threading.Event(), threading.Event()
                original = pool.nodes[0].client.artifacts
                calls = []

                def slow_download(job_id):
                    calls.append(job_id)
                    entered.set()
                    release.wait(5)
                    return original(job_id)

                # Scale the production 120s header/reconnect boundary down so
                # this real HTTP test spans several complete timeout windows.
                real_max = max
                def short_window(*values):
                    if len(values) == 2 and 120 in values:
                        return .1
                    return real_max(*values)

                with patch.object(pool.nodes[0].client, 'artifacts', side_effect=slow_download), \
                        patch('remote_compute.client.max', side_effect=short_window, create=True), \
                        ThreadPoolExecutor(max_workers=2) as transfers:
                    pending = transfers.submit(client.fetch, identity, delete=False)
                    try:
                        self.assertTrue(entered.wait(2))
                        time.sleep(.4)
                        self.assertEqual(client.health(brief=True)['role'], 'scheduler')
                        self.assertEqual(client.status(identity)['status'], 'succeeded')
                        self.assertFalse(pending.done())
                        # Another reader observes the same one preparation.
                        raw = client.request('/jobs/'+identity+'/artifacts', binary=True,
                                             extra_headers={'X-DCO-Artifact-Poll':'1'})
                        self.assertEqual(client.connections.response_status, 202)
                        self.assertEqual(json.loads(raw), {'id': identity, 'status': 'preparing'})
                    finally:
                        release.set()
                    result = pending.result(timeout=4)
                self.assertEqual(result['stdout'].strip(), 'original result')
                self.assertEqual(calls, [identity])
                self.assertEqual(client.fetch(identity, delete=False), result)
                self.assertIn(identity, worker.jobs)
                client.delete(identity)

    def test_failed_preparation_ends_attempt_and_same_id_can_resume(self):
        with scratch() as root, provider(Path(root)/'worker') as (_, node):
            with Pool([node]) as pool, center(pool, Path(root)/'center') as (state, client):
                identity = client.submit(python("print('preserved')"))['id']
                client.wait(identity)
                failure = RemoteError('provider unavailable')
                failure.status_code = 403
                with patch.object(pool.nodes[0].client, 'artifacts', side_effect=failure) as download:
                    with self.assertRaisesRegex(RemoteError, 'Artifact preparation failed') as caught:
                        client.fetch(identity, delete=False)
                    self.assertEqual(caught.exception.status_code, 409)
                    self.assertEqual(download.call_count, 1)
                self.assertEqual(client.status(identity)['status'], 'succeeded')
                self.assertEqual(client.fetch(identity)['stdout'].strip(), 'preserved')

    def test_continuous_upstream_outage_exhausts_one_bounded_preparation(self):
        with scratch() as root, provider(Path(root)/'worker') as (_, node):
            with Pool([node]) as pool, center(pool, Path(root)/'center') as (_, client):
                identity = client.submit(python("print('retained')"))['id']
                client.wait(identity)
                real_max = max
                def short_window(*values):
                    return .1 if len(values) == 2 and 120 in values else real_max(*values)
                with patch.object(pool.nodes[0].client, 'artifacts', side_effect=RemoteError('provider still restarting')) as download, \
                        patch('remote_compute.client.max', side_effect=short_window, create=True):
                    started = time.monotonic()
                    with self.assertRaisesRegex(RemoteError, 'Artifact preparation failed'):
                        client.fetch(identity, delete=False)
                    self.assertLess(time.monotonic()-started, 2)
                    self.assertGreaterEqual(download.call_count, 2)
                self.assertEqual(client.status(identity)['status'], 'succeeded')
                self.assertEqual(client.fetch(identity)['stdout'].strip(), 'retained')

    def test_delete_fences_background_preparation_and_other_control_flows(self):
        with scratch() as root, provider(Path(root)/'worker') as (_, node):
            with Pool([node]) as pool, center(pool, Path(root)/'center') as (state, client):
                identity = client.submit(python('pass'))['id']
                client.wait(identity)
                entered, release = threading.Event(), threading.Event()
                original = pool.nodes[0].client.artifacts
                def delayed(job_id):
                    entered.set()
                    release.wait(5)
                    return original(job_id)
                with patch.object(pool.nodes[0].client, 'artifacts', side_effect=delayed), \
                        ThreadPoolExecutor(max_workers=1) as transfers:
                    client.request('/jobs/'+identity+'/artifacts', binary=True,
                                   extra_headers={'X-DCO-Artifact-Poll':'1'})
                    self.assertTrue(entered.wait(2))
                    deletion = transfers.submit(Client(client.url).delete, identity)
                    try:
                        time.sleep(.1)
                        self.assertFalse(deletion.done())
                        self.assertEqual(client.health(brief=True)['role'], 'scheduler')
                    finally:
                        release.set()
                    self.assertEqual(deletion.result(timeout=3)['deleted'], identity)
                self.assertNotIn(identity, state.jobs)

    def test_final_full_receipt_replaces_stale_compact_step_progress(self):
        with scratch() as root, provider(Path(root)/'worker') as (_, node):
            with Pool([node]) as pool, center(pool, Path(root)/'center') as (state, client):
                identity = client.submit(python('pass'))['id']
                client.wait(identity)
                remote = node.status(identity)
                self.assertNotIn('completed', remote)
                with state.condition:
                    job = state.jobs[identity]
                    job['result'].update(completed=0, total=1)
                    state._update(job, remote)
                self.assertEqual(client.status(identity)['completed'], 1)
                self.assertEqual(client.status(identity)['total'], 1)
                # Explicit executor progress keeps its own useful work units.
                with state.condition:
                    state._update(job, dict(remote, completed=32, total=32))
                self.assertEqual(client.status(identity)['completed'], 32)
                # Scientific work counters and command-step counters have
                # different units; omit obsolete compact fields when a full
                # receipt instead explicitly reports NFE/current/progress.
                for counter in ({'nfe': 50}, {'current': 50}, {'progress': .5}):
                    with state.condition:
                        job['result'].update(completed=0, total=1)
                        state._update(job, dict(remote, **counter))
                    self.assertNotIn('completed', client.status(identity))
                    for key, value in counter.items():
                        self.assertEqual(client.status(identity)[key], value)
                client.fetch(identity)
