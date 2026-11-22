"""Public observations and independent worker admissions over real HTTP."""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import sys
import threading
import time
import unittest
from unittest.mock import patch
from uuid import uuid4

from remote_compute import Client, Pool
from remote_compute import worker as worker_module
from remote_compute.bundle import task_bundle
from remote_compute.progress import diagnose
from remote_compute.liveboard import display_progress
from remote_compute.view import _progress_label
from test_center import center
from test_compute import provider, python, scratch, until


class HandoffTests(unittest.TestCase):
    def test_percentage_uses_reported_work(self):
        for job, expected in (
            (dict(progress={'percent':42.5}), .425),
            (dict(progress={'schema':'dco.progress.v1','unit':'%','completed':12}), .12),
            (dict(completed=1,total=2,step_progress=dict(completed=1,total=2)), None),
        ):
            with self.subTest(job=job):
                self.assertEqual((display_progress(job) or {}).get('ratio'), expected)
                self.assertEqual(bool(_progress_label(job)), expected is not None)
        self.assertEqual(_progress_label(dict(completed=999,total=1000)), '99%')
        self.assertEqual(_progress_label(dict(completed=1000,total=1000)), '100%')

    def test_worker_observation_reaches_public_client_before_acknowledgement(self):
        with scratch() as root, provider(Path(root)/'worker', python_runtimes={'ml':[sys.executable]}) as (worker, node), \
                Pool([node]) as pool, center(pool, Path(root)/'center') as (_, operator):
            public = Client(operator.url, operator.token, state_dir=Path(root)/'public')
            original = worker.submit
            ack, launch = threading.Event(), threading.Event()
            accepted, preparing = threading.Event(), threading.Event()
            spawn = worker._start_process
            def late_ack(*args, **kwargs):
                result = original(*args, **kwargs)
                accepted.set()
                if not ack.wait(8):
                    raise AssertionError('acceptance reply stalled')
                return result
            def late_launch(*args, **kwargs):
                preparing.set()
                if not launch.wait(8):
                    raise AssertionError('launch stalled')
                return spawn(*args, **kwargs)
            with patch.object(worker, 'submit', late_ack), patch.object(worker, '_start_process', late_launch):
                identity = public.submit(python("print('accepted once')"))['id']
                try:
                    self.assertTrue(accepted.wait(5) and preparing.wait(5))
                    pool.refresh()
                    first = public.status(identity)
                    self.assertEqual(first['dispatch_stage'], 'awaiting_worker')
                    self.assertEqual(first['worker_status'], 'running')
                    self.assertEqual(diagnose(first)['code'], 'worker_preparing')
                    pool.refresh()
                    next_row = public.statuses([identity])[identity]
                    self.assertGreater(next_row['worker_sampled_at'], first['worker_sampled_at'])
                    self.assertEqual(_progress_label(next_row), '')
                finally:
                    launch.set()
                    ack.set()
            self.assertEqual(public.wait(identity, wait_timeout=8)['status'], 'succeeded')
            self.assertEqual(public.fetch(identity)['attempts'], [])

    def test_blocked_unpack_protects_its_input_and_only_serializes_its_own_id(self):
        with scratch() as root, provider(Path(root)/'worker', cpus=2) as (worker, client):
            identity = uuid4().hex
            asset = client.sync_files({'source': b'reuse'})
            task = python("print('original')", assets={'src':asset})
            payload = task_bundle(task)
            entered, release = threading.Event(), threading.Event()
            original = worker_module.unpack
            calls = []
            def delayed(data, directory, *args, **kwargs):
                if directory.name == identity:
                    calls.append(identity)
                    entered.set()
                    if not release.wait(8):
                        raise AssertionError('test did not release original')
                return original(data, directory, *args, **kwargs)

            with patch.object(worker_module, 'unpack', delayed), ThreadPoolExecutor(3) as pool:
                first = pool.submit(client.submit_bundle, identity, payload)
                try:
                    self.assertTrue(entered.wait(5))
                    duplicate = pool.submit(client.submit_bundle, identity, payload)
                    short = pool.submit(client.submit, python("print('short')"))
                    short_id = short.result(timeout=3)['id']
                    self.assertFalse(duplicate.done())
                    self.assertIn(asset, worker._protected_assets(recent=False))
                    with self.assertRaises(worker_module.Conflict):
                        worker_module.retire_asset(worker, asset)
                finally:
                    release.set()
                self.assertEqual(first.result()['id'], duplicate.result()['id'])
                self.assertEqual(calls, [identity])
            self.assertFalse(worker.accepting_assets)
            for job_id in (short_id, identity):
                self.assertEqual(client.wait(job_id, wait_timeout=8)['status'], 'succeeded')
                client.fetch(job_id)

    def test_public_poll_observes_assets_handoff_worker_preparation_and_execution(self):
        with scratch() as root, provider(Path(root)/'worker', python_runtimes={'ml':[sys.executable]}) as (worker, node), Pool([node]) as pool, \
                center(pool, Path(root)/'center') as (owner, operator):
            client = Client(operator.url, operator.token, state_dir=Path(root)/'public')
            identity = uuid4().hex
            asset = client.sync_files({'src':b'known source'}, job_id=identity)
            releases = [threading.Event() for _ in range(4)]
            enters = [threading.Event() for _ in releases]
            def delayed(fn, index):
                def run(*args, **kwargs):
                    enters[index].set()
                    if not releases[index].wait(8):
                        raise AssertionError('test phase stalled')
                    return fn(*args, **kwargs)
                return run

            unpack = worker_module.unpack
            def unpack_input(data, directory, *args, **kwargs):
                action = delayed(unpack, 2) if directory.name == identity else unpack
                return action(data, directory, *args, **kwargs)
            with patch.object(owner, '_ensure_assets', delayed(owner._ensure_assets, 0)), \
                    patch.object(worker, 'submit', delayed(worker.submit, 1)), \
                    patch.object(worker_module, 'unpack', unpack_input), \
                    patch.object(worker, '_start_process', delayed(worker._start_process, 3)):
                try:
                    client.submit(python("print('measured')",assets={'src':asset}), job_id=identity)
                    for index, code in enumerate(('assets', 'awaiting_worker', 'unpacking', 'worker_preparing')):
                        self.assertTrue(enters[index].wait(8), (code, client.status(identity)))
                        def snapshot():
                            return client.statuses([identity])[identity]
                        if code == 'unpacking':
                            pool.refresh()
                        until(lambda: diagnose(snapshot())['code'] == code)
                        row = snapshot()
                        self.assertNotIn('assigned_node', row)
                        self.assertEqual(_progress_label(row), '')
                        if code == 'unpacking':
                            self.assertNotIn('worker_status', row)
                            pool.refresh()
                            self.assertGreater(snapshot()['worker_unpack_elapsed_seconds'], row['worker_unpack_elapsed_seconds'])
                        elif code == 'worker_preparing':
                            until(lambda: snapshot().get('worker_sampled_at',0) > row['worker_sampled_at'])
                            self.assertGreater(snapshot()['worker_preparation_elapsed_seconds'],
                                               row['worker_preparation_elapsed_seconds'])
                        else:
                            time.sleep(.12)
                        releases[index].set()
                finally:
                    for event in releases:
                        event.set()
            row = client.wait(identity, wait_timeout=10)
            self.assertEqual(row['status'], 'succeeded')
            self.assertGreaterEqual(row['asset_preparation_seconds'], .1)
            self.assertGreaterEqual(row['handoff_transport']['reply_header_seconds'], .1)
            self.assertGreaterEqual(row['handoff_seconds'], row['handoff_transport']['reply_header_seconds'])
            self.assertEqual(row['handoff_transport']['post_attempts'], 1)
            self.assertGreaterEqual(row['worker_admission_seconds'], row['bundle_unpack_seconds'])
            self.assertGreater(row['execution_started'], row['submitted'])
            self.assertGreater(row['execution_timing']['process_window_seconds'], 0)
            self.assertEqual(client.fetch(identity)['cpu_seconds'], row['cpu_seconds'])
