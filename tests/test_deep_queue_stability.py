"""Exercise the operator path while durable queue files are under load."""

from concurrent.futures import ThreadPoolExecutor
import os
from pathlib import Path
import threading
import time
import unittest
from unittest.mock import ANY, patch

from remote_compute import Client, Pool, RemoteError
from remote_compute.bundle import task_bundle, unpack
from remote_compute.relay_bulk import RelayBulkBudget
from test_center import center
from remote_compute.worker import Handler
from test_compute import provider, python, scratch, until


class DeepQueueStabilityTests(unittest.TestCase):
    def test_cancel_during_worker_unpack_cannot_launch_after_the_cancel(self):
        with scratch() as directory, provider(Path(directory) / 'worker', cpus=2) as (worker, api):
            sending = threading.Event()
            resume = threading.Event()
            identity = 'f' * 32
            payload = task_bundle(python('raise AssertionError("cancelled input ran")'))
            api.health()

            def unpack_after_cancel(*args, **options):
                sending.set()
                self.assertTrue(resume.wait(8))
                return unpack(*args, **options)

            with patch('remote_compute.worker.unpack', side_effect=unpack_after_cancel), \
                    ThreadPoolExecutor(max_workers=1) as executor:
                upload = executor.submit(api.submit_bundle, identity, payload)
                try:
                    self.assertTrue(sending.wait(5))
                    stop = api.cancel_many([identity])[identity]
                    self.assertTrue(stop['cancel_requested'])
                finally:
                    resume.set()
                self.assertEqual(upload.result(timeout=8)['status'], 'cancelled')
            result = api.status(identity)
            self.assertEqual((result['status'], result['cpu_seconds'], result['started']),
                             ('cancelled', 0, None))
            self.assertTrue(result['cancelled_before_process'])
            until(lambda: not worker.receiving_submissions)

    def test_kill_all_settles_while_worker_post_is_still_blocked(self):
        for admitted in (False, True):
            with self.subTest(admitted=admitted), scratch() as directory:
                root = Path(directory)
                with provider(root / 'worker', cpus=2) as (worker, worker_api), Pool([worker_api]) as pool:
                    with center(pool, root / 'center') as (owner, api):
                        identity = 'd' * 32
                        post = '/jobs/' + identity
                        blocked = threading.Event()
                        release = threading.Event()
                        drained = threading.Event()
                        original_submit = worker.submit
                        original_reply = Handler._reply

                        def block_submit(*args, **kwargs):
                            blocked.set()
                            try:
                                self.assertTrue(release.wait(10))
                                return original_submit(*args, **kwargs)
                            finally:
                                drained.set()

                        def block_reply(handler, value, status=200, **kwargs):
                            if handler.server.worker is worker and handler.command == 'POST' and handler.path == post:
                                blocked.set()
                                try:
                                    self.assertTrue(release.wait(10))
                                    return original_reply(handler, value, status, **kwargs)
                                finally:
                                    drained.set()
                            return original_reply(handler, value, status, **kwargs)

                        blocker = (patch.object(Handler, '_reply', block_reply) if admitted else
                                   patch.object(worker, 'submit', side_effect=block_submit))
                        with blocker:
                            try:
                                api.submit(python('import time; time.sleep(30)', retries=0), job_id=identity)
                                self.assertTrue(blocked.wait(5))
                                self.assertEqual(api.status(identity)['dispatch_stage'], 'awaiting_worker')
                                if admitted:
                                    until(lambda: worker_api.status(identity).get('execution_started'))
                                self.assertIn(identity, api.cancel_all()['pending'])
                                until(lambda: api.status(identity)['status']=='cancelled', seconds=4)
                                until(lambda: api.health()['used_cpus']==0 and pool.nodes[0].dispatch_room)
                                row = api.status(identity)
                                self.assertFalse(drained.is_set())
                                self.assertEqual(row.get('cancelled_before_submit', False), not admitted)
                                self.assertEqual(row.get('handoff_fenced', False), not admitted)
                                if not admitted:
                                    self.assertEqual(row['cpu_seconds'], 0)
                                    self.assertIn(identity, worker.handoff_cancellations)
                                    self.assertTrue(worker.handoff_cancellations_path.is_file())
                            finally:
                                release.set()
                                drained.wait(5)
                        if not admitted:
                            with self.assertRaises(RemoteError) as error:
                                worker_api.status(identity)
                            self.assertEqual(error.exception.status_code, 404)
                if not admitted:
                    with provider(root / 'worker', cpus=2) as (_, restarted):
                        with self.assertRaisesRegex(RemoteError, 'handoff was cancelled') as error:
                            restarted.submit(python('raise AssertionError("late POST started")'), job_id=identity)
                        self.assertEqual(error.exception.status_code, 409)

    def test_worker_signals_whole_batch_before_saving_a_receipt(self):
        with scratch() as directory, provider(Path(directory) / 'worker', cpus=3) as (worker, api):
            identities = [api.submit(python('import time; time.sleep(60)'))['id'] for _ in range(3)]
            until(lambda: all(worker.jobs.active[i].get('process') for i in identities))
            jobs = [worker.jobs.active[i] for i in identities]
            save = worker._save
            caller = threading.current_thread()
            verified = []

            def check(job):
                if threading.current_thread() is caller:
                    self.assertTrue(all(item['cancel'].is_set() and item['result']['kill_signal_sent']
                                        for item in jobs))
                    verified.append(job['result']['id'])
                return save(job)

            with worker.condition, patch.object(worker, '_save', side_effect=check):
                receipts = worker.cancel_many(identities)
            self.assertEqual(set(receipts), set(identities))
            self.assertEqual(verified, identities)
            self.assertTrue(all(row.get('kill_signal_sent') for row in receipts.values()))
            until(lambda: all(api.status(i)['status'] == 'cancelled' for i in identities))

    def test_deep_queue_health_and_batched_cancel(self):
        count = int(os.environ.get('DCO_STRESS_QUEUE', '128'))
        with scratch() as directory:
            root = Path(directory)
            with provider(root / 'worker', cpus=4) as (_, worker):
                relay = RelayBulkBudget([int(worker.url.rsplit(':', 1)[1])], None)
                with patch('remote_compute.client._RELAY_BULK_BUDGET', relay):
                    with Pool([dict(name='relay', client=worker)]) as pool:
                        with center(pool, root / 'center') as (owner, api):
                            running = [api.submit(python('import time; time.sleep(600)', retries=0, timeout=650))['id']
                                       for _ in range(4)]
                            until(lambda: all(s['status'] == 'running' for s in api.statuses(running).values()))
                            api.control(paused=True)
                            descriptor = python('raise AssertionError("queued work was started")', retries=0)
                            observer = Client(api.url, operator=True, operator_token=owner.operator_token)
                            timings = []
                            stop = threading.Event()

                            def observe():
                                while not stop.is_set():
                                    started = time.monotonic()
                                    health = observer.health(display=True)
                                    self.assertFalse(health['nodes'][0]['error'])
                                    timings.append(time.monotonic() - started)
                                    stop.wait(.1)

                            def submit(index):
                                identity = f'{index + 1:032x}'
                                self.assertEqual(api.submit(descriptor, job_id=identity)['id'], identity)
                                return identity

                            with ThreadPoolExecutor(max_workers=13) as executor:
                                samples = executor.submit(observe)
                                try:
                                    queued = list(executor.map(submit, range(count)))
                                    until(lambda: api.health(display=True)['queued'] == count)
                                    started = time.monotonic()
                                    with patch.object(worker, 'cancel_many', wraps=worker.cancel_many) as batch:
                                        stopped = api.cancel_all()
                                        acknowledged = time.monotonic() - started
                                        self.assertEqual(set(stopped['pending']), set(queued + running))
                                        following = submit(count)
                                        until(lambda: api.status(queued[-1])['status'] == 'cancelled', seconds=90)
                                        until(lambda: all(s['status'] == 'cancelled'
                                                          for s in api.statuses(running).values()))
                                        batch.assert_called_once_with(ANY)
                                    self.assertEqual(api.status(following)['status'], 'queued')
                                    self.assertLess(acknowledged, 2)
                                finally:
                                    stop.set()
                                samples.result()
                            ordered = sorted(timings)
                            p99 = ordered[min(len(ordered) - 1, int(len(ordered) * .99))]
                            self.assertLess(p99, 1.5)
                            print(f'queue={count} health_samples={len(timings)} health_p99_s={p99:.3f} '
                                  f'kill_ack_s={acknowledged:.3f} worker_batches=1', flush=True)

    def test_backpressure_preserves_retry_and_published_credit(self):
        with scratch() as directory:
            root = Path(directory)
            with provider(root / 'worker', cpus=2) as (_, worker), Pool([worker]) as pool:
                with center(pool, root / 'center', paused=True) as (owner, api):
                    owner.max_queued = 1
                    descriptor = python('print(1)', project='bounded', retries=0)
                    identity = 'a' * 32
                    self.assertEqual(api.submit(descriptor, job_id=identity)['id'], identity)
                    self.assertEqual(api.submit(descriptor, job_id=identity)['id'], identity)
                    with self.assertRaises(RemoteError) as error:
                        api.submit(descriptor, job_id='b' * 32)
                    self.assertEqual(error.exception.status_code, 429)
                    advice = api.admission('bounded', max_cpus=1, ready_tasks=4096, queue_ratio=0)
                    self.assertEqual(advice['submission_slots'], 0)
                    self.assertEqual(api.health()['queued'], 1)
