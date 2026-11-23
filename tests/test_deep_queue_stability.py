"""Exercise the operator path while durable queue files are under load."""

from concurrent.futures import ThreadPoolExecutor
import os
from pathlib import Path
import threading
import time
import unittest
from unittest.mock import ANY, patch

from remote_compute import Client, Pool, RemoteError
from remote_compute.relay_bulk import RelayBulkBudget
from test_center import center
from test_compute import provider, python, scratch, until


class DeepQueueStabilityTests(unittest.TestCase):
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
