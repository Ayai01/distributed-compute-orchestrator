"""Fixed-width work must receive its share despite a stream of small jobs."""
from pathlib import Path
import time
import unittest

from remote_compute import Pool
from test_center import center
from test_compute import provider, python, scratch, until


HOLD = "from pathlib import Path;import time\nwhile not Path('release').exists():time.sleep(.02)"


class FixedQueueFairnessTests(unittest.TestCase):
    def test_single_cpu_job_grows_and_returns_at_completed_boundaries(self):
        with scratch() as directory, provider(Path(directory)/'worker', cpus=8) as (_, worker):
            with Pool([worker]) as pool, center(pool, Path(directory)/'center') as (_, client):
                peers = [client.submit(python(HOLD, project=project, cpus=2, memory_mb=64))['id']
                         for project in ('AOP', 'ARH', 'RGCN')]
                until(lambda: all(client.status(key)['status']=='running' for key in peers))
                accepted = client.submit(python(HOLD, project='Bench', cpus=1, max_cpus=4, memory_mb=80))
                identity = accepted['id']
                until(lambda: client.status(identity)['status']=='running')
                self.assertEqual(client.status(identity)['cpus'], 1)
                grant = client.resize(identity, 4, base_memory_mb=64, memory_per_cpu_mb=16)
                self.assertEqual((grant['cpus'], grant['memory_mb']), (2, 96))
                client.write_job_file(peers[0], 'release', 'done')
                client.wait(peers[0])
                client.fetch(peers[0])
                grant = client.resize(identity, 4, base_memory_mb=64, memory_per_cpu_mb=16)
                self.assertEqual((grant['cpus'], grant['memory_mb']), (4, 128))
                grant = client.resize(identity, 1, base_memory_mb=64, memory_per_cpu_mb=16)
                self.assertEqual((grant['cpus'], grant['memory_mb']), (1, 80))
                self.assertEqual(client.status(identity)['request_sha256'], accepted['request_sha256'])
                for key in (*peers[1:], identity):
                    client.write_job_file(key, 'release', 'done')
                    client.wait(key)
                    client.fetch(key)

    def test_four_single_cpu_projects_refill_fairly_over_multiple_rounds(self):
        with scratch() as directory, provider(Path(directory)/'worker', cpus=8) as (_, worker):
            with Pool([worker]) as pool, center(pool, Path(directory)/'center') as (_, client):
                client.control(paused=True)
                submitted = {client.submit(python(HOLD, project=project, memory_mb=64))['id']:project
                             for project in ('AOP', 'ARH', 'Bench', 'RGCN') for _ in range(6)}
                collected = set()
                client.control(paused=False)
                def running():
                    rows = {project:[] for project in ('AOP', 'ARH', 'Bench', 'RGCN')}
                    for key, project in submitted.items():
                        if key not in collected and client.status(key, brief=True)['status']=='running':
                            rows[project].append(key)
                    return rows
                def balanced():
                    resources = client.health()['projects']
                    return (len(resources)==4 and all(row['used']==row['target']==2 for row in resources.values())
                            and all(len(keys)==2 for keys in running().values()))
                until(balanced)
                for _ in range(3):
                    # Provider health can lag the grants. Use original task IDs
                    # confirmed running before exercising the next refill.
                    selected = {project:keys[0] for project,keys in running().items()}
                    for key in selected.values(): client.write_job_file(key, 'release', 'done')
                    for key in selected.values():
                        client.wait(key)
                        client.fetch(key)
                        collected.add(key)
                    until(balanced)
                for key in submitted:
                    if key not in collected:
                        client.cancel(key)
                        client.wait(key)
                        client.fetch(key)

    def test_freed_cpus_accumulate_for_an_under_share_fixed_job(self):
        with scratch() as directory, provider(Path(directory)/'worker', cpus=4) as (_, worker):
            with Pool([worker]) as pool, center(pool, Path(directory)/'center') as (_, client):
                running = [client.submit(python(HOLD, project='small'))['id'] for _ in range(4)]
                until(lambda: all(client.status(key)['status']=='running' for key in running))
                client.control(paused=True)
                wide = client.submit(python(HOLD, project='wide', cpus=2))['id']
                refill = client.submit(python(HOLD, project='small'))['id']
                client.control(paused=False)

                client.write_job_file(running[0], 'release', 'done')
                client.wait(running[0])
                time.sleep(.3)
                self.assertEqual(client.status(wide)['status'], 'queued')
                self.assertEqual(client.status(refill)['status'], 'queued')
                self.assertEqual(pool.nodes[0].reserved, 3)

                client.write_job_file(running[1], 'release', 'done')
                until(lambda: client.status(wide)['status']=='running')
                self.assertEqual(client.status(wide)['cpus'], 2)
                self.assertEqual(client.status(refill)['status'], 'queued')
                for key in (*running, wide, refill):
                    client.cancel(key)
                    client.wait(key)
                    client.fetch(key)

    def test_higher_priority_work_can_use_the_freed_cpu(self):
        with scratch() as directory, provider(Path(directory)/'worker', cpus=4) as (_, worker):
            with Pool([worker]) as pool, center(pool, Path(directory)/'center') as (_, client):
                running = [client.submit(python(HOLD, project='small'))['id'] for _ in range(4)]
                until(lambda: all(client.status(key)['status']=='running' for key in running))
                client.control(paused=True)
                wide = client.submit(python(HOLD, project='wide', cpus=2))['id']
                urgent = client.submit(python(HOLD, project='small', priority=1))['id']
                client.write_job_file(running[0], 'release', 'done')
                client.wait(running[0])
                client.control(paused=False)
                until(lambda: client.status(urgent)['status']=='running')
                self.assertEqual(client.status(wide)['status'], 'queued')
                for key in (*running, wide, urgent):
                    client.cancel(key)
                    client.wait(key)
                    client.fetch(key)

    def test_memory_blocked_fixed_work_does_not_hold_cpu(self):
        code = 'data=bytearray(64*1024*1024)\n' + HOLD
        with scratch() as directory, provider(Path(directory)/'worker', cpus=4, memory_mb=128) as (_, worker):
            with Pool([worker]) as pool, center(pool, Path(directory)/'center') as (_, client):
                busy = client.submit(python(code, project='busy', memory_mb=96))['id']
                until(lambda: client.status(busy)['status']=='running')
                until(lambda: worker.health()['memory_available_mb'] < 80)
                client.control(paused=True)
                wide = client.submit(python(HOLD, project='wide', cpus=2, memory_mb=80))['id']
                small = client.submit(python(HOLD, project='small', memory_mb=32))['id']
                client.control(paused=False)
                until(lambda: client.status(small)['status']=='running')
                self.assertEqual(client.status(wide)['status'], 'queued')
                for key in (busy, wide, small):
                    client.cancel(key)
                    client.wait(key)
                    client.fetch(key)

    def test_cancelling_the_waiter_releases_the_accumulated_cpu(self):
        with scratch() as directory, provider(Path(directory)/'worker', cpus=4) as (_, worker):
            with Pool([worker]) as pool, center(pool, Path(directory)/'center') as (_, client):
                running = [client.submit(python(HOLD, project='small'))['id'] for _ in range(4)]
                until(lambda: all(client.status(key)['status']=='running' for key in running))
                client.control(paused=True)
                wide = client.submit(python(HOLD, project='wide', cpus=2))['id']
                refill = client.submit(python(HOLD, project='small'))['id']
                client.write_job_file(running[0], 'release', 'done')
                client.wait(running[0])
                client.control(paused=False)
                time.sleep(.3)
                self.assertEqual(client.status(refill)['status'], 'queued')
                client.cancel(wide)
                until(lambda: client.status(refill)['status']=='running')
                for key in (*running, wide, refill):
                    client.cancel(key)
                    client.wait(key)
                    client.fetch(key)

    def test_only_one_worker_accumulates_a_fixed_jobs_cpus(self):
        with scratch() as directory, provider(Path(directory)/'left', cpus=4) as (_, left), \
                provider(Path(directory)/'right', cpus=4) as (_, right):
            with Pool([left, right]) as pool, center(pool, Path(directory)/'center') as (_, client):
                running = [client.submit(python(HOLD, project='small'))['id'] for _ in range(8)]
                until(lambda: all(client.status(key)['status']=='running' for key in running))
                locations = {}
                for key in running:
                    locations.setdefault(client.status(key)['assigned_node'], []).append(key)
                self.assertEqual(sorted(map(len, locations.values())), [4, 4])
                client.control(paused=True)
                wide = client.submit(python(HOLD, project='wide', cpus=2))['id']
                refill = client.submit(python(HOLD, project='small'))['id']
                for keys in locations.values():
                    client.write_job_file(keys[0], 'release', 'done')
                    client.wait(keys[0])
                client.control(paused=False)
                until(lambda: client.status(refill)['status']=='running')
                self.assertEqual(client.status(wide)['status'], 'queued')
                waiting = next(node for node in pool.nodes if node.reserved==3)
                client.write_job_file(locations[waiting.name][1], 'release', 'done')
                until(lambda: client.status(wide)['status']=='running')
                self.assertEqual(client.status(wide)['assigned_node'], waiting.name)
                for key in (*running, wide, refill):
                    client.cancel(key)
                    client.wait(key)
                    client.fetch(key)
