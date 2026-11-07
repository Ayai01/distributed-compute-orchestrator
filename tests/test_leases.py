"""Executor demand, rolling tails and automatic spare-CPU reuse over real HTTP."""
from pathlib import Path
import unittest
from unittest.mock import patch

from remote_compute import Client, Pool, RemoteError
from test_center import center
from test_compute import provider, python, scratch, until


HOLD = "from pathlib import Path;import time\nwhile not Path('release').exists():time.sleep(.02)"


class CpuLeaseTests(unittest.TestCase):
    def test_retained_resource_reply_reuses_funded_width_and_throttles_retries(self):
        for message in ('Insufficient memory for resource resize; existing grant retained',
                          'Center resource reply timed out; existing grant retained'):
            client=Client();error=RemoteError(message);error.status_code=409
            with self.subTest(message=message),patch.object(client,'resize',side_effect=[error,error,dict(cpus=8,memory_mb=1152)])as resize, \
                    patch.object(client,'status',return_value=dict(status='running',cpus=4,memory_mb=512)), \
                    patch('remote_compute.leases.time.monotonic',return_value=10)as clock:
                lease=client.cpu_lease('a'*32,max_cpus=8,base_memory_mb=128,memory_per_cpu_mb=128)
                self.assertEqual(lease.refresh(ready_cpus=8),3)
                self.assertEqual(lease.memory_mb,512)
                self.assertEqual(lease.refresh(ready_cpus=8),3)
                self.assertEqual(resize.call_count,1)
                clock.return_value=11
                self.assertEqual(lease.refresh(ready_cpus=8),3)
                self.assertEqual(resize.call_count,2)
                clock.return_value=12
                self.assertEqual(lease.refresh(ready_cpus=8),8)

    def test_retained_grant_does_not_fund_a_larger_memory_phase_or_missing_active_threads(self):
        for message in ('Insufficient memory for resource resize; existing grant retained',
                          'Center resource reply timed out; existing grant retained'):
            client=Client();error=RemoteError(message);error.status_code=409
            for options,observed,active in (
                    (dict(memory_mb=513),dict(status='running',cpus=2,memory_mb=512),0),
                    (dict(base_memory_mb=513,memory_per_cpu_mb=1),dict(status='running',cpus=2,memory_mb=512),0),
                    ({},dict(status='running',cpus=1,memory_mb=512),2),
                    ({},dict(status='cancelled',cpus=2,memory_mb=512),0)):
                with self.subTest(options=options,observed=observed), \
                        patch.object(client,'resize',side_effect=error),patch.object(client,'status',return_value=observed):
                    lease=client.cpu_lease('a'*32,max_cpus=8,cpus=2,**options)
                    with self.assertRaises(RemoteError)as raised:lease.refresh(active_cpus=active,ready_cpus=6)
                    self.assertIs(raised.exception,error)

    def test_other_conflicts_are_not_treated_as_retained_memory_grants(self):
        client=Client();error=RemoteError('Only a running task can change its CPU grant');error.status_code=409
        with patch.object(client,'resize',side_effect=error),patch.object(client,'status')as status:
            with self.assertRaises(RemoteError):client.cpu_lease('a'*32).refresh(ready_cpus=8)
            status.assert_not_called()

    def test_tail_returns_idle_slots_and_peer_expands_without_a_new_cpu_request(self):
        with scratch() as root, provider(Path(root)/'worker', cpus=8) as (_, worker):
            with Pool([worker]) as pool, center(pool, Path(root)/'center') as (_, client):
                ready_hold=HOLD.replace('while not',"Path('lease-started').write_text('yes')\nwhile not",1)
                def started(identity):
                    try:return client.read_job_file(identity,'lease-started')==b'yes'
                    except RemoteError as error:
                        if error.status_code==404:return False
                        raise
                first = client.submit(python(ready_hold, project='A',  max_cpus=1024, memory_mb=64))['id']
                remaining = {first}
                try:
                    until(lambda:started(first))
                    lease = client.cpu_lease(first, max_cpus=1024, cpus=1)
                    self.assertEqual(lease.refresh(ready_cpus=1024), 8)
                    second = client.submit(python(ready_hold, project='B',  max_cpus=1024, memory_mb=64))['id']
                    remaining.add(second)
                    # The remaining child keeps one CPU; seven unused slots return.
                    self.assertEqual(lease.refresh(active_cpus=1, ready_cpus=0), 1)
                    until(lambda:started(second))
                    peer = client.cpu_lease(second, max_cpus=1024, cpus=1)
                    self.assertEqual(peer.refresh(ready_cpus=1024), 7)
                    self.assertEqual(client.status(first)['cpus'] + client.status(second)['cpus'], 8)
                    client.write_job_file(first, 'release', 'done')
                    client.wait(first); client.fetch(first)
                    remaining.remove(first)
                    with patch('remote_compute.leases.time.monotonic', return_value=peer.updated+2):
                        self.assertEqual(peer.refresh(ready_cpus=1024), 8)
                finally:
                    for identity in remaining:
                        client.write_job_file(identity, 'release', 'done')
                        client.wait(identity); client.fetch(identity)

    def test_phase_memory_model_limits_useful_parallelism(self):
        with scratch() as root, provider(Path(root)/'worker', cpus=8, memory_mb=1024) as (_, worker):
            with Pool([worker]) as pool, center(pool, Path(root)/'center') as (_, client):
                identity = client.submit(python(HOLD,  max_cpus=1024, memory_mb=64))['id']
                try:
                    until(lambda: client.status(identity)['status'] == 'running')
                    lease = client.cpu_lease(identity, max_cpus=1024, cpus=1,
                                             base_memory_mb=128, memory_per_cpu_mb=128)
                    self.assertEqual(lease.refresh(ready_cpus=1024), 7)
                    self.assertEqual(lease.memory_mb, 1024)
                    self.assertEqual(lease.refresh(active_cpus=1, ready_cpus=0), 1)
                    self.assertEqual(lease.memory_mb, 256)
                finally:
                    client.write_job_file(identity, 'release', 'done')
                    client.wait(identity); client.fetch(identity)

    def test_identical_demand_polls_once_per_second_and_changed_demand_returns_now(self):
        client = Client()
        with patch.object(client, 'resize', return_value=dict(cpus=4, memory_mb=64)) as resize, \
                patch('remote_compute.leases.time.monotonic', return_value=10) as clock:
            lease = client.cpu_lease('a'*32, max_cpus=1024)
            self.assertEqual(lease.refresh(ready_cpus=32), 4)
            lease.refresh(ready_cpus=32)
            self.assertEqual(resize.call_count, 1)
            clock.return_value = 11
            lease.refresh(ready_cpus=32)
            self.assertEqual(resize.call_count, 2)
            lease.refresh(active_cpus=1, ready_cpus=0)
            self.assertEqual(resize.call_count, 3)
            self.assertEqual(resize.call_args.args, ('a'*32, 1))
            self.assertEqual(resize.call_args.kwargs['active_cpus'], 1)

    def test_invalid_active_report_does_not_send_a_resource_request(self):
        client = Client()
        lease = client.cpu_lease('a'*32, cpus=2)
        with patch.object(client, 'resize') as resize:
            for value in (-1, 3, True, 1.5):
                with self.subTest(value=value), self.assertRaises(ValueError):
                    lease.refresh(active_cpus=value)
            with self.assertRaises(ValueError):lease.refresh(ready_cpus=-1)
            resize.assert_not_called()

    def test_stable_single_thread_tail_needs_no_resource_heartbeat(self):
        client = Client()
        with patch.object(client, 'resize', return_value=dict(cpus=1, memory_mb=64)) as resize, \
                patch('remote_compute.leases.time.monotonic', return_value=10) as clock:
            lease = client.cpu_lease('a'*32)
            self.assertEqual(lease.refresh(active_cpus=1, ready_cpus=0), 1)
            clock.return_value = 100
            self.assertEqual(lease.refresh(active_cpus=1, ready_cpus=0), 1)
            self.assertEqual(resize.call_count, 1)


if __name__ == '__main__':unittest.main()
