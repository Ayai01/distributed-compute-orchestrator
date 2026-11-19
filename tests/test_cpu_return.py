import threading
from types import SimpleNamespace
import unittest
from unittest.mock import Mock

from remote_compute import RemoteError
from remote_compute.leases import CpuLease
from remote_compute.worker import Worker


class CpuReturnTests(unittest.TestCase):
    def test_locally_usable_width_is_not_a_confirmed_cpu_return(self):
        for active in (0, 1):
            with self.subTest(active=active):
                client = Mock(url='http://127.0.0.1:1234')
                rejected = RemoteError('Insufficient RAM for active workers; existing grant retained')
                rejected.status_code = 409
                client.resize.side_effect = [rejected, {'cpus': active, 'memory_mb': 4096}]
                client.status.return_value = dict(status='running', cpus=8, memory_mb=4096,
                                                  memory_phase={'memory_mb': 4096})
                lease = CpuLease(client, 'a'*32, max_cpus=8, cpus=8, memory_mb=4096)
                self.assertEqual(lease.refresh(active_cpus=active, ready_cpus=0), active)
                self.assertTrue(lease.resize_pending)
                lease.updated = float('-inf')
                self.assertEqual(lease.refresh(active_cpus=active, ready_cpus=0), active)
                self.assertEqual(client.resize.call_count, 2)
                self.assertFalse(lease.resize_pending)

    def test_worker_returns_idle_cpus_under_an_incumbent_ram_grant(self):
        job = dict(task=dict(cpus=8, max_cpus=12, memory_mb=4096),
                   result=dict(status='running', cpus=8, memory_mb=4096,
                               active_cpus=8, memory_phase={'memory_mb': 4096}),
                   cancel=threading.Event(), rss_mb=3600, private_mb=3600,
                   memory_observation=SimpleNamespace(envelope=(3600, 3600), active_cpus=8))
        worker = SimpleNamespace(condition=threading.Condition(), jobs={'a'*32: job},
                                 stopping=False, used=8, memory_capacity=16384,
                                 pressure_guard=SimpleNamespace(state={'limited': False}),
                                 _memory_state=lambda: {'memory_available_mb': 0},
                                 _cpu_capacity=lambda: 24, _save=Mock())
        result = Worker.resize(worker, 'a'*32, 1, active_cpus=1, min_cpus=0,
                               memory_mb=4096, memory_budget_mb=3000)
        self.assertEqual((result['cpus'], result['memory_mb'], worker.used), (1, 4096, 1))
        self.assertEqual(job['result']['active_cpus'], 1)
