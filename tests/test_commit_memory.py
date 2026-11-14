"""Admission follows Windows commit headroom without changing physical RAM facts."""
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from remote_compute import resources
from remote_compute.worker import Worker
from remote_compute.memory import PressureGuard

GIB=1024


class CommitMemoryTests(unittest.TestCase):
    def test_windows_reads_allocation_headroom_from_native_status(self):
        def sample(pointer):
            value=pointer._obj
            value.total=48*GIB*resources.MIB
            value.available=12*GIB*resources.MIB
            value.page_total=60*GIB*resources.MIB
            value.page_available=5*GIB*resources.MIB
            return 1
        api=SimpleNamespace(kernel32=SimpleNamespace(GlobalMemoryStatusEx=sample))
        with patch.object(resources.os,'name','nt'),patch.object(resources.c,'windll',api,create=True):
            self.assertEqual(resources.commit_memory(),(60*GIB,5*GIB))
            self.assertEqual(resources.memory(),(48*GIB,12*GIB))

    def test_non_windows_keeps_existing_physical_admission(self):
        with patch.object(resources.os,'name','posix'):
            self.assertIsNone(resources.commit_memory())

    def test_host_keeps_physical_and_commit_observations_separate(self):
        with patch.object(resources,'memory',return_value=(48*GIB,12*GIB)), \
                patch.object(resources,'commit_memory',return_value=(60*GIB,4*GIB)), \
                patch.object(resources,'cpu_ticks',return_value=(100,20)):
            result=resources.Host().sample()
        self.assertEqual(result['memory_total_mb'],48*GIB)
        self.assertEqual(result['host_memory_available_mb'],12*GIB)
        self.assertEqual(result['host_commit_limit_mb'],60*GIB)
        self.assertEqual(result['host_commit_available_mb'],4*GIB)

    def worker(self,commit):
        worker=Worker.__new__(Worker)
        worker.pressure_guard=PressureGuard()
        worker.host=Mock();worker.host.sample.return_value=dict(memory_total_mb=48*GIB,
            host_memory_available_mb=40*GIB,host_commit_available_mb=commit)
        worker.memory_capacity=32*GIB;worker.memory_reserve=16*GIB
        worker.memory_headroom=2*GIB;worker.memory_budget=0
        worker.jobs={}
        return worker

    def test_commit_limits_new_admission_but_preserves_existing_grants(self):
        worker=self.worker(6*GIB)
        worker.jobs={'active':dict(result=dict(status='running'),task=dict(memory_mb=10*GIB),
            rss_mb=GIB,rss_sampled=True)}
        state=worker._memory_state()
        self.assertEqual(state['memory_available_mb'],0)
        self.assertEqual(state['host_memory_available_mb'],40*GIB)
        self.assertEqual(worker.jobs['active']['task']['memory_mb'],10*GIB)

    def test_unsampled_reservations_are_counted_before_commit_is_consumed(self):
        worker=self.worker(6*GIB)
        worker.jobs={'starting':dict(result=dict(status='running'),task=dict(memory_mb=3*GIB))}
        self.assertEqual(worker._memory_state()['memory_available_mb'],2*GIB)

    def test_startup_fixed_task_keeps_future_commit_until_calibrated(self):
        worker=self.worker(12*GIB)
        job=dict(result=dict(status='running'),task=dict(memory_mb=6*GIB),
                 rss_mb=2*GIB,private_mb=GIB,rss_sampled=True)
        worker.jobs={'loading':job}
        state=worker._memory_state()
        self.assertEqual(state['memory_reserved_mb'],6*GIB)
        self.assertEqual(state['commit_unallocated_mb'],5*GIB)
        self.assertEqual(state['memory_available_mb'],6*GIB)
        job['private_mb']=7*GIB
        state=worker._memory_state()
        self.assertEqual(state['commit_unallocated_mb'],0)
        self.assertEqual(state['memory_available_mb'],11*GIB)

    def test_exhaustion_recovery_and_missing_optional_field(self):
        worker=self.worker(GIB)
        self.assertEqual(worker._memory_state()['memory_available_mb'],0)
        worker.host.sample.return_value['host_commit_available_mb']=10*GIB
        self.assertEqual(worker._memory_state()['memory_available_mb'],9*GIB)
        del worker.host.sample.return_value['host_commit_available_mb']
        self.assertEqual(worker._memory_state()['memory_available_mb'],32*GIB)


if __name__=='__main__':unittest.main()
