"""Default budget, live OS headroom, and reservation lifetime contracts."""
import unittest
from unittest.mock import Mock, patch

from remote_compute.worker import Worker
from remote_compute.client import Node, Pool
from test_compute import provider, python, scratch, until
from test_center import center

GIB=1024


class MemoryPolicyTests(unittest.TestCase):
    def test_default_budget_and_live_headroom_are_separate(self):
        sample=dict(memory_total_mb=48*GIB,host_memory_available_mb=40*GIB)
        with scratch() as root,patch('remote_compute.worker.Host.sample',return_value=sample),provider(root) as (worker,_):
            self.assertEqual(worker.memory_capacity,32*GIB)
            self.assertEqual(worker.health()['memory_headroom_mb'],8*GIB)
            sample['host_memory_available_mb']=12*GIB
            state=worker._memory_state()
            self.assertEqual((state['memory_capacity_mb'],state['memory_effective_mb'],state['memory_available_mb']),
                             (32*GIB,4*GIB,4*GIB))
            sample['host_memory_available_mb']=6*GIB
            self.assertEqual(worker._memory_state()['memory_available_mb'],0)
            sample['host_memory_available_mb']=40*GIB
            self.assertEqual(worker._memory_state()['memory_available_mb'],32*GIB)

    def test_live_headroom_uses_sampled_rss_instead_of_overestimated_requests(self):
        worker=Worker.__new__(Worker)
        worker.host=Mock();worker.host.sample.return_value=dict(memory_total_mb=48*GIB,host_memory_available_mb=24*GIB)
        worker.memory_capacity=32*GIB;worker.memory_reserve=16*GIB
        worker.memory_headroom=8*GIB;worker.memory_budget=0
        worker.jobs={'running':dict(result=dict(status='running'),task=dict(memory_mb=10*GIB),rss_mb=4*GIB,rss_sampled=True)}
        state=worker._memory_state()
        self.assertEqual(state['memory_available_mb'],16*GIB)
        self.assertEqual(state['memory_reserved_mb'],4*GIB)
        self.assertEqual(state['memory_requested_mb'],10*GIB)
        # Lowering the budget does not rewrite the task's existing grant.
        worker.memory_capacity=3*GIB
        self.assertEqual(worker._memory_state()['memory_available_mb'],0)
        self.assertEqual(worker.jobs['running']['task']['memory_mb'],10*GIB)

    def test_online_auto_budget_preserves_zero_in_saved_configuration(self):
        sample=dict(memory_total_mb=48*GIB,host_memory_available_mb=40*GIB)
        with scratch() as root,patch('remote_compute.worker.Host.sample',return_value=sample),provider(root,memory_mb=16*GIB) as (worker,_):
            worker.save_settings=Mock()
            worker.control(dict(memory_mb=0,memory_reserve_mb=16*GIB,memory_headroom_mb=8*GIB))
            self.assertEqual(worker.memory_capacity,32*GIB)
            worker.save_settings.assert_called_once_with(dict(memory_mb=0,memory_reserve_mb=16*GIB,memory_headroom_mb=8*GIB))
            with self.assertRaises(ValueError):worker.control(dict(memory_headroom_mb=True))

    def test_unsampled_process_keeps_initial_reservation(self):
        worker=Worker.__new__(Worker)
        worker.host=Mock();worker.host.sample.return_value=dict(memory_total_mb=48*GIB,host_memory_available_mb=24*GIB)
        worker.memory_capacity=32*GIB;worker.memory_reserve=16*GIB
        worker.memory_headroom=8*GIB;worker.memory_budget=0
        worker.jobs={'starting':dict(result=dict(status='running'),task=dict(memory_mb=10*GIB),rss_mb=0)}
        state=worker._memory_state()
        self.assertEqual(state['memory_reserved_mb'],10*GIB)
        self.assertEqual(state['memory_available_mb'],6*GIB)

    def test_scheduler_charges_actual_rss_and_unreported_dispatch(self):
        node=Node('probe',None)
        node.health=dict(memory_accounting='rss',memory_capacity_mb=16*GIB,
                         memory_reserved_mb=2*GIB,memory_available_mb=14*GIB,
                         active_jobs=[dict(id='running',memory_mb=12*GIB,rss_mb=2*GIB)])
        node.live_jobs={'running'};node.reserved_memory_mb=12*GIB
        self.assertEqual(node.memory_free,14*GIB)
        self.assertEqual(node.memory_accounted,2*GIB)
        node.live_jobs.add('dispatching');node.reserved_memory_mb+=4*GIB
        self.assertEqual(node.memory_free,10*GIB)
        self.assertEqual(node.memory_accounted,6*GIB)

    def test_real_process_with_high_estimate_does_not_block_another_job(self):
        with scratch() as root,provider(root+'/worker',cpus=2,memory_mb=512,memory_headroom_mb=0) as (_,node):
            pool=Pool([dict(name='test',url=node.url)])
            try:
                with center(pool,root+'/center') as (_,client):
                    first=client.submit(python("import time;print('ready',flush=True);time.sleep(30)",memory_mb=400))['id']
                    try:
                        until(lambda:'ready' in client.logs(first)['stdout'])
                        second=client.submit(python("print('admitted')",memory_mb=400))['id']
                        self.assertEqual(client.wait(second,wait_timeout=5)['status'],'succeeded')
                        self.assertEqual(client.status(first)['status'],'running')
                        self.assertIn('admitted',client.fetch(second)['stdout'])
                    finally:
                        client.cancel(first);client.wait(first);client.fetch(first)
            finally:pool.close()


if __name__=='__main__':unittest.main()
