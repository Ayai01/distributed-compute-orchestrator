"""Default budget, live OS headroom, and reservation lifetime contracts."""
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from remote_compute.worker import Worker
from remote_compute.client import Node, Pool
from remote_compute.memory import MemoryObservation,PressureGuard
from test_compute import provider, python, scratch, until
from test_center import center

GIB=1024


class MemoryPolicyTests(unittest.TestCase):
    def test_configured_commit_reserve_drives_pressure_admission(self):
        host=dict(memory_total_mb=48*GIB,host_memory_available_mb=20*GIB,
            memory_headroom_mb=0,host_commit_available_mb=1800,memory_commit_headroom_mb=2000)
        self.assertTrue(PressureGuard().update(host,now=0)['critical'])
        host['memory_commit_headroom_mb']=1000
        self.assertFalse(PressureGuard().update(host,now=0)['critical'])

    def test_private_usage_is_cleared_before_starting_the_next_command(self):
        from remote_compute import command
        from remote_compute.process import Process
        with scratch()as root,provider(root,cpus=1)as(worker,client):
            snapshots=[]
            def start(*args,**kwargs):
                current=next(iter(worker.jobs.values()))
                snapshots.append(current.get('private_mb'))
                return Process(*args,**kwargs)
            task=command(['${tool:python}','-c','import time;time.sleep(.3)'],max_cpus=1)
            task['steps']*=2
            with patch('remote_compute.worker.Process',side_effect=start),patch('remote_compute.worker.process_usage',return_value=dict(rss_mb=8,private_mb=100,cpu_seconds=0)):
                result=client.run(task)
            self.assertEqual(result['status'],'succeeded',result.get('stderr'))
            self.assertEqual(snapshots,[None,None])
            self.assertEqual(result['peak_private_mb'],100)

    def test_materialized_private_commit_is_not_reserved_again_from_rss(self):
        worker=Worker.__new__(Worker);worker.host=Mock()
        worker.pressure_guard=PressureGuard()
        worker.host.sample.return_value=dict(memory_total_mb=4000,host_memory_available_mb=1200,host_commit_available_mb=1524)
        worker.memory_capacity=2000;worker.memory_reserve=0;worker.memory_headroom=100;worker.memory_budget=2000
        worker.jobs={'job':dict(result=dict(status='running'),task=dict(memory_mb=1000,memory_model=True),
                               rss_mb=400,private_mb=900,rss_sampled=True)}
        value=worker._memory_state()
        self.assertEqual(value['memory_unallocated_mb'],600)
        self.assertEqual(value['commit_unallocated_mb'],100)
        self.assertEqual(value['memory_available_mb'],400)

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
        worker.pressure_guard=PressureGuard()
        worker.host=Mock();worker.host.sample.return_value=dict(memory_total_mb=48*GIB,host_memory_available_mb=24*GIB)
        worker.memory_capacity=32*GIB;worker.memory_reserve=16*GIB
        worker.memory_headroom=8*GIB;worker.memory_budget=0
        observation=MemoryObservation(now=0)
        for now in (0,1,2):observation.observe(4*GIB,now=now)
        worker.jobs={'running':dict(result=dict(status='running'),task=dict(memory_mb=10*GIB),rss_mb=4*GIB,rss_sampled=True,memory_observation=observation)}
        state=worker._memory_state()
        self.assertEqual(state['memory_available_mb'],15*GIB)
        self.assertEqual(state['memory_reserved_mb'],5*GIB)
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
        worker.pressure_guard=PressureGuard()
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

    def test_host_preserves_per_job_commit_gap_without_cross_job_offsets(self):
        node=Node('host',SimpleNamespace(url='host'))
        node.health=dict(machine_id='host',memory_accounting='rss',
            memory_reserved_mb=4*GIB,rss_mb=4*GIB,private_mb=9*GIB,
            host_memory_available_mb=40*GIB,host_commit_available_mb=8*GIB,
            memory_headroom_mb=2*GIB,commit_unallocated_mb=5*GIB)
        pool=Pool.__new__(Pool);pool.nodes=[node]
        self.assertEqual(pool._host_memory_free(node),2*GIB)
        # A fresh dispatch has no sample yet and retains its full reservation.
        node.reserved_memory_mb=2*GIB
        self.assertEqual(pool._host_memory_free(node),0)

    def test_real_process_with_high_estimate_does_not_block_another_job(self):
        with scratch() as root,provider(root+'/worker',cpus=2,memory_mb=512,memory_headroom_mb=0) as (_,node):
            pool=Pool([dict(name='test',url=node.url,token=node.token)])
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
