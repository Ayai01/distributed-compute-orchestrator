"""Overestimated requests, private commit, phase peaks and recovery use real headroom."""
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock
import unittest

from remote_compute import Client, Pool, RemoteError
from remote_compute.client import Node
from remote_compute.memory import MemoryObservation, reservation
from remote_compute.worker import Worker
from test_compute import provider, python, scratch, until
from test_center import center


def observed(rss=96,private=95):
    value=MemoryObservation(now=0)
    for now in (0,1,2):value.observe(rss,private,now=now)
    return value


class ObservedMemoryTests(unittest.TestCase):
    def worker(self,jobs):
        worker=Worker.__new__(Worker)
        worker.host=Mock();worker.host.sample.return_value=dict(memory_total_mb=4096,
            host_memory_available_mb=2048,host_commit_available_mb=512)
        worker.memory_capacity=2048;worker.memory_reserve=0;worker.memory_headroom=64;worker.memory_budget=2048
        worker.jobs=jobs
        return worker

    def job(self,**values):
        return dict(result=dict(status='running'),task=dict(memory_mb=768),
                    rss_mb=96,private_mb=95,rss_sampled=True,**values)

    def test_startup_retains_declared_peak_and_stable_sampling_reclaims_it(self):
        job=self.job(memory_observation=MemoryObservation(now=0));worker=self.worker({'a':job})
        self.assertEqual(worker._memory_state()['memory_available_mb'],0)
        job['memory_observation']=observed()
        state=worker._memory_state()
        self.assertEqual((state['memory_reserved_mb'],state['commit_unallocated_mb']),(128,32))
        self.assertEqual(state['memory_available_mb'],416)
        self.assertEqual((state['memory_requested_mb'],job['task']['memory_mb']),(768,768))

    def test_commit_counts_each_task_without_cross_job_private_offsets(self):
        a=self.job(memory_observation=observed());b=self.job(memory_observation=observed(96,900))
        b['private_mb']=900
        state=self.worker({'a':a,'b':b})._memory_state()
        self.assertEqual(state['commit_unallocated_mb'],32+225)
        self.assertEqual(state['memory_available_mb'],191)

    def test_file_mapped_rss_is_not_charged_again_as_private_commit(self):
        job=self.job(memory_observation=observed(1024,20));job.update(rss_mb=1024,private_mb=20)
        state=self.worker({'mapped':job})._memory_state()
        self.assertEqual(state['memory_reserved_mb'],1280)
        self.assertEqual(state['commit_unallocated_mb'],32)
        self.assertEqual(state['memory_available_mb'],416)

    def test_modeled_and_fixed_estimates_both_calibrate_after_stable_samples(self):
        job=self.job(memory_observation=observed());job['memory_phase_model']=dict(base_memory_mb=768,memory_per_cpu_mb=0)
        self.assertEqual(reservation(job),(128,127))
        job['memory_phase_model']=None
        self.assertEqual(reservation(job),(128,127))

    def test_growth_room_recent_peak_and_phase_reset(self):
        value=MemoryObservation(now=0)
        for now,amount in [(0,100),(1,150),(2,200)]:value.observe(amount,amount,now)
        self.assertEqual(value.envelope,(300,300))
        value.observe(96,95,now=7)
        self.assertEqual(value.envelope,(250,250))
        value.observe(96,95,now=13)
        self.assertEqual(value.envelope,(128,127))
        job=self.job(memory_observation=value)
        job['memory_observation']=MemoryObservation(now=13)
        self.assertEqual(reservation(job),(768,768))

    def test_zero_or_repeated_cached_samples_do_not_complete_warmup(self):
        value=MemoryObservation(now=0)
        for now in (0,.01,.02,.03):value.observe(96,95,now)
        value.observe(0,now=10)
        self.assertIsNone(value.envelope)

    def test_short_observed_peak_survives_sample_coalescing(self):
        value=MemoryObservation(now=0)
        for now,amount in [(0,96),(.1,400),(.25,96),(1,96),(2,96)]:value.observe(amount,amount,now)
        self.assertEqual(value.envelope,(500,500))

    def test_scheduler_holds_reclaimed_credit_until_fresh_worker_observation(self):
        node=Node('n',None);node.health=dict(active_jobs=[dict(id='a',memory_mb=768,
            memory_reservation_mb=128,memory_commit_reservation_mb=127)],memory_accounting='rss',
            memory_capacity_mb=768,memory_reserved_mb=128,memory_available_mb=640)
        node.live_jobs={'a'};node.reserved_memory_mb=768
        self.assertEqual(node.memory_credit('a',768),127)
        self.assertEqual(node.memory_free,640)
        node.memory_holds['a']={'amount':641,'after':None}
        self.assertEqual(node.memory_unreported,641)
        self.assertEqual(node.memory_free,-1)

    def test_fixed_cpu_growth_keeps_the_calibrated_memory_envelope(self):
        with scratch()as root,provider(root+'/worker',cpus=2,memory_mb=512,memory_headroom_mb=0)as(worker,client):
            worker.host.sample=Mock(return_value=dict(memory_total_mb=4096,host_memory_available_mb=2048,host_commit_available_mb=768))
            identity=client.submit(python('import time;time.sleep(20)',max_cpus=2,memory_mb=400))['id']
            try:
                until(lambda:client.status(identity)['status']=='running')
                client.resize(identity,1)
                until(lambda:worker.health()['memory_reserved_mb']<100,seconds=6)
                worker.host.sample.return_value['host_commit_available_mb']=128
                self.assertEqual(client.resize(identity,2)['cpus'],2)
                self.assertLess(worker.health()['memory_reserved_mb'],100)
            finally:client.cancel(identity);client.wait(identity);client.fetch(identity)

    def test_cpu_only_resize_inherits_the_active_stage_model(self):
        with scratch()as root,provider(root+'/worker',cpus=3,memory_mb=1024,memory_headroom_mb=0)as(worker,node):
            with Pool([node])as pool,center(pool,root+'/center')as(_,client):
                identity=client.submit(python('import time;time.sleep(20)',max_cpus=3,base_memory_mb=64,memory_per_cpu_mb=32))['id']
                try:
                    until(lambda:client.status(identity)['status']=='running')
                    self.assertEqual(client.resize(identity,1)['memory_mb'],96)
                    self.assertEqual(client.resize(identity,3)['memory_mb'],160)
                    self.assertEqual(client.resize(identity,2,base_memory_mb=128,memory_per_cpu_mb=48)['memory_mb'],224)
                    self.assertEqual(client.resize(identity,1)['memory_mb'],176)
                finally:client.cancel(identity);client.wait(identity);client.fetch(identity)

    def test_next_command_restores_parent_fixed_default_and_environment(self):
        import json
        code="import os,sys;sys.path.insert(0,os.environ['REMOTE_COMPUTE_HOME']);from remote_compute import Client;Client(os.environ['REMOTE_COMPUTE_URL']).resize(os.environ['REMOTE_COMPUTE_JOB_ID'],1,base_memory_mb=256,memory_per_cpu_mb=0)"
        with scratch()as root,provider(root+'/worker',cpus=1,memory_mb=1024,memory_headroom_mb=0)as(_,node):
            with Pool([node])as pool,center(pool,root+'/center')as(_,client):
                task=python(code,memory_mb=64)
                task['steps'].append(['${tool:python}','-c',"import os;print(os.environ['REMOTE_COMPUTE_MEMORY_MODEL'])"])
                receipt=client.run(task)
                self.assertEqual(receipt['status'],'succeeded',receipt.get('stderr'))
                self.assertEqual(receipt['memory_mb'],64)
                self.assertEqual(json.loads(receipt['stdout'].strip()),{})

    def test_actual_high_request_low_usage_unblocks_windows_commit(self):
        with scratch()as root,provider(root+'/worker',cpus=2,memory_mb=512,memory_headroom_mb=0)as(worker,node):
            worker.host.sample=Mock(return_value=dict(memory_total_mb=4096,host_memory_available_mb=2048,host_commit_available_mb=512))
            with Pool([node])as pool,center(pool,root+'/center')as(_,client):
                first=client.submit(python("import time;print('ready',flush=True);time.sleep(20)",memory_mb=400))['id']
                try:
                    until(lambda:'ready'in client.logs(first)['stdout'])
                    second=client.submit(python("print('second')",memory_mb=400))['id']
                    self.assertIn(client.status(second)['status'],('queued','running','succeeded'))
                    self.assertEqual(client.wait(second,wait_timeout=8)['status'],'succeeded')
                    self.assertEqual(client.status(first)['status'],'running')
                    result=client.fetch(second);self.assertIn('second',result['stdout'])
                    self.assertEqual(client.status(first)['memory_mb'],400)
                    until(lambda:worker.health()['commit_unallocated_mb']<100,seconds=6)
                finally:
                    client.cancel(first);client.wait(first);client.fetch(first)
                until(lambda:pool.nodes[0].reserved_memory_mb==0)
                self.assertEqual(worker.health()['memory_reserved_mb'],0)

    def test_real_resize_cannot_spend_the_original_reclaimed_estimate_twice(self):
        with scratch()as root,provider(root+'/worker',cpus=1,memory_mb=512,memory_headroom_mb=0)as(worker,client):
            worker.host.sample=Mock(return_value=dict(memory_total_mb=4096,host_memory_available_mb=2048,host_commit_available_mb=768))
            identity=client.submit(python('import time;time.sleep(20)',memory_mb=400))['id']
            try:
                until(lambda:worker.health()['memory_reserved_mb']<100,seconds=6)
                worker.host.sample.return_value['host_commit_available_mb']=128
                with self.assertRaises(RemoteError):client.resize(identity,1,memory_mb=300)
                self.assertEqual(client.status(identity)['memory_mb'],400)
            finally:client.cancel(identity);client.wait(identity);client.fetch(identity)

    def test_zero_available_memory_still_accepts_and_later_executes_a_queued_plan(self):
        with scratch()as root,provider(root+'/worker',cpus=1,memory_mb=512,memory_headroom_mb=0)as(worker,node):
            worker.host.sample=Mock(return_value=dict(memory_total_mb=4096,host_memory_available_mb=0,host_commit_available_mb=0))
            with Pool([node])as pool,center(pool,root+'/center')as(_,client),ThreadPoolExecutor(1)as threads:
                ordinary=Client(client.url)
                future=threads.submit(ordinary.execute,{'one':{'task':python("print('done')",memory_mb=128)}},project='queue-zero',output_dir=root+'/out')
                until(lambda:client.health()['queued']==1)
                self.assertEqual(worker.health()['memory_available_mb'],0)
                worker.host.sample.return_value.update(host_memory_available_mb=2048,host_commit_available_mb=2048)
                pool.refresh()
                self.assertEqual(future.result(timeout=8)[0]['status'],'succeeded')

    def test_success_failure_timeout_and_cancel_release_both_ledgers(self):
        with scratch()as root,provider(root+'/worker',cpus=1,memory_mb=1024,memory_headroom_mb=0)as(worker,node):
            with Pool([node])as pool,center(pool,root+'/center')as(_,client):
                for state,code,timeout in [('succeeded','pass',3),('failed','raise RuntimeError("intentional")',3),
                                           ('timed_out','import time;time.sleep(10)',.3),('cancelled','import time;time.sleep(10)',3)]:
                    identity=client.submit(python(code,memory_mb=768,timeout=timeout))['id']
                    if state=='cancelled':
                        until(lambda:client.status(identity)['status']=='running');client.cancel(identity)
                    self.assertEqual(client.wait(identity,wait_timeout=6)['status'],state)
                    client.fetch(identity)
                    until(lambda:pool.nodes[0].reserved==pool.nodes[0].reserved_memory_mb==0)
                    self.assertEqual(worker.health()['memory_reserved_mb'],0)
                    self.assertEqual(worker.health()['commit_unallocated_mb'],0)
                    self.assertEqual(pool.nodes[0].memory_holds,{})


if __name__=='__main__':unittest.main()
