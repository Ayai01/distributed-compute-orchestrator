"""Real task trees calibrate modeled estimates and pressure kills keep evidence."""
import json
from pathlib import Path
import threading
import unittest
from unittest.mock import Mock,patch

from remote_compute import Pool,python_map,python_task
from remote_compute.memory import calibrated_model,plan_memory,pressure_victim,MemoryObservation,PressureGuard
from remote_compute.client import Node
from remote_compute.worker import Worker
from test_compute import scratch,provider,python,until
from test_center import center


class AdaptiveMemoryTests(unittest.TestCase):
    def test_funded_shared_margin_allows_growth_without_repricing_the_same_padding(self):
        for fixed in (False,True):
            with self.subTest(fixed=fixed):
                worker,job=self.sampled_worker(cpus=2)
                phase=dict(memory_mb=8192)if fixed else dict(base_memory_mb=4096,memory_per_cpu_mb=4096)
                job['task']['memory_mb']=8192 if fixed else 12288
                job['task']['memory_model']=None if fixed else phase
                job['memory_phase_model']=None if fixed else phase
                job['result']['memory_phase']=phase
                job['memory_width_samples']=[(1,128),(2,128)]
                reply=worker.resize('one',4,active_cpus=2,memory_budget_mb=127,**phase)
                self.assertEqual((reply['cpus'],reply['memory_mb']),(4,127))
                self.assertEqual(reply['memory_phase'],phase)
                self.assertEqual(job['memory_adaptive_model'].get('memory_mb',job['memory_adaptive_model'].get('base_memory_mb')),128)

    def test_funded_padding_does_not_approve_unknown_growth_or_actual_overuse(self):
        from remote_compute.worker import Conflict
        for changed,budget in ((False,95),(True,127)):
            worker,job=self.sampled_worker(cpus=2)
            phase=dict(base_memory_mb=4096,memory_per_cpu_mb=4096)
            job['task'].update(memory_model=phase,memory_mb=12288);job['memory_phase_model']=phase
            job['result']['memory_phase']=phase
            request=dict(base_memory_mb=8192,memory_per_cpu_mb=8192)if changed else phase
            with self.subTest(changed=changed,budget=budget),self.assertRaises(Conflict):
                worker.resize('one',4,active_cpus=2,memory_budget_mb=budget,**request)
        worker,job=self.sampled_worker(cpus=2)
        job['memory_width_samples']=[(1,96),(2,128)]
        with self.assertRaises(Conflict):
            worker.resize('one',4,active_cpus=2,memory_budget_mb=127)
        self.assertEqual(job['task']['cpus'],2)

    def test_shared_working_set_does_not_multiply_with_cpu_count(self):
        model=dict(base_memory_mb=64,memory_per_cpu_mb=1024)
        quote=calibrated_model(model,model,4,1024,1000,True,points=[(1,1024),(4,1024)])
        self.assertEqual(quote,dict(base_memory_mb=1024,memory_per_cpu_mb=0))
        self.assertEqual(plan_memory(64,2048,**quote),(64,1024))

    def test_small_observed_growth_keeps_the_shared_baseline_once(self):
        model=dict(base_memory_mb=64,memory_per_cpu_mb=1024)
        quote=calibrated_model(model,model,4,1056,1000,True,points=[(1,1024),(4,1056)])
        self.assertEqual(quote,dict(base_memory_mb=1013,memory_per_cpu_mb=11))
        self.assertEqual(plan_memory(64,2048,**quote),(64,1717))

    def test_independent_process_growth_retains_its_measured_increment(self):
        model=dict(base_memory_mb=64,memory_per_cpu_mb=1024)
        quote=calibrated_model(model,model,4,1000,900,True,points=[(1,400),(4,1000)])
        self.assertEqual(quote,dict(base_memory_mb=200,memory_per_cpu_mb=200))
        self.assertEqual(plan_memory(64,13000,**quote),(64,13000))

    def test_fixed_estimate_also_learns_actual_process_growth(self):
        model=dict(memory_mb=8192)
        quote=calibrated_model(model,model,4,1000,900,True,points=[(1,400),(4,1000)])
        self.assertEqual(quote,dict(base_memory_mb=200,memory_per_cpu_mb=200))
        self.assertEqual(plan_memory(64,13000,**quote),(64,13000))

    def test_a_single_width_quotes_whole_observed_slots_until_shared_baseline_is_measured(self):
        model=dict(base_memory_mb=256,memory_per_cpu_mb=768)
        quote=calibrated_model(model,model,4,128,100,True)
        self.assertEqual(quote,dict(base_memory_mb=0,memory_per_cpu_mb=32))
        self.assertEqual(plan_memory(64,1024,**quote),(32,1024))
        shared=calibrated_model(model,model,8,128,100,True,points=[(4,128),(8,128)])
        self.assertEqual(plan_memory(64,1024,**shared),(64,128))

    def test_short_item_gaps_preserve_the_width_of_the_sampled_work(self):
        observation=MemoryObservation(now=0)
        for at,width in ((0,2),(1,0),(2,1)):observation.observe(96,95,now=at,active_cpus=width)
        self.assertEqual(observation.active_cpus,2)
        self.assertIsNotNone(observation.envelope)

    def test_retired_working_set_does_not_stay_reserved_by_old_width_samples(self):
        worker,job=self.sampled_worker(cpus=4)
        job.update(memory_width_samples=[(1,4096),(4,4096)],memory_width_sampled_at=0,
                   memory_adaptive_model=dict(base_memory_mb=4096,memory_per_cpu_mb=0))
        with patch('remote_compute.worker.time.monotonic',return_value=20):
            reply=worker.resize('one',4,active_cpus=4)
        self.assertEqual(reply['memory_mb'],128)
        self.assertEqual(job['memory_adaptive_model'],dict(base_memory_mb=128,memory_per_cpu_mb=0))

    def sampled_worker(self,cpus=1,per=768):
        worker=Worker.__new__(Worker);worker.condition=threading.Condition(threading.RLock())
        worker.pressure_guard=PressureGuard()
        worker.capacity=4;worker.used=cpus;worker.memory_capacity=1024;worker.stopping=False
        worker.cpu_overcommit_factor=1.0
        worker._memory_state=Mock(return_value=dict(memory_available_mb=897));worker._save=Mock()
        observation=MemoryObservation(now=0)
        for at in (0,1,2):observation.observe(96,95,now=at)
        model=dict(base_memory_mb=64,memory_per_cpu_mb=per,memory_limit_mb=None)
        job=dict(task=dict(cpus=cpus,max_cpus=4,memory_mb=64+per*cpus,memory_model=model),
                 result=dict(status='running',active_cpus=cpus),cancel=threading.Event(),
                 rss_mb=96,private_mb=95,memory_observation=observation,memory_phase_model=model)
        worker.jobs={'one':job}
        return worker,job

    def test_same_phase_keeps_learned_cost_while_new_width_warms_up(self):
        worker,job=self.sampled_worker()
        first=worker.resize('one',4,active_cpus=1)
        self.assertEqual(first['memory_mb'],512)
        for _ in range(3):
            again=worker.resize('one',4,active_cpus=4)
            self.assertEqual((again['cpus'],again['memory_mb']),(4,512))

    def test_cold_one_slot_quote_does_not_subtract_unmeasured_shared_base(self):
        model=dict(base_memory_mb=256,memory_per_cpu_mb=512)
        quote=calibrated_model(model,model,1,286,280,True,
                               learned=dict(base_memory_mb=256,memory_per_cpu_mb=30),points=[(1,286)])
        self.assertEqual(quote,dict(base_memory_mb=0,memory_per_cpu_mb=286))
        self.assertEqual(plan_memory(15,1536,**quote),(5,1430))
        self.assertEqual(plan_memory(15,4290,**quote),(15,4290))

    def test_probe_funds_every_active_slot_instead_of_only_one(self):
        quote=dict(base_memory_mb=0,memory_per_cpu_mb=64)
        self.assertEqual(plan_memory(4,95,floor=2,**quote)[0],0)
        self.assertEqual(plan_memory(4,128,floor=2,**quote),(2,128))
        # Estimates remain soft: two cold probes fund 256 MiB each.
        self.assertEqual(plan_memory(4,550,floor=2,base_memory_mb=0,memory_per_cpu_mb=512),(2,512))

    def test_a_cold_probe_cannot_replace_known_actual_use_with_a_smaller_budget(self):
        from remote_compute.worker import Conflict
        worker,job=self.sampled_worker(cpus=2,per=512)
        job.update(rss_mb=900,private_mb=890,memory_width_samples=[(1,400),(2,1125)])
        with self.assertRaises(Conflict):worker.resize('one',4,active_cpus=2,memory_budget_mb=550)
        self.assertEqual(job['task']['cpus'],2)

    def test_explicit_shared_phase_keeps_width_independent_funding(self):
        model=dict(base_memory_mb=4096,memory_per_cpu_mb=0)
        quote=calibrated_model(model,model,1,128,100,True)
        self.assertEqual(plan_memory(64,128,**quote),(64,128))

    def test_business_ceiling54_and_general_ceiling1024_are_not_cut_to_group_size(self):
        for maximum,expected in ((54,54),(1024,104)):
            with self.subTest(max_cpus=maximum):
                worker,job=self.sampled_worker();worker.capacity=104;worker.memory_capacity=65536
                worker._memory_state.return_value['memory_available_mb']=65000
                job['task']['max_cpus']=maximum
                grant=worker.resize('one',0,active_cpus=1)
                self.assertEqual(grant['cpus'],expected)
                self.assertEqual(job['task']['max_cpus'],maximum)

    def test_repricing_the_same_width_preserves_valid_samples(self):
        worker,job=self.sampled_worker(cpus=4,per=128)
        observation=job['memory_observation']
        reply=worker.resize('one',4,active_cpus=4)
        self.assertEqual(reply['memory_mb'],128)
        self.assertIs(job['memory_observation'],observation)

    def test_new_phase_discards_the_previous_learned_cost(self):
        worker,job=self.sampled_worker()
        worker.resize('one',4,active_cpus=1)
        changed=worker.resize('one',4,active_cpus=0,base_memory_mb=256,memory_per_cpu_mb=256)
        self.assertEqual((changed['cpus'],changed['memory_mb']),(3,1024))
        self.assertNotIn('memory_adaptive_model',job)
        self.assertNotIn('memory_width_samples',job)

    def test_explicit_fixed_phase_uses_observed_memory_for_cpu_growth(self):
        worker,job=self.sampled_worker()
        job['task'].pop('memory_model');job['task']['memory_mb']=8192
        job['memory_phase_model']=None;job['result']['memory_phase']=dict(memory_mb=8192)
        reply=worker.resize('one',4,active_cpus=1,memory_mb=8192)
        self.assertEqual((reply['cpus'],reply['memory_mb']),(4,128))

    def test_center_uses_learned_cost_during_warmup_only_for_the_same_phase(self):
        node=Node('worker',None);phase=dict(base_memory_mb=64,memory_per_cpu_mb=768)
        learned=dict(base_memory_mb=0,memory_per_cpu_mb=128)
        node.health=dict(active_jobs=[dict(id='one',cpus=4,active_cpus=0,memory_phase_model=phase,
                         memory_calibrated=False,memory_adaptive_model=learned)])
        self.assertEqual(node.resize_memory_model('one',phase),learned)
        changed=dict(base_memory_mb=512,memory_per_cpu_mb=512)
        self.assertEqual(node.resize_memory_model('one',changed),changed)

    def test_next_command_discards_previous_phase_learning_before_requesting_cpus(self):
        with scratch()as root,provider(root+'/worker',cpus=1,memory_mb=2048,memory_headroom_mb=0)as(worker,client):
            before=[];automatic=worker._automatic_grant
            def grant(job):
                before.append(job.get('memory_adaptive_model'));return automatic(job)
            task=python("from pathlib import Path;import time;print('ready',flush=True)\nwhile not Path('release').exists():time.sleep(.02)",
                        max_cpus=1,base_memory_mb=128,memory_per_cpu_mb=768)
            task['steps'].append(['${tool:python}','-c',"print('next')"])
            with patch.object(worker,'_automatic_grant',side_effect=grant):
                identity=client.submit(task)['id']
                try:
                    until(lambda:'ready'in client.logs(identity)['stdout'])
                    with worker.condition:worker.jobs[identity]['memory_adaptive_model']=dict(base_memory_mb=0,memory_per_cpu_mb=1)
                    client.write_job_file(identity,'release','go')
                    self.assertEqual(client.wait(identity)['status'],'succeeded')
                    self.assertEqual(before,[None,None])
                finally:
                    if client.status(identity)['status']=='running':client.cancel(identity);client.wait(identity)
                    client.fetch(identity)

    def test_http_retained_grant_preserves_a_calibrated_phase(self):
        from remote_compute import Client,RemoteError
        phase=dict(base_memory_mb=64,memory_per_cpu_mb=768)
        with scratch()as root,provider(root+'/worker',cpus=4,memory_mb=1024,memory_headroom_mb=0)as(worker,node):
            with Pool([node])as pool,center(pool,root+'/center')as(_,client):
                task=python('import time;print("ready",flush=True);time.sleep(20)',max_cpus=4,**phase)
                identity=client.submit(task)['id'];original=client.status(identity)['request_sha256']
                try:
                    until(lambda:'ready'in client.logs(identity)['stdout'])
                    until(lambda:any(j.get('memory_observation')and j['memory_observation'].envelope is not None for j in worker.jobs.values()),seconds=6)
                    client.resize(identity,1,active_cpus=1,**phase);pool.refresh()
                    reply=client.resize(identity,4,active_cpus=1,**phase)
                    self.assertEqual(reply['cpus'],4)
                    self.assertLess(reply['memory_mb'],64+768*4)
                    with pool.condition:
                        self.assertEqual(pool.nodes[0].memory_credit(identity,reply['memory_mb']),reply['memory_mb'])
                    public=Client(client.url);receipt=public.status(identity)
                    self.assertEqual(receipt['memory_phase'],phase)
                    self.assertEqual(receipt['request_sha256'],original)
                    error=RemoteError('Center resource reply timed out; existing grant retained');error.status_code=409
                    with patch.object(public,'resize',side_effect=error):
                        lease=public.cpu_lease(identity,max_cpus=4,cpus=4,**phase)
                        self.assertEqual(lease.refresh(active_cpus=4,ready_cpus=1),4)
                finally:client.cancel(identity);client.wait(identity);client.fetch(identity)

    def test_http_fixed_phase_keeps_its_declared_identity_after_calibration(self):
        from remote_compute import Client,RemoteError
        phase=dict(memory_mb=8192)
        with scratch()as root,provider(root+'/worker',cpus=4,memory_mb=1024,memory_headroom_mb=0)as(worker,node):
            with Pool([node])as pool,center(pool,root+'/center')as(_,client):
                identity=client.submit(python('import time;print("ready",flush=True);time.sleep(20)',max_cpus=4,**phase))['id']
                try:
                    until(lambda:'ready'in client.logs(identity)['stdout'])
                    self.assertEqual(client.status(identity)['memory_phase'],phase)
                    self.assertLessEqual(client.status(identity)['memory_mb'],1024)
                    until(lambda:any(j.get('memory_observation')and j['memory_observation'].envelope is not None for j in worker.jobs.values()),seconds=6)
                    pool.refresh();reply=client.resize(identity,4,active_cpus=1,**phase)
                    self.assertEqual(reply['cpus'],4)
                    self.assertLess(reply['memory_mb'],1024)
                    self.assertEqual(reply['memory_phase'],phase)
                    public=Client(client.url)
                    self.assertEqual(public.status(identity)['memory_phase'],phase)
                    error=RemoteError('Center resource reply timed out; existing grant retained');error.status_code=409
                    with patch.object(public,'resize',side_effect=error):
                        lease=public.cpu_lease(identity,max_cpus=4,cpus=4,**phase)
                        self.assertEqual(lease.refresh(active_cpus=4,ready_cpus=1),4)
                finally:client.cancel(identity);client.wait(identity);client.fetch(identity)

    def test_acknowledged_growth_credit_survives_a_stale_worker_sample(self):
        node=Node('worker',None)
        node.health=dict(active_jobs=[dict(id='one',memory_mb=832,memory_reservation_mb=100,memory_commit_reservation_mb=90)])
        node.memory_holds['one']=dict(amount=742,after=None)
        self.assertEqual(node.memory_credit('one',800),90)
        node.memory_holds['one']['after']=1
        self.assertEqual(node.memory_credit('one',800),800)
        node.memory_holds['one']=dict(amount=0,after=1,credit=900)
        self.assertEqual(node.memory_credit('one',900),900)

    def test_stable_tree_scales_to_next_width_but_new_phase_uses_its_estimate(self):
        model=dict(base_memory_mb=256,memory_per_cpu_mb=768)
        learned=calibrated_model(model,model,4,1200,1000,True)
        self.assertEqual(learned,dict(base_memory_mb=0,memory_per_cpu_mb=300))
        self.assertEqual(plan_memory(8,2400,**learned),(8,2400))
        changed=dict(base_memory_mb=1024,memory_per_cpu_mb=1024)
        self.assertEqual(calibrated_model(changed,model,4,1200,1000,True),changed)
        self.assertEqual(calibrated_model(model,model,4,1200,1000,False),model)

    def test_only_two_active_workers_under_eight_granted_slots_fund_eight_real_workers(self):
        node=Node('worker',None);model=dict(base_memory_mb=256,memory_per_cpu_mb=768)
        row=dict(id='one',cpus=8,active_cpus=2,memory_phase_model=model,memory_calibrated=True,
                 memory_reservation_mb=1200,memory_commit_reservation_mb=1000)
        node.health=dict(active_jobs=[row])
        learned=node.resize_memory_model('one',model)
        self.assertEqual(learned,dict(base_memory_mb=0,memory_per_cpu_mb=600))
        self.assertEqual(plan_memory(8,4800,**learned),(8,4800))
        row['active_cpus']=0
        self.assertEqual(node.resize_memory_model('one',model),model)
        row['memory_sample_cpus']=0
        self.assertEqual(node.resize_memory_model('one',model),dict(base_memory_mb=0,memory_per_cpu_mb=1200))

    def test_estimate_exceeding_ram_gets_one_funded_probe(self):
        self.assertEqual(plan_memory(8,512,memory_mb=8192),(1,256))
        self.assertEqual(plan_memory(8,512,base_memory_mb=4096,memory_per_cpu_mb=768),(1,256))
        self.assertEqual(plan_memory(8,128,memory_mb=8192)[0],0)

    def test_pressure_targets_the_newest_started_tree(self):
        def job(start,rss,private):
            return dict(task=dict(priority=0),result=dict(status='running',started=start),process=Mock(),cancel=threading.Event(),rss_mb=rss,private_mb=private)
        old=job(1,2000,2000);new=job(3,1000,100);middle=job(2,100,1000)
        host=dict(memory_total_mb=4096,host_memory_available_mb=64,host_commit_available_mb=2048)
        self.assertIs(pressure_victim([old,new,middle],host),new)
        host.update(host_memory_available_mb=2048,host_commit_available_mb=64)
        self.assertIs(pressure_victim([old,new,middle],host),new)
        host['host_commit_available_mb']=2048
        self.assertIsNone(pressure_victim([old,new,middle],host))

    def test_oversized_request_runs_without_changing_frozen_input(self):
        with scratch()as root,provider(root+'/worker',cpus=1,memory_mb=512,memory_headroom_mb=0)as(_,node):
            with Pool([node])as pool,center(pool,root+'/center')as(_,client):
                result=client.run(python("print('oversized estimate ran')",memory_mb=8192))
                self.assertEqual(result['status'],'succeeded',result.get('error'))
                self.assertIn('oversized estimate ran',result['stdout'])

    def test_entry_inherits_fixed_memory_without_coupling_it_to_inner_cpu_count(self):
        with scratch()as root,provider(root+'/worker',cpus=8,memory_mb=1024,memory_headroom_mb=0)as(_,node):
            with Pool([node])as pool,center(pool,root+'/center')as(_,client):
                task=python_task('fixed_work:main',max_cpus=8,memory_mb=8192,timeout=30)
                code='import time\nfrom remote_compute import map_items\ndef one(value):\n time.sleep(.5)\n return value\ndef main():return map_items(one,list(range(30)),backend="thread")\n'
                result=client.run(task,files={'fixed_work.py':code})
                self.assertEqual(result['status'],'succeeded',result.get('stderr'))
                self.assertEqual(result['peak_cpus'],8)
                self.assertEqual(json.loads(result['files']['.dco-results.json'])['items'][0]['value'],list(range(30)))

    def test_modeled_map_fills_eight_cpus_with_total_estimate_over_physical_budget(self):
        with scratch()as root,provider(root+'/worker',cpus=8,memory_mb=1024,memory_headroom_mb=0)as(worker,node):
            worker.host.sample=Mock(return_value=dict(memory_total_mb=4096,host_memory_available_mb=2048,host_commit_available_mb=2048))
            with Pool([node])as pool,center(pool,root+'/center')as(_,client):
                task=python_map('memory_work:one',list(range(30)),max_cpus=8,timeout=30,
                                base_memory_mb=64,memory_per_cpu_mb=768)
                result=client.run(task,files={'memory_work.py':'import time\ndef one(value):\n time.sleep(.5)\n return value\n'},output_dir=root+'/delivery')
                self.assertEqual(result['status'],'succeeded',str({k:result.get(k)for k in ('status','peak_cpus','elapsed','completed','total','error','stderr')}))
                self.assertEqual(result['peak_cpus'],8,str({k:result.get(k)for k in ('cpus','memory_mb','peak_rss_mb','resize_count')}))
                path=Path(result['output_dir'])/'files/.dco-results.json'
                self.assertEqual(len(json.loads(path.read_bytes())['items']),30)

    def test_real_pressure_terminates_tree_and_retains_partial_output_cost_and_id(self):
        with scratch()as root,provider(root+'/worker',cpus=1,memory_mb=512,memory_headroom_mb=0)as(worker,node):
            host=dict(memory_total_mb=4096,host_memory_available_mb=2048,host_commit_available_mb=2048)
            worker.host.sample=Mock(side_effect=lambda:dict(host))
            with Pool([node])as pool,center(pool,root+'/center')as(_,client):
                code="from pathlib import Path;import time;Path('partial.txt').write_text('kept');time.sleep(20)"
                identity=client.submit(python(code,memory_mb=256,outputs=['partial.txt'],retries=0))['id']
                try:
                    until(lambda:any(j.get('rss_mb',0)>0 for j in worker.jobs.values()))
                    host['host_memory_available_mb']=64
                    state=client.wait(identity,wait_timeout=6)
                    self.assertEqual(state['status'],'interrupted')
                    self.assertEqual(state['attempts'],[])
                    host['host_memory_available_mb']=2048
                    result=client.fetch(identity,root+'/delivery')
                    self.assertEqual(result['id'],identity)
                    self.assertEqual(result['error_kind'],'memory_pressure')
                    self.assertGreaterEqual(result['cpu_seconds'],0)
                    self.assertEqual((Path(result['output_dir'])/'files/partial.txt').read_text(),'kept')
                    until(lambda:worker.used==pool.nodes[0].reserved==0)
                finally:
                    host['host_memory_available_mb']=2048
                    if client.statuses([identity])[identity]['status']not in ('unknown','failed'):
                        client.cancel(identity);client.wait(identity);client.fetch(identity)
