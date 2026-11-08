"""Real task trees calibrate modeled estimates and pressure kills keep evidence."""
import json
from pathlib import Path
import threading
import unittest
from unittest.mock import Mock

from remote_compute import Pool,python_map
from remote_compute.memory import calibrated_model,plan_memory,pressure_victim
from remote_compute.client import Node
from test_compute import scratch,provider,python,until
from test_center import center


class AdaptiveMemoryTests(unittest.TestCase):
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

    def test_estimate_exceeding_ram_gets_one_funded_probe(self):
        self.assertEqual(plan_memory(8,512,memory_mb=8192),(1,256))
        self.assertEqual(plan_memory(8,512,base_memory_mb=4096,memory_per_cpu_mb=768),(1,256))
        self.assertEqual(plan_memory(8,128,memory_mb=8192)[0],0)

    def test_pressure_targets_low_priority_largest_tree_in_the_scarce_resource(self):
        def job(priority,rss,private):
            return dict(task=dict(priority=priority),result=dict(status='running'),cancel=threading.Event(),rss_mb=rss,private_mb=private)
        high=job(2,2000,2000);physical=job(0,1000,100);commit=job(0,100,1000)
        host=dict(memory_total_mb=4096,host_memory_available_mb=64,host_commit_available_mb=2048)
        self.assertIs(pressure_victim([high,physical,commit],host),physical)
        host.update(host_memory_available_mb=2048,host_commit_available_mb=64)
        self.assertIs(pressure_victim([high,physical,commit],host),commit)
        host['host_commit_available_mb']=2048
        self.assertIsNone(pressure_victim([high,physical,commit],host))

    def test_oversized_request_runs_without_changing_frozen_input(self):
        with scratch()as root,provider(root+'/worker',cpus=1,memory_mb=512,memory_headroom_mb=0)as(_,node):
            with Pool([node])as pool,center(pool,root+'/center')as(_,client):
                result=client.run(python("print('oversized estimate ran')",memory_mb=8192))
                self.assertEqual(result['status'],'succeeded',result.get('error'))
                self.assertIn('oversized estimate ran',result['stdout'])

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
                identity=client.submit(python(code,memory_mb=256,outputs=['partial.txt']))['id']
                try:
                    until(lambda:any(j.get('rss_mb',0)>0 for j in worker.jobs.values()))
                    host['host_memory_available_mb']=64
                    state=client.wait(identity,wait_timeout=6)
                    self.assertEqual(state['status'],'failed')
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
