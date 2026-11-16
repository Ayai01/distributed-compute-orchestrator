"""A bounded temporary CPU ceiling keeps real active grants and stage order."""
from pathlib import Path
import threading
import unittest
from unittest.mock import patch

from remote_compute import Pool,RemoteError
from remote_compute.allocation import cpu_capacity,validate_cpu_overcommit,validate_cpu_utilization
from remote_compute.client import Node
from test_compute import provider,python,scratch,until
import test_center as fixtures

HOLD="import os,time\nfrom pathlib import Path\nprint('ready',os.environ['REMOTE_COMPUTE_MAX_CPUS'],flush=True)\nwhile not Path('release').exists():time.sleep(.02)"


class CpuOvercommitTests(unittest.TestCase):
    def test_configured_utilization_stops_borrowing_at_its_exact_boundary(self):
        self.assertEqual(cpu_capacity(8,8,69.99,1.5,70),12)
        self.assertEqual(cpu_capacity(8,8,70,1.5,70),8)
        for value in(0,101,True,float('nan'),'70'):
            with self.subTest(value=value),self.assertRaises(ValueError):validate_cpu_utilization(value)

    def test_limit_is_based_on_full_base_not_previous_borrowed_ceiling(self):
        for base,factor,ceiling in [(16,1.5,24),(24,1.25,30),(96,1.25,120),(5,1.25,6)]:
            with self.subTest(base=base,factor=factor):
                self.assertEqual(cpu_capacity(base,base-1,20,factor),base)
                self.assertEqual(cpu_capacity(base,base,20,factor),ceiling)
                self.assertEqual(cpu_capacity(base,ceiling,89.99,factor),ceiling)
                for percent in (90,100,None,float('nan')):
                    self.assertEqual(cpu_capacity(base,ceiling,percent,factor),base)

    def test_node_routing_limit_scales_from_its_own_base(self):
        node=Node('worker',None,limit=96,reserved=96)
        node.health=dict(cpus=120,base_cpus=104,cpu_percent=20,cpu_overcommit_factor=1.25)
        self.assertEqual((node.base_capacity,node.capacity),(96,120))
        node.reserved=120
        self.assertEqual(node.capacity,120)
        node.health['cpu_percent']=90
        self.assertEqual(node.capacity,96)
        node.health['cpu_percent']=20;node.reserved=95
        self.assertEqual(node.capacity,96)
        node.reserved=96;node.health['used_cpus']=95
        self.assertEqual(node.capacity,96)
        node.health['used_cpus']=96
        self.assertEqual(node.capacity,120)

    def test_factor_control_persists_and_rejects_out_of_range_values(self):
        with scratch()as root,provider(root,cpus=2,cpu_overcommit_factor=1.5)as(worker,client):
            saved=[];worker.save_settings=saved.append
            self.assertEqual(client.health()['cpu_overcommit_factor'],1.5)
            client.control(cpu_overcommit_factor=1.25)
            self.assertEqual(client.health()['cpu_overcommit_factor'],1.25)
            self.assertEqual(saved,[dict(cpu_overcommit_factor=1.25)])
            for value in (True,0,.9,1.51,'1.25',float('inf'),float('nan')):
                with self.subTest(value=value),self.assertRaises(ValueError):validate_cpu_overcommit(value)
            with self.assertRaises(RemoteError):client.control(cpu_overcommit_factor=2)
            self.assertEqual(worker.cpu_overcommit_factor,1.25)

    def test_ninety_percent_blocks_new_borrowing_without_stopping_originals(self):
        with scratch()as root,provider(root,cpus=2,cpu_overcommit_factor=1.5)as(worker,remote):
            host=dict(worker.host.sample(),cpu_percent=20)
            with patch.object(worker.host,'sample',side_effect=lambda:dict(host)), \
                    Pool([remote],refresh=.05)as pool,fixtures.center(pool,Path(root)/'center')as(_,client):
                ids=[client.submit(python(HOLD,memory_mb=64))['id']for _ in range(4)]
                until(lambda:all('ready'in client.logs(i)['stdout']for i in ids[:3]),seconds=10)
                self.assertEqual(worker.used,3)
                self.assertEqual(client.status(ids[3])['status'],'queued')
                self.assertTrue(remote.status(ids[2])['cpu_overcommit'])
                host['cpu_percent']=90
                pool.refresh()
                self.assertEqual(client.health()['cpus'],2)
                client.write_job_file(ids[0],'release','done');client.wait(ids[0])
                until(lambda:worker.used==2)
                self.assertEqual(client.status(ids[3])['status'],'queued')
                self.assertEqual(client.status(ids[2])['status'],'running')
                client.write_job_file(ids[1],'release','done');client.wait(ids[1])
                until(lambda:'ready'in client.logs(ids[3])['stdout'],seconds=5)
                self.assertFalse(remote.status(ids[3])['cpu_overcommit'])
                for i in ids[2:]:client.write_job_file(i,'release','done')
                for i in ids:
                    self.assertEqual(client.wait(i)['status'],'succeeded');client.fetch(i)

    def test_resize_reclaims_only_idle_borrowed_slots_and_keeps_declared_maximum(self):
        with scratch()as root,provider(root,cpus=4,cpu_overcommit_factor=1.5)as(worker,client):
            host=dict(worker.host.sample(),cpu_percent=20)
            with patch.object(worker.host,'sample',side_effect=lambda:dict(host)):
                identity=client.submit(python(HOLD,max_cpus=1024,memory_mb=64))['id']
                until(lambda:'ready'in client.logs(identity)['stdout'])
                self.assertIn('1024',client.logs(identity)['stdout'])
                self.assertEqual(client.resize(identity,1024,active_cpus=4)['cpus'],6)
                host['cpu_percent']=90
                self.assertEqual(client.resize(identity,1024,active_cpus=6)['cpus'],6)
                self.assertEqual(client.resize(identity,0,active_cpus=6)['cpus'],6)
                self.assertEqual(client.resize(identity,1024,active_cpus=4)['cpus'],4)
                self.assertEqual(client.status(identity)['max_cpus'],1024)
                host['cpu_percent']=89.99
                self.assertEqual(client.resize(identity,1024,active_cpus=4)['cpus'],6)
                client.write_job_file(identity,'release','done')
                self.assertEqual(client.wait(identity)['status'],'succeeded');client.fetch(identity)

    def test_extra_capacity_follows_warmup_then_new_tasks_then_wider_work(self):
        state=fixtures.AdmissionPlanningTests().center([8]);node=state.pool.nodes[0]
        node.health.update(base_cpus=8,cpu_percent=20,cpu_overcommit_factor=1.5)
        node.reserved=8
        for index in range(2):
            state.jobs[str(index)]=dict(task=dict(steps=[['echo']],cpus=4,max_cpus=1024,memory_mb=64,project='A'),
                elastic_cpus=True,node=node,cancel=threading.Event(),desired_cpus=64,
                result=dict(status='running',submitted=index))
        waiting=dict(task=dict(steps=[['echo']],cpus=1,max_cpus=1024,memory_mb=64,project='A'),
            elastic_cpus=True,node=None,cancel=threading.Event(),desired_cpus=64,
            result=dict(status='queued',submitted=2))
        state.jobs['waiting']=waiting
        self.assertEqual(node.capacity,12)
        resources=state.project_resources()
        self.assertEqual(state._cooperative_cpu_target('0',64,resources),4)
        waiting['node']=node;waiting['result']['status']='running';node.reserved=9
        self.assertEqual(state._cooperative_cpu_target('waiting',64,state.project_resources()),4)
        del state.jobs['waiting']
        self.assertEqual(state._cooperative_cpu_target('0',64,state.project_resources()),6)
        node.health['cpu_percent']=90
        self.assertEqual(state._cooperative_cpu_target('0',64,state.project_resources()),4)


if __name__=='__main__':unittest.main()
