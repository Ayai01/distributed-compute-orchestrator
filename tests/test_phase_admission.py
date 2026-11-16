"""A paid preparation phase waits for the next funded slot before entry."""
import json
import os
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from remote_compute import Client,Pool,RemoteError,python_task
from remote_compute.worker import Conflict
from test_center import center
from test_compute import provider,scratch


class PhaseAdmissionTests(unittest.TestCase):
    identity='b'*32
    phase=dict(base_memory_mb=384,memory_per_cpu_mb=128)
    old=dict(status='running',cpus=1,memory_mb=268,
             memory_phase=dict(base_memory_mb=256,memory_per_cpu_mb=128))

    def refused(self):
        error=RemoteError('Insufficient memory for resource resize; existing grant retained')
        error.status_code=409
        return error

    def env(self,client):
        return dict(REMOTE_COMPUTE_JOB_ID=self.identity,REMOTE_COMPUTE_URL=client.url,
                    REMOTE_COMPUTE_WORKER='1')

    def test_new_phase_waits_and_negotiates_only_one_slot(self):
        client=Client();funded=dict(cpus=1,memory_mb=512,memory_phase=self.phase)
        with patch.dict(os.environ,self.env(client)),patch.object(client,'status',return_value=self.old), \
                patch.object(client,'resize',side_effect=[self.refused(),self.refused(),funded])as resize, \
                patch('remote_compute.client._pause'):
            lease=client.cpu_lease(self.identity,max_cpus=4,**self.phase)
            self.assertEqual(lease.refresh(ready_cpus=4),1)
            self.assertEqual([x.args[1]for x in resize.call_args_list],[4,1,1])
            self.assertEqual(lease.confirmed['memory_phase'],self.phase)

    def test_pending_request_is_observed_before_another_post(self):
        client=Client();pending=dict(self.old,resource_request={'id':1})
        settled=dict(status='running',cpus=4,memory_mb=320,memory_phase=self.phase)
        with patch.dict(os.environ,self.env(client)),patch.object(client,'status',side_effect=[self.old,pending,settled]), \
                patch.object(client,'resize',side_effect=self.refused())as resize,patch('remote_compute.client._pause'):
            self.assertEqual(client.cpu_lease(self.identity,max_cpus=4,**self.phase).refresh(ready_cpus=4),1)
            self.assertEqual(resize.call_count,1)

    def test_wait_respects_original_deadline_and_does_not_start_new_work(self):
        client=Client();env=dict(self.env(client),REMOTE_COMPUTE_DEADLINE=str(time.monotonic()-1))
        with patch.dict(os.environ,env),patch.object(client,'status',return_value=self.old), \
                patch.object(client,'resize',side_effect=self.refused())as resize:
            with self.assertRaises(TimeoutError):client.cpu_lease(self.identity,max_cpus=4,**self.phase).refresh(ready_cpus=4)
            self.assertEqual(resize.call_count,1)

    def test_terminal_and_permission_errors_end_the_wait(self):
        client=Client();forbidden=RemoteError('Operator token required');forbidden.status_code=403
        for observed in (dict(self.old,status='cancelled'),forbidden):
            with self.subTest(observed=str(observed)),patch.dict(os.environ,self.env(client)), \
                    patch.object(client,'status',side_effect=[self.old,observed]), \
                    patch.object(client,'resize',side_effect=self.refused())as resize:
                with self.assertRaises(RemoteError):client.cpu_lease(self.identity,max_cpus=4,**self.phase).refresh(ready_cpus=4)
                self.assertEqual(resize.call_count,1)

    def test_noninherited_or_active_work_does_not_wait_for_a_different_phase(self):
        client=Client()
        for inherited,active in ((False,0),(True,1)):
            with self.subTest(inherited=inherited,active=active),patch.dict(os.environ,self.env(client)if inherited else {},clear=True), \
                    patch.object(client,'status',return_value=self.old),patch.object(client,'resize',side_effect=self.refused())as resize:
                with self.assertRaises(RemoteError):client.cpu_lease(self.identity,max_cpus=4,**self.phase).refresh(active_cpus=active,ready_cpus=3)
                self.assertEqual(resize.call_count,1)

    def test_real_http_known_calibrated_phase_reaches_body_once_after_refusal(self):
        source='''import time
from pathlib import Path
from remote_compute import run_call
def prepare():
 end=time.monotonic()+.15
 while time.monotonic()<end:pass
 return 1
def body():
 p=Path('calls');n=int(p.read_text())+1 if p.exists()else 1;p.write_text(str(n));return n
def entry():
 run_call(prepare,limits=dict(max_cpus=1,base_memory_mb=256,memory_per_cpu_mb=128))
 return run_call(body,limits=dict(max_cpus=1,base_memory_mb=384,memory_per_cpu_mb=128))
'''
        with scratch()as folder,provider(Path(folder)/'worker',cpus=4,memory_mb=4096,memory_headroom_mb=0)as(worker,remote), \
                Pool([remote])as pool,center(pool,Path(folder)/'center')as(_,client):
            original=worker.resize;refused=[];requests=[]
            def resize(identity,cpus,**options):
                if options.get('base_memory_mb')==384:
                    requests.append(cpus)
                    if len(refused)<2:
                        with worker.condition:
                            job=worker.jobs[identity]
                            job['task']['memory_mb']=job['result']['memory_mb']=268
                        refused.append(identity)
                        raise Conflict('Insufficient memory for resource resize; existing grant retained')
                return original(identity,cpus,**options)
            with patch.object(worker,'resize',side_effect=resize):
                result=client.run(python_task('business:entry',max_cpus=4,base_memory_mb=384,memory_per_cpu_mb=128,timeout=15,retries=0),files={'business.py':source})
            self.assertEqual(result['status'],'succeeded',result.get('stderr'))
            self.assertEqual(json.loads(result['files']['.dco-results.json'])['items'][0]['value'],1)
            self.assertEqual(len(refused),2)
            self.assertTrue(all(n in(0,1)for n in requests))
            self.assertIn(0,requests)
            self.assertGreater(result['cpu_seconds'],.1)
            self.assertEqual(worker.used,0)
