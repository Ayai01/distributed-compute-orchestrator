"""A smaller phase reuses one paid slot without inventing a new calibration."""
import json
import os
from pathlib import Path
import unittest
from unittest.mock import patch

from remote_compute import Client,Pool,RemoteError,python_task
from remote_compute.worker import Conflict
from test_center import center
from test_compute import provider,scratch


class PhaseDownshiftTests(unittest.TestCase):
    identity='d'*32
    previous=dict(base_memory_mb=768,memory_per_cpu_mb=256)
    phase=dict(base_memory_mb=256,memory_per_cpu_mb=128)

    def refused(self):
        error=RemoteError('Insufficient memory for resource resize; existing grant retained')
        error.status_code=409
        return error

    def env(self,client):
        return dict(REMOTE_COMPUTE_JOB_ID=self.identity,REMOTE_COMPUTE_URL=client.url,REMOTE_COMPUTE_WORKER='1')

    def test_retained_probe_stays_single_core_and_keeps_the_acknowledged_phase(self):
        client=Client();old=dict(status='running',cpus=1,memory_mb=94,memory_phase=self.previous)
        funded=dict(cpus=4,memory_mb=768,memory_phase=self.phase)
        with patch.dict(os.environ,self.env(client)),patch.object(client,'status',return_value=old), \
                patch.object(client,'resize',side_effect=[self.refused(),self.refused(),funded]), \
                patch('remote_compute.leases.time.monotonic',return_value=10)as clock, \
                patch.object(client,'_read_reconnecting',side_effect=AssertionError('No new slot is needed')):
            lease=client.cpu_lease(self.identity,max_cpus=4,**self.phase)
            self.assertEqual(lease.refresh(ready_cpus=4),1)
            self.assertTrue(lease.phase_pending)
            self.assertEqual(lease.confirmed['memory_phase'],self.previous)
            clock.return_value=11
            self.assertEqual(lease.refresh(active_cpus=1,ready_cpus=3),1)
            self.assertEqual(lease.confirmed['memory_phase'],self.previous)
            clock.return_value=12
            self.assertEqual(lease.refresh(active_cpus=1,ready_cpus=3),4)
            self.assertFalse(lease.phase_pending)
            self.assertEqual(lease.confirmed['memory_phase'],self.phase)

    def test_idle_slots_are_returned_under_the_old_phase_before_the_probe(self):
        client=Client();old=dict(status='running',cpus=4,memory_mb=94,memory_phase=self.previous)
        with patch.dict(os.environ,self.env(client)),patch.object(client,'status',return_value=old), \
                patch.object(client,'resize',side_effect=[self.refused(),dict(cpus=1,memory_mb=94,memory_phase=self.previous)])as resize:
            lease=client.cpu_lease(self.identity,max_cpus=4,cpus=4,**self.phase)
            self.assertEqual(lease.refresh(ready_cpus=4),1)
            self.assertEqual(resize.call_args_list[1].args,(self.identity,1))
            self.assertEqual(resize.call_args_list[1].kwargs,dict(active_cpus=0,**self.previous))
            self.assertEqual(lease.confirmed['memory_phase'],self.previous)

    def test_single_core_pending_phase_is_reconfirmed_before_steady_heartbeat_stops(self):
        client=Client();old=dict(status='running',cpus=1,memory_mb=94,memory_phase=self.previous)
        with patch.dict(os.environ,self.env(client)),patch.object(client,'status',return_value=old), \
                patch.object(client,'resize',side_effect=[self.refused(),self.refused(),dict(cpus=1,memory_mb=384,memory_phase=self.phase)])as resize, \
                patch('remote_compute.leases.time.monotonic',return_value=10)as clock:
            lease=client.cpu_lease(self.identity,max_cpus=1,**self.phase)
            lease.refresh(ready_cpus=1)
            lease.refresh(active_cpus=1,ready_cpus=0)
            clock.return_value=11
            lease.refresh(active_cpus=1,ready_cpus=0)
            self.assertFalse(lease.phase_pending)
            self.assertEqual(lease.confirmed['memory_phase'],self.phase)
            lease.refresh(active_cpus=1,ready_cpus=0)
            self.assertEqual(resize.call_count,3)

    def test_resource_wait_continues_across_transport_windows_within_original_deadline(self):
        client=Client();waiting=RemoteError('resource pending');waiting.phase_waiting=True
        funded=dict(cpus=1,memory_mb=512)
        with patch('remote_compute.leases.remaining_time',return_value=20), \
                patch('remote_compute.leases.check_deadline')as deadline, \
                patch.object(client,'_read_reconnecting',side_effect=[waiting,funded])as reconnect:
            self.assertEqual(client.cpu_lease(self.identity,max_cpus=4)._wait_for_phase(self.identity),funded)
            self.assertEqual(reconnect.call_count,2)
            deadline.assert_called_once()
        for original_remaining,error in ((None,waiting),(20,RemoteError('permanent read error'))):
            with self.subTest(remaining=original_remaining,error=str(error)), \
                    patch('remote_compute.leases.remaining_time',return_value=original_remaining), \
                    patch.object(client,'_read_reconnecting',side_effect=error)as reconnect:
                with self.assertRaises(RemoteError):client.cpu_lease(self.identity,max_cpus=4)._wait_for_phase(self.identity)
                self.assertEqual(reconnect.call_count,1)

    def test_a_refused_idle_return_keeps_only_one_usable_slot(self):
        client=Client();old=dict(status='running',cpus=4,memory_mb=94,memory_phase=self.previous)
        for second in (self.refused(),RemoteError('permission denied')):
            with self.subTest(error=str(second)),patch.dict(os.environ,self.env(client)), \
                    patch.object(client,'status',return_value=old), \
                    patch.object(client,'resize',side_effect=[self.refused(),second])as resize:
                lease=client.cpu_lease(self.identity,max_cpus=4,cpus=4,**self.phase)
                if getattr(second,'status_code',None)==409:
                    self.assertEqual(lease.refresh(ready_cpus=4),1)
                    self.assertTrue(lease.phase_pending)
                else:
                    with self.assertRaises(RemoteError):lease.refresh(ready_cpus=4)
                self.assertEqual(resize.call_count,2)

    def test_real_http_repeated_maps_finish_during_a_refused_downshift(self):
        for backend in ('thread','process'):
            source='''import time
from pathlib import Path
from remote_compute import run_call,map_items
def prepare():
 end=time.monotonic()+.05
 while time.monotonic()<end:pass
def one(index):
 p=Path('called-'+str(index));n=int(p.read_text())+1 if p.exists()else 1;p.write_text(str(n))
 time.sleep(.02);return [index,n]
def entry():
 values=[]
 for batch in range(3):
  run_call(prepare,limits=dict(max_cpus=1,base_memory_mb=768,memory_per_cpu_mb=256))
  values.extend(map_items(one,range(batch*8,batch*8+8),backend=BACKEND,limits=dict(max_cpus=4,base_memory_mb=256,memory_per_cpu_mb=128)))
 return values
'''.replace('BACKEND',repr(backend))
            with self.subTest(backend=backend),scratch()as folder, \
                    provider(Path(folder)/'worker',cpus=4,memory_mb=4096,memory_headroom_mb=0)as(worker,remote), \
                    Pool([remote])as pool,center(pool,Path(folder)/'center')as(_,client):
                original=worker.resize;refused=[]
                def resize(identity,cpus,**options):
                    if options.get('base_memory_mb')==256:
                        with worker.condition:
                            job=worker.jobs[identity]
                            job['task']['memory_mb']=job['result']['memory_mb']=94
                        refused.append((cpus,options.get('active_cpus')))
                        raise Conflict('Insufficient memory for resource resize; existing grant retained')
                    return original(identity,cpus,**options)
                with patch.object(worker,'resize',side_effect=resize):
                    result=client.run(python_task('business:entry',max_cpus=4,base_memory_mb=768,memory_per_cpu_mb=256,timeout=30,retries=0),files={'business.py':source})
                self.assertEqual(result['status'],'succeeded',result.get('stderr'))
                values=json.loads(result['files']['.dco-results.json'])['items'][0]['value']
                self.assertEqual(values,[[i,1]for i in range(24)])
                self.assertGreaterEqual(len(refused),3)
                self.assertTrue(all((active or 0)<=1 for _,active in refused))
                self.assertEqual(result['attempts'],[])
                self.assertGreater(result['cpu_seconds'],0)
                self.assertEqual(worker.used,0)
