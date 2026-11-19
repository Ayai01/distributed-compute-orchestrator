"""A fixed-width body starts once, after its actual CPU grant is funded."""
import json
import os
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from remote_compute import Client,Pool,python_task,run_call
from remote_compute.deadline import resource_window,remaining_time,check_deadline
from test_center import center
from test_compute import provider,scratch,python,until


SOURCE='''import os,time
from pathlib import Path
from remote_compute import map_items,run_call
def kernel():
    p=Path('calls');p.write_text(str(int(p.read_text())+1 if p.exists()else 1))
    return dict(cpus=int(os.environ['REMOTE_COMPUTE_CPUS']),calls=int(p.read_text()))
def entry(wait=5):
    Path('paid-preparation').write_text('retained')
    deadline=os.environ['REMOTE_COMPUTE_DEADLINE'];started=time.monotonic()
    try:
        value=run_call(kernel,limits=dict(exact_cpus=4,start_timeout=wait,memory_mb=768))
    except TimeoutError as error:
        value=dict(error=str(error),calls=int(Path('calls').read_text())if Path('calls').exists()else 0)
        value['next_stage']=run_call(lambda:int(os.environ['REMOTE_COMPUTE_CPUS']),limits=dict(max_cpus=1))
    value.update(waited=time.monotonic()-started,deadline_preserved=deadline==os.environ['REMOTE_COMPUTE_DEADLINE'])
    return value
def prepare(value):
    if value == 1:
        Path('preparing').touch()
        while not Path('resume').exists():time.sleep(.02)
    return value
def single():
    prepared=map_items(prepare,[1,2],backend='thread',limits={'memory_mb':2304})
    linked=run_call(lambda:sum(prepared),limits={'max_cpus':1,'memory_mb':2304})
    result=run_call(kernel,limits={'exact_cpus':1,'memory_mb':512,'start_timeout':5})
    result.update(prepared=prepared,linked=linked)
    return result
'''


class ExactKernelTests(unittest.TestCase):
    def test_single_slot_survives_map_link_and_fixed_commit_with_a_peer_queued(self):
        with scratch() as folder, provider(Path(folder)/'worker',cpus=1,memory_mb=4096,memory_headroom_mb=0) as (worker,remote):
            with Pool([remote]) as pool, center(pool,Path(folder)/'center') as (_,client):
                resize=worker.resize
                requests=[]

                def record(identity,cpus,**options):
                    result=resize(identity,cpus,**options)
                    requests.append((identity, cpus, options.get('active_cpus'), options.get('min_cpus'), result['cpus']))
                    return result

                with patch.object(worker,'resize',side_effect=record):
                    identity=client.submit(python_task('business:single',max_cpus=1,memory_mb=512,timeout=20,retries=0),
                                           files={'business.py':SOURCE})['id']
                    until(lambda:(worker.root/'jobs'/identity/'input'/'preparing').exists())
                    peer=client.submit(python("print('one released core')",max_cpus=1,project='peer',timeout=12))['id']
                    self.assertEqual(client.status(peer)['status'],'queued')
                    client.write_job_file(identity,'resume','yes')
                    self.assertEqual(client.wait(identity)['status'],'succeeded')
                    result=client.fetch(identity,delete=False)
                    self.assertGreaterEqual(client.wait(peer)['started'],result['finished'])
                value=json.loads(result['files']['.dco-results.json'])['items'][0]['value']
                self.assertEqual(value,dict(cpus=1,calls=1,prepared=[1,2],linked=3))
                original=[entry[1:] for entry in requests if entry[0]==identity]
                self.assertTrue(original)
                self.assertTrue(all(asked==granted==1 for asked,_active,_min,granted in original),original)
                self.assertEqual([entry for entry in original if entry[2]==1],[(1,1,1,1)])
                self.assertEqual(client.fetch(peer,delete=False)['id'],peer)
                self.assertEqual(worker.used,0)

    def test_waiting_fixed_width_returns_all_idle_cpus_until_the_group_is_funded(self):
        with scratch()as folder,provider(Path(folder)/'worker',cpus=4,memory_mb=4096,memory_headroom_mb=0)as(worker,remote):
            with Pool([remote])as pool,center(pool,Path(folder)/'center')as(_,client):
                original=worker.resize;requests=[];partial=0
                def resize(identity,cpus,**options):
                    nonlocal partial
                    if options.get('min_cpus')==4 and not options.get('active_cpus')and partial<2:
                        partial+=1;worker.capacity=partial
                    try:result=original(identity,cpus,**options)
                    finally:worker.capacity=4
                    requests.append(dict(active=options.get('active_cpus'),cpus=cpus,grant=result['cpus']))
                    return result
                with patch.object(worker,'resize',side_effect=resize):
                    result=client.run(python_task('business:entry',max_cpus=4,memory_mb=512,timeout=15,retries=0),files={'business.py':SOURCE})
                self.assertEqual(result['status'],'succeeded',result.get('stderr'))
                value=json.loads(result['files']['.dco-results.json'])['items'][0]['value']
                self.assertEqual((value['cpus'],value['calls']),(4,1));self.assertTrue(value['deadline_preserved'])
                self.assertEqual([r['grant']for r in requests[:2]],[0,0])
                self.assertTrue(all(r['active']==0 for r in requests[:3]),requests)
                self.assertTrue(any(r['active']==4 and r['grant']==4 for r in requests),requests)
                self.assertEqual(worker.used,0)

    def test_waits_for_peer_release_without_preempting_or_resubmitting_it(self):
        with scratch()as folder,provider(Path(folder)/'worker',cpus=4,memory_mb=4096,memory_headroom_mb=0)as(worker,remote):
            with Pool([remote])as pool,center(pool,Path(folder)/'center')as(_,client):
                peer=client.submit(python("from pathlib import Path\nimport time\nwhile not Path('release').exists():time.sleep(.02)",max_cpus=3,project='peer',timeout=15))['id']
                until(lambda:worker.health()['used_cpus']==3)
                identity=client.submit(python_task('business:entry',max_cpus=4,memory_mb=512,timeout=12,retries=0),files={'business.py':SOURCE})['id']
                try:
                    until(lambda:client.status(identity).get('min_cpus')==4 and client.status(identity)['cpus']==0)
                    self.assertEqual(worker.used,3)
                    self.assertEqual(client.status(peer)['status'],'running')
                finally:client.write_job_file(peer,'release','yes')
                client.wait(identity,wait_timeout=12);result=client.fetch(identity)
                self.assertEqual(result['status'],'succeeded',result.get('stderr'))
                value=json.loads(result['files']['.dco-results.json'])['items'][0]['value']
                self.assertEqual((value['cpus'],value['calls']),(4,1))
                self.assertEqual(client.wait(peer)['status'],'succeeded')
                self.assertEqual(client.fetch(peer)['id'],peer)
                self.assertEqual(worker.used,0)

    def test_timeout_preserves_preparation_and_does_not_enter_fixed_body(self):
        with scratch()as folder,provider(Path(folder)/'worker',cpus=1,memory_mb=4096,memory_headroom_mb=0)as(worker,remote):
            result=remote.run(python_task('business:entry',kwargs=dict(wait=.2),max_cpus=4,memory_mb=512,timeout=8,
                retries=0,outputs=['paid-preparation']),files={'business.py':SOURCE})
            self.assertEqual(result['status'],'succeeded',result.get('stderr'))
            value=json.loads(result['files']['.dco-results.json'])['items'][0]['value']
            self.assertEqual(value['calls'],0);self.assertEqual(value['next_stage'],1)
            self.assertIn('needs 4 CPUs; obtained 0',value['error'])
            self.assertTrue(value['deadline_preserved']);self.assertLess(value['waited'],2)
            self.assertEqual(result['files']['paid-preparation'],b'retained')
            self.assertEqual(worker.used,0)

    def test_invalid_fixed_stage_does_not_enter_or_contact_provider(self):
        env=dict(REMOTE_COMPUTE_WORKER='1',REMOTE_COMPUTE_MAX_CPUS='4',REMOTE_COMPUTE_DEADLINE='')
        for limits in (dict(exact_cpus=0),dict(exact_cpus=True),dict(exact_cpus=5),dict(exact_cpus=4,max_cpus=4),
                       dict(exact_cpus=4,start_timeout=float('inf')),dict(start_timeout=1)):
            with self.subTest(limits=limits),patch.dict(os.environ,env),patch('remote_compute.Client')as client:
                with self.assertRaises(ValueError):run_call(lambda:self.fail('body entered'),limits=limits)
                client.assert_not_called()
        with patch.dict(os.environ,dict(env,DCO_ITEM_PROCESS='1')):
            with self.assertRaisesRegex(ValueError,'top-level'):run_call(lambda:None,limits=dict(exact_cpus=4))

    def test_startup_window_bounds_resource_http_not_the_kernel_deadline(self):
        with scratch()as folder,provider(Path(folder)/'worker')as(worker,remote):
            def slow(identity,cpus,**options):time.sleep(.3);return dict(cpus=1,memory_mb=256)
            with patch.object(worker,'resize',side_effect=slow),resource_window(.05):
                started=time.monotonic()
                with self.assertRaises(TimeoutError):
                    remote.resize('a'*32,4)
                self.assertLess(time.monotonic()-started,.25)

    def test_resource_window_is_nested_and_restores_original_deadline(self):
        with patch.dict(os.environ,REMOTE_COMPUTE_DEADLINE='100'),patch('remote_compute.deadline.time.monotonic',return_value=90):
            self.assertEqual(remaining_time(),10)
            with resource_window(3):
                self.assertEqual(remaining_time(),3)
                with resource_window(20):self.assertEqual(remaining_time(),3)
                with resource_window(1):self.assertEqual(remaining_time(),1)
            self.assertEqual(remaining_time(),10)
            with resource_window(None):self.assertEqual(remaining_time(),10)
            with resource_window(20):self.assertEqual(remaining_time(),10)
