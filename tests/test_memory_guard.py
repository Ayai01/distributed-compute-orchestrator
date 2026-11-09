"""Real pressure drains at safe boundaries; recovery preserves paid items."""
import json,os,threading,time,unittest
from pathlib import Path
from unittest.mock import Mock,patch
from remote_compute import Pool,python_task,python_map
from remote_compute.memory import PressureGuard,pressure_victim,memory_boundary
from test_compute import provider,scratch,until
from test_center import center
import test_pressure_retry as pressure_fixtures


class MemoryGuardTests(unittest.TestCase):
    def host(self,commit=8192,physical=16000):
        return dict(memory_total_mb=48907,host_memory_available_mb=physical,host_commit_available_mb=commit)

    def test_spare_physical_ram_and_low_commit_warn_then_continue_without_a_kill(self):
        guard=PressureGuard();host=self.host(commit=380)
        first=guard.update(host,0);self.assertTrue(first['limited']);self.assertTrue(first['waiting'])
        settled=guard.update(host,3)
        self.assertTrue(settled['limited']);self.assertFalse(settled['waiting']);self.assertFalse(settled['terminate'])
        self.assertIsNone(pressure_victim([],host))
        recovered=guard.update(self.host(),4);self.assertTrue(recovered['waiting'])
        self.assertFalse(guard.update(self.host(),6)['limited'])

    def test_transient_emergency_does_not_kill_but_sustained_or_exhausted_does(self):
        guard=PressureGuard()
        self.assertFalse(guard.update(self.host(commit=64),0)['terminate'])
        self.assertFalse(guard.update(self.host(),.1)['terminate'])
        self.assertFalse(guard.update(self.host(commit=64),1)['terminate'])
        self.assertTrue(guard.update(self.host(commit=64),3)['terminate'])
        self.assertTrue(PressureGuard().update(self.host(commit=16),0)['terminate'])
        self.assertTrue(guard.update(self.host(physical=64),6)['terminate'])

    def test_boundary_waits_without_borrowing_and_keeps_the_original_deadline(self):
        with scratch()as root:
            root=Path(root);gate=root/'pause';gate.touch();finished=threading.Event()
            with patch.dict(os.environ,REMOTE_COMPUTE_MEMORY_WAIT=str(gate),REMOTE_COMPUTE_JOB_DIR=str(root),REMOTE_COMPUTE_DEADLINE=str(time.monotonic()+3)):
                thread=threading.Thread(target=lambda:(memory_boundary(),finished.set()));thread.start()
                until(lambda:(root/'input/.dco-memory-waits').is_dir())
                self.assertFalse(finished.wait(.1));gate.unlink();thread.join(2);self.assertTrue(finished.is_set())
                self.assertEqual(list((root/'input/.dco-memory-waits').iterdir()),[])
                gate=root/'pause-again';gate.touch()
                with patch.dict(os.environ,REMOTE_COMPUTE_MEMORY_WAIT=str(gate),REMOTE_COMPUTE_DEADLINE=str(time.monotonic()+.15)):
                    with self.assertRaises(TimeoutError):memory_boundary()
                self.assertTrue(any((root/'input/.dco-memory-waits').iterdir()))

    def test_soft_commit_warning_stops_new_admission_without_terminating_running_work(self):
        with scratch()as root,provider(Path(root)/'worker',cpus=2,memory_headroom_mb=0)as(worker,client):
            host=self.host();worker.host.sample=Mock(side_effect=lambda:dict(host))
            task=python_task('work:one',max_cpus=2,retries=0,timeout=15)
            code="from pathlib import Path;import time\ndef one():\n Path('started').write_text('yes')\n while not Path('release').exists():time.sleep(.02)\n return 1\n"
            identity=client.submit(task,files={'work.py':code})['id']
            try:
                until(lambda:(worker.jobs[identity]['directory']/'input/started').exists())
                host['host_commit_available_mb']=380
                until(lambda:worker.pressure_guard.state['limited'])
                second=client.submit(task,files={'work.py':code})['id']
                time.sleep(2.2)
                self.assertEqual(client.status(identity)['status'],'running')
                self.assertEqual(client.status(second)['status'],'queued')
                self.assertFalse(worker.jobs[identity]['cancel'].is_set())
                reply=worker.resize(identity,2,active_cpus=1)
                self.assertEqual(reply['cpus'],1)
                host['host_commit_available_mb']=8192
                until(lambda:not worker.pressure_guard.state['limited'])
                client.write_job_file(identity,'release','go');client.wait(identity)
                until(lambda:(worker.jobs[second]['directory']/'input/started').exists())
                client.write_job_file(second,'release','go');self.assertEqual(client.wait(second)['status'],'succeeded')
            finally:
                host['host_commit_available_mb']=8192
                for i in [j['result']['id']for j in worker.jobs.active.values()]:
                    client.cancel(i);client.wait(i)

    def test_resource_zero_retries_requeues_same_map_id_and_keeps_published_successes(self):
        with scratch()as root:
            root=Path(root);state=root/'counts';state.mkdir()
            with provider(root/'worker',cpus=1,shared={'counts':state})as(worker,remote),Pool([remote])as pool,center(pool,root/'center')as(_,client),patch('remote_compute.coordinator.RESOURCE_RECOVERY_DELAYS',(.1,.2)):
                code='''from pathlib import Path
import time
def one(i,counts):
 p=Path(counts)/str(i);n=int(p.read_text())+1 if p.exists()else 1;p.write_text(str(n))
 Path('item%s.txt'%i).write_text(str(i))
 if i==1 and n==1:time.sleep(20)
 return i
'''
                task=python_map('work:one',[0,1,2],kwargs={'counts':'${shared:counts}'},max_cpus=1,retries=0,timeout=25,outputs=['item*.txt'])
                accepted=client.submit(task,files={'work.py':code});identity=accepted['id']
                until(lambda:(state/'1').exists())
                pressure_fixtures.PressureRetryTests().stop(worker,identity)
                result=client.wait(identity,wait_timeout=10)
                self.assertEqual(result['status'],'succeeded',result)
                self.assertEqual(result['id'],identity);self.assertEqual(result['request_sha256'],accepted['request_sha256'])
                self.assertEqual(result['max_retries'],0);self.assertEqual(result['resource_recovery_limit'],2)
                self.assertEqual(len(result['attempts']),1)
                self.assertEqual([(state/str(i)).read_text()for i in range(3)],['1','2','1'])
                receipt=client.fetch(identity,delete=False)
                execution=json.loads(receipt['files']['.dco-results.json'])
                self.assertEqual([r['value']for r in execution['items']],[0,1,2])
                self.assertEqual(execution['item_states'][0]['resource_recovered'],True)
                self.assertEqual(receipt['files']['item0.txt'],b'0')
                self.assertGreater(result['total_cpu_seconds'],0)

    def test_second_interruption_before_the_entry_retains_first_success_and_its_file(self):
        with scratch()as root:
            root=Path(root);counts=root/'counts';counts.mkdir()
            with provider(root/'worker',cpus=1,shared={'counts':counts})as(worker,remote),Pool([remote])as pool,center(pool,root/'center')as(_,client),patch('remote_compute.coordinator.RESOURCE_RECOVERY_DELAYS',(.1,.2)):
                code='''from pathlib import Path
import time
def one(i,counts):
 p=Path(counts)/str(i);n=int(p.read_text())+1 if p.exists()else 1;p.write_text(str(n))
 Path('item%s.txt'%i).write_text(str(i))
 if i==1 and n==1:time.sleep(20)
 return i
'''
                identity=client.submit(python_map('work:one',[0,1,2],kwargs={'counts':'${shared:counts}'},max_cpus=1,retries=0,timeout=25,outputs=['item*.txt']),files={'work.py':code})['id']
                until(lambda:(counts/'1').exists())
                original=worker._automatic_grant;before_entry=[]
                def automatic(job):
                    if job['result']['id']!=identity and not before_entry:
                        before_entry.append(job['result']['id'])
                        pressure_fixtures.PressureRetryTests().stop(worker,job['result']['id'])
                    return original(job)
                with patch.object(worker,'_automatic_grant',side_effect=automatic):
                    pressure_fixtures.PressureRetryTests().stop(worker,identity)
                    result=client.wait(identity,wait_timeout=12)
                self.assertEqual(result['status'],'succeeded',result)
                self.assertEqual(len(result['attempts']),2)
                self.assertEqual([(counts/str(i)).read_text()for i in range(3)],['1','2','1'])
                receipt=client.fetch(identity)
                self.assertEqual(receipt['files']['item0.txt'],b'0')
                rows=json.loads(receipt['files']['.dco-results.json'])
                self.assertTrue(rows['item_states'][0]['resource_recovered'])

    def test_resource_reentry_wait_can_be_cancelled_without_restarting_the_body(self):
        with scratch()as root:
            root=Path(root);counts=root/'counts';counts.mkdir()
            with provider(root/'worker',shared={'counts':counts})as(worker,remote),Pool([remote])as pool,center(pool,root/'center')as(_,client):
                code="from pathlib import Path;import time\ndef one(counts):\n p=Path(counts)/'calls';p.write_text('called');time.sleep(20)\n"
                identity=client.submit(python_task('work:one',kwargs={'counts':'${shared:counts}'},max_cpus=1,retries=0,timeout=25),files={'work.py':code})['id']
                until(lambda:(counts/'calls').exists())
                pressure_fixtures.PressureRetryTests().stop(worker,identity)
                until(lambda:client.status(identity)['status']=='queued')
                queued=client.status(identity);self.assertGreater(queued['resource_retry_after'],time.time())
                self.assertEqual(len(queued['attempts']),1)
                client.cancel(identity);result=client.wait(identity,wait_timeout=3)
                self.assertEqual(result['status'],'cancelled')
                self.assertEqual(result['resource_recovery_count'],1)
                self.assertEqual(worker.health()['used_cpus'],0)

    def test_resource_zero_retries_still_has_a_finite_two_recovery_limit(self):
        with scratch()as root:
            root=Path(root)
            with provider(root/'worker')as(worker,remote),Pool([remote])as pool,center(pool,root/'center')as(_,client),patch('remote_compute.coordinator.RESOURCE_RECOVERY_DELAYS',(.1,.2)):
                identity=client.submit(python_task('work:one',max_cpus=1,retries=0,timeout=25),files={'work.py':'import time\ndef one():time.sleep(20)\n'})['id']
                stopped=set();deadline=time.monotonic()+10
                while time.monotonic()<deadline:
                    for job in list(worker.jobs.active.values()):
                        i=job['result']['id']
                        if job['result']['status']=='running'and i not in stopped:
                            pressure_fixtures.PressureRetryTests().stop(worker,i);stopped.add(i)
                    result=client.status(identity)
                    if result['status']in('interrupted','failed','succeeded'):break
                    time.sleep(.02)
                self.assertEqual(result['status'],'interrupted',result)
                self.assertEqual(len(result['attempts']),2)
                self.assertEqual(result['max_retries'],0)
                self.assertEqual(len(stopped),3)
                self.assertEqual(client.fetch(identity)['status'],'interrupted')

    def test_missing_or_empty_later_manifest_keeps_completed_state_and_files(self):
        from remote_compute.bundle import pack
        from remote_compute.recovery import resume_map,carry_outputs,restore_map_results
        task=python_map('work:one',[0,1],max_cpus=1)
        row=dict(index=0,status='succeeded',value=7,error=None)
        previous=dict(id='original',request_sha256='frozen',function='work:one',total=2,items=[row])
        for state in (None,dict(items=[])):
            payload={'files/item0.txt':b'paid output','files/frozen.py':b'changed'}
            if state is not None:payload['files/.dco-results.json']=json.dumps(state).encode()
            raw=pack(payload)
            recovered=resume_map(task,'original','frozen',raw,previous)
            self.assertEqual(recovered,previous)
            inputs={'input/frozen.py':b'original'};carry_outputs(inputs,raw)
            self.assertEqual(inputs['input/item0.txt'],b'paid output')
            self.assertEqual(inputs['input/frozen.py'],b'original')
            files={'files/.dco-results.json':json.dumps(dict(items=[],item_states=[])).encode()}
            restore_map_results(files,recovered,status='interrupted')
            final=json.loads(files['files/.dco-results.json'])
            self.assertEqual(final['items'],[row]);self.assertEqual(final['completed'],1)

    def test_recovery_queue_waits_for_actual_host_recovery(self):
        with scratch()as root:
            root=Path(root);counts=root/'counts';counts.mkdir()
            with provider(root/'worker',shared={'counts':counts},memory_headroom_mb=0)as(worker,remote),Pool([remote])as pool,center(pool,root/'center')as(_,client),patch('remote_compute.coordinator.RESOURCE_RECOVERY_DELAYS',(.1,.2)):
                host=self.host();worker.host.sample=Mock(side_effect=lambda:dict(host))
                code="from pathlib import Path;import time\ndef one(counts):\n p=Path(counts)/'calls';n=int(p.read_text())+1 if p.exists()else 1;p.write_text(str(n))\n if n==1:time.sleep(20)\n return n\n"
                identity=client.submit(python_task('work:one',kwargs={'counts':'${shared:counts}'},max_cpus=1,retries=0,timeout=25),files={'work.py':code})['id']
                until(lambda:(counts/'calls').exists())
                host['host_commit_available_mb']=380
                until(lambda:worker.pressure_guard.state['limited']);pool.refresh()
                pressure_fixtures.PressureRetryTests().stop(worker,identity)
                until(lambda:client.status(identity)['status']=='queued')
                time.sleep(.35)
                self.assertEqual(client.status(identity)['status'],'queued')
                self.assertEqual((counts/'calls').read_text(),'1')
                host['host_commit_available_mb']=8192
                result=client.wait(identity,wait_timeout=8)
                self.assertEqual(result['status'],'succeeded',result)
                self.assertEqual((counts/'calls').read_text(),'2')

    def test_command_wait_deadline_does_not_start_another_resource_rpc(self):
        from test_compute import python
        with scratch()as root,provider(Path(root)/'worker',memory_headroom_mb=0)as(worker,client):
            host=self.host();worker.host.sample=Mock(side_effect=lambda:dict(host))
            task=python("from pathlib import Path;import time;Path('ready').touch()\nwhile not Path('release').exists():time.sleep(.01)",timeout=1.5,retries=0)
            task['steps'].append(['${tool:python}','-c',"raise AssertionError('past deadline')"])
            calls=[];original=worker._automatic_grant
            def automatic(job):calls.append(True);return original(job)
            with patch.object(worker,'_automatic_grant',side_effect=automatic):
                identity=client.submit(task)['id']
                until(lambda:(worker.jobs[identity]['directory']/'input/ready').exists())
                host['host_commit_available_mb']=64
                until(lambda:worker.pressure_guard.state['waiting'])
                client.write_job_file(identity,'release','go')
                result=client.wait(identity,wait_timeout=4)
            self.assertEqual(result['error_kind'],'memory_pressure')
            self.assertEqual(len(calls),1)
            self.assertEqual(len(result['steps']),1)
