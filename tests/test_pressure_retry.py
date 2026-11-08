"""Measured-pressure recovery keeps the original identity, fees and prefixes."""
import io
import json
from pathlib import Path
import threading
import time
import unittest
import zipfile
from remote_compute import Client, Pool, command, python_map
from remote_compute.memory import pressure_victim
from test_compute import provider, scratch, until
from test_center import center


class PressureRetryTests(unittest.TestCase):
    def task(self, delay_first=True, retries=2):
        code=('import json,pathlib,sys,time\n'
              'p=pathlib.Path(sys.argv[1])/"count"\n'
              'n=int(p.read_text())+1 if p.exists() else 1\n'
              'p.write_text(str(n))\n'
              'pathlib.Path("prefix.json").write_text(json.dumps(dict(attempt=n)))\n'
              'end=time.process_time()+.05\n'
              'while time.process_time()<end: pass\n'
              + ('time.sleep(20) if n==1 else None\n' if delay_first else 'time.sleep(20)\n')
              + 'print("complete")\n')
        return command(['${tool:python}','-B','-c',code,'${shared:state}'],
                       retries=retries, max_cpus=1, outputs=['prefix.json'], timeout=30)

    def stop(self, worker, identity):
        with worker.condition:
            job=worker.jobs[identity]
            job['result'].update(error_kind='memory_pressure',error='Task terminated under critical host RAM/commit pressure',
                                 memory_pressure=dict(memory_total_mb=48907,host_memory_available_mb=11786,host_commit_available_mb=908))
            job['memory_pressure']=True
            job['cancel'].set()

    def test_actual_host_measurements_alone_trigger_protection(self):
        job=dict(task=dict(priority=0,memory_mb=1024**3),result=dict(status='running'),
                 rss_mb=96,private_mb=95,cancel=threading.Event())
        host=dict(memory_total_mb=48907,host_memory_available_mb=11786,host_commit_available_mb=8192)
        self.assertIsNone(pressure_victim([job],host))
        host['host_commit_available_mb']=908
        self.assertIs(pressure_victim([job],host),job)

    def test_default_retries_recover_pressure_under_same_public_id(self):
        with scratch() as temporary:
            root=Path(temporary);data=root/'state';data.mkdir()
            with provider(root/'worker',shared={'state':data})as(worker,remote),Pool([remote])as pool,center(pool,root/'center')as(_,operator):
                client=Client(operator.url);task=self.task();accepted=client.submit(task);identity=accepted['id']
                until(lambda:(data/'count').exists())
                self.stop(worker,identity)
                result=client.wait(identity,wait_timeout=12)
                self.assertEqual(result['status'],'succeeded',result)
                self.assertEqual(result['id'],identity)
                self.assertEqual(result['request_sha256'],accepted['request_sha256'])
                self.assertEqual(result['max_retries'],2)
                self.assertEqual(len(result['attempts']),1)
                self.assertEqual(result['attempts'][0]['error_kind'],'memory_pressure')
                self.assertAlmostEqual(result['total_cpu_seconds'],result['cpu_seconds']+result['attempts'][0]['cpu_seconds'])
                receipt=client.fetch(identity,root/'fetched',delete=False)
                archived=root/'fetched/files'/receipt['attempts'][0]['artifact']
                with zipfile.ZipFile(archived)as archive:
                    self.assertEqual(json.loads(archive.read('files/prefix.json'))['attempt'],1)
                    self.assertNotIn('worker_url',json.loads(archive.read('result.json')))
                self.assertEqual(json.loads((root/'fetched/files/prefix.json').read_bytes())['attempt'],2)
                old=operator.status(identity)['attempts'][0]['worker_id']
                client.delete(identity)
                self.assertEqual(remote.statuses([old])[old]['status'],'unknown')

    def test_explicit_zero_retries_keeps_the_frozen_compute_budget(self):
        self.assertNotIn('retries',python_map('m:f',[1]))
        self.assertEqual(python_map('m:f',[1],retries=0)['retries'],0)
        with scratch()as temporary:
            root=Path(temporary);data=root/'state';data.mkdir()
            with provider(root/'worker',shared={'state':data})as(worker,remote),Pool([remote])as pool,center(pool,root/'center')as(_,client):
                identity=client.submit(self.task(retries=0))['id']
                until(lambda:(data/'count').exists());self.stop(worker,identity)
                result=client.wait(identity,wait_timeout=8)
                self.assertEqual(result['status'],'interrupted',result)
                self.assertEqual(result['attempts'],[])
                self.assertEqual((data/'count').read_text(),'1')

    def test_persistent_pressure_exhausts_two_infrastructure_retries(self):
        with scratch() as temporary:
            root=Path(temporary);data=root/'state';data.mkdir()
            with provider(root/'worker',shared={'state':data})as(worker,remote),Pool([remote])as pool,center(pool,root/'center')as(_,client):
                identity=client.submit(self.task(delay_first=False))['id'];stopped=set();deadline=time.monotonic()+12
                while time.monotonic()<deadline:
                    for row in worker.health()['active_jobs']:
                        wid=row['id']
                        if row['status']=='running' and wid not in stopped and (worker.root/'jobs'/wid/'input/prefix.json').exists():
                            self.stop(worker,wid);stopped.add(wid)
                    value=client.status(identity)
                    if value['status'] in ('failed','interrupted','succeeded'):break
                    time.sleep(.02)
                self.assertEqual(value['status'],'interrupted',value)
                self.assertEqual(len(value['attempts']),2)
                self.assertEqual(len(stopped),3)
                self.assertEqual(value['infrastructure_retry_limit'],2)


if __name__=='__main__':unittest.main()
