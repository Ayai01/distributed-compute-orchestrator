"""A crashed center reattaches the original worker process and grants."""
import json
import shutil
import time
from pathlib import Path
import subprocess
import sys
import threading
import unittest
from unittest.mock import patch

from remote_compute import Client, Pool, RemoteError
from test_center import center
from test_compute import provider,python,scratch,until


class RecoveryTests(unittest.TestCase):
    def test_status_snapshot_hides_interrupted_commit_until_retry_decision(self):
        from remote_compute.bundle import atomic_json
        blocked, release = threading.Event(), threading.Event()

        def slow_commit(path, value):
            if value['status'] == 'interrupted':
                blocked.set()
                if not release.wait(6):
                    raise AssertionError('Test did not release the retry commit')
            return atomic_json(path, value)

        with scratch() as folder, provider(Path(folder) / 'lost') as (_, lost), \
                provider(Path(folder) / 'healthy') as (_, healthy):
            with Pool([lost, healthy], policy='round_robin') as pool, \
                    center(pool, Path(folder) / 'center') as (_, client):
                def unavailable(*args, **kwargs):
                    pool.nodes[0].enabled = False
                    return dict(status='interrupted', error='Worker stopped', finished=time.time())
                try:
                    with patch.object(lost, 'submit_bundle', return_value={}), \
                            patch.object(lost, 'status', side_effect=unavailable), \
                            patch('remote_compute.coordinator.atomic_json', side_effect=slow_commit):
                        accepted = client.submit(python("print('original-result')"))
                        identity = accepted['id']
                        self.assertTrue(blocked.wait(3))
                        before = time.monotonic()
                        self.assertNotEqual(client.status(identity)['status'], 'interrupted')
                        self.assertLess(time.monotonic() - before, 1.5)
                        release.set()
                        result = client.wait(identity, wait_timeout=10)
                        self.assertEqual(result['status'], 'succeeded', result)
                        self.assertEqual(result['request_sha256'], accepted['request_sha256'])
                        self.assertEqual(len(result['attempts']), 1)
                        self.assertEqual(client.fetch(identity)['stdout'].strip(), 'original-result')
                finally:
                    release.set()

    def test_reconnected_gpu_without_local_reservation_stays_accounted_after_profile_returns(self):
        from types import SimpleNamespace
        from remote_compute.client import Node
        node = Node('worker', SimpleNamespace(url='test'))
        node.known_jobs.add('recovered')
        node.live_jobs.add('recovered')
        job = dict(id='recovered', status='running', accelerator=dict(kind='cuda'))
        node.refresh_external_accelerators([job])
        self.assertFalse(node.external_accelerator_reservations)
        node.health['accelerators'] = [dict(kind='cuda', count=1, memory_mb=8192)]
        for _ in range(2):
            node.refresh_external_accelerators([job])
            self.assertEqual(len(node.external_accelerator_reservations), 1)
            self.assertFalse(node.can_match_accelerator(dict(kind='cuda')))
        node.live_jobs.remove('recovered')
        node.refresh_external_accelerators([job])  # A stale sample after completion.
        self.assertFalse(node.external_accelerator_reservations)
        self.assertTrue(node.can_match_accelerator(dict(kind='cuda')))
        node.reserve_accelerator(dict(kind='cuda'), 'owned')
        node.live_jobs.add('owned')
        node.refresh_external_accelerators([dict(job, id='owned')])
        self.assertFalse(node.external_accelerator_reservations)

    def test_cpu_fallback_survives_retry_when_gpu_becomes_an_available_destination(self):
        with scratch() as temporary:
            root = Path(temporary)
            with provider(root/'lost') as (_, lost), provider(root/'gpu',
                    accelerators=[dict(kind='cuda', count=1, memory_mb=8192, ids=['0'])]) as (_, healthy):
                with Pool([lost, healthy], policy='round_robin') as pool, center(pool, root/'center') as (_, client):
                    def unavailable(*args, **kwargs):
                        pool.nodes[0].enabled = False
                        return dict(status='interrupted', error='Worker stopped', finished=time.time())
                    with patch.object(lost, 'submit_bundle', return_value={}), patch.object(
                            lost, 'status', side_effect=unavailable):
                        accepted = client.submit(python(
                            "import os;print(os.environ['DCO_ACCELERATOR_COUNT'])",
                            accelerator=dict(kind='cuda', fallback_after_seconds=0)))
                        result = client.wait(accepted['id'], wait_timeout=10)
                    self.assertEqual(result['status'], 'succeeded', result)
                    self.assertEqual(result['id'], accepted['id'])
                    self.assertEqual(result['request_sha256'], accepted['request_sha256'])
                    self.assertEqual(len(result['attempts']), 1)
                    self.assertIsNone(result['accelerator'])
                    self.assertEqual(result['stdout'].strip(), '0')
                    self.assertEqual(result['accelerator_fallback']['reason'], 'queue_timeout')

    def test_cpu_fallback_survives_queued_restart_and_running_reattachment(self):
        with scratch() as temporary, provider(Path(temporary)/'worker') as (worker, remote):
            root = Path(temporary)
            task = python("import os;print(os.environ['DCO_ACCELERATOR_COUNT'])",
                          accelerator=dict(kind='cuda', fallback_after_seconds=30))
            with Pool([remote]) as pool, center(pool, root/'center') as (state, client):
                finished = client.submit(task)
                self.assertEqual(client.wait(finished['id'])['status'], 'succeeded')
                client.control(paused=True)
                queued = client.submit(task)
                with state.condition:
                    state._fallback_accelerator(state.jobs[queued['id']])
                for accepted in (finished, queued):
                    self.assertIsNone(client.status(accepted['id'])['accelerator'])
            for accepted, status in ((finished, 'running'), (queued, 'queued')):
                path = root/'center/jobs'/accepted['id']/'result.json'
                result = json.loads(path.read_text())
                result.update(status=status, finished=None)
                path.write_text(json.dumps(result))
            from remote_compute.policy import normalize_accelerators
            worker.accelerators = normalize_accelerators([
                dict(kind='cuda', count=1, memory_mb=8192, ids=['0'])])
            with Pool([remote]) as pool, center(pool, root/'center') as (_, client):
                for accepted in (finished, queued):
                    result = client.wait(accepted['id'], wait_timeout=10)
                    self.assertEqual(result['status'], 'succeeded', result)
                    self.assertEqual(result['request_sha256'], accepted['request_sha256'])
                    self.assertIsNone(result['accelerator'])
                    self.assertEqual(result['accelerator_fallback']['reason'], 'unavailable')
                    self.assertEqual(result['stdout'].strip(), '0')
                    self.assertFalse(result['attempts'])

    def test_lost_worker_retries_original_request_on_healthy_worker(self):
        for failure in ('missing','interrupted'):
            with self.subTest(failure=failure),scratch() as temporary:
                root=Path(temporary)
                with provider(root/'lost') as (_,lost),provider(root/'healthy') as (_,healthy):
                    with Pool([lost,healthy],policy='round_robin') as pool,center(pool,root/'center') as (_,client):
                        broken=pool.nodes[0]
                        def unavailable(*args,**kwargs):
                            broken.enabled=False
                            if failure=='interrupted':
                                return dict(status='interrupted',error='Worker stopped before completion',finished=time.time())
                            error=RemoteError('worker unavailable')
                            error.status_code=404
                            raise error
                        with patch.object(lost,'submit_bundle',return_value={}),patch.object(lost,'status',side_effect=unavailable):
                            task=python("from pathlib import Path;Path('answer').write_text('42')",outputs=['answer'])
                            accepted=client.submit(task)
                            result=client.wait(accepted['id'],wait_timeout=10)
                            self.assertEqual(result['status'],'succeeded',result)
                            self.assertEqual(result['max_retries'],2)
                            self.assertEqual(result['id'],accepted['id'])
                            self.assertEqual(result['request_sha256'],accepted['request_sha256'])
                            self.assertEqual(result['assigned_node'],'node2')
                            self.assertEqual(len(result['attempts']),1)
                            self.assertNotEqual(result['worker_id'],accepted['id'])
                            # A delayed worker sample must not show a retried job
                            # again as unrelated work or hide a real direct job.
                            snapshot=pool.snapshot()
                            second=snapshot['nodes'][1]
                            second.update(external_cpus=1,active_jobs=[
                                dict(id=result['worker_id'],cpus=1),dict(id='direct',cpus=1)])
                            with patch.object(pool,'snapshot',return_value=snapshot):
                                self.assertEqual([j['id'] for j in client.health()['nodes'][1]['active_jobs']],['direct'])
                            self.assertEqual(client.fetch(accepted['id'])['files']['answer'],b'42')
                            until(lambda:sum(n.reserved for n in pool.nodes)==0)

    def test_temporarily_refused_worker_keeps_the_original_process_and_grant(self):
        for code in (111,10061):
            with self.subTest(code=code),scratch() as temporary,provider(Path(temporary)/'worker') as (_,remote):
                with Pool([remote],refresh=60) as pool,center(pool,Path(temporary)/'center') as (_,client):
                    original=remote.status
                    disconnected=threading.Event();restored=threading.Event()
                    def status(*args,**kwargs):
                        if not restored.is_set():
                            disconnected.set()
                            error=RemoteError('worker port temporarily refused connection')
                            error.transport_code=code
                            raise error
                        return original(*args,**kwargs)
                    with patch.object(remote,'status',side_effect=status):
                        identity=client.submit(python("from pathlib import Path;import time\nwhile not Path('release').exists():time.sleep(.02)\nprint('original')"))['id']
                        try:
                            self.assertTrue(disconnected.wait(3))
                            until(lambda:'connection_error' in client.status(identity))
                            self.assertNotIn(client.status(identity)['status'],('interrupted','queued'))
                            self.assertEqual(pool.nodes[0].reserved,1)
                            self.assertEqual(client.status(identity)['attempts'],[])
                            remote.write_job_file(identity,'release','done')
                            restored.set()
                            result=client.wait(identity,wait_timeout=5)
                            self.assertEqual(result['status'],'succeeded',result)
                            self.assertEqual(result['attempts'],[])
                            self.assertEqual(client.fetch(identity)['stdout'].strip(),'original')
                            until(lambda:pool.nodes[0].reserved==0)
                        finally:
                            restored.set()

    def test_retry_limit_and_machine_operation_opt_out(self):
        with scratch() as temporary,provider(Path(temporary)/'worker') as (_,node):
            root=Path(temporary)
            with Pool([node],refresh=.05) as pool,center(pool,root/'center') as (_,client):
                def stopped(*args,**kwargs):
                    return dict(status='interrupted',error='Worker stopped before completion',finished=time.time())
                with patch.object(node,'submit_bundle',return_value={}),patch.object(node,'status',side_effect=stopped):
                    identity=client.submit(python('pass',retries=1))['id']
                    result=client.wait(identity,wait_timeout=5)
                    self.assertEqual(result['status'],'interrupted')
                    self.assertEqual(len(result['attempts']),1)
                    client.delete(identity)
                    accepted=client.machine('node1').python('pass',wait=False)
                    result=client.wait(accepted['id'],wait_timeout=5)
                    self.assertEqual(result['max_retries'],0)
                    self.assertEqual(result['attempts'],[])
                    client.delete(accepted['id'])

    def test_center_recovers_queued_and_completed_uncollected_jobs(self):
        with scratch() as temporary,provider(Path(temporary)/'worker') as (_,node):
            root=Path(temporary)
            with Pool([node]) as pool,center(pool,root/'center') as (_,client):
                finished=client.submit(python("print('original result')"))['id']
                client.wait(finished)
                client.control(paused=True)
                queued=client.submit(python("print('queued result')"))['id']
            # Persist exactly the two crash states, without touching worker data.
            for identity,status in ((finished,'running'),(queued,'queued')):
                path=root/'center/jobs'/identity/'result.json'
                result=json.loads(path.read_text())
                result.update(status=status,finished=None)
                result.pop('max_retries',None)  # Upgrade an existing 0.8.2 receipt.
                path.write_text(json.dumps(result))
            with Pool([node]) as pool,center(pool,root/'center') as (_,client):
                for identity,output in ((finished,'original result'),(queued,'queued result')):
                    self.assertEqual(client.wait(identity,wait_timeout=10)['status'],'succeeded')
                    result=client.fetch(identity)
                    self.assertEqual(result['stdout'].strip(),output)
                    self.assertEqual(result['max_retries'],2)
                    self.assertFalse(result['attempts'])

    def test_persistent_worker_results_survive_loss_of_temporary_volume(self):
        with scratch() as temporary:
            root=Path(temporary);state=root/'persistent';ram=root/'ram'
            with provider(state,temp_root=ram) as (_,client):
                identity=client.submit(python("from pathlib import Path;Path('checkpoint').write_text('saved')",outputs=['checkpoint']))['id']
                self.assertEqual(client.wait(identity)['status'],'succeeded')
            shutil.rmtree(ram)
            with provider(state,temp_root=ram) as (_,client):
                result=client.fetch(identity)
                self.assertEqual(result['status'],'succeeded')
                self.assertEqual(result['files']['checkpoint'],b'saved')

    def test_center_crash_keeps_original_id_process_and_resource_resize(self):
        with scratch() as temporary,provider(Path(temporary)/'worker',cpus=2) as (worker,node):
            root=Path(temporary);stop=root/'stop'
            code='''import json,sys,time
from pathlib import Path
from remote_compute import Pool
from remote_compute.coordinator import Coordinator
from remote_compute.worker import Server
import threading
root=Path(sys.argv[2])
pool=Pool([dict(name='original',url=sys.argv[1])],refresh=.1)
state=Coordinator(pool,root/'center')
server=Server(('127.0.0.1',0),state)
threading.Thread(target=server.serve_forever,daemon=True).start()
print(json.dumps(dict(url='http://127.0.0.1:'+str(server.server_port),operator_token=state.operator_token)),flush=True)
try:
 while not (root/'stop').exists():time.sleep(.02)
finally:
 state.close();server.shutdown();server.server_close();pool.close()
'''
            def start():
                process=subprocess.Popen([sys.executable,'-B','-c',code,node.url,str(root)],
                    stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,encoding='utf-8')
                info=json.loads(process.stdout.readline())
                return process,Client(info['url'], operator=True, operator_token=info['operator_token'])
            process,client=start()
            try:
                task=python("import os,time;from pathlib import Path\nPath('pid.txt').write_text(str(os.getpid()))\nwhile not Path('release').exists():time.sleep(.02)\nprint('once')",
                            cpus=0,max_cpus=2,memory_mb=128,outputs=['pid.txt'],project='recovery')
                accepted=client.submit(task);identity=accepted['id']
                until(lambda:identity in worker.jobs and worker.get(identity)['status']=='running')
                until(lambda:(worker.jobs[identity]['directory']/'input/pid.txt').exists())
                original_pid=client.read_job_file(identity,'pid.txt')
                process.kill();process.communicate()
                process,client=start()
                until(lambda:client.status(identity)['status']=='running')
                self.assertEqual(client.read_job_file(identity,'pid.txt'),original_pid)
                health=client.health()
                self.assertEqual((health['used_cpus'],health['projects']['recovery']['used']),(2,2))
                self.assertEqual(health['nodes'][0]['external_cpus'],0)
                grant=client.resize(identity,1,memory_mb=64)
                self.assertEqual((grant['cpus'],grant['memory_mb']),(1,64))
                self.assertEqual(list(worker.jobs),[identity])
                client.write_job_file(identity,'release','go');client.wait(identity)
                result=client.fetch(identity)
                self.assertEqual(result['stdout'].strip(),'once')
                self.assertEqual(result['request_sha256'],accepted['request_sha256'])
                self.assertEqual(client.health()['used_cpus'],0)
            finally:
                stop.write_text('stop')
                try:process.communicate(timeout=10)
                except subprocess.TimeoutExpired:process.kill();process.communicate()
