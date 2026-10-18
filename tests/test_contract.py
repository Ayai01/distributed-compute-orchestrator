"""Generic capability routing, bounded sampling and reusable calibration."""
import json
from pathlib import Path
import threading
import time
import unittest
from unittest.mock import patch

from remote_compute import Client, Pool
from remote_compute.benchmark import calibrated, NAME
from test_compute import provider, python, scratch, until
from test_center import center


class ContractTests(unittest.TestCase):
    def test_project_view_is_aggregate_and_rejects_node_selection(self):
        with scratch() as directory, provider(Path(directory) / 'worker', cpus=2, tags=['rtl']) as (_, remote):
            with Pool([remote]) as pool, center(pool, Path(directory) / 'center') as (_, operator):
                project = Client(operator.url)
                capabilities = project.capabilities()
                self.assertNotIn('nodes', capabilities)
                self.assertNotIn('node', capabilities)
                self.assertEqual(capabilities['cpu']['capacity'], 2)
                self.assertIn('rtl', capabilities['tags'])
                public_health = project.health()
                self.assertNotIn('nodes', public_health)
                self.assertNotIn('projects', public_health)
                with self.assertRaisesRegex(Exception, 'operator-only'):
                    project.nodes()
                forged = Client(operator.url, operator=True)
                with self.assertRaisesRegex(Exception, 'Operator token'):
                    forged.nodes()
                with self.assertRaisesRegex(Exception, 'node selection'):
                    project.submit(python('print(1)', node='worker'))
                result = project.run(python('print(2)'))
                self.assertEqual(result['status'], 'succeeded', result)
                self.assertNotIn('assigned_node', result)
                self.assertNotIn('worker_url', result)

    def test_project_view_bounds_optional_scheduler_hints(self):
        with scratch() as directory, provider(Path(directory) / 'worker') as (_, remote):
            with Pool([remote]) as pool, center(pool, Path(directory) / 'center') as (_, operator):
                project = Client(operator.url)
                with self.assertRaisesRegex(Exception, 'queue_ratio'):
                    project.admission('bounded', queue_ratio=5)
                with self.assertRaisesRegex(Exception, 'resource_share'):
                    project.admission('bounded', resource_share=17)

    def test_accelerator_request_uses_aggregate_profiles_and_serializes_gpu_jobs(self):
        with scratch() as directory, provider(
                Path(directory) / 'gpu', cpus=2,
                accelerators=[dict(kind='cuda', count=1, memory_mb=8192,
                                   tags=['gpu', 'gpu:training'], ids=['0'])]) as (_, remote):
            with Pool([remote]) as pool, center(pool, Path(directory) / 'center') as (_, operator):
                project = Client(operator.url)
                capabilities = project.capabilities()
                self.assertEqual(capabilities['accelerators'], [
                    dict(kind='cuda', count=1, memory_mb=8192, tags=['gpu', 'gpu:training'])])
                request = dict(kind='cuda', memory_mb=4096, requires=['gpu:training'])
                self.assertEqual(project.admission('gpu', accelerator=request)['capable_cpus'], 2)
                first = project.submit(python('import time; time.sleep(.35)', project='gpu', accelerator=request))['id']
                until(lambda: project.status(first, brief=True)['status'] == 'running')
                second = project.submit(python('print(2)', project='gpu', accelerator=request))['id']
                self.assertEqual(project.status(second, brief=True)['status'], 'queued')
                self.assertEqual(project.wait(first)['status'], 'succeeded')
                result = project.wait(second)
                self.assertEqual(result['status'], 'succeeded')
                self.assertIn('tools', result)
                self.assertTrue(all('argv' not in info for info in result['tools'].values()))
                self.assertNotIn('D:\\', json.dumps(result))
                self.assertNotIn('cwd', result)
                self.assertNotIn('assigned_node', result)

    def test_accelerator_requirement_does_not_fallback_to_cpu(self):
        with scratch() as directory, provider(Path(directory) / 'cpu', cpus=2) as (_, remote):
            with Pool([remote]) as pool, center(pool, Path(directory) / 'center') as (_, operator):
                project = Client(operator.url)
                request = dict(kind='cuda', memory_mb=4096, requires=['gpu:training'])
                self.assertEqual(project.admission('gpu', accelerator=request)['capable_cpus'], 0)

    def test_tool_info_is_availability_only_and_shared_alias_route_without_device_names(self):
        with scratch() as directory:
            root=Path(directory);data=root/'data';data.mkdir()
            with provider(root/'a',tools={'science':__import__('sys').executable}) as (a,ac), provider(root/'b',shared={'data':data},tools={'science':__import__('sys').executable}) as (b,bc):
                with Pool([ac,bc]) as pool,center(pool,root/'center') as (_,client):
                    options=dict(requires=['science','shared:data'])
                    grant=client.admission('science',**options)
                    self.assertEqual(grant['capable_cpus'],1)
                    result=client.run(python('print(7)',project='science',**options))
                    self.assertEqual(result['status'],'succeeded',result)
                    self.assertEqual(result['assigned_node'],'node2')
                    self.assertEqual(client.admission('science')['capable_cpus'],2)
                    accepted=ac.submit(python('print(1'))
                    self.assertEqual(accepted['status'],'queued')

    def test_brief_identity_does_not_compute_full_resources(self):
        with scratch() as directory,provider(directory) as (worker,client):
            with patch.object(worker,'_memory_state',side_effect=AssertionError('full resource scan')):
                self.assertEqual(client.health(brief=True)['role'],'worker')
            sample=client.health(sample=True)
            self.assertIn('active_jobs',sample)
            self.assertNotIn('tools',sample)
            self.assertNotIn('benchmark',sample)
            self.assertLess(len(json.dumps(sample)),len(json.dumps(client.health())))

    def test_slow_node_does_not_delay_other_nodes_sampling(self):
        with scratch() as directory,provider(Path(directory)/'slow',cpus=2) as (_,slow),provider(Path(directory)/'fast',cpus=2) as (_,fast):
            gate=threading.Event();entered=threading.Event()
            with Pool([slow,fast],refresh=.02) as pool:
                original=slow.health
                def delayed(**options):
                    entered.set();gate.wait(2);return original(**options)
                try:
                    with patch.object(slow,'health',side_effect=delayed):
                        self.assertTrue(entered.wait(1))
                        fast.control(cpus=1)
                        until(lambda:pool.nodes[1].capacity==1,seconds=.5)
                        self.assertFalse(gate.is_set())
                finally:gate.set()

    def test_conditional_file_reads_cross_center_without_retransmitting(self):
        with scratch() as directory,provider(Path(directory)/'worker') as (_,worker):
            with Pool([worker]) as pool,center(pool,Path(directory)/'center') as (_,client):
                identity=client.submit(python('import time;time.sleep(2)'))['id']
                until(lambda:client.status(identity)['status']=='running')
                from remote_compute.client import RemoteError
                with self.assertRaises(RemoteError) as missing:
                    client.read_job_file_changed(identity,'missing.json')
                self.assertEqual(missing.exception.status_code,404)
                conflict=RemoteError('not ready');conflict.status_code=409
                with patch.object(pool.nodes[0].client,'read_job_file_changed',side_effect=conflict):
                    with self.assertRaises(RemoteError) as not_ready:
                        client.read_job_file_changed(identity,'state.json')
                    self.assertEqual(not_ready.exception.status_code,409)
                client.write_job_file(identity,'state.json',b'first'*1000)
                first=client.read_job_file_changed(identity,'state.json')
                unchanged=client.read_job_file_changed(identity,'state.json',first['version'])
                self.assertEqual(first['data'],b'first'*1000)
                self.assertFalse(unchanged['changed']);self.assertIsNone(unchanged['data'])
                client.write_job_file(identity,'state.json',b'next'*1000)
                updated=client.read_job_file_changed(identity,'state.json',first['version'])
                self.assertTrue(updated['changed']);self.assertEqual(updated['data'],b'next'*1000)
                client.wait(identity);client.fetch(identity)

    def test_saved_calibration_reused_without_load_probe_or_new_kernel(self):
        with scratch() as directory:
            path=Path(directory)/'calibration.json';identity=dict(cpu='test',threads=12)
            result=dict(name=NAME,available=True,threads=12,score=5)
            with patch('remote_compute.benchmark.cpu_identity',return_value=identity),patch('remote_compute.resources.cpu_ticks',side_effect=[(100,80),(200,170)]) as ticks,patch('remote_compute.benchmark.time.sleep'),patch('remote_compute.benchmark.measure',return_value=result) as measure:
                self.assertEqual(calibrated(12,cache_file=path),result)
                self.assertEqual(calibrated(12,cache_file=path),result)
                self.assertEqual(ticks.call_count,2);measure.assert_called_once()
            with patch('remote_compute.benchmark.cpu_identity',return_value=dict(cpu='test',threads=24)),patch('remote_compute.resources.cpu_ticks',side_effect=[(100,80),(200,90)]),patch('remote_compute.benchmark.time.sleep'),patch('remote_compute.benchmark.measure') as measure:
                self.assertFalse(calibrated(24,cache_file=path)['available'])
                measure.assert_not_called()
                self.assertEqual(json.loads(path.read_text())['benchmark'],result)

    def test_known_content_reuses_remote_asset_before_preparing_zip(self):
        from remote_compute.client import Client
        from remote_compute.bundle import pack
        client=Client();present=set();uploads=[]
        def request(path,data=None):
            if data is not None:present.add(path);uploads.append(data)
            return dict(exists=path in present)
        with patch.object(client,'request',side_effect=request),patch('remote_compute.client.pack',wraps=pack) as compress:
            factory=__import__('unittest.mock',fromlist=['Mock']).Mock(return_value={'data':b'x'*1000})
            first=client.sync_files(factory,content_id='data-v1')
            self.assertEqual(client.sync_files(factory,content_id='data-v1'),first)
            self.assertEqual(factory.call_count,1);self.assertEqual(compress.call_count,1)
            present.clear()
            self.assertEqual(client.sync_files(factory,content_id='data-v1'),first)
            self.assertEqual(len(uploads),2);self.assertEqual(factory.call_count,2)
