"""Generic capability routing, bounded sampling and reusable calibration."""
import json
from pathlib import Path
import threading
import time
import unittest
from unittest.mock import patch

from remote_compute import Pool
from remote_compute.benchmark import calibrated, NAME
from test_compute import provider, python, scratch, until
from test_center import center


class ContractTests(unittest.TestCase):
    def test_engine_constraint_and_shared_alias_route_without_device_names(self):
        with scratch() as directory:
            root=Path(directory);data=root/'data';data.mkdir()
            with provider(root/'a',tools={'science':__import__('sys').executable}) as (a,ac), provider(root/'b',shared={'data':data},tools={'science':__import__('sys').executable}) as (b,bc):
                a.tool_info['science']['sha256']='a'*64;b.tool_info['science']['sha256']='b'*64
                with Pool([ac,bc]) as pool,center(pool,root/'center') as (_,client):
                    options=dict(requires=['science','shared:data'],tool_sha256={'science':['b'*64]})
                    grant=client.admission('science',**options)
                    self.assertEqual(grant['capable_cpus'],1)
                    self.assertEqual(grant['tool_sha256'],{'science':['b'*64]})
                    result=client.run(python('print(7)',project='science',**options))
                    self.assertEqual(result['status'],'succeeded',result)
                    self.assertEqual(result['assigned_node'],'node2')
                    self.assertEqual(client.admission('science',tool_sha256={'science':['c'*64]})['capable_cpus'],0)
                    from remote_compute.client import RemoteError
                    with self.assertRaisesRegex(RemoteError,'SHA256 constraint'):
                        ac.submit(python('print(1)',tool_sha256={'science':['b'*64]}))

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
