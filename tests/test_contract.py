"""Generic capability routing, bounded sampling and reusable calibration."""
import json
import os
from pathlib import Path
import sys
import threading
import time
import unittest
from unittest.mock import patch

from remote_compute import Client, Pool
from remote_compute.benchmark import calibrated, NAME
from test_compute import provider, python, scratch, until
from test_center import center


class ContractTests(unittest.TestCase):
    def test_own_task_file_preserves_tool_prefix_for_polling_and_hybrid(self):
        from remote_compute.local import LocalClient
        code = """import json,os,sys
from pathlib import Path
sys.path.insert(0,os.environ['REMOTE_COMPUTE_HOME'])
from remote_compute import Client
identity=os.environ.get('REMOTE_COMPUTE_WORKER_JOB_ID') or os.environ['REMOTE_COMPUTE_JOB_ID']
record=json.loads((Path(os.environ['REMOTE_COMPUTE_JOB_DIR'])/'result.json').read_text(encoding='utf-8'))
assert record['id']==identity
client=Client(os.environ['REMOTE_COMPUTE_URL'],os.environ.get('REMOTE_COMPUTE_TOKEN',''))
public=client.status(identity)
Path('context.json').write_text(json.dumps(dict(tools=record['tools'],public_tools=public.get('tools',{}))),encoding='utf-8')
"""
        for mode in ('polling', 'hybrid'):
            with self.subTest(mode=mode), scratch() as temporary:
                root = Path(temporary)
                prefix = [sys.executable, '-B', str(root / 'helper with spaces.py'), 'runtime with spaces', 'magic']
                with provider(root / 'worker', tools={'magic': prefix}) as (worker, remote):
                    selected = LocalClient(worker) if mode == 'hybrid' else remote
                    with Pool([selected]) as pool, center(pool, root / 'center') as (_, operator):
                        client = Client(operator.url)
                        receipt = client.run(python(code, outputs=['context.json'], requires=['magic'], memory_mb=64))
                        self.assertEqual(receipt['status'], 'succeeded', receipt.get('stderr'))
                        result = json.loads(receipt['files']['context.json'])
                        self.assertEqual(result['tools']['magic']['argv'], prefix)
                        self.assertTrue(result['tools']['magic']['available'])
                        if mode == 'hybrid':
                            # The callback addresses the scheduler's project view;
                            # a worker-local command path is intentionally private.
                            self.assertNotIn('argv', result['public_tools'].get('magic', {}))
                        else:
                            self.assertEqual(result['public_tools']['magic']['argv'], prefix)

    def test_project_view_is_aggregate_and_rejects_node_selection(self):
        with scratch() as directory, provider(Path(directory) / 'worker', cpus=2,
                tools={'verilator': sys.executable}, tags=['rtl']) as (_, remote):
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

    def test_accelerator_profiles_cannot_count_one_device_twice(self):
        from remote_compute.policy import normalize_accelerators
        invalid = ([dict(kind='cuda', count=2, ids=['0', '0'])],
                   [dict(kind='cuda'), dict(kind='cuda')],
                   [dict(kind='cuda', ids=['GPU-test']), dict(kind='cuda', ids=['GPU-test'])])
        for profiles in invalid:
            with self.subTest(profiles=profiles), self.assertRaisesRegex(ValueError, 'Duplicate accelerator device'):
                normalize_accelerators(profiles)
        profiles = normalize_accelerators([dict(kind='cuda', ids=['0']),
                                          dict(kind='cuda', ids=['1']), dict(kind='xpu', ids=['0'])])
        self.assertEqual(sum(p['count'] for p in profiles), 3)

    def test_accelerator_requirement_does_not_fallback_to_cpu(self):
        with scratch() as directory, provider(Path(directory) / 'cpu', cpus=2) as (_, remote):
            with Pool([remote]) as pool, center(pool, Path(directory) / 'center') as (_, operator):
                project = Client(operator.url)
                request = dict(kind='cuda', memory_mb=4096, requires=['gpu:training'])
                self.assertEqual(project.admission('gpu', accelerator=request)['capable_cpus'], 0)

    def test_optional_accelerator_wait_is_validated(self):
        from remote_compute.policy import normalize_accelerator_requirement
        for value in (True, -1, float('nan'), float('inf'), '30', None):
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, 'fallback_after_seconds'):
                normalize_accelerator_requirement(dict(kind='cuda', fallback_after_seconds=value))
        for value in (0, .25, 30):
            self.assertEqual(normalize_accelerator_requirement(
                dict(kind='cuda', fallback_after_seconds=value))['fallback_after_seconds'], value)

    def test_worker_accelerator_environment_comes_only_from_its_reservation(self):
        forged = dict(DCO_ACCELERATOR_KIND='cuda', DCO_ACCELERATOR_COUNT='99',
                      DCO_ACCELERATOR_MEMORY_MB='999999', DCO_ACCELERATOR_IDS='forged',
                      CUDA_VISIBLE_DEVICES='forged')
        code = 'import json,os;print(json.dumps({key:os.environ[key] for key in ' + repr(list(forged)) + '}))'
        supplied = {**forged, **{key.lower(): value for key, value in forged.items()}}
        with scratch() as directory, patch.dict(os.environ, supplied), provider(
                Path(directory)/'gpu', env=supplied,
                accelerators=[dict(kind='cuda', count=1, memory_mb=8192, ids=['0'])]) as (_, remote):
            cpu = remote.run(python(code, env=supplied))
            self.assertEqual(cpu['status'], 'succeeded', cpu)
            self.assertEqual(json.loads(cpu['stdout']), dict(
                DCO_ACCELERATOR_KIND='', DCO_ACCELERATOR_COUNT='0',
                DCO_ACCELERATOR_MEMORY_MB='0', DCO_ACCELERATOR_IDS='', CUDA_VISIBLE_DEVICES='-1'))
            gpu = remote.run(python(code, env=supplied, accelerator=dict(kind='cuda')))
            self.assertEqual(gpu['status'], 'succeeded', gpu)
            self.assertEqual(json.loads(gpu['stdout']), dict(
                DCO_ACCELERATOR_KIND='cuda', DCO_ACCELERATOR_COUNT='1',
                DCO_ACCELERATOR_MEMORY_MB='8192', DCO_ACCELERATOR_IDS='0', CUDA_VISIBLE_DEVICES='0'))

    def test_optional_accelerator_without_gpu_runs_cpu_under_original_identity(self):
        with scratch() as directory, provider(Path(directory)/'cpu', cpus=2) as (_, remote):
            with Pool([remote]) as pool, center(pool, Path(directory)/'center') as (_, operator):
                project = Client(operator.url)
                request = dict(kind='cuda', fallback_after_seconds=30)
                self.assertIn('accelerator_fallback', project.capabilities()['capabilities'])
                grant = project.admission('optional', accelerator=request)
                self.assertEqual((grant['capable_cpus'], grant['fallback_capable_cpus']), (0, 2))
                accepted = project.submit(python(
                    "import os;print(os.environ['DCO_ACCELERATOR_COUNT'])", accelerator=request))
                result = project.wait(accepted['id'])
                self.assertEqual(result['status'], 'succeeded', result)
                self.assertEqual(result['id'], accepted['id'])
                self.assertEqual(result['request_sha256'], accepted['request_sha256'])
                self.assertEqual(result['stdout'].strip(), '0')
                self.assertIsNone(result['accelerator'])
                self.assertEqual(result['requested_accelerator']['fallback_after_seconds'], 30)
                self.assertEqual(result['accelerator_fallback']['reason'], 'unavailable')
                self.assertFalse(result['attempts'])

    def test_busy_optional_accelerator_falls_back_while_mandatory_request_stays_queued(self):
        with scratch() as directory, provider(Path(directory)/'gpu', cpus=2,
                accelerators=[dict(kind='cuda', count=1, memory_mb=8192, ids=['0'])]) as (_, remote):
            with Pool([remote]) as pool, center(pool, Path(directory)/'center') as (_, client):
                hold = "from pathlib import Path;import time\nwhile not Path('release').exists():time.sleep(.02)"
                first = client.submit(python(hold, accelerator=dict(kind='cuda')))['id']
                until(lambda: client.status(first)['status'] == 'running')
                mandatory = client.submit(python('print(1)', accelerator=dict(kind='cuda')))['id']
                accepted = client.submit(python(
                    "import os;print(os.environ['DCO_ACCELERATOR_COUNT'])",
                    accelerator=dict(kind='cuda', fallback_after_seconds=.25)))
                optional = accepted['id']
                self.assertEqual(client.status(optional)['status'], 'queued')
                result = client.wait(optional, wait_timeout=5)
                self.assertEqual(result['status'], 'succeeded', result)
                self.assertEqual(result['accelerator_fallback']['reason'], 'queue_timeout')
                self.assertGreaterEqual(result['accelerator_fallback']['at'] - accepted['submitted'], .25)
                self.assertEqual(result['request_sha256'], accepted['request_sha256'])
                self.assertEqual(result['stdout'].strip(), '0')
                self.assertEqual(client.status(first)['status'], 'running')
                self.assertEqual(client.status(mandatory)['status'], 'queued')
                client.write_job_file(first, 'release', 'done')
                self.assertEqual(client.wait(first)['status'], 'succeeded')
                self.assertEqual(client.wait(mandatory)['status'], 'succeeded')

    def test_dispatched_optional_accelerator_keeps_gpu_after_wait_expires(self):
        with scratch() as directory, provider(Path(directory)/'gpu',
                accelerators=[dict(kind='cuda', count=1, memory_mb=8192, ids=['0'])]) as (_, remote):
            with Pool([remote]) as pool, center(pool, Path(directory)/'center') as (state, client):
                hold = "from pathlib import Path;import time,os\nwhile not Path('release').exists():time.sleep(.02)\nprint(os.environ['DCO_ACCELERATOR_COUNT'])"
                identity = client.submit(python(hold, accelerator=dict(
                    kind='cuda', fallback_after_seconds=30)))['id']
                until(lambda: client.status(identity)['status'] == 'running')
                with state.condition:
                    job = state.jobs[identity]
                    with patch('remote_compute.coordinator.time.time',
                               return_value=job['result']['submitted'] + 60):
                        state._fallback_accelerator(job)
                    self.assertEqual(job['task']['accelerator']['kind'], 'cuda')
                    self.assertNotIn('accelerator_fallback', job['result'])
                client.write_job_file(identity, 'release', 'done')
                result = client.wait(identity)
                self.assertEqual(result['status'], 'succeeded', result)
                self.assertEqual(result['stdout'].strip(), '1')
                self.assertNotIn('accelerator_fallback', result)

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

    @unittest.skipUnless(os.name == 'nt', 'Native Windows CPU identity')
    def test_windows_cached_calibration_starts_without_wmi(self):
        from remote_compute.benchmark import cpu_identity
        with scratch() as directory, \
                patch('platform.machine', side_effect=AssertionError('WMI must not run')):
            identity=cpu_identity(12)
            self.assertIn(identity['architecture'], ('AMD64','ARM64','x86','ARM','IA64'))
            self.assertEqual(identity['logical_cpus'], os.cpu_count())
            path=Path(directory)/'calibration.json'
            result=dict(name=NAME,available=True,threads=12,score=5)
            path.write_text(json.dumps(dict(identity=identity,benchmark=result)))
            with patch('remote_compute.benchmark.measure') as measure, \
                    patch('remote_compute.resources.cpu_ticks') as ticks:
                self.assertEqual(calibrated(12,cache_file=path), result)
                measure.assert_not_called()
                ticks.assert_not_called()

    def test_saved_calibration_reused_without_load_probe_or_new_kernel(self):
        with scratch() as directory:
            path=Path(directory)/'calibration.json';identity=dict(cpu='test',threads=12)
            result=dict(name=NAME,available=True,threads=12,score=5)
            with patch('remote_compute.benchmark.cpu_identity',return_value=identity),patch('remote_compute.resources.cpu_ticks',side_effect=[(100,80),(200,170)]) as ticks,patch('remote_compute.benchmark.time.sleep'),patch('remote_compute.benchmark.measure',return_value=result) as measure:
                self.assertEqual(calibrated(12,cache_file=path,force=True),result)
                self.assertEqual(calibrated(12,cache_file=path),result)
                self.assertEqual(ticks.call_count,2);measure.assert_called_once()
            with patch('remote_compute.benchmark.cpu_identity',return_value=dict(cpu='test',threads=24)),patch('remote_compute.resources.cpu_ticks',side_effect=[(100,80),(200,90)]),patch('remote_compute.benchmark.time.sleep'),patch('remote_compute.benchmark.measure') as measure:
                self.assertFalse(calibrated(24,cache_file=path)['available'])
                measure.assert_not_called()
                self.assertEqual(json.loads(path.read_text())['benchmark'],result)

    def test_known_content_reuses_remote_asset_before_preparing_zip(self):
        from remote_compute.client import Client
        from remote_compute.bundle import pack
        client=Client();present=set();uploads=[];lookups=[]
        def request(path,data=None):
            if data is not None:present.add(path);uploads.append(data)
            else:lookups.append(path)
            return dict(exists=path in present)
        with patch.object(client,'request',side_effect=request),patch('remote_compute.client.pack',wraps=pack) as compress:
            factory=__import__('unittest.mock',fromlist=['Mock']).Mock(return_value={'data':b'x'*1000})
            first=client.sync_files(factory,content_id='data-v1')
            self.assertEqual(client.sync_files(factory,content_id='data-v1'),first)
            self.assertEqual(factory.call_count,1);self.assertEqual(compress.call_count,1)
            present.clear()
            self.assertEqual(client.sync_files(factory,content_id='data-v1'),first)
            self.assertEqual(len(uploads),2);self.assertEqual(factory.call_count,2)
            self.assertEqual(len(lookups),3)

    def test_concurrent_content_sync_prepares_once_and_keeps_each_job_handoff(self):
        from concurrent.futures import ThreadPoolExecutor
        from remote_compute.bundle import pack
        client=Client();present=set();holds=set();uploads=[]
        entered=threading.Event();release=threading.Event();lock=threading.Lock()
        def factory():
            entered.set()
            self.assertTrue(release.wait(3))
            return {'model':b'frozen model'}
        def request(path,data=None,extra_headers=None):
            with lock:
                holds.add(extra_headers['X-DCO-Asset-Job'])
                if data is not None:present.add(path);uploads.append(data)
                return dict(exists=path in present)
        identities=[f'{i:032x}' for i in range(8)]
        with patch.object(client,'request',side_effect=request),patch('remote_compute.client.pack',wraps=pack) as compress:
            with ThreadPoolExecutor(max_workers=8) as executor:
                pending=[executor.submit(client.sync_files,factory,content_id='model-v1',job_id=identity)
                         for identity in identities]
                self.assertTrue(entered.wait(3))
                time.sleep(.05)
                release.set()
                assets=[item.result(3) for item in pending]
            self.assertEqual(len(set(assets)),1)
            self.assertEqual(compress.call_count,1)
            self.assertEqual(len(uploads),1)
            self.assertEqual(holds,set(identities))

    def test_distinct_content_prepares_concurrently_and_failed_preparation_can_retry(self):
        from concurrent.futures import ThreadPoolExecutor
        client=Client();entered=threading.Barrier(2)
        def factory():
            entered.wait(3)
            return {'data':b'content'}
        with patch.object(client,'request',return_value={'exists':True}):
            with ThreadPoolExecutor(max_workers=2) as executor:
                pending=[executor.submit(client.sync_files,factory,content_id=key) for key in ('one','two')]
                for item in pending:item.result(4)
            with self.assertRaisesRegex(ValueError,'prepare failed'):
                client.sync_files(lambda: (_ for _ in ()).throw(ValueError('prepare failed')),content_id='retry')
            self.assertTrue(client.sync_files({'data':b'content'},content_id='retry'))

    def test_fetch_to_directory_reads_each_payload_once_and_deletes_after_extract(self):
        import zipfile
        from remote_compute.bundle import pack
        client=Client();identity='e'*32
        raw=pack({'result.json':json.dumps(dict(id=identity,status='succeeded')).encode(),
                  'files/large.bin':b'x'*10000,'stdout.log':b'finished'})
        reads=[];read=zipfile.ZipFile.read
        def read_once(archive,name,*args,**kwargs):
            reads.append(name.filename if isinstance(name,zipfile.ZipInfo) else name)
            return read(archive,name,*args,**kwargs)
        with scratch() as directory:
            root=Path(directory)
            def delete(value):
                self.assertEqual(value,identity)
                self.assertEqual((root/'files/large.bin').read_bytes(),b'x'*10000)
            with patch.object(client,'artifacts',return_value=raw),patch.object(client,'delete',side_effect=delete), \
                    patch.object(zipfile.ZipFile,'read',read_once):
                result=client.fetch(identity,root)
            self.assertEqual(result['files'],['large.bin'])
            self.assertEqual(reads.count('files/large.bin'),1)
