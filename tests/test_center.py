"""The center is exercised through the same client used by project adapters."""

from contextlib import contextmanager
import io
import json
from pathlib import Path
import threading
import time
import unittest
from unittest.mock import patch

from remote_compute import Client, Pool, RemoteError
from remote_compute.coordinator import Coordinator
from remote_compute.cli import SchedulerShell
from remote_compute.worker import Conflict, Server
from test_compute import provider, python, scratch, until


@contextmanager
def center(pool, root):
    state = Coordinator(pool, root)
    server = Server(('127.0.0.1', 0), state)
    thread = threading.Thread(target=server.serve_forever, kwargs={'poll_interval': .05}, daemon=True)
    thread.start()
    try:
        yield state, Client(f'http://127.0.0.1:{server.server_port}')
    finally:
        state.close()
        server.shutdown()
        thread.join()
        server.server_close()


class CenterTests(unittest.TestCase):
    def test_auto_uses_idle_smaller_node_while_larger_node_is_busy(self):
        with scratch() as root, provider(Path(root)/'big',cpus=3) as (_,big), provider(Path(root)/'small',cpus=2) as (_,small):
            with Pool([dict(name='big',url=big.url),dict(name='small',url=small.url)]) as pool, center(pool,Path(root)/'center') as (_,client):
                busy=client.submit(python("from pathlib import Path; import time\nwhile not Path('release').exists(): time.sleep(.02)",
                                          cpus=3,node='big',project='busy'))['id']
                until(lambda: client.status(busy)['status']=='running')
                automatic=client.submit(python("print('parallel')",cpus=0,project='auto'))['id']
                until(lambda: client.status(automatic)['status'] in ('running','succeeded'))
                result=client.wait(automatic)
                self.assertEqual((result['assigned_node'],result['cpus']),('small',2))
                self.assertEqual(client.status(busy)['status'],'running')
                client.fetch(automatic)
                client.write_job_file(busy,'release','done')
                client.wait(busy);client.fetch(busy)

    def test_completed_cached_tasks_do_not_reappear_as_external_work(self):
        with scratch() as root, provider(Path(root)/'worker',cpus=2) as (_,worker):
            with Pool([worker],refresh=60) as pool, center(pool,Path(root)/'center') as (_,client):
                identity=client.submit(python('import time;time.sleep(.5)',project='test'))['id']
                self.assertEqual(client.status(identity)['project'],'test')
                until(lambda: client.status(identity)['status']=='running')
                pool.refresh()
                self.assertEqual(len(pool.nodes[0].health['active_jobs']),1)
                client.wait(identity)
                client.fetch(identity)
                until(lambda: pool.nodes[0].reserved==0)
                snapshot=client.health()
                self.assertEqual(snapshot['used_cpus'],0)
                self.assertEqual(snapshot['nodes'][0]['active_jobs'],[])
                direct=worker.submit(python('import time;time.sleep(.5)'))['id']
                until(lambda: worker.status(direct)['status']=='running')
                pool.refresh()
                snapshot=client.health()
                self.assertEqual(snapshot['used_cpus'],1)
                self.assertEqual([j['id'] for j in snapshot['nodes'][0]['active_jobs']],[direct])
                worker.wait(direct)
                worker.fetch(direct)

    def test_brief_polling_omits_payload_and_wait_returns_full_provenance(self):
        with scratch() as root, provider(Path(root)/'worker') as (_,client):
            identity=client.submit(python("import time; print('complete'); time.sleep(.2)"))['id']
            brief=client.status(identity,brief=True)
            self.assertIn('status',brief)
            self.assertNotIn('tools',brief)
            self.assertNotIn('stdout',brief)
            result=client.wait(identity)
            self.assertIn('tools',result)
            self.assertIn('complete',result['stdout'])
            client.fetch(identity)

    def test_json_compression_is_negotiated_and_client_decodes_it(self):
        import gzip
        import http.client
        from urllib.parse import urlsplit
        with scratch() as root, provider(Path(root)/'worker') as (_,client):
            endpoint=urlsplit(client.url)
            connection=http.client.HTTPConnection(endpoint.hostname,endpoint.port)
            try:
                connection.request('GET','/health')
                response=connection.getresponse();plain=response.read()
                self.assertIsNone(response.getheader('Content-Encoding'))
                connection.request('GET','/health',headers={'Accept-Encoding':'gzip'})
                response=connection.getresponse();compressed=response.read()
                self.assertEqual(response.getheader('Content-Encoding'),'gzip')
                self.assertLess(len(compressed),len(plain))
                self.assertEqual(json.loads(gzip.decompress(compressed))['node'],client.health()['node'])
            finally:
                connection.close()

    def test_precompressed_artifacts_are_stored_without_another_deflate(self):
        import gzip
        import zipfile
        from remote_compute.bundle import pack
        data=gzip.compress(b'scientific arrays '*10000)
        payload=pack({'result.pkl.gz':data,'metrics.json':b'{"passed":true}'})
        with zipfile.ZipFile(io.BytesIO(payload)) as archive:
            self.assertEqual(archive.getinfo('result.pkl.gz').compress_type,zipfile.ZIP_STORED)
            self.assertEqual(archive.read('result.pkl.gz'),data)
            self.assertEqual(archive.getinfo('metrics.json').compress_type,zipfile.ZIP_DEFLATED)

    def test_private_worker_runs_without_http_and_packs_result_once(self):
        from remote_compute.local import LocalClient
        from remote_compute.worker import Worker
        import remote_compute.worker as module
        with scratch() as root:
            worker = Worker(Path(root)/'worker',cpus=2)
            try:
                local = LocalClient(worker)
                with self.assertRaises(RemoteError):
                    Client(local.url).health()
                with Pool([local]) as pool, center(pool,Path(root)/'center') as (_,client):
                    client.health()  # Keep the caller's one center connection open.
                    with patch('remote_compute.client.http.client.HTTPConnection',side_effect=AssertionError('worker opened HTTP')):
                        with patch('remote_compute.worker.pack',wraps=module.pack) as packed:
                            result=client.run(python("from pathlib import Path; Path('value').write_text('42')",outputs=['value']))
                        self.assertEqual(packed.call_count,1)
                    self.assertEqual(result['files']['value'],b'42')
                    self.assertEqual(result['worker_url'],local.url)
                    self.assertEqual(local.status(),[])
            finally:
                worker.close()

    def test_concurrent_jobs_check_and_upload_an_asset_once(self):
        with scratch() as root, provider(Path(root)/'worker',cpus=4) as (_,node):
            with Pool([node]) as pool, center(pool,Path(root)/'center') as (_,client):
                asset=client.sync_files({'model':b'model data'})
                client.control(paused=True)
                identities=[client.submit(python('pass',assets={'model':asset}))['id'] for _ in range(8)]
                original=pool.nodes[0].client.request
                counts=[]
                def request(path,data=None,**kwargs):
                    if path=='/assets/'+asset:counts.append('POST' if data is not None else 'GET')
                    return original(path,data,**kwargs)
                with patch.object(pool.nodes[0].client,'request',side_effect=request), patch('remote_compute.coordinator.pack',side_effect=AssertionError('repacked fixed task')):
                    client.control(paused=False)
                    for identity in identities:client.wait(identity)
                for identity in identities:client.fetch(identity)
                self.assertEqual(counts,['GET','POST'])

    def test_auto_cpu_result_preserves_original_submission_identity(self):
        with scratch() as root, provider(Path(root)/'worker',cpus=2) as (_,node):
            with Pool([node]) as pool, center(pool,Path(root)/'center') as (_,client):
                task=python('pass',cpus=0)
                accepted=client.submit(task);client.wait(accepted['id'])
                repeated=client.submit(task,job_id=accepted['id'])
                self.assertEqual(repeated['request_sha256'],accepted['request_sha256'])
                client.fetch(accepted['id'])

    def test_logs_are_empty_while_worker_submission_is_in_progress(self):
        with scratch() as root, provider(Path(root)/'worker') as (_,node):
            with Pool([node]) as pool, center(pool,Path(root)/'center') as (_,client):
                entered, release = threading.Event(), threading.Event()
                original = pool.nodes[0].client.submit_bundle
                def delayed(*args):
                    entered.set()
                    release.wait(5)
                    return original(*args)
                with patch.object(pool.nodes[0].client,'submit_bundle',side_effect=delayed):
                    identity = client.submit(python("print('ready')"))['id']
                    try:
                        self.assertTrue(entered.wait(5))
                        self.assertEqual(client.logs(identity),dict(stdout='',stderr=''))
                    finally:
                        release.set()
                    client.wait(identity)
                    self.assertIn('ready',client.fetch(identity)['stdout'])

    def test_worker_control_errors_return_an_http_response(self):
        with scratch() as root, provider(Path(root)/'worker', cpus=2) as (_, node):
            with Pool([node]) as pool, center(pool,Path(root)/'center') as (_,client):
                for changes in ({'node':'node1','worker':{'cpus':999999}}, {'node':'missing','worker':{'cpus':1}}):
                    with self.assertRaises(RemoteError) as raised:
                        client.control(**changes)
                    self.assertEqual(raised.exception.status_code,400)
                self.assertEqual(client.health()['cpus'],2)

    def test_center_sizes_prefetch_window_from_share_and_ratio(self):
        with scratch() as root, provider(Path(root)/'worker',cpus=7) as (_,node):
            with Pool([node]) as pool, center(pool,Path(root)/'center') as (_,client):
                client.control(paused=True)
                for _ in range(7):client.submit(python('pass',project='A',resource_share=2))
                window=client.admission('B',resource_share=1,queue_ratio=1)
                self.assertEqual((window['target_cpus'],window['running_tasks'],window['queued_tasks'],window['in_flight']),(2,2,2,4))
                self.assertEqual(client.admission('B',resource_share=1,queue_ratio=0)['in_flight'],2)
                self.assertEqual(client.admission('B',resource_share=1,queue_ratio=.5)['in_flight'],3)
                for job in client.status():client.cancel(job['id']);client.delete(job['id'])
    def test_late_parallel_project_waits_for_its_share_instead_of_receiving_one_thread(self):
        with scratch() as root, provider(Path(root)/'worker', cpus=4) as (_, node):
            with Pool([node]) as pool, center(pool, Path(root)/'center') as (_, client):
                task=python("from pathlib import Path; import time\nwhile not Path('release').exists(): time.sleep(.02)",project='A')
                jobs=[client.submit(task)['id'] for _ in range(4)]
                until(lambda: all(client.status(job)['status']=='running' for job in jobs))
                parallel=client.submit(python('import time; time.sleep(.4)',cpus=0,project='B'))['id']
                client.write_job_file(jobs[0],'release','go')
                client.wait(jobs[0]);client.fetch(jobs[0])
                time.sleep(.1)
                self.assertEqual(client.status(parallel)['status'],'queued')
                client.write_job_file(jobs[1],'release','go')
                client.wait(jobs[1]);client.fetch(jobs[1])
                until(lambda: client.status(parallel)['status'] in ('running','succeeded'))
                self.assertEqual(client.status(parallel)['cpus'],2)
                for job in jobs[2:]:
                    client.write_job_file(job,'release','go');client.wait(job);client.fetch(job)
                client.wait(parallel);client.fetch(parallel)

    def test_weighted_integer_allocation_and_unused_share_lending(self):
        from remote_compute.allocation import weighted_threads
        self.assertEqual(weighted_threads(58, dict(A=1,B=1,C=1), dict(A=100,B=100,C=100)),dict(A=20,B=19,C=19))
        self.assertEqual(weighted_threads(58, dict(A=2,B=1), dict(A=100,B=100)),dict(A=39,B=19))
        self.assertEqual(weighted_threads(58, dict(A=1,B=1,C=1), dict(A=1,B=100,C=100)),dict(A=1,B=29,C=28))

    def test_three_projects_share_all_threads_and_auto_job_obeys_share(self):
        with scratch() as root, provider(Path(root)/'worker', cpus=7) as (_, node):
            with Pool([node]) as pool, center(pool, Path(root)/'center') as (_, client):
                client.control(paused=True)
                identities=[]
                identities.append(client.submit(python('import time; time.sleep(1)',cpus=0,project='A'))['id'])
                for project in ('B','C'):
                    for _ in range(2):
                        identities.append(client.submit(python('import time; time.sleep(1)',project=project))['id'])
                client.control(paused=False)
                until(lambda: sum(p['used'] for p in client.health()['projects'].values()) == 7)
                allocations=client.health()['projects']
                self.assertEqual({k:v['used'] for k,v in allocations.items()},dict(A=3,B=2,C=2))
                for identity in identities:
                    client.wait(identity);client.fetch(identity)

    def test_automatic_cpu_grant_uses_remaining_node_slots(self):
        with scratch() as root, provider(Path(root)/'worker', cpus=4) as (_, node):
            with Pool([node]) as pool, center(pool, Path(root)/'center') as (_, client):
                busy = client.submit(python('import time; time.sleep(1)', cpus=1))['id']
                until(lambda: client.status(busy)['status'] == 'running')
                result = client.run(python("import os; print(os.environ['GRANT'])", cpus=0, env={'GRANT':'${cpus}'}))
                self.assertEqual(result['status'], 'succeeded')
                self.assertEqual(result['cpus'], 3)
                self.assertEqual(result['stdout'].strip(), '3')
                client.wait(busy)
                client.fetch(busy)

    def test_polling_reuses_one_tcp_connection(self):
        with scratch() as root, provider(Path(root)/'worker') as (_, client):
            ports = set()
            for _ in range(300):
                client.health()
                ports.add(client.connections.connection.sock.getsockname()[1])
            self.assertEqual(len(ports), 1)

    def test_forward_result_provenance_and_two_sided_collection(self):
        with scratch() as root, provider(Path(root)/'worker') as (worker, node):
            with Pool([node]) as pool, center(pool, Path(root)/'center') as (state, client):
                task = python("from pathlib import Path; print('actual'); Path('x').write_text('42')", outputs=['x'])
                result = client.run(task)
                self.assertEqual(result['status'], 'succeeded')
                self.assertEqual(result['files']['x'], b'42')
                self.assertEqual(result['assigned_node'], 'node1')
                self.assertGreater(result['peak_rss_mb'], 0)
                self.assertGreaterEqual(result['cpu_seconds'], 0)
                self.assertEqual(result['tools']['python']['sha256'], worker.tool_info['python']['sha256'])
                self.assertEqual(client.status(), [])
                self.assertEqual(node.status(), [])
                self.assertEqual(list((state.root/'jobs').iterdir()), [])

    def test_memory_blocks_until_inflight_task_finishes(self):
        with scratch() as root, provider(Path(root)/'worker', cpus=2, memory_mb=128) as (_, node):
            with Pool([node]) as pool, center(pool, Path(root)/'center') as (_, client):
                task = python("import time; data=bytearray(32*1024*1024); time.sleep(.3)", memory_mb=80)
                first = client.submit(task)['id']
                until(lambda: client.status(first)['status'] == 'running')
                second = client.submit(task)['id']
                one, two = client.wait(first), client.wait(second)
                self.assertGreaterEqual(two['started'], one['finished'])
                self.assertGreater(one['peak_rss_mb'], 30)
                self.assertEqual(node.health()['memory_reserved_mb'], 0)

    def test_physical_memory_is_counted_once_for_two_workers(self):
        with scratch() as root, provider(Path(root)/'one', memory_mb=128) as (_, one), provider(Path(root)/'two', memory_mb=128) as (_, two):
            with Pool([one, two]) as pool, center(pool, Path(root)/'center') as (_, client):
                value = client.health()
                self.assertEqual(value['role'], 'scheduler')
                self.assertEqual(value['memory_total_mb'], one.health()['memory_total_mb'])
                self.assertEqual(value['memory_capacity_mb'], 256)
                self.assertEqual({v['node'] for v in value['tools']['python']['variants']}, {'node1', 'node2'})
                self.assertEqual(len(client.nodes()['nodes']), 2)
                client.control(node='node1', routing={'memory_limit_mb': 64})
                self.assertEqual(client.nodes()['nodes'][0]['memory_capacity_mb'], 64)

    def test_assets_priority_and_cancellation_use_center_ids(self):
        with scratch() as root, provider(Path(root)/'worker') as (_, node):
            with Pool([node]) as pool, center(pool, Path(root)/'center') as (_, client):
                asset = client.sync_files({'model': b'content'})
                client.control(paused=True)
                task = python("import os; from pathlib import Path; print(Path(os.environ['MODEL']).read_text())",
                              assets={'models': asset}, env={'MODEL': '${asset:models}/model'}, priority=5)
                identity = client.submit(task)['id']
                cancelled = client.submit(python("print('cancel')"))['id']
                client.cancel(cancelled)
                self.assertEqual(client.status(cancelled)['status'], 'cancelled')
                client.control(paused=False)
                self.assertEqual(client.wait(identity)['status'], 'succeeded')
                self.assertIn('content', client.fetch(identity)['stdout'])
                self.assertEqual(client.fetch(cancelled)['status'], 'cancelled')

    def test_single_executable_variable_in_environment(self):
        with scratch() as root, provider(Path(root)/'worker') as (_, node):
            result = node.run(python("import os; print(os.environ['ENGINE'])", env={'ENGINE': '${tool:python}'}))
            self.assertEqual(result['status'], 'succeeded')
            self.assertIn('python', result['stdout'].lower())

    def test_center_root_has_one_owner(self):
        with scratch() as root, provider(Path(root)/'worker') as (_, node):
            with Pool([node]) as pool, center(pool, Path(root)/'center') as (_, client):
                client.control(paused=True)
                identity = client.submit(python("print('kept')"))['id']
                with self.assertRaises(Conflict):
                    Coordinator(pool, Path(root)/'center')
                self.assertEqual(client.status(identity)['status'], 'queued')
                client.cancel(identity)
                client.fetch(identity)

    def test_console_batch_quit_cancels_owned_jobs_and_saves_memory_limit(self):
        from contextlib import redirect_stdout
        with scratch() as root, provider(Path(root)/'worker') as (_, node):
            root = Path(root)
            source = root/'batch.json'
            source.write_text(json.dumps([{'task': python("import time; time.sleep(15)")} for _ in range(4)]))
            with Pool([node]) as pool, center(pool, root/'center') as (_, client):
                shell = SchedulerShell(pool, client)
                try:
                    with io.StringIO() as output, redirect_stdout(output):
                        client.control(node='node1', routing={'memory_limit_mb': 128})
                        with patch('remote_compute.commands.ROOT', root):
                            shell.do_save('')
                        saved = json.loads((root/'nodes.local.json').read_text())
                        self.assertEqual(saved['nodes'][0]['memory_limit_mb'], 128)
                        client.control(node='node1', routing={'memory_limit_mb': 0})
                        shell.do_batch(f'"{source}" "{root}/results"')
                        until(lambda: any(x['status'] == 'running' for x in client.status()))
                        before = time.monotonic()
                        shell.close()
                        self.assertLess(time.monotonic() - before, 4)
                finally:
                    if not shell.closing.is_set():
                        shell.close()
                self.assertEqual(client.status(), [])
                self.assertEqual(node.status(), [])


if __name__ == '__main__':
    unittest.main()
