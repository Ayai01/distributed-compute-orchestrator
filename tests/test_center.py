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
from remote_compute.client import PoolBusy
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


@contextmanager
def host_load(worker, percent, machine):
    sample = dict(worker.host.sample(), cpu_percent=percent, logical_cpus=8, machine_id=machine)
    with patch.object(worker.host, 'sample', return_value=sample):
        yield


class CenterTests(unittest.TestCase):
    def test_submission_pool_backpressure_keeps_id_and_can_be_cancelled(self):
        for cancel in (False,True):
            with self.subTest(cancel=cancel),scratch() as root,provider(Path(root)/'worker') as (worker,remote):
                original=remote.submit_bundle;calls=[];blocked=threading.Event()
                def submit(identity,payload,**options):
                    calls.append(identity)
                    if cancel or len(calls)<=2:
                        blocked.set()
                        raise PoolBusy('local queue busy; nothing sent')
                    return original(identity,payload,**options)
                with Pool([remote]) as pool,center(pool,Path(root)/'center') as (state,client),patch.object(remote,'submit_bundle',side_effect=submit):
                    identity=client.submit(python('print(42)'))['id']
                    self.assertTrue(blocked.wait(2))
                    self.assertNotIn(client.status(identity)['status'],('interrupted','failed'))
                    self.assertEqual(pool.nodes[0].reserved,1)
                    if cancel:client.cancel(identity)
                    result=client.wait(identity)
                    self.assertEqual(result['status'],'cancelled' if cancel else 'succeeded',result)
                    self.assertEqual(set(calls),{identity})
                    if cancel:
                        self.assertNotIn(identity,worker.jobs)
                        client.delete(identity)
                    else:
                        self.assertEqual(client.fetch(identity)['stdout'].strip(),'42')
                    until(lambda:pool.nodes[0].reserved==0)

    def test_overlapping_node_sets_have_feasible_project_targets_and_admission(self):
        with scratch() as root,provider(Path(root)/'pc',cpus=2,tags=['windows_only']) as (_,pc), \
                provider(Path(root)/'tx',cpus=6,tags=['windows_only']) as (_,tx), \
                provider(Path(root)/'server',cpus=12,tags=['linux_only']) as (_,server):
            with Pool([pc,tx,server]) as pool,center(pool,Path(root)/'center') as (_,client):
                client.control(paused=True)
                tasks=[python('pass',project='ARH',node='node1'),
                       python('pass',project='ARH',cpus=0,node='node2'),
                       python('pass',project='Bench',cpus=0,max_cpus=2,node='node3')]
                tasks += [python('pass',project='Bench',requires=['windows_only']) for _ in range(10)]
                identities=[client.submit(task)['id'] for task in tasks]
                resources=client.health()['projects']
                self.assertEqual({p:r['target'] for p,r in resources.items()},{'ARH':5,'Bench':5})
                self.assertEqual(client.admission('ARH',node='node2',cpus=0)['target_cpus'],5)
                for identity in identities:client.cancel(identity);client.delete(identity)

    def test_auto_reloan_returns_excess_only_when_the_running_task_requests_it(self):
        hold="from pathlib import Path; import time\nwhile not Path('release').exists():time.sleep(.02)"
        with scratch() as root,provider(Path(root)/'worker',cpus=4) as (_,worker):
            with Pool([worker]) as pool,center(pool,Path(root)/'center') as (_,client):
                first=client.submit(python(hold,project='A',cpus=0,memory_mb=64))['id']
                until(lambda:client.status(first)['status']=='running')
                second=client.submit(python(hold,project='B',cpus=0,memory_mb=64))['id']
                self.assertEqual(client.status(first)['cpus'],4)
                self.assertEqual(client.resize(first,4,memory_mb=64)['cpus'],4)
                grant=client.resize(first,0,memory_mb=64)
                self.assertEqual(grant['cpus'],2)
                until(lambda:client.status(second)['status']=='running')
                self.assertEqual(client.status(second)['cpus'],2)
                for identity in (first,second):
                    client.write_job_file(identity,'release','done');client.wait(identity);client.fetch(identity)

    def test_capability_handshake_omits_large_dashboard_and_keeps_full_health(self):
        from remote_compute.local import LocalClient
        with scratch() as root, provider(Path(root)/'worker') as (worker,node):
            worker.benchmark['trials']=['retained benchmark detail'*1000]*4
            with Pool([node]) as pool, center(pool,Path(root)/'center') as (_,client):
                full=client.health()
                self.assertGreater(len(json.dumps(full)),90000)
                for backend in (client,node,LocalClient(worker)):
                    brief=backend.health(brief=True)
                    self.assertLess(len(json.dumps(brief)),512)
                    self.assertEqual(set(brief),{'protocol','node','role','version','capabilities'})
                    self.assertIn('resize_memory_model',brief['capabilities'])
                client._capabilities=None
                with patch.object(client,'request',wraps=client.request) as requests:
                    with self.assertRaises(RemoteError):client.resize('0'*32,0,memory_mb=64)
                    self.assertEqual(requests.call_args_list[0].args,('/health',))
                    self.assertEqual(requests.call_args_list[0].kwargs,{'brief':True})
                self.assertIn('nodes',client.health())

    def test_project_shares_and_prefetch_exclude_unusable_server_capacity(self):
        with scratch() as root, provider(Path(root)/'native',cpus=2,cpu_limit=2,tags=['native_only']) as (_,native), \
                provider(Path(root)/'bulk',cpus=8,cpu_limit=8,tags=['bulk_only']) as (_,bulk):
            with Pool([native,bulk]) as pool, center(pool,Path(root)/'center') as (_,client):
                client.control(paused=True)
                identities=[client.submit(python('pass',project=project,requires=['native_only']))['id']
                            for project in ('A','B') for _ in range(4)]
                resources=client.health()['projects']
                self.assertEqual([resources[p]['target'] for p in ('A','B')],[1,1])
                self.assertEqual([resources[p]['capacity'] for p in ('A','B')],[2,2])
                window=client.admission('B',requires=['native_only'],queue_ratio=1)
                self.assertEqual((window['target_cpus'],window['in_flight']),(1,2))
                for identity in identities:client.cancel(identity);client.delete(identity)

    def test_window_tracks_new_worker_capabilities_and_budget_reductions(self):
        import sys
        with scratch() as root, provider(Path(root)/'one',cpus=2,cpu_limit=2,tools={'bench_python':sys.executable}) as (_,one), \
                provider(Path(root)/'two',cpus=4,cpu_limit=4) as (_,two):
            with Pool([one,two]) as pool, center(pool,Path(root)/'center') as (_,client):
                window=client.window('Bench',requires=['bench_python'],queue_ratio=0)
                self.assertEqual((window.current(),window.maximum),(2,2))
                client.control(node='node2',worker={'tools':{'bench_python':[sys.executable]}})
                window.updated=0
                self.assertEqual((window.current(),window.maximum),(6,6))
                client.control(node='node2',worker={'tools':{'bench_python':None}})
                window.updated=0
                self.assertEqual((window.current(),window.maximum),(2,2))

    def test_modeled_resize_fits_cpu_and_ram_and_rechecks_later_budget(self):
        with scratch() as root,provider(Path(root)/'worker',cpus=4,memory_mb=768,memory_reserve_mb=0) as (_,worker):
            with Pool([worker]) as pool,center(pool,Path(root)/'center') as (state,client):
                identity=client.submit(python('import time;time.sleep(30)',memory_mb=64))['id']
                until(lambda:client.status(identity)['status']=='running')
                grant=client.resize(identity,0,base_memory_mb=128,memory_per_cpu_mb=256)
                self.assertEqual((grant['cpus'],grant['memory_mb']),(2,640))
                self.assertEqual((pool.nodes[0].reserved,pool.nodes[0].reserved_memory_mb),(2,640))
                grant=client.resize(identity,0,base_memory_mb=128,memory_per_cpu_mb=256,memory_limit_mb=384)
                self.assertEqual((grant['cpus'],grant['memory_mb']),(1,384))
                client.control(node='node1',worker={'memory_mb':1536})
                grant=client.resize(identity,0,base_memory_mb=128,memory_per_cpu_mb=256,memory_limit_mb=1152)
                self.assertEqual((grant['cpus'],grant['memory_mb']),(4,1152))
                self.assertEqual(worker.status(identity)['cpus'],4)
                client.cancel(identity);client.wait(identity);client.fetch(identity)
                until(lambda:pool.nodes[0].reserved==0)
                self.assertEqual(pool.nodes[0].reserved_memory_mb,0)

    def test_modeled_resize_rejection_preserves_existing_grant_and_demand(self):
        with scratch() as root,provider(Path(root)/'worker',cpus=4,memory_mb=512,memory_reserve_mb=0) as (_,worker):
            with Pool([worker]) as pool,center(pool,Path(root)/'center') as (state,client):
                identity=client.submit(python('import time;time.sleep(30)',memory_mb=64))['id']
                until(lambda:client.status(identity)['status']=='running')
                with self.assertRaises(RemoteError):client.resize(identity,0,base_memory_mb=1024,memory_per_cpu_mb=64)
                self.assertEqual((pool.nodes[0].reserved,pool.nodes[0].reserved_memory_mb),(1,64))
                self.assertNotIn('desired_cpus',state.jobs[identity])
                client.cancel(identity);client.wait(identity);client.fetch(identity)

    def test_memory_model_legacy_fallback_and_validation(self):
        from unittest.mock import Mock
        from remote_compute.allocation import plan_resize
        client=Client('http://old-center');client._capabilities=['resize','resize_auto','resize_memory']
        client.request=Mock(return_value=dict(cpus=1,memory_mb=64))
        self.assertFalse(client.resize('original',0,base_memory_mb=128,memory_per_cpu_mb=64)['supported'])
        self.assertEqual(client.request.call_count,1)
        self.assertNotIn('resize',client.request.call_args.args[0])
        with self.assertRaises(ValueError):plan_resize(4,64,128,64,None,1024)
        for base,per in ((None,64),(64,None),(-1,64),(64,True),(0,0)):
            with self.assertRaises(ValueError):plan_resize(4,None,base,per,None,1024)

    def test_live_tool_alias_keeps_accepted_tool_provenance(self):
        import sys
        with scratch() as root,provider(Path(root)/'worker',cpus=2,tools={'g++':[sys.executable]}) as (_,worker):
            with Pool([worker]) as pool,center(pool,Path(root)/'center') as (_,client):
                self.assertIn('g++',worker.health()['tags'])
                identity=client.submit(python('import time;time.sleep(30)'))['id']
                until(lambda:client.status(identity)['status']=='running')
                before=worker.status(identity)['tools']
                client.control(node='node1',worker={'tools':{'verified_python':[sys.executable]}})
                self.assertIn('verified_python',worker.health()['tags'])
                result=client.run(dict(python('print(42)'),requires=['verified_python'],steps=[['${tool:verified_python}','-c','print(42)']]))
                self.assertEqual(result['status'],'succeeded',result)
                self.assertNotIn('verified_python',worker.status(identity)['tools'])
                self.assertEqual(worker.status(identity)['tools'],before)
                client.control(node='node1',worker={'tools':{'verified_python':None}})
                self.assertNotIn('verified_python',worker.health()['tags'])
                client.cancel(identity);client.wait(identity);client.fetch(identity)

    def test_auto_resize_rechecks_budget_after_a_partial_initial_grant(self):
        with scratch() as root,provider(Path(root)/'worker',cpus=4,cpu_limit=4) as (_,worker):
            with Pool([worker]) as pool,center(pool,Path(root)/'center') as (_,client):
                blocker=client.submit(python('import time;time.sleep(30)',cpus=3,project='other'))['id']
                until(lambda:client.status(blocker)['status']=='running')
                identity=client.submit(python('import time;time.sleep(30)',cpus=0,max_cpus=3,project='A'))['id']
                until(lambda:client.status(identity)['status']=='running')
                self.assertEqual(client.status(identity)['cpus'],1)
                client.cancel(blocker);client.wait(blocker);client.fetch(blocker)
                grant=client.resize(identity,0,memory_mb=384)
                self.assertEqual((grant['cpus'],grant['memory_mb']),(3,384))
                self.assertEqual(grant['requested_cpus'],0)
                self.assertEqual(worker.status(identity)['cpus'],3)
                self.assertIn('resize_auto',client.health()['capabilities'])
                client.resize(identity,1,memory_mb=128)
                client.control(node='node1',worker={'cpus':2})
                self.assertEqual(client.resize(identity,0,memory_mb=256)['cpus'],2)
                client.cancel(identity);client.wait(identity);client.fetch(identity)
                until(lambda:pool.nodes[0].reserved==0)
                self.assertEqual(pool.nodes[0].reserved_memory_mb,0)

    def test_auto_resize_keeps_existing_grant_on_older_center(self):
        from unittest.mock import Mock
        client=Client('http://old-center')
        client._capabilities=['resize','resize_memory']
        client.request=Mock(return_value=dict(id='original',cpus=1,memory_mb=256,assigned_node='same'))
        grant=client.resize('original',0,memory_mb=512)
        self.assertFalse(grant['supported'])
        self.assertEqual((grant['cpus'],grant['memory_mb']),(1,256))
        self.assertEqual(client.request.call_count,1)
        self.assertNotIn('resize',client.request.call_args.args[0])

    def test_node_public_ip_override_survives_load_configure_snapshot_and_save(self):
        from types import SimpleNamespace
        with scratch() as root,provider(Path(root)/'worker') as (_,worker):
            address='203.0.113.42'
            with Pool([dict(name='remote',client=worker,host_ip=address)]) as pool:
                self.assertEqual(pool.snapshot()['nodes'][0]['host_ip'],address)
                for invalid in (None,'http://server','not-an-ip'):
                    with self.assertRaises(ValueError):pool.configure('remote',host_ip=invalid)
                pool.configure('remote',host_ip='203.0.113.43')
                shell=SimpleNamespace(pool=pool,center=None)
                with patch('remote_compute.commands.ROOT',Path(root)):
                    SchedulerShell.do_save(shell,'')
                saved=json.loads((Path(root)/'nodes.local.json').read_text())
                self.assertEqual(saved['nodes'][0]['host_ip'],'203.0.113.43')

    def test_callback_timeout_applies_only_to_callback_default_clients(self):
        with patch.dict('os.environ',{'REMOTE_COMPUTE_URL':'http://center:1234','REMOTE_COMPUTE_TIMEOUT':'120'}):
            self.assertEqual(Client('http://center:1234/').timeout,120)
            self.assertEqual(Client('http://center:1234',timeout=2).timeout,2)
            self.assertEqual(Client('http://other:1234').timeout,10)

    def test_unchanged_grant_does_not_contact_worker(self):
        with scratch() as root,provider(Path(root)/'worker') as (_,worker):
            with Pool([worker]) as pool,center(pool,Path(root)/'center') as (_,client):
                identity=client.submit(python('import time;time.sleep(30)'))['id']
                until(lambda:client.status(identity)['status']=='running')
                with patch.object(pool.nodes[0].client,'resize') as resize:
                    grant=client.resize(identity,1,memory_mb=256)
                    self.assertEqual((grant['cpus'],grant['memory_mb']),(1,256))
                    resize.assert_not_called()
                client.cancel(identity);client.wait(identity);client.fetch(identity)

    def test_slow_resize_reserves_growth_without_blocking_admission(self):
        with scratch() as root,provider(Path(root)/'worker',cpus=4) as (_,worker):
            with Pool([worker]) as pool,center(pool,Path(root)/'center') as (state,client):
                identity=client.submit(python('import time;time.sleep(30)',project='A'))['id']
                until(lambda:client.status(identity)['status']=='running')
                backend=pool.nodes[0].client;original=backend.resize
                entered,release=threading.Event(),threading.Event();results=[]
                def delayed(*args,**kwargs):
                    entered.set();release.wait(5)
                    return original(*args,**kwargs)
                with patch.object(backend,'resize',side_effect=delayed):
                    thread=threading.Thread(target=lambda:results.append(state.resize(identity,4,memory_mb=512)))
                    thread.start()
                    try:
                        self.assertTrue(entered.wait(2))
                        quick=Client(client.url,timeout=.5)
                        self.assertEqual(quick.health()['used_cpus'],4)
                        self.assertEqual(quick.admission('B')['target_cpus'],2)
                        self.assertEqual(pool.nodes[0].reserved_memory_mb,512)
                        self.assertEqual(worker.health()['used_cpus'],1)
                        other=quick.submit(python('pass',project='B'))['id']
                        self.assertEqual(quick.status(other)['status'],'queued')
                    finally:
                        release.set();thread.join(5)
                self.assertEqual(results[0]['cpus'],4)
                client.cancel(identity);client.wait(identity);client.fetch(identity)
                client.wait(other);client.fetch(other)
                until(lambda:pool.nodes[0].reserved==0)
                self.assertEqual(pool.nodes[0].reserved_memory_mb,0)

    def test_late_resize_ack_after_completion_does_not_release_twice(self):
        with scratch() as root,provider(Path(root)/'worker',cpus=4) as (_,worker):
            with Pool([worker]) as pool,center(pool,Path(root)/'center') as (state,client):
                identity=client.submit(python('import time;time.sleep(30)'))['id']
                until(lambda:client.status(identity)['status']=='running')
                backend=pool.nodes[0].client;original=backend.resize
                entered,release=threading.Event(),threading.Event();results=[]
                def delayed(*args,**kwargs):
                    value=original(*args,**kwargs);entered.set();release.wait(5)
                    return value
                with patch.object(backend,'resize',side_effect=delayed):
                    thread=threading.Thread(target=lambda:results.append(state.resize(identity,4)))
                    thread.start()
                    try:
                        self.assertTrue(entered.wait(2))
                        client.cancel(identity);client.wait(identity);client.fetch(identity)
                        until(lambda:pool.nodes[0].reserved==0)
                    finally:
                        release.set();thread.join(5)
                self.assertEqual(results[0]['cpus'],4)
                self.assertEqual(pool.nodes[0].reserved,0)
                self.assertEqual(pool.nodes[0].reserved_memory_mb,0)

    def test_rejected_resize_restores_cpu_and_ram_reservations(self):
        with scratch() as root,provider(Path(root)/'worker',cpus=4) as (_,worker):
            with Pool([worker]) as pool,center(pool,Path(root)/'center') as (state,client):
                identity=client.submit(python('import time;time.sleep(30)'))['id']
                until(lambda:client.status(identity)['status']=='running')
                error=RemoteError('resize rejected');error.status_code=409
                with patch.object(pool.nodes[0].client,'resize',side_effect=error):
                    with self.assertRaises(RemoteError):client.resize(identity,4,memory_mb=512)
                self.assertEqual((pool.nodes[0].reserved,pool.nodes[0].reserved_memory_mb),(1,256))
                self.assertNotIn('desired_cpus',state.jobs[identity])
                client.cancel(identity);client.wait(identity);client.fetch(identity)

    def test_uncertain_resize_keeps_budget_and_reconfirms_same_grant(self):
        with scratch() as root,provider(Path(root)/'worker',cpus=4) as (_,worker):
            with Pool([worker]) as pool,center(pool,Path(root)/'center') as (state,client):
                identity=client.submit(python('import time;time.sleep(30)'))['id']
                until(lambda:client.status(identity)['status']=='running')
                backend=pool.nodes[0].client;original=backend.resize
                def lost_reply(*args,**kwargs):
                    original(*args,**kwargs)
                    raise RemoteError('lost resize acknowledgement')
                with patch.object(backend,'resize',side_effect=lost_reply),patch.object(backend,'status',side_effect=RemoteError('status unavailable')):
                    with self.assertRaises(RemoteError):state.resize(identity,4,memory_mb=512)
                    self.assertEqual((pool.nodes[0].reserved,pool.nodes[0].reserved_memory_mb),(4,512))
                    self.assertTrue(state.jobs[identity]['resize_uncertain'])
                with patch.object(backend,'resize',wraps=original) as resize:
                    self.assertEqual(client.resize(identity,4,memory_mb=512)['cpus'],4)
                    self.assertEqual(resize.call_count,1)
                self.assertNotIn('resize_uncertain',state.jobs[identity])
                client.cancel(identity);client.wait(identity);client.fetch(identity)
                until(lambda:pool.nodes[0].reserved==0)
                self.assertEqual(pool.nodes[0].reserved_memory_mb,0)

    def test_serial_hint_is_center_metadata_and_not_forwarded_to_older_worker(self):
        import zipfile
        with scratch() as root,provider(Path(root)/'worker') as (_,worker):
            original=worker.submit_bundle
            def submit(identity,payload,**options):
                with zipfile.ZipFile(io.BytesIO(payload)) as archive:
                    self.assertNotIn('single_thread_weight',json.loads(archive.read('task.json')))
                return original(identity,payload,**options)
            with Pool([worker]) as pool,center(pool,Path(root)/'center') as (_,client),patch.object(worker,'submit_bundle',side_effect=submit):
                result=client.run(python('print(42)',single_thread_weight=2))
                self.assertEqual(result['status'],'succeeded',result)
                self.assertEqual(result['single_thread_weight'],2)
                self.assertIn('single_thread_weight',client.health()['capabilities'])



    def test_saved_remote_node_keeps_local_token_when_console_only_has_center_client(self):
        from contextlib import redirect_stdout
        from types import SimpleNamespace
        with scratch() as directory:
            root=Path(directory)
            remote=dict(name='remote',url='https://203.0.113.42:26133',weight=1.0,limit=0,
                        memory_limit_mb=0,enabled=True)
            (root/'nodes.local.json').write_text(json.dumps({'nodes':[dict(remote,token='private-worker-token')]}))
            shell=SimpleNamespace(pool=SimpleNamespace(nodes=[]),
                                  center=SimpleNamespace(nodes=lambda:dict(policy='adaptive',nodes=[remote]),token=''))
            with patch('remote_compute.commands.ROOT',root),io.StringIO() as output,redirect_stdout(output):
                SchedulerShell.do_save(shell,'')
            saved=json.loads((root/'nodes.local.json').read_text())['nodes'][0]
            self.assertEqual(saved['token'],'private-worker-token')

    def test_offline_worker_keeps_health_and_local_submission_available(self):
        with scratch() as root, provider(Path(root)/'local') as (_,local), provider(Path(root)/'offline') as (_,offline):
            with patch.object(offline,'health',side_effect=RemoteError('worker unavailable')):
                with Pool([dict(name='local',client=local),dict(name='offline',client=offline)]) as pool:
                    with center(pool,Path(root)/'center') as (_,client):
                        health=client.health()
                        self.assertEqual(health['cpus'],1)
                        self.assertEqual(health['nodes'][1]['active_jobs'],[])
                        self.assertIn('worker unavailable',health['nodes'][1]['error'])
                        self.assertEqual(client.run(python("print('local works')"))['stdout'].strip(),'local works')

    def test_known_offline_worker_keeps_queued_work_until_reconnected(self):
        with scratch() as root, provider(Path(root)/'worker') as (_,worker), Pool([dict(name='remote',client=worker)],refresh=60) as pool:
            with center(pool,Path(root)/'center') as (_,client):
                with patch.object(worker,'health',side_effect=RemoteError('worker unavailable')):
                    pool.refresh()
                    identity=client.submit(python("print('executed once')",node='remote'))['id']
                    time.sleep(.15)
                    self.assertEqual(client.status(identity)['status'],'queued')
                    self.assertEqual(worker.status(),[])
                    self.assertEqual(client.health()['cpus'],0)
                pool.refresh()
                client.wait(identity)
                self.assertEqual(client.fetch(identity)['stdout'].strip(),'executed once')

    def test_interrupted_result_recovers_worker_outcome_and_upstream_failure_is_typed(self):
        for private in (False,True):
            with self.subTest(private=private),scratch() as root,provider(Path(root)/'worker') as (state,worker):
                if private:
                    from remote_compute.local import LocalClient
                    worker=LocalClient(state)
                with Pool([worker]) as pool,center(pool,Path(root)/'center') as (coordinator,client):
                    for code,expected in (("from pathlib import Path;Path('value').write_text('42')",'succeeded'),("raise RuntimeError('actual failure')",'failed')):
                        identity=client.submit(python(code,outputs=['value']))['id']
                        client.wait(identity)
                        with coordinator.condition:
                            coordinator.jobs[identity]['result'].update(status='interrupted',error='connection lost')
                        with patch.object(worker,'request',side_effect=RemoteError('still unavailable')):
                            with self.assertRaises(RemoteError) as raised:client.fetch(identity,delete=False)
                            self.assertEqual(raised.exception.status_code,502)
                        result=client.fetch(identity,delete=False)
                        self.assertEqual(result['status'],expected)
                        self.assertEqual(result['id'],identity)
                        self.assertEqual(result['assigned_node'],'node1')
                        if expected=='succeeded':
                            self.assertEqual(result['files']['value'],b'42')
                            self.assertNotIn('error',result)
                        else:
                            self.assertIn('actual failure',result['stderr'])
                        self.assertEqual(client.status(identity)['status'],expected)
                        client.delete(identity)

    def test_cpu_placement_prefers_idle_host_then_fills_thread_budgets(self):
        code="from pathlib import Path;import time\nwhile not Path('release').exists():time.sleep(.05)"
        for policy in ('adaptive','balanced'):
            with self.subTest(policy=policy), scratch() as root:
                with provider(Path(root)/'busy',cpus=8) as (busy,one), provider(Path(root)/'idle',cpus=8) as (idle,two):
                    with host_load(busy,75,'busy-host'), host_load(idle,0,'idle-host'):
                        with Pool([dict(name='busy',url=one.url),dict(name='idle',url=two.url)],policy=policy,refresh=60) as pool:
                            pool.nodes[0].durations['cpu-placement']=.01
                            pool.nodes[1].durations['cpu-placement']=1
                            with center(pool,Path(root)/'center') as (_,client):
                                client.control(paused=True)
                                ids=[client.submit(python(code,project='CPU-placement',label='cpu-placement'))['id'] for _ in range(16)]
                                client.control(paused=False)
                                until(lambda:client.health()['used_cpus']==16)
                                until(lambda:all(client.status(i)['status']=='running' for i in ids))
                                assigned=[client.status(i)['assigned_node'] for i in ids]
                                self.assertEqual(assigned[:6],['idle']*6)
                                self.assertEqual((assigned.count('busy'),assigned.count('idle')),(8,8))
                                for identity in ids:
                                    client.write_job_file(identity,'release','done');client.wait(identity);client.fetch(identity)
                                until(lambda:all(n.reserved==0 for n in pool.nodes))
                                client.control(node='idle',routing=dict(memory_limit_mb=128))
                                result=client.run(python('pass',project='CPU-placement',memory_mb=256))
                                self.assertEqual(result['assigned_node'],'busy')

    def test_cpu_placement_counts_sampled_work_once_and_pending_grants_immediately(self):
        code="from pathlib import Path;import time\nwhile not Path('release').exists():time.sleep(.05)"
        with scratch() as root, provider(Path(root)/'warm',cpus=8) as (warm,one), provider(Path(root)/'busy',cpus=8) as (busy,two):
            with host_load(warm,50,'warm-host'), host_load(busy,75,'busy-host'):
                with Pool([dict(name='warm',url=one.url),dict(name='busy',url=two.url)],refresh=60) as pool:
                    with center(pool,Path(root)/'center') as (_,client):
                        initial=client.submit(python(code,cpus=4,node='warm',project='CPU-placement'))['id']
                        until(lambda:client.status(initial)['status']=='running')
                        pool.refresh()
                        client.control(paused=True)
                        ids=[client.submit(python(code,project='CPU-placement'))['id'] for _ in range(3)]
                        client.control(paused=False)
                        until(lambda:client.health()['used_cpus']==7)
                        self.assertEqual([client.status(i)['assigned_node'] for i in ids],['warm','warm','busy'])
                        for identity in [initial,*ids]:
                            client.write_job_file(identity,'release','done');client.wait(identity);client.fetch(identity)

    def test_auto_window_runs_both_nodes_without_extra_prefetch(self):
        with scratch() as root, provider(Path(root)/'one',cpus=2) as (_,one), provider(Path(root)/'two',cpus=2) as (_,two):
            with Pool([one,two]) as pool, center(pool,Path(root)/'center') as (_,client):
                window=client.window('automatic',cpus=0,queue_ratio=0)
                self.assertEqual(window.current(),2)
                ids=[client.submit(python("from pathlib import Path;import time\nwhile not Path('release').exists():time.sleep(.02)",
                    cpus=0,project='automatic'))['id'] for _ in range(window.current())]
                until(lambda:client.health()['used_cpus']==4)
                self.assertEqual(len({client.status(i)['assigned_node'] for i in ids}),2)
                for identity in ids:
                    client.write_job_file(identity,'release','done');client.wait(identity);client.fetch(identity)

    def test_auto_window_refills_after_running_tasks_return_cpu_credits(self):
        hold="from pathlib import Path;import time\nwhile not Path('release').exists():time.sleep(.02)"
        with scratch() as root, provider(Path(root)/'one',cpus=4,cpu_limit=4) as (_,one), provider(Path(root)/'two',cpus=4,cpu_limit=4) as (_,two):
            with Pool([one,two]) as pool, center(pool,Path(root)/'center') as (_,client):
                window=client.window('phased',cpus=0,queue_ratio=0)
                self.assertEqual((window.maximum,window.current()),(8,2))
                ids=[client.submit(python(hold,cpus=0,project='phased'))['id'] for _ in range(2)]
                until(lambda:client.health()['used_cpus']==8)
                until(lambda:all(client.status(identity)['status']=='running' for identity in ids))
                for identity in ids:client.resize(identity,1)
                window.updated=0
                self.assertEqual(window.current(),4)
                queued=client.admission('phased',cpus=0,queue_ratio=1)
                self.assertEqual((queued['running_tasks'],queued['queued_tasks'],queued['in_flight'],queued['max_in_flight']),
                                 (4,2,6,10))
                first=list(ids)
                ids += [client.submit(python(hold,cpus=0,project='phased'))['id'] for _ in range(2)]
                until(lambda:client.health()['used_cpus']==8)
                self.assertTrue(all(client.status(identity)['cpus']==1 for identity in first))
                window.updated=0
                self.assertEqual(window.current(),4)
                for identity in ids:
                    client.write_job_file(identity,'release','done');client.wait(identity);client.fetch(identity)

    def test_capped_auto_window_counts_partial_grants_and_keeps_prefetch_bounded(self):
        hold="from pathlib import Path;import time\nwhile not Path('release').exists():time.sleep(.02)"
        with scratch() as root, provider(Path(root)/'worker',cpus=4,cpu_limit=4) as (_,worker):
            with Pool([worker]) as pool, center(pool,Path(root)/'center') as (_,client):
                ids=[client.submit(python(hold,cpus=0,max_cpus=2,project='capped'))['id'] for _ in range(2)]
                until(lambda:client.health()['used_cpus']==4)
                until(lambda:all(client.status(identity)['status']=='running' for identity in ids))
                client.resize(ids[0],1)
                window=client.admission('capped',cpus=0,max_cpus=2,queue_ratio=1)
                self.assertEqual((window['running_tasks'],window['queued_tasks'],window['max_in_flight']),(3,2,6))
                ids.append(client.submit(python(hold,cpus=0,max_cpus=2,project='capped'))['id'])
                until(lambda:client.health()['used_cpus']==4)
                self.assertEqual(client.status(ids[-1])['cpus'],1)
                for identity in ids:
                    client.write_job_file(identity,'release','done');client.wait(identity);client.fetch(identity)

    def test_auto_window_counts_only_its_own_projects_running_tasks(self):
        with scratch() as root, provider(Path(root)/'worker',cpus=4,cpu_limit=4) as (_,worker):
            with Pool([worker]) as pool, center(pool,Path(root)/'center') as (_,client):
                identity=client.submit(python('import time;time.sleep(30)',cpus=0,project='other'))['id']
                until(lambda:client.status(identity)['status']=='running')
                window=client.admission('newcomer',cpus=0,queue_ratio=0)
                self.assertEqual((window['running_tasks'],window['in_flight']),(1,1))
                client.cancel(identity);client.wait(identity);client.fetch(identity)

    def test_cancellation_is_sent_once_and_terminal_polling_stays_bounded(self):
        with scratch() as root, provider(Path(root)/'worker') as (_,worker):
            with Pool([worker]) as pool, center(pool,Path(root)/'center') as (_,client):
                identity=client.submit(python('import time;time.sleep(30)'))['id']
                until(lambda:client.status(identity)['status']=='running')
                backend=pool.nodes[0].client;original=backend.status;deadline=time.monotonic()+.7
                def delayed_terminal(*args,**kwargs):
                    result=original(*args,**kwargs)
                    if time.monotonic()<deadline:result['status']='running'
                    return result
                with patch.object(backend,'cancel',wraps=backend.cancel) as cancel,patch.object(backend,'status',side_effect=delayed_terminal) as status:
                    client.cancel(identity);client.wait(identity)
                    self.assertEqual(cancel.call_count,1)
                    self.assertLessEqual(status.call_count,9)
                client.fetch(identity)

    def test_stale_probe_does_not_count_our_released_job_as_external(self):
        with scratch() as root, provider(Path(root)/'worker',cpus=4) as (_,worker):
            with Pool([worker],refresh=60) as pool, center(pool,Path(root)/'center') as (_,client):
                code='import time;time.sleep(30)'
                identity=client.submit(python(code,memory_mb=1))['id']
                until(lambda:client.status(identity)['status']=='running')
                until(lambda:any(j['id']==identity and j['rss_mb']>1 for j in worker.health()['active_jobs']))
                external=worker.submit(python(code))['id']
                until(lambda:worker.status(external)['status']=='running')
                self.assertEqual(client.resize(identity,3)['cpus'],3)
                entered,release=threading.Event(),threading.Event();original=pool.nodes[0].client.health
                def delayed_health(**options):
                    result=original(**options);entered.set();release.wait(5);return result
                with patch.object(pool.nodes[0].client,'health',side_effect=delayed_health):
                    probe=threading.Thread(target=pool.refresh);probe.start()
                    try:
                        self.assertTrue(entered.wait(3))
                        self.assertEqual(client.resize(identity,1)['cpus'],1)
                        client.cancel(identity);client.wait(identity);client.fetch(identity)
                        until(lambda:pool.nodes[0].reserved==0)
                    finally:release.set();probe.join(5)
                self.assertEqual(pool.nodes[0].external,1)
                self.assertAlmostEqual(pool.nodes[0].external_memory_mb,256,places=2)
                self.assertIn(identity,pool.nodes[0].known_jobs)
                self.assertFalse(pool.nodes[0].live_jobs)
                pool.refresh()
                self.assertFalse(pool.nodes[0].known_jobs)
                worker.cancel(external);worker.wait(external);worker.fetch(external)
                pool.refresh()
                self.assertEqual(pool.nodes[0].external,0)
                self.assertEqual(pool.nodes[0].external_memory_mb,0)

    def test_disconnected_running_job_keeps_grant_and_recovers_original_id(self):
        with scratch() as root, provider(Path(root)/'worker',cpus=2) as (_,worker):
            with Pool([worker],refresh=60) as pool, center(pool,Path(root)/'center') as (_,client):
                backend=pool.nodes[0].client;original=backend.status;failed=threading.Event();restored=threading.Event()
                def disconnected(*args,**kwargs):
                    result=original(*args,**kwargs)
                    if not restored.is_set():
                        failed.set()
                        raise RemoteError('test connection interrupted')
                    return result
                with patch.object(backend,'status',side_effect=disconnected):
                    code="from pathlib import Path;import time\nwhile not Path('release').exists():time.sleep(.02)\nPath('value').write_text('42')"
                    identity=client.submit(python(code,outputs=['value']))['id']
                    self.assertTrue(failed.wait(3))
                    self.assertNotEqual(client.status(identity)['status'],'interrupted')
                    self.assertEqual(pool.nodes[0].reserved,1)
                    pool.refresh()
                    self.assertEqual(pool.nodes[0].external,0)
                    worker.write_job_file(identity,'release','done')
                    restored.set()
                    self.assertEqual(client.wait(identity)['status'],'succeeded')
                    self.assertNotIn('connection_error',client.status(identity))
                    self.assertEqual(client.fetch(identity)['files']['value'],b'42')
                until(lambda:pool.nodes[0].reserved==0)
                pool.refresh()
                self.assertEqual(pool.nodes[0].external,0)

    def test_client_reset_closes_keepalive_without_a_server_traceback(self):
        import http.client
        import os
        import socket
        import struct
        from urllib.parse import urlsplit
        from remote_compute.worker import Handler
        finished=threading.Event();original=Handler.finish
        def finish(handler):
            try:original(handler)
            finally:finished.set()
        with scratch() as root, provider(Path(root)/'worker') as (_,client):
            endpoint=urlsplit(client.url)
            with patch.object(Server,'handle_error') as errors,patch.object(Handler,'finish',finish):
                connection=http.client.HTTPConnection(endpoint.hostname,endpoint.port)
                connection.request('GET','/health',headers={'X-Remote-Compute-Keepalive':'1'});connection.getresponse().read()
                connection.sock.setsockopt(socket.SOL_SOCKET,socket.SO_LINGER,struct.pack('hh' if os.name=='nt' else 'ii',1,0))
                connection.close()
                self.assertTrue(finished.wait(3))
                errors.assert_not_called()
            self.assertEqual(client.health()['cpus'],1)

    def test_resize_lost_reply_recovers_the_original_grant(self):
        with scratch() as root, provider(Path(root)/'worker',cpus=4) as (_,worker):
            with Pool([worker]) as pool, center(pool,Path(root)/'center') as (_,client):
                identity=client.submit(python('import time; time.sleep(30)'))['id']
                until(lambda:client.status(identity)['status']=='running')
                original=pool.nodes[0].client.resize
                def lost_reply(*args):
                    original(*args)
                    raise RemoteError('connection closed after accepting resize')
                with patch.object(pool.nodes[0].client,'resize',side_effect=lost_reply) as resize:
                    self.assertEqual(client.resize(identity,4)['cpus'],4)
                    self.assertEqual(resize.call_count,1)
                self.assertEqual(pool.nodes[0].reserved,4)
                self.assertEqual(worker.health()['used_cpus'],4)
                client.cancel(identity);client.wait(identity);client.fetch(identity)
                until(lambda:pool.nodes[0].reserved==0)
                self.assertEqual(worker.health()['used_cpus'],0)

    def test_submission_window_refreshes_shares_and_honors_node(self):
        with scratch() as root, provider(Path(root)/'big',cpus=4,cpu_limit=4) as (_,big), provider(Path(root)/'small',cpus=2,cpu_limit=2) as (_,small):
            with Pool([dict(name='big',url=big.url),dict(name='small',url=small.url)]) as pool, center(pool,Path(root)/'center') as (_,client):
                client.control(paused=True)
                identities=[client.submit(python('pass',project='A'))['id'] for _ in range(6)]
                window=client.window('B',queue_ratio=0)
                self.assertEqual((window.maximum,window.current()),(6,3))
                pinned=client.admission('B',queue_ratio=0,node='small')
                self.assertEqual((pinned['target_cpus'],pinned['max_in_flight']),(2,2))
                for identity in identities:client.cancel(identity);client.delete(identity)
                window.updated=0
                self.assertEqual(window.current(),6)
                client.control(node='big',worker=dict(cpus=2))
                window.updated=0
                self.assertEqual(window.current(),4)

    def test_task_callback_resizes_original_job_and_preserves_bundle_identity(self):
        import hashlib
        from remote_compute.bundle import task_bundle
        with scratch() as root, provider(Path(root)/'worker',cpus=4,token='worker-token') as (_,worker):
            with Pool([worker]) as pool, center(pool,Path(root)/'center') as (state,client):
                code=("import os,sys,json,time\nfrom pathlib import Path\n"
                      "sys.path.insert(0,os.environ['REMOTE_COMPUTE_HOME'])\nfrom remote_compute import Client\n"
                      "c=Client(os.environ['REMOTE_COMPUTE_URL'],os.environ['REMOTE_COMPUTE_TOKEN'])\n"
                      "r=c.resize(os.environ['REMOTE_COMPUTE_JOB_ID'],4)\n"
                      "Path('grant.json').write_text(json.dumps(r))\n"
                      "while not Path('release').exists(): time.sleep(.02)\n"
                      "c.resize(os.environ['REMOTE_COMPUTE_JOB_ID'],1)\n")
                task=python(code,project='callback',outputs=['grant.json'])
                identity=client.submit(task)['id']
                until(lambda:client.status(identity)['cpus']==4)
                self.assertEqual(client.health()['used_cpus'],4)
                self.assertEqual(worker.health()['used_cpus'],4)
                with state.condition:
                    state._update(state.jobs[identity],dict(status='running',cpus=1))
                self.assertEqual(client.status(identity)['cpus'],4)
                client.write_job_file(identity,'release','done')
                client.wait(identity)
                result=client.fetch(identity,delete=False)
                self.assertEqual(result['status'],'succeeded',result.get('stderr'))
                self.assertEqual(result['cpus'],1)
                self.assertEqual((result['initial_cpus'],result['peak_cpus'],result['resize_count']),(1,4,2))
                self.assertEqual(json.loads(result['files']['grant.json'])['id'],identity)
                self.assertEqual(result['request_sha256'],hashlib.sha256(task_bundle(task)).hexdigest())
                self.assertEqual(client.submit(task,job_id=identity)['id'],identity)
                client.delete(identity)
                until(lambda:pool.nodes[0].reserved==0)
                self.assertEqual(worker.health()['used_cpus'],0)

    def test_running_grants_obey_project_shares_and_cancel_releases_latest_grant(self):
        with scratch() as root, provider(Path(root)/'worker',cpus=4) as (_,worker):
            with Pool([worker]) as pool, center(pool,Path(root)/'center') as (_,client):
                client.control(paused=True)
                task="from pathlib import Path;import time\nwhile not Path('release').exists():time.sleep(.02)"
                first=client.submit(python(task,project='A'))['id']
                second=client.submit(python(task,project='B'))['id']
                with self.assertRaises(RemoteError):client.resize(first,2)
                client.control(paused=False)
                until(lambda:all(client.status(i)['status']=='running' for i in (first,second)))
                self.assertEqual(client.resize(first,4)['cpus'],3)
                self.assertEqual(client.resize(second,4)['cpus'],1)
                self.assertEqual(client.health()['used_cpus'],4)
                self.assertEqual(client.resize(first,1)['cpus'],1)
                self.assertEqual(client.resize(second,4)['cpus'],3)
                client.cancel(first);client.wait(first);client.fetch(first)
                until(lambda:pool.nodes[0].reserved==3)
                self.assertEqual(client.resize(second,4)['cpus'],4)
                client.write_job_file(second,'release','done')
                result=client.wait(second);client.fetch(second)
                self.assertEqual(result['cpus'],4)
                until(lambda:pool.nodes[0].reserved==0)
                self.assertEqual(worker.health()['used_cpus'],0)

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
                started=worker.status(identity)['started']
                self.assertEqual(pool.nodes[0].health['active_jobs'][0]['started'],started)
                until(lambda:client.health()['nodes'][0]['active_jobs'][0]['started']==started)
                with pool.condition:
                    pool.nodes[0].health['active_jobs'][0]['rss_mb']=123.5
                self.assertEqual(client.health()['nodes'][0]['active_jobs'][0]['rss_mb'],123.5)
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
                self.assertEqual(snapshot['nodes'][0]['active_jobs'][0]['started'],worker.status(direct)['started'])
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
                    client.health()
                    from http.client import HTTPConnection
                    def center_connection(host,port,**options):
                        self.assertEqual((host,port),(client.endpoint.hostname,client.endpoint.port))
                        return HTTPConnection(host,port,**options)
                    with patch('remote_compute.client.http.client.HTTPConnection',side_effect=center_connection):
                        with patch('remote_compute.worker.pack',wraps=module.pack) as packed:
                            result=client.run(python("from pathlib import Path; Path('value').write_text('42')",outputs=['value']))
                        self.assertEqual(packed.call_count,1)
                    self.assertEqual(result['files']['value'],b'42')
                    self.assertEqual(result['worker_url'],local.url)
                    self.assertEqual(local.status(),[])
            finally:
                worker.close()

    def test_slow_asset_does_not_block_other_assets_or_control_jobs(self):
        from concurrent.futures import ThreadPoolExecutor
        with scratch() as root, provider(Path(root)/'worker') as (_,worker):
            with Pool([worker]) as pool, center(pool,Path(root)/'center') as (state,client):
                large=client.sync_files({'large':b'large model'})
                small=client.sync_files({'small':b'controller source'})
                node=pool.nodes[0]
                original=node.client.request
                entered,release=threading.Event(),threading.Event()
                def delayed(path,data=None,**options):
                    if path=='/assets/'+large and data is not None:
                        entered.set();release.wait(3)
                    return original(path,data,**options)
                with patch.object(node.client,'request',side_effect=delayed),ThreadPoolExecutor(max_workers=3) as transfers:
                    blocked=transfers.submit(state._ensure_assets,node,[large])
                    try:
                        self.assertTrue(entered.wait(2))
                        transfers.submit(state._ensure_assets,node,[small]).result(timeout=1)
                        transfers.submit(state._ensure_assets,node,[]).result(timeout=1)
                        self.assertFalse(blocked.done())
                    finally:
                        release.set();blocked.result(timeout=3)
                self.assertTrue({large,small}.issubset(node.known_assets))

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
                def delayed(*args,**kwargs):
                    entered.set()
                    release.wait(5)
                    return original(*args,**kwargs)
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
                ports.add(client._transport.idle[-1].sock.getsockname()[1])
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
