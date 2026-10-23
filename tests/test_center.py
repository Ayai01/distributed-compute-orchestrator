"""The center is exercised through the same client used by project adapters."""

from contextlib import contextmanager
import io
import json
from pathlib import Path
from types import SimpleNamespace
import threading
import time
import unittest
from unittest.mock import patch

from remote_compute import Client, Pool, RemoteError
from remote_compute.client import PoolBusy, RequestCancelled
from remote_compute.coordinator import Coordinator
from remote_compute.cli import SchedulerShell
from remote_compute.worker import Conflict, Server
from test_compute import provider, python, scratch, until


class AdmissionPlanningTests(unittest.TestCase):
    def center(self, capacities):
        from types import SimpleNamespace
        from remote_compute.client import Node
        pool=Pool.__new__(Pool)
        pool.condition=threading.Condition()
        pool.nodes=[]
        for index,capacity in enumerate(capacities):
            node=Node(str(index),SimpleNamespace(url=str(index)))
            node.health=dict(cpus=capacity,cpu_limit=capacity,tags=['ngspice47'],
                             memory_capacity_mb=8192,memory_available_mb=8192,host_memory_available_mb=8192)
            pool.nodes.append(node)
        state=Coordinator.__new__(Coordinator)
        state.pool,state.condition,state.jobs=pool,threading.Condition(),{}
        return state

    def test_admission_matches_legacy_capability_aliases_like_dispatch(self):
        state=self.center([2,4])
        for tag in ('ngspice47','ng47'):
            self.assertEqual(state.admission(dict(project='A',requires=[tag]))['capable_cpus'],6)

    def test_capped_auto_window_covers_each_nodes_partial_grant(self):
        for capacities,cap,expected in (([2,2,2],8,3),([3,3,3],2,6),([24,30,96],96,3)):
            with self.subTest(capacities=capacities,cap=cap):
                window=self.center(capacities).admission(dict(project='A',cpus=0,max_cpus=cap,queue_ratio=1))
                self.assertEqual((window['running_tasks'],window['queued_tasks']),(expected,expected))
                self.assertEqual(window['max_in_flight'],sum(capacities)+expected)

    def test_fixed_cpu_admission_excludes_nodes_below_the_task_width(self):
        window=self.center([2,8]).admission(dict(project='A',cpus=4,queue_ratio=0))
        self.assertEqual((window['capable_cpus'],window['target_cpus'],window['running_tasks']),(8,8,2))

    def test_fixed_cpu_window_does_not_combine_remainders_from_separate_devices(self):
        window=self.center([3,3]).admission(dict(project='A',cpus=2,queue_ratio=0))
        self.assertEqual((window['running_tasks'],window['max_in_flight']),(2,2))

    def test_fixed_cpu_window_can_repack_fractional_project_node_targets(self):
        state=self.center([12,12])
        for project in ('A','B'):
            for index in range(3):
                state.jobs[project+str(index)]=dict(
                    task=dict(steps=[['echo']],cpus=4,memory_mb=64,project=project),
                    auto_cpus=False,node=None,result=dict(status='queued'))
        resources=state.project_resources()
        for project in ('A','B'):
            self.assertEqual(sorted(resources[project]['node_targets'].values()),[6,6])
            window=state.admission(dict(project=project,cpus=4,queue_ratio=0))
            self.assertEqual((window['target_cpus'],window['running_tasks'],window['in_flight']),(12,3,3))

    def test_paused_workers_do_not_inflate_ready_project_share(self):
        state=self.center([2,8])
        state.pool.nodes[1].health['paused']=True
        window=state.admission(dict(project='A',queue_ratio=0))
        self.assertEqual((window['capable_cpus'],window['target_cpus'],window['running_tasks']),(2,2,2))
        state.pool.nodes[0].health['paused']=True
        window=state.admission(dict(project='A',queue_ratio=0))
        self.assertEqual((window['capable_cpus'],window['target_cpus']),(0,0))

    def test_cooperative_target_fits_fixed_work_before_dividing_automatic_jobs(self):
        state=self.center([16]);node=state.pool.nodes[0];node.reserved=16
        def job(cpus,automatic,held=None,requires=()):
            return dict(task=dict(steps=[['echo']],cpus=cpus,memory_mb=64,project='RGCN',requires=list(requires)),
                auto_cpus=automatic,node=held,cancel=threading.Event(),result=dict(status='running' if held else 'queued'))
        state.jobs={'active':job(16,True,node),'waiting':job(0,True),'freeze':job(1,False)}
        resources=state.project_resources()
        self.assertEqual(state._cooperative_cpu_target('active',16,resources),8)
        # An indivisible twelve-CPU task must receive its full width, rather
        # than an unusable eight-CPU fraction of a max-min division.
        state.jobs['freeze']['task']['cpus']=12
        self.assertEqual(state._cooperative_cpu_target('active',16,state.project_resources()),2)

    def test_cooperative_target_ignores_unready_or_incompatible_waiters(self):
        state=self.center([4]);node=state.pool.nodes[0];node.reserved=4
        active=dict(task=dict(steps=[['echo']],cpus=4,memory_mb=64,project='A'),auto_cpus=True,node=node,
            cancel=threading.Event(),result=dict(status='running'))
        state.jobs={'active':active}
        for key,changes in (('incompatible',{'requires':['missing']}),('too_large',{'memory_mb':99999}),
                            ('lower_priority',{'priority':-1}),('cancelled',{})):
            state.jobs[key]=dict(task=dict(active['task'],cpus=0,**changes),auto_cpus=True,node=None,
                cancel=threading.Event(),result=dict(status='queued'))
        state.jobs['cancelled']['cancel'].set()
        self.assertEqual(state._cooperative_cpu_target('active',4,state.project_resources()),4)

    def test_cooperative_target_places_flexible_waiter_on_another_node(self):
        state=self.center([4,4]);node=state.pool.nodes[0];node.reserved=4
        state.jobs={key:dict(task=dict(steps=[['echo']],cpus=4 if key=='active' else 0,memory_mb=64,project='A'),
            auto_cpus=True,node=node if key=='active' else None,cancel=threading.Event(),
            result=dict(status='running' if key=='active' else 'queued')) for key in ('active','waiting')}
        self.assertEqual(state._cooperative_cpu_target('active',4,state.project_resources()),4)

    def test_growth_headroom_respects_priority_memory_lifecycle_and_node(self):
        state=self.center([4,4]);node=state.pool.nodes[0];node.reserved=3
        def job(project,cpus,held=None,automatic=False):
            return dict(task=dict(steps=[['echo']],cpus=cpus,memory_mb=64,project=project),
                node=held,auto_cpus=automatic,cancel=threading.Event(),
                result=dict(status='running' if held else 'queued'))
        active=job('A',1,node,True);active['desired_cpus']=4
        state.jobs={'active':active,'borrower':job('B',2,node),'waiting':job('B',1)}
        candidate=state.jobs['waiting']['task']
        def headroom():return state._growth_headroom(candidate,state.project_resources())
        self.assertEqual(headroom(),{'0':1})
        # Higher priority work can use the free slot, without changing A's demand.
        candidate['priority']=1
        self.assertEqual(headroom(),{})
        candidate['priority']=0
        active['phase_memory']=dict(base_memory_mb=64,memory_per_cpu_mb=1024)
        node.health['memory_available_mb']=100
        self.assertEqual(headroom(),{})
        node.health['memory_available_mb']=8192
        self.assertEqual(headroom(),{'0':1})
        active['desired_cpus']=1
        self.assertEqual(headroom(),{})
        active['desired_cpus']=4;active['cancel'].set()
        self.assertEqual(headroom(),{})
        active['cancel'].clear();active['result']['status']='succeeded'
        self.assertEqual(headroom(),{})

    def test_growth_headroom_leaves_a_slot_for_same_project_ready_work(self):
        state=self.center([4]);node=state.pool.nodes[0];node.reserved=1
        active=dict(task=dict(steps=[['echo']],cpus=1,memory_mb=64,project='A'),
            auto_cpus=True,node=node,cancel=threading.Event(),result=dict(status='running'),desired_cpus=4)
        waiting=dict(task=dict(active['task'],cpus=0),auto_cpus=True,node=None,
            cancel=threading.Event(),result=dict(status='queued'))
        state.jobs={'active':active,'waiting':waiting}
        self.assertEqual(state._growth_headroom(waiting['task'],state.project_resources()),{'0':1})

    def test_queue_rechecks_used_threads_per_share_after_each_allocation(self):
        state=self.center([16]);state.queue=['A1','A2','B1','urgent']
        for key in state.queue:
            state.jobs[key]=dict(task=dict(steps=[['echo']],cpus=1,project=key[0],priority=3 if key=='urgent' else 0))
        budgets={'A':dict(used=0,resource_share=1.),'B':dict(used=2,resource_share=1.),'u':dict(used=8,resource_share=1.)}
        iterator=state._queued_candidates(budgets)
        self.assertEqual(next(iterator),('urgent',False))
        self.assertEqual(next(iterator),('A1',False))
        budgets['A']['used']=4
        self.assertEqual(next(iterator),('B1',False))
        self.assertEqual(next(iterator),('A2',False))


@contextmanager
def center(pool, root):
    state = Coordinator(pool, root)
    server = Server(('127.0.0.1', 0), state)
    thread = threading.Thread(target=server.serve_forever, kwargs={'poll_interval': .05}, daemon=True)
    thread.start()
    try:
        # The scheduler test harness represents the visible DCO operator
        # console; project-facing redaction is covered with a normal Client.
        yield state, Client(f'http://127.0.0.1:{server.server_port}', operator=True,
                            operator_token=state.operator_token)
    finally:
        state.close()
        server.shutdown()
        thread.join()
        server.server_close()


@contextmanager
def host_load(worker, percent, machine):
    # These CPU-placement fixtures must not inherit the live host's RAM pressure.
    sample = dict(worker.host.sample(), cpu_percent=percent, logical_cpus=8, machine_id=machine,
                  memory_total_mb=32768,host_memory_available_mb=24576)
    with patch.object(worker.host, 'sample', return_value=sample):
        yield


class CenterTests(unittest.TestCase):
    def test_disabled_idle_node_stops_probes_but_draining_and_maintenance_refresh_work(self):
        with scratch() as root, provider(Path(root)/'worker') as (_, worker):
            with patch.object(worker, 'health', wraps=worker.health) as probe:
                with Pool([dict(name='sleeping',client=worker,enabled=False)],refresh=.02) as pool:
                    node=pool.nodes[0]
                    time.sleep(.08)
                    self.assertEqual(probe.call_count,0)
                    pool.refresh(nodes=[node])
                    self.assertEqual(probe.call_count,1)
                    with pool.condition:
                        node.live_jobs.add('accepted-job')
                    until(lambda:probe.call_count>1)
                    with pool.condition:
                        node.live_jobs.clear()
                    pool.refresh_lock.acquire()
                    try:
                        pending=pool.probe_futures.get(node.name)
                        if pending is not None:pending.result()
                        before=probe.call_count
                    finally:
                        pool.refresh_lock.release()
                    time.sleep(.08)
                    self.assertEqual(probe.call_count,before)
                    pool.configure('sleeping',enabled=True)
                    until(lambda:probe.call_count>before)

    def test_single_node_isolation_drains_keeps_receipts_and_maintenance_path(self):
        hold="from pathlib import Path;import time\nwhile not Path('release').exists():time.sleep(.02)\nprint('original-result')"
        with scratch() as root,provider(Path(root)/'one',cpus=1) as (_,one), \
                provider(Path(root)/'two',cpus=1) as (_,two):
            with Pool([one,two]) as pool,center(pool,Path(root)/'center') as (_,client):
                name=pool.nodes[0].name
                original=client.submit(python(hold,node=name,project='original'))['id']
                until(lambda:client.status(original)['status']=='running')
                before=client.status(original)
                try:
                    client.control(node=name,routing=dict(enabled=False))
                    waiting=client.submit(python("print('after-resume')",node=name,project='waiting'))['id']
                    other=client.run(python("print('other-node')",project='other'))
                    self.assertEqual(other['status'],'succeeded')
                    self.assertNotEqual(other['assigned_node'],name)
                    self.assertEqual(client.status(waiting)['status'],'queued')
                    self.assertEqual(client.status(original)['started'],before['started'])
                    client.write_job_file(original,'release','done')
                    client.wait(original)
                    result=client.fetch(original)
                    self.assertEqual((result['id'],result['status'],result['stdout'].strip()),
                                     (original,'succeeded','original-result'))
                    self.assertFalse(result.get('attempts'))
                    self.assertGreater(result['cpu_seconds'],0)
                    # The existing direct worker connection remains usable
                    # after its production routing has drained.
                    maintenance=one.run(python("print('isolated-maintenance')"))
                    self.assertEqual(maintenance['stdout'].strip(),'isolated-maintenance')
                    self.assertEqual(client.status(waiting)['status'],'queued')
                finally:
                    client.control(node=name,routing=dict(enabled=True))
                client.wait(waiting)
                self.assertEqual(client.fetch(waiting)['assigned_node'],name)

    def test_global_pause_blocks_dispatch_but_allows_existing_phase_growth(self):
        with scratch() as root,provider(Path(root)/'worker',cpus=4) as (_,worker),Pool([worker]) as pool:
            with center(pool,Path(root)/'center') as (_,client):
                hold="from pathlib import Path;import time\nwhile not Path('release').exists():time.sleep(.02)"
                active=client.submit(python(hold,cpus=0,memory_mb=64,project='A'))['id']
                until(lambda:client.status(active)['status']=='running')
                before=client.status(active)
                self.assertEqual(client.resize(active,1)['cpus'],1)
                was_paused=client.health()['paused']
                try:
                    client.control(paused=True)
                    waiting=client.submit(python(hold,cpus=1,memory_mb=64,project='B'))['id']
                    reply=client.resize(active,2,base_memory_mb=64,memory_per_cpu_mb=32)
                    self.assertEqual((reply['cpus'],reply['memory_mb']),(2,128))
                    time.sleep(.15)
                    self.assertEqual(client.status(waiting)['status'],'queued')
                    after=client.status(active)
                    self.assertEqual(after['status'],'running')
                    self.assertEqual(after['started'],before['started'])
                    self.assertEqual(after['request_sha256'],before['request_sha256'])
                    self.assertEqual(pool.nodes[0].capacity,4)
                    self.assertFalse(pool.nodes[0].health.get('paused',False))
                finally:
                    client.control(paused=was_paused)
                until(lambda:client.status(waiting)['status']=='running')
                for identity in (active,waiting):
                    client.write_job_file(identity,'release','done');client.wait(identity);client.fetch(identity)

    def test_freed_cpu_waits_for_running_phase_growth_before_lending_again(self):
        with scratch() as root,provider(Path(root)/'worker',cpus=4) as (_,worker),Pool([worker]) as pool:
            with center(pool,Path(root)/'center') as (state,client):
                sleep="import time;time.sleep(30)"
                borrowers=[client.submit(python(sleep,cpus=1,memory_mb=64,project='Bench'))['id'] for _ in range(3)]
                until(lambda:all(client.status(i)['status']=='running' for i in borrowers))
                active=client.submit(python(sleep,cpus=0,memory_mb=64,project='ARH'))['id']
                until(lambda:client.status(active)['status']=='running')
                original=client.status(active)['request_sha256']
                self.assertEqual(client.resize(active,2)['cpus'],1)
                waiting=client.submit(python(sleep,cpus=1,memory_mb=64,project='Bench'))['id']
                client.cancel(borrowers[0]);client.wait(borrowers[0]);client.fetch(borrowers[0])
                time.sleep(.3)
                self.assertEqual(client.status(waiting)['status'],'queued')
                self.assertEqual(pool.nodes[0].reserved,3)
                self.assertEqual(client.resize(active,2)['cpus'],2)
                self.assertEqual(client.status(active)['request_sha256'],original)
                self.assertEqual(client.resize(active,1)['cpus'],1)
                until(lambda:client.status(waiting)['status']=='running')
                for identity in borrowers[1:]+[active,waiting]:
                    client.cancel(identity);client.wait(identity);client.fetch(identity)

    def test_slow_initial_probe_does_not_delay_local_control(self):
        with scratch() as root,provider(Path(root)/'local') as (_,local),provider(Path(root)/'slow') as (_,slow):
            release=threading.Event();timer=threading.Timer(3,release.set)
            original=slow.health
            def delayed(**options):
                release.wait(5)
                return original(**options)
            timer.start()
            try:
                with patch.object(slow,'health',side_effect=delayed):
                    started=time.monotonic()
                    with Pool([dict(name='slow',client=slow),dict(name='local',client=local)]) as pool:
                        try:
                            self.assertLess(time.monotonic()-started,2)
                            self.assertTrue(pool.nodes[0].error)
                            with center(pool,Path(root)/'center') as (_,client):
                                started=time.monotonic()
                                client.control(node='local',worker={'cpus':1})
                                self.assertLess(time.monotonic()-started,1)
                                self.assertEqual(client.run(python('print(42)'))['stdout'].strip(),'42')
                        finally:release.set()
            finally:
                release.set();timer.cancel()

    def test_missing_asset_rejects_before_acceptance_then_accepts_the_same_id(self):
        import hashlib
        from remote_compute.bundle import pack
        reference={'reference.txt':b'frozen comparison reference'}
        asset=hashlib.sha256(pack(reference)).hexdigest()
        identity='b'*32
        with scratch() as root,provider(Path(root)/'worker') as (_,worker):
            with Pool([worker]) as pool,center(pool,Path(root)/'center') as (state,client):
                task=python("from pathlib import Path;print(Path(r'${asset:reference}/reference.txt').read_text())",
                    assets={'reference':asset})
                with self.assertRaisesRegex(RemoteError,'Synchronize missing asset first: '+asset):
                    client.submit(task,job_id=identity)
                self.assertNotIn(identity,state.jobs)
                self.assertFalse((state.root/'jobs'/identity).exists())
                self.assertEqual(client.sync_files(reference),asset)
                self.assertEqual(client.submit(task,job_id=identity)['id'],identity)
                self.assertEqual(client.wait(identity)['status'],'succeeded')
                self.assertEqual(client.fetch(identity)['stdout'].strip(),'frozen comparison reference')

    def test_uncertain_submission_recovers_original_id_without_releasing_grant(self):
        for failure in ('accepted','not_received','query_disconnected','cancel_disconnected'):
            with self.subTest(failure=failure),scratch() as root,provider(Path(root)/'worker') as (_,remote):
                with Pool([remote],refresh=60) as pool,center(pool,Path(root)/'center') as (_,client):
                    original_submit,original_status=remote.submit_bundle,remote.status
                    calls=[];disconnected=threading.Event();restored=threading.Event()
                    def submit(identity,payload,**options):
                        calls.append(identity)
                        if len(calls)==1:
                            if failure!='not_received':original_submit(identity,payload,**options)
                            raise RemoteError('submission reply timed out')
                        return original_submit(identity,payload,**options)
                    def status(*args,**kwargs):
                        if failure.endswith('disconnected') and not restored.is_set():
                            disconnected.set()
                            error=RemoteError('worker port temporarily unavailable')
                            error.transport_code=10061
                            raise error
                        return original_status(*args,**kwargs)
                    hold="from pathlib import Path;import time\nwhile not Path('release').exists():time.sleep(.02)\nprint('executed once')"
                    with patch.object(remote,'submit_bundle',side_effect=submit),patch.object(remote,'status',side_effect=status):
                        identity=client.submit(python(hold))['id']
                        try:
                            if failure.endswith('disconnected'):
                                self.assertTrue(disconnected.wait(3))
                                self.assertEqual(client.status(identity)['status'],'dispatching')
                                self.assertEqual(pool.nodes[0].reserved,1)
                                if failure=='cancel_disconnected':client.cancel(identity)
                            restored.set()
                            if failure!='cancel_disconnected':
                                until(lambda:client.status(identity)['status']=='running')
                                remote.write_job_file(identity,'release','done')
                            result=client.wait(identity,wait_timeout=5)
                            self.assertEqual(result['status'],'cancelled' if failure=='cancel_disconnected' else 'succeeded',result)
                            self.assertEqual(result['attempts'],[])
                            self.assertEqual(set(calls),{identity})
                            self.assertEqual(len(calls),2 if failure=='not_received' else 1)
                            self.assertNotIn('connection_error',result)
                            client.fetch(identity)
                            until(lambda:pool.nodes[0].reserved==0)
                        finally:
                            restored.set()

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

    def test_late_lease_answer_preserves_the_original_failed_outcome(self):
        code=("import os,sys\nsys.path.insert(0,os.environ['REMOTE_COMPUTE_HOME'])\n"
              "from remote_compute import Client\n"
              "Client(os.environ['REMOTE_COMPUTE_URL'],os.environ['REMOTE_COMPUTE_TOKEN'],timeout=.1,lease_timeout=.1).resize("
              "os.environ['REMOTE_COMPUTE_JOB_ID'],1,memory_mb=128)\n")
        with scratch() as root,provider(Path(root)/'worker',memory_mb=1024) as (worker,remote), \
                Pool([remote]) as pool,center(pool,Path(root)/'center') as (state,client):
            original=state.resize
            def delayed(identity,*args,**kwargs):
                until(lambda:worker.get(identity)['status']=='failed')
                return original(identity,*args,**kwargs)
            with patch.object(state,'resize',side_effect=delayed):
                identity=client.submit(python(code,memory_mb=400))['id']
                outcome=client.wait(identity,wait_timeout=5)
            self.assertEqual(outcome['status'],'failed',outcome)
            self.assertEqual(outcome['attempts'],[])
            self.assertEqual(remote.status(identity)['status'],'failed')
            self.assertEqual(remote.request('/jobs/'+identity+'/lease',
                dict(request_id=1,grant=dict(cpus=99,memory_mb=999))),dict(accepted=True))
            self.assertEqual(remote.status(identity)['cpus'],1)
            result=client.fetch(identity)
            self.assertEqual(result['steps'][0]['returncode'],1)
            self.assertIn('timed out',result['stderr'])
            until(lambda:pool.nodes[0].reserved==0 and worker.used==0)

    def test_local_lease_can_release_grant_during_host_health_failure(self):
        code=("import os,sys,time\nfrom pathlib import Path\n"
              "sys.path.insert(0,os.environ['REMOTE_COMPUTE_HOME'])\nfrom remote_compute import Client\n"
              "while not Path('release-phase').exists():time.sleep(.01)\n"
              "grant=Client(os.environ['REMOTE_COMPUTE_URL'],os.environ['REMOTE_COMPUTE_TOKEN'],lease_timeout=3).resize("
              "os.environ['REMOTE_COMPUTE_JOB_ID'],1,memory_mb=128)\n"
              "print(str(grant['cpus'])+':'+str(grant['memory_mb']))\n")
        with scratch() as root,provider(Path(root)/'worker',cpus=2,memory_mb=1024,memory_reserve_mb=0) as (worker,remote), \
                Pool([remote]) as pool,center(pool,Path(root)/'center') as (_,client):
            identity=client.submit(python(code,cpus=2,memory_mb=512))['id']
            until(lambda:client.status(identity)['status']=='running')
            with patch.object(pool,'_probe',return_value=None):
                pool.refresh()
                with pool.condition:pool.nodes[0].error='temporary health timeout'
                client.write_job_file(identity,'release-phase','ready')
                outcome=client.wait(identity,wait_timeout=5)
            result=client.fetch(identity)
            self.assertEqual(outcome['status'],'succeeded',result.get('stderr'))
            self.assertEqual((result['id'],result['cpus'],result['memory_mb']),(identity,1,128))
            self.assertEqual(result['stdout'].strip(),'1:128')
            self.assertFalse(result['attempts'])
            until(lambda:pool.nodes[0].reserved==0 and worker.used==0)
            self.assertEqual(pool.nodes[0].reserved_memory_mb,0)

    def test_host_health_failure_rejects_memory_growth_then_recovers_original_job(self):
        with scratch() as root,provider(Path(root)/'worker',cpus=2,memory_mb=1024,memory_reserve_mb=0) as (worker,remote), \
                Pool([remote]) as pool,center(pool,Path(root)/'center') as (_,client):
            identity=client.submit(python("from pathlib import Path;import time\nwhile not Path('done').exists():time.sleep(.01)",memory_mb=512))['id']
            until(lambda:client.status(identity)['status']=='running')
            with patch.object(pool,'_probe',return_value=None):
                pool.refresh()
                with pool.condition:pool.nodes[0].error='temporary health timeout'
                with self.assertRaises(RemoteError) as denied:client.resize(identity,1,memory_mb=768)
                self.assertEqual(denied.exception.status_code,409)
                self.assertIn('Insufficient memory',str(denied.exception))
                self.assertEqual((remote.status(identity)['cpus'],remote.status(identity)['memory_mb']),(1,512))
                self.assertEqual(pool.nodes[0].reserved_memory_mb,512)
            pool.refresh()
            grant=client.resize(identity,1,memory_mb=768)
            self.assertEqual((grant['cpus'],grant['memory_mb']),(1,768))
            client.write_job_file(identity,'done','ready')
            self.assertEqual(client.wait(identity,wait_timeout=5)['status'],'succeeded')
            self.assertEqual(client.fetch(identity)['id'],identity)
            until(lambda:pool.nodes[0].reserved==0 and worker.used==0)
            self.assertEqual(pool.nodes[0].reserved_memory_mb,0)

    def test_old_center_memory_return_error_uses_worker_confirmed_grant_once(self):
        code=("import os,sys\nsys.path.insert(0,os.environ['REMOTE_COMPUTE_HOME'])\n"
              "from remote_compute import Client\n"
              "grant=Client(os.environ['REMOTE_COMPUTE_URL'],os.environ['REMOTE_COMPUTE_TOKEN'],lease_timeout=3).resize("
              "os.environ['REMOTE_COMPUTE_JOB_ID'],1,memory_mb=128)\n"
              "assert grant['existing_grant_confirmed'] and not grant['resize_applied']\n"
              "print(str(grant['cpus'])+':'+str(grant['memory_mb']))\n")
        with scratch() as root,provider(Path(root)/'worker',memory_mb=1024,memory_reserve_mb=0) as (worker,remote), \
                Pool([remote]) as pool,center(pool,Path(root)/'center') as (state,client):
            with patch.object(state,'resize',side_effect=ValueError('min() iterable argument is empty')) as resize:
                identity=client.submit(python(code,memory_mb=512))['id']
                outcome=client.wait(identity,wait_timeout=5)
                result=client.fetch(identity)
            self.assertEqual(outcome['status'],'succeeded',result.get('stderr'))
            self.assertEqual(result['stdout'].strip(),'1:512')
            self.assertEqual(resize.call_count,1)
            self.assertEqual((result['id'],result['memory_mb'],result['attempts']),(identity,512,[]))
            until(lambda:pool.nodes[0].reserved==0 and worker.used==0)
            self.assertEqual(pool.nodes[0].reserved_memory_mb,0)

    def test_overlapping_node_sets_have_feasible_project_targets_and_admission(self):
        with scratch() as root,provider(Path(root)/'pc',cpus=2,tags=['physical_profile_a']) as (_,pc), \
                provider(Path(root)/'tx',cpus=6,tags=['physical_profile_a']) as (_,tx), \
                provider(Path(root)/'server',cpus=12,tags=['physical_profile_b']) as (_,server):
            with Pool([pc,tx,server]) as pool,center(pool,Path(root)/'center') as (_,client):
                client.control(paused=True)
                tasks=[python('pass',project='ARH',node='node1'),
                       python('pass',project='ARH',cpus=0,node='node2'),
                       python('pass',project='Bench',cpus=0,max_cpus=2,node='node3')]
                tasks += [python('pass',project='Bench',requires=['physical_profile_a']) for _ in range(10)]
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
                # Arrival alone keeps the running pool intact. Its next
                # drained finite request participates in project fairness.
                self.assertEqual(client.resize(first,4,memory_mb=64)['cpus'],2)
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
                self.assertNotIn('phase_memory',state.jobs[identity])
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

    def test_cooperative_boundary_admits_same_project_auto_and_fixed_jobs(self):
        with scratch() as root,provider(Path(root)/'worker',cpus=4,cpu_limit=4) as (_,worker):
            with Pool([worker]) as pool,center(pool,Path(root)/'center') as (_,client):
                first=client.submit(python('import time;time.sleep(30)',cpus=0,project='RGCN'))['id']
                until(lambda:client.status(first)['status']=='running')
                before=client.status(first)['request_sha256']
                second=client.submit(python('import time;time.sleep(30)',cpus=0,project='RGCN'))['id']
                fixed=client.submit(python('import time;time.sleep(30)',cpus=1,project='RGCN'))['id']
                grant=client.resize(first,0,base_memory_mb=64,memory_per_cpu_mb=32)
                self.assertLess(grant['cpus'],4)
                until(lambda:client.status(second)['status']=='running' and client.status(fixed)['status']=='running')
                self.assertEqual(client.status(first)['request_sha256'],before)
                self.assertEqual(pool.nodes[0].reserved,4)
                for identity in (fixed,second):
                    client.cancel(identity);client.wait(identity);client.fetch(identity)
                self.assertEqual(client.resize(first,0)['cpus'],4)
                client.cancel(first);client.wait(first);client.fetch(first)
                until(lambda:pool.nodes[0].reserved==0)
                self.assertEqual(pool.nodes[0].reserved_memory_mb,0)

    def test_finite_phase_demand_from_auto_job_yields_to_ready_fixed_work(self):
        with scratch() as root,provider(Path(root)/'worker',cpus=4,cpu_limit=4) as (_,worker):
            with Pool([worker]) as pool,center(pool,Path(root)/'center') as (_,client):
                active=client.submit(python('import time;time.sleep(30)',cpus=0,project='RGCN'))['id']
                until(lambda:client.status(active)['status']=='running')
                self.assertEqual(client.status(active)['cpus'],4)
                before=client.status(active)['request_sha256']
                waiting=client.submit(python('import time;time.sleep(30)',cpus=3,project='RGCN'))['id']
                # RGCN reports the useful width at a drained native boundary,
                # including when it equals the previous phase's grant.
                grant=client.resize(active,4,base_memory_mb=64,memory_per_cpu_mb=32)
                self.assertEqual((grant['cpus'],grant['requested_cpus']),(1,4))
                until(lambda:client.status(waiting)['status']=='running')
                self.assertEqual(pool.nodes[0].reserved,4)
                client.cancel(waiting);client.wait(waiting);client.fetch(waiting)
                # A finite useful width remains an upper bound after the peer
                # finishes; it does not request the whole node like cpus=0.
                self.assertEqual(client.resize(active,2)['cpus'],2)
                self.assertEqual(client.status(active)['request_sha256'],before)
                client.cancel(active);client.wait(active);client.fetch(active)
                until(lambda:pool.nodes[0].reserved==0)
                self.assertEqual(pool.nodes[0].reserved_memory_mb,0)

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
                self.assertNotIn('phase_memory',state.jobs[identity])
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
                        with patch.object(worker,'artifacts',side_effect=RemoteError('still unavailable')):
                            # Check the server's typed upstream failure directly;
                            # fetch intentionally waits through transient 502s.
                            with self.assertRaises(RemoteError) as raised:client.request('/jobs/'+identity+'/artifacts',binary=True)
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

    def test_overlapping_auto_projects_start_the_restricted_job_before_flexible_work(self):
        hold="from pathlib import Path;import time\nwhile not Path('release').exists():time.sleep(.02)"
        with scratch() as root,provider(Path(root)/'one',cpus=2,tags=['special']) as (_,one), \
                provider(Path(root)/'two',cpus=2) as (_,two):
            with Pool([one,two]) as pool,center(pool,Path(root)/'center') as (_,client):
                client.control(paused=True)
                flexible=client.submit(python(hold,project='A',cpus=0))['id']
                restricted=client.submit(python(hold,project='B',cpus=0,requires=['special']))['id']
                client.control(paused=False)
                until(lambda:all(client.status(identity)['status']=='running' for identity in (flexible,restricted)))
                self.assertEqual(client.status(restricted)['assigned_node'],'node1')
                self.assertEqual(client.status(flexible)['assigned_node'],'node2')
                self.assertEqual(client.health()['used_cpus'],4)
                for identity in (flexible,restricted):
                    client.write_job_file(identity,'release','done');client.wait(identity);client.fetch(identity)

    def test_paused_workers_keep_running_grants_in_project_fairness(self):
        hold="from pathlib import Path;import time\nwhile not Path('release').exists():time.sleep(.02)"
        with scratch() as root,provider(Path(root)/'one',cpus=12) as (_,one), \
                provider(Path(root)/'two',cpus=12) as (_,two):
            with Pool([one,two]) as pool,center(pool,Path(root)/'center') as (_,client):
                existing=client.submit(python(hold,project='A',cpus=0,node='node1'))['id']
                until(lambda:client.status(existing)['status']=='running')
                client.control(node='node1',worker=dict(paused=True))
                client.control(paused=True)
                queued=client.submit(python(hold,project='A',cpus=0))['id']
                other=client.submit(python(hold,project='B',cpus=0))['id']
                self.assertEqual(client.admission('A',cpus=0,queue_ratio=0)['target_cpus'],12)
                self.assertEqual(client.admission('B',cpus=0,queue_ratio=0)['target_cpus'],12)
                client.control(paused=False)
                until(lambda:client.status(other)['status']=='running')
                self.assertEqual(client.status(other)['cpus'],12)
                self.assertEqual(client.status(queued)['status'],'queued')
                resources=client.health()['projects']
                self.assertEqual({name:(row['target'],row['used']) for name,row in resources.items()},
                                 {'A':(12,12),'B':(12,12)})
                for identity in (existing,other):
                    client.write_job_file(identity,'release','done');client.wait(identity);client.fetch(identity)
                until(lambda:client.status(queued)['status']=='running')
                self.assertEqual(client.status(queued)['assigned_node'],'node2')
                client.write_job_file(queued,'release','done');client.wait(queued);client.fetch(queued)

    def test_capped_auto_window_starts_every_device_without_prefetch(self):
        hold="from pathlib import Path;import time\nwhile not Path('release').exists():time.sleep(.02)"
        with scratch() as root,provider(Path(root)/'one',cpus=2) as (_,one), \
                provider(Path(root)/'two',cpus=2) as (_,two),provider(Path(root)/'three',cpus=2) as (_,three):
            with Pool([one,two,three]) as pool,center(pool,Path(root)/'center') as (_,client):
                window=client.window('automatic',cpus=0,max_cpus=8,queue_ratio=0)
                identities=[client.submit(python(hold,cpus=0,max_cpus=8,project='automatic'))['id']
                            for _ in range(window.current())]
                until(lambda:client.health()['used_cpus']==6)
                self.assertEqual(len({client.status(identity)['assigned_node'] for identity in identities}),3)
                for identity in identities:
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
                sampled={}
                def delayed_health(**options):
                    result=original(**options)
                    sampled.update(next(j for j in result['active_jobs'] if j['id']==external))
                    entered.set();release.wait(5);return result
                with patch.object(pool.nodes[0].client,'health',side_effect=delayed_health):
                    probe=threading.Thread(target=pool.refresh);probe.start()
                    try:
                        self.assertTrue(entered.wait(3))
                        self.assertEqual(client.resize(identity,1)['cpus'],1)
                        client.cancel(identity);client.wait(identity);client.fetch(identity)
                        until(lambda:pool.nodes[0].reserved==0)
                    finally:release.set();probe.join(5)
                self.assertEqual(pool.nodes[0].external,1)
                self.assertAlmostEqual(pool.nodes[0].external_memory_mb,
                                       sampled['rss_mb'] or sampled['memory_mb'],delta=.03)
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
                connection.request('GET','/health',headers={'X-DCO-Keepalive':'1'});connection.getresponse().read()
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

    def test_asset_revision_preserves_the_inflight_hash_lock_and_releases_it_afterward(self):
        from concurrent.futures import ThreadPoolExecutor
        with scratch() as root,provider(Path(root)/'worker') as (_,worker):
            with Pool([worker]) as pool,center(pool,Path(root)/'center') as (state,client):
                asset=client.sync_files({'model.npz':bytes(range(256))*8192})
                node=pool.nodes[0]
                original=node.client.request
                entered,release=threading.Event(),threading.Event()
                uploads=[]
                def delayed(path,data=None,**options):
                    if path=='/assets/'+asset and data is not None:
                        uploads.append(len(data))
                        entered.set();release.wait(5)
                    return original(path,data,**options)
                with patch.object(node.client,'request',side_effect=delayed),ThreadPoolExecutor(max_workers=2) as transfers:
                    first=transfers.submit(state._ensure_assets,node,[asset])
                    try:
                        self.assertTrue(entered.wait(2))
                        with node.asset_lock:
                            transfer=node._asset_upload_locks[asset]
                            revision=node.health.get('asset_revision',0)+1
                            node.health['asset_revision']=revision
                        second=transfers.submit(state._ensure_assets,node,[asset])
                        until(lambda:node.asset_session[-1]==revision)
                        with node.asset_lock:
                            self.assertIs(node._asset_upload_locks[asset],transfer)
                    finally:
                        release.set()
                    first.result(timeout=3);second.result(timeout=3)
                self.assertEqual(uploads,[(state.root/'assets'/(asset+'.zip')).stat().st_size])
                self.assertIn(asset,node.known_assets)
                del transfer
                self.assertFalse(node._asset_upload_locks)

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

    def test_asset_cancellation_preserves_active_shared_consumers_and_releases_cancelled_waiters(self):
        for cancelled in ('owner','waiter','both'):
            with self.subTest(cancelled=cancelled),scratch() as root,provider(Path(root)/'worker',cpus=2) as (worker,remote):
                with host_load(worker,0,'asset-cancellation'),Pool([remote]) as pool,center(pool,Path(root)/'center') as (state,client):
                    asset=client.sync_files({'input':b'frozen shared input'})
                    original=remote.request
                    entered,release=threading.Event(),threading.Event();uploads=[]
                    def delayed(path,data=None,**options):
                        if path=='/assets/'+asset and data is not None:
                            uploads.append(asset);entered.set()
                            while not release.wait(.01):
                                if options.get('cancel') is not None and options['cancel']():
                                    raise RequestCancelled('abandoned asset')
                        return original(path,data,**options)
                    with patch.object(remote,'request',side_effect=delayed):
                        first=client.submit(python('print(42)',assets={'input':asset}))['id']
                        try:
                            self.assertTrue(entered.wait(2))
                            second=client.submit(python('print(42)',assets={'input':asset}))['id']
                            until(lambda:client.status(second)['status']=='dispatching')
                            if cancelled in ('owner','both'):client.cancel(first)
                            if cancelled in ('waiter','both'):client.cancel(second)
                            if cancelled in ('waiter','both'):
                                self.assertEqual(client.wait(second,wait_timeout=2)['status'],'cancelled')
                                self.assertNotIn(second,worker.jobs)
                            if cancelled=='both':
                                self.assertEqual(client.wait(first,wait_timeout=2)['status'],'cancelled')
                                self.assertNotIn(first,worker.jobs)
                            release.set()
                            for identity,is_cancelled in ((first,cancelled in ('owner','both')),
                                                         (second,cancelled in ('waiter','both'))):
                                result=client.wait(identity,wait_timeout=3)
                                self.assertEqual(result['status'],'cancelled' if is_cancelled else 'succeeded')
                                if is_cancelled:self.assertIsNone(result['started'])
                                client.fetch(identity)
                            self.assertEqual(uploads,[asset])
                            until(lambda:pool.nodes[0].reserved==0)
                        finally:
                            release.set()

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
            # Exercise admission before the partial-grant grace expires,
            # independently of the host's HTTP and process-startup latency.
            now=time.time();clock=SimpleNamespace(time=lambda:now,sleep=time.sleep)
            with patch('remote_compute.coordinator.time',clock), \
                    Pool([node]) as pool, center(pool, Path(root)/'center') as (_, client):
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
                self.assertGreaterEqual(result['peak_rss_mb'], 0)
                self.assertGreaterEqual(result['cpu_seconds'], 0)
                self.assertEqual(set(result['tools']['python']), {'argv', 'available'})
                self.assertNotIn('sha256', result['tools']['python'])
                self.assertEqual(client.status(), [])
                self.assertEqual(node.status(), [])
                self.assertEqual(list((state.root/'jobs').iterdir()), [])

    def test_memory_blocks_until_inflight_task_finishes(self):
        with scratch() as root, provider(Path(root)/'worker', cpus=2, memory_mb=128) as (_, node):
            with Pool([node]) as pool, center(pool, Path(root)/'center') as (_, client):
                task = python("from pathlib import Path; import time\ndata=bytearray(64*1024*1024)\nfor i in range(0,len(data),4096): data[i]=1\nwhile not Path('release').exists(): time.sleep(.02)", memory_mb=80)
                first = client.submit(task)['id']
                until(lambda: client.status(first)['status'] == 'running')
                until(lambda:node.health()['rss_mb']>50)
                second = client.submit(python("print('admitted')",memory_mb=80))['id']
                self.assertIn(client.status(second)['status'],('queued','dispatching'))
                client.write_job_file(first,'release','go')
                one, two = client.wait(first), client.wait(second)
                self.assertGreaterEqual(two['started'], one['finished'])
                self.assertGreater(one['peak_rss_mb'], 50)
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
