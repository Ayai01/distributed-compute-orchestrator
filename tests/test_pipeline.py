"""Durable original-ID collection is independent from preparation/publication."""
import json,subprocess,sys,threading,time,unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch
from remote_compute import Client,Pool,command,Deferred,Skipped,ExecutionPaused
from remote_compute.pipeline import ExecutionFailure
from test_compute import scratch,provider
from test_center import center

def task(number):
    return dict(task=command(['${tool:python}','-c','print('+str(number)+')'],max_cpus=1))


class ReadyQueueTests(unittest.TestCase):
    def test_retries_keep_catalog_order_and_allow_fresh_inputs_to_advance(self):
        from remote_compute.pipeline import _ReadyQueue
        retries=['retry'+str(i)for i in range(8)];fresh=['normal0','normal1']
        ready=_ReadyQueue(retries+fresh)
        for key in reversed(retries):ready.add(key,retry=True)
        for key in reversed(fresh):ready.add(key)
        ready.add(retries[0],retry=True)
        order=[]
        while ready:order.append(ready.pop())
        self.assertEqual(len(order),10);self.assertEqual(len(set(order)),10)
        self.assertEqual([key for key in order if key in retries],retries)
        self.assertEqual([key for key in order if key in fresh],fresh)
        self.assertLessEqual(order.index('normal0'),3)
        self.assertLessEqual(order.index('normal1'),7)


class PipelineTests(unittest.TestCase):
    def test_collection_timings_separate_local_queue_fetch_publish_and_retirement(self):
        with scratch()as root,provider(Path(root)/'worker',cpus=4)as(_,worker):
            with Pool([worker])as pool,center(pool,Path(root)/'center')as(_,client):
                ids=[client.submit(task(i)['task'])['id']for i in range(6)]
                for identity in ids:client.wait(identity)
                fetch,delete=client.fetch,client.delete
                guard=threading.Lock();active=peak=0
                def slow_fetch(*args,**kwargs):
                    nonlocal active,peak
                    with guard:active+=1;peak=max(peak,active)
                    try:time.sleep(.12);return fetch(*args,**kwargs)
                    finally:
                        with guard:active-=1
                def slow_delete(*args,**kwargs):time.sleep(.03);return delete(*args,**kwargs)
                with patch.object(client,'fetch',side_effect=slow_fetch),patch.object(client,'delete',side_effect=slow_delete):
                    result=client.execute({str(i):dict(job_id=identity)for i,identity in enumerate(ids)},project='collection-timing',output_dir=Path(root)/'delivery',publish=lambda r:time.sleep(.02))
                self.assertEqual(len(result),6)
                self.assertEqual(peak,2)
                states=[json.loads(p.read_bytes())for p in (Path(root)/'delivery/jobs').glob('*/state.json')]
                self.assertTrue(all(s['state']=='collected'for s in states))
                timings=[s['collection_timing']for s in states]
                self.assertTrue(any(t['queue_seconds']>=.1 for t in timings))
                for timing in timings:
                    self.assertGreaterEqual(timing['fetch_seconds'],.12)
                    self.assertGreaterEqual(timing['publication_seconds'],.02)
                    self.assertGreaterEqual(timing['retirement_seconds'],.03)
                    self.assertGreater(timing['transport']['archive_bytes'],0)
                self.assertTrue(all(s['status']=='unknown'for s in client.statuses(ids).values()))

    def test_cancellation_fence_after_submitting_save_survives_recovery_without_reposting(self):
        from remote_compute.bundle import atomic_json
        with scratch()as root,provider(Path(root)/'worker',cpus=1)as(_,worker):
            with Pool([worker])as pool,center(pool,Path(root)/'center')as(_,client):
                cancel=threading.Event();output=Path(root)/'delivery';plan={'one':task(1)}
                def save(path,value,*args,**options):
                    result=atomic_json(path,value,*args,**options)
                    if value.get('state')=='submitting':cancel.set()
                    return result
                with patch('remote_compute.pipeline.atomic_json',side_effect=save),patch.object(client,'submit_bundle')as post:
                    with self.assertRaises(ExecutionPaused):
                        client.execute(plan,project='parallel-test',output_dir=output,cancel_when=cancel.is_set,ready_tasks=1)
                    post.assert_not_called()
                row=json.loads(next((output/'jobs').glob('*/state.json')).read_bytes())
                self.assertTrue(row['cancel_requested']);self.assertEqual(row['state'],'prepared')
                with patch.object(client,'submit_bundle')as post:
                    with self.assertRaises(ExecutionPaused):
                        client.execute(plan,project='parallel-test',output_dir=output,ready_tasks=1)
                    post.assert_not_called()
                resumed=json.loads(next((output/'jobs').glob('*/state.json')).read_bytes())
                self.assertEqual(resumed['id'],row['id'])
                result=client.execute({'peer':task(2)},project='parallel-test',output_dir=Path(root)/'peer',ready_tasks=1)
                self.assertEqual(result[0]['stdout'].strip(),'2')

    def test_cancellation_intent_is_durable_while_submit_reply_is_outstanding(self):
        from remote_compute.client import RemoteError
        with scratch()as root,provider(Path(root)/'worker',cpus=1)as(_,worker):
            with Pool([worker])as pool,center(pool,Path(root)/'center')as(_,client):
                cancel=threading.Event();entered=threading.Event();release=threading.Event()
                output=Path(root)/'delivery';plan={'one':task(1)}
                def lost_reply(identity,data):
                    entered.set()
                    if not release.wait(5):raise RuntimeError('Submit reply not released')
                    error=RemoteError('Submit response lost');error.status_code=502;raise error
                with patch.object(client,'submit_bundle',side_effect=lost_reply),ThreadPoolExecutor(1)as callers:
                    future=callers.submit(client.execute,plan,project='parallel-test',output_dir=output,
                        cancel_when=cancel.is_set,ready_tasks=1)
                    try:
                        self.assertTrue(entered.wait(3));cancel.set();deadline=time.monotonic()+3
                        path=next((output/'jobs').glob('*/state.json'))
                        while not json.loads(path.read_bytes()).get('cancel_requested'):
                            if time.monotonic()>deadline:self.fail('Cancellation intent was not durable during POST')
                            time.sleep(.025)
                    finally:release.set()
                    with self.assertRaises(ExecutionFailure):future.result(timeout=6)
                row=json.loads(path.read_bytes());self.assertEqual(row['state'],'prepared')
                self.assertTrue(row['cancel_requested'])
                with patch.object(client,'submit_bundle')as post:
                    with self.assertRaises(ExecutionPaused):
                        client.execute(plan,project='parallel-test',output_dir=output,ready_tasks=1)
                    post.assert_not_called()

    def _wait_for_partial(self,client,identities):
        from remote_compute.client import RemoteError
        deadline=time.monotonic()+4
        while time.monotonic()<deadline:
            if identities:
                try:
                    if client.read_job_file(identities[0],'partial.txt')==b'kept':return
                except RemoteError:pass
            time.sleep(.025)
        self.fail('Original job did not publish its partial file')

    def test_cancel_when_cancels_only_this_call_and_publishes_partial_output(self):
        with scratch()as root,provider(Path(root)/'worker',cpus=2)as(_,worker):
            with Pool([worker])as pool,center(pool,Path(root)/'center')as(_,client):
                cancel=threading.Event();identities=[];published=[]
                code="from pathlib import Path; import time; Path('partial.txt').write_text('kept'); time.sleep(15)"
                def prepare(spec,identity):identities.append(identity);return spec
                def publish(receipt):
                    published.append(receipt['id'])
                    self.assertEqual((Path(receipt['output_dir'])/'files/partial.txt').read_text(),'kept')
                plan={'running':dict(task=command(['${tool:python}','-c',code],max_cpus=1,outputs=['partial.txt'])),
                      'later':dict(spec=task(2),after=['running'])}
                with ThreadPoolExecutor(2)as callers:
                    first=callers.submit(client.execute,plan,project='parallel-test',output_dir=Path(root)/'cancel',
                        prepare=prepare,publish=publish,cancel_when=cancel.is_set,ready_tasks=2)
                    peer=callers.submit(client.execute,{'peer':task(7)},project='parallel-test',
                        output_dir=Path(root)/'peer',ready_tasks=2)
                    try:self._wait_for_partial(client,identities)
                    finally:cancel.set()
                    with self.assertRaises(ExecutionPaused)as paused:first.result(timeout=6)
                    self.assertEqual(paused.exception.remaining,['later'])
                    self.assertEqual(paused.exception.results[0]['status'],'cancelled')
                    self.assertEqual(published,identities)
                    self.assertEqual(peer.result(timeout=6)[0]['status'],'succeeded')
                self.assertEqual(client.statuses(identities)[identities[0]]['status'],'unknown')

    def test_cancel_after_lost_submit_reply_uses_the_original_id(self):
        from remote_compute.client import RemoteError
        with scratch()as root,provider(Path(root)/'worker',cpus=1)as(_,worker):
            with Pool([worker])as pool,center(pool,Path(root)/'center')as(_,client):
                cancel=threading.Event();accepted=[];published=[];submit=client.submit_bundle
                def lost_reply(identity,data):
                    submit(identity,data);accepted.append(identity);cancel.set()
                    error=RemoteError('Lost accepted reply');error.status_code=502;raise error
                with patch.object(client,'submit_bundle',side_effect=lost_reply),patch.object(client,'cancel',wraps=client.cancel)as stop:
                    results=client.execute({'one':dict(task=command(['${tool:python}','-c','import time; time.sleep(15)'],max_cpus=1))},
                        project='parallel-test',output_dir=Path(root)/'delivery',cancel_when=cancel.is_set,
                        publish=lambda row:published.append(row['id']))
                self.assertEqual([row['id']for row in results],accepted)
                self.assertEqual(published,accepted);stop.assert_called_once_with(accepted[0])
                self.assertEqual(results[0]['status'],'cancelled')

    def test_cancelled_publication_failure_resumes_without_resubmission(self):
        with scratch()as root,provider(Path(root)/'worker',cpus=1)as(_,worker):
            with Pool([worker])as pool,center(pool,Path(root)/'center')as(_,client):
                cancel=threading.Event();identities=[];output=Path(root)/'delivery'
                code="from pathlib import Path; import time; Path('partial.txt').write_text('kept'); time.sleep(15)"
                plan={'one':dict(task=command(['${tool:python}','-c',code],max_cpus=1,outputs=['partial.txt']))}
                def prepare(spec,identity):identities.append(identity);return spec
                def publish(receipt):raise RuntimeError('Publication needs recovery')
                with ThreadPoolExecutor(1)as callers:
                    future=callers.submit(client.execute,plan,project='parallel-test',output_dir=output,
                        prepare=prepare,publish=publish,cancel_when=cancel.is_set,ready_tasks=1)
                    try:self._wait_for_partial(client,identities)
                    finally:cancel.set()
                    with self.assertRaises(ExecutionFailure):future.result(timeout=6)
                with patch.object(client,'submit_bundle',side_effect=AssertionError('Original must not be resubmitted')):
                    results=client.execute(plan,project='parallel-test',output_dir=output,ready_tasks=1)
                self.assertEqual([(r['id'],r['status'])for r in results],[(identities[0],'cancelled')])
                self.assertEqual((Path(results[0]['output_dir'])/'files/partial.txt').read_text(),'kept')
                self.assertEqual(client.statuses(identities)[identities[0]]['status'],'unknown')

    def test_cancel_after_unregistered_submit_future_drains_the_original_id(self):
        from remote_compute.pipeline_control import _CallPool
        with scratch()as root,provider(Path(root)/'worker',cpus=1)as(_,worker):
            with Pool([worker])as pool,center(pool,Path(root)/'center')as(_,client):
                cancel=threading.Event();identities=[];original=_CallPool.submit
                def submit(executor,function,*args,**options):
                    future=original(executor,function,*args,**options)
                    if function.__name__=='submit'and not identities:
                        future.result(timeout=4);identities.append(args[0]['id']);cancel.set();raise KeyboardInterrupt
                    return future
                with patch.object(_CallPool,'submit',submit):
                    result=client.execute({'one':dict(task=command(['${tool:python}','-c','import time; time.sleep(15)'],max_cpus=1))},
                        project='parallel-test',output_dir=Path(root)/'delivery',cancel_when=cancel.is_set)
                self.assertEqual([(r['id'],r['status'])for r in result],[(identities[0],'cancelled')])
                self.assertEqual(client.statuses(identities)[identities[0]]['status'],'unknown')

    def test_cancel_rpc_failure_still_publishes_the_unregistered_accepted_receipt(self):
        from remote_compute.client import RemoteError
        from remote_compute.pipeline_control import _CallPool
        with scratch()as root,provider(Path(root)/'worker',cpus=1)as(_,worker):
            with Pool([worker])as pool,center(pool,Path(root)/'center')as(_,client):
                cancel=threading.Event();identities=[];published=[];original=_CallPool.submit
                def submit(executor,function,*args,**options):
                    future=original(executor,function,*args,**options)
                    if function.__name__=='submit'and not identities:
                        future.result(timeout=4);identities.append(args[0]['id']);cancel.set();raise KeyboardInterrupt
                    return future
                error=RemoteError('Cancel request rejected');error.status_code=403
                with patch.object(_CallPool,'submit',submit),patch.object(client,'cancel',side_effect=error):
                    with self.assertRaises(ExecutionFailure)as failed:
                        client.execute({'one':dict(task=command(['${tool:python}','-c','import time; time.sleep(.5); print(8)'],max_cpus=1))},project='parallel-test',output_dir=Path(root)/'delivery',
                            cancel_when=cancel.is_set,publish=lambda row:published.append(row['id']))
                self.assertEqual(published,identities)
                self.assertEqual([row['id']for row in failed.exception.results],identities)
                self.assertEqual(failed.exception.failures[0]['stage'],'cancel')
                self.assertEqual(client.statuses(identities)[identities[0]]['status'],'unknown')

    def test_rejected_unaccepted_original_releases_credit_before_another_directory_runs(self):
        from remote_compute.client import RemoteError
        with scratch()as root,provider(Path(root)/'worker',cpus=1)as(_,worker):
            with Pool([worker])as pool,center(pool,Path(root)/'center')as(_,client):
                rejected=RemoteError('Invalid submitted request');rejected.status_code=400
                first=Path(root)/'rejected'
                with patch.object(client,'submit_bundle',side_effect=rejected):
                    with self.assertRaises(ExecutionFailure):
                        client.execute({'bad':task(1)},project='parallel-test',output_dir=first,ready_tasks=1)
                row=json.loads(next((first/'jobs').glob('*/state.json')).read_bytes())
                self.assertEqual(row['state'],'prepared')
                self.assertEqual(client.statuses([row['id']])[row['id']]['status'],'unknown')
                result=client.execute({'good':task(2)},project='parallel-test',
                    output_dir=Path(root)/'next',ready_tasks=1)
                self.assertEqual(result[0]['stdout'].strip(),'2')

    def test_definite_rejection_releases_credit_while_its_peer_publication_is_blocked(self):
        from remote_compute.client import RemoteError
        with scratch()as root,provider(Path(root)/'worker',cpus=2)as(_,worker):
            with Pool([worker])as pool,center(pool,Path(root)/'center')as(_,client):
                bad=[];entered=threading.Event();release=threading.Event();submit=client.submit_bundle
                def prepare(number,identity):
                    if number==2:bad.append(identity)
                    return task(number)
                def rejecting(identity,data):
                    if identity in bad:
                        error=RemoteError('Invalid submitted request');error.status_code=400;raise error
                    return submit(identity,data)
                def publish(receipt):
                    entered.set()
                    if not release.wait(6):raise RuntimeError('Publication was not released')
                with patch.object(client,'submit_bundle',side_effect=rejecting),ThreadPoolExecutor(2)as callers:
                    first=callers.submit(client.execute,{'good':1,'bad':2},project='parallel-test',
                        output_dir=Path(root)/'first',prepare=prepare,publish=publish,ready_tasks=2)
                    try:
                        self.assertTrue(entered.wait(4))
                        peer=callers.submit(client.execute,{'peer':task(3)},project='parallel-test',
                            output_dir=Path(root)/'peer',ready_tasks=2)
                        self.assertEqual(peer.result(timeout=3)[0]['stdout'].strip(),'3')
                    finally:release.set()
                    with self.assertRaises(ExecutionFailure):first.result(timeout=6)

    def test_project_state_writer_releases_the_lock_before_execution_continues(self):
        from hashlib import sha256
        with scratch()as root,provider(Path(root)/'worker',cpus=1)as(_,worker):
            with Pool([worker])as pool,center(pool,Path(root)/'center')as(_,client):
                identity=sha256((client.url+'\0parallel-test').encode()).hexdigest()
                lock=Path.cwd()/'.local/dco-client'/identity
                code="import sys; from pathlib import Path; from remote_compute.worker import lock_root; hold=lock_root(Path(sys.argv[1]),'pipeline'); print('locked',flush=True); sys.stdin.readline(); hold.close()"
                with subprocess.Popen([sys.executable,'-B','-c',code,str(lock)],
                        stdin=subprocess.PIPE,stdout=subprocess.PIPE,text=True)as child,ThreadPoolExecutor(1)as callers:
                    try:
                        self.assertEqual(child.stdout.readline().strip(),'locked')
                        future=callers.submit(client.execute,{},project='parallel-test',output_dir=Path(root)/'delivery')
                        time.sleep(.1);self.assertFalse(future.done())
                    finally:child.communicate('\n',timeout=5)
                    self.assertEqual(future.result(timeout=5),[])
                self.assertEqual(child.returncode,0)

    def test_missing_recovery_receipt_keeps_original_credit_and_identity(self):
        with scratch()as root,provider(Path(root)/'worker',cpus=1)as(_,worker):
            with Pool([worker])as pool,center(pool,Path(root)/'center')as(_,client):
                original='abcde012345678901234567890123456';first=Path(root)/'recovery'
                with self.assertRaises(ExecutionFailure):
                    client.execute({'old':dict(job_id=original)},project='parallel-test',
                        output_dir=first,ready_tasks=1)
                row=json.loads(next((first/'jobs').glob('*/state.json')).read_bytes())
                self.assertEqual((row['id'],row['state'],row['recovery']),(original,'new',True))
                stopped=threading.Event();prepared=[]
                def prepare(spec,identity):prepared.append(identity);return spec
                with ThreadPoolExecutor(1)as callers:
                    future=callers.submit(client.execute,{'new':task(2)},project='parallel-test',
                        output_dir=Path(root)/'peer',prepare=prepare,ready_tasks=1,stop_when=stopped.is_set)
                    try:
                        time.sleep(.15);self.assertEqual(prepared,[]);self.assertFalse(future.done())
                    finally:stopped.set()
                    with self.assertRaises(ExecutionPaused):future.result(timeout=3)
                self.assertEqual(client.statuses([original])[original]['status'],'unknown')

    def test_finishing_one_call_does_not_wait_for_its_peers_blocked_publication(self):
        with scratch()as root,provider(Path(root)/'worker',cpus=2)as(_,worker):
            with Pool([worker])as pool,center(pool,Path(root)/'center')as(_,client):
                entered=threading.Event();release=threading.Event()
                def publish(receipt):
                    entered.set()
                    if not release.wait(5):raise RuntimeError('Peer publication was not released')
                with ThreadPoolExecutor(2)as callers:
                    first=callers.submit(client.execute,{'one':task(1)},project='parallel-test',
                        output_dir=Path(root)/'first',publish=publish,ready_tasks=2)
                    try:
                        self.assertTrue(entered.wait(3))
                        second=callers.submit(client.execute,{'two':task(2)},project='parallel-test',
                            output_dir=Path(root)/'second',ready_tasks=2)
                        self.assertEqual(second.result(timeout=3)[0]['stdout'].strip(),'2')
                    finally:release.set()
                    self.assertEqual(first.result(timeout=6)[0]['stdout'].strip(),'1')

    def test_callback_reentry_is_explicit_and_preserves_the_parent_receipt(self):
        from remote_compute.worker import Conflict
        with scratch()as root,provider(Path(root)/'worker',cpus=2)as(_,worker):
            with Pool([worker])as pool,center(pool,Path(root)/'center')as(_,client):
                published=[]
                def publish(receipt):
                    with self.assertRaisesRegex(Conflict,'callbacks cannot reenter'):
                        client.execute({},project='parallel-test',output_dir=Path(root)/'nested')
                    published.append(receipt['id'])
                result=client.execute({'one':task(1)},project='parallel-test',
                    output_dir=Path(root)/'delivery',publish=publish,ready_tasks=1)
                self.assertEqual([row['id']for row in result],published)
                self.assertEqual(client.statuses(published)[published[0]]['status'],'unknown')

    def test_concurrent_calls_use_one_explicit_project_window(self):
        with scratch()as root,provider(Path(root)/'worker',cpus=2)as(_,worker):
            with Pool([worker])as pool,center(pool,Path(root)/'center')as(_,client):
                entered=threading.Event();release=threading.Event()
                def prepare(spec,identity):
                    entered.set()
                    if not release.wait(4):raise RuntimeError('Preparation was not released')
                    return task(spec)
                with ThreadPoolExecutor(1)as callers:
                    first=callers.submit(client.execute,{'one':1},project='parallel-test',
                        output_dir=Path(root)/'first',prepare=prepare,ready_tasks=2)
                    try:
                        self.assertTrue(entered.wait(3))
                        with self.assertRaisesRegex(ValueError,'same ready_tasks'):
                            client.execute({},project='parallel-test',output_dir=Path(root)/'second',ready_tasks=3)
                    finally:release.set()
                    self.assertEqual(first.result(timeout=6)[0]['stdout'].strip(),'1')

    def test_interrupt_after_shared_submit_enqueue_drains_the_same_original_id(self):
        from remote_compute.pipeline_control import _CallPool
        with scratch()as root,provider(Path(root)/'worker',cpus=2)as(_,worker):
            with Pool([worker])as pool,center(pool,Path(root)/'center')as(_,client):
                original=_CallPool.submit;interrupted=[]
                def submit(executor,function,*args,**options):
                    future=original(executor,function,*args,**options)
                    if function.__name__=='submit'and not interrupted:
                        interrupted.append(args[0]['id']);raise KeyboardInterrupt
                    return future
                with patch.object(_CallPool,'submit',submit):
                    with self.assertRaises(ExecutionPaused)as paused:
                        client.execute({'one':task(1),'next':dict(spec=2,after=['one'])},
                            project='parallel-test',output_dir=Path(root)/'delivery')
                self.assertEqual([r['id']for r in paused.exception.results],interrupted)
                self.assertEqual(paused.exception.remaining,['next'])
                self.assertEqual(client.statuses(interrupted)[interrupted[0]]['status'],'unknown')

    def test_failed_publication_holds_project_credit_across_controller_recovery(self):
        with scratch()as root,provider(Path(root)/'worker',cpus=2)as(_,worker):
            with Pool([worker])as pool,center(pool,Path(root)/'center')as(_,client):
                plan={'one':task(1)};first_dir=Path(root)/'first';paid=[]
                def failed(receipt):paid.append(receipt['id']);raise RuntimeError('Scientific publication failed')
                with self.assertRaises(ExecutionFailure):
                    client.execute(plan,project='parallel-test',output_dir=first_dir,publish=failed,ready_tasks=1)
                prepared=threading.Event();join=threading.Event();original_window=client.window
                def window(*args,**options):
                    value=original_window(*args,**options);join.set();return value
                def prepare(number,identity):prepared.set();return task(number)
                with patch.object(client,'window',side_effect=window),ThreadPoolExecutor(2)as callers:
                    second=callers.submit(client.execute,{'two':2},project='parallel-test',
                        output_dir=Path(root)/'second',prepare=prepare,ready_tasks=1)
                    self.assertTrue(join.wait(3));self.assertFalse(prepared.wait(.2))
                    resumed=callers.submit(client.execute,plan,project='parallel-test',output_dir=first_dir,ready_tasks=1)
                    self.assertEqual(resumed.result(timeout=8)[0]['id'],paid[0])
                    self.assertEqual(second.result(timeout=8)[0]['stdout'].strip(),'2')
                self.assertEqual(client.statuses(paid)[paid[0]]['status'],'unknown')

    def test_parallel_plans_share_the_four_preparation_workers(self):
        with scratch()as root,provider(Path(root)/'worker',cpus=4)as(_,worker):
            with Pool([worker])as pool,center(pool,Path(root)/'center')as(_,client):
                guard=threading.Lock();entered=threading.Event();release=threading.Event();active=peak=0
                def prepare(number,identity):
                    nonlocal active,peak
                    with guard:
                        active+=1;peak=max(peak,active)
                        if active==4:entered.set()
                    try:
                        if not release.wait(5):raise RuntimeError('Preparation workers did not share capacity')
                        return task(number)
                    finally:
                        with guard:active-=1
                with ThreadPoolExecutor(6)as callers:
                    futures=[callers.submit(client.execute,{str(j):j for j in (i*2,i*2+1)},
                        project='parallel-test',output_dir=Path(root)/str(i),prepare=prepare,ready_tasks=32)for i in range(6)]
                    try:self.assertTrue(entered.wait(4))
                    finally:release.set()
                    results=[f.result(timeout=12)for f in futures]
                self.assertEqual(peak,4);self.assertEqual(sum(map(len,results)),12)

    def test_slow_project_window_does_not_block_another_project(self):
        with scratch()as root,provider(Path(root)/'worker',cpus=2)as(_,worker):
            with Pool([worker])as pool,center(pool,Path(root)/'center')as(_,client):
                entered=threading.Event();release=threading.Event();original=client.window
                def window(project,**options):
                    if project=='slow-project':
                        entered.set()
                        if not release.wait(5):raise RuntimeError('Other project was blocked by the slow window')
                    return original(project,**options)
                with patch.object(client,'window',side_effect=window),ThreadPoolExecutor(2)as callers:
                    first=callers.submit(client.execute,{'one':task(1)},project='slow-project',output_dir=Path(root)/'slow')
                    try:
                        self.assertTrue(entered.wait(3))
                        second=callers.submit(client.execute,{'two':task(2)},project='fast-project',output_dir=Path(root)/'fast')
                        self.assertEqual(second.result(timeout=3)[0]['stdout'].strip(),'2')
                    finally:release.set()
                    self.assertEqual(first.result(timeout=6)[0]['stdout'].strip(),'1')

    def test_same_project_parallel_directories_keep_independent_plans_and_ids(self):
        with scratch()as root,provider(Path(root)/'worker',cpus=2)as(_,worker):
            with Pool([worker])as pool,center(pool,Path(root)/'center')as(_,client):
                entered=set();guard=threading.Lock();both=threading.Event()
                def prepare(number,identity):
                    with guard:
                        entered.add(number)
                        if len(entered)==2:both.set()
                    if not both.wait(3):raise RuntimeError('Independent execution did not enter preparation')
                    return task(number)
                with ThreadPoolExecutor(2)as callers:
                    futures=[callers.submit(client.execute,{'one':i},project='parallel-test',
                        output_dir=Path(root)/('delivery'+str(i)),prepare=prepare,ready_tasks=2)for i in (1,2)]
                    results=[f.result(timeout=10)for f in futures]
                self.assertEqual(entered,{1,2})
                self.assertEqual([r[0]['stdout'].strip()for r in results],['1','2'])
                self.assertNotEqual(results[0][0]['id'],results[1][0]['id'])
                for i,result in zip((1,2),results):
                    row=json.loads(next((Path(root)/('delivery'+str(i))/'jobs').glob('*/state.json')).read_text())
                    self.assertEqual((row['state'],row['id']),('collected',result[0]['id']))

    def test_same_project_credit_stays_held_until_publication_and_delete_finish(self):
        with scratch()as root,provider(Path(root)/'worker',cpus=2)as(_,worker):
            with Pool([worker])as pool,center(pool,Path(root)/'center')as(_,client):
                entered=threading.Event();release=threading.Event();prepared=threading.Event();original=[]
                def publish(receipt):
                    original.append(receipt['id']);entered.set()
                    if not release.wait(5):raise RuntimeError('Publication was not released')
                def prepare(number,identity):prepared.set();return task(number)
                with ThreadPoolExecutor(2)as callers:
                    first=callers.submit(client.execute,{'one':task(1)},project='parallel-test',
                        output_dir=Path(root)/'first',publish=publish,ready_tasks=1)
                    try:
                        self.assertTrue(entered.wait(4))
                        second=callers.submit(client.execute,{'two':2},project='parallel-test',
                            output_dir=Path(root)/'second',prepare=prepare,ready_tasks=1)
                        self.assertFalse(prepared.wait(.2))
                        self.assertEqual(client.status(original[0])['status'],'succeeded')
                    finally:release.set()
                    self.assertEqual(first.result(timeout=8)[0]['id'],original[0])
                    self.assertEqual(second.result(timeout=8)[0]['stdout'].strip(),'2')

    def test_one_output_directory_has_one_writer_even_across_projects(self):
        from remote_compute.worker import Conflict
        with scratch()as root,provider(Path(root)/'worker',cpus=1)as(_,worker):
            with Pool([worker])as pool,center(pool,Path(root)/'center')as(_,client):
                entered=threading.Event();release=threading.Event()
                def prepare(number,identity):
                    entered.set()
                    if not release.wait(4):raise RuntimeError('Preparation was not released')
                    return task(number)
                with ThreadPoolExecutor(1)as callers:
                    first=callers.submit(client.execute,{'one':1},project='first-project',
                        output_dir=Path(root)/'shared',prepare=prepare)
                    try:
                        self.assertTrue(entered.wait(3))
                        with self.assertRaises(Conflict):
                            client.execute({'one':task(2)},project='second-project',output_dir=Path(root)/'shared')
                    finally:release.set()
                    self.assertEqual(first.result(timeout=6)[0]['stdout'].strip(),'1')

    def test_resuming_deferred_dependency_work_rechecks_the_same_original_id(self):
        with scratch()as root,provider(Path(root)/'worker',cpus=1)as(_,worker):
            with Pool([worker])as pool,center(pool,Path(root)/'center')as(_,client):
                identities=[];resumed=False
                def prepare(spec,identity):
                    if spec=='recovery':
                        identities.append(identity)
                        if not resumed:return Deferred('Operator handoff pending')
                    return Skipped('Existing scientific result')
                plan={'recovery':dict(spec='recovery',after=['skip1','skip2']),
                      'skip1':'skip1','skip2':'skip2','bulk':'bulk'}
                output=Path(root)/'delivery'
                with self.assertRaises(ExecutionPaused)as paused:
                    client.execute(plan,project='order-test',output_dir=output,prepare=prepare,
                        ready_tasks=1,stop_when=lambda:bool(identities))
                self.assertIn('recovery',paused.exception.remaining)
                resumed=True
                with patch.object(client,'submit_bundle',side_effect=AssertionError('Paid results are reused')):
                    result=client.execute(plan,project='order-test',output_dir=output,prepare=prepare,ready_tasks=1)
                self.assertEqual(len(identities),2);self.assertEqual(identities[0],identities[1])
                self.assertIn('recovery',result.skipped)

    def test_newly_satisfied_dependency_keeps_its_catalog_priority(self):
        with scratch()as root,provider(Path(root)/'worker',cpus=1)as(_,worker):
            with Pool([worker])as pool,center(pool,Path(root)/'center')as(_,client):
                prepared=[]
                def prepare(spec,identity):
                    prepared.append(spec);return Skipped('Existing scientific result')
                plan={'recovery':dict(spec='recovery',after=['prefix']),'prefix':'prefix',
                      **{'bulk'+str(i):'bulk'+str(i)for i in range(8)}}
                result=client.execute(plan,project='order-test',output_dir=Path(root)/'delivery',prepare=prepare,ready_tasks=1)
                self.assertEqual(prepared,['prefix','recovery']+['bulk'+str(i)for i in range(8)])
                self.assertEqual(result.skipped,prepared)

    def test_due_deferred_item_rejoins_at_its_catalog_position_with_original_id(self):
        with scratch()as root,provider(Path(root)/'worker',cpus=1)as(_,worker):
            with Pool([worker])as pool,center(pool,Path(root)/'center')as(_,client):
                ready=threading.Event();prepared=[];identities=[]
                def prepare(spec,identity):
                    prepared.append(spec)
                    if spec=='waiting':
                        identities.append(identity)
                        if not ready.is_set():return Deferred('Operator handoff pending')
                    if spec=='prefix':
                        time.sleep(1.08);ready.set()
                    return Skipped('Existing scientific result')
                plan={'waiting':'waiting','prefix':'prefix',**{'bulk'+str(i):'bulk'+str(i)for i in range(8)}}
                client.execute(plan,project='order-test',output_dir=Path(root)/'delivery',prepare=prepare,ready_tasks=1)
                self.assertEqual(prepared,['waiting','prefix','waiting']+['bulk'+str(i)for i in range(8)])
                self.assertEqual(len(identities),2);self.assertEqual(identities[0],identities[1])

    def test_unrelated_completion_does_not_bypass_deferred_retry_interval(self):
        with scratch()as root,provider(Path(root)/'worker',cpus=1)as(_,worker):
            with Pool([worker])as pool,center(pool,Path(root)/'center')as(_,client):
                retries=[];ordinary=[]
                def prepare(spec,identity):
                    if spec=='waiting':
                        retries.append(time.monotonic());return Deferred('External input pending')
                    time.sleep(.02);ordinary.append(spec);return Skipped('Existing result')
                plan={'waiting':'waiting',**{'bulk'+str(i):'bulk'+str(i)for i in range(10)}}
                with self.assertRaises(ExecutionPaused):
                    client.execute(plan,project='order-test',output_dir=Path(root)/'delivery',prepare=prepare,
                        ready_tasks=1,stop_when=lambda:len(retries)>=2)
                self.assertEqual(len(ordinary),10)
                self.assertGreaterEqual(len(retries),2)
                self.assertTrue(all(b-a>=.95 for a,b in zip(retries,retries[1:])),retries)

    def test_interrupt_after_publication_leaves_original_transport_pending_until_delete(self):
        with scratch()as root,provider(Path(root)/'worker',cpus=2)as(_,worker):
            with Pool([worker])as pool,center(pool,Path(root)/'center')as(_,client):
                original=client.delete;published=[];interrupted=[]
                def remove(identity):
                    if not interrupted:interrupted.append(identity);raise KeyboardInterrupt
                    return original(identity)
                out=Path(root)/'delivery';plan={'one':task(1)}
                with patch.object(client,'delete',side_effect=remove):
                    with self.assertRaises(ExecutionPaused)as paused:
                        client.execute(plan,project='pipeline-test',output_dir=out,publish=lambda r:published.append(r['id']))
                self.assertEqual(paused.exception.remaining,['one'])
                self.assertEqual([r['id']for r in paused.exception.results],published)
                self.assertEqual(client.status(published[0])['status'],'succeeded')
                with patch.object(client,'submit_bundle',side_effect=AssertionError('Original must not resubmit')):
                    results=client.execute(plan,project='pipeline-test',output_dir=out,publish=lambda r:self.fail('Already published'))
                self.assertEqual([r['id']for r in results],published)
                self.assertEqual(client.statuses(published)[published[0]]['status'],'unknown')

    def test_changed_business_inputs_or_original_id_reject_the_same_output_directory(self):
        with scratch()as root,provider(Path(root)/'worker',cpus=2)as(_,worker):
            with Pool([worker])as pool,center(pool,Path(root)/'center')as(_,client):
                out=Path(root)/'delivery';original=client.execute({'one':task(1)},project='pipeline-test',output_dir=out)
                with patch.object(client,'submit_bundle',side_effect=AssertionError('Changed plan must not submit')):
                    for value in (task(2),dict(job_id=original[0]['id']),dict(job_id='f'*32)):
                        with self.assertRaisesRegex(ValueError,'immutable business plan'):
                            client.execute({'one':value},project='pipeline-test',output_dir=out)

    def test_maintenance_admission_gate_survives_center_restart(self):
        with scratch()as root,provider(Path(root)/'worker',cpus=2)as(_,worker):
            with Pool([worker])as pool:
                state=Path(root)/'center'
                with center(pool,state)as(_,operator):operator.control(accepting=False)
                with center(pool,state)as(_,operator):
                    client=Client(operator.url)
                    self.assertFalse(client.admission('pipeline-test')['accepting'])
                    with self.assertRaisesRegex(Exception,'new task admissions'):
                        client.submit(task(1)['task'])
                    operator.control(accepting=True)

    def test_interrupt_in_prepare_submit_or_publish_keeps_the_original_identity(self):
        for where in ('prepare','submit','publish'):
            with self.subTest(where=where),scratch()as root,provider(Path(root)/'worker',cpus=2)as(_,worker):
                with Pool([worker])as pool,center(pool,Path(root)/'center')as(_,client):
                    original=client.submit_bundle;accepted=[]
                    def submit(identity,data,**options):
                        result=original(identity,data,**options);accepted.append(identity)
                        if where=='submit':raise KeyboardInterrupt
                        return result
                    def prepare(spec,identity):
                        if where=='prepare':raise KeyboardInterrupt
                        return spec
                    def publish(receipt):
                        if where=='publish':raise KeyboardInterrupt
                    out=Path(root)/'delivery'
                    with patch.object(client,'submit_bundle',side_effect=submit):
                        with self.assertRaises(ExecutionPaused)as paused:
                            client.execute({'one':task(1),'next':dict(spec=task(2),after=['one'])},
                                project='pipeline-test',output_dir=out,prepare=prepare,publish=publish)
                        if where=='publish':
                            self.assertEqual(paused.exception.results,[])
                            self.assertIn('one',paused.exception.remaining)
                    if accepted:
                        state=json.loads(next(out.rglob('state.json')).read_bytes())
                        with patch.object(client,'submit_bundle',side_effect=AssertionError('Original must not resubmit')):
                            # Resume just the already-frozen plan; the dependent is skipped by its business gate.
                            results=client.execute({'one':task(1),'next':dict(spec=task(2),after=['one'])},
                                project='pipeline-test',output_dir=out,prepare=lambda spec,identity:Skipped('Qualification complete'))
                        self.assertEqual(results[0]['id'],accepted[0])

    def test_results_are_published_before_remote_delete_and_resume_keeps_ids(self):
        with scratch()as root,provider(Path(root)/'worker',cpus=4)as(_,worker):
            with Pool([worker])as pool,center(pool,Path(root)/'center')as(_,client):
                published=[]
                def publish(receipt):
                    self.assertEqual(client.status(receipt['id'])['status'],'succeeded')
                    self.assertTrue((Path(receipt['output_dir'])/'stdout.log').is_file())
                    published.append(receipt['id'])
                requests={str(i):task(i)for i in range(3)};out=Path(root)/'delivery'
                first=client.execute(requests,project='pipeline-test',output_dir=out,publish=publish)
                self.assertEqual(len(published),3)
                with patch.object(client,'submit_bundle',side_effect=AssertionError('Original ID already collected')):
                    second=client.execute(requests,project='pipeline-test',output_dir=out,publish=publish)
                self.assertEqual({r['id']for r in first},{r['id']for r in second})
                self.assertEqual(len(published),3)
                self.assertTrue(all(r['status']=='unknown'for r in client.statuses(published).values()))
                self.assertFalse(list(out.rglob('request.zip')))

    def test_blocked_publication_does_not_stop_other_preparation(self):
        with scratch()as root,provider(Path(root)/'worker',cpus=4)as(_,worker):
            with Pool([worker])as pool,center(pool,Path(root)/'center')as(_,client):
                fourth=threading.Event();guard=threading.Lock();published=[];prepared=[]
                def prepare(spec,identity):
                    prepared.append(spec)
                    if spec==3:fourth.set()
                    return task(spec)
                def publish(receipt):
                    with guard:
                        first=not published;published.append(receipt['id'])
                    if first:self.assertTrue(fourth.wait(5),'Preparation waited behind publication')
                results=client.execute({str(i):i for i in range(5)},project='pipeline-test',
                    output_dir=Path(root)/'delivery',prepare=prepare,publish=publish,ready_tasks=2)
                self.assertEqual(len(results),5);self.assertEqual(prepared,list(range(5)))

    def test_publication_failure_resumes_from_fetched_original_receipt(self):
        with scratch()as root,provider(Path(root)/'worker',cpus=2)as(_,worker):
            with Pool([worker])as pool,center(pool,Path(root)/'center')as(_,client):
                out=Path(root)/'delivery';requests={'one':task(1)}
                def fail(receipt):raise RuntimeError('Business store temporarily unavailable')
                with self.assertRaises(ExecutionFailure):
                    client.execute(requests,project='pipeline-test',output_dir=out,publish=fail)
                state=json.loads(next(out.rglob('state.json')).read_bytes());identity=state['id']
                self.assertEqual(state['state'],'publication_failed')
                self.assertEqual(client.status(identity)['status'],'succeeded')
                with patch.object(client,'submit_bundle',side_effect=AssertionError('Paid result must not rerun')):
                    results=client.execute(requests,project='pipeline-test',output_dir=out)
                self.assertEqual(results[0]['id'],identity)
                self.assertEqual(client.statuses([identity])[identity]['status'],'unknown')

    def test_dependencies_wait_for_publication_without_blocking_preparation(self):
        with scratch()as root,provider(Path(root)/'worker',cpus=4)as(_,worker):
            with Pool([worker])as pool,center(pool,Path(root)/'center')as(_,client):
                checkpoint=[];prepared=[]
                def prepare(spec,identity):
                    prepared.append(spec)
                    if spec=='next':self.assertEqual(checkpoint,['prefix'])
                    if spec=='unused':return Skipped('Native budget already complete')
                    return task(1)
                def publish(receipt):
                    if receipt['business_key']=='prefix':checkpoint.append('prefix')
                plan={'next':dict(spec='next',after=['prefix']),
                      'unused':dict(spec='unused',after=['next']),'prefix':dict(spec='prefix')}
                results=client.execute(plan,project='pipeline-test',output_dir=Path(root)/'delivery',prepare=prepare,publish=publish)
                self.assertEqual(prepared,['prefix','next','unused'])
                self.assertEqual(len(results),2);self.assertEqual(results.skipped,['unused'])

    def test_deferred_inputs_do_not_occupy_all_preparation_threads(self):
        with scratch()as root,provider(Path(root)/'worker',cpus=4)as(_,worker):
            with Pool([worker])as pool,center(pool,Path(root)/'center')as(_,client):
                published=threading.Event()
                def prepare(spec,identity):
                    if spec!='prefix'and not published.is_set():return Deferred('Awaiting native checkpoint')
                    return task(1)
                def publish(receipt):
                    if receipt['business_key']=='prefix':published.set()
                plan={str(i):i for i in range(4)};plan['prefix']='prefix'
                results=client.execute(plan,project='pipeline-test',output_dir=Path(root)/'delivery',prepare=prepare,publish=publish)
                self.assertEqual(len(results),5)

    def test_maintenance_gate_defers_new_work_and_keeps_original_id_recovery(self):
        with scratch()as root,provider(Path(root)/'worker',cpus=4)as(_,worker):
            with Pool([worker])as pool,center(pool,Path(root)/'center')as(_,operator):
                client=Client(operator.url)
                original=client.submit(task(1)['task'])['id']
                operator.control(accepting=False)
                requests={'new':task(2)};out=Path(root)/'delivery'
                with self.assertRaises(ExecutionPaused)as paused:
                    client.execute(requests,project='pipeline-test',output_dir=out)
                self.assertEqual(paused.exception.remaining,['new'])
                self.assertEqual(operator.status(original)['id'],original)
                operator.control(accepting=True)
                results=client.execute(requests,project='pipeline-test',output_dir=out)
                self.assertEqual(len(results),1)
                client.wait(original);client.fetch(original)

    def test_existing_original_id_is_collected_without_repreparing_or_resubmitting(self):
        with scratch()as root,provider(Path(root)/'worker',cpus=2)as(_,worker):
            with Pool([worker])as pool,center(pool,Path(root)/'center')as(_,client):
                identity=client.submit(task(1)['task'])['id']
                with patch.object(client,'submit_bundle',side_effect=AssertionError('Original request must not rerun')):
                    results=client.execute({'original':dict(job_id=identity)},project='pipeline-test',output_dir=Path(root)/'delivery')
                self.assertEqual(results[0]['id'],identity)

    def test_closed_admissions_still_drain_recovered_ids_and_defer_new_work(self):
        with scratch()as root,provider(Path(root)/'worker',cpus=2)as(_,worker):
            with Pool([worker])as pool,center(pool,Path(root)/'center')as(_,operator):
                client=Client(operator.url);identity=client.submit(task(1)['task'])['id']
                operator.control(accepting=False)
                plan={'original':dict(job_id=identity),'new':dict(spec=2,after=['original'])}
                with patch.object(client,'submit_bundle',side_effect=AssertionError('Maintenance must not submit')):
                    with self.assertRaises(ExecutionPaused)as paused:
                        client.execute(plan,project='pipeline-test',output_dir=Path(root)/'delivery')
                self.assertEqual([r['id']for r in paused.exception.results],[identity])
                self.assertEqual(paused.exception.remaining,['new'])
                self.assertEqual(client.statuses([identity])[identity]['status'],'unknown')

    def test_keyboard_interrupt_drains_accepted_ids_before_raising_paused(self):
        with scratch()as root,provider(Path(root)/'worker',cpus=2)as(_,worker):
            with Pool([worker])as pool,center(pool,Path(root)/'center')as(_,client):
                original=client.statuses;interrupted=[]
                def interrupt_once(identities):
                    if not interrupted:interrupted.append(True);raise KeyboardInterrupt
                    return original(identities)
                with patch.object(client,'statuses',side_effect=interrupt_once):
                    with self.assertRaises(ExecutionPaused)as paused:
                        client.execute({'one':task(1),'next':dict(spec=2,after=['one'])},
                            project='pipeline-test',output_dir=Path(root)/'delivery')
                self.assertEqual(len(paused.exception.results),1)
                self.assertEqual(paused.exception.remaining,['next'])
                self.assertEqual(client.status(),[])
