"""Durable original-ID collection is independent from preparation/publication."""
import json,threading,time,unittest
from pathlib import Path
from unittest.mock import patch
from remote_compute import Client,Pool,command,Deferred,Skipped,ExecutionPaused
from remote_compute.pipeline import ExecutionFailure
from test_compute import scratch,provider
from test_center import center

def task(number):
    return dict(task=command(['${tool:python}','-c','print('+str(number)+')'],max_cpus=1))

class PipelineTests(unittest.TestCase):
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
                        with self.assertRaises(ExecutionPaused):
                            client.execute({'one':task(1),'next':dict(spec=task(2),after=['one'])},
                                project='pipeline-test',output_dir=out,prepare=prepare,publish=publish)
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
