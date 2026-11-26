"""Operator dispatch metadata leaves accepted scientific requests immutable."""
import hashlib,json,shutil,sys,unittest,zipfile
from pathlib import Path
from unittest.mock import patch
from remote_compute import Client,Pool,RemoteError
from remote_compute.policy import FRACTIONAL_PRIORITY,MAX_PRIORITY,validate_priority
from test_compute import provider,scratch,python,until
from test_center import center


class PriorityControlTests(unittest.TestCase):
    def test_running_reattach_keeps_original_dispatch_override(self):
        from test_leases import HOLD
        with scratch()as root,provider(Path(root)/'worker',cpus=1)as(_,worker),Pool([worker])as pool:
            with center(pool,Path(root)/'first',paused=True)as(state,client):
                identity=client.submit(python(HOLD,priority=-3,max_cpus=1))['id']
                client.control(job=identity,priority=2);client.control(paused=False)
                until(lambda:client.status(identity)['status']=='running')
                recovered=Path(root)/'second';target=recovered/'jobs'/identity;target.mkdir(parents=True)
                with state.condition:
                    for name in ('request.zip','result.json','admission.json'):
                        source=state.root/'jobs'/identity/name
                        if source.exists():shutil.copyfile(source,target/name)
                before=(target/'request.zip').read_bytes()
                (recovered/'history.index.jsonl').write_text(json.dumps(dict(id=identity,terminal=False,assets=[],worker=identity))+'\n')
                with Pool([worker])as peers,center(peers,recovered,paused=True)as(restored,other):
                    until(lambda:restored.jobs[identity].get('node')is not None)
                    self.assertEqual(restored.jobs[identity]['task']['priority'],2)
                    self.assertEqual((target/'request.zip').read_bytes(),before)
                    other.write_job_file(identity,'release','done')
                    client.wait(identity);other.wait(identity)
                    self.assertEqual(other.status(identity)['dispatch_priority'],2)

    def test_override_reorders_queue_and_preserves_original_id_payload_and_receipt(self):
        with scratch()as root,provider(Path(root)/'worker',cpus=1)as(_,node),Pool([node])as pool:
            with center(pool,Path(root)/'center',paused=True)as(state,client):
                original=python("print('original')",priority=0.2)
                first=client.submit(original)['id'];second=client.submit(python("print('later')",priority=0.4))['id']
                archive=state.root/'jobs'/first/'request.zip';before=archive.read_bytes()
                reply=client.control(job=first,priority=0.6)
                self.assertEqual((reply['submitted_priority'],reply['dispatch_priority']),(0.2,0.6))
                queued={row['id']: row['priority'] for row in client.health()['queued_jobs']}
                self.assertEqual(queued,{first:0.6,second:0.4})
                self.assertEqual(reply['request_sha256'],hashlib.sha256(before).hexdigest())
                self.assertEqual(client.submit(original,job_id=first)['id'],first)
                self.assertEqual(archive.read_bytes(),before)
                with zipfile.ZipFile(archive)as bundle:self.assertEqual(json.loads(bundle.read('task.json'))['priority'],0.2)
                client.control(paused=False);client.wait(first);client.wait(second)
                a=client.fetch(first,delete=False);b=client.fetch(second,delete=False)
                self.assertLess(a['started'],b['started'])
                self.assertEqual((a['id'],a['submitted_priority'],a['dispatch_priority']),(first,0.2,0.6))
                self.assertEqual(a['priority'],0.6)
                self.assertEqual(a['request_sha256'],hashlib.sha256(before).hexdigest())
                self.assertEqual(a['stdout'].strip(),'original')
                client.delete(first);client.delete(second)

    def test_override_restores_from_queued_receipt_without_rewriting_request(self):
        with scratch()as root,provider(Path(root)/'worker',cpus=1)as(_,node),Pool([node])as pool:
            with center(pool,Path(root)/'first',paused=True)as(state,client):
                peer=client.submit(python("print('peer')",priority=0.2))['id']
                identity=client.submit(python("print(1)",priority=-1))['id'];client.control(job=identity,priority=0.5)
                recovered=Path(root)/'second';directory=recovered/'jobs'/identity
                for key in (peer,identity):
                    target=recovered/'jobs'/key;target.mkdir(parents=True)
                    for name in ('request.zip','result.json','admission.json'):
                        source=state.root/'jobs'/key/name
                        if source.exists():shutil.copyfile(source,target/name)
                # Restore from the original acceptance order, with the lower
                # effective priority first in the durable index.
                (recovered/'history.index.jsonl').write_text(''.join(json.dumps(dict(id=key,terminal=False,assets=[],worker=key))+'\n'for key in (peer,identity)))
                before=(directory/'request.zip').read_bytes()
                with center(pool,recovered,paused=True)as(restored,other):
                    self.assertEqual(restored.jobs[identity]['task']['priority'],0.5)
                    self.assertEqual(other.status(identity)['submitted_priority'],-1)
                    self.assertEqual((directory/'request.zip').read_bytes(),before)
                    other.control(paused=False);other.wait(identity);other.wait(peer)
                    first=other.fetch(identity);last=other.fetch(peer)
                    self.assertLess(first['started'],last['started'])

    def test_project_client_cannot_change_dispatch_priority(self):
        with scratch()as root,provider(Path(root)/'worker')as(_,node),Pool([node])as pool:
            with center(pool,Path(root)/'center',paused=True)as(state,client):
                identity=client.submit(python('print(1)'))['id'];ordinary=Client(client.url,state.token,operator=False)
                with self.assertRaises(RemoteError)as raised:ordinary.control(job=identity,priority=2)
                self.assertEqual(raised.exception.status_code,403)
                self.assertNotIn('dispatch_priority',client.status(identity))

    def test_running_job_keeps_its_grant_and_priority(self):
        with scratch()as root,provider(Path(root)/'worker')as(_,node),Pool([node])as pool:
            with center(pool,Path(root)/'center')as(state,client):
                identity=client.submit(python('import time;time.sleep(15)'))['id']
                until(lambda:client.status(identity)['status']=='running')
                with self.assertRaises(RemoteError)as raised:client.control(job=identity,priority=2)
                self.assertEqual(raised.exception.status_code,409)
                self.assertNotIn('dispatch_priority',client.status(identity))
                client.cancel(identity);client.wait(identity);client.fetch(identity)

    def test_priority_validation_does_not_change_the_queue(self):
        with scratch()as root,provider(Path(root)/'worker')as(_,node),Pool([node])as pool:
            with center(pool,Path(root)/'center',paused=True)as(state,client):
                identity=client.submit(python('print(1)'))['id']
                for value in (True,0,0.0,-0.2,"0.2",MAX_PRIORITY+1):
                    with self.subTest(value=value),self.assertRaises(RemoteError)as raised:
                        client.control(job=identity,priority=value)
                    self.assertEqual(raised.exception.status_code,400)
                self.assertNotIn('dispatch_priority',client.status(identity))

    def test_failed_metadata_write_does_not_change_effective_priority(self):
        with scratch()as root,provider(Path(root)/'worker')as(_,node),Pool([node])as pool:
            with center(pool,Path(root)/'center',paused=True)as(state,client):
                identity=client.submit(python('print(1)'))['id']
                before=(state.root/'jobs'/identity/'result.json').read_bytes()
                with patch('remote_compute.coordinator.atomic_json',side_effect=OSError('injected metadata failure')):
                    with self.assertRaises(RemoteError):client.control(job=identity,priority=2)
                self.assertEqual(state.jobs[identity]['task']['priority'],1)
                self.assertEqual((state.root/'jobs'/identity/'result.json').read_bytes(),before)
                self.assertNotIn('dispatch_priority',client.status(identity))


    def test_public_fractional_priority_keeps_signed_request_and_follows_integer(self):
        from remote_compute import command
        from remote_compute.bundle import task_bundle
        # A normal project uses the public ML Python alias, with no placement fields.
        with scratch() as root, provider(Path(root)/'worker', cpus=1,
                    python_runtimes={'ml':[sys.executable]}) as (_, worker), Pool([worker]) as pool:
            with center(pool, Path(root)/'center', paused=True) as (state, operator):
                client = Client(operator.url, state.token, operator=False)
                self.assertIn(FRACTIONAL_PRIORITY, client.health()['capabilities'])
                options = dict(max_cpus=1, requires=['python'], retries=0)
                low_task = command(['${tool:python}', '-c', 'print(20)'], priority=0.2, **options)
                low_bundle = task_bundle(low_task)
                request_hash = hashlib.sha256(low_bundle).hexdigest()
                low = client.submit(low_task)['id']
                high = client.submit(command(['${tool:python}', '-c', 'print(100)'],
                                             priority=1, **options))['id']
                self.assertEqual(client.status(low)['priority'], 0.2)
                self.assertEqual((state.root/'jobs'/low/'request.zip').read_bytes(), low_bundle)
                operator.control(paused=False)
                self.assertEqual(client.wait(low)['status'], 'succeeded')
                self.assertEqual(client.wait(high)['status'], 'succeeded')
                receipt = client.fetch(low, delete=False)
                self.assertIs(type(receipt['priority']), float)
                self.assertEqual(receipt['priority'], 0.2)
                self.assertEqual(receipt['request_sha256'], request_hash)
                self.assertEqual(receipt['stdout'].strip(), '20')
                self.assertEqual(worker.status(low)['priority'], 0.2)
                self.assertLess(operator.status(high)['started'], operator.status(low)['started'])


    def test_mixed_worker_versions_use_separate_integer_and_fractional_routing(self):
        from remote_compute.placement import _matching_pass
        from test_center import AdmissionPlanningTests
        scheduler = AdmissionPlanningTests().center([8, 8])
        older, updated = scheduler.pool.nodes
        updated.health['capabilities'] = [FRACTIONAL_PRIORITY]
        matching = _matching_pass(scheduler.pool)
        task = dict(steps=[['echo']], cpus=1)
        self.assertEqual(matching(dict(task, priority=1)), (older, updated))
        self.assertEqual(matching(dict(task, priority=0.2)), (updated,))
        self.assertEqual(matching(dict(task, priority=1.0)), (updated,))

    def test_cli_parsers_and_validation_preserve_numeric_type(self):
        from remote_compute.commands import machine_parser, submission_parser
        run = submission_parser('run')
        direct = machine_parser('exec')
        for text, expected in (('0.2', 0.2), ('1.0', 1.0), ('10', 10), ('-1', -1)):
            with self.subTest(text=text):
                first = run.parse_args(['task.json', '--priority', text]).priority
                second = direct.parse_args(['--node', 'node', '--priority', text, 'echo']).priority
                for value in (first, second):
                    self.assertIs(type(value), type(expected))
                    self.assertEqual(value, expected)
        for value in (True, 0.0, -1.0, "0.2", float("nan"), float("inf"), MAX_PRIORITY+0.1):
            with self.subTest(value=value), self.assertRaises(ValueError):
                validate_priority(value, legacy_signed=True)


if __name__=='__main__':unittest.main()
