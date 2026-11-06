"""Operator dispatch metadata leaves accepted scientific requests immutable."""
import hashlib,json,shutil,unittest,zipfile
from pathlib import Path
from unittest.mock import patch
from remote_compute import Client,Pool,RemoteError
from remote_compute.policy import MIN_PRIORITY,MAX_PRIORITY
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
                original=python("print('original')",priority=0)
                first=client.submit(original)['id'];second=client.submit(python("print('later')",priority=1))['id']
                archive=state.root/'jobs'/first/'request.zip';before=archive.read_bytes()
                reply=client.control(job=first,priority=2)
                self.assertEqual((reply['submitted_priority'],reply['dispatch_priority']),(0,2))
                self.assertEqual(reply['request_sha256'],hashlib.sha256(before).hexdigest())
                self.assertEqual(client.submit(original,job_id=first)['id'],first)
                self.assertEqual(archive.read_bytes(),before)
                with zipfile.ZipFile(archive)as bundle:self.assertEqual(json.loads(bundle.read('task.json'))['priority'],0)
                client.control(paused=False);client.wait(first);client.wait(second)
                a=client.fetch(first,delete=False);b=client.fetch(second,delete=False)
                self.assertLess(a['started'],b['started'])
                self.assertEqual((a['id'],a['submitted_priority'],a['dispatch_priority']),(first,0,2))
                self.assertEqual(a['request_sha256'],hashlib.sha256(before).hexdigest())
                self.assertEqual(a['stdout'].strip(),'original')
                client.delete(first);client.delete(second)

    def test_override_restores_from_queued_receipt_without_rewriting_request(self):
        with scratch()as root,provider(Path(root)/'worker',cpus=1)as(_,node),Pool([node])as pool:
            with center(pool,Path(root)/'first',paused=True)as(state,client):
                peer=client.submit(python("print('peer')",priority=1))['id']
                identity=client.submit(python("print(1)",priority=-1))['id'];client.control(job=identity,priority=2)
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
                    self.assertEqual(restored.jobs[identity]['task']['priority'],2)
                    self.assertEqual(other.status(identity)['submitted_priority'],-1)
                    self.assertEqual((directory/'request.zip').read_bytes(),before)
                    other.control(paused=False);other.wait(identity);other.wait(peer)
                    first=other.fetch(identity);last=other.fetch(peer)
                    self.assertLess(first['started'],last['started'])

    def test_project_client_cannot_change_dispatch_priority(self):
        with scratch()as root,provider(Path(root)/'worker')as(_,node),Pool([node])as pool:
            with center(pool,Path(root)/'center',paused=True)as(state,client):
                identity=client.submit(python('print(1)'))['id'];ordinary=Client(client.url,operator=False)
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
                for value in (True,1.5,MIN_PRIORITY-1,MAX_PRIORITY+1):
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
                self.assertEqual(state.jobs[identity]['task']['priority'],0)
                self.assertEqual((state.root/'jobs'/identity/'result.json').read_bytes(),before)
                self.assertNotIn('dispatch_priority',client.status(identity))


if __name__=='__main__':unittest.main()
