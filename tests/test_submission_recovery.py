"""A dispatching receipt can precede provider acceptance at center restart."""
import hashlib
import json
from pathlib import Path
import time
import unittest
from unittest.mock import patch

from remote_compute import Pool
from remote_compute.bundle import atomic_json,task_bundle
from test_center import center
from test_compute import provider,python,scratch


class PendingSubmissionRecoveryTests(unittest.TestCase):
    def restore(self,root,remote,task,identity):
        job=root/'jobs'/identity;job.mkdir(parents=True)
        payload=task_bundle(task);(job/'request.zip').write_bytes(payload)
        atomic_json(job/'result.json',dict(id=identity,status='dispatching',submitted=time.time(),
            started=None,finished=None,worker_url=remote.url,assigned_node='node1',
            cpus=1,memory_mb=256,max_cpus=1,attempts=[],max_retries=0,
            label='pending-original',project='test',steps=[],stdout='',stderr='',
            request_sha256=hashlib.sha256(payload).hexdigest()))

    def test_missing_unconfirmed_post_resumes_same_id_without_business_retry(self):
        with scratch()as folder,provider(Path(folder)/'worker')as(worker,remote):
            identity='b'*32;root=Path(folder)/'center'
            task=python("from pathlib import Path;p=Path('calls');p.write_text(str(int(p.read_text())+1 if p.exists()else 1));print('original-body')",max_cpus=1,retries=0,outputs=['calls'])
            self.restore(root,remote,task,identity)
            with Pool([remote])as pool,center(pool,root)as(_,client):
                self.assertEqual(client.wait(identity)['status'],'succeeded')
                result=client.fetch(identity,delete=False)
                self.assertEqual(result['id'],identity)
                self.assertEqual(result['files']['calls'],b'1')
                self.assertEqual(result['attempts'],[])
                self.assertTrue(result['submission_recovered'])
                self.assertGreaterEqual(result['cpu_seconds'],0)
                self.assertEqual(worker.used,0)

    def test_finished_provider_id_is_observed_without_reposting_body(self):
        with scratch()as folder,provider(Path(folder)/'worker')as(worker,remote):
            identity='c'*32;root=Path(folder)/'center'
            task=python("print('already-paid')",max_cpus=1,retries=0)
            paid=remote.run(task,job_id=identity,keep=True)
            self.restore(root,remote,task,identity)
            with patch.object(remote,'submit_bundle',side_effect=AssertionError('accepted original must not be reposted')):
                with Pool([remote])as pool,center(pool,root)as(_,client):
                    self.assertEqual(client.wait(identity)['status'],'succeeded')
                    result=client.fetch(identity,delete=False)
                    self.assertEqual(result['cpu_seconds'],paid['cpu_seconds'])
                    self.assertEqual(result['stdout'],paid['stdout'])
                    self.assertEqual(result['attempts'],[])
                    self.assertNotIn('submission_recovered',result)
