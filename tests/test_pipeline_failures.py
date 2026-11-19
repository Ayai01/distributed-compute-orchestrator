"""An original's error blocks its descendants, not independent ready work."""
import json
from pathlib import Path
import unittest
from unittest.mock import patch

from remote_compute import Pool,command,ExecutionFailure
from remote_compute.client import RemoteError
from test_compute import scratch,provider
from test_center import center
from test_pipeline import task


class PipelineFailureTests(unittest.TestCase):
    def test_status_transport_errors_keep_accepted_ids_and_resume_collection(self):
        with scratch()as root,provider(Path(root)/'worker',cpus=2)as(_,worker):
            with Pool([worker])as pool,center(pool,Path(root)/'center')as(_,client):
                sent=[];outages=[]
                submit,statuses=client.submit_bundle,client.statuses

                def submit_once(identity,data,**options):
                    self.assertNotIn(identity,sent)
                    sent.append(identity)
                    return submit(identity,data,**options)

                def intermittent(identities,*,compact=False):
                    if compact and len(outages)<2:
                        outages.append(tuple(identities))
                        error=RemoteError('Status temporarily unavailable');error.status_code=503
                        raise error
                    return statuses(identities,compact=compact)

                with patch.object(client,'statuses',side_effect=intermittent), \
                        patch.object(client,'submit_bundle',side_effect=submit_once), \
                        patch.object(client,'_read_reconnecting',side_effect=lambda read,**options:read()):
                    results=client.execute({'first':task(1),'second':task(2)},project='status-recovery',
                        output_dir=Path(root)/'delivery',ready_tasks=2)
                self.assertEqual(len(outages),2)
                self.assertIn(outages[0][0],outages[1])
                self.assertEqual({row['id'] for row in results},set(sent))
                self.assertEqual(len(sent),2)
                self.assertTrue(all(row['status']=='succeeded' for row in results))

    def test_prepare_failure_releases_credit_and_resumes_original_identity(self):
        with scratch()as root,provider(Path(root)/'worker',cpus=2)as(_,worker):
            with Pool([worker])as pool,center(pool,Path(root)/'center')as(_,client):
                output=Path(root)/'delivery';prepared=[];broken=True
                plan={'bad':0,'child':dict(spec=8,after=['bad']),
                      'grandchild':dict(spec=9,after=['child']),**{str(i):i for i in range(1,5)}}
                def prepare(value,identity):
                    prepared.append((value,identity))
                    if value==0 and broken:raise RuntimeError('Preparation failed')
                    return task(value)
                with self.assertRaises(ExecutionFailure)as failed:
                    client.execute(plan,project='failure-test',output_dir=output,prepare=prepare,ready_tasks=1)
                self.assertEqual(len(failed.exception.results),4)
                self.assertEqual(failed.exception.blocked,['child','grandchild'])
                self.assertEqual([value for value,_ in prepared],list(range(5)))
                original=prepared[0][1];broken=False
                results=client.execute(plan,project='failure-test',output_dir=output,prepare=prepare,ready_tasks=1)
                self.assertEqual(len(results),7)
                self.assertEqual(next(r['id']for r in results if r['business_key']=='bad'),original)

    def test_rejected_submit_keeps_ready_independent_work_flowing(self):
        with scratch()as root,provider(Path(root)/'worker',cpus=2)as(_,worker):
            with Pool([worker])as pool,center(pool,Path(root)/'center')as(_,client):
                rejected=[];submit=client.submit_bundle
                def post(identity,data):
                    if not rejected:
                        rejected.append(identity);error=RemoteError('Invalid task');error.status_code=400;raise error
                    return submit(identity,data)
                output=Path(root)/'delivery'
                with patch.object(client,'submit_bundle',side_effect=post),self.assertRaises(ExecutionFailure)as failed:
                    client.execute({str(i):task(i)for i in range(5)},project='failure-test',
                                   output_dir=output,ready_tasks=1)
                self.assertEqual(len(failed.exception.results),4)
                self.assertEqual(failed.exception.remaining,['0'])
                row=next(json.loads(p.read_bytes())for p in (output/'jobs').glob('*/state.json')
                         if json.loads(p.read_bytes())['id']==rejected[0])
                self.assertEqual(row['state'],'prepared')

    def test_worker_failure_does_not_unlock_dependencies_or_stop_independent_jobs(self):
        with scratch()as root,provider(Path(root)/'worker',cpus=2)as(_,worker):
            with Pool([worker])as pool,center(pool,Path(root)/'center')as(_,client):
                plan={'bad':dict(task=command(['${tool:python}','-c','raise SystemExit(3)'],max_cpus=1)),
                      'child':dict(spec=task(8),after=['bad']),
                      **{str(i):task(i)for i in range(1,5)}}
                output=Path(root)/'delivery'
                with self.assertRaises(ExecutionFailure)as failed:
                    client.execute(plan,project='failure-test',output_dir=output,ready_tasks=1)
                self.assertEqual(len(failed.exception.results),5)
                self.assertEqual(failed.exception.blocked,['child'])
                self.assertEqual(failed.exception.remaining,['child'])
                batch=json.loads((output/'batch.json').read_bytes())
                self.assertEqual(batch['failures'][0]['stage'],'execution')
                self.assertEqual(batch['blocked'],['child'])

    def test_publication_failure_retains_original_and_continues_independents_then_recovers(self):
        with scratch()as root,provider(Path(root)/'worker',cpus=2)as(_,worker):
            with Pool([worker])as pool,center(pool,Path(root)/'center')as(_,client):
                output=Path(root)/'delivery';published=[];original=[]
                plan={'bad':task(0),'child':dict(spec=task(9),after=['bad']),
                      **{str(i):task(i)for i in range(1,6)}}
                def publish(receipt):
                    if receipt['business_key']=='bad':
                        original.append(receipt['id']);raise RuntimeError('Publish failed')
                    published.append(receipt['id'])
                with self.assertRaises(ExecutionFailure)as failed:
                    client.execute(plan,project='failure-test',output_dir=output,publish=publish,ready_tasks=2)
                self.assertEqual(len(failed.exception.results),5)
                self.assertEqual(len(published),5)
                self.assertEqual(failed.exception.blocked,['child'])
                self.assertEqual(client.status(original[0])['status'],'succeeded')
                submit=client.submit_bundle
                def post(identity,data):
                    self.assertNotIn(identity,published+original);return submit(identity,data)
                with patch.object(client,'submit_bundle',side_effect=post):
                    results=client.execute(plan,project='failure-test',output_dir=output,ready_tasks=2)
                self.assertEqual(len(results),7)
                self.assertEqual(client.statuses(original)[original[0]]['status'],'unknown')

    def test_unpublished_error_filling_window_returns_recovery_instead_of_waiting(self):
        with scratch()as root,provider(Path(root)/'worker',cpus=1)as(_,worker):
            with Pool([worker])as pool,center(pool,Path(root)/'center')as(_,client):
                output=Path(root)/'delivery';seen=[]
                def publish(receipt):seen.append(receipt['id']);raise RuntimeError('Store offline')
                with self.assertRaises(ExecutionFailure)as failed:
                    client.execute({'bad':task(0),'later':task(1)},project='failure-test',
                                   output_dir=output,publish=publish,ready_tasks=1)
                self.assertEqual(failed.exception.remaining,['bad','later'])
                self.assertEqual(len(seen),1)
                self.assertEqual(client.status(seen[0])['status'],'succeeded')
