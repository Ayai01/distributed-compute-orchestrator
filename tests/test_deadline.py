"""Keep completed outputs and item identity across hard/cooperative deadlines."""
import json
import os
from pathlib import Path
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

from remote_compute import check_deadline, remaining_time, python_map, python_task
from remote_compute.execution_timing import summarize
from test_compute import provider, python, scratch, until


BUSINESS='''import json,time
from pathlib import Path
from remote_compute import check_deadline,remaining_time

def evaluate(value):
    if value==0:
        Path('completed.json').write_text(json.dumps(dict(value=value)))
        return value
    Path('active').write_text('ready')
    time.sleep(30)
    return value

def cooperative(value):
    paid=[]
    try:
        while True:
            check_deadline(reserve_seconds=1)
            paid.append(len(paid)+1)
            time.sleep(.02)
    except TimeoutError:
        Path('paid-prefix.json').write_text(json.dumps(dict(paid=paid,remaining=remaining_time())))
        raise
'''


class DeadlineTests(unittest.TestCase):
    def test_wall_timing_counts_overlapping_native_work_and_resource_wait_once(self):
        with scratch() as folder:
            root = Path(folder)
            (root / 'phase-pending.log').write_text('11 resource\n19 idle\n', encoding='ascii')
            (root / 'phase-native.log').write_text('14 native\n18 idle\n', encoding='ascii')
            result = summarize(root, 8, 10, 20)
        self.assertEqual(result, dict(worker_received_to_process_seconds=2,
            worker_received_to_native_seconds=6, process_window_seconds=10, native_execution_seconds=4,
            resource_wait_seconds=4, other_execution_seconds=2))

    def test_no_deadline_outside_worker(self):
        with patch.dict(os.environ,{},clear=True):
            self.assertIsNone(remaining_time())
            self.assertIsNone(check_deadline(reserve_seconds=1))

    def test_shared_monotonic_deadline_and_publication_reserve(self):
        with patch.dict(os.environ,REMOTE_COMPUTE_DEADLINE='100'),patch('remote_compute.deadline.time.monotonic',return_value=98):
            self.assertEqual(remaining_time(),2)
            self.assertEqual(check_deadline(reserve_seconds=1),2)
            with self.assertRaises(TimeoutError):check_deadline(reserve_seconds=2)
        with patch.dict(os.environ,REMOTE_COMPUTE_DEADLINE='100'),patch('remote_compute.deadline.time.monotonic',return_value=101):
            self.assertEqual(remaining_time(),0)
            with self.assertRaises(TimeoutError):check_deadline()

    def test_reserve_is_finite_and_nonnegative(self):
        for reserve in (-1,float('inf'),float('nan')):
            with self.subTest(reserve=reserve),self.assertRaises(ValueError):
                check_deadline(reserve_seconds=reserve)

    def test_attempt_is_persisted_before_an_item_can_execute(self):
        from remote_compute.execution import _map_items
        with scratch()as root:
            marker=Path(root)/'attempt'
            def started(index):
                time.sleep(.05);marker.write_text(str(index))
            def item(function,value,kwargs):return marker.read_text()
            with patch.dict(os.environ,dict(REMOTE_COMPUTE_WORKER='1',REMOTE_COMPUTE_URL='',REMOTE_COMPUTE_MAX_CPUS='1',REMOTE_COMPUTE_DEADLINE='')),\
                    patch('remote_compute.execution._process_item',side_effect=item):
                self.assertEqual(_map_items('unused:item',[0],on_start=started),['0'])

    def test_hard_timeout_preserves_results_and_distinguishes_unstarted_items(self):
        with scratch()as root,provider(Path(root)/'worker',cpus=1)as(worker,client):
            task=python_map('business:evaluate',[0,1,2],max_cpus=1,timeout=3,outputs=['completed.json'])
            result=client.run(task,files={'business.py':BUSINESS})
            self.assertEqual(result['status'],'timed_out',result.get('stderr'))
            self.assertEqual(json.loads(result['files']['completed.json']),dict(value=0))
            manifest=json.loads(result['files']['.dco-results.json'])
            self.assertEqual(manifest['status'],'timed_out')
            self.assertEqual(manifest['completed'],1)
            self.assertEqual([i['status']for i in manifest['item_states']],['succeeded','interrupted','not_started'])
            self.assertEqual(manifest['items'][0]['value'],0)
            self.assertEqual(worker.health()['used_cpus'],0)
            self.assertAlmostEqual(result['deadline']-result['execution_started'],3,delta=.2)

    def test_expired_initial_grant_does_not_start_a_command(self):
        with scratch()as root,provider(Path(root)/'worker',cpus=1)as(worker,client):
            task=python_map('business:evaluate',[0],max_cpus=1,timeout=1,outputs=['completed.json'])
            with patch('remote_compute.worker.PREPARATION_TIMEOUT', .1), \
                    patch.object(worker,'_automatic_grant',side_effect=lambda job:time.sleep(.2)):
                result=client.run(task,files={'business.py':BUSINESS})
            self.assertEqual(result['status'],'timed_out')
            self.assertEqual(result['error_kind'],'startup_timeout')
            self.assertEqual(result['preparation_stage'],'resource_grant')
            self.assertNotIn('execution_started',result)
            self.assertEqual(result['steps'],[])
            self.assertNotIn('completed.json',result['files'])
            manifest=json.loads(result['files']['.dco-results.json'])
            self.assertEqual(manifest['item_states'],[dict(index=0,status='not_started',finished=manifest['item_states'][0]['finished'])])
            self.assertEqual(worker.health()['used_cpus'],0)

    def test_fixed_memory_short_batch_gets_full_execution_time_after_preparation(self):
        with scratch() as root, provider(Path(root)/'worker',cpus=3) as (worker,client):
            task=python("from pathlib import Path; Path('result.txt').write_text('ready')",
                        max_cpus=1,memory_mb=192,timeout=.8,outputs=['result.txt'])
            with patch.object(worker,'_automatic_grant',side_effect=lambda job:time.sleep(1.0)):
                with ThreadPoolExecutor(9) as senders:
                    ids=list(senders.map(lambda _:client.submit(task)['id'],range(9)))
                results=[client.wait(i,wait_timeout=20) for i in ids]
            self.assertEqual({r['status'] for r in results},{'succeeded'},results)
            for identity,result in zip(ids,results):
                self.assertGreaterEqual(result['preparation_seconds'],1.0)
                self.assertAlmostEqual(result['deadline']-result['execution_started'],.8,delta=.02)
                self.assertEqual(client.fetch(identity)['files']['result.txt'],b'ready')

    def test_kernel_preparation_does_not_spend_execution_time(self):
        business = """import time
from remote_compute.deadline import check_deadline, preparation_window
def run():
    with preparation_window(2.5):
        time.sleep(1.25)
        check_deadline()
    return 'started with an independent execution budget'
"""
        with scratch() as root, provider(Path(root)/'worker') as (_,client):
            result=client.run(python_task('business:run',max_cpus=1,timeout=1),
                              files={'business.py':business})
            self.assertEqual(result['status'],'succeeded',result.get('stderr'))
            self.assertGreaterEqual(result['kernel_preparation_seconds'],1.2)
            self.assertEqual(json.loads(result['files']['.dco-results.json'])['completed'],1)

    def test_cooperative_boundary_publishes_paid_prefix_before_hard_limit(self):
        with scratch()as root,provider(Path(root)/'worker',cpus=1)as(worker,client):
            task=python_map('business:cooperative',[0],max_cpus=1,timeout=4,outputs=['paid-prefix.json'])
            result=client.run(task,files={'business.py':BUSINESS})
            self.assertEqual(result['status'],'failed',result.get('stderr'))
            prefix=json.loads(result['files']['paid-prefix.json'])
            self.assertGreater(len(prefix['paid']),0)
            self.assertEqual(prefix['paid'],list(range(1,len(prefix['paid'])+1)))
            self.assertGreater(prefix['remaining'],0)
            self.assertLessEqual(prefix['remaining'],1)
            manifest=json.loads(result['files']['.dco-results.json'])
            self.assertEqual(manifest['item_states'][0]['status'],'failed')
            self.assertIn('execution deadline reached',manifest['items'][0]['error'])
            self.assertEqual(worker.health()['used_cpus'],0)

    def test_cancel_keeps_completed_result_and_finalizes_each_item(self):
        with scratch()as root,provider(Path(root)/'worker',cpus=1)as(worker,client):
            task=python_map('business:evaluate',[0,1,2],max_cpus=1,timeout=15,outputs=['completed.json'])
            identity=client.submit(task,files={'business.py':BUSINESS})['id']
            until(lambda:(worker.root/'jobs'/identity/'input/active').is_file(),seconds=10)
            client.cancel(identity);client.wait(identity)
            result=client.fetch(identity)
            self.assertEqual(result['status'],'cancelled')
            manifest=json.loads(result['files']['.dco-results.json'])
            self.assertEqual(manifest['status'],'cancelled')
            self.assertEqual(manifest['completed'],1)
            self.assertEqual([i['status']for i in manifest['item_states']],['succeeded','interrupted','not_started'])
            self.assertEqual(json.loads(result['files']['completed.json']),dict(value=0))
            self.assertEqual(worker.health()['used_cpus'],0)

    def test_cancel_during_finalization_matches_receipt_without_losing_success(self):
        from remote_compute.execution import finish_execution
        with scratch()as root,provider(Path(root)/'worker',cpus=1)as(worker,client):
            finalized=[]
            def finish(path,status,error=None,*,spec=None):
                finish_execution(path,status,error,spec=spec);finalized.append(status)
                if status=='succeeded':
                    identity=Path(path).parent.parent.name
                    worker.cancel(identity)
            with patch('remote_compute.execution.finish_execution',side_effect=finish):
                result=client.run(python_map('business:evaluate',[0],max_cpus=1),files={'business.py':BUSINESS})
            self.assertEqual(result['status'],'cancelled')
            manifest=json.loads(result['files']['.dco-results.json'])
            self.assertEqual(manifest['status'],'cancelled')
            self.assertEqual(manifest['items'][0]['value'],0)
            self.assertEqual(manifest['item_states'][0]['status'],'succeeded')
            self.assertEqual(finalized,['succeeded','cancelled'])
            self.assertEqual(worker.health()['used_cpus'],0)

    def test_finalization_failure_is_reported_and_releases_resources(self):
        with scratch()as root,provider(Path(root)/'worker',cpus=1)as(worker,client):
            with patch('remote_compute.execution.finish_execution',side_effect=RuntimeError('finalization injected failure')):
                result=client.run(python_map('business:evaluate',[0],max_cpus=1),files={'business.py':BUSINESS})
            self.assertEqual(result['status'],'succeeded')
            self.assertEqual(result['execution_state_error'],'finalization injected failure')
            self.assertEqual(json.loads(result['files']['.dco-results.json'])['items'][0]['value'],0)
            self.assertEqual(worker.health()['used_cpus'],0)


if __name__=='__main__':unittest.main()
