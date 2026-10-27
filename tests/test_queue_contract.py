"""Project queue preparation, known-ID collection and old-service compatibility."""
from pathlib import Path
import unittest
from unittest.mock import patch

from remote_compute import Client, Pool, RemoteError, RequestCancelled
from remote_compute.client import PoolBusy
import test_center as centers
from test_compute import provider, python, scratch


class ReadyQueueTests(unittest.TestCase):
    def test_ready_depth_does_not_change_cpu_fairness_or_reservations(self):
        state = centers.AdmissionPlanningTests().center([16])
        state.jobs = {name: dict(task=dict(steps=[['echo']], cpus=4, memory_mb=64, project=name),
                                node=None, result=dict(status='queued')) for name in ('A', 'B')}
        before = state.admission(dict(project='A', cpus=4))
        value = state.admission(dict(project='A', cpus=4, ready_tasks=256))
        for field in ('target_cpus', 'capable_cpus', 'running_tasks'):
            self.assertEqual(value[field], before[field])
        self.assertEqual((value['in_flight'], value['max_in_flight'], value['project_active_tasks']), (256, 256, 1))
        self.assertEqual(value['queued_tasks'], 256-value['running_tasks'])
        self.assertEqual(value['queue_scope'], 'project')
        self.assertEqual(state.pool.nodes[0].reserved, 0)
        self.assertEqual(state.jobs['A']['task']['cpus'], 4)

    def test_queue_has_one_project_counter_and_excludes_terminal_receipts(self):
        state = centers.AdmissionPlanningTests().center([8])
        state.jobs = {str(i): dict(task=dict(steps=[['echo']], cpus=1, memory_mb=64, project=project),
                                  node=None, result=dict(status=status))
                      for i, (project, status) in enumerate([('A','queued'), ('A','running'),
                                                             ('A','succeeded'), ('B','queued')])}
        value = state.admission(dict(project='A', ready_tasks=1))
        self.assertEqual(value['project_active_tasks'], 2)
        self.assertEqual((value['in_flight'], value['max_in_flight']), (1, 1))
        self.assertEqual(len(state.jobs), 4)  # Reducing a window never cancels admitted work.

    def test_invalid_ready_depth_rejected_before_transport(self):
        state = centers.AdmissionPlanningTests().center([8])
        client = Client()
        for value in (0, -1, True, 1.5, 4097):
            with self.subTest(value=value), patch.object(client, 'request') as request:
                with self.assertRaises(ValueError):
                    client.admission('A', ready_tasks=value)
                request.assert_not_called()
                with self.assertRaises(ValueError):
                    state.admission(dict(project='A', ready_tasks=value))

    def test_new_sdk_keeps_ready_queue_on_old_center_without_extra_poll(self):
        client = Client()
        original = dict(running_tasks=2, queued_tasks=2, in_flight=4, max_in_flight=8, target_cpus=8)
        with patch.object(client, 'request', return_value=original.copy()) as request:
            window = client.window('A', cpus=4, ready_tasks=256)
            self.assertEqual((window.maximum, window.current()), (256, 256))
            self.assertEqual(window.value['target_cpus'], 8)
            self.assertEqual(window.value['ready_source'], 'sdk_legacy')
            self.assertEqual(request.call_count, 1)

    def test_unconsumed_terminal_and_uncertain_ids_keep_queue_credits(self):
        client = Client()
        value = dict(running_tasks=2, queued_tasks=2, in_flight=4, max_in_flight=8)
        with patch.object(client, 'request', return_value=value):
            window = client.window('A', cpus=4, ready_tasks=4)
        pending = {'running': 'a'*32, 'terminal_unpublished': 'b'*32,
                   'submission_unknown': 'c'*32, 'queued': 'd'*32}
        self.assertEqual(window.available(len(pending)), 0)
        del pending['terminal_unpublished']  # Only after durable publication and original-ID cleanup.
        self.assertEqual(window.available(len(pending)), 1)
        window.value['in_flight'] = 2  # A reduced window drains without cancelling existing IDs.
        self.assertEqual(window.available(len(pending)), 0)
        self.assertEqual(len(pending), 3)
        for invalid in (-1, True, 1.5):
            with self.assertRaises(ValueError):
                window.available(invalid)


class BatchCollectionTests(unittest.TestCase):
    def test_sdk_chunks_only_supplied_ids_without_global_reads(self):
        client = Client()
        client._capabilities = ['batch_status']
        calls = []
        def request(path, data):
            self.assertEqual(path, '/jobs/status')
            calls.append(data['ids'])
            return {identity: dict(id=identity, status='running') for identity in data['ids']}
        ids = ['%032x' % value for value in range(257)]
        with patch.object(client, 'request', side_effect=request):
            values = client.statuses(ids + [ids[0]])
        self.assertEqual(len(values), 257)
        self.assertEqual([len(batch) for batch in calls], [128, 128, 1])

    def test_legacy_missing_is_observation_and_transport_error_propagates(self):
        client = Client()
        client._capabilities = []
        missing = RemoteError('Original missing')
        missing.status_code = 404
        with patch.object(client, 'status', side_effect=missing) as status:
            values = client.statuses(['a'*32])
            self.assertEqual(values['a'*32]['status'], 'unknown')
            status.assert_called_once_with('a'*32, brief=True)
        failure = RemoteError('connection refused')
        with patch.object(client, 'status', side_effect=failure):
            with self.assertRaises(RemoteError) as caught:
                client.statuses(['a'*32])
            self.assertIs(caught.exception, failure)

    def test_invalid_ids_and_empty_list_do_not_send_requests(self):
        client = Client()
        with patch.object(client, 'request') as request:
            self.assertEqual(client.statuses([]), {})
            for ids in (['abc'], ['g'*32], [True]):
                with self.assertRaises(ValueError):
                    client.statuses(ids)
            request.assert_not_called()

    def test_wait_any_returns_terminal_brief_and_never_resubmits_or_cancels(self):
        client = Client()
        states = {'a'*32: dict(id='a'*32, status='running'),
                  'b'*32: dict(id='b'*32, status='succeeded')}
        with patch.object(client, 'statuses', return_value=states), \
                patch.object(client, 'submit') as submit, patch.object(client, 'cancel') as cancel:
            self.assertEqual(client.wait_any(states), {'b'*32: states['b'*32]})
            submit.assert_not_called()
            cancel.assert_not_called()

    def test_wait_cancel_timeout_and_missing_preserve_original_work(self):
        client = Client()
        identity = 'a'*32
        with patch.object(client, 'statuses') as statuses:
            with self.assertRaises(RequestCancelled):
                client.wait_any([identity], cancel=lambda: True)
            statuses.assert_not_called()
        with patch.object(client, 'statuses', return_value={identity: dict(id=identity,status='running')}):
            with self.assertRaisesRegex(RemoteError, 'Batch wait limit'):
                client.wait_any([identity], poll=.01, max_poll=.01, wait_timeout=.02)
        with patch.object(client, 'statuses', return_value={identity: dict(id=identity,status='unknown')}):
            with self.assertRaises(RemoteError) as caught:
                client.wait_any([identity])
            self.assertEqual((caught.exception.status_code, caught.exception.missing_ids), (404, [identity]))

    def test_error_fields_are_stable_for_http_and_transport_callers(self):
        for error in (RemoteError('offline'), PoolBusy('busy'), RequestCancelled('stopped')):
            self.assertIsNone(error.status_code)
            self.assertFalse(error.transport_retryable)

    def test_http_batch_uses_only_known_originals_and_preserves_final_receipt(self):
        with scratch() as root, provider(Path(root)/'worker') as (_, worker), \
                Pool([worker]) as pool, centers.center(pool, Path(root)/'center') as (scheduler, operator):
            identity = operator.submit(python("print('retained-original')"))['id']
            operator.wait(identity)
            project = Client(operator.url)
            project.health(brief=True)
            original_path = scheduler.root/'jobs'/identity/'result.json'
            before = original_path.stat().st_mtime_ns
            ids = [identity] + ['%032x' % value for value in range(127)]
            original_request = project.request
            calls = []
            def request(path, *args, **kwargs):
                calls.append(path)
                return original_request(path, *args, **kwargs)
            with patch.object(project, 'request', side_effect=request), \
                    patch.object(type(scheduler.jobs), '__iter__', side_effect=AssertionError('Global history scan')):
                values = project.statuses(ids)
            self.assertEqual(calls, ['/jobs/status'])
            self.assertEqual(len(values), 128)
            self.assertEqual(values[identity]['status'], 'succeeded')
            self.assertNotIn('worker_url', values[identity])
            self.assertNotIn('assigned_node', values[identity])
            self.assertNotIn('stdout', values[identity])
            self.assertTrue(all(values[value]['status'] == 'unknown' for value in ids[1:]))
            self.assertEqual(original_path.stat().st_mtime_ns, before)
            self.assertEqual(len(scheduler.jobs), 1)
            with self.assertRaises(RemoteError) as caught:
                project.request('/jobs/status', {'ids': ids+['f'*32]})
            self.assertEqual(caught.exception.status_code, 400)
            receipt = project.fetch(identity, delete=False)
            self.assertEqual(receipt['stdout'].strip(), 'retained-original')
            self.assertIn('cpu_seconds', receipt)
            self.assertEqual(receipt['id'], identity)
            project.delete(identity)
            self.assertEqual(project.statuses([identity])[identity]['status'], 'unknown')


if __name__ == '__main__':
    unittest.main()
