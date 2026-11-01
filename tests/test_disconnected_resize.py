"""A missing online project quota does not end accepted worker computation."""
from types import SimpleNamespace
import threading
import time
import unittest
from unittest.mock import Mock

from remote_compute.coordinator import Coordinator
import test_center


class DisconnectedResizeTests(unittest.TestCase):
    def test_offline_projection_keeps_grant_then_reborrows_on_same_id(self):
        state = test_center.AdmissionPlanningTests().center([8])
        state.stopping = False
        state._publish = Mock()
        state._save = Mock()
        node = state.pool.nodes[0]
        node.health['capabilities'] = ['resize']
        node.reserved = 1
        node.reserved_memory_mb = 64
        node.live_jobs.add('original')
        node.client.resize = Mock(side_effect=lambda identity, cpus: dict(id=identity, cpus=cpus, memory_mb=64))
        task = dict(steps=[['echo']], cpus=1, max_cpus=8, memory_mb=64, project='Analog-Opt-Bench')
        job = dict(task=task, node=node, cancel=threading.Event(), elastic_cpus=True,
                   result=dict(id='original', status='running', cpus=1, memory_mb=64))
        state.jobs = {'original': job}
        node.error = 'Temporary TLS EOF'
        self.assertNotIn('Analog-Opt-Bench', state.project_resources())
        grant = state.resize('original', 4)
        self.assertEqual((grant['id'], grant['cpus'], grant['memory_mb']), ('original', 1, 64))
        self.assertEqual((node.reserved, task['cpus'], job['result']['status']), (1, 1, 'running'))
        node.client.resize.assert_not_called()
        node.error = ''
        self.assertEqual(state.resize('original', 4)['cpus'], 4)
        self.assertEqual(node.reserved, 4)
        self.assertEqual(list(state.jobs), ['original'])

    def test_polling_lease_retains_worker_until_authoritative_success(self):
        identity = 'b' * 32
        client = Mock()
        final = dict(id=identity, status='succeeded', cpu_seconds=7.25,
                     started=time.time() - 2, finished=time.time(), steps=[])
        client.status.side_effect = [dict(id=identity, status='running',
                                         resource_request=dict(request_id=1, cpus=4)), final, final]
        client.request.return_value = {'accepted': True}
        node = SimpleNamespace(client=client, name='worker', capacity=8, memory_capacity=1024,
                               health={'capabilities': ['resize']}, detached=False, enabled=True)
        task = dict(cpus=1, max_cpus=8, memory_mb=64, project='Analog-Opt-Bench')
        job = dict(node=node, task=task, cancel=threading.Event(), reattach=True,
                   result=dict(id=identity, status='running', submitted=time.time() - 10,
                               dispatched=time.time() - 3, cpus=1, cpu_seconds=None))
        state = Coordinator.__new__(Coordinator)
        state.condition = threading.Condition()
        state.stopping = False
        state.pool = SimpleNamespace(condition=threading.Condition(), _release=Mock())
        state.jobs = {identity: job}
        state.threads = {threading.current_thread()}
        state.project_resources = Mock(return_value={})
        state._save = Mock()
        state._retry = Mock()
        state._execute(job)
        self.assertEqual((job['result']['id'], job['result']['status'], job['result']['cpu_seconds']),
                         (identity, 'succeeded', 7.25))
        reply = client.request.call_args[0][1]
        self.assertEqual((reply['request_id'], reply['grant']['cpus']), (1, 1))
        client.submit_bundle.assert_not_called()
        client.cancel.assert_not_called()
        state._retry.assert_not_called()
        state.pool._release.assert_called_once_with(node, task, final, job_id=identity)


if __name__ == '__main__':
    unittest.main()
