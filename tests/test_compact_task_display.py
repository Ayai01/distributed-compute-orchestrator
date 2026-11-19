import threading
import unittest

from remote_compute.task_tags import compact_tag
from remote_compute.view import format_status
import test_center as center_fixtures


class CompactTaskDisplayTests(unittest.TestCase):
    def test_task_label_output_is_independent_of_project(self):
        text = 'design check | s4101 | 4 samples'
        for project in ('project-a', 'project-b'):
            job = {'project': project, 'label': text}
            self.assertEqual(compact_tag(job, 48), text)
            self.assertEqual(job['label'], text)

    def test_task_table_spends_the_removed_share_priority_width_on_tag(self):
        job = {'id': '1234'+'a'*28, 'project': 'RGCN', 'label': 'FSA -KCL s19b2 ep:96 ~600s',
            'cpus': 4, 'max_cpus':128, 'status': 'running', 'memory_mb': 4096, 'total_steps': 1}
        value = {'role': 'scheduler', 'nodes': [{'name': 'GPU-A', 'capacity': 16, 'active_jobs': [job]}]}
        for width in (80, 110, 139, 140, 160, 200):
            with self.subTest(width=width):
                text = '\n'.join(format_status(value, width=width).fit(36))
                self.assertNotIn('Share/Pri', text)
                self.assertIn('CPU/上限',text)
                self.assertIn('4/128',text)
                if width >= 140:
                    self.assertIn('ep:96', text)


class GPUQueuePriorityTests(unittest.TestCase):
    def test_gpu_blocked_waiter_and_ready_serial_cpu_work_are_distinguished(self):
        state = center_fixtures.AdmissionPlanningTests().center([16]); node = state.pool.nodes[0]; node.reserved = 4
        node.health['accelerators'] = [{'kind': 'cuda', 'count': 1, 'memory_mb': 8192, 'tags': ['gpu:training']}]
        accelerator = {'kind': 'cuda', 'count': 1, 'memory_mb': 2048, 'requires': ['gpu:training']}
        self.assertIsNotNone(node.reserve_accelerator(accelerator, 'running'))
        def job(held=None, gpu=False, cpus=0):
            task = {'steps': [['echo']], 'cpus': cpus, 'max_cpus': 16, 'memory_mb': 64, 'project': 'A'}
            if gpu:
                task['accelerator'] = accelerator
            return {'task': task, 'auto_cpus': True, 'elastic_cpus': True, 'node': held,
                'cancel': threading.Event(), 'result': {'status': 'running' if held else 'queued'}, 'desired_cpus': 16}
        state.jobs = {'running': job(node, True, 4), 'gpu-waiter': job(gpu=True)}
        self.assertEqual(state._cooperative_cpu_target('running', 16, state.project_resources()), 16)
        state.jobs['cpu-ready'] = job()
        self.assertEqual(state._cooperative_cpu_target('running', 16, state.project_resources()), 4)
        state.jobs['cpu-ready']['node'] = node
        state.jobs['cpu-ready']['task']['cpus'] = 1
        state.jobs['cpu-ready']['result']['status'] = 'running'
        targets = [state._cooperative_cpu_target(key, 16, state.project_resources()) for key in ('running', 'cpu-ready')]
        self.assertEqual(targets, [8, 8])


if __name__ == '__main__':
    unittest.main()
