"""Platform is a public execution constraint, not a provider or tool alias."""
from pathlib import Path
import sys
import threading
from types import SimpleNamespace
import unittest
import uuid

from remote_compute import Client, Pool, RemoteError, command, python_map, python_call, python_task
from remote_compute.client import Node
from remote_compute.coordinator import Coordinator, _matching_pass
from remote_compute.policy import validate_platform
from remote_compute.worker import validate_task
from test_compute import provider, scratch
from test_center import center


class PlatformRoutingTests(unittest.TestCase):
    def state(self):
        pool = Pool.__new__(Pool)
        pool.condition = threading.Condition()
        pool.nodes = []
        for name, platform, cpus, tags in [('a', 'linux', 4, ['ng47']), ('b', 'win32', 2, ['ml'])]:
            node = Node(name, SimpleNamespace(url=name))
            node.health = dict(platform=platform, cpus=cpus, cpu_limit=cpus, tags=tags,
                memory_capacity_mb=8192, memory_available_mb=8192, host_memory_available_mb=8192,
                tools={'python': dict(available=True)}, python_backends=['cpu', 'ml'])
            pool.nodes.append(node)
        state = Coordinator.__new__(Coordinator)
        state.pool, state.condition, state.jobs = pool, threading.Condition(), {}
        return state

    def test_platform_validation_and_constructor_forwarding(self):
        for value in (None, 'linux', 'win32', 'darwin', 'freebsd14'):
            self.assertEqual(validate_platform(value), value)
        for value in ('', 'Linux', 'linux,win32', 'linux/path', True, [], 32, 'x'*33):
            with self.subTest(value=value), self.assertRaises(ValueError):
                validate_platform(value)
        self.assertNotIn('platform', command(['echo']))
        for builder, args in [(command, (['echo'],)), (python_map, ('m:f', [1])),
                              (python_call, ('m:f',)), (python_task, ('m:f',))]:
            task = builder(*args, platform='linux', max_cpus=64)
            self.assertEqual(validate_task(task, 64, allow_auto=True)['platform'], 'linux')
            self.assertEqual(task['max_cpus'], 64)
        with self.assertRaises(ValueError):
            validate_task(dict(command(['echo']), platform=[]), 64, allow_auto=True)

    def test_mixed_platform_cache_keeps_exact_matching_sets(self):
        state = self.state()
        matching = _matching_pass(state.pool)
        task = command(['${tool:python}'])
        for platform, expected in [('linux', ['a']), ('win32', ['b']), (None, ['a', 'b']), ('darwin', [])]:
            for _ in range(2):
                self.assertEqual([n.name for n in matching(dict(task, platform=platform))], expected)

    def test_aggregate_discovery_and_admission_respect_platform(self):
        state = self.state()
        self.assertEqual(state.capabilities()['platforms'], ['linux', 'win32'])
        for platform, cpus, tags in [('linux', 4, ['ng47']), ('win32', 2, ['ml']), ('darwin', 0, [])]:
            view = state.capabilities(platform=platform)
            self.assertEqual(view['cpu']['capacity'], cpus)
            self.assertEqual(view['tags'], tags)
            self.assertEqual(view['platforms'], [platform] if cpus else [])
            offer = state.admission(dict(project='test', platform=platform))
            self.assertEqual(offer['capable_cpus'], cpus)
            self.assertEqual(offer['target_cpus'], cpus)
            self.assertEqual(offer['platform'], platform)

    def test_platform_survives_accelerator_fallback(self):
        state = self.state()
        request = dict(project='test', platform='linux', accelerator=dict(kind='cuda', fallback_after_seconds=0))
        offer = state.admission(request)
        self.assertEqual(offer['capable_cpus'], 0)
        self.assertEqual(offer['fallback_capable_cpus'], 4)
        task = command(['${tool:python}'], platform='linux', accelerator=request['accelerator'])
        job = dict(task=task, result=dict(status='queued', submitted=0))
        state._save = lambda job: None
        state._fallback_accelerator(job)
        self.assertIsNone(task['accelerator'])
        self.assertEqual([n.name for n in state.pool._matching(task)], ['a'])

    def test_worker_rejects_mismatch_before_materializing_or_executing(self):
        wrong = 'linux' if sys.platform != 'linux' else 'win32'
        with scratch() as directory, provider(directory) as (worker, client):
            identity = uuid.uuid4().hex
            with self.assertRaisesRegex(RemoteError, 'platform differs'):
                client.submit(command(['${tool:python}', '-c', 'raise RuntimeError("executed")'],
                                      platform=wrong), job_id=identity)
            self.assertFalse((worker.root/'jobs'/identity).exists())
            self.assertEqual(client.statuses([identity])[identity]['status'], 'unknown')
            result = client.run(command(['${tool:python}', '-c', 'import sys;print(sys.platform)'],
                                        platform=sys.platform), job_id=identity)
            self.assertEqual(result['status'], 'succeeded', result)
            self.assertEqual(result['stdout'].strip(), sys.platform)

    def test_public_http_routing_discovery_and_frozen_request_identity(self):
        with scratch() as directory:
            root = Path(directory)
            with provider(root/'worker', cpus=2) as (_, remote), Pool([remote]) as pool:
                with center(pool, root/'center') as (state, operator):
                    client = Client(operator.url)
                    self.assertIn('task_platform', client.capabilities()['capabilities'])
                    self.assertEqual(client.capabilities()['platforms'], [sys.platform])
                    self.assertEqual(client.capabilities(platform=sys.platform)['cpu']['capacity'], 2)
                    self.assertEqual(client.capabilities(platform='darwin')['cpu']['capacity'], 0)
                    self.assertEqual(client.admission('test', platform=sys.platform)['capable_cpus'], 2)
                    for query in ('?platform=', '?platform=linux&platform=win32', '?platform=Linux'):
                        with self.assertRaises(RemoteError):
                            client.request('/capabilities'+query)
                    identity = uuid.uuid4().hex
                    task = command(['${tool:python}', '-c', 'import sys;print(sys.platform)'],
                                   platform=sys.platform, max_cpus=64, project='test')
                    accepted = client.submit(task, job_id=identity)
                    self.assertEqual(client.submit(task, job_id=identity)['id'], identity)
                    result = client.wait(identity)
                    self.assertEqual(result['status'], 'succeeded', result)
                    self.assertEqual(result['stdout'].strip(), sys.platform)
                    self.assertEqual(state.jobs[identity]['task']['platform'], sys.platform)
                    self.assertEqual(result['request_sha256'], accepted['request_sha256'])
                    self.assertNotIn('assigned_node', result)
                    # No provider is an explicit routing failure, never execution on another OS.
                    unmatched = client.submit(dict(task, platform='darwin'), job_id=uuid.uuid4().hex)['id']
                    rejected = client.wait(unmatched)
                    self.assertEqual(rejected['status'], 'failed', rejected)
                    self.assertIn('platform', rejected['error'])
                    self.assertIsNone(state.jobs[unmatched]['node'])


if __name__ == '__main__':
    unittest.main()
