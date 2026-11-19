"""One ordinary project package reaches the same providers in admission and dispatch."""
from contextlib import contextmanager
from functools import partial
import hashlib
from pathlib import Path
import sys
import unittest
import uuid
from unittest.mock import patch

from remote_compute import Client, Pool, RemoteError, command
from remote_compute.bundle import task_bundle
from remote_compute.packages import PACKAGES, project_package, provider_environment
from test_center import center
from test_compute import provider, scratch


def profile(*names):
    packages = [PACKAGES[name] for name in names]
    return dict(tags=sorted({tag for package in packages for tag in package.requirements}
                            | {package.accelerator for package in packages if package.accelerator}),
                tools={name: {'available': True} for package in packages for name in package.tools},
                shared=sorted({name for package in packages for name in package.shared}),
                python_backends=['cpu', 'ml'])


@contextmanager
def packaged_worker(root, *names, cpus=1, **options):
    backends = {'ml': [sys.executable]}
    backends.update({device['kind']: [sys.executable] for device in options.get('accelerators', ())})
    with provider(root, cpus=cpus, python_runtimes=backends, **options) as (worker, client):
        health = worker.health
        with patch.object(worker, 'health', side_effect=lambda **kwargs: {
                **health(**kwargs), **profile(*names), 'python_backends': sorted(worker.python_runtimes)}):
            yield worker, client


class CapabilityPackageTests(unittest.TestCase):
    def test_rtl_package_supplies_the_bound_firtool(self):
        task = dict(env={}, requires=['ml'], _capability_package='rtl')
        for platform, executable, expected in (
            ('linux', '/opt/eda/firtool/firtool', '/opt/eda/firtool'),
            ('win32', 'D:/EDA Tools/CIRCT/firtool.exe', r'D:\EDA Tools\CIRCT'),
        ):
            health = dict(platform=platform, tools={'firtool': {'available': True, 'argv': [executable]}})
            with self.subTest(platform=platform):
                self.assertEqual(provider_environment(task, health)['CHISEL_FIRTOOL_PATH'], expected)
                self.assertEqual(provider_environment(dict(task, env={'CHISEL_FIRTOOL_PATH': 'explicit'}),
                                                      health)['CHISEL_FIRTOOL_PATH'], 'explicit')

    def test_release_aliases_produce_one_package(self):
        groups = {
            'python': [[], ['python'], ['ml']],
            'ng47': [['spice'], ['ng47'], ['ngspice47', 'python']],
            'hsp': [['hspice'], ['hsp']],
            'onnx': [['onnx'], ['onnx-int8'], ['onnx', 'onnx-int8']],
            'rtl': [['rtl'], ['chisel'], ['rtl', 'shared:arh', 'single-core-phase-handoff'],
                    ['rtl', 'onnx', 'onnx-int8']],
            'timing': [['timing'], ['yosys', 'opensta'], ['make', 'g++', 'opensta', 'iverilog']],
        }
        for package, expressions in groups.items():
            for requires in expressions:
                with self.subTest(requires=requires):
                    self.assertEqual(project_package(requires), (package, None))
        for requires in (['rtl', 'layout'], ['cpu'], ['cpu', 'rtl'], ['single-core-phase-handoff'],
                         ['rtl', 'pe64'], ['elf-aapcs64'], ['custom'], ['rtl', 'shared:private']):
            with self.subTest(requires=requires), self.assertRaises(ValueError):
                project_package(requires)
        pack, accelerator = project_package(['npu'], {'count': 2, 'memory_mb': 256})
        self.assertEqual(pack, 'npu')
        self.assertEqual(accelerator, dict(kind='npu', count=2, memory_mb=256, requires=[]))
        self.assertEqual(project_package(['rtl', 'ml', 'cuda'], {'kind': 'cuda'})[0], 'cuda')
        with self.assertRaisesRegex(ValueError, 'resource quantities'):
            project_package(['cuda'], {'kind': 'cuda', 'requires': ['gpu:training']})

    def test_real_http_quote_dispatch_and_original_recovery(self):
        with scratch() as folder:
            root = Path(folder)
            with packaged_worker(root / 'basic', 'python', cpus=4) as (_, basic), \
                    packaged_worker(root / 'full', 'rtl', cpus=2) as (_, full), \
                    Pool([dict(name='basic', client=basic), dict(name='full', client=full)]) as pool:
                with center(pool, root / 'center', paused=True) as (state, operator):
                    client = Client(operator.url, operator.token)
                    catalog = client.capabilities()
                    self.assertEqual(catalog['default_package'], 'python')
                    self.assertEqual(set(catalog['capability_cpus']), set(PACKAGES))
                    for field in ('tags', 'tools', 'accelerators', 'nodes', 'platforms'):
                        self.assertNotIn(field, catalog)
                    self.assertNotIn('cpu', catalog['capability_cpus'])
                    self.assertNotIn('spice', catalog['capability_cpus'])
                    self.assertEqual(catalog['capability_cpus']['python']['capacity'], 6)
                    self.assertEqual(catalog['capability_cpus']['onnx']['capacity'], 2)
                    self.assertEqual(catalog['capability_cpus']['rtl']['capacity'], 2)
                    self.assertEqual(client.admission('audit', requires=['npu'])['in_flight'], 0)
                    argv = ['${tool:python}', '-c', 'print(42)']
                    old_aliases = ['rtl', 'shared:arh', 'single-core-phase-handoff']
                    self.assertEqual(client.admission('audit', requires=old_aliases)['capable_cpus'], 2)
                    self.assertEqual(client.admission('audit', requires=['rtl'])['capable_cpus'], 2)
                    self.assertEqual(client.admission('audit')['capable_cpus'], 6)
                    for options in (
                        dict(requires=['rtl', 'pe64']),
                        dict(requires=['rtl', 'layout']),
                        dict(requires=['rtl'], env={'INPUT': '${shared:private}'}),
                        dict(requires=['rtl'], env={'ENGINE': '${tool:opensta}'}),
                        dict(requires=['rtl'], cache_ref='a' * 32),
                        dict(requires=['rtl'], native_cache=dict(storage='b' * 64, platform=sys.platform,
                                                                 architecture='aarch64')),
                        dict(requires=['cuda'], accelerator={'kind': 'cuda', 'requires': ['gpu:training']}),
                    ):
                        identity = uuid.uuid4().hex
                        with self.subTest(options=options), self.assertRaises(RemoteError) as rejected:
                            client.submit_bundle(identity, task_bundle(command(argv, **options)))
                        self.assertEqual(rejected.exception.status_code, 400)
                        self.assertEqual(client.statuses([identity])[identity]['status'], 'unknown')
                    payload = task_bundle(command(argv, requires=old_aliases, max_cpus=1, project='audit'))
                    identity = uuid.uuid4().hex
                    submitted = client.submit_bundle(identity, payload)
                    self.assertEqual(submitted['capability_package'], 'rtl')
                    self.assertEqual([node.name for node in pool._matching(state.jobs[identity]['task'])], ['full'])
                    self.assertEqual(state.jobs[identity]['result']['request_sha256'], hashlib.sha256(payload).hexdigest())
                    state.close = partial(state.close, preserve_queued=True)
                with center(pool, root / 'center') as (state, operator):
                    client = Client(operator.url, operator.token)
                    result = client.wait(identity, wait_timeout=20)
                    self.assertEqual(result['status'], 'succeeded', result.get('error'))
                    self.assertEqual(result['stdout'].strip(), '42')
                    self.assertEqual(result['capability_package'], 'rtl')
                    self.assertNotIn('tools', result)
                    self.assertEqual(operator.status(identity)['assigned_node'], 'full')
                    self.assertEqual(client.submit_bundle(identity, payload)['id'], identity)

    def test_ng47_uses_standard_binding_and_restores_accepted_legacy_receipt(self):
        with scratch() as folder:
            root = Path(folder)
            with packaged_worker(root / 'worker', 'ng47', tools={'ng47': sys.executable}) as (_, remote), \
                    Pool([remote]) as pool, center(pool, root / 'center') as (state, operator):
                client = Client(operator.url, operator.token)
                raw = command(['${tool:ngspice}', '-c', 'print(47)'], requires=['ng47'], max_cpus=1)
                self.assertEqual(client.admission('AOP', requires=['ng47'])['capable_cpus'], 1)
                self.assertEqual(client.admission('rgcn-v3', requires=['ng47'])['capable_cpus'], 1)
                result = client.run(raw)
                self.assertEqual((result['status'], result['stdout'].strip()), ('succeeded', '47'))
                retained = state._restored_task(
                    command(['${tool:onnx}', '-c', 'pass'], requires=['cpu']),
                    {'capability_package': 'cpu', 'accelerator': None})
                self.assertEqual(retained['_capability_package'], 'cpu')
                self.assertEqual(retained['steps'][0][0], '${tool:onnx}')


if __name__ == '__main__':
    unittest.main()
