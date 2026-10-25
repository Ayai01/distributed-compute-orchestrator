"""Capability changes follow the tools and paths the worker can actually use."""

from contextlib import redirect_stdout
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import patch

from remote_compute import Client, Pool, setup_tools, command
from remote_compute.cli import main
from remote_compute.worker import Worker
from remote_compute.policy import canonical_capabilities, clean_tool_snapshot
from test_compute import provider, scratch, until
from test_center import center


class CapabilityTests(unittest.TestCase):
    def test_benchmark_uses_available_semantic_numeric_runtime_before_legacy_or_core(self):
        for semantic in ('python_scientific', 'python_numpy', 'python_torch_pyg'):
            with self.subTest(semantic=semantic), scratch() as directory:
                bindings = {semantic: '/science/python', 'bench_python': '/legacy/python'}
                def probe(tools):
                    return tools, {name: {'available': True} for name in tools}
                with patch.object(Worker, '_probe_tools', side_effect=probe), \
                        provider(directory, tools=bindings) as (worker, _):
                    self.assertEqual(worker.benchmark_python, '/science/python')

    def test_benchmark_skips_unavailable_semantic_runtime_and_keeps_legacy_fallback(self):
        with scratch() as directory:
            def probe(tools):
                return tools, {name: {'available': name != 'python_scientific'} for name in tools}
            with patch.object(Worker, '_probe_tools', side_effect=probe), \
                    provider(directory, tools={'python_scientific': '/missing/python',
                                               'bench_python': '/legacy/python'}) as (worker, _):
                self.assertEqual(worker.benchmark_python, '/legacy/python')

    def test_semantic_and_legacy_templates_share_the_registered_binding(self):
        aliases = (('ngspice47', 'ng47'), ('circt-translate', 'circt'),
                   ('onnx_cpu', 'onnx'), ('aop_hspice_python', 'python_numpy'),
                   ('aop_python', 'python_numpy'), ('arh_python', 'python_numpy'),
                   ('rgcn_python', 'python_numpy'), ('bench_python', 'python_scientific'),
                   ('rgcn_train', 'python_torch_pyg'), ('rgcn_fiber', 'python_torch_pyg'))
        for legacy, semantic in aliases:
            for configured, requested in ((legacy, semantic), (semantic, legacy)):
                with self.subTest(configured=configured, requested=requested), scratch() as directory:
                    with provider(Path(directory) / 'worker', tools={configured: sys.executable}) as (_, client):
                        template = '${tool:' + requested + '}'
                        result = client.run(command([template, '-c', 'import os;print(os.environ["SELECTED"])'],
                            env={'SELECTED': template}, requires=[semantic]))
                        self.assertEqual(result['status'], 'succeeded', result)
                        self.assertEqual(Path(result['stdout'].strip()).resolve(), Path(sys.executable).resolve())

    def test_semantic_template_retains_multi_argument_tool_prefix(self):
        with scratch() as directory, provider(Path(directory) / 'worker',
                tools={'ngspice47': [sys.executable, '-I']}) as (_, client):
            result = client.run(command(['${tool:ng47}', '-c', 'print("prefix retained")'], requires=['ng47']))
            self.assertEqual(result['status'], 'succeeded', result)
            self.assertEqual(result['stdout'].strip(), 'prefix retained')

    def test_missing_alias_uses_an_available_binding_for_same_capability(self):
        with scratch() as directory, provider(Path(directory) / 'worker',
                tools={'ngspice47': str(Path(directory) / 'missing'), 'ng47': sys.executable}) as (_, client):
            result = client.run(command(['${tool:ngspice47}', '-c', 'print("ready")'], requires=['ng47']))
            self.assertEqual(result['status'], 'succeeded', result)
            self.assertEqual(result['stdout'].strip(), 'ready')

    def test_registered_punctuation_tool_executes_and_expands_in_environment(self):
        for name in ('clang++', 'compiler.v1'):
            with self.subTest(name=name), scratch() as directory:
                with provider(Path(directory) / 'worker', tools={name: sys.executable}) as (_, client):
                    template = '${tool:' + name + '}'
                    result = client.run(command([template, '-c', 'import os;print(os.environ["SELECTED"])'],
                                                env={'SELECTED': template}, requires=[name]))
                    self.assertEqual(result['status'], 'succeeded', result)
                    self.assertEqual(Path(result['stdout'].strip()).resolve(), Path(sys.executable).resolve())

    def test_restored_receipts_drop_engine_hashes_and_retired_aliases(self):
        value=dict(id='original',tools={'ng47_sha256_'+'x'*43:dict(available=True),
                   'ngspice47':dict(available=True,sha256='old',supporting_sha256={'engine':'old'})},
                   attempts=[dict(tools={'python':dict(available=True,sha256='old')})])
        self.assertTrue(clean_tool_snapshot(value))
        self.assertEqual(value,dict(id='original',tools={'ng47':dict(available=True)},
                                   attempts=[dict(tools={'python':dict(available=True)})]))
        self.assertFalse(clean_tool_snapshot(value))

    def test_capabilities_reject_engine_hashes_and_platform_selectors(self):
        for name in ('ng47_sha256_'+'x'*43, 'engine_hash_1234', 'tool_'+('a'*64),
                     'win32', 'linux', 'runtime_windows', 'ng47_sha-256_x', 'ng47_sha_256_x',
                     'linux64', 'win10', 'ubuntu', 'x86_64', 'aarch64'):
            with self.subTest(name=name), self.assertRaisesRegex(ValueError, 'semantic tool names'):
                canonical_capabilities([name])
        self.assertEqual(canonical_capabilities(['ngspice47','ng47','rgcn_python_linux']),
                         ['ng47','python_numpy'])

    def test_shared_runtime_profiles_use_every_compatible_provider(self):
        with scratch() as directory:
            root = Path(directory)
            with provider(root/'legacy', cpus=1, tools={'arh_python':sys.executable}) as (_, legacy), \
                    provider(root/'canonical', cpus=2, tools={'python_numpy':sys.executable}) as (_, canonical):
                with Pool([legacy, canonical]) as pool, center(pool, root/'center') as (_, operator):
                    project = Client(operator.url)
                    self.assertEqual(project.admission('runtime', requires=['rgcn_python'])['capable_cpus'], 3)
                    public = operator.request('/capabilities')
                    self.assertIn('python_numpy', public['tags'])
                    self.assertIn('python_numpy', public['tools'])
                    self.assertFalse({'arh_python','rgcn_python'} & set(public['tools']))
                    task = command(['${tool:aop_python}', '-c',
                                    'import os,pathlib,time;assert pathlib.Path(os.environ["PY"]).is_file();'
                                    'time.sleep(.35);print("ready")'],
                                   env={'PY':'${tool:rgcn_python}'}, requires=['python_numpy'], project='runtime')
                    ids = [operator.submit(task)['id'] for _ in range(3)]
                    results = [operator.wait(identity) for identity in ids]
                    self.assertTrue(all(r['status']=='succeeded' and r['stdout'].strip()=='ready' for r in results), results)
                    self.assertEqual(len({r['assigned_node'] for r in results}), 2)

    def test_tool_path_templates_only_route_to_ready_providers(self):
        with scratch() as directory:
            root = Path(directory)
            with provider(root/'missing') as (_, missing), \
                    provider(root/'ready', tools={'compiler.v1':sys.executable}) as (_, ready):
                with Pool([missing, ready]) as pool, center(pool, root/'center') as (_, client):
                    result = client.run(command(['${tool:python}', '-c',
                        'import os;print(os.environ["COMPILER"])'], env={'COMPILER':'${tool:compiler.v1}'}))
                    self.assertEqual(result['status'], 'succeeded', result)
                    self.assertEqual(Path(result['stdout'].strip()), Path(sys.executable))
                    self.assertEqual(result['worker_url'], ready.url)

    def test_client_normalizes_older_services_without_changing_bindings(self):
        response = dict(protocol=1, tags=['aop_python','arh_python','rgcn_fiber','rgcn_train'],
                        tools=['bench_python','python_scientific','onnx_cpu'], capabilities=['admission'])
        with patch.object(Client, 'request', return_value=response):
            result = Client().capabilities()
        self.assertEqual(result['tags'], ['python_numpy','python_torch_pyg'])
        self.assertEqual(result['tools'], ['onnx','python_scientific'])

    def test_runtime_binding_migration_preserves_an_accepted_job(self):
        with scratch() as directory, provider(Path(directory)/'worker',
                tools={'rgcn_fiber':sys.executable}) as (worker, client):
            task = command(['${tool:rgcn_fiber}', '-c',
                            'import time;time.sleep(.3);print("original")'])
            task['requires'] = ['rgcn_fiber']
            identity = client.submit(task)['id']
            until(lambda: client.status(identity)['status']=='running')
            before = client.status(identity)
            worker.control({'tools':{'python_torch_pyg':sys.executable,'rgcn_fiber':None}})
            result = client.wait(identity)
            self.assertEqual(result['status'], 'succeeded', result)
            self.assertEqual(result['stdout'].strip(), 'original')
            for key in ('id','request_sha256','started'):
                self.assertEqual(result[key], before[key])
            following = client.run(command(['${tool:rgcn_train}', '-c', 'print("next")'],
                                           requires=['python_torch_pyg']))
            self.assertEqual(following['status'], 'succeeded', following)
            self.assertEqual(following['stdout'].strip(), 'next')

    def test_rolling_worker_retains_registered_aliases_until_retired(self):
        with scratch() as directory, provider(Path(directory)/'worker',
                tools={'arh_python':sys.executable,'python_numpy':sys.executable}) as (worker, _):
            self.assertTrue({'arh_python','python_numpy'} <= set(worker.health()['tags']))
            worker.control({'tools':{'arh_python':None}})
            self.assertNotIn('arh_python', worker.health()['tags'])
            self.assertIn('python_numpy', worker.health()['tags'])

    def test_receipt_alias_merge_keeps_the_ready_canonical_binding(self):
        value = dict(tools={'onnx':dict(available=True,argv=['canonical']),
                           'onnx_cpu':dict(available=False,argv=['missing']),
                           'rgcn_python':dict(available=True,argv=['native']),
                           'rgcn_train':dict(available=True,argv=['training'])})
        self.assertTrue(clean_tool_snapshot(value))
        self.assertEqual(value['tools'], {'onnx':dict(available=True,argv=['canonical']),
            'rgcn_python':dict(available=True,argv=['native']),
            'rgcn_train':dict(available=True,argv=['training'])})

    def test_tool_probe_does_not_hash_or_read_executable_contents(self):
        worker=Worker.__new__(Worker);worker.env={}
        with patch('hashlib.sha256',side_effect=AssertionError('engine hash generated')), \
                patch.object(Path,'read_bytes',side_effect=AssertionError('engine contents read')):
            tools,info=worker._probe_tools({'python':sys.executable})
        self.assertEqual(set(info['python']),{'argv','available'})
        self.assertTrue(info['python']['available'])
        with self.assertRaisesRegex(ValueError,'semantic tool names'):
            worker._probe_tools({'ng47_sha256_'+'x'*43:sys.executable})
        with self.assertRaisesRegex(ValueError,'semantic tool names'):
            worker._probe_tools({'rgcn_python_linux':sys.executable})

    def test_worker_does_not_publish_operating_system_as_capability(self):
        with scratch() as directory, provider(Path(directory)/'worker') as (worker,_):
            self.assertNotIn(sys.platform,worker.health()['tags'])

    def test_removed_tool_clears_canonical_tag_and_readd_restores_it(self):
        with scratch() as directory:
            with provider(Path(directory)/'worker', tools={'ngspice47': sys.executable},
                          tags=['ngspice47', 'custom']) as (worker, _):
                self.assertIn('ng47', worker.health()['tags'])
                worker.control({'tools': {'ngspice47': None}})
                self.assertNotIn('ng47', worker.health()['tags'])
                self.assertIn('custom', worker.health()['tags'])
                worker.control({'tools': {'ngspice47': sys.executable}})
                self.assertIn('ng47', worker.health()['tags'])
                self.assertNotIn('ngspice47', worker.health()['tags'])

    def test_same_capability_survives_until_its_last_alias_is_removed(self):
        with scratch() as directory:
            with provider(Path(directory)/'worker',
                          tools={'onnx': sys.executable, 'onnx_cpu': sys.executable}) as (worker, _):
                worker.control({'tools': {'onnx_cpu': None}})
                self.assertIn('onnx', worker.health()['tags'])
                worker.control({'tools': {'onnx': None}})
                self.assertNotIn('onnx', worker.health()['tags'])

    def test_stale_configured_tags_do_not_restore_missing_tools_or_shared_paths(self):
        with scratch() as directory:
            root = Path(directory)
            ready = root/'ready'
            ready.mkdir()
            with provider(root/'worker', tools={'verilator': str(root/'missing-verilator'),
                          'ngspice47': str(root/'missing-ngspice')},
                          shared={'ready': ready, 'old': root/'absent'},
                          tags=['rtl', 'ngspice47', 'shared:old', 'custom']) as (worker, _):
                tags = worker.health()['tags']
                self.assertTrue({'python', 'custom', 'shared:ready'}.issubset(tags))
                self.assertFalse({'rtl', 'ng47', 'shared:old'}.intersection(tags))

    @unittest.skipUnless(os.name == 'nt', 'MSYS2 wrapper detection on Windows')
    def test_rtl_wrapper_requires_bash_and_detects_the_same_path_as_execution(self):
        with scratch() as directory:
            root = Path(directory)
            engine = root/'ucrt64/bin/verilator_bin.exe'
            engine.parent.mkdir(parents=True)
            engine.touch()
            worker = Worker.__new__(Worker)
            worker.env = {'MSYS2_ROOT': str(root)}
            wrapper = Path(sys.modules[Worker.__module__].__file__).with_name('rtl.py')
            tools = {'verilator': [sys.executable, str(wrapper)]}
            self.assertFalse(worker._probe_tools(tools)[1]['verilator']['available'])
            bash = root/'usr/bin/bash.exe'
            bash.parent.mkdir(parents=True)
            bash.touch()
            self.assertTrue(worker._probe_tools(tools)[1]['verilator']['available'])
            worker.env = {'PATH': str(bash.parent)}
            self.assertTrue(worker._probe_tools(tools)[1]['verilator']['available'])

    def test_explicit_temp_root_does_not_initialize_shared_ramdisk(self):
        with scratch() as directory:
            root = Path(directory)
            config = root/'worker.json'
            config.write_text(json.dumps({'temp_root': str(root/'ordinary-disk')}), encoding='utf8')
            with patch('remote_compute.cli.require_visible'), \
                    patch('remote_compute.cli.handoff_python', return_value=False), \
                    patch('remote_compute.cli.resolve', return_value=None), \
                    patch('remote_compute.cli.Worker') as worker, \
                    patch('remote_compute.cli.Server'), \
                    patch('remote_compute.cli.WorkerShell.loop'), \
                    patch('remote_compute.ramdisk.scratch', side_effect=AssertionError('unexpected RAM setup')), \
                    io.StringIO() as output, redirect_stdout(output):
                self.assertEqual(main(['--service-child', '--mode', 'worker', '--port', '0',
                                       '--config', str(config), '--root', str(root/'state')]), 0)
                self.assertEqual(worker.call_args.kwargs['temp_root'], str(root/'ordinary-disk'))


class RuntimeSetupTests(unittest.TestCase):
    def test_inaccessible_launcher_keeps_current_working_python(self):
        for error in (PermissionError('launcher inaccessible'),
                      subprocess.CalledProcessError(1, ['py', '-0p']),
                      subprocess.TimeoutExpired(['py', '-0p'], 10)):
            with self.subTest(error=type(error).__name__), \
                    patch.object(setup_tools.sys, 'version_info', (3, 13)), \
                    patch.object(setup_tools.shutil, 'which', side_effect=lambda name: 'py' if name == 'py' else None), \
                    patch.object(setup_tools, '_run', side_effect=error):
                self.assertEqual(setup_tools.preferred_python(), Path(sys.executable))

    def test_native_installed_314_is_preferred(self):
        with patch.object(setup_tools.sys, 'version_info', (3, 13)), \
                patch.object(setup_tools.shutil, 'which', return_value=sys.executable), \
                patch.object(setup_tools, '_run') as run:
            self.assertEqual(setup_tools.preferred_python(), Path(sys.executable).absolute())
            run.assert_not_called()

    @unittest.skipUnless(os.name == 'nt', 'Windows science installer')
    def test_setup_registers_shared_profiles_and_only_verified_ng47(self):
        for version, expected in (('ngspice-47', True), ('ngspice-46', False)):
            with self.subTest(version=version), scratch() as directory:
                root = Path(directory)
                with patch.object(setup_tools, '_python', return_value=(Path(sys.executable), {})), \
                        patch.object(setup_tools, '_ngspice',
                                     return_value=(root/'.tools/ngspice/bin/ngspice.exe', {'version': version})):
                    result = setup_tools.install(root, temporary_root=root/'tmp')
                self.assertEqual('ng47' in result['tools'], expected)
                self.assertFalse({'aop_python','arh_python','rgcn_python','bench_python'} & set(result['tools']))
                self.assertTrue({'python_numpy','python_scientific'} <= set(result['tools']))
                self.assertNotIn('ARH_INFERENCE_PYTHON', result['env'])
                self.assertIn('ngspice', result['tools'])


if __name__ == '__main__':
    unittest.main()
