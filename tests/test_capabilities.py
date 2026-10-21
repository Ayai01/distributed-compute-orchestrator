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

from remote_compute import setup_tools, command
from remote_compute.cli import main
from remote_compute.worker import Worker
from remote_compute.policy import canonical_capabilities, clean_tool_snapshot
from test_compute import provider, scratch


class CapabilityTests(unittest.TestCase):
    def test_semantic_and_legacy_templates_share_the_registered_binding(self):
        aliases = (('ngspice47', 'ng47'), ('circt-translate', 'circt'),
                   ('onnx_cpu', 'onnx'), ('aop_hspice_python', 'aop-hs'))
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
                         ['ng47','rgcn_python'])

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
    def test_setup_registers_ngspice47_only_for_the_installed_47_engine(self):
        for version, expected in (('ngspice-47', True), ('ngspice-46', False)):
            with self.subTest(version=version), scratch() as directory:
                root = Path(directory)
                with patch.object(setup_tools, '_python', return_value=(Path(sys.executable), {})), \
                        patch.object(setup_tools, '_ngspice',
                                     return_value=(root/'.tools/ngspice/bin/ngspice.exe', {'version': version})):
                    result = setup_tools.install(root, temporary_root=root/'tmp')
                self.assertEqual('ngspice47' in result['tools'], expected)
                self.assertIn('ngspice', result['tools'])


if __name__ == '__main__':
    unittest.main()
