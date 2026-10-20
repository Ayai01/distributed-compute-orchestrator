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

from remote_compute import setup_tools
from remote_compute.cli import main
from remote_compute.worker import Worker
from test_compute import provider, scratch


class CapabilityTests(unittest.TestCase):
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
