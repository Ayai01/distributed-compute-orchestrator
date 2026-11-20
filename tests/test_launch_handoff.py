"""A duplicate launcher reloads the original terminal before taking its lock."""
from contextlib import contextmanager, nullcontext, redirect_stdout
import io
import json
import os
from pathlib import Path
import sys
import unittest
from unittest.mock import MagicMock, patch

from remote_compute import cli, startup, RemoteError, network
from test_compute import scratch


class LaunchHandoffTests(unittest.TestCase):
    def test_managed_launch_keeps_the_active_entry_and_explicit_versions_stay_pinned(self):
        runtime = Path.cwd() / '.runtime'
        installed = runtime / 'v1.0.29'
        active = runtime / 'active' / 'main.py'
        with patch.object(cli, 'ROOT', installed):
            with patch.object(sys, 'argv', [str(active)]):
                self.assertEqual(cli.service_entry(), active)
            with patch.object(sys, 'argv', [str(installed / 'main.py')]):
                self.assertEqual(cli.service_entry(), installed / 'main.py')

    def test_frpc_uses_its_configured_transport_without_inheriting_http_proxy(self):
        for executable in ('frpc','frps'):
            with self.subTest(executable=executable),scratch() as directory:
                root=Path(directory)
                with patch.dict(os.environ,{'http_proxy':'http://inherited.example:8080',
                                            'HTTPS_PROXY':'http://retained.example:8080',
                                            'DCO_TEST_SETTING':'retained'}), \
                     patch.object(network.subprocess,'run') as verify, \
                     patch.object(network.subprocess,'Popen') as child:
                    verify.return_value.stdout=network.FRP_VERSION
                    with network.FrpProcess(root/(executable+'.exe' if os.name=='nt' else executable),
                                            root/'network.json',root/'network.log'):
                        environment=child.call_args.kwargs['env']
                        self.assertEqual(any(key.lower()=='http_proxy' for key in environment),executable=='frps')
                        self.assertEqual(environment['HTTPS_PROXY'],'http://retained.example:8080')
                        self.assertEqual(environment['DCO_TEST_SETTING'],'retained')
                        self.assertIn('http_proxy',os.environ)

    def test_local_service_owns_its_profile_until_exit_and_starts_after_http(self):
        for mode, profile in [('worker','worker'), ('scheduler','center'), ('hybrid','center')]:
            with self.subTest(mode=mode), scratch() as directory:
                root=Path(directory);private=root/'.local';private.mkdir()
                (private/'worker.json').write_text(json.dumps(dict(mode=mode,token='isolated-device',temp_root=str(root/'tmp'))))
                config=private/'network'/(profile+'.json');config.parent.mkdir();config.write_text('{}')
                events=[]
                @contextmanager
                def console(stopping):
                    stopping.set();yield
                with patch.object(cli,'ROOT',root), patch.object(cli,'require_visible'), \
                     patch.object(cli,'handoff_python',return_value=False), patch.object(cli,'resolve',return_value=None), \
                     patch.object(cli,'console_close',side_effect=console), patch.object(cli,'keep_awake',return_value=nullcontext()), \
                     patch.object(cli,'load_nodes',return_value={'nodes':[]}), patch.object(cli,'Pool') as pool, \
                     patch.object(cli,'Coordinator') as center, patch.object(cli,'Worker') as worker, \
                     patch.object(cli,'Server') as server, patch.object(cli.threading,'Thread') as thread, \
                     patch.object(network,'FrpProcess') as tunnel, redirect_stdout(io.StringIO()):
                    worker.return_value.startup_recoveries=[]
                    worker.return_value.health.return_value={'node':'isolated-local'}
                    worker.return_value.capacity=2
                    worker.return_value.root=root/'state'
                    server.return_value.server_port=26133
                    center.return_value.paused=False
                    thread.return_value.start.side_effect=lambda *args:events.append('http')
                    thread.return_value.is_alive.return_value=False
                    tunnel.return_value.__enter__.side_effect=lambda *args:events.append('tunnel')
                    tunnel.return_value.__exit__.side_effect=lambda *args:events.append('closed')
                    self.assertEqual(cli.main(['--service-child','--no-dashboard']),0)
                    executable=root/'.tools/frp/0.71.1'/('frpc.exe' if os.name=='nt' else 'frpc')
                    tunnel.assert_called_once_with(executable,config,config.with_suffix('.log'))
                    self.assertLess(events.index('http'),events.index('tunnel'))
                    self.assertEqual(events[-1],'closed')

    def test_missing_profile_remote_console_and_ordinary_action_do_not_start_frp(self):
        with scratch() as directory:
            root=Path(directory)
            with patch.object(cli,'ROOT',root),patch.object(network,'FrpProcess') as tunnel:
                from contextlib import ExitStack
                with ExitStack() as lifetime:cli._start_network(lifetime,'worker')
                tunnel.assert_not_called()
            (root/'.local/network').mkdir(parents=True)
            (root/'.local/network/center.json').write_text('{}')
            (root/'.local/network/worker.json').write_text('{}')
            with patch.object(cli,'ROOT',root),patch.object(network,'FrpProcess') as tunnel, \
                 patch.object(cli,'require_visible'), patch.object(cli,'single_instance',return_value=nullcontext()), \
                 patch.object(cli,'console_close',return_value=nullcontext()), \
                 patch.object(cli,'load_nodes',return_value={'nodes':[]}),patch.object(cli,'Pool') as pool, \
                 patch.object(cli,'Client') as client, patch.object(cli,'Server') as server, \
                 patch.object(cli,'Coordinator'),patch.object(cli.threading,'Thread'), \
                 patch.object(cli,'SchedulerShell') as shell,patch.object(cli,'resolve',return_value=None), \
                 redirect_stdout(io.StringIO()):
                client.return_value.health.return_value={'role':'scheduler'}
                client.return_value.nodes.return_value={'nodes':[]}
                server.return_value.server_port=26133
                self.assertEqual(cli.main(['--url','http://127.0.0.1:26133']),0)
                self.assertEqual(cli.main(['--mode','scheduler','nodes']),0)
                tunnel.assert_not_called()

    def test_failed_http_bind_leaves_tunnel_unstarted(self):
        with scratch() as directory:
            root=Path(directory);(root/'.local/network').mkdir(parents=True)
            (root/'.local/network/center.json').write_text('{}')
            (root/'.local/worker.json').write_text('{"mode":"scheduler","token":"isolated"}')
            @contextmanager
            def console(stopping):
                stopping.set();yield
            with patch.object(cli,'ROOT',root),patch.object(network,'FrpProcess') as tunnel, \
                 patch.object(cli,'require_visible'),patch.object(cli,'handoff_python',return_value=False), \
                 patch.object(cli,'console_close',side_effect=console),patch.object(cli,'keep_awake',return_value=nullcontext()), \
                 patch.object(cli,'resolve',return_value=None),patch.object(cli,'load_nodes',return_value={'nodes':[]}), \
                 patch.object(cli,'Pool'),patch.object(cli,'Coordinator'), \
                 patch.object(cli,'Server',side_effect=OSError('isolated bind failure')), \
                 redirect_stdout(io.StringIO()),patch('sys.stderr',io.StringIO()):
                self.assertEqual(cli.main(['--service-child','--no-dashboard']),2)
                tunnel.assert_not_called()

    def test_managed_restart_waits_for_new_pid_in_the_original_service(self):
        old = {'pid': 10, 'role': 'worker', 'version': '0.8.45'}
        new = {'pid': 11, 'role': 'worker', 'version': '0.8.46'}
        public, operator = MagicMock(), MagicMock()
        public.health.return_value = old
        operator.health.side_effect = [old, new]
        with patch.object(startup, 'available', return_value=False), patch.object(startup, 'owners', return_value=[]), \
             patch.object(startup, 'Client', side_effect=[public, operator]) as clients, \
             patch.object(startup, 'terminate', side_effect=AssertionError('managed restart must be graceful')), \
             patch.object(startup.time, 'sleep'), redirect_stdout(io.StringIO()):
            with self.assertRaisesRegex(startup.Cancelled, '原窗口完成重载.*0.8.46'):
                startup.resolve('127.0.0.1', 26133, policy='restart', reuse_role='worker', operator_token='test')
        operator.restart.assert_called_once_with()
        self.assertEqual(operator.health.call_count, 2)
        self.assertTrue(clients.call_args_list[1].kwargs['operator'])

    def test_busy_original_remains_in_its_terminal(self):
        public, operator = MagicMock(), MagicMock()
        public.health.return_value = {'pid': 10, 'role': 'scheduler'}
        operator.health.return_value = {'pid': 10, 'role': 'scheduler'}
        error = RemoteError('active original jobs'); error.status_code = 409
        operator.restart.side_effect = error
        with patch.object(startup, 'available', return_value=False), patch.object(startup, 'owners', return_value=[]), \
             patch.object(startup, 'Client', side_effect=[public, operator]), \
             patch.object(startup, 'terminate', side_effect=AssertionError('original work must continue')), \
             redirect_stdout(io.StringIO()):
            with self.assertRaisesRegex(startup.Cancelled, '原窗口仍有任务'):
                startup.resolve('127.0.0.1', 26133, policy='restart', reuse_role='scheduler')
        operator.health.assert_called_once_with()

    def test_original_service_wins_over_windows_specific_ip_bind_probe(self):
        public, operator = MagicMock(), MagicMock()
        public.health.return_value = {'pid': 10, 'role': 'scheduler'}
        operator.health.side_effect = [{'pid': 10, 'role': 'scheduler'},
                                       {'pid': 11, 'role': 'scheduler', 'version': '0.8.46'}]
        with patch.object(startup, 'available', return_value=True), patch.object(startup, 'owners', return_value=[{'pid':10}]), \
             patch.object(startup, 'Client', side_effect=[public, operator]), redirect_stdout(io.StringIO()):
            with self.assertRaisesRegex(startup.Cancelled, '原窗口完成重载'):
                startup.resolve('127.0.0.1', 26133, policy='restart', reuse_role='scheduler')
        operator.restart.assert_called_once_with()

    def test_unrelated_listener_is_not_restarted(self):
        client = MagicMock(); client.health.return_value = {'pid': 10, 'role': 'other'}
        with patch.object(startup, 'available', return_value=False), patch.object(startup, 'owners', return_value=[]), \
             patch.object(startup, 'Client', return_value=client), redirect_stdout(io.StringIO()):
            with self.assertRaises(startup.Cancelled):
                startup.resolve('127.0.0.1', 26133, policy='restart', reuse_role='worker')
        client.restart.assert_not_called()

    def test_existing_service_is_resolved_before_the_supervisor_lock(self):
        with scratch() as folder:
            config = Path(folder) / 'worker.json'; config.write_text('{"host":null}')
            with patch.object(cli, 'ROOT', Path(folder)), patch.object(cli, 'require_visible'), patch.object(cli, 'resolve', side_effect=startup.Cancelled('原窗口已重载')) as resolve, \
                 patch.object(cli, 'single_instance', side_effect=AssertionError('old supervisor still owns this lock')), \
                 redirect_stdout(io.StringIO()):
                self.assertEqual(cli.main(['--config', str(config), '--mode', 'worker', '--on-port-conflict', 'restart']), 0)
            self.assertEqual(resolve.call_args.kwargs['policy'], 'restart')
            self.assertEqual(resolve.call_args.kwargs['reuse_role'], 'worker')
            self.assertEqual(resolve.call_args.args[0], '127.0.0.1')

    def test_local_center_preflight_reads_its_original_operator_token(self):
        with scratch() as folder:
            root = Path(folder)
            config = root / 'worker.json'; config.write_text('{"host":null}')
            secret = root / '.local/state/scheduler/operator.token'
            secret.parent.mkdir(parents=True); secret.write_text('local-original-secret')
            with patch.object(cli, 'ROOT', root), patch.object(cli, 'require_visible'), \
                 patch.object(cli, 'resolve', side_effect=startup.Cancelled('原窗口已重载')) as resolve, \
                 redirect_stdout(io.StringIO()):
                self.assertEqual(cli.main(['--config', str(config), '--mode', 'hybrid', '--on-port-conflict', 'restart']), 0)
            self.assertEqual(resolve.call_args.kwargs['operator_token'], 'local-original-secret')


if __name__ == '__main__':
    unittest.main()
