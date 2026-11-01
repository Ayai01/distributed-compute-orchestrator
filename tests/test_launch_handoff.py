"""A duplicate launcher reloads the original terminal before taking its lock."""
from contextlib import redirect_stdout
import io
from pathlib import Path
import unittest
from unittest.mock import MagicMock, patch

from remote_compute import cli, startup, RemoteError
from test_compute import scratch


class LaunchHandoffTests(unittest.TestCase):
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
        with patch.object(startup, 'available', return_value=True), patch.object(startup, 'owners', return_value=[]), \
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
