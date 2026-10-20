"""Manual cache commands use the existing center and own only their RPC jobs."""

from contextlib import redirect_stderr, redirect_stdout
import io
import os
from pathlib import Path
import sys
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from remote_compute.cli import main
from remote_compute.commands import SchedulerShell, cache_parser
from test_compute import scratch, until


class CacheCommandTests(unittest.TestCase):
    def test_parser_accepts_simple_project_names_and_kind(self):
        for name, expected in [('all', 'all'), ('AOP', 'aop'), ('RGCN', 'rgcn'),
                               ('ARH-DSE', 'arh'), ('Bench', 'bench')]:
            with self.subTest(project=name):
                args = cache_parser().parse_args([name])
                self.assertEqual((args.project, args.kind), (expected, 'all'))
        args = cache_parser().parse_args(['ARH-DSE-v2', '--kind', 'research'])
        self.assertEqual((args.project, args.kind), ('arh', 'research'))
        with redirect_stderr(io.StringIO()):
            for argv in ([], ['missing'], ['all', '--kind', 'automatic']):
                with self.subTest(argv=argv), self.assertRaises(SystemExit):
                    cache_parser().parse_args(argv)

    def test_help_and_completion_expose_manual_command_without_syncing(self):
        shell = SchedulerShell(Mock(), Mock())
        output = io.StringIO()
        shell.stdout = output
        sync = Mock()
        try:
            with patch.dict(sys.modules, {'remote_compute.cache_sync': SimpleNamespace(sync_cache=sync)}), redirect_stdout(output):
                shell.onecmd('help')
                shell.onecmd('help sync_cache')
                def suggest(line):
                    return shell.suggest(line, len(line), {})
                self.assertEqual(suggest('sync_ca')['matches'], ['sync_cache'])
                self.assertIn('all', suggest('sync_cache ')['matches'])
                self.assertEqual(suggest('sync_cache ar')['matches'], ['arh'])
                self.assertEqual(suggest('sync_cache all --ki')['matches'], ['--kind'])
                self.assertEqual(suggest('sync_cache all --kind ')['matches'], ['all', 'assets', 'research'])
            self.assertIn('sync_cache all', output.getvalue())
            self.assertIn('--kind', output.getvalue())
            sync.assert_not_called()
        finally:
            shell.close()

    def test_console_runs_in_background_and_releases_its_completed_handles(self):
        client = Mock(url='http://center')
        shell = SchedulerShell(Mock(), client)
        entered, release = threading.Event(), threading.Event()
        identity = 'c' * 32
        def synchronize(target, project, **options):
            self.assertIs(target, client)
            self.assertEqual((project, options['kind']), ('arh', 'research'))
            self.assertIs(options['stop_event'], shell.closing)
            options['on_submit'](target, identity)
            entered.set()
            if not release.wait(3):
                raise RuntimeError('Fixture was not released')
            return dict(status='succeeded', projects=[])
        try:
            with patch.dict(sys.modules, {'remote_compute.cache_sync': SimpleNamespace(sync_cache=synchronize)}), redirect_stdout(io.StringIO()) as output:
                shell.stdout = output
                shell.onecmd('sync_cache ARH-DSE --kind research')
                self.assertTrue(entered.wait(2))
                self.assertIn(identity, shell.handles)
                self.assertTrue(shell.futures)
                shell.onecmd('help sync_cache')
                release.set()
                shell.do_wait('')
                until(lambda:not shell.futures)
                self.assertNotIn(identity, shell.handles)
                client.cancel.assert_not_called()
        finally:
            release.set()
            shell.close()

    def test_console_exit_stops_only_its_sync_and_preserves_other_jobs(self):
        client = Mock(url='http://center')
        shell = SchedulerShell(Mock(), client)
        entered, cancelled = threading.Event(), threading.Event()
        client.cancel.side_effect = lambda identity: cancelled.set()
        identity = 'd' * 32
        def synchronize(target, project, **options):
            options['on_submit'](target, identity)
            entered.set()
            if not options['stop_event'].wait(3):
                raise RuntimeError('Console did not signal shutdown')
            if not cancelled.wait(3):
                raise RuntimeError('Console did not cancel its accepted task')
            return dict(status='partial', projects=[])
        try:
            with patch.dict(sys.modules, {'remote_compute.cache_sync': SimpleNamespace(sync_cache=synchronize)}), redirect_stdout(io.StringIO()):
                shell.onecmd('sync_cache all')
                self.assertTrue(entered.wait(2))
                shell.close()
            client.cancel.assert_called_once_with(identity)
            client.status.assert_not_called()
            self.assertFalse(shell.handles)
        finally:
            if not shell.closing.is_set():
                shell.close()

    def test_console_rejects_direct_worker_route(self):
        shell = SchedulerShell(Mock())
        try:
            with patch.object(shell, '_start') as start, redirect_stdout(io.StringIO()) as output:
                shell.onecmd('sync_cache all')
            start.assert_not_called()
            self.assertIn('调度中心', output.getvalue())
        finally:
            shell.close()

    def _invoke_cli(self, argv, *, status='succeeded', role='scheduler'):
        client = Mock(url='http://center')
        client.health.return_value = dict(node='center', role=role)
        client.nodes.return_value = dict(nodes=[])
        pool = Mock(closed=False)
        sync = Mock(return_value=dict(status=status, projects=[]))
        with scratch() as directory, \
                patch.dict(sys.modules, {'remote_compute.cache_sync': SimpleNamespace(sync_cache=sync)}), \
                patch.dict(os.environ, {'REMOTE_COMPUTE_TOKEN':'', 'REMOTE_COMPUTE_OPERATOR_TOKEN':''}), \
                patch('remote_compute.cli.ROOT', Path(directory)), \
                patch('remote_compute.cli.Client', return_value=client) as clients, \
                patch('remote_compute.cli.Pool', return_value=pool), \
                patch('remote_compute.cli.Worker') as worker, \
                patch('remote_compute.cli.Coordinator') as coordinator, \
                patch('remote_compute.cli.Server') as server, \
                redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            code = main(argv)
            worker.assert_not_called()
            coordinator.assert_not_called()
            server.assert_not_called()
        return code, client, clients, sync

    def test_cli_defaults_to_existing_local_center_and_both_cache_kinds(self):
        code, client, clients, sync = self._invoke_cli(['sync_cache', 'all'])
        self.assertEqual(code, 0)
        self.assertTrue(all(call.args[0]=='http://127.0.0.1:26133' for call in clients.call_args_list))
        self.assertEqual(sync.call_args.args, (client, 'all'))
        self.assertEqual(sync.call_args.kwargs['kind'], 'all')
        self.assertIsNotNone(sync.call_args.kwargs['on_submit'])
        self.assertFalse(sync.call_args.kwargs['stop_event'].is_set())

    def test_cli_uses_explicit_center_and_reports_partial_or_failed_sync(self):
        for status in ('partial', 'failed'):
            with self.subTest(status=status):
                code, client, clients, sync = self._invoke_cli(
                    ['--url', 'http://example.test:26133', 'sync_cache', 'Bench', '--kind', 'assets'],
                    status=status)
                self.assertEqual(code, 1)
                self.assertTrue(all(call.args[0]=='http://example.test:26133' for call in clients.call_args_list))
                self.assertEqual(sync.call_args.args, (client, 'bench'))
                self.assertEqual(sync.call_args.kwargs['kind'], 'assets')

    def test_cli_does_not_sync_when_viewing_nodes_or_pointed_at_worker(self):
        code, _, _, sync = self._invoke_cli(['--url', 'http://center', 'nodes'])
        self.assertEqual(code, 0)
        sync.assert_not_called()
        code, _, _, sync = self._invoke_cli(['sync_cache', 'all'], role='worker')
        self.assertEqual(code, 2)
        sync.assert_not_called()


if __name__ == '__main__':
    unittest.main()
