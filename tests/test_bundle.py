"""Private config publication must not expose credentials through temporary files."""

from contextlib import redirect_stdout
import io
import json
import os
from pathlib import Path
import stat
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from remote_compute.bundle import atomic_json
from remote_compute import cli
from remote_compute.commands import SchedulerShell
from test_compute import scratch


class PrivateJsonTests(unittest.TestCase):
    def test_private_config_uses_exclusive_owner_only_temp_before_replace(self):
        with scratch() as directory:
            root = Path(directory)
            target = root / 'worker.local.json'
            target.write_text('{"cpus":1}', encoding='utf8')
            replace = Path.replace
            published = []

            def publish(pending, destination):
                self.assertEqual(destination, target)
                self.assertEqual(json.loads(target.read_text()), {'cpus': 1})
                self.assertEqual(json.loads(pending.read_text()), {'cpus': 2, 'token': 'test-secret'})
                if os.name != 'nt':
                    self.assertEqual(stat.S_IMODE(pending.stat().st_mode), 0o600)
                published.append(pending)
                return replace(pending, destination)

            with patch('os.open', wraps=os.open) as create, patch.object(Path, 'replace', publish):
                atomic_json(target, {'cpus': 2, 'token': 'test-secret'}, private=True)
            self.assertEqual(len(published), 1)
            self.assertNotEqual(published[0], target.with_suffix('.tmp'))
            self.assertTrue(any(call.args[1] & os.O_EXCL and call.args[2] == 0o600
                                for call in create.call_args_list))
            self.assertEqual(json.loads(target.read_text())['cpus'], 2)
            self.assertEqual(list(root.iterdir()), [target])

    @unittest.skipIf(os.name == 'nt', 'POSIX file modes and umask')
    def test_private_save_repairs_permissive_config_under_permissive_umask(self):
        with scratch() as directory:
            target = Path(directory) / 'worker.local.json'
            target.write_text('{}', encoding='utf8')
            target.chmod(0o664)
            previous = os.umask(0)
            try:
                atomic_json(target, {'token': 'test-secret'}, private=True)
            finally:
                os.umask(previous)
            self.assertEqual(stat.S_IMODE(target.stat().st_mode), 0o600)
            atomic_json(target, {'token': 'test-secret', 'cpus': 2}, private=True)
            self.assertEqual(stat.S_IMODE(target.stat().st_mode), 0o600)

    def test_private_replace_failure_preserves_config_and_cleans_temp(self):
        with scratch() as directory:
            root = Path(directory)
            target = root / 'worker.local.json'
            target.write_bytes(b'{"cpus":1}')
            with patch.object(Path, 'replace', side_effect=OSError('publish failed')):
                with self.assertRaisesRegex(OSError, 'publish failed'):
                    atomic_json(target, {'token': 'test-secret'}, private=True)
            self.assertEqual(target.read_bytes(), b'{"cpus":1}')
            self.assertEqual(list(root.iterdir()), [target])

    def test_invalid_json_leaves_no_private_temp(self):
        with scratch() as directory:
            root = Path(directory)
            with self.assertRaises(ValueError):
                atomic_json(root / 'worker.local.json', {'cpus': float('nan')}, private=True)
            self.assertEqual(list(root.iterdir()), [])

    def test_ordinary_results_keep_default_publication_path(self):
        with scratch() as directory:
            target = Path(directory) / 'result.json'
            with patch('remote_compute.bundle.tempfile.mkstemp') as create:
                atomic_json(target, {'answer': 42})
            create.assert_not_called()
            self.assertEqual(json.loads(target.read_text()), {'answer': 42})
            self.assertFalse(target.with_suffix('.tmp').exists())

    def test_setup_saves_worker_config_privately_without_starting_service(self):
        with scratch() as directory:
            target = Path(directory) / 'worker.local.json'
            target.write_text('{"cpus":2,"token":"test-secret"}', encoding='utf8')
            with patch('remote_compute.setup_tools.install', return_value={'tools': {}, 'env': {}}), \
                 patch('remote_compute.cli.atomic_json', wraps=atomic_json) as write, \
                 patch('remote_compute.cli.display'):
                self.assertEqual(cli.main(['--config', str(target), 'setup']), 0)
            self.assertTrue(write.call_args.kwargs['private'])
            self.assertEqual(json.loads(target.read_text())['token'], 'test-secret')

    def test_save_nodes_keeps_token_and_uses_private_publication(self):
        with scratch() as directory:
            root = Path(directory)
            target = root / 'nodes.local.json'
            remote = dict(name='remote', url='https://example.test:26133', weight=1.0,
                          limit=0, memory_limit_mb=0, enabled=True)
            target.write_text(json.dumps({'nodes': [dict(remote, token='test-secret')]}), encoding='utf8')
            shell = SimpleNamespace(pool=SimpleNamespace(nodes=[]),
                                    center=SimpleNamespace(nodes=lambda: dict(policy='adaptive', nodes=[remote]),
                                                           token=''))
            with patch('remote_compute.commands.ROOT', root), \
                 patch('remote_compute.commands.atomic_json', wraps=atomic_json) as write, \
                 redirect_stdout(io.StringIO()):
                SchedulerShell.do_save(shell, '')
            self.assertTrue(write.call_args.kwargs['private'])
            self.assertEqual(json.loads(target.read_text())['nodes'][0]['token'], 'test-secret')
            self.assertEqual(list(root.iterdir()), [target])
