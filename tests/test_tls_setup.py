"""Native certificate setup, private handoff and idle-only worker reload."""
from contextlib import redirect_stdout
import io
import json
import os
from pathlib import Path
import shutil
import ssl
import unittest
from unittest.mock import Mock, patch

from remote_compute import Client, RemoteError
from remote_compute.cli import main
from remote_compute.tls_setup import _make_certificate, configure
from test_access import external_peer
from test_compute import provider, scratch


class TLSSetupTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if os.name != 'nt' and not shutil.which('openssl'):
            raise unittest.SkipTest('native certificate creation requires openssl on this platform')
        cls.scratch = scratch()
        cls.root = Path(cls.scratch.__enter__())
        cls.cert = cls.root / 'certs' / 'worker.crt'
        cls.key = cls.root / 'certs' / 'worker.key'
        try:
            _make_certificate(cls.cert, cls.key, '203.0.113.42')
        except BaseException:
            cls.scratch.__exit__(None, None, None)
            raise

    @classmethod
    def tearDownClass(cls):
        cls.scratch.__exit__(None, None, None)

    def settings(self, root, **changes):
        value = dict(mode='worker', port=26133, cpus=3, memory_mb=4096,
                     tools={'python': '/existing/python'}, shared={'research': '/existing/research'},
                     env={'USER_SETTING': 'retained'}, token='this-node-secret',
                     certfile=str(self.cert), keyfile=str(self.key))
        value.update(changes)
        (root / 'worker.local.json').write_text(json.dumps(value), encoding='utf-8')
        return value

    def test_generated_certificate_authenticates_external_tls_and_reuses_device_configuration(self):
        with scratch() as temporary, patch.dict(os.environ, {'REMOTE_COMPUTE_TOKEN': ''}):
            root = Path(temporary); previous = self.settings(root)
            (root / '.local').mkdir()
            result = configure(root, '203.0.113.42', 26143, restart=False)
            config = json.loads((root / 'worker.local.json').read_text())
            for key, value in previous.items():
                if key != 'port':self.assertEqual(config[key], value)
            self.assertEqual(config['port'], 26143)
            profile = json.loads(Path(result['connection_profile']).read_text())
            self.assertEqual(profile['url'], 'https://203.0.113.42:26143')
            self.assertEqual(profile['token'], previous['token'])
            self.assertEqual(profile['certificate_pem'], self.cert.read_text())
            self.assertNotIn('PRIVATE KEY', json.dumps(profile))
            self.assertNotIn(previous['token'], json.dumps(result))
            certificate_before = (self.cert.stat().st_mtime_ns, self.cert.read_bytes())
            with patch('remote_compute.tls_setup.socket.gethostname', return_value='renamed-device'):
                again = configure(root, '203.0.113.42', 26143, restart=False)
            self.assertEqual(again['certificate_sha256'], result['certificate_sha256'])
            self.assertEqual((self.cert.stat().st_mtime_ns, self.cert.read_bytes()), certificate_before)
            self.assertEqual(json.loads(Path(again['connection_profile']).read_text())['node'], 'renamed-device')
            with provider(root / 'state', token=profile['token']) as (worker, _), \
                    external_peer(worker, certfile=str(self.cert), keyfile=str(self.key)) as server:
                url = 'https://127.0.0.1:' + str(server.server_port)
                self.assertEqual(Client(url, profile['token'], ca_file=str(self.cert)).health()['role'], 'worker')
                for token in ('', 'wrong-token'):
                    with self.subTest(token=token), self.assertRaisesRegex(RemoteError, '401'):
                        Client(url, token, ca_file=str(self.cert)).health()
                with self.assertRaises(RemoteError):Client(url, profile['token']).health()

    def test_active_work_keeps_original_console_and_waits_for_idle(self):
        with scratch() as temporary, patch.dict(os.environ, {'REMOTE_COMPUTE_TOKEN': ''}):
            root = Path(temporary); self.settings(root); (root / '.local').mkdir()
            live = Mock()
            live.health.return_value = dict(role='worker', tool_root=str(root), used_cpus=1,
                                            queued=0, active_jobs=[dict(id='original-id')])
            with patch('remote_compute.tls_setup.Client', return_value=live):
                result = configure(root, '203.0.113.42', 26143)
            self.assertEqual(result['status'], 'configured_waiting_for_idle')
            live.restart.assert_not_called()
            self.assertEqual(json.loads((root / 'worker.local.json').read_text())['cpus'], 3)

    def test_idle_reload_verifies_tls_in_the_original_console(self):
        with scratch() as temporary, patch.dict(os.environ, {'REMOTE_COMPUTE_TOKEN': ''}):
            root = Path(temporary); self.settings(root); (root / '.local').mkdir()
            live, secure = Mock(), Mock()
            live.health.return_value = dict(role='worker', tool_root=str(root), used_cpus=0, queued=0, active_jobs=[])
            secure.health.return_value = dict(role='worker', node='actual-device', version='test')
            with patch('remote_compute.tls_setup.Client', side_effect=[live, secure]) as constructor:
                result = configure(root, '203.0.113.42', 26143)
            live.restart.assert_called_once_with()
            self.assertEqual(result['status'], 'https_verified_in_original_console')
            self.assertEqual(constructor.call_args.args[0], 'https://127.0.0.1:26143')
            self.assertEqual(constructor.call_args.kwargs['ca_file'], str(self.cert))

    def test_other_service_on_port_is_not_modified_or_restarted(self):
        with scratch() as temporary, patch.dict(os.environ, {'REMOTE_COMPUTE_TOKEN': ''}):
            root = Path(temporary); self.settings(root)
            before = (root / 'worker.local.json').read_bytes()
            live = Mock(); live.health.return_value = dict(role='worker', tool_root=str(root / 'other'))
            with patch('remote_compute.tls_setup.Client', return_value=live), \
                    self.assertRaisesRegex(ValueError, 'another DCO service'):
                configure(root, '203.0.113.42', 26143)
            self.assertEqual((root / 'worker.local.json').read_bytes(), before)
            live.restart.assert_not_called()

    def test_cli_runs_setup_without_starting_another_service(self):
        with scratch() as temporary, patch('remote_compute.cli.ROOT', Path(temporary)), \
                patch('remote_compute.cli.require_visible', side_effect=AssertionError('one-shot setup')), \
                patch('remote_compute.tls_setup.configure', return_value={'status': 'configured'}) as setup, \
                redirect_stdout(io.StringIO()):
            self.assertEqual(main(['--port', '26143', 'setup-https', '--public-address', '203.0.113.42', '--no-restart']), 0)
            self.assertEqual(setup.call_args.args, (Path(temporary), '203.0.113.42', 26143))
            self.assertFalse(setup.call_args.kwargs['restart'])
