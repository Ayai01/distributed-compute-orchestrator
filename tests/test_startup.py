"""Startup handles occupied ports before touching service state."""
from pathlib import Path
import socket
import subprocess
import sys
import unittest
from unittest.mock import patch

from remote_compute import startup
from remote_compute.cli import ROOT
from test_center import center
from test_compute import provider, scratch
from remote_compute import Pool


class StartupTests(unittest.TestCase):
    def test_cancel_preserves_listener(self):
        with socket.socket() as listener:
            listener.bind(('127.0.0.1', 0))
            listener.listen()
            port = listener.getsockname()[1]
            with patch.object(startup, 'owners', return_value=[]), patch.object(startup, 'terminate') as kill:
                with self.assertRaises(startup.Cancelled):
                    startup.resolve('127.0.0.1', port, policy='cancel')
                kill.assert_not_called()
            self.assertFalse(startup.available('127.0.0.1', port))

    def test_real_listener_pid_can_be_killed_and_port_reused(self):
        code = ("import socket,time; s=socket.socket(); s.bind(('127.0.0.1',0)); "
                "s.listen(); print(s.getsockname()[1],flush=True); time.sleep(60)")
        child = subprocess.Popen([sys.executable, '-u', '-c', code], stdout=subprocess.PIPE, text=True)
        try:
            port = int(child.stdout.readline())
            self.assertIn(child.pid, [p['pid'] for p in startup.owners(port)])
            self.assertIsNone(startup.resolve('127.0.0.1', port, policy='kill'))
            child.wait(timeout=5)
            self.assertTrue(startup.available('127.0.0.1', port))
        finally:
            if child.poll() is None:
                child.kill()
                child.wait()
            child.stdout.close()

    def test_existing_center_reused_without_stopping_jobs(self):
        with scratch() as directory:
            with provider(Path(directory) / 'worker') as (_, client), Pool([client]) as pool:
                with center(pool, Path(directory) / 'center') as (_, hub):
                    port = int(hub.url.rsplit(':', 1)[1])
                    with patch.object(startup, 'owners', return_value=[]), patch.object(startup, 'terminate') as kill:
                        self.assertEqual(startup.resolve('127.0.0.1', port, policy='reuse', reuse_role='scheduler'), hub.url)
                        self.assertEqual(hub.health()['role'], 'scheduler')
                        kill.assert_not_called()

    def test_admin_choice_uses_identified_process(self):
        with patch.object(startup, 'available', side_effect=[False, True]), \
                patch.object(startup, 'owners', return_value=[{'pid': 12345}]), \
                patch.object(startup, 'terminate') as kill:
            self.assertIsNone(startup.resolve('127.0.0.1', 26133, policy='admin-kill'))
            kill.assert_called_once_with([12345], admin=True)

    def test_noninteractive_conflict_has_actionable_message(self):
        with patch.object(startup, 'available', return_value=False), \
                patch.object(startup, 'owners', return_value=[]), \
                patch.object(startup.sys.stdin, 'isatty', return_value=False):
            with self.assertRaisesRegex(RuntimeError, '--on-port-conflict'):
                startup.resolve('127.0.0.1', 26133)

    def test_unstarted_center_points_to_launch_scripts(self):
        with socket.socket() as probe:
            probe.bind(('127.0.0.1', 0))
            port = probe.getsockname()[1]
        result = subprocess.run([sys.executable, '-B', str(ROOT / 'main.py'), '--url', f'http://127.0.0.1:{port}', 'jobs'],
                                capture_output=True, text=True, encoding='utf-8', timeout=15)
        self.assertEqual(result.returncode, 2)
        self.assertIn('start.cmd', result.stderr)
        self.assertIn('start.sh', result.stderr)


if __name__ == '__main__':
    unittest.main()
