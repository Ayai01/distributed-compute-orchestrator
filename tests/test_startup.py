"""Startup handles occupied ports before touching service state."""
from pathlib import Path
from contextlib import redirect_stderr, nullcontext
import io
import json
import os
import socket
import subprocess
import sys
import unittest
from unittest.mock import MagicMock, patch

from remote_compute import startup, terminal
from remote_compute.cli import ROOT
from test_center import center
from test_compute import provider, python, scratch, until
from remote_compute import Pool


class StartupTests(unittest.TestCase):
    @unittest.skipUnless(os.name == 'nt', 'Windows ConPTY host visibility')
    def test_conpty_requires_its_own_visible_terminal_not_a_chat_or_editor_window(self):
        kernel, user = MagicMock(), MagicMock()
        kernel.GetConsoleWindow.return_value = 1
        user.GetShellWindow.return_value = 2
        owners = {1: 40, 2: 10, 3: 20, 4: 50}
        classes = {1: 'PseudoConsoleWindow', 3: 'Chrome_WidgetWin_1',
                   4: 'CASCADIA_HOSTING_WINDOW_CLASS'}
        processes = {30: (20, 'python.exe'), 40: (20, 'conhost.exe'),
                     20: (10, 'ChatGPT.exe'), 10: (0, 'explorer.exe'),
                     50: (10, 'WindowsTerminal.exe')}

        def owner(hwnd, pointer):
            pointer._obj.value = owners[hwnd]

        def window_class(hwnd, buffer, _size):
            buffer.value = classes[hwnd]
            return len(buffer.value)

        user.GetWindowThreadProcessId.side_effect = owner
        user.GetClassNameW.side_effect = window_class
        user.IsWindowVisible.side_effect = lambda hwnd: hwnd in (3, 4)
        user.EnumWindows.side_effect = lambda callback, unused: [callback(hwnd, 0) for hwnd in (3, 4)]
        with patch('ctypes.WinDLL', side_effect=lambda name, **unused: kernel if name == 'kernel32' else user), \
                patch.object(terminal, '_windows_processes', return_value=processes), \
                patch.object(terminal.os, 'getpid', return_value=30):
            # The unrelated visible Windows Terminal cannot admit this PTY.
            for application in ('ChatGPT.exe', 'Code.exe'):
                processes[20] = (10, application)
                self.assertFalse(terminal._windows_visible())
            processes[20] = (10, 'WindowsTerminal.exe')
            classes[3] = 'CASCADIA_HOSTING_WINDOW_CLASS'
            self.assertTrue(terminal._windows_visible())
            user.IsWindowVisible.side_effect = lambda hwnd: hwnd == 4
            self.assertFalse(terminal._windows_visible())

    @unittest.skipUnless(os.name == 'nt', 'Native Windows console visibility')
    def test_native_visible_console_passes_and_same_hidden_console_fails(self):
        code = '''import ctypes as c,sys
from pathlib import Path
from remote_compute.terminal import require_visible
from ctypes import wintypes as w
kernel=c.WinDLL('kernel32'); user=c.WinDLL('user32')
kernel.GetConsoleWindow.restype=w.HWND
user.ShowWindow.argtypes=[w.HWND,c.c_int]
window=kernel.GetConsoleWindow()
require_visible()
try:
    user.ShowWindow(window,0)
    try:require_visible()
    except RuntimeError:pass
    else:raise AssertionError('hidden console accepted')
finally:user.ShowWindow(window,5)
require_visible()
Path(sys.argv[1]).write_text('visible-hidden-visible passed',encoding='utf-8')
'''
        with scratch() as directory:
            evidence = Path(directory) / 'visibility.txt'
            child = subprocess.Popen([sys.executable, '-B', '-c', code, str(evidence)],
                                     creationflags=subprocess.CREATE_NEW_CONSOLE, cwd=ROOT)
            try:
                self.assertEqual(child.wait(timeout=10), 0)
                self.assertEqual(evidence.read_text(encoding='utf-8'), 'visible-hidden-visible passed')
            finally:
                if child.poll() is None:
                    child.kill()
                    child.wait()

    @unittest.skipUnless(os.name == 'nt', 'Native Windows console close')
    def test_native_console_close_cancels_original_remote_task_and_releases_state(self):
        import ctypes as c
        from ctypes import wintypes as w
        from remote_compute import Client, RemoteError
        user = c.WinDLL('user32')
        user.PostMessageW.argtypes = [w.HWND, w.UINT, w.WPARAM, w.LPARAM]
        code = '''import ctypes as c,sys
from pathlib import Path
kernel=c.WinDLL('kernel32');kernel.GetConsoleWindow.restype=c.c_void_p
Path(sys.argv.pop(1)).write_text(str(kernel.GetConsoleWindow()),encoding='utf-8')
from remote_compute.cli import main
raise SystemExit(main())
'''
        for no_dashboard in (False, True):
            with self.subTest(no_dashboard=no_dashboard), scratch() as directory:
                root = Path(directory)
                with provider(root/'worker') as (worker, remote):
                    config = root/'config.json'
                    config.write_text('{}', encoding='utf-8')
                    nodes = root/'nodes.json'
                    nodes.write_text(json.dumps({'nodes': [
                        {'name': 'isolated', 'url': remote.url}]}), encoding='utf-8')
                    with socket.socket() as probe:
                        probe.bind(('127.0.0.1', 0))
                        port = probe.getsockname()[1]
                    hwnd_path = root/'console-hwnd.txt'
                    command = [sys.executable, '-B', '-c', code, str(hwnd_path),
                               '--mode', 'scheduler', '--port', str(port), '--config', str(config),
                               '--nodes', str(nodes), '--root', str(root/'center')]
                    if no_dashboard:
                        command.append('--no-dashboard')
                    child = subprocess.Popen(command, cwd=ROOT, creationflags=subprocess.CREATE_NEW_CONSOLE)
                    client = Client('http://127.0.0.1:' + str(port), timeout=1)
                    identity = None
                    try:
                        def ready():
                            try:
                                return client.health(brief=True)['role'] == 'scheduler'
                            except RemoteError:
                                return False
                        until(ready, seconds=15)
                        identity = client.submit(python('import time;time.sleep(30)',
                                                        cpus=1, memory_mb=128))['id']
                        def running():
                            try:
                                return remote.status(identity, brief=True)['status'] == 'running'
                            except RemoteError as error:
                                if error.status_code == 404:
                                    return False
                                raise
                        until(running)
                        self.assertTrue(user.PostMessageW(int(hwnd_path.read_text()), 0x10, 0, 0))
                        child.wait(timeout=10)
                        until(lambda: remote.status(identity, brief=True)['status'] == 'cancelled')
                        receipt = json.loads((root/'center/scheduler/jobs'/identity/'result.json').read_text())
                        self.assertEqual(receipt['status'], 'cancelled')
                        self.assertEqual(receipt['id'], identity)
                        from remote_compute.worker import lock_root
                        lock = lock_root(root/'center/scheduler', 'scheduler')
                        lock.close()
                    finally:
                        try:
                            if identity:
                                remote.cancel(identity)
                                remote.wait(identity, wait_timeout=5)
                        finally:
                            if child.poll() is None:
                                user.PostMessageW(int(hwnd_path.read_text()), 0x10, 0, 0)
                                child.wait(timeout=10)

    @unittest.skipUnless(sys.platform == 'linux', 'Native Linux terminal hangup')
    def test_native_tty_hangup_cancels_original_remote_task_and_releases_state(self):
        import pty
        from remote_compute import Client, RemoteError
        with scratch() as directory:
            root = Path(directory)
            with provider(root/'worker') as (worker, remote):
                config = root/'config.json'
                config.write_text('{}', encoding='utf-8')
                nodes = root/'nodes.json'
                nodes.write_text(json.dumps({'nodes': [
                    {'name': 'isolated', 'url': remote.url}]}), encoding='utf-8')
                with socket.socket() as probe:
                    probe.bind(('127.0.0.1', 0))
                    port = probe.getsockname()[1]
                master, slave = pty.openpty()
                code = ('import fcntl,termios;fcntl.ioctl(0,termios.TIOCSCTTY,0);'
                        'from remote_compute.cli import main;raise SystemExit(main())')
                child = subprocess.Popen([sys.executable, '-B', '-c', code, '--service-child',
                    '--mode', 'scheduler', '--port', str(port), '--config', str(config),
                    '--nodes', str(nodes), '--root', str(root/'center'), '--no-dashboard'],
                    cwd=ROOT, stdin=slave, stdout=slave, stderr=slave, start_new_session=True)
                os.close(slave)
                client = Client('http://127.0.0.1:' + str(port), timeout=1)
                identity = None
                try:
                    def ready():
                        try:
                            return client.health(brief=True)['role'] == 'scheduler'
                        except RemoteError:
                            return False
                    until(ready, seconds=15)
                    identity = client.submit(python('import time;time.sleep(30)',
                                                    cpus=1, memory_mb=128))['id']
                    def running():
                        try:
                            return remote.status(identity, brief=True)['status'] == 'running'
                        except RemoteError as error:
                            if error.status_code == 404:
                                return False
                            raise
                    until(running)
                    os.close(master)
                    master = None
                    child.wait(timeout=10)
                    until(lambda: remote.status(identity, brief=True)['status'] == 'cancelled')
                    receipt = json.loads((root/'center/scheduler/jobs'/identity/'result.json').read_text())
                    self.assertEqual(receipt['status'], 'cancelled')
                    self.assertEqual(receipt['id'], identity)
                    from remote_compute.worker import lock_root
                    lock = lock_root(root/'center/scheduler', 'scheduler')
                    lock.close()
                finally:
                    try:
                        if identity:
                            remote.cancel(identity)
                            remote.wait(identity, wait_timeout=5)
                    finally:
                        if master is not None:
                            os.close(master)
                        if child.poll() is None:
                            child.wait(timeout=10)

    def test_all_service_entry_modes_reject_redirects_before_startup_side_effects(self):
        from remote_compute.cli import main
        for mode in ('scheduler', 'hybrid', 'worker'):
            for extra in ([], ['--headless'], ['--no-dashboard'], ['--service-child'], ['--service-child', '--headless']):
                with self.subTest(mode=mode, extra=extra), \
                        patch.object(terminal.sys.stdin, 'isatty', return_value=False), \
                        patch('remote_compute.cli.Worker') as worker, \
                        patch('remote_compute.cli.resolve') as resolve, \
                        io.StringIO() as output, redirect_stderr(output):
                    self.assertEqual(main(['--mode', mode, '--config', 'absent-device.json', *extra]), 2)
                    self.assertIn('可见终端', output.getvalue())
                    for action in (worker, resolve):
                        action.assert_not_called()

    def test_real_detached_launch_refuses_before_creating_root(self):
        with scratch() as directory:
            root = Path(directory) / 'must-not-be-created'
            for mode in ('scheduler', 'hybrid', 'worker'):
                result = subprocess.run([sys.executable, '-B', str(ROOT/'main.py'), '--mode', mode,
                    '--headless', '--port', '0', '--root', str(root)],
                    input='', capture_output=True, text=True, encoding='utf-8', timeout=10)
                self.assertEqual(result.returncode, 2, result.stderr)
                self.assertIn('可见终端', result.stderr)
                self.assertFalse(root.exists())

    def test_client_actions_keep_pipe_support_and_console_connection_is_checked(self):
        from remote_compute.cli import main
        with patch('remote_compute.cli.require_visible') as visible, \
                patch('remote_compute.cli.single_instance', return_value=nullcontext()), \
                patch('remote_compute.cli.run_scheduler', return_value=0):
            self.assertEqual(main(['--url', 'http://127.0.0.1:26133', 'jobs']), 0)
            visible.assert_not_called()
            self.assertEqual(main(['--url', 'http://127.0.0.1:26133']), 0)
            visible.assert_called_once_with()

    def test_headless_does_not_disable_the_terminal_dashboard(self):
        from remote_compute.cli import main
        with patch('remote_compute.cli.require_visible'), \
                patch('remote_compute.cli.single_instance', return_value=nullcontext()), \
                patch('remote_compute.cli.run_scheduler', return_value=0) as run:
            self.assertEqual(main(['--url', 'http://127.0.0.1:26133', '--headless']), 0)
            self.assertTrue(run.call_args.args[0].no_browser)
            self.assertFalse(run.call_args.args[0].no_dashboard)
            self.assertEqual(main(['--url', 'http://127.0.0.1:26133', '--no-dashboard']), 0)
            self.assertTrue(run.call_args.args[0].no_dashboard)

    def test_redirected_output_and_hidden_windows_are_rejected(self):
        with patch.object(terminal.sys.stdin, 'isatty', return_value=True), \
                patch.object(terminal.sys.stdout, 'isatty', return_value=False), \
                patch.object(terminal, '_windows_visible') as probe:
            with self.assertRaisesRegex(RuntimeError, '可见终端'):
                terminal.require_visible()
            probe.assert_not_called()
        if os.name == 'nt':
            with patch.object(terminal.sys.stdin, 'isatty', return_value=True), \
                    patch.object(terminal.sys.stdout, 'isatty', return_value=True), \
                    patch.object(terminal, '_windows_visible', return_value=False):
                with self.assertRaisesRegex(RuntimeError, '可见终端'):
                    terminal.require_visible()

    def test_posix_requires_foreground_and_attached_multiplexers(self):
        with patch.object(terminal.os, 'tcgetpgrp', return_value=42, create=True), \
                patch.object(terminal.os, 'getpgrp', return_value=42, create=True), \
                patch.object(terminal.sys.stdin, 'fileno', return_value=0), \
                patch.dict(os.environ, {}, clear=True):
            self.assertTrue(terminal._posix_visible())
            with patch.object(terminal.os, 'tcgetpgrp', return_value=43):
                self.assertFalse(terminal._posix_visible())
            with patch.dict(os.environ, {'TMUX': '/tmp/tmux/default,1,0', 'TMUX_PANE': '%2'}), \
                    patch.object(terminal.subprocess, 'run') as run:
                run.return_value.returncode, run.return_value.stdout = 0, '0\n'
                self.assertFalse(terminal._posix_visible())
                run.return_value.stdout = '1\n'
                self.assertTrue(terminal._posix_visible())
                self.assertIn('%2', run.call_args.args[0])
            with patch.dict(os.environ, {'STY': '123.dco'}), \
                    patch.object(terminal.subprocess, 'run') as run:
                run.return_value.stdout = '\t123.dco\t(Detached)\n'
                self.assertFalse(terminal._posix_visible())
                run.return_value.stdout = '\t123.dco\t(Attached)\n'
                self.assertTrue(terminal._posix_visible())

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
            with patch.object(startup.subprocess,'run',side_effect=AssertionError('WMI/subprocess in listener inspection')) if os.name=='nt' else nullcontext():
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
                        with patch.object(startup.sys.stdin, 'isatty', return_value=True), patch('builtins.input', return_value=''):
                            with self.assertRaises(startup.Cancelled):
                                startup.resolve('127.0.0.1', port, reuse_role='scheduler')
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
