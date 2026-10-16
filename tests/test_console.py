"""Operator-console behavior with the same command parser and keyboard loop."""
import argparse
from collections import deque
from contextlib import redirect_stderr, redirect_stdout
import ctypes
from ctypes import wintypes
import io
import os
import sys
import unittest
from unittest.mock import Mock, patch

from remote_compute.cli import Shell
from remote_compute.console import Dashboard, FocusRefresh, _enable_ansi, _input_view, _layout, _width


class Keyboard:
    def __init__(self, keys):
        self.keys = deque(keys)
        self.ready = False

    def kbhit(self):
        # One key per rendered frame, as when an operator types normally.
        self.ready = not self.ready
        return bool(self.keys) and self.ready

    def getwch(self):
        key = self.keys.popleft()
        if key is KeyboardInterrupt:
            raise KeyboardInterrupt
        return key


class OperatorShell(Shell):
    prompt = 'test> '

    def __init__(self):
        super().__init__()
        self.completed = 0

    def do_ping(self, _line):
        """ping: verify the operator command line remains usable."""
        self.completed += 1
        print('PING_OK')

    def do_bad(self, _line):
        parser = argparse.ArgumentParser(prog='bad')
        parser.add_argument('--required', required=True)
        parser.parse_args([])

    def do_interrupt(self, _line):
        raise KeyboardInterrupt


class ConsoleTests(unittest.TestCase):
    def test_focus_refresh_uses_one_or_thirty_seconds_and_updates_on_return(self):
        refresh=FocusRefresh()
        self.assertTrue(refresh.due(0,True))
        self.assertFalse(refresh.due(.9,True))
        self.assertTrue(refresh.due(1,True))
        self.assertFalse(refresh.due(2,False))
        self.assertFalse(refresh.due(30,False))
        self.assertTrue(refresh.due(31,False))
        self.assertTrue(refresh.due(31.1,True))
        self.assertFalse(refresh.due(31.9,True))
        self.assertTrue(refresh.due(32.1,True))
        self.assertTrue(refresh.due(32.2,False,force=True))
    def test_interrupt_help_and_parser_errors_keep_the_console_usable(self):
        keys = [*'discard', KeyboardInterrupt, *'ping\rhelp ping\rbad\rinterrupt\rping\rquit\r']
        keyboard, restore = Keyboard(keys), Mock()
        stream, errors = io.StringIO(), io.StringIO()
        with redirect_stdout(stream), redirect_stderr(errors):
            shell = OperatorShell()
            original = shell.stdout
            dashboard = Dashboard(shell, lambda: {'node': 'device', 'cpus': 3})
            with patch.dict(sys.modules, msvcrt=keyboard), \
                    patch('remote_compute.console._enable_ansi', return_value=restore), \
                    patch('remote_compute.console.time.sleep'), \
                    patch('remote_compute.console.shutil.get_terminal_size', return_value=os.terminal_size((80, 24))):
                self.assertTrue(dashboard.run())
            self.assertIs(sys.stdout, stream)
            self.assertIs(sys.stderr, errors)
            self.assertIs(shell.stdout, original)
        output = '\n'.join(dashboard.output)
        self.assertEqual(shell.completed, 2)
        self.assertIn('ping: verify', output)
        self.assertIn('error:', output)
        self.assertNotIn('Unknown syntax: discard', output)
        self.assertEqual(output.count('已返回操作终端'), 2)
        self.assertEqual(errors.getvalue(), '')
        self.assertIn('PING_OK', stream.getvalue())
        restore.assert_called_once()

    def test_output_history_can_be_paged_without_discarding_commands(self):
        keyboard = Keyboard(['\xe0', 'I', '\xe0', 'Q', *'quit\r'])
        with redirect_stdout(io.StringIO()) as stream:
            shell = OperatorShell()
            dashboard = Dashboard(shell, lambda: {'node': 'device', 'cpus': 3})
            for index in range(20):
                dashboard.write(f'log {index:02}\n')
            with patch.dict(sys.modules, msvcrt=keyboard), \
                    patch('remote_compute.console._enable_ansi', return_value=lambda: None), \
                    patch('remote_compute.console.time.sleep'):
                dashboard.run()
        self.assertIn('log 08', stream.getvalue())
        self.assertIn('log 19', stream.getvalue())
        self.assertIn('log 00', dashboard.output)

    def test_notices_and_command_feedback_survive_a_crowded_small_terminal(self):
        status = ['device resources ' + str(i) for i in range(35)]
        notices = ['file update ' + str(i) for i in range(6)]
        output = ['command output ' + str(i) for i in range(12)]
        rows = _layout(status, notices, output, 24)
        self.assertLessEqual(len(rows), 22)
        self.assertEqual(rows[0], status[0])
        for line in notices + output[-6:]:
            self.assertIn(line, rows)
        tiny = _layout(status, notices, output, 8)
        self.assertIn(notices[-1], tiny)
        self.assertIn(output[-1], tiny)

    def test_long_command_cursor_is_visible_at_both_ends_and_in_the_middle(self):
        line = 'put --node Aya1-TX "D:/工程目录/' + '很长的工程文件/' * 20 + 'last.py"'
        for width in (1, 8, 40, 110):
            for cursor in (0, 1, 32, len(line) // 2, len(line)):
                view, column = _input_view('调度> ', line, cursor, width)
                self.assertLessEqual(_width(view), width)
                self.assertGreaterEqual(column, 0)
                self.assertLess(column, width)
        view, _ = _input_view('调度> ', line, len(line), 40)
        self.assertTrue(view.endswith('last.py"'))
        self.assertIn('‹', view)

    def test_ansi_unavailable_uses_original_command_loop(self):
        class Terminal(io.StringIO):
            def isatty(self):
                return True
        with redirect_stdout(Terminal()), patch.object(sys.stdin, 'isatty', return_value=True), \
                patch('remote_compute.cli.os.name', 'nt'), \
                patch.dict(sys.modules, msvcrt=Keyboard([])), \
                patch('remote_compute.console._enable_ansi', return_value=None):
            shell = OperatorShell()
            with patch.object(shell, 'cmdloop') as plain:
                shell.loop(lambda: {'node': 'device'})
                plain.assert_called_once()

    def test_windows_console_handle_is_pointer_sized_and_mode_is_restored(self):
        handle = 2 ** 40 + 7
        def mode(_handle, value):
            self.assertEqual(_handle, handle)
            value._obj.value = 3
            return True
        kernel = Mock()
        kernel.GetStdHandle.return_value = handle
        kernel.GetConsoleMode.side_effect = mode
        kernel.SetConsoleMode.return_value = True
        with patch.object(ctypes, 'WinDLL', create=True, return_value=kernel):
            restore = _enable_ansi()
            self.assertEqual(kernel.GetConsoleMode.argtypes[0], wintypes.HANDLE)
            self.assertEqual(kernel.SetConsoleMode.argtypes[0], wintypes.HANDLE)
            kernel.SetConsoleMode.assert_called_with(handle, 7)
            restore()
            kernel.SetConsoleMode.assert_called_with(handle, 3)
            kernel.SetConsoleMode.return_value = False
            self.assertIsNone(_enable_ansi())


if __name__ == '__main__':
    unittest.main()
