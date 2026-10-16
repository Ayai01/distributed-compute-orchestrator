"""Operator-console behavior with the same command parser and keyboard loop."""
import argparse
from collections import deque
from contextlib import redirect_stderr, redirect_stdout
import ctypes
from ctypes import wintypes
import io
import os
import sys
import threading
import unittest
from unittest.mock import Mock, patch

from remote_compute.cli import Shell
from remote_compute.console import Dashboard, FocusRefresh, format_status, _enable_ansi, _input_view, _layout, _width


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
    def test_plain_posix_console_exits_on_reload_without_waiting_for_input(self):
        import select
        shell=OperatorShell();alive=threading.Event();alive.set()
        def waiting(*_args):
            alive.clear()
            return [],[],[]
        shell.stdout=io.StringIO()
        with patch.object(select,'select',side_effect=waiting),patch.object(sys.stdin,'readline') as read:
            shell._poll_input(alive.is_set)
        read.assert_not_called()

    def test_command_tables_fit_inside_the_dashboard_and_keep_complete_ids(self):
        from remote_compute.commands import table
        identity = '1234567890abcdef'*2
        row = dict(node='Aya1-TX',id=identity,status='running',cpus=30,memory_mb=512,
                   label='长标签工程/电路仿真/'+'corner-validation-'*20+'ss27')
        for columns in (44,64,80,110,140):
            with patch('remote_compute.console.shutil.get_terminal_size',return_value=os.terminal_size((columns,32))):
                dashboard=Dashboard(OperatorShell(),lambda: {})
                with redirect_stdout(dashboard):
                    table([row],['node','id','status','label','cpus','memory_mb'])
                output=list(dashboard.output)
                self.assertTrue(all(_width(line)<=columns-5 for line in output))
                self.assertIn(identity,'\n'.join(output))
                self.assertIn('ss27','\n'.join(output))

    def test_framed_columns_fit_chinese_and_long_labels_at_multiple_widths(self):
        from remote_compute.view import _label
        job=dict(id='a'*32,project='AOP',label='check/AOP/aop/ngspice/BGR_SKY130_SSFF_m40_p125_4corners/ss27',
                 cpus=1,memory_mb=256,status='running')
        self.assertEqual(_label(job),'BGR/ss27')
        value=dict(role='scheduler',node='Aya1-PC',cpus=54,used_cpus=24,
            nodes=[dict(node='Aya1-PC',capacity=24,logical_cpus=28,active_jobs=[job]*24)],projects={})
        for width in (44,64,80,110,140):
            status=format_status(value,width=width)
            rows=_layout(status,['write D:/工程目录/'+'很长的目录/'*20+'main.py'],[],32)
            for row in rows:
                self.assertEqual(_width(row),width)
            text='\n'.join(rows)
            self.assertIn('BGR/ss27',text)
            self.assertIn('24 个 / 1 组',text)
            self.assertNotIn('m40_p125',text)

    def test_small_dashboard_keeps_both_devices_and_tasks_visible(self):
        nodes=[]
        for name in ('Aya1-PC','Aya1-TX'):
            nodes.append(dict(name=name,node=name,capacity=24,used_cpus=12,
                active_jobs=[dict(id=name+'12345678',label='task-'+name,cpus=1,status='running')]))
        status=format_status(dict(role='scheduler',node='Aya1-PC',nodes=nodes,cpus=54,used_cpus=24,
                                  projects={name:dict(resource_share=1,used=8,target=18,queued=2) for name in ('AOP','RGCN','ARH')}))
        rows=_layout(status,[f'notice {i}' for i in range(6)],[f'output {i}' for i in range(10)],24)
        text='\n'.join(rows)
        self.assertLessEqual(len(rows),22)
        for expected in ('task-Aya1-PC','task-Aya1-TX','notice 5','output 9','设备资源','工程份额','当前任务'):
            self.assertIn(expected,text)

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
        self.assertIn('log 14', stream.getvalue())
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
