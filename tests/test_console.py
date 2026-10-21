"""Operator-console behavior with the same command parser and keyboard loop."""
import argparse
from collections import deque
from contextlib import redirect_stderr, redirect_stdout
import ctypes
from ctypes import wintypes
import io
import os
import re
import sys
import threading
import unittest
from unittest.mock import Mock, patch

from remote_compute.cli import Shell
from remote_compute.console import (Dashboard, FocusRefresh, RowPainter, CommandCompletion,
    format_status, _enable_ansi, _initial_size, _input_view, _layout, _width, _clip,
    _command_rows, _console_size_probe, _paint_row)
from remote_compute.commands import SchedulerShell, WorkerShell


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

    def close(self):
        pass


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
    def setUp(self):
        keyboard=patch('remote_compute.console._PosixKeyboard',side_effect=lambda:sys.modules['msvcrt'])
        keyboard.start();self.addCleanup(keyboard.stop)

    def test_dashboard_colors_are_width_neutral_and_respect_no_color(self):
        import remote_compute.console as console
        with patch.object(console, '_COLOR_ENABLED', True):
            painted = _paint_row('任务运行 失败')
            self.assertIn('\x1b[', painted)
            self.assertTrue(painted.endswith('\x1b[0m'))
        with patch.object(console, '_COLOR_ENABLED', False):
            self.assertEqual(_paint_row('任务运行 失败'), '任务运行 失败')
        painter = RowPainter()
        with patch.object(console, '_COLOR_ENABLED', True):
            painter.frame(['任务运行'], 40, 1, 0)
        self.assertNotIn('\x1b[', painter.rows[0])

    def test_two_column_section_color_stops_before_task_content(self):
        import remote_compute.console as console
        jobs = [dict(id=f'a{i:03x}'+'b'*28, project='Bench', label='probe',
                     cpus=2, memory_mb=512, status='running') for i in range(20)]
        value = dict(role='scheduler', node='Aya1-PC', cpus=24, used_cpus=20,
                     nodes=[dict(name='Aya1-PC', capacity=24, active_jobs=jobs)],
                     projects={'Bench':dict(resource_share=1, used=20, target=24, queued=0)})
        blue, reset = '\x1b[34;1m', '\x1b[0m'
        with patch.object(console, '_COLOR_ENABLED', True):
            for width in (120, 160, 200):
                rows = format_status(value, width=width).fit(28)
                for title in ('启动基准', '工程份额', '运行能力'):
                    row = next(row for row in rows if row.startswith('+ '+title))
                    self.assertIn('probe', row)
                    boundary = len(_clip(row, 56))
                    painted = _paint_row(row)
                    self.assertEqual(painted, blue+row[:boundary]+reset+_paint_row(row[boundary:]))
                    self.assertNotIn(blue, painted[len(blue)+boundary+len(reset):])
                    self.assertIn('\x1b[35;1mAya1-PC'+reset, painted)
                    self.assertIn('probe', painted)
                    if width >= 160:
                        self.assertIn('\x1b[32m运行'+reset, painted)
                    self.assertEqual(re.sub(r'\x1b\[[0-9;]*m', '', painted), row)

    def test_adjacent_panel_titles_keep_their_own_color_spans(self):
        import remote_compute.console as console
        from remote_compute.view import _line, _rule
        blue, reset = '\x1b[34;1m', '\x1b[0m'
        right = _rule('当前任务 20 个 | Ctrl+Tab 翻页', 80)
        with patch.object(console, '_COLOR_ENABLED', True):
            left = _rule('设备资源', 56)
            row = left+right[1:]
            self.assertEqual(_paint_row(row), blue+left+reset+blue+right[1:]+reset)
            left = _line('Aya1-PC 运行', 56)[:-1]
            row = left+right
            self.assertEqual(_paint_row(row), _paint_row(left)+blue+right+reset)
            clipped = _rule('当前任务 20 个 | Ctrl+Tab 翻页', 20)
            self.assertEqual(_paint_row(clipped), blue+clipped+reset)

    def test_command_suggestions_use_cached_nodes_ids_and_real_parser_options(self):
        shell = object.__new__(SchedulerShell)
        shell.pool = Mock()
        identity = 'abcd' + '1'*28
        snapshot = dict(nodes=[dict(name='Aya1-PC', active_jobs=[dict(id=identity)]),
                               dict(name='Aya1-TX', active_jobs=[])])
        def suggest(line):
            return shell.suggest(line, len(line), snapshot)
        self.assertEqual(suggest('cpu')['matches'], ['cpus'])
        self.assertEqual(suggest('cpus Aya')['matches'], ['Aya1-PC','Aya1-TX'])
        self.assertEqual(suggest('logs Aya1-PC abc')['matches'], [identity])
        self.assertEqual(suggest('logs Aya1-TX abc')['matches'], [])
        self.assertEqual(suggest('jobs abc')['matches'], [identity])
        self.assertEqual(suggest('exec --node Aya')['matches'], ['Aya1-PC','Aya1-TX'])
        self.assertEqual(suggest('exec --node ')['matches'], ['Aya1-PC','Aya1-TX'])
        self.assertEqual(suggest('exec --no')['matches'], ['--node'])
        self.assertEqual(suggest('run --cpus ')['matches'], [])
        self.assertIn('--max-cpus', suggest('run --')['matches'])
        self.assertEqual(suggest('policy b')['matches'], ['balanced'])
        self.assertIn('[节点] N', suggest('cpus Aya1-PC 12')['usage'])
        self.assertEqual(suggest('logs abc')['matches'], [identity])
        shell.pool.assert_not_called()
        worker = WorkerShell(Mock())
        local = dict(active_jobs=[dict(id=identity)])
        self.assertEqual(worker.suggest('logs ab', 7, local)['matches'], [identity])
        self.assertIn('all', worker.suggest('cancel ', 7, local)['matches'])
        self.assertEqual(worker.suggest('cpus ', 5, local)['matches'], [])

    def test_operator_short_ids_local_resource_defaults_and_actionable_errors(self):
        identity='abcd'+'1'*28
        center=Mock();pool=Mock()
        center.status.return_value=[dict(id=identity)]
        center.nodes.return_value=dict(policy='adaptive',nodes=[])
        center.logs.return_value=dict(stdout='ready')
        center.cancel.return_value=dict(status='cancelled')
        center.fetch.return_value=dict(status='succeeded',output_dir='D:/结果')
        shell=SchedulerShell(pool,center)
        try:
            output=io.StringIO()
            with redirect_stdout(output),patch('remote_compute.commands.socket.gethostname',return_value='local-device'):
                shell.onecmd('cpus 12')
                center.control.assert_called_with(node='local-device',worker={'cpus':12})
                shell.onecmd('memory 1.5G')
                center.control.assert_called_with(node='local-device',worker={'memory_mb':1536})
                shell.onecmd('logs abcd')
                center.logs.assert_called_with(identity)
                shell.onecmd('fetch abcd "D:/结果"')
                center.fetch.assert_called_with(identity,'D:/结果')
                shell.onecmd('cancel local-device abcd')
                center.cancel.assert_called_once_with(identity)
                center.status.return_value=[dict(id=identity),dict(id='abcd'+'2'*28)]
                shell.onecmd('cancel abcd')
                self.assertEqual(center.cancel.call_count,1)
                shell.onecmd('cpus local-device wrong')
                shell.onecmd('cpuss 12')
                shell.onecmd('help')
            self.assertIn('ID匹配多个任务',output.getvalue())
            self.assertIn('用法：cpus',output.getvalue())
            self.assertIn('你可能要输入 cpus',output.getvalue())
            self.assertIn('cpus 16',output.getvalue())
        finally:
            shell.close()

    def test_completion_extends_prefix_cycles_both_directions_and_preserves_suffix(self):
        shell = object.__new__(SchedulerShell)
        shell.pool = Mock()
        snapshot = dict(nodes=[dict(name='Aya1-PC'),dict(name='Aya1-TX')])
        completion = CommandCompletion()
        def complete(line, cursor=None, reverse=False):
            cursor = len(line) if cursor is None else cursor
            return completion.apply(line, cursor, shell.suggest(line,cursor,snapshot), reverse=reverse)
        line, cursor = complete('cpu')
        self.assertEqual((line,cursor), ('cpus ',5))
        line, cursor = complete('cpus Aya')
        self.assertEqual(line, 'cpus Aya1-')
        line, cursor = complete(line,cursor)
        self.assertEqual(line, 'cpus Aya1-PC')
        line, cursor = complete(line,cursor)
        self.assertEqual(line, 'cpus Aya1-TX')
        line, cursor = complete(line,cursor,reverse=True)
        self.assertEqual(line, 'cpus Aya1-PC')
        completion.last = None
        line, cursor = complete('cpus Aya1-TX 12', len('cpus Aya1-T'))
        self.assertEqual(line, 'cpus Aya1-TX 12')
        self.assertEqual(cursor,len('cpus Aya1-TX'))
        completion.last = None
        self.assertEqual(complete('unknown'), ('unknown',7))

    def test_footer_shows_selected_candidate_short_ids_and_command_usage(self):
        shell, completion = OperatorShell(), CommandCompletion()
        matches = [f'{i:04x}' + 'a'*28 for i in range(30)]
        context = dict(usage='jobs [任务ID]', matches=matches)
        completion.matches, completion.index, completion.last = matches, 29, ('jobs f',6)
        rows, column = _command_rows(shell,'jobs f',6,79,context,completion)
        self.assertEqual(len(rows),4)
        self.assertIn('[001d]',rows[1])
        self.assertNotIn('a'*28,rows[1])
        self.assertIn('jobs [任务ID]',rows[0])
        self.assertTrue(rows[-1].startswith(shell.prompt))
        self.assertEqual(column,len(shell.prompt)+6)
        self.assertTrue(all(_width(row)<=79 for row in rows))

    def test_input_and_four_row_footer_stay_inside_small_and_resized_windows(self):
        jobs = [dict(id=f'{i:04x}'+'b'*28, label='long-label/'*20, cpus=1, status='running') for i in range(70)]
        health = dict(node='Aya1-PC',role='worker',cpus=24,active_jobs=jobs,
                      events=[dict(time=i,ip='local',message='notice '+str(i)) for i in range(8)])
        original = RowPainter.frame
        for columns, height in ((80,1),(80,3),(80,4),(44,8),(80,16),(120,24),(160,51)):
            with self.subTest(size=(columns,height)), redirect_stdout(io.StringIO()):
                frames = []
                def paint(painter, rows, width, count, cursor):
                    self.assertEqual(len(rows),height)
                    self.assertTrue(rows[-2 if height >= 5 else -1].startswith('test> '))
                    self.assertTrue(all(_width(row)<=columns-1 for row in rows[-min(4,height):]))
                    frames.append(list(rows))
                    return original(painter,rows,width,count,cursor)
                dashboard = Dashboard(OperatorShell(),lambda:health)
                for i in range(30):
                    dashboard.write(f'output {i}\n')
                with patch.dict(sys.modules,msvcrt=Keyboard([*'quit\r'])), \
                        patch('remote_compute.console._enable_ansi',return_value=lambda:None), \
                        patch('remote_compute.console.time.sleep'), \
                        patch('remote_compute.console.shutil.get_terminal_size',return_value=os.terminal_size((columns,height))), \
                        patch.object(RowPainter,'frame',paint):
                    dashboard.run()
                self.assertGreater(len(frames),1)
                if height>=4:
                    usage_index = -5 if height >= 5 else -4
                    choice_index = -4 if height >= 5 else -3
                    self.assertIn('用法:',frames[0][usage_index])
                    self.assertIn('候选:',frames[0][choice_index])

    def test_typing_tab_completes_command_without_changing_task_page(self):
        with redirect_stdout(io.StringIO()) as stream:
            shell = OperatorShell()
            with patch.dict(sys.modules,msvcrt=Keyboard(['\t',*'pi\t\rquit\r'])), \
                    patch('remote_compute.console._enable_ansi',return_value=lambda:None), \
                    patch('remote_compute.console.time.sleep'):
                Dashboard(shell, lambda:dict(role='worker',cpus=24)).run()
        self.assertEqual(shell.completed,1)
        self.assertNotIn('Unknown syntax',stream.getvalue())
        self.assertIn('PING_OK',stream.getvalue())

    def test_live_ui_handoff_keeps_input_history_and_painter_without_clearing_screen(self):
        with redirect_stdout(io.StringIO()) as stream:
            shell = OperatorShell()
            painter = RowPainter()
            state = dict(line='pi',cursor=2,history=['ping'],selected=1,painter=painter)
            dashboard = Dashboard(shell,lambda:dict(role='worker',cpus=24))
            dashboard.write('earlier output\n')
            with patch.dict(sys.modules,msvcrt=Keyboard(['\t','\r','\xe0','H','\r',*'quit\r'])), \
                    patch('remote_compute.console._enable_ansi',return_value=lambda:None), \
                    patch('remote_compute.console.time.sleep'):
                dashboard.run(state)
        self.assertEqual(shell.completed,2)
        self.assertIn('earlier output','\n'.join(dashboard.output))
        self.assertTrue(painter.rows[-2].startswith('test> quit'))
        for code in ('\x1b[2J','\x1b[?1049h','\x1b[?1049l'):
            self.assertNotIn(code,stream.getvalue())

    def test_window_resize_reflows_content_without_losing_the_bottom_input(self):
        geometry, seen = [160,51], []
        original = RowPainter.frame
        def paint(painter, rows, columns, height, cursor):
            self.assertEqual(len(rows),height)
            self.assertTrue(rows[-2].startswith('test> '))
            seen.append((columns,height))
            result=original(painter,rows,columns,height,cursor)
            geometry[:] = (80,12) if len(seen)==1 else (120,24)
            return result
        with redirect_stdout(io.StringIO()), \
                patch.dict(sys.modules,msvcrt=Keyboard([*'quit\r'])), \
                patch('remote_compute.console._enable_ansi',return_value=lambda:None), \
                patch('remote_compute.console.time.sleep'), \
                patch('remote_compute.console.shutil.get_terminal_size',side_effect=lambda *_:os.terminal_size(geometry)), \
                patch.object(RowPainter,'frame',paint):
            Dashboard(OperatorShell(),lambda:dict(role='worker',cpus=24)).run()
        self.assertEqual(seen[:3],[(160,51),(80,12),(120,24)])

    def test_visible_viewport_ignores_large_buffer_and_environment_dimensions(self):
        kernel = Mock()
        kernel.GetStdHandle.return_value = 2**40+7
        def fill(_handle, ptr):
            info=ptr._obj
            info.size.x,info.size.y=180,9000
            info.window.left,info.window.top,info.window.right,info.window.bottom=0,8,119,31
            return True
        kernel.GetConsoleScreenBufferInfo.side_effect=fill
        with patch.dict(os.environ,LINES='9000',COLUMNS='180'), patch.object(ctypes,'WinDLL',create=True,return_value=kernel):
            probe=_console_size_probe()
            self.assertEqual(probe(),os.terminal_size((120,24)))
            kernel.GetConsoleScreenBufferInfo.assert_called_with(kernel.GetStdHandle.return_value,unittest.mock.ANY)

    def test_unused_status_rows_go_to_command_feedback_before_blank_padding(self):
        status=format_status(dict(role='worker',node='device',cpus=3),width=109)
        rows=_layout(status,[],[f'log {i}' for i in range(20)],32,reserved=4)
        self.assertLessEqual(len(rows),28)
        for i in range(14,20):
            self.assertIn(f'log {i}','\n'.join(rows))

    def test_direct_device_shows_public_ip(self):
        value=dict(role='scheduler',node='Aya1-PC',nodes=[
            dict(name='Server',url='https://203.0.113.42:26133',
                 capacity=96,peers=[dict(ip='192.0.2.10')])])
        text='\n'.join(format_status(value,width=159).fit(38))
        self.assertIn('203.0.113.42',text)
        self.assertNotIn('本机',text)
        self.assertIn('203.0.113.42',text)

    def test_device_ip_is_distinct_from_visitors_and_keeps_complete_ipv4(self):
        value=dict(role='scheduler',node='Aya1-PC',nodes=[
            dict(name='Aya1-PC',url='local://Aya1-PC',capacity=24,peers=[dict(ip='local')]),
            dict(name='Aya1-TX',url='http://192.168.100.100:26133',capacity=30,
                 peers=[dict(ip='127.0.0.1'),dict(ip='192.168.1.6')])])
        text='\n'.join(format_status(value,width=159).fit(38))
        self.assertIn('设备IP',text)
        self.assertIn('192.168.100.100',text)
        self.assertNotIn('连接方向',text)
        self.assertNotIn('127.0.0.1',text)
        worker=dict(node='Aya1-TX',role='worker',cpus=30,host_ip='192.168.1.10')
        self.assertIn('192.168.1.10','\n'.join(format_status(worker,url='http://0.0.0.0:26133',width=159)))
        worker['host_ip']=None
        self.assertIn('等待连接','\n'.join(format_status(worker,url='http://0.0.0.0:26133',width=159)))

    def test_center_and_worker_notices_merge_by_operation_and_keep_executing_device(self):
        from remote_compute.view import format_notices
        center=[dict(time=10,ip='127.0.0.1',message='upload main.py [aafe1122]'),
                dict(time=12,ip='127.0.0.1',message='upload main.py [aafe3344]')]
        worker=[dict(time=10.2,ip='192.168.1.6',message='upload main.py [aafe1122]'),
                dict(time=12.2,ip='192.168.1.6',message='upload main.py [aafe3344]'),
                dict(time=13,ip='terminal',message='CPU budget 30'),
                dict(time=14,ip='terminal',message='CPU budget 30')]
        rows=format_notices(dict(node='Aya1-PC',events=center,nodes=[dict(name='Aya1-TX',events=worker)]))
        self.assertEqual(len(rows),4)
        self.assertEqual(sum('upload main.py' in row for row in rows),2)
        self.assertTrue(all('Aya1-TX 192.168.1.6' in row for row in rows[:2]))
        self.assertTrue(all('[aafe]' in row for row in rows[:2]))

    def test_notification_numeric_short_hash_keeps_identifier_color(self):
        import remote_compute.console as console
        from remote_compute.view import format_notices
        with patch.object(console,'_COLOR_ENABLED',True):
            for prefix in ('1234','0010','9876','1a34'):
                message='submit ['+prefix+'a'*28+'] CPU 12 RAM 4096'
                row=format_notices(dict(node='Aya1-PC',events=[dict(time=10,ip='local',message=message)]))[0]
                painted=_paint_row(row)
                self.assertIn('[\x1b[36m'+prefix+'\x1b[0m]',painted)
                self.assertIn('RAM \x1b[33m4096\x1b[0m',painted)
                self.assertEqual(re.sub(r'\x1b\[[0-9;]*m','',painted),row)

    def test_jobs_prefix_selects_one_record_and_lists_colliding_ids(self):
        from remote_compute.commands import SchedulerShell
        identities=['1234'+'a'*28,'1234'+'b'*28]
        records=[dict(id=identity,status='running',label='test-label',cpus=1) for identity in identities]
        client=Mock()
        client.status.side_effect=lambda identity=None,**_kwargs: (next(r for r in records if r['id']==identity) if identity else records)
        shell=SchedulerShell(Mock(),client)
        try:
            with redirect_stdout(io.StringIO()) as output:
                shell.do_jobs('1234a')
            self.assertIn(identities[0],output.getvalue())
            self.assertNotIn(identities[1],output.getvalue())
            with redirect_stdout(io.StringIO()) as output:
                shell.do_jobs('1234')
            for identity in identities:
                self.assertIn(identity,output.getvalue())
            self.assertIn('匹配多个任务',output.getvalue())
        finally:
            shell.close()

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
            self.assertIn('当前任务 24 个',text)
            self.assertIn('aaaa',text)
            self.assertNotIn('aaaaaaaa',text)
            self.assertNotIn('m40_p125',text)

    def test_project_and_task_descriptions_are_compact_without_changing_request_labels(self):
        from remote_compute.view import _label, _project
        cases=[('rocket-nn-host','rocket-nn-host/boom/shufflenet_v2_x0_5/kitten/f4d61bbf5462da2d','NCNN','BOOM ShuffleV2-0.5 kitten'),
               ('rocket-nn-host','BOOM yolov8n-dog d9bc25af61428879','NCNN','BOOM YOLOv8n dog'),
               ('RGCN','RGCN/fsa-online-frozen-model-seed42','RGCN','FSA online-s42'),
               ('RGCN','RGCN/tsa-neural_previous_control-fresh','RGCN','TSA nn-prev'),
               ('RGCN','rgcn-tsa-pre','RGCN','TSA pre'),
               ('RGCN','RGCN/tsa/nn-prev/s42','RGCN','TSA/nn-prev/s42'),
               ('Analog-Opt-Bench','Analog/class_ab/FACE-MO/b96000','Bench','class_ab/FACE-MO/b96k'),
               ('Analog-Opt-Bench','Analog/panda-bookshelf_tall-0005/3x12000=36000 NFE budget','Bench','bookshelf_tall#5/3x12k'),
               ('Analog-Opt-Bench','Analog/panda-cage-0003/3x48000=144000 NFE budget','Bench','cage#3/3x48k'),
               ('Analog-Opt-Bench','Analog/native-mw11/6x12000=72000 NFE budget','Bench','native-mw11/6x12k'),
               ('AOP','aop/ngspice/LDO_SKY130_SSFF_m40_p125_4corners/ss125','AOP','LDO/ss125'),
               ('ARH-DSE','ARH-DSE/mac','ARH','mac'),
               ('ARH-DSE','BOOM NCNN unrolled native gate','ARH','BOOM NCNN 展开验收'),
               ('ARH-DSE','BOOM NCNN native gate','ARH','BOOM NCNN 原生验收'),
               ('yolov8-quant-lab','YOLO/integer-range-and-cost','YOLO','INT4 range/cost'),
               ('RGCN','RGCN','RGCN','-')]
        for project,label,source,description in cases:
            job=dict(project=project,label=label)
            self.assertEqual((_project(project),_label(job)),(source,description))
            self.assertEqual(job['label'],label)
            self.assertLessEqual(_width(description),26)

    def test_resource_headers_and_parent_labels_fit_without_midword_elision(self):
        job=dict(id='abcd'+'f'*28,project='Bench',cpus=1,memory_mb=1024,status='running',
                 label='Analog/panda-bookshelf_tall-0005/3x12000=36000 NFE budget')
        value=dict(role='scheduler',node='Aya1-PC',cpus=14,used_cpus=1,projects={},
                   nodes=[dict(name='Aya1-PC',capacity=14,logical_cpus=28,active_jobs=[job])])
        for width in (54,56,80,100,110,120,160,200):
            with self.subTest(width=width):
                rows=format_status(value,width=width).fit(42)
                text='\n'.join(rows)
                self.assertIn('进程/预留',text)
                self.assertIn('已用/预算(逻辑)',text)
                self.assertNotIn('进程...留',text)
                self.assertNotIn('已用...辑)',text)
                self.assertTrue(all(_width(row)==width for row in rows))
                if width>=100:self.assertIn('bookshelf_tall#5/3x12k',text)
        self.assertEqual(job['label'],'Analog/panda-bookshelf_tall-0005/3x12000=36000 NFE budget')

    def test_individual_tasks_keep_four_character_ids_across_pages_and_two_columns(self):
        jobs=[dict(id=f'{i:04x}'+'a'*28,project='Bench',label='very-long-circuit-name/'*6+f'seed-{i}',
                   cpus=2,memory_mb=512,status='running') for i in range(43)]
        value=dict(role='scheduler',node='Aya1-PC',cpus=54,used_cpus=24,
                   nodes=[dict(name='Aya1-PC',capacity=24,logical_cpus=28,active_jobs=jobs[:20]),
                          dict(name='Aya1-TX',capacity=30,logical_cpus=32,active_jobs=jobs[20:])],
                   projects={'Bench':dict(resource_share=1,used=24,target=54,queued=3)})
        for width in (44,80,110,120,160,200):
            status=format_status(value,width=width)
            first=status.fit(28)
            pages=status.page_count
            self.assertGreater(pages,1)
            seen=[]
            for page in range(pages):
                status.task_page=page
                rows=status.fit(28)
                self.assertTrue(all(_width(row)==width for row in rows))
                self.assertLessEqual(len(rows),28)
                for row in rows:
                    seen.extend(job['id'][:4] for job in jobs if any(prefix+job['id'][:4]+' |' in row for prefix in ('| ','+ ')))
            self.assertCountEqual(seen,[job['id'][:4] for job in jobs])
            self.assertIn('1/2','\n'.join(first))
            self.assertNotIn('温度','\n'.join(first))
            self.assertNotIn('在线','\n'.join(first))
            if width>=120:
                self.assertTrue(any('设备资源' in row and '当前任务' in row for row in first))

    def test_tab_changes_task_pages_while_preserving_input_and_output_history(self):
        keyboard=Keyboard(['\t','\x00','\x0f',*'ping\rquit\r'])
        jobs=[dict(id=f'{i:04x}'+'b'*28,label=f'unique-task-{i}',cpus=1,status='running') for i in range(50)]
        with redirect_stdout(io.StringIO()) as stream:
            shell=OperatorShell()
            dashboard=Dashboard(shell,lambda: dict(node='Aya1-PC',role='worker',cpus=24,active_jobs=jobs))
            with patch.dict(sys.modules,msvcrt=keyboard), \
                    patch('remote_compute.console._enable_ansi',return_value=lambda: None), \
                    patch('remote_compute.console.time.sleep'), \
                    patch('remote_compute.console.shutil.get_terminal_size',return_value=os.terminal_size((160,32))):
                dashboard.run()
            self.assertRegex(stream.getvalue(),r'1/\d+页')
            self.assertRegex(stream.getvalue(),r'2/\d+页')
            self.assertEqual(shell.completed,1)

    def test_elapsed_seconds_sort_globally_before_pagination_and_leave_unstarted_last(self):
        jobs=[dict(id=f'{i:04x}'+'a'*28,project='Bench',label=f'task-{i}',cpus=2,
                   status='running',started=1000-i*10) for i in range(35)]
        waiting=dict(id='ffff'+'b'*28,label='not-started',cpus=1,status='dispatching',started=None)
        value=dict(role='scheduler',nodes=[dict(name='Aya1-PC',active_jobs=[waiting,*jobs[::2]]),
                                          dict(name='Aya1-TX',active_jobs=jobs[1::2])],projects={})
        with patch('remote_compute.view.time.time',return_value=1200.9):
            for width in (44,64,80,110,120,160,200):
                status=format_status(value,width=width)
                status.fit(22)
                self.assertGreater(status.page_count,1)
                self.assertEqual([row[0] for row in status.records],
                                 [job['id'][:4] for job in reversed(jobs)]+['ffff'])
                self.assertEqual([status.records[0][7],status.records[-2][7],status.records[-1][7]],
                                 ['540','200','-'])
                seen=[]
                for page in range(status.page_count):
                    status.task_page=page
                    rows=status.fit(22)
                    self.assertTrue(all(_width(row)==width for row in rows))
                    self.assertIn('运行秒','\n'.join(rows))
                    for row in rows:
                        seen.extend(record[0] for record in status.records
                                    if any(prefix+record[0]+' |' in row for prefix in ('| ','+ ')))
                self.assertEqual(seen,[row[0] for row in status.records])

    def test_console_grows_to_screen_limit_without_shrinking_existing_window(self):
        kernel,user=Mock(),Mock()
        user.IsWindowVisible.return_value=True
        def information(_handle,pointer):
            value=pointer._obj
            value.size.x,value.size.y=120,300
            value.window.right,value.window.bottom=119,29
            return True
        kernel.GetConsoleScreenBufferInfo.side_effect=information
        kernel.GetLargestConsoleWindowSize.side_effect=lambda _handle: kernel.GetLargestConsoleWindowSize.restype(150,44)
        with patch.object(ctypes,'WinDLL',create=True,side_effect=lambda name:kernel if name=='kernel32' else user):
            self.assertTrue(_initial_size())
        size=kernel.SetConsoleScreenBufferSize.call_args.args[1]
        window=kernel.SetConsoleWindowInfo.call_args.args[2]._obj
        self.assertEqual((size.x,size.y),(150,300))
        self.assertEqual((window.right,window.bottom),(149,43))
        user.IsWindowVisible.return_value=False
        kernel.SetConsoleScreenBufferSize.reset_mock()
        with patch.object(ctypes,'WinDLL',create=True,side_effect=lambda name:kernel if name=='kernel32' else user):
            self.assertFalse(_initial_size())
        kernel.SetConsoleScreenBufferSize.assert_not_called()

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

    @unittest.skipUnless(os.name == 'nt', 'Native Windows terminal ancestry')
    def test_hidden_console_focus_uses_native_snapshot_without_powershell(self):
        from remote_compute.console import _focus_probe
        kernel, user = Mock(), Mock()
        kernel.GetConsoleWindow.return_value = 100
        user.IsWindowVisible.return_value = False
        foreground = [40]
        def owner(window, process):
            process._obj.value = foreground[0]
        user.GetWindowThreadProcessId.side_effect = owner
        processes = {os.getpid():(40,'python.exe'),40:(9,'WindowsTerminal.exe'),9:(0,'explorer.exe')}
        with patch('ctypes.WinDLL', side_effect=lambda name, **kwargs: kernel if name=='kernel32' else user), \
                patch('remote_compute.terminal._windows_processes', return_value=processes) as snapshot, \
                patch('subprocess.run', side_effect=AssertionError('WMI must not run')):
            focused = _focus_probe()
            self.assertTrue(focused())
            foreground[0] = 9
            self.assertFalse(focused())
            snapshot.assert_called_once_with()

    def test_focus_refresh_uses_one_or_five_seconds_and_updates_on_return(self):
        refresh=FocusRefresh()
        self.assertTrue(refresh.due(0,True))
        self.assertFalse(refresh.due(.9,True))
        self.assertTrue(refresh.due(1,True))
        self.assertFalse(refresh.due(2,False))
        self.assertFalse(refresh.due(5.9,False))
        self.assertTrue(refresh.due(6,False))
        self.assertTrue(refresh.due(6.1,True))
        self.assertFalse(refresh.due(6.9,True))
        self.assertTrue(refresh.due(7.1,True))
        self.assertTrue(refresh.due(7.2,False,force=True))
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
        self.assertIn('<', view)

    def test_incremental_refresh_updates_only_changed_rows_without_scroll_or_clear(self):
        painter=RowPainter()
        rows=['header','CPU 10%','label and ID a12f','prompt> ']
        painter.frame(rows,80,4,8)
        self.assertEqual(painter.frame(rows,80,4,8),'')
        rows[1]='CPU 24%'
        frame=painter.frame(rows,80,4,8)
        self.assertIn('\x1b[2;1HCPU 24%\x1b[K',frame)
        for text in ('header','label and ID','\n','\r','\x1b[J','\x1b[2J'):
            self.assertNotIn(text,frame)
        self.assertIn('\x1b[4;9H',frame)
        self.assertEqual(painter.frame(rows,80,4,5),'\x1b[4;6H')
        resized=painter.frame(rows,90,4,5)
        self.assertIn('header',resized)
        self.assertNotIn('\n',resized)

    def test_ambiguous_characters_use_native_width_and_all_panel_edges_stay_fixed(self):
        from remote_compute.view import set_width_probe
        try:
            set_width_probe(lambda char:2 if char in '·×' else 1)
            self.assertEqual(_width('·×中|'),7)
            job=dict(id='abcd'+'f'*28,label='长标签/·符号/'+'corner-'*30+'终点',cpus=2,status='running')
            value=dict(role='scheduler',node='Aya1-PC',cpus=54,nodes=[dict(name='Aya1-PC',capacity=24,active_jobs=[job])])
            for width in (120,159,199):
                rows=format_status(value,width=width).fit(26)
                for row in rows:
                    self.assertEqual(_width(row),width)
                for row in rows[2:]:
                    self.assertIn(row[0],'+|')
                    self.assertIn(row[-1],'+|')
                    self.assertIn(_clip(row,56)[-1],'+|')
                self.assertIn('abcd','\n'.join(rows))
                self.assertNotIn('· Tab','\n'.join(rows))
        finally:
            set_width_probe(None)

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
        with patch('remote_compute.console.os.name','nt'),patch.object(ctypes, 'WinDLL', create=True, return_value=kernel):
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
