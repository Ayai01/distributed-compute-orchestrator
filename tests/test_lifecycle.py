"""Shared scratch and persisted operator controls survive concurrent starts/reloads."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from remote_compute import Client, Pool, RemoteError
from remote_compute import process as process_module, ramdisk, repository
from remote_compute.cli import ROOT, main
from remote_compute.process import Process, keep_awake
from test_center import center
from test_compute import provider, python, until, scratch
from service_fixture import write_entry


class ServicePowerTests(unittest.TestCase):
    def test_overlapping_cleanup_requests_share_only_new_process_snapshots(self):
        ready=threading.Event()
        class QueuedLock:
            def __init__(self):
                self.lock=threading.Lock();self.count_lock=threading.Lock();self.count=0
            def __enter__(self):
                with self.count_lock:
                    self.count+=1
                    if self.count==8:ready.set()
                self.lock.acquire()
            def __exit__(self,*args):self.lock.release()
        class Entry:
            name='123'
            def __truediv__(self,_):return SimpleNamespace(read_text=lambda:'123 (stub) S 1 2')
        scans=[]
        def entries():
            scans.append(1)
            if len(scans)==1:self.assertTrue(ready.wait(3))
            return [Entry()]
        with patch.object(process_module,'_proc_lock',QueuedLock()), \
                patch.object(process_module,'_proc_sample',(0.0,{})), \
                patch.object(process_module,'Path',return_value=SimpleNamespace(iterdir=entries)):
            with ThreadPoolExecutor(max_workers=8) as pool:
                results=list(pool.map(lambda _:process_module._proc_rows(fresh=True),range(8)))
            self.assertEqual(len(scans),2)
            self.assertTrue(all(result=={123:['S','1','2']} for result in results))
            process_module._proc_rows(fresh=True)
            self.assertEqual(len(scans),3)  # A later cleanup still gets a new scan.

    def test_reloaded_child_hands_old_supervisor_to_preferred_runtime_by_normal_exit(self):
        with patch('remote_compute.cli.require_visible'), \
                patch('remote_compute.setup_tools.preferred_python',return_value=Path('C:/preferred314/python.exe')), \
                patch('remote_compute.cli.subprocess.Popen') as launch, \
                patch('remote_compute.cli.Worker') as worker:
            self.assertEqual(main(['--service-child','--mode','worker','--config','device.json','--root','owned-root']),0)
        command=launch.call_args.args[0]
        self.assertEqual(command[0],'C:\\preferred314\\python.exe' if os.name=='nt' else 'C:/preferred314/python.exe')
        self.assertNotIn('--service-child',command)
        self.assertEqual(command[-6:],['--mode','worker','--config','device.json','--root','owned-root'])
        self.assertEqual(launch.call_args.kwargs['env']['PYTHONUTF8'],'1')
        worker.assert_not_called()

    def test_all_service_modes_hold_request_until_cleanup_and_supervisor_exit(self):
        for mode in ('scheduler', 'hybrid', 'worker'):
            for child in (False, True):
                with self.subTest(mode=mode, child=child), scratch() as directory:
                    root = Path(directory)
                    config = root / 'config.json'
                    config.write_text('{}', encoding='utf8')
                    events = []

                    @contextmanager
                    def awake():
                        events.append('awake')
                        try:
                            yield
                        finally:
                            events.append('released')

                    def running(*args, **kwargs):
                        self.assertEqual(events, ['awake'])
                        return 0

                    with patch('remote_compute.cli.require_visible'), \
                            patch('remote_compute.cli.keep_awake', awake), \
                            patch('remote_compute.cli.supervise', side_effect=running), \
                            patch('remote_compute.cli.run_scheduler', side_effect=running), \
                            patch('remote_compute.cli.WorkerShell.loop', side_effect=running):
                        args = ['--mode', mode, '--config', str(config), '--root', str(root/'worker'),
                                '--port', '0', '--cpus', '1']
                        if child:
                            args.append('--service-child')
                        self.assertEqual(main(args), 0)
                    self.assertEqual(events, ['awake', 'released'])

    def test_client_commands_do_not_hold_machine_awake(self):
        with patch('remote_compute.cli.require_visible'), \
                patch('remote_compute.cli.keep_awake') as awake, \
                patch('remote_compute.cli.run_scheduler', return_value=0):
            self.assertEqual(main(['--url', 'http://127.0.0.1:26133', 'nodes']), 0)
            self.assertEqual(main(['--url', 'http://127.0.0.1:26133']), 0)
        awake.assert_not_called()

    @unittest.skipUnless(os.name == 'nt', 'Windows execution-state ownership')
    def test_native_request_restores_previous_thread_state_after_exception(self):
        import ctypes as c
        from ctypes import wintypes as w
        state = c.WinDLL('kernel32').SetThreadExecutionState
        state.argtypes, state.restype = [w.DWORD], w.DWORD
        continuous, system, display = 0x80000000, 0x1, 0x2
        previous = state(continuous | display)
        self.assertTrue(previous)
        try:
            with self.assertRaisesRegex(RuntimeError, 'service stopped'):
                with keep_awake():
                    self.assertEqual(state(continuous | system), continuous | system)
                    raise RuntimeError('service stopped')
            self.assertEqual(state(continuous | display), continuous | display)
        finally:
            state(continuous | previous)


class LinuxWorkerScratchTests(unittest.TestCase):
    def test_worker_uses_job_tmpdir_instead_of_implicit_dev_shm(self):
        with patch.object(ramdisk.os, 'name', 'posix'), \
                patch.dict(os.environ, {'REMOTE_COMPUTE_WORKER': '1'}):
            self.assertIsNone(ramdisk.ensure_shared())


@unittest.skipUnless(os.name == 'nt', 'Windows shared ImDisk lifecycle')
class SharedRamdiskTests(unittest.TestCase):
    def test_concurrent_starts_create_one_marked_eight_gib_volume(self):
        # Use the real cross-project Win32 mutex, but emulate every disk operation.
        with tempfile.TemporaryDirectory(prefix='compute-ramdisk-') as directory:
            root = Path(directory)
            created = threading.Event()
            barrier = threading.Barrier(4)
            commands = []

            def disk_command(argv):
                commands.append(argv)
                self.assertIn('-a', argv)
                time.sleep(.03)
                created.set()
                return ''

            def path(value):
                return root if value == 'Z:/' else Path(value)

            def start():
                barrier.wait()
                return ramdisk.ensure_shared()

            with patch.object(ramdisk, 'Path', side_effect=path), \
                    patch.object(ramdisk, 'volumes', side_effect=lambda: [root] if created.is_set() else []), \
                    patch.object(ramdisk, 'is_ram', return_value=True), \
                    patch.object(ramdisk, 'driver', return_value='imdisk.exe'), \
                    patch.object(ramdisk, '_run', side_effect=disk_command), \
                    patch.object(ramdisk.shutil, 'disk_usage', return_value=SimpleNamespace(total=ramdisk.SIZE)):
                with ThreadPoolExecutor(max_workers=4) as executor:
                    result = list(executor.map(lambda _: start(), range(4)))
            self.assertEqual(result, [root] * 4)
            self.assertEqual(len(commands), 1)
            self.assertEqual(commands[0][commands[0].index('-s') + 1], '8G')
            marker = json.loads((root / ramdisk.MARKER).read_text(encoding='utf8'))
            self.assertEqual(marker['capacity_bytes'], 8 * 1024**3)
            self.assertTrue(marker['shared'])

    def test_four_gib_volume_extends_by_difference_and_preserves_files(self):
        with tempfile.TemporaryDirectory(prefix='compute-ramdisk-') as directory:
            root = Path(directory)
            cache = root / 'existing-cache.bin'
            cache.write_bytes(b'existing simulation evidence')
            capacity = 4 * 1024**3
            commands = []

            def disk_command(argv):
                nonlocal capacity
                commands.append(argv)
                if '-l' in argv:
                    return f'Size: {capacity} bytes\n'
                self.assertIn('-e', argv)
                self.assertEqual(int(argv[argv.index('-s') + 1]), 4 * 1024**3)
                capacity += int(argv[argv.index('-s') + 1])
                return ''

            with patch.object(ramdisk, 'shared_volume', return_value=root), \
                    patch.object(ramdisk, 'driver', return_value='imdisk.exe'), \
                    patch.object(ramdisk, '_run', side_effect=disk_command), \
                    patch.object(ramdisk.shutil, 'disk_usage', side_effect=lambda _: SimpleNamespace(total=capacity-4096)):
                self.assertEqual(ramdisk.ensure_shared(), root)
            self.assertEqual(capacity, ramdisk.SIZE)
            self.assertEqual(len(commands), 2)
            self.assertTrue(all('-a' not in command for command in commands))
            self.assertEqual(cache.read_bytes(), b'existing simulation evidence')

    def test_failed_resize_never_creates_a_second_volume(self):
        with tempfile.TemporaryDirectory(prefix='compute-ramdisk-') as directory:
            root = Path(directory)
            marker = root / ramdisk.MARKER
            marker.write_text('original marker', encoding='utf8')
            with patch.object(ramdisk, 'shared_volume', return_value=root), \
                    patch.object(ramdisk, 'driver', return_value='imdisk.exe'), \
                    patch.object(ramdisk, '_run', side_effect=['Size: 4294967296 bytes', OSError('resize failed')]) as run, \
                    patch.object(ramdisk.shutil, 'disk_usage', return_value=SimpleNamespace(total=4 * 1024**3)):
                with self.assertRaisesRegex(OSError, 'resize failed'):
                    ramdisk.ensure_shared()
            self.assertEqual(run.call_count, 2)
            self.assertTrue(all('-a' not in call.args[0] for call in run.call_args_list))
            self.assertEqual(marker.read_text(encoding='utf8'), 'original marker')


@unittest.skipUnless(shutil.which('git'), 'Repository update uses Git')
class RepositoryUpdateTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='compute-update-')
        self.addCleanup(temporary.cleanup)
        self.base = Path(temporary.name)
        self.source = self.base / 'origin'
        self.source.mkdir()
        self.git(self.source, 'init', '-b', 'main')
        self.configure(self.source)
        (self.source / '.gitignore').write_text('.tools/\nworker.local.json\n', encoding='utf8')
        (self.source / 'code.py').write_text('print(1)\n', encoding='utf8')
        self.git(self.source, 'add', '.')
        self.git(self.source, 'commit', '-m', 'initial')
        self.checkout = self.base / 'worker'
        self.git(self.base, 'clone', str(self.source), str(self.checkout))
        self.configure(self.checkout)
        self.root_patch = patch.object(repository, 'ROOT', self.checkout)
        self.url_patch = patch.object(repository, 'URL', str(self.source))
        self.root_patch.start()
        self.url_patch.start()
        self.addCleanup(self.root_patch.stop)
        self.addCleanup(self.url_patch.stop)

    def git(self, root, *argv):
        result = subprocess.run(['git', '-c', 'safe.directory=' + root.as_posix(), '-C', str(root), *argv],
            capture_output=True, text=True, encoding='utf8', errors='replace', check=True)
        return result.stdout.strip()

    def configure(self, root):
        self.git(root, 'config', 'user.name', 'Lifecycle Test')
        self.git(root, 'config', 'user.email', 'lifecycle@example.test')

    def change(self, root, value):
        (root / 'code.py').write_text('print(' + str(value) + ')\n', encoding='utf8')
        self.git(root, 'commit', '-am', 'code ' + str(value))
        return self.git(root, 'rev-parse', 'HEAD')

    def test_fast_forward_preserves_device_config_and_local_tools(self):
        before = self.git(self.checkout, 'rev-parse', 'HEAD')
        config = self.checkout / 'worker.local.json'
        config.write_text('{"cpus":31}', encoding='utf8')
        tool = self.checkout / '.tools' / 'runtime.bin'
        tool.parent.mkdir()
        tool.write_bytes(b'local tool')
        target = self.change(self.source, 2)
        check = repository.update(check=True)
        self.assertEqual((check['current'], check['available'], check['updated']), (before, target, False))
        result = repository.update()
        self.assertEqual((result['current'], result['available']), (target, target))
        self.assertTrue(result['updated'])
        self.assertTrue(result['restart_required'])
        self.assertEqual(config.read_text(encoding='utf8'), '{"cpus":31}')
        self.assertEqual(tool.read_bytes(), b'local tool')
        again = repository.update()
        self.assertFalse(again['updated'])
        self.assertFalse(again['restart_required'])

    def test_local_ahead_commit_is_preserved_and_reported(self):
        before = self.change(self.checkout, 3)
        with self.assertRaisesRegex(ValueError, 'push.*reconcile'):
            repository.update()
        self.assertEqual(self.git(self.checkout, 'rev-parse', 'HEAD'), before)

    def test_diverged_local_commit_is_preserved_and_reported(self):
        before = self.change(self.checkout, 3)
        self.change(self.source, 2)
        with self.assertRaisesRegex(ValueError, 'push.*reconcile'):
            repository.update()
        self.assertEqual(self.git(self.checkout, 'rev-parse', 'HEAD'), before)

    def test_uncommitted_source_is_preserved(self):
        self.change(self.source, 2)
        source = self.checkout / 'code.py'
        source.write_text('device work in progress\n', encoding='utf8')
        with self.assertRaisesRegex(ValueError, 'Commit local source changes'):
            repository.update()
        self.assertEqual(source.read_text(encoding='utf8'), 'device work in progress\n')


class ControlLifecycleTests(unittest.TestCase):
    def test_hybrid_reloads_without_console_relaunch_and_preserves_unfetched_results(self):
        with tempfile.TemporaryDirectory(prefix='compute-hybrid-reload-') as directory:
            root=Path(directory);copied=root/'tool';copied.mkdir()
            write_entry(copied)
            shutil.copytree(ROOT/'remote_compute',copied/'remote_compute',ignore=shutil.ignore_patterns('__pycache__'))
            config=copied/'worker.local.json'
            config.write_text(json.dumps(dict(mode='hybrid',cpus=2,root=str(root/'worker'),console=False)),encoding='utf8')
            with socket.socket() as probe:
                probe.bind(('127.0.0.1',0));port=probe.getsockname()[1]
            client=Client(f'http://127.0.0.1:{port}',timeout=1)
            with (root/'service.log').open('wb') as log:
                process=Process([sys.executable,'-B',str(copied/'main.py'),'--headless','--port',str(port),
                                 '--root',str(root/'state'),'--cpus=2'],copied,dict(os.environ,PYTHONPATH='',PYTHONUTF8='1'),log,log)
                try:
                    def ready():
                        try:return 'restart' in client.health().get('capabilities',[])
                        except RemoteError:return False
                    until(ready,seconds=15)
                    initial=client.health()
                    name=initial['node']
                    client.control(node=name,worker=dict(cpus=1))
                    pending=client.submit(python("import time;time.sleep(.6);print('retained output')"))['id']
                    until(lambda:client.status(pending)['status']=='running')
                    with self.assertRaises(RemoteError) as busy:client.restart()
                    self.assertEqual(busy.exception.status_code,409)
                    reloaded=copied/'remote_compute/__init__.py'
                    reloaded.write_text(reloaded.read_text(encoding='utf8').replace('__version__ = ', '__version__ = "reload-" + '),encoding='utf8')
                    self.assertTrue(client.restart(name,wait=True,wait_timeout=5)['restarting'])
                    def fresh():
                        try:return client.health()['pid']!=initial['pid']
                        except RemoteError:return False
                    until(fresh,seconds=15)
                    health=client.health()
                    self.assertTrue(health['version'].startswith('reload-'))
                    self.assertEqual(health['nodes'][0]['capacity'],1)
                    result=client.fetch(pending)
                    self.assertEqual(result['status'],'succeeded')
                    self.assertIn('retained output',result['stdout'])
                    self.assertEqual(client.run(python("print('after center reload')"))['status'],'succeeded')
                finally:
                    process.stop();process.close()

    @unittest.skipUnless((os.cpu_count() or 1) >= 3, 'Requires a two-thread worker')
    def test_supervisor_reloads_latest_live_budgets_instead_of_original_cli(self):
        with tempfile.TemporaryDirectory(prefix='compute-reload-') as directory:
            root = Path(directory)
            copied = root / 'tool'
            copied.mkdir()
            write_entry(copied)
            shutil.copytree(ROOT / 'remote_compute', copied / 'remote_compute',
                            ignore=shutil.ignore_patterns('__pycache__'))
            config = copied / 'worker.local.json'
            config.write_text(json.dumps({'console': False, 'tags': ['reload-check']}), encoding='utf8')
            with socket.socket() as probe:
                probe.bind(('127.0.0.1', 0))
                port = probe.getsockname()[1]
            client = Client(f'http://127.0.0.1:{port}', timeout=1)
            with (root / 'worker.log').open('wb') as log:
                process = Process([sys.executable, '-B', str(copied / 'main.py'), '--mode', 'worker',
                    '--headless', '--port', str(port), '--root', str(root / 'state'),
                    '--cpus=2', '--memory-mb', '512'], copied,
                    dict(os.environ, PYTHONPATH='', PYTHONUTF8='1'), log, log)
                try:
                    def ready():
                        try:
                            return 'restart' in client.health()['capabilities']
                        except RemoteError:
                            return False
                    until(ready, seconds=15)
                    initial = client.health()
                    self.assertEqual((initial['cpus'], initial['memory_capacity_mb']), (2, 512))
                    client.control(cpus=1, memory_mb=768)
                    saved = json.loads(config.read_text(encoding='utf8'))
                    self.assertEqual((saved['cpus'], saved['memory_mb']), (1, 768))
                    self.assertEqual(saved['tags'], ['reload-check'])
                    self.assertTrue(client.restart()['restarting'])

                    def reloaded():
                        try:
                            return client.health()['pid'] != initial['pid']
                        except RemoteError:
                            return False
                    until(reloaded, seconds=15)
                    health = client.health()
                    self.assertEqual((health['cpus'], health['memory_capacity_mb']), (1, 768))
                    self.assertIn('reload-check', health['tags'])
                    self.assertEqual(client.run(python("print('reloaded budget')"))['status'], 'succeeded')
                finally:
                    process.stop()
                    process.close()

    def test_job_file_states_and_binary_round_trip_through_center(self):
        with tempfile.TemporaryDirectory(prefix='compute-jobfiles-') as directory:
            root = Path(directory)
            with provider(root / 'worker', cpus=1) as (_, worker), Pool([worker]) as pool, \
                    center(pool, root / 'center') as (_, client):
                client.control(paused=True)
                task = python("from pathlib import Path; import time\n"
                    "while not Path('nested/control.bin').exists(): time.sleep(.02)\n"
                    "Path('echo.bin').write_bytes(Path('nested/control.bin').read_bytes())")
                identity = client.submit(task)['id']
                with self.assertRaises(RemoteError) as pending:
                    client.read_job_file(identity, 'missing.bin')
                self.assertEqual(pending.exception.status_code, 409)
                client.control(paused=False)
                until(lambda: client.status(identity)['status'] == 'running')
                with self.assertRaises(RemoteError) as missing:
                    client.read_job_file(identity, 'missing.bin')
                self.assertEqual(missing.exception.status_code, 404)
                payload = b'\x00\xff\x80binary\r\n'
                self.assertEqual(client.write_job_file(identity, 'nested/control.bin', payload)['bytes'], len(payload))
                self.assertEqual(client.wait(identity)['status'], 'succeeded')
                self.assertEqual(client.read_job_file(identity, 'echo.bin'), payload)
                client.fetch(identity)
                self.assertEqual(client.status(), [])
                self.assertEqual(worker.status(), [])


if __name__ == '__main__':
    unittest.main()
