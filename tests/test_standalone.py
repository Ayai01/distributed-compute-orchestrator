"""A copied worker starts alone and reloads updates over its existing connection."""

import json
import os
from pathlib import Path
import shutil
import socket
import sys
import unittest
from unittest.mock import patch

from remote_compute import Client, Pool, RemoteError
from remote_compute.cli import ROOT
from remote_compute.process import Process
from test_center import center
from test_compute import provider, python, scratch, until


class StandaloneTests(unittest.TestCase):
    def test_missing_projects_and_unreadable_execution_alias_do_not_block_worker(self):
        with scratch() as directory:
            root = Path(directory)
            with patch('remote_compute.worker.open', side_effect=OSError(22, 'Invalid argument')):
                with provider(root/'worker', shared={'old-project': root/'missing'},
                              tools={'old_python': str(root/'missing/python.exe')}) as (_, client):
                    health = client.health()
                    self.assertEqual(health['shared'], [])
                    self.assertIn('old-project', health['unavailable_shared'])
                    self.assertFalse(health['tools']['old_python']['available'])
                    self.assertIsNone(health['tools']['python']['sha256'])
                    result = client.run(python("print('independent worker')"))
                    self.assertEqual(result['status'], 'succeeded')
                    self.assertIn('independent worker', result['stdout'])
                    with self.assertRaisesRegex(RemoteError, 'unavailable'):
                        client.run(python('pass', env={'MODEL': '${shared:old-project}/model'}))

    def test_shared_path_tasks_only_route_to_nodes_with_that_path(self):
        with scratch() as directory:
            root = Path(directory)
            model = root/'models'
            model.mkdir()
            (model/'data').write_text('correct model')
            with provider(root/'bare', shared={'models': root/'missing'}) as (_, bare), \
                    provider(root/'ready', shared={'models': model}) as (_, ready):
                with Pool([bare, ready], policy='round_robin') as pool:
                    task = python("from pathlib import Path; import os; print(Path(os.environ['DATA']).read_text())",
                                  env={'DATA': '${shared:models}/data'})
                    result = pool.run(task)
                    self.assertEqual(result['assigned_node'], 'node2')
                    self.assertIn('correct model', result['stdout'])

    def test_copied_worker_can_be_updated_and_restarted_through_center(self):
        with scratch() as directory:
            root = Path(directory)
            copied = root/'copied-tool'
            copied.mkdir()
            shutil.copyfile(ROOT/'main.py', copied/'main.py')
            shutil.copytree(ROOT/'remote_compute', copied/'remote_compute', ignore=shutil.ignore_patterns('__pycache__'))
            config = {'shared': {'old-project': str(root/'missing')}, 'cpus': 1, 'memory_mb': 512}
            (copied/'worker.local.json').write_text(json.dumps(config), encoding='utf-8')
            with socket.socket() as probe:
                probe.bind(('127.0.0.1', 0))
                port = probe.getsockname()[1]
            client = Client(f'http://127.0.0.1:{port}', timeout=1)
            with (root/'worker.log').open('wb') as log:
                process = Process([sys.executable, '-B', str(copied/'main.py'), '--mode', 'worker', '--headless',
                                   '--port', str(port), '--root', str(root/'state')], copied,
                                  dict(os.environ, PYTHONPATH='', PYTHONUTF8='1'), log, log)
                try:
                    def ready():
                        try:
                            return 'restart' in client.health()['capabilities']
                        except RemoteError:
                            return False
                    until(ready, seconds=10)
                    initial_pid = client.health()['pid']
                    with Pool([client]) as pool, center(pool, root/'center') as (_, hub):
                        machine = hub.machine('node1', cwd=str(copied))
                        job = machine.python('import time; time.sleep(.5)', wait=False)
                        until(lambda: hub.status(job['id'])['status'] == 'running')
                        with self.assertRaisesRegex(RemoteError, 'active jobs'):
                            machine.restart()
                        self.assertEqual(hub.wait(job['id'])['status'], 'succeeded')
                        hub.fetch(job['id'])
                        source = machine.read_text('remote_compute/worker.py')
                        self.assertIn('"role": "worker",',source)
                        updated = source.replace('"role": "worker",',
                                                 '"role": "worker", "revision": "updated",',1)
                        machine.write_text('remote_compute/worker.py', updated)
                        self.assertNotIn('revision', client.health())
                        self.assertTrue(machine.restart()['restarting'])
                        def reloaded():
                            try:
                                health = client.health()
                                return health['pid'] != initial_pid and health.get('revision') == 'updated'
                            except RemoteError:
                                return False
                        until(reloaded, seconds=10)
                        self.assertIn('old-project', client.health()['unavailable_shared'])
                        self.assertIn('still reachable', machine.python("print('still reachable')")['stdout'])
                finally:
                    process.stop()
                    process.close()


if __name__ == '__main__':
    unittest.main()
