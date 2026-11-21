"""Isolated public routing and launch checks for executable RAM scratch."""
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from remote_compute import Client, Pool, command
from remote_compute.ramdisk import ExecutableScratch
from remote_compute.cleanup import Reclaimer
from remote_compute.worker import ExecutableScratchUnavailable
from test_capability_packages import packaged_worker
from test_center import center
from test_compute import until


def task(amount=None):
    env = dict.fromkeys(('TMP', 'TEMP', 'TMPDIR', 'REMOTE_COMPUTE_JOB_TEMP'), 'unused-path') if amount else None
    return command(
        ['${tool:python}', '-B', '-c',
         'import json,os; paths=[os.environ[k] for k in ("TMP","TEMP","TMPDIR","REMOTE_COMPUTE_JOB_TEMP")]; '
         'assert len(set(paths))==1; print(json.dumps(paths[0]))'],
        env=env,
        requires=['rtl'], project='scratch-check', max_cpus=1, memory_mb=128,
        timeout=10, retries=0, executable_ram_mb=amount)


class ExecutableScratchTests(unittest.TestCase):
    def test_public_ram_requirement_selects_only_qualified_worker(self):
        with TemporaryDirectory(prefix='dco-executable-scratch-') as folder:
            root = Path(folder)
            ram = root / 'ram-scratch'
            ram.mkdir()
            with packaged_worker(root / 'disk', 'rtl') as (disk, disk_client), \
                    packaged_worker(root / 'ram', 'rtl') as (worker, ram_client):
                worker.ram_scratch = ExecutableScratch(ram)
                worker.temp_reclaimers[ram.resolve()] = Reclaimer(ram / '.trash')
                nodes = [dict(name='disk', client=disk_client), dict(name='ram', client=ram_client)]
                with patch.object(worker.ram_scratch, 'space', return_value=(512, 512)) as space, \
                        Pool(nodes, refresh=.05) as pool, center(pool, root / 'center') as (_, operator):
                    public = Client(operator.url, operator.token)
                    qualified = public.admission('scratch-check', requires=['rtl'], executable_ram_mb=256, activity=True)
                    ordinary = public.admission('scratch-check', requires=['rtl'])
                    self.assertEqual(qualified['executable_ram_mb'], 256)
                    self.assertEqual(qualified['capable_cpus'], 1)
                    self.assertEqual(ordinary['capable_cpus'], 2)
                    self.assertNotIn('nodes', qualified)
                    result = public.run(task(256), keep=True)
                    self.assertEqual(result['status'], 'succeeded', result)
                    self.assertIn(ram, Path(json.loads(result['stdout'])).resolve().parents)
                    self.assertEqual(operator.status(result['id'])['assigned_node'], 'ram')

                    space.return_value = (128, 128)
                    until(lambda: public.admission('scratch-check', requires=['rtl'],
                                                   executable_ram_mb=256)['capable_cpus'] == 0)
                    with patch.object(worker, '_start_process', wraps=worker._start_process) as spawn:
                        impossible = public.run(task(256), keep=True)
                        self.assertEqual(impossible['status'], 'failed', impossible)
                        self.assertEqual(impossible.get('cpu_seconds', 0), 0)
                        spawn.assert_not_called()

                    # No RAM requirement exercises the ordinary disk provider.
                    with patch.object(worker, 'paused', True):
                        until(lambda: next(row for row in operator.nodes()['nodes']
                                           if row['name'] == 'ram')['paused'])
                        result = public.run(task(), keep=True)
                    self.assertEqual(result['status'], 'succeeded', result)
                    self.assertEqual(operator.status(result['id'])['assigned_node'], 'disk')
                    self.assertNotIn(ram, Path(json.loads(result['stdout'])).resolve().parents)

    def test_ram_loss_at_launch_retries_under_original_without_paid_prefix(self):
        with TemporaryDirectory(prefix='dco-executable-ram-loss-') as folder:
            root = Path(folder)
            ram = root / 'ram-scratch'
            ram.mkdir()
            with packaged_worker(root / 'worker', 'rtl') as (worker, peer):
                worker.ram_scratch = ExecutableScratch(ram)
                worker.temp_reclaimers[ram.resolve()] = Reclaimer(ram / '.trash')
                require = worker._require_executable_ram
                checks = []

                def lose_at_launch(amount):
                    checks.append(amount)
                    if len(checks) == 2:
                        raise ExecutableScratchUnavailable()
                    return require(amount)

                with patch.object(worker.ram_scratch, 'space', return_value=(512, 512)), \
                        patch.object(worker, '_require_executable_ram', side_effect=lose_at_launch), \
                        Pool([peer], refresh=.05) as pool, center(pool, root / 'center') as (_, operator):
                    public = Client(operator.url, operator.token)
                    original = public.submit(dict(task(256), retries=1))['id']
                    public.wait(original, wait_timeout=12)
                    result = public.fetch(original, delete=False)

                self.assertEqual(result['id'], original)
                self.assertEqual(result['status'], 'succeeded', result)
                self.assertEqual(checks, [256, 256, 256, 256])
                self.assertEqual(len(result['attempts']), 1)
                self.assertEqual(result['attempts'][0].get('cpu_seconds', 0), 0)
                self.assertEqual(len(result['stdout'].splitlines()), 1)
                self.assertIn(ram, Path(json.loads(result['stdout'])).resolve().parents)
