"""Operator controls stay usable when every compute slot is occupied."""
import json
from pathlib import Path
import unittest
from unittest.mock import patch

from remote_compute import Pool, RemoteError
from remote_compute.console import format_status
from remote_compute.telemetry import Activity
from remote_compute.worker import cpu_budget
from test_center import center
from test_compute import provider, python, scratch, until


class OperatorTests(unittest.TestCase):
    def test_running_job_progress_and_stop_do_not_need_a_compute_slot(self):
        with scratch() as directory, provider(Path(directory) / 'worker', cpus=1) as (_, worker):
            with Pool([worker]) as pool, center(pool, Path(directory) / 'center') as (_, client):
                source = """from pathlib import Path
import time,os
Path('progress.json').write_text('working')
print(os.environ['REMOTE_COMPUTE_WORKER'],os.environ['REMOTE_COMPUTE_JOB_ID'],flush=True)
while not Path('control.json').exists(): time.sleep(.02)
print(Path('control.json').read_text(),flush=True)
"""
                identity = client.submit(python(source, cpus=1))['id']
                until(lambda: client.status(identity)['status'] == 'running')
                def ready():
                    try: return client.read_job_file(identity, 'progress.json') == b'working'
                    except RemoteError: return False
                until(ready)
                self.assertEqual(worker.health()['used_cpus'], 1)
                with self.assertRaises(RemoteError): client.write_job_file(identity, '../escape', 'x')
                client.write_job_file(identity, 'control.json', '{"stop":true}')
                result = client.wait(identity)
                self.assertEqual(result['status'], 'succeeded', result)
                self.assertIn(identity, result['stdout'])
                self.assertIn('"stop":true', result['stdout'])
                client.fetch(identity)

    def test_cpu_default_and_ceiling_keep_one_thread_for_host(self):
        for count, expected in ((28, 27), (32, 31), (1, 1)):
            with patch('remote_compute.worker.os.cpu_count', return_value=count):
                self.assertEqual(cpu_budget(), expected)
                self.assertEqual(cpu_budget(expected), expected)
                with self.assertRaises(ValueError): cpu_budget(expected + 1)

    def test_notifications_are_bounded_and_dashboard_has_requested_fields(self):
        activity = Activity()
        activity.access('192.168.1.6')
        for index in range(10): activity.notify('write source ' + str(index), '192.168.1.6')
        snapshot = activity.snapshot()
        self.assertEqual(len(snapshot['events']), 6)
        self.assertTrue(snapshot['events'][0]['message'].endswith('4'))
        text = '\n'.join(format_status(dict(node='Aya1-TX', role='worker', cpus=31, used_cpus=3,
                    logical_cpus=32, cpu_percent=40, memory_total_mb=32768, host_memory_available_mb=16000,
                    memory_available_mb=15000, temperature_c=52, tags=['ngspice'], **snapshot)))
        for expected in ('Aya1-TX', '3/31', '32', '52°C', '192.168.1.6', 'ngspice', '内存'):
            self.assertIn(expected, text)

    def test_live_cpu_change_saves_settings_and_preserves_active_job(self):
        with scratch() as directory, provider(Path(directory) / 'worker', cpus=2) as (worker, client):
            settings = Path(directory) / 'worker.json'
            worker.save_settings = lambda changes: settings.write_text(json.dumps(changes))
            identity = client.submit(python('import time;time.sleep(.3)', cpus=2))['id']
            until(lambda: client.status(identity)['status'] == 'running')
            client.control(cpus=1)
            self.assertEqual(json.loads(settings.read_text()), {'cpus': 1})
            self.assertEqual(client.wait(identity)['status'], 'succeeded')
            self.assertEqual(client.health()['cpus'], 1)
