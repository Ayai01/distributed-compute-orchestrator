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
    def test_zero_cpu_budget_drains_and_survives_restart(self):
        source = "from pathlib import Path\nimport time\nwhile not Path('release').exists():time.sleep(.02)"
        with scratch() as directory:
            root = Path(directory)
            with provider(root/'one', cpus=2) as (worker, remote), provider(root/'two', cpus=2) as (_, second):
                with Pool([remote, second]) as pool:
                    scope = {'nodes':['node1'], 'reason':'Validate zero CPU scheduling'}
                    with center(pool, root/'center') as (_, client):
                        identity = client.submit(dict(python(source,max_cpus=2),placement_scope=scope))['id']
                        until(lambda:client.status(identity)['status']=='running' and client.health()['nodes'][0]['used_cpus']==2)
                        try:
                            view = client.request('/control/resources',dict(node='node1',changes={'cpus':0}))
                            row = view['nodes'][0]
                            self.assertEqual((row['configured']['cpus'],row['effective']['cpus'],row['actual']['cpus']),(0,0,2))
                            self.assertTrue(row['enabled'])
                            self.assertTrue(row['cpu_suspended'])
                            self.assertEqual(client.status(identity)['status'],'running')
                        finally:
                            client.write_job_file(identity,'release','yes')
                        self.assertEqual(client.wait(identity)['status'],'succeeded')
                        free = client.submit(python('print(2)'))['id']
                        self.assertEqual(client.wait(free)['assigned_node'],'node2')
                    with center(pool, root/'center') as (_, client):
                        row = client.health()['nodes'][0]
                        self.assertTrue(row['cpu_suspended'])
                        self.assertEqual(row['base_capacity'],0)
                        queued = client.submit(dict(python('print(1)'),placement_scope=scope))['id']
                        self.assertEqual(client.status(queued)['status'],'queued')
                        snapshot = client.control(node='node1',worker={'cpus':None})
                        self.assertFalse(snapshot['nodes'][0]['cpu_suspended'])
                        self.assertEqual(client.wait(queued)['assigned_node'],'node1')

    def test_resource_control_surface_persists_policy_and_keeps_active_grants(self):
        source="from pathlib import Path\nimport time\nprint('ready',flush=True)\nwhile not Path('release').exists():time.sleep(.02)"
        with scratch()as root,provider(Path(root)/'worker',cpus=4,memory_mb=4096)as(worker,remote),Pool([remote])as pool,center(pool,Path(root)/'center')as(_,client):
            identity=client.submit(python(source,max_cpus=4,memory_mb=512))['id']
            until(lambda:'ready'in client.logs(identity)['stdout'])
            until(lambda:worker.used==4)
            saved=[];worker.save_settings=saved.append
            changes=dict(cpus=2,cpu_limit=3,cpu_overcommit_factor=1.25,cpu_overcommit_utilization=70,
                memory_mb=256,memory_reserve_mb=512,memory_headroom_mb=256,memory_commit_headroom_mb=512)
            try:
                view=client.request('/control/resources',dict(node='node1',changes=changes))
                row=view['nodes'][0]
                self.assertEqual(row['name'],'node1');self.assertEqual(row['configured'],changes)
                self.assertEqual(saved,[changes]);self.assertEqual(row['effective']['base_cpus'],2)
                self.assertEqual(row['actual']['cpus'],4);self.assertEqual(row['restart_required'],[])
                self.assertNotIn('url',row);self.assertNotIn('active_jobs',row)
                self.assertEqual(client.resize(identity,4,active_cpus=4,min_cpus=4)['cpus'],4)
                self.assertEqual(client.status(identity)['id'],identity)
                self.assertEqual(client.status(identity)['status'],'running')
                for invalid in(dict(tools={'python':['wrong']}),dict(cpus=True),dict(cpu_overcommit_utilization=0)):
                    with self.subTest(invalid=invalid),self.assertRaises(RemoteError)as rejected:
                        client.request('/control/resources',dict(node='node1',changes=invalid))
                    self.assertEqual(rejected.exception.status_code,400)
                self.assertEqual(client.request('/control/resources')['nodes'][0]['configured'],changes)
            finally:client.write_job_file(identity,'release','yes')
            self.assertEqual(client.wait(identity)['status'],'succeeded')
            client.fetch(identity)
            until(lambda:worker.used==0)

    def test_device_ip_comes_from_accepted_connection_and_survives_loopback_probe(self):
        activity=Activity()
        self.assertIsNone(activity.snapshot()['host_ip'])
        activity.access('192.168.1.6','192.168.1.10')
        activity.access('127.0.0.1','127.0.0.1')
        self.assertEqual(activity.snapshot()['host_ip'],'192.168.1.10')
        self.assertEqual({peer['ip'] for peer in activity.snapshot()['peers']},{'192.168.1.6','127.0.0.1'})

    def test_compute_submissions_do_not_displace_operator_notices(self):
        activity=Activity()
        activity.notify('write source.py','192.168.1.6')
        for i in range(100):
            activity.submitted(dict(id=str(i),label='candidate',project='RGCN'),'192.168.1.6')
        activity.submitted(dict(id='abcdef12',label='machine/exec git',project='maintenance'),'192.168.1.6')
        events=activity.snapshot()['events']
        self.assertEqual(len(events),2)
        self.assertEqual(events[0]['message'],'write source.py')
        self.assertIn('machine/exec git',events[1]['message'])

    def test_device_config_can_lower_hard_cpu_ceiling(self):
        with scratch() as directory, patch('remote_compute.worker.os.cpu_count',return_value=28):
            with provider(Path(directory)/'worker',cpus=0,cpu_limit=24) as (worker,client):
                self.assertEqual(client.health()['cpus'],24)
                self.assertEqual(client.health()['cpu_limit'],24)
                with self.assertRaisesRegex(RemoteError,'0..24'):
                    client.control(cpus=25)
                client.control(cpus=8)
                saved=[]
                worker.save_settings=saved.append
                self.assertEqual(client.control(cpus=0)['cpus'],24)
                self.assertEqual(saved,[{'cpus':0}])
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
                identity = client.submit(python(source, max_cpus=1))['id']
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

    def test_cpu_default_and_ceiling_match_logical_threads(self):
        for count, expected in ((28, 28), (32, 32), (1, 1)):
            with patch('remote_compute.worker.os.cpu_count', return_value=count):
                self.assertEqual(cpu_budget(), expected)
                self.assertEqual(cpu_budget(expected), expected)
                with self.assertRaises(ValueError): cpu_budget(expected + 1)

    def test_notifications_are_bounded_and_dashboard_has_requested_fields(self):
        activity = Activity()
        activity.access('192.168.1.6', '192.168.1.10')
        for index in range(10): activity.notify('write source ' + str(index), '192.168.1.6')
        snapshot = activity.snapshot()
        self.assertEqual(len(snapshot['events']), 6)
        self.assertTrue(snapshot['events'][0]['message'].endswith('4'))
        text = '\n'.join(format_status(dict(node='Aya1-TX', role='worker', cpus=31, used_cpus=3,
                    logical_cpus=32, cpu_percent=40, memory_total_mb=32768, host_memory_available_mb=16000,
                    memory_available_mb=15000, temperature_c=52, tags=['ng47'], **snapshot)))
        for expected in ('Aya1-TX', '3/31', '32', '192.168.1.10', 'ng47', 'RAM'):
            self.assertIn(expected, text)
        self.assertNotIn('温度',text)

    def test_live_cpu_change_saves_settings_and_preserves_active_job(self):
        with scratch() as directory, provider(Path(directory) / 'worker', cpus=2) as (worker, client):
            settings = Path(directory) / 'worker.json'
            worker.save_settings = lambda changes: settings.write_text(json.dumps(changes))
            identity = client.submit(python('import time;time.sleep(.3)', max_cpus=2))['id']
            until(lambda: client.status(identity)['status'] == 'running')
            client.control(cpus=1)
            self.assertEqual(json.loads(settings.read_text()), {'cpus': 1})
            self.assertEqual(client.wait(identity)['status'], 'succeeded')
            self.assertEqual(client.health()['cpus'], 1)
