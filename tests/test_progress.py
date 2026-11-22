"""Real work reports propagate without task control or invented denominators."""
import json
import os
from pathlib import Path
import threading
import unittest
from unittest.mock import patch

from remote_compute import Client,Pool,python_call,python_map,report_progress
from remote_compute.bundle import command_step_progress, progress_fields
from remote_compute.progress import SCHEMA,diagnose,normalize,sample,report_items
from remote_compute import liveboard
from remote_compute.view import _progress_label, _width, StatusRows
from test_compute import scratch, provider, python, until
from test_center import center


class ProgressTests(unittest.TestCase):
    def test_silent_native_stage_keeps_runtime_and_report_clocks_separate(self):
        p=normalize(dict(source='business',phase='link_model',completed=0,unit='links',
                         last_heartbeat=100,last_advanced=100))
        job=dict(id='original',status='running',progress=p,
                 runtime=dict(sampled_at=400,last_cpu_advanced=399,process_alive=True,cpu_seconds=352))
        d=diagnose(job,now=400)
        self.assertEqual((d['code'],d['level'],d['reason']),('cpu_active','info','暂未报告新进度'))
        self.assertEqual((d['worker_sample_age_seconds'],d['heartbeat_age_seconds'],d['cpu_advanced_age_seconds']),(0,300,1))
        self.assertTrue(d['process_alive']);self.assertTrue(d['business_report_stale']);self.assertTrue(d['progress_stale'])
        self.assertNotIn('total',p);self.assertNotIn('ratio',p);self.assertEqual(job['runtime']['cpu_seconds'],352)
        waiting={**job,'resource_request':dict(cpus=4)}
        self.assertEqual(diagnose(waiting,now=400)['code'],'resource_wait')
        stalled={**job,'runtime':dict(job['runtime'],last_cpu_advanced=100)}
        self.assertEqual(diagnose(stalled,now=400)['code'],'stalled')
        unknown={**job,'runtime':dict(sampled_at=400,process_alive=True)}
        self.assertEqual((diagnose(unknown,now=400)['code'],diagnose(unknown,now=400)['level']),('running','info'))
        self.assertIsNone(diagnose(unknown,now=400)['cpu_advanced_age_seconds'])
        stale={**job,'runtime':dict(job['runtime'],sampled_at=100)}
        self.assertEqual(diagnose(stale,now=400)['code'],'worker_sample_stale')
        self.assertEqual(diagnose(job,now=400,offline=True)['code'],'worker_unreachable')

    def test_compact_transport_preserves_invalid_report_evidence(self):
        from remote_compute.bundle import progress
        job=dict(id='original',status='running',progress_error='invalid_or_unreadable_progress',
                 runtime=dict(sampled_at=400,last_cpu_advanced=399,process_alive=True))
        compact=progress(job)
        self.assertEqual(diagnose(compact,now=400)['code'],'progress_invalid')
        self.assertEqual(diagnose(compact,now=400)['progress_error'],job['progress_error'])
        self.assertNotEqual(diagnose(dict(compact,progress_error=None),now=400)['code'],'progress_invalid')

    def test_terminal_and_web_reclassify_legacy_worker_diagnostic_from_actual_samples(self):
        job=dict(id='original',status='running',cpus=1,progress=normalize(dict(source='business',
            phase='native',completed=7,unit='cycles',last_heartbeat=100,last_advanced=100)),
            runtime=dict(sampled_at=400,last_cpu_advanced=399,process_alive=True),
            diagnostic=dict(code='heartbeat_lost',level='warning',cpu_active=True))
        node=dict(name='worker',capacity=4,base_capacity=4,reserved=1,active_jobs=[job])
        health=dict(role='scheduler',nodes=[node],queued=0,used_cpus=1)
        with patch('remote_compute.view.time.time',return_value=400):terminal=StatusRows(health)
        self.assertEqual(terminal.records[0][5],'计算中')
        value=liveboard.snapshot(health,[node],now=400)
        self.assertEqual(value['jobs'][0]['diagnostic']['code'],'cpu_active')
        self.assertFalse(value['alerts']);self.assertNotIn('total',value['jobs'][0]['progress'])
        self.assertEqual(value['jobs'][0]['progress']['completed'],7)

    def test_report_is_local_atomic_and_heartbeat_keeps_work_counters(self):
        with scratch()as root,patch.dict(os.environ,REMOTE_COMPUTE_JOB_DIR=str(root)):
            with patch('remote_compute.progress.time.time',return_value=100):
                self.assertTrue(report_progress(phase='native',completed=4,total=20,unit='cycles',counters={'MAC':300}))
            with patch('remote_compute.progress.time.time',return_value=105):self.assertTrue(report_progress())
            p=json.loads((Path(root)/'.dco-progress.json').read_text())
            self.assertEqual((p['completed'],p['total'],p['reported_at'],p['ratio']),(4,20,105,.2))
            self.assertEqual(p['counters'],{'MAC':300})
            self.assertEqual(list(Path(root).glob('.dco-progress-*')),[])

    def test_unknown_endpoint_has_no_percentage_or_step_denominator(self):
        p=normalize(dict(phase='replay',completed=200,unit='cycles',counters={'MAC':900}))
        fields=progress_fields(dict(progress=p,total_steps=1,steps=[]))
        self.assertNotIn('ratio',p);self.assertNotIn('total',fields)
        self.assertEqual(_progress_label(fields), '')

    def test_legacy_progress_shows_percentage_without_phase_only_text(self):
        self.assertEqual(_progress_label(dict(progress=dict(completed=1,total=4))),'25%')
        self.assertEqual(_progress_label(dict(progress=dict(phase='compile',completed=7))), '')

    def test_command_progress_validates_denominator_and_prefers_business_units(self):
        steps = dict(total_steps=2, steps=[{'returncode': 0}, {'started': 12}])
        report = normalize(dict(phase='native', completed=900, unit='cycles',
                                counters={'nominal_single_pass_cycles': 1000}))
        unknown = dict(id='original', progress=report, nominal_single_pass_cycles=1000)
        self.assertEqual(_progress_label(unknown), '')
        self.assertNotIn('ratio', liveboard.display_progress(unknown))
        mixed = dict(unknown, total=1000, current=5, target=10)
        self.assertEqual(_progress_label(mixed), '')
        dirty = dict(unknown, total_steps=2, steps=[{'finished': 'invalid'}, {'started': 12}])
        self.assertNotEqual(_progress_label(dirty), '50%')
        self.assertIsNone(liveboard.display_progress(dirty).get('ratio'))
        legacy_dirty = {key: dirty[key] for key in ('total_steps', 'steps')}
        self.assertNotIn('total', progress_fields(legacy_dirty))
        self.assertEqual(_progress_label(progress_fields(legacy_dirty)), '')
        self.assertIsNone(command_step_progress(dict(steps, total_steps=True)))
        self.assertIsNone(command_step_progress(dict(steps, total_steps=1)))
        self.assertIsNone(command_step_progress(dict(steps, step_progress={'completed': 1, 'total': 0},
                                                      total_steps=None)))
        fallback = dict(unknown, **steps)
        self.assertEqual(progress_fields(fallback)['step_progress'], dict(completed=1, total=2))
        self.assertEqual(_progress_label(fallback), '50%')
        self.assertEqual(liveboard.display_progress(fallback)['ratio'], .5)
        business = dict(fallback, progress=normalize(dict(completed=9, total=10, unit='cycles')))
        self.assertEqual(_progress_label(business), '90%')
        self.assertEqual(liveboard.display_progress(business)['ratio'], .9)
        self.assertNotIn('step_progress', progress_fields(business))

    def test_running_two_command_job_surfaces_one_completed_step_end_to_end(self):
        second = '''from pathlib import Path
from remote_compute import report_progress
from time import sleep
report_progress(phase='simulation', completed=900, unit='cycles',
                counters={'nominal_single_pass_cycles': 1000})
while not Path('release').exists():
    sleep(.05)
'''
        with scratch() as folder:
            root = Path(folder)
            with provider(root / 'worker') as (_, worker), Pool([worker]) as pool, \
                    center(pool, root / 'center') as (owner, client):
                task = python("print('first command finished')")
                task['steps'].append(python(second)['steps'][0])
                original = client.submit(task)['id']
                try:
                    def active(health):
                        rows = health['nodes'][0]['active_jobs'] if health['role'] == 'scheduler' else health['active_jobs']
                        return next((row for row in rows if row['id'] == original), {})

                    expected = dict(completed=1, total=2)
                    until(lambda: active(worker.health()).get('step_progress') == expected, seconds=10)
                    until(lambda: client.statuses([original])[original].get('step_progress') == expected, seconds=10)
                    self.assertEqual(client.statuses([original], compact=True)[original], {'status': 'running'})
                    until(lambda: active(owner.health()).get('step_progress') == expected, seconds=10)
                    health = owner.health()
                    rendered = liveboard.snapshot(health, health['nodes'])
                    row = next(row for row in rendered['jobs'] if row['id'] == original)
                    self.assertEqual((row['progress']['ratio'], row['progress']['unit']), (.5, 'steps'))
                    for width in (80, 100, 140):
                        with self.subTest(width=width):
                            lines = StatusRows(health, width=width)
                            task_row = next(line for line in lines if original[:4] in line and '50%' in line)
                            self.assertNotIn('1/2', task_row.split('|')[-2])
                            self.assertTrue(all(_width(line) <= width for line in lines))
                finally:
                    client.write_job_file(original, 'release', 'finish')
                self.assertEqual(client.wait(original, wait_timeout=10)['status'], 'succeeded')
                self.assertEqual(client.fetch(original)['id'], original)

    def test_invalid_work_reports_are_rejected_and_local_use_is_optional(self):
        with patch.dict(os.environ,{},clear=True):self.assertFalse(report_progress(phase='local'))
        for fields in (dict(completed=5,total=4),dict(completed=0,total=0),dict(counters={'n':float('nan')}),
                       dict(completed=True),dict(stale_after=0),dict(phase='\nsecret'),dict(counters={str(i):i for i in range(17)})):
            with self.subTest(fields=fields),self.assertRaises(ValueError):normalize(fields)

    def test_worker_distinguishes_report_heartbeat_from_actual_advancement(self):
        with scratch()as root,patch.dict(os.environ,REMOTE_COMPUTE_JOB_DIR=str(root)):
            job=dict(directory=Path(root),result=dict(status='running'))
            report_progress(phase='native',completed=1,counters={'MAC':1})
            job['result'].update(sample(job,1,now=100,force=True));a=job['result']['progress']['last_advanced']
            report_progress(metrics={'inference_cpu_seconds':80});job['result'].update(sample(job,2,now=105,force=True))
            self.assertEqual(job['result']['progress']['last_advanced'],a)
            report_progress(counters={'MAC':2});job['result'].update(sample(job,3,now=110,force=True))
            self.assertEqual(job['result']['progress']['last_advanced'],110)
            (Path(root)/'.dco-progress.json').write_text('{')
            job['result'].update(sample(job,4,now=115,force=True))
            self.assertEqual(job['result']['progress']['counters'],{'MAC':2})
            self.assertEqual(diagnose(job['result'],now=115)['code'],'progress_invalid')

    def test_diagnosis_separates_soft_observations_and_existing_failures(self):
        p=normalize(dict(completed=5,counters={},last_heartbeat=195,last_advanced=10,stale_after=60))
        base=dict(status='running',progress=p,runtime=dict(sampled_at=200,last_cpu_advanced=199,process_alive=True))
        cases=[(base,'cpu_active'),({**base,'runtime':dict(sampled_at=200,last_cpu_advanced=10)},'stalled'),
               ({**base,'status':'queued'},'queued'),({**base,'status':'dispatching'},'preparing'),
               ({**base,'progress':dict(p,last_heartbeat=10)},'cpu_active'),
               ({**base,'runtime':dict(sampled_at=10)},'worker_sample_stale'),
               ({**base,'runtime':dict(process_alive=False)},'process_lost'),
               ({**base,'resource_request':dict(cpus=4)},'resource_wait'),
               ({**base,'progress':dict(p,waiting=dict(kind='business',reason='checkpoint'))},'waiting'),
               ({**base,'status':'failed'},'failed'),({**base,'status':'succeeded'},'succeeded')]
        for job,code in cases:
            with self.subTest(code=code):self.assertEqual(diagnose(job,now=200)['code'],code)
        self.assertEqual(diagnose(base,now=200,offline=True)['code'],'worker_unreachable')

    def test_business_progress_owns_primary_view_over_executor_fallback(self):
        with scratch()as root,patch.dict(os.environ,REMOTE_COMPUTE_JOB_DIR=str(root)):
            report_items(1,4);report_progress(phase='native',completed=100,total=None,unit='cycles')
            self.assertFalse(report_items(2,4))
            p=json.loads((Path(root)/'.dco-progress.json').read_text())
            self.assertEqual(p['completed'],100);self.assertNotIn('total',p)
            p.update(last_heartbeat=1,last_advanced=1,source='executor')
            self.assertEqual(diagnose(dict(status='running',progress=p,runtime=dict(sampled_at=200,last_cpu_advanced=199)),now=200)['code'],'cpu_active')

    def test_web_projection_contains_work_wait_and_checkpoint_but_no_arbitrary_payload(self):
        p=normalize(dict(phase='replay',completed=10,counters={'native_records':80},last_heartbeat=100,
            last_advanced=90,checkpoint=dict(interval_seconds=1500,saved_at=95,available=True,resume='segment-4')))
        row=dict(id='original',identity_scope='task',status='running',cpus=1,progress={**p,'private':'SECRET'},
                 runtime=dict(sampled_at=100,last_cpu_advanced=100,process_alive=True))
        n=dict(name='worker',capacity=8,base_capacity=8,reserved=1,external_cpus=0,active_jobs=[row])
        value=liveboard.snapshot(dict(queued=1,queued_jobs=[dict(id='q',status='queued')]),[n],now=100)
        j=value['jobs'][0]
        self.assertEqual(j['identity_scope'],'task');self.assertEqual(j['progress']['counters']['native_records'],80)
        self.assertNotIn('ratio',j['progress']);self.assertNotIn('SECRET',json.dumps(value))
        self.assertEqual(value['jobs'][1]['diagnostic']['code'],'queued')
        self.assertTrue(value['coverage']['queued_job_details'])

    def test_final_receipt_keeps_business_denominator_and_original_identity(self):
        from remote_compute.coordinator import Coordinator
        owner=Coordinator.__new__(Coordinator)
        owner._attempt_total=lambda result:None;owner._save=lambda job:None
        job=dict(task=dict(cpus=1,memory_mb=64),result=dict(id='original',submitted=1,completed=0,total=1))
        p=normalize(dict(completed=5,total=20,unit='cycles'))
        owner._update(job,dict(id='attempt',submitted=2,status='succeeded',progress=p))
        self.assertEqual(job['result']['id'],'original')
        self.assertEqual((job['result']['completed'],job['result']['total']),(5,20))
        self.assertEqual(job['result']['progress']['ratio'],.25)

    def test_real_http_status_health_and_display_receive_same_business_report(self):
        source='''from remote_compute import report_progress
from pathlib import Path
import time
def work():
 report_progress(phase='native',completed=17,total=None,unit='cycles',counters={'MAC':900},checkpoint={'available':True,'resume':'next-segment','interval_seconds':1500})
 while not Path('release').exists():
  report_progress()
  time.sleep(.1)
 return 17
'''
        with scratch()as root,provider(Path(root)/'worker',cpus=2)as(_,worker),Pool([worker])as pool,center(pool,Path(root)/'center')as(owner,client):
            i=client.submit(python_call('work:work',max_cpus=2,memory_mb=64),files={'work.py':source})['id']
            until(lambda:client.status(i).get('progress',{}).get('completed')==17,seconds=10)
            health=owner.health();rows=health['nodes'];jobs=[j for n in rows for j in n.get('active_jobs',[])]
            self.assertEqual(next(j for j in jobs if j['id']==i)['progress']['phase'],'native')
            value=liveboard.snapshot(health,rows)
            self.assertEqual(value['jobs'][0]['id'],i)
            self.assertEqual(value['jobs'][0]['progress']['checkpoint']['resume'],'next-segment')
            shown = value['jobs'][0]['progress']
            self.assertEqual((shown['completed'], shown['total'], shown['ratio'], shown['unit']),
                             (0, 1, 0, 'steps'))
            self.assertEqual((shown['business_completed'], shown['business_unit']), (17, 'cycles'))
            client.write_job_file(i,'release','done');client.wait(i,wait_timeout=10)
            result=client.fetch(i)
            self.assertEqual(result['status'],'succeeded')
            self.assertEqual(result['progress']['completed'],17)
            self.assertNotIn('total',result['progress'])

    def test_managed_map_automatically_reports_completed_items(self):
        source='''from pathlib import Path
import time
def work(x):
 if x:
  while not Path('release').exists():time.sleep(.05)
 return x
'''
        with scratch()as root,provider(Path(root)/'worker',cpus=2)as(_,worker),Pool([worker])as pool,center(pool,Path(root)/'center')as(owner,client):
            i=client.submit(python_map('work:work',range(4),backend='thread',max_cpus=2,memory_mb=128),files={'work.py':source})['id']
            until(lambda:client.status(i).get('progress',{}).get('completed')==1,seconds=10)
            p=client.status(i)['progress']
            self.assertEqual((p['total'],p['source'],p['ratio']),(4,'executor',.25))
            client.write_job_file(i,'release','done');client.wait(i,wait_timeout=10)
            result=client.fetch(i);self.assertEqual(result['status'],'succeeded')
            self.assertEqual(result['progress']['completed'],4)

    def test_recent_failure_is_displayed_from_hot_receipt_without_history_scan(self):
        source='from remote_compute import report_progress\ndef work():\n report_progress(phase="native",completed=2,total=5)\n raise ValueError("fixture failure")\n'
        with scratch()as root,provider(Path(root)/'worker',cpus=1)as(_,worker),Pool([worker])as pool,center(pool,Path(root)/'center')as(owner,client):
            i=client.submit(python_call('work:work',max_cpus=1,memory_mb=64),files={'work.py':source})['id']
            until(lambda:client.status(i)['status']=='failed',seconds=10)
            with patch.object(owner.jobs, 'receipt_reader', side_effect=AssertionError('cold history read')):
                h = owner.health()
            row = next(j for j in h['recent_failures'] if j['id'] == i)
            self.assertEqual(diagnose(row)['code'], 'failed')
            self.assertEqual(liveboard.display_progress(row)['ratio'], .4)
            client.fetch(i)


if __name__=='__main__':unittest.main()
