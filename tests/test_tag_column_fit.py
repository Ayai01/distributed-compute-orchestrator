import unittest
from copy import deepcopy
from unittest.mock import patch
from remote_compute.runtimes import EXECUTION_CAPABILITIES
from remote_compute.task_tags import compact_tag
from remote_compute.view import format_status,_width


class TagColumnTests(unittest.TestCase):
    def test_scheduler_renders_a_capability_without_a_display_label(self):
        node = dict(name='school', capacity=32, tags=['rtl','hdl-cache'])
        health = dict(role='scheduler', nodes=[node])
        with patch('remote_compute.runtimes.EXECUTION_CAPABILITIES', (*EXECUTION_CAPABILITIES, 'hdl-cache')):
            for width in (54, 110, 180):
                rows = format_status(health, width=width).fit(25)
                self.assertIn('hdl-cache', '\n'.join(rows))

    def test_producer_label_is_not_interpreted_as_progress(self):
        job = dict(label='1/2 build phase', project='custom')
        self.assertEqual(compact_tag(job, 32), job['label'])
        self.assertEqual(compact_tag(job, 8), '1/2 buil')

    def test_narrow_panel_handles_double_width_character(self):
        job = dict(label='验证 staging area')
        self.assertEqual(compact_tag(job, 3, measure=_width), '验')
        self.assertEqual(compact_tag(job, 1, measure=_width), '-')

    def test_current_task_shows_actual_cpu_and_configured_maximum(self):
        job=dict(id='abcd'+'f'*28,label='run corner comparison',cpus=4,max_cpus=128,status='running')
        rows=format_status(dict(role='worker',node='test',cpus=8,active_jobs=[job]),width=110)
        text='\n'.join(rows)
        self.assertIn('CPU/上限',text);self.assertIn('4/128',text)
        self.assertNotIn('1/4',text)

    def test_all_projects_fit_real_tag_cells_without_ellipsis(self):
        labels=[('Analog-Opt-Bench','Class bounded staged p75_precision9_relaxed50 agemoea2 73137011 4 real designs'),
            ('RGCN','FSA -KCL s19b2 ep:96 ~600s'),('ARH-DSE','Rocket BP seed=4101 1234567890abcdef 4 real designs'),
            ('AOP','check/AOP/aop/ngspice/BGR_SKY130_SSFF_m40_p125_4corners/ss27')]
        for width in(20,38,54,80,90,110,125,135,140,163,180,200):
            value=dict(role='scheduler',nodes=[dict(name='Aya1-PC',capacity=20,active_jobs=[dict(id=('%04x'%i)+'f'*28,project=project,label=label,cpus=1,status='running',memory_mb=256,total_steps=1)for i,(project,label)in enumerate(labels)])])
            before=deepcopy(value);rows=format_status(value,width=width).fit(48);task=False
            for row in rows:
                if '当前任务'in row:task=True
                if task and '|'in row and any(('%04x'%i)in row for i in range(len(labels))):
                    self.assertNotIn('...',row,(width,row));self.assertNotIn('\u2026',row,(width,row));self.assertLessEqual(_width(row),width)
            self.assertEqual(value,before)

    def test_normal_width_preserves_the_complete_opaque_tag(self):
        job = {'label': 'sample | validation | 42'}
        self.assertEqual(compact_tag(job, 40), job['label'])


if __name__=='__main__':unittest.main()
