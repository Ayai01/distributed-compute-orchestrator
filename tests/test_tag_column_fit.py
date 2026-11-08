import unittest
from copy import deepcopy
from remote_compute.task_tags import compact_tag
from remote_compute.view import format_status,_width


class TagColumnTests(unittest.TestCase):
    def test_progress_does_not_become_the_subject_or_invent_question_mark_identity(self):
        self.assertEqual(compact_tag(dict(label='1/2 RACER ladder block-170')),'1/2 RACER LDR b170')
        self.assertNotIn('?',compact_tag(dict(label='A long operation without an assigned identity')))

    def test_live_ladder_tags_keep_distinct_block_numbers_in_narrow_panels(self):
        jobs=[dict(id=('%04x'%number)+'f'*28,label='RACER ladder block-'+str(number),
                   project='RACER-MO-ladder-first10',status='running',cpus=4,max_cpus=13,
                   completed=1,total=2,memory_mb=256)for number in (170,171)]
        value=dict(role='scheduler',nodes=[dict(name='Aya1-PC',capacity=24,active_jobs=jobs)])
        original=deepcopy(value)
        for width in (54,80,110,120,135,140,180,200):
            rows=format_status(value,width=width).fit(40)
            for number in (170,171):
                row=next(line for line in rows if ('%04x'%number)in line)
                self.assertNotIn('????',row,(width,row))
                self.assertNotIn('1 RACER',row,(width,row))
                self.assertTrue('b'+str(number)in row or'block-'+str(number)in row,(width,row))
        self.assertEqual(value,original)

    def test_hash_only_and_embedded_hashes_get_readable_short_tags(self):
        self.assertEqual(compact_tag({'label':'a'*20}),'RUN :aaaa')
        value=compact_tag({'label':'PSL seed '+('b'*32)+' ev:10k'})
        self.assertIn('PSL',value);self.assertIn('bbbb',value)
        self.assertNotIn('b'*8,value);self.assertLessEqual(_width(value),24)

    def test_current_task_shows_actual_cpu_and_configured_maximum(self):
        job=dict(id='abcd'+'f'*28,label='BGR PSL s26 ev:10k',cpus=4,max_cpus=128,status='running')
        rows=format_status(dict(role='worker',node='test',cpus=8,active_jobs=[job]),width=110)
        text='\n'.join(rows)
        self.assertIn('CPU/max',text);self.assertIn('4/128',text)
        self.assertNotIn('1/4',text)

    def test_long_native_label_has_real_short_seed_and_work(self):
        job=dict(project='Analog-Opt-Bench',id='a'*32,label='Class bounded staged p75_precision9_relaxed50 agemoea2 73137011 4 real designs')
        before=deepcopy(job);self.assertEqual(compact_tag(job),'ABP9R AGE sfb73 ng:4');self.assertEqual(job,before)

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

    def test_seed_control_and_work_are_preserved_at_normal_width(self):
        tag=compact_tag(dict(project='RGCN',label='FSA -KCL s19b2 ep:96 ~600s'))
        for token in('FSA','-KCL','s19b2','ep:96'):self.assertIn(token,tag)
        self.assertNotIn('~',tag)


if __name__=='__main__':unittest.main()
