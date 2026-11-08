import unittest
from copy import deepcopy
from remote_compute.view import _task_memory,format_status,_width

class TaskMemoryDisplayTests(unittest.TestCase):
    def test_actual_rss_and_original_linear_estimate_are_distinct(self):
        job=dict(cpus=4,rss_mb=42,memory_mb=74,peak_rss_mb=15000,
                 memory_phase=dict(base_memory_mb=4096,memory_per_cpu_mb=4096))
        original=deepcopy(job)
        self.assertEqual(_task_memory(job),'0.1/20.0')
        self.assertEqual(_task_memory({**job,'cpus':64}),'0.1/260.0')
        self.assertEqual(job,original)

    def test_fixed_phase_keeps_the_original_request_after_calibration(self):
        self.assertEqual(_task_memory(dict(rss_mb=42,memory_mb=74,memory_phase=dict(memory_mb=8192))),'0.1/8.0')

    def test_floor_precision_and_missing_sample(self):
        self.assertEqual(_task_memory(dict(rss_mb=0,memory_mb=16)),'0.1/0.1')
        self.assertEqual(_task_memory(dict(rss_mb=1300,memory_mb=6144)),'1.3/6.0')
        self.assertEqual(_task_memory(dict(memory_mb=8192)),'-/8.0')

    def test_parent_model_is_available_before_first_phase_reply(self):
        self.assertEqual(_task_memory(dict(cpus=1,rss_mb=30,memory_phase_model=dict(base_memory_mb=4096,memory_per_cpu_mb=4096))),'0.1/8.0')

    def test_terminal_pair_fits_and_keeps_the_block_identity(self):
        job=dict(id='abcd'+'f'*28,project='RACER-MO-ladder-first10',label='RACER ladder block-170',
                 status='running',cpus=4,max_cpus=64,rss_mb=42,memory_mb=74,
                 memory_phase=dict(base_memory_mb=4096,memory_per_cpu_mb=4096))
        value=dict(role='scheduler',nodes=[dict(name='Aya1-PC',capacity=24,active_jobs=[job])])
        original=deepcopy(value)
        for width in (80,90,100,110,120,135,140,180,200):
            with self.subTest(width=width):
                rows=format_status(value,width=width).fit(40)
                text='\n'.join(rows)
                if'RAM实/报'in text:self.assertIn('0.1/20.0',text)
                self.assertNotIn('????',text)
                self.assertTrue(all(_width(row)<=width for row in rows))
        self.assertEqual(value,original)

if __name__=='__main__':unittest.main()
