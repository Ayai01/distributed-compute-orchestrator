"""Startup throughput measurement and its resource/UI contract."""

import time
import unittest
from unittest.mock import patch

from remote_compute import Pool
from remote_compute.resources import cpu_benchmark
from remote_compute.view import format_status
from test_compute import provider, scratch


class BenchmarkTests(unittest.TestCase):
    def test_two_second_native_measurement_reports_total_work_and_reuses_result(self):
        cpu_benchmark.cache_clear()
        started=time.perf_counter()
        result=cpu_benchmark(2)
        self.assertGreaterEqual(result['seconds'],2.0)
        self.assertLess(time.perf_counter()-started,6.0)
        self.assertEqual(result['threads'],2)
        self.assertEqual(result['unit'],'kiter/s')
        self.assertGreater(result['iterations'],0)
        self.assertAlmostEqual(result['score'],result['iterations']/result['seconds']/1000,delta=result['score']*.001)
        with patch('remote_compute.resources.hashlib.pbkdf2_hmac') as kernel:
            self.assertIs(cpu_benchmark(2),result)
        kernel.assert_not_called()

    def test_device_budget_propagates_to_health_nodes_and_dashboard(self):
        for logical,budget in ((28,24),(32,30)):
            with self.subTest(budget=budget),scratch() as directory, \
                    patch('remote_compute.worker.os.cpu_count',return_value=logical), \
                    provider(directory,cpus=budget,cpu_limit=budget) as (_,client):
                baseline=client.health()['benchmark']
                self.assertEqual(baseline['threads'],budget)
                with Pool([client]) as pool:
                    self.assertEqual(pool.snapshot()['nodes'][0]['benchmark'],baseline)
                for width in (80,110,160):
                    text='\n'.join(format_status(client.health(),width=width))
                    self.assertIn('kiter/s',text)
                    if width>=120:
                        self.assertIn(f'{budget}线程',text)
                    elif width>=100:
                        self.assertIn(f'({budget})',text)

    def test_invalid_thread_budget_fails_without_starting_kernel(self):
        with patch('remote_compute.resources.hashlib.pbkdf2_hmac') as kernel:
            for budget in (0,-1,True,1.5):
                with self.subTest(budget=budget),self.assertRaises(ValueError):
                    cpu_benchmark(budget)
        kernel.assert_not_called()
