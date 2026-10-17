"""Startup throughput measurement and its resource/UI contract."""

import io
import json
import math
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from remote_compute import Pool
from remote_compute.benchmark import BLOCK, NAME, MixedLoad, _lane, measure
from remote_compute.client import Node
from remote_compute.resources import cpu_benchmark
from remote_compute.view import format_status
from test_compute import provider, scratch


class BenchmarkTests(unittest.TestCase):
    def test_startup_calls_mixed_measurement_once_per_budget(self):
        cpu_benchmark.cache_clear()
        result=dict(name=NAME,score=2.5,unit='Miter/s',threads=24,seconds=1.7,total_seconds=2.0)
        try:
            with patch('remote_compute.benchmark.measure',return_value=result) as kernel:
                self.assertIs(cpu_benchmark(24),result)
                self.assertIs(cpu_benchmark(24),result)
            kernel.assert_called_once_with(24)
        finally:
            cpu_benchmark.cache_clear()

    def test_mixed_iteration_has_nonlinear_circuit_rtl_and_scientific_state(self):
        load=MixedLoad()
        expected_dot=math.fsum(a*b for a,b in zip(load.matrix[0],load.vector))
        load.block(1)
        v=load.volts[0]
        self.assertAlmostEqual(4*v+0.001*math.expm1(v/0.026),0.25,places=8)
        self.assertAlmostEqual(load.vector[0],0.995*0.01+0.005*expected_dot+0.0001*v)
        self.assertNotEqual(load.word,0x12345678)
        self.assertTrue(0<=load.word<=0xffffffff)
        for _ in range(4):load.block()
        other=MixedLoad(); other.block(1)
        for _ in range(4):other.block()
        self.assertEqual((load.word,load.volts,load.vector,load.ram),
                         (other.word,other.volts,other.vector,other.ram))
        self.assertTrue(all(math.isfinite(x) for x in load.volts+load.vector))

    def test_lane_warms_then_only_counts_work_before_the_shared_deadline(self):
        output=io.StringIO()
        with patch('remote_compute.benchmark.sys.stdin',io.StringIO('1.05\n')), \
                patch('remote_compute.benchmark.sys.stdout',output), \
                patch('remote_compute.benchmark.time.monotonic',side_effect=[1,1.1,1.1]):
            _lane()
        ready,result=output.getvalue().splitlines()
        self.assertEqual(ready,'ready')
        self.assertEqual(json.loads(result)['iterations'],BLOCK)

    def test_all_lanes_start_together_with_bootstrap_inside_the_two_second_budget(self):
        lanes, starts=[], []
        def spawn(*args,**kwargs):
            lane=Mock(stdin=io.StringIO(),stdout=io.StringIO('ready\n'),stderr=io.StringIO(),returncode=0)
            lane.poll.return_value=0
            def communicate(**options):
                starts.append(lane.stdin.getvalue())
                return json.dumps(dict(iterations=64000,finished=102.001,checksum=[1,2,3])),''
            lane.communicate.side_effect=communicate
            lanes.append(lane)
            return lane
        with patch('remote_compute.benchmark.subprocess.Popen',side_effect=spawn) as popen, \
                patch('remote_compute.benchmark.time.monotonic',side_effect=[100,100.4,100.41,100.42,102.02]):
            result=measure(2)
        self.assertEqual(popen.call_count,2)
        self.assertTrue(all('-I' in call.args[0] and '-B' in call.args[0] for call in popen.call_args_list))
        self.assertEqual(result['threads'],2)
        self.assertEqual(result['iterations'],128000)
        self.assertAlmostEqual(result['score'],128000/1.601/1000000,places=6)
        self.assertEqual(result['total_seconds'],2.02)
        self.assertEqual(result['name'],NAME)
        self.assertEqual(starts,['102.0\n','102.0\n'])
        self.assertEqual(result['components'],['circuit-newton','rtl-state-ram','scientific-dot'])
        for lane in lanes:
            lane.wait.assert_called_once()
            self.assertTrue(all(s.closed for s in (lane.stdin,lane.stdout,lane.stderr)))

    def test_bootstrap_interruption_reaps_only_its_own_lanes(self):
        lane=Mock(stdin=io.StringIO(),stdout=io.StringIO(),stderr=io.StringIO())
        lane.stdout.readline=Mock(side_effect=KeyboardInterrupt)
        lane.poll.return_value=None
        with patch('remote_compute.benchmark.subprocess.Popen',return_value=lane),self.assertRaises(KeyboardInterrupt):
            measure(1)
        lane.kill.assert_called_once()
        lane.wait.assert_called_once()
        self.assertTrue(all(s.closed for s in (lane.stdin,lane.stdout,lane.stderr)))

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
                    self.assertIn('Miter/s',text)
                    if width>=100:
                        self.assertIn(f'({budget})',text)

    def test_dashboard_distinguishes_the_new_mix_from_a_legacy_hash_snapshot(self):
        value=dict(role='worker',node='Aya1-PC',cpus=24,
                   benchmark=dict(name=NAME,unit='Miter/s',score=1.234567,threads=24))
        self.assertIn('1.235(24)','\n'.join(format_status(value,width=160)))
        value['benchmark'].update(name='pbkdf2-sha256-v1',unit='kiter/s',score=140000)
        text='\n'.join(format_status(value,width=160))
        self.assertIn('旧哈希(24)',text)
        self.assertNotIn('140.000',text)

    def test_invalid_thread_budget_fails_without_starting_kernel(self):
        with patch('remote_compute.benchmark.measure') as kernel:
            for budget in (0,-1,True,1.5):
                with self.subTest(budget=budget),self.assertRaises(ValueError):
                    cpu_benchmark(budget)
        kernel.assert_not_called()


class CalibrationTests(unittest.TestCase):
    def pool(self,policy='adaptive'):
        # Exercise placement without workers, subprocesses or production load.
        from threading import Condition
        pool=Pool.__new__(Pool)
        pool.policy,pool.condition,pool.closed,pool.rotation=policy,Condition(),False,0
        pool.nodes=[Node(name,SimpleNamespace(url=name)) for name in ('one','two')]
        for node in pool.nodes:
            node.health=dict(cpus=4,logical_cpus=4,cpu_percent=0,memory_total_mb=8192,
                host_memory_available_mb=8192,memory_available_mb=8192,memory_capacity_mb=8192,machine_id=node.name,
                benchmark=dict(name=NAME,score=4,threads=4,unit='Miter/s'))
        return pool

    def choose(self,pool):
        return pool._acquire(dict(steps=[['echo']],cpus=1,memory_mb=64,label='same-work'),block=False).name

    def test_unmeasured_tasks_use_mixed_speed_per_measured_cpu(self):
        pool=self.pool()
        pool.nodes[1].health['benchmark'].update(score=30,threads=10)
        self.assertEqual(self.choose(pool),'two')

    def test_real_same_label_timings_replace_the_mixed_prior(self):
        pool=self.pool()
        pool.nodes[0].health['benchmark'].update(score=40)
        pool.nodes[0].durations['same-work']=5
        pool.nodes[1].durations['same-work']=1
        self.assertEqual(self.choose(pool),'two')

    def test_legacy_hash_and_mixed_scores_are_never_compared(self):
        pool=self.pool()
        pool.nodes[1].health['benchmark'].update(name='pbkdf2-sha256-v1',score=1000000)
        self.assertEqual(self.choose(pool),'one')

    def test_lower_cpu_load_precedes_even_a_faster_mixed_prior(self):
        pool=self.pool()
        pool.nodes[0].health.update(cpu_percent=75)
        pool.nodes[0].health['benchmark'].update(score=40)
        self.assertEqual(self.choose(pool),'two')

    def test_balanced_policy_keeps_using_load_instead_of_benchmark_scores(self):
        pool=self.pool('balanced')
        pool.nodes[1].health['benchmark'].update(score=40)
        self.assertEqual(self.choose(pool),'one')
