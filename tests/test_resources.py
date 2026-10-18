"""Approximate native load, resource calibration and compact UI contracts."""
import itertools
import json
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from remote_compute import Pool
from remote_compute.benchmark import BATCH, NAME, _sample, measure
from remote_compute.client import Node
from remote_compute.resources import cpu_benchmark
from remote_compute.view import _capabilities, format_status, _width
from test_compute import provider, scratch


class BenchmarkTests(unittest.TestCase):
    def test_startup_calls_native_measurement_once_per_budget(self):
        cpu_benchmark.cache_clear()
        result=dict(name=NAME,score=2.5,unit='Miter/s',threads=24,seconds=1.9,total_seconds=2.0)
        try:
            with patch('remote_compute.benchmark.calibrated',return_value=result) as kernel:
                self.assertIs(cpu_benchmark(24),result)
                self.assertIs(cpu_benchmark(24),result)
            kernel.assert_called_once_with(24,executable=None,environment=())
        finally:
            cpu_benchmark.cache_clear()

    def test_native_lanes_share_deadline_with_equal_single_and_multi_budgets(self):
        states=[]
        def create(np):
            state=Mock()
            state.checksum.return_value=[1,2,3]
            states.append(state)
            return state
        clock=itertools.count(10000)
        np=SimpleNamespace(__version__='test',lib=SimpleNamespace())
        with patch('remote_compute.benchmark.MixedLoad',side_effect=create), \
                patch('remote_compute.benchmark.time.monotonic',side_effect=lambda:next(clock)/100):
            result=_sample(np,2,100,duration=2)
        self.assertEqual(result['name'],NAME)
        self.assertEqual(result['threads'],2)
        self.assertTrue(4<=result['total_seconds']<4.6)
        self.assertEqual(result['iterations'],sum(s.block.call_count-1 for s in states[1:])*BATCH)
        self.assertAlmostEqual(result['score'],result['iterations']/result['seconds']/1000000,places=4)
        self.assertEqual(result['single']['threads'],1)
        self.assertEqual(result['single']['iterations'],(states[0].block.call_count-1)*BATCH)
        self.assertGreater(result['single']['score'],0)
        self.assertTrue(2<=result['single']['seconds']<2.1)

    def test_helper_limits_blas_threads_without_changing_service_environment(self):
        from os import environ
        before=dict(environ)
        helper=Mock()
        helper.returncode=0
        helper.communicate.return_value=(json.dumps(dict(name=NAME,available=False)),'')
        factory=Mock()
        factory.return_value.__enter__=Mock(return_value=helper)
        factory.return_value.__exit__=Mock(return_value=False)
        with patch('remote_compute.benchmark.subprocess.Popen',factory):
            self.assertFalse(measure(24)['available'])
        child_env=factory.call_args.kwargs['env']
        for name in ('OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','OMP_NUM_THREADS','BLIS_NUM_THREADS','VECLIB_MAXIMUM_THREADS'):
            self.assertEqual(child_env[name],'1')
        self.assertEqual(dict(environ),before)
        self.assertIn('-I',factory.call_args.args[0])
        self.assertIn('-B',factory.call_args.args[0])

    def test_timeout_reaps_the_single_helper_and_leaves_worker_available(self):
        import subprocess
        helper=Mock()
        helper.communicate.side_effect=[subprocess.TimeoutExpired('estimate',5),('','')]
        factory=Mock()
        factory.return_value.__enter__=Mock(return_value=helper)
        factory.return_value.__exit__=Mock(return_value=False)
        with patch('remote_compute.benchmark.subprocess.Popen',factory):
            result=measure(24)
        self.assertFalse(result['available'])
        self.assertNotIn('score',result)
        helper.kill.assert_called_once()
        self.assertEqual(helper.communicate.call_count,2)

    def test_estimate_uses_existing_scientific_runtime_and_library_settings(self):
        helper=Mock(returncode=0)
        helper.communicate.return_value=(json.dumps(dict(name=NAME,available=True)),'')
        with patch('remote_compute.benchmark.subprocess.Popen') as factory:
            factory.return_value.__enter__.return_value=helper
            result=measure(96,executable='/science/bin/python',environment=(('OPENBLAS_CORETYPE','Haswell'),))
        self.assertEqual(factory.call_args.args[0][0],'/science/bin/python')
        self.assertEqual(factory.call_args.kwargs['env']['OPENBLAS_CORETYPE'],'Haswell')
        self.assertEqual(result['python'],'/science/bin/python')

    def test_warmup_failure_breaks_barrier_and_leaves_no_benchmark_threads(self):
        import threading
        load=Mock()
        load.block.side_effect=RuntimeError('warmup failed')
        with patch('remote_compute.benchmark.MixedLoad',return_value=load), \
                self.assertRaises((RuntimeError,threading.BrokenBarrierError)):
            _sample(Mock(),2,100)
        self.assertFalse(any(t.name.startswith('native-benchmark') for t in threading.enumerate()))

    def test_device_budget_stays_in_health_but_is_not_repeated_in_score(self):
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
                    self.assertNotIn('Miter/s(T)',text)
                    self.assertNotIn('1.000('+str(budget)+')',text)

    def test_dashboard_distinguishes_native_python_and_hash_kernel_versions(self):
        value=dict(role='worker',node='Aya1-PC',cpus=24,
                   benchmark=dict(name=NAME,unit='Miter/s',score=1.234567,threads=24))
        self.assertIn('1.23','\n'.join(format_status(value,width=160)))
        for name,label in (('mixed-cpu-v1','待校准'),('pbkdf2-sha256-v1','待校准')):
            value['benchmark'].update(name=name,unit='Miter/s',score=140)
            text='\n'.join(format_status(value,width=160))
            self.assertIn(label,text)
            self.assertNotIn('140.000',text)

    def test_dashboard_shows_single_and_total_without_exceeding_existing_width(self):
        value=dict(role='worker',node='Aya1-PC',cpus=24,benchmark=dict(name=NAME,unit='Miter/s',
                   score=56.78,threads=24,single=dict(name=NAME,score=1.234,unit='Miter/s',threads=1)))
        for width in (48,56,80,110,160):
            with self.subTest(width=width):
                lines=format_status(value,width=width)
                self.assertTrue(all(_width(line)<=width for line in lines))
                self.assertIn('1.23/56.8','\n'.join(lines))

    def test_capabilities_group_aliases_without_folding_or_changing_protocol_tags(self):
        tags=['aop_python','arh_python','bench_python','rgcn_python','rgcn_train',
              'cpu','win32','ngspice','python','rtl','verilator']
        self.assertEqual(_capabilities(tags),'ng RTL Py ML')
        self.assertEqual(len(tags),11)
        self.assertEqual(_capabilities([*tags,'ngspice47','java','scala-cli','firtool','shared:arh']),'ng47 RTL Chisel Py ML')
        self.assertEqual(_capabilities([*tags,'long-custom-tag'*8]),'ng RTL Py ML '+'long-custom-tag'*8)
        self.assertIn('HS',_capabilities([*tags,'hspice','aop_hspice_python']))
        value=dict(role='worker',node='Aya1-TX',cpus=30,tags=tags)
        for width in (56,80,110,160):
            lines=format_status(value,width=width)
            self.assertTrue(all(_width(line)<=width for line in lines))
            self.assertIn('ng RTL Py ML','\n'.join(lines))

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

    def choose(self,pool,weight=1.0):
        return pool._acquire(dict(steps=[['echo']],cpus=1,memory_mb=64,label='same-work',single_thread_weight=weight),block=False).name

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

    def test_underused_grants_do_not_replace_measured_cpu_load(self):
        pool=self.pool()
        first,second=pool.nodes
        for node in pool.nodes:node.health.update(cpus=16,logical_cpus=16)
        first.reserved=8;first.health.update(used_cpus=8,cpu_percent=10)
        second.health.update(cpu_percent=40)
        self.assertEqual(self.choose(pool),'one')

    def test_different_native_workloads_are_not_compared(self):
        pool=self.pool()
        pool.nodes[0].health['benchmark'].update(name='numpy-mixed-v2',score=1000)
        pool.nodes[1].health['benchmark'].update(name='numpy-mixed-v3',score=1)
        self.assertEqual(pool._speed_priors(pool.nodes,1),{'one':1.0,'two':1.0})

    def test_balanced_policy_uses_speed_only_within_close_cpu_load(self):
        pool=self.pool('balanced')
        pool.nodes[1].health['benchmark'].update(score=40)
        self.assertEqual(self.choose(pool),'two')

    def test_serial_hint_changes_placement_without_changing_cpu_or_project_share(self):
        for policy in ('adaptive','balanced'):
            for weight,expected in ((.5,'two'),(1.0,'two'),(2.0,'one')):
                with self.subTest(policy=policy,weight=weight):
                    pool=self.pool(policy)
                    one,two=pool.nodes
                    two.health.update(cpus=16,logical_cpus=16)
                    one.health['benchmark'].update(score=4,single=dict(name=NAME,score=8,unit='Miter/s',threads=1))
                    two.health['benchmark'].update(score=16,threads=16,single=dict(name=NAME,score=2,unit='Miter/s',threads=1))
                    if weight==1:
                        priors=pool._speed_priors(pool.nodes,weight)
                        self.assertAlmostEqual(priors['one'],priors['two'])
                    self.assertEqual(self.choose(pool,weight),expected)
                    self.assertEqual(sum(n.reserved for n in pool.nodes),1)

    def test_lower_cpu_load_overrides_extreme_serial_hint(self):
        pool=self.pool()
        pool.nodes[0].health.update(cpu_percent=75)
        for n,rate in zip(pool.nodes,(100,1)):
            n.health['benchmark']['single']=dict(name=NAME,score=rate,unit='Miter/s',threads=1)
        self.assertEqual(self.choose(pool,1000),'two')

    def test_missing_single_measurement_is_neutral_and_hash_bulk_is_ignored(self):
        pool=self.pool()
        pool.nodes[0].health['benchmark'].update(name='pbkdf2-sha256-v1',score=1e12)
        pool.nodes[1].health['benchmark']['single']=dict(name=NAME,score=100,unit='Miter/s',threads=1)
        priors=pool._speed_priors(pool.nodes,2.0)
        self.assertEqual(priors,{'one':1.0,'two':1.0})

    def test_serial_weight_validation_and_default_packet_compatibility(self):
        from remote_compute import command
        from remote_compute.worker import validate_task
        self.assertNotIn('single_thread_weight',command(['echo']))
        self.assertEqual(validate_task(command(['echo'],single_thread_weight=2),4)['single_thread_weight'],2)
        for value in (0,-1,True,False,None,'2',float('inf'),float('nan')):
            with self.subTest(value=value),self.assertRaisesRegex(ValueError,'single_thread_weight'):
                validate_task(command(['echo'],single_thread_weight=value),4)
