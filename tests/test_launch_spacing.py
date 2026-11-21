"""Task startup pacing does not serialize execution or cancellation."""
from concurrent.futures import ThreadPoolExecutor
import threading
import time
import unittest
from unittest.mock import patch

from remote_compute import Pool
from remote_compute.client import Node
from remote_compute.worker import Worker
from remote_compute.memory import PressureGuard
from remote_compute.coordinator import Coordinator
from test_compute import provider,python,scratch,until
from test_center import center


class LaunchSpacingTests(unittest.TestCase):
    def worker(self):
        worker=Worker.__new__(Worker)
        worker.pressure_process_lock=threading.Lock();worker.pressure_processes={}
        worker.pressure_guard=PressureGuard()
        return worker

    @staticmethod
    def pending(identity='next', cancel=None, **values):
        return dict(task=dict(cpus=1, memory_mb=256), cancel=cancel if cancel is not None else threading.Event(),
                    result=dict(id=identity), **values)

    def test_concurrent_initial_process_creation_is_spaced(self):
        worker=self.worker()
        worker.start_lock=threading.Lock();worker.next_start=0.;worker.start_interval=.5;worker.stopping=False
        barrier=threading.Barrier(3);times=[]
        def start(_):
            barrier.wait()
            return worker._start_process(self.pending(str(threading.get_ident())), ['echo'])
        def process(*args,**kwargs):
            times.append(time.monotonic());return object()
        with patch('remote_compute.worker.Process',side_effect=process),ThreadPoolExecutor(3)as pool:
            self.assertEqual(len(list(pool.map(start,range(3)))),3)
        self.assertEqual(len(times),3)
        self.assertTrue(all(b-a>=.5 for a,b in zip(times,times[1:])),times)

    def test_funded_short_jobs_start_while_a_cold_probe_is_pending(self):
        with scratch() as root, provider(root, cpus=4, memory_mb=2048) as (_, client), \
                patch('remote_compute.memory.MemoryObservation.observe'), \
                patch('remote_compute.worker.PREPARATION_TIMEOUT', 2):
            probe = client.submit(python('import time; time.sleep(12)', memory_mb=8192, timeout=15))['id']
            try:
                until(lambda: client.status(probe).get('execution_started') is not None)
                blocked = client.submit(python('print("cold")', memory_mb=8192, short_job=True))['id']
                funded = [client.submit(python(f'print({i})', memory_mb=512, short_job=True))['id']
                          for i in range(2)]
                for i, identity in enumerate(funded):
                    result = client.wait(identity, wait_timeout=8)
                    self.assertEqual(result['id'], identity)
                    self.assertEqual(result['status'], 'succeeded', result.get('preparation_stage'))
                    self.assertLess(result['preparation_seconds'], 2)
                    self.assertEqual(client.fetch(identity)['stdout'].strip(), str(i))
                waiting = client.wait(blocked, wait_timeout=8)
                self.assertEqual(waiting['error_kind'], 'startup_timeout')
                self.assertEqual(waiting['preparation_stage'], 'memory_calibration')
                self.assertEqual(waiting['cpu_seconds'], 0)
                self.assertEqual(client.status(probe)['status'], 'running')
            finally:
                client.cancel(probe)
                client.wait(probe, wait_timeout=8)

    def test_cancelled_start_does_not_launch_or_wait_out_the_interval(self):
        worker=self.worker()
        worker.start_lock=threading.Lock();worker.next_start=time.monotonic()+10;worker.start_interval=.5;worker.stopping=False
        cancel=threading.Event();cancel.set()
        with patch('remote_compute.worker.Process')as process:
            started=time.monotonic()
            previous=worker.next_start
            self.assertIsNone(worker._start_process(self.pending(cancel=cancel), ['echo']))
            self.assertLess(time.monotonic()-started,.1)
            self.assertEqual(worker.next_start,previous)
            worker.stopping=True;cancel.clear()
            self.assertIsNone(worker._start_process(self.pending(cancel=cancel), ['echo']))
            self.assertLess(time.monotonic()-started,.1)
            self.assertEqual(worker.next_start,previous)
            process.assert_not_called()

    def test_center_spaces_dispatches_without_waiting_for_completion(self):
        with scratch()as root,provider(root+'/worker',cpus=3)as(_,worker):
            pool=Pool([dict(name='test',url=worker.url,token=worker.token)])
            try:
                with center(pool,root+'/center',paused=True)as(_,client):
                    ids=[client.submit(python('import time;time.sleep(2)'))['id']for _ in range(3)]
                    client.control(paused=False)
                    rows=[client.wait(identity,wait_timeout=8)for identity in ids]
                    self.assertTrue(all(row['status']=='succeeded'for row in rows),rows)
                    dispatches=sorted(row['dispatched']for row in rows)
                    self.assertTrue(all(b-a>=.5 for a,b in zip(dispatches,dispatches[1:])),dispatches)
                    self.assertLess(max(row['started']for row in rows),min(row['finished']for row in rows))
                    for identity in ids:client.fetch(identity)
            finally:pool.close()

    def test_worker_stop_during_spacing_wait_keeps_the_gate_and_skips_creation(self):
        worker=self.worker()
        worker.start_lock=threading.Lock();worker.next_start=time.monotonic()+.06;worker.start_interval=.5;worker.stopping=False
        previous=worker.next_start
        def stop():
            time.sleep(.01);worker.stopping=True
        with patch('remote_compute.worker.Process')as process,ThreadPoolExecutor(1)as pool:
            stopping=pool.submit(stop)
            self.assertIsNone(worker._start_process(self.pending(), ['echo']))
            stopping.result();process.assert_not_called()
        self.assertEqual(worker.next_start,previous)

    def test_center_stop_during_spacing_wait_keeps_the_gate_and_skips_submission(self):
        state=Coordinator.__new__(Coordinator)
        state.condition=threading.Condition();state.stopping=False
        node=Node('test',None);node.next_dispatch=time.monotonic()+.06
        previous=node.next_dispatch
        def stop():
            time.sleep(.01);state.stopping=True
        with patch.object(state,'_save')as save,ThreadPoolExecutor(1)as pool:
            stopping=pool.submit(stop)
            self.assertFalse(state._pace_submission(dict(node=node,cancel=threading.Event(),result={})))
            stopping.result();save.assert_not_called()
        self.assertEqual(node.next_dispatch,previous)

    def test_worker_overcommit_start_is_one_second_from_adjacent_starts(self):
        class Clock:
            now=10.
            def wait(self,delay):self.now+=delay;return False
            def is_set(self):return False
        clock=Clock();worker=self.worker();times=[]
        worker.start_lock=threading.Lock();worker.next_start=0.;worker.start_interval=.5;worker.stopping=False
        with patch('remote_compute.worker.time.monotonic',side_effect=lambda:clock.now), \
                patch('remote_compute.worker.Process',side_effect=lambda *args,**kwargs:times.append(clock.now)):
            for borrowed in (False,True,False):
                worker._start_process(self.pending('clock', cancel=clock, cpu_overcommit=borrowed), ['echo'])
        self.assertEqual(times,[10.,11.,12.])

    def test_device_dispatch_gates_are_independent_and_borrowed_entry_waits_one_second(self):
        class Clock:
            now=10.
            def wait(self,delay):self.now+=delay;return False
            def is_set(self):return False
        clock=Clock();state=Coordinator.__new__(Coordinator);state.stopping=False
        state.condition=threading.Condition();state._save=lambda job:None
        one,two=Node('one',None),Node('two',None);times=[]
        with patch('remote_compute.coordinator.time.monotonic',side_effect=lambda:clock.now):
            for node,borrowed in ((one,False),(two,False),(one,True)):
                self.assertTrue(state._pace_submission(dict(node=node,cancel=clock,result={},cpu_overcommit=borrowed)))
                times.append(clock.now)
        self.assertEqual(times,[10.,10.,11.])


if __name__=='__main__':unittest.main()
