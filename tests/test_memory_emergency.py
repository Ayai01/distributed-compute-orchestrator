"""Kill real trees without starving the host or using the scheduler lock."""
import threading,time,unittest
from pathlib import Path
from unittest.mock import Mock
from remote_compute.memory import PressureGuard,pressure_victim
from test_compute import provider,scratch,until,python

class MemoryEmergencyTests(unittest.TestCase):
    def test_actual_floor_is_immediate_and_recovery_is_stable(self):
        g=PressureGuard();host=dict(memory_total_mb=32768,memory_headroom_mb=4096,host_memory_available_mb=4000,host_commit_available_mb=8000)
        self.assertTrue(g.update(host,0)['terminate'])
        host['host_memory_available_mb']=8000
        self.assertTrue(g.update(host,.1)['waiting'])
        self.assertFalse(g.update(host,2.2)['limited'])
        host['host_commit_available_mb']=900
        self.assertTrue(g.update(host,3)['terminate'])

    def test_two_falling_samples_predict_danger_but_one_jump_does_not(self):
        g=PressureGuard();h=dict(memory_total_mb=32768,memory_headroom_mb=4096,host_commit_available_mb=16000)
        self.assertFalse(g.update(dict(h,host_memory_available_mb=10000),0)['terminate'])
        self.assertFalse(g.update(dict(h,host_memory_available_mb=8000),.1)['terminate'])
        self.assertTrue(g.update(dict(h,host_memory_available_mb=6000),.2)['terminate'])

    def test_newest_unsampled_tree_is_selected_before_larger_older_job(self):
        def job(i,at):return dict(result=dict(id=i,status='running',started=at),cancel=threading.Event(),process=Mock(),task=dict(priority=0))
        old,new=job('old',1),job('new',2)
        old['rss_mb']=9000
        self.assertIs(pressure_victim([old,new],{},emergency=True),new)

    def test_latest_real_process_is_killed_while_scheduler_lock_is_blocked(self):
        with scratch()as root,provider(Path(root)/'worker',cpus=2,memory_headroom_mb=0)as(w,c):
            a=c.submit(python('import time;time.sleep(60)'))['id']
            until(lambda:w.jobs[a].get('process')is not None)
            b=c.submit(python('import time;time.sleep(60)'))['id']
            until(lambda:w.jobs[b].get('process')is not None,seconds=8)
            process=w.jobs[b]['process'];old=w.jobs[a]['process']
            done=threading.Event()
            with w.condition:
                def protect():
                    w._memory_emergency(dict(memory_total_mb=32768,memory_headroom_mb=4096,host_memory_available_mb=3500,host_commit_available_mb=9000));done.set()
                t=threading.Thread(target=protect);start=time.monotonic();t.start()
                self.assertTrue(done.wait(1),'emergency kill waited for global scheduler lock')
                until(lambda:process.child.poll()is not None,seconds=1)
                self.assertLess(time.monotonic()-start,1)
                self.assertIsNone(old.child.poll())
            t.join();c.cancel(a)
            until(lambda:c.status(b)['status']=='failed')
            self.assertEqual(c.status(b)['error_kind'],'memory_pressure')

if __name__=='__main__':unittest.main()
