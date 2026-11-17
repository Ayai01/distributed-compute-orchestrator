"""Original-ID recovery never borrows process observations from a paid attempt."""
import json,time,unittest
from pathlib import Path
from unittest.mock import patch
from remote_compute import Pool,python_task
from test_compute import provider,scratch,until
from test_center import center
import test_pressure_retry as pressure_fixture

class AttemptObservationTests(unittest.TestCase):
 def test_memory_recovery_retains_old_observations_only_inside_paid_attempt(self):
  with scratch() as folder, provider(Path(folder)/'worker',cpus=2) as (worker,remote), Pool([remote]) as pool, center(pool,Path(folder)/'center') as (state,client), patch('remote_compute.coordinator.RESOURCE_RECOVERY_DELAYS',(.01,.02)):
   code="import time\nfrom remote_compute import report_progress\ndef one():\n report_progress(phase='original phase',completed=3,total=10)\n time.sleep(30)\n"
   task=python_task('work:one',max_cpus=4,base_memory_mb=6912,memory_per_cpu_mb=0,retries=0,timeout=35)
   identity=client.submit(task,files={'work.py':code})['id']
   until(lambda:client.status(identity).get('progress',{}).get('completed')==3,seconds=8)
   original=client.status(identity);sha=original['request_sha256']
   with state.condition:
    state.jobs[identity]['result']['resource_recovery_memory_mb']=12345
   worker.start_lock.acquire()
   try:
    pressure_fixture.PressureRetryTests().stop(worker,identity)
    until(lambda:client.status(identity).get('worker_id') is not None and client.status(identity)['status']=='running',seconds=8)
    current=client.status(identity)
    self.assertNotIn('runtime',current)
    self.assertNotIn('progress',current)
    self.assertNotIn('progress_error',current)
    self.assertNotIn(current.get('diagnostic',{}).get('code'),('finishing','cpu_active','process_lost'))
    self.assertNotIn('memory_reservation_mb',current)
    self.assertNotIn('memory_commit_reservation_mb',current)
    self.assertEqual(current['request_sha256'],sha)
    self.assertGreaterEqual(current['resource_recovery_memory_mb'],12345)
    self.assertEqual(len(current['attempts']),1)
    attempt=current['attempts'][0]
    self.assertEqual(attempt['progress']['completed'],3)
    self.assertFalse(attempt['runtime']['process_alive'])
    self.assertGreaterEqual(attempt['cpu_seconds'],0)
    self.assertEqual(state.jobs[identity]['desired_cpus'],1)
    self.assertEqual(current['memory_mb'],6912)
   finally:
    worker.start_lock.release()
    client.cancel(identity)
    result=client.wait(identity,wait_timeout=8)
   self.assertEqual(result['request_sha256'],sha)
   self.assertEqual(result['attempts'][0]['cpu_seconds'],attempt['cpu_seconds'])
   self.assertEqual(result['total_cpu_seconds'],sum(a['cpu_seconds'] for a in result['attempts'])+result['cpu_seconds'])

if __name__=='__main__':unittest.main()
