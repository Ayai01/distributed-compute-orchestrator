"""One-slot admission, useful growth and real memory constraints stay fair."""
from pathlib import Path
import unittest
from remote_compute import Pool,command,python_map
from test_compute import provider,python,scratch,until
from test_center import center

HOLD="from pathlib import Path;import time\nprint('ready',flush=True)\nwhile not Path('release').exists():time.sleep(.02)"
BUSINESS="""import time
from pathlib import Path
def work(value):
    if isinstance(value,str):
        while not Path(value).exists():time.sleep(.02)
    else:time.sleep(value)
    return value
"""


class AutomaticQueueFairnessTests(unittest.TestCase):
    def finish(self,client,identities):
        for identity in identities:
            client.cancel(identity);client.wait(identity);client.fetch(identity)

    def test_one_freed_slot_admits_another_project_without_a_minimum_width(self):
        with scratch()as root,provider(Path(root)/'worker',cpus=4)as(_,worker):
            with Pool([worker])as pool,center(pool,Path(root)/'center')as(_,client):
                busy=[client.submit(python(HOLD,project='serial'))['id']for _ in range(4)]
                until(lambda:all(client.status(i)['status']=='running'for i in busy))
                wide=client.submit(python(HOLD,project='parallel',max_cpus=16))['id']
                self.assertEqual(client.status(wide)['status'],'queued')
                client.write_job_file(busy[0],'release','done');client.wait(busy[0])
                until(lambda:'ready'in client.logs(wide)['stdout'])
                self.assertEqual(worker.status(wide)['initial_cpus'],1)
                self.assertEqual(client.status(wide)['cpus'],1)
                self.assertEqual(client.health()['used_cpus'],4)
                self.finish(client,[*busy,wide])

    def test_map_tail_returns_slots_for_a_new_project(self):
        with scratch()as root,provider(Path(root)/'worker',cpus=4)as(_,worker):
            with Pool([worker])as pool,center(pool,Path(root)/'center')as(_,client):
                first=client.submit(python_map('business:work',[.3,.3,.3,'${work}/release'],
                    max_cpus=4,project='first'),files={'business.py':BUSINESS})['id']
                until(lambda:client.status(first)['status']=='running' and worker.status(first).get('peak_cpus')==4)
                later=client.submit(python(HOLD,max_cpus=4,project='later'))['id']
                until(lambda:'ready'in client.logs(later)['stdout'],seconds=10)
                self.assertEqual(client.status(first)['status'],'running')
                until(lambda:client.status(first)['cpus']==1)
                self.assertEqual(client.status(first)['cpus'],1)
                self.assertEqual(worker.status(later)['initial_cpus'],1)
                self.assertLessEqual(client.health()['used_cpus'],4)
                self.finish(client,[first,later])

    def test_higher_priority_new_work_gets_the_available_slot(self):
        with scratch()as root,provider(Path(root)/'worker',cpus=2)as(_,worker):
            with Pool([worker])as pool,center(pool,Path(root)/'center')as(_,client):
                busy=[client.submit(python(HOLD,project='serial'))['id']for _ in range(2)]
                until(lambda:all(client.status(i)['status']=='running'for i in busy))
                low=client.submit(python(HOLD,max_cpus=4,priority=0,project='low'))['id']
                high=client.submit(python(HOLD,max_cpus=4,priority=10,project='high'))['id']
                client.write_job_file(busy[0],'release','done');client.wait(busy[0])
                until(lambda:'ready'in client.logs(high)['stdout'])
                self.assertEqual(client.status(low)['status'],'queued')
                self.assertEqual(client.status(high)['cpus'],1)
                self.finish(client,[*busy,low,high])

    def test_high_priority_waiter_preserves_existing_four_then_enters_with_one(self):
        with scratch()as root,provider(Path(root)/'worker',cpus=4)as(_,worker):
            with Pool([worker])as pool,center(pool,Path(root)/'center')as(_,client):
                first=client.submit(python(HOLD,max_cpus=1024,priority=-1,project='existing'))['id']
                until(lambda:'ready'in client.logs(first)['stdout'])
                client.control(paused=True)
                client.resize(first,1,active_cpus=1)
                later=client.submit(python(HOLD,max_cpus=1,priority=10,project='new'))['id']
                self.assertEqual(client.resize(first,64,active_cpus=1)['cpus'],4)
                client.control(paused=False)
                self.assertEqual(client.status(later)['status'],'queued')
                self.assertEqual(client.resize(first,1,active_cpus=1)['cpus'],1)
                until(lambda:'ready'in client.logs(later)['stdout'])
                self.assertEqual(worker.status(later)['initial_cpus'],1)
                client.write_job_file(later,'release','done');client.wait(later)
                self.assertEqual(client.resize(first,64,active_cpus=1)['cpus'],4)
                self.assertEqual(client.status(first)['max_cpus'],1024)
                self.finish(client,[first,later])

    def test_memory_blocked_work_does_not_hold_an_unused_cpu(self):
        with scratch()as root,provider(Path(root)/'worker',cpus=4,memory_mb=256,memory_headroom_mb=0)as(_,worker):
            with Pool([worker])as pool,center(pool,Path(root)/'center')as(_,client):
                busy=client.submit(python(HOLD,base_memory_mb=128,memory_per_cpu_mb=64,project='resident'))['id']
                until(lambda:'ready'in client.logs(busy)['stdout'])
                blocked=client.submit(python(HOLD,max_cpus=4,memory_mb=128,priority=10,project='large'))['id']
                small=client.submit(python(HOLD,memory_mb=32,project='small'))['id']
                until(lambda:'ready'in client.logs(small)['stdout'])
                self.assertEqual(client.status(blocked)['status'],'queued')
                self.assertEqual(client.status(small)['cpus'],1)
                self.finish(client,[busy,blocked,small])
