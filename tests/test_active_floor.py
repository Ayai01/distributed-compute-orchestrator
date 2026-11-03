"""Active children keep grants until they finish; empty slots remain elastic."""
from pathlib import Path
import threading
import unittest

from remote_compute import Pool, RemoteError
import test_center as fixtures
from test_center import center
from test_compute import provider, python, scratch, until


class ActiveFloorTests(unittest.TestCase):
    def test_polling_worker_lease_transports_the_active_floor(self):
        code="""import json,os,sys
sys.path.insert(0,os.environ['REMOTE_COMPUTE_HOME'])
from remote_compute import Client
c=Client(os.environ['REMOTE_COMPUTE_URL'],os.environ.get('REMOTE_COMPUTE_TOKEN',''))
g=c.resize(os.environ['REMOTE_COMPUTE_JOB_ID'],4,min_cpus=1)
print(json.dumps(dict(cpus=g['cpus'],lease=os.environ.get('REMOTE_COMPUTE_LEASE'))))
"""
        with scratch()as root,provider(Path(root)/'worker',cpus=4)as (_,worker):
            with Pool([worker])as pool,center(pool,Path(root)/'center')as (_,client):
                result=client.run(python(code,cpus=1,max_cpus=4,memory_mb=64))
                self.assertEqual(result['status'],'succeeded',result)
                self.assertIn('"cpus": 4',result['stdout'])
                self.assertIn('"lease": "1"',result['stdout'])

    def test_new_tail_entrant_does_not_dilute_older_four_thread_jobs(self):
        state=fixtures.AdmissionPlanningTests().center([24]);node=state.pool.nodes[0]
        # Five full jobs, a three-thread tail, and a newly admitted one-thread
        # job fill 24. Fairly splitting all seven warmups used to yield 3/4,
        # and repeated tail admissions could leave ten jobs at 2/3 each.
        widths=[4,4,4,4,4,3,1];node.reserved=sum(widths)
        for index,width in enumerate(widths):
            key=str(index)
            state.jobs[key]=dict(task=dict(steps=[['echo']],cpus=width,max_cpus=24,
                memory_mb=64,project='A'),elastic_cpus=True,node=node,
                cancel=threading.Event(),desired_cpus=3 if index==5 else 24,
                result=dict(status='running',submitted=index))
        resources=state.project_resources()
        grants=[state._cooperative_cpu_target(key,state.jobs[key]['desired_cpus'],resources)for key in state.jobs]
        self.assertEqual(grants,widths)

    def test_fairness_releases_only_finished_child_slots(self):
        state=fixtures.AdmissionPlanningTests().center([24]);node=state.pool.nodes[0]
        node.health['capabilities']=['resize','resize_min_cpus']
        node.reserved=24;node.reserved_memory_mb=384;state.stopping=False
        state._publish=lambda job:None;state._save=lambda job:None
        node.client.resize=lambda identity,cpus,**kw:dict(id=identity,cpus=cpus,memory_mb=64)
        for index in range(6):
            key=str(index);node.live_jobs.add(key)
            state.jobs[key]=dict(task=dict(steps=[['echo']],cpus=4,max_cpus=24,
                memory_mb=64,project='A'),elastic_cpus=True,node=node,
                cancel=threading.Event(),desired_cpus=24,
                result=dict(id=key,status='running',cpus=4,memory_mb=64))
        state.jobs['waiting']=dict(task=dict(steps=[['echo']],cpus=1,max_cpus=24,
            memory_mb=64,project='B'),node=None,cancel=threading.Event(),result=dict(status='queued'))
        self.assertEqual(state.resize('0',24,min_cpus=4)['cpus'],4)
        # Returning one completed child is a safe boundary, even while three
        # siblings are still computing. Fairness may reclaim only that slot.
        state.jobs['waiting']['task']['priority']=1
        self.assertEqual(state.resize('0',24,min_cpus=3)['cpus'],3)
        self.assertEqual(node.reserved,23)
        del state.jobs['waiting']
        for key in list(state.jobs)[1:]:
            node.reserved-=state.jobs[key]['task']['cpus'];node.live_jobs.remove(key);del state.jobs[key]
        self.assertEqual(state.resize('0',24,min_cpus=3)['cpus'],24)
        self.assertEqual(node.reserved,24)

    def test_sdk_center_worker_preserve_floor_and_reject_impossible_memory(self):
        with scratch()as root,provider(Path(root)/'worker',cpus=4,memory_mb=1024)as (_,worker):
            with Pool([worker])as pool,center(pool,Path(root)/'center')as (_,client):
                hold="from pathlib import Path;import time\nwhile not Path('release').exists():time.sleep(.02)"
                first=client.submit(python(hold,cpus=1,max_cpus=8,project='A',memory_mb=64))['id']
                second=None
                try:
                    until(lambda:client.status(first)['status']=='running')
                    self.assertEqual(client.resize(first,4,min_cpus=1)['cpus'],4)
                    second=client.submit(python(hold,cpus=1,max_cpus=8,project='B',priority=1,memory_mb=64))['id']
                    self.assertEqual(client.resize(first,4,min_cpus=4)['cpus'],4)
                    self.assertEqual(worker.status(first)['cpus'],4)
                    self.assertEqual(client.status(second)['status'],'queued')
                    with self.assertRaises(RemoteError):
                        client.resize(first,4,min_cpus=4,base_memory_mb=128,memory_per_cpu_mb=512)
                    self.assertEqual(worker.status(first)['cpus'],4)
                    self.assertEqual(client.resize(first,4,min_cpus=3)['cpus'],3)
                    until(lambda:client.status(second)['status']=='running')
                    self.assertEqual(client.resize(first,4,min_cpus=2)['cpus'],2)
                    with self.assertRaises(RemoteError):client.resize(first,4,min_cpus=3)
                finally:
                    for identity in (first,second):
                        if identity:client.write_job_file(identity,'release','done')
                    for identity in (first,second):
                        if identity:client.wait(identity);client.fetch(identity)


if __name__=='__main__':unittest.main()
