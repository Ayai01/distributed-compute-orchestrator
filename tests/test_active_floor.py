"""Active children keep grants until they finish; empty slots remain elastic."""
from pathlib import Path
import threading
import unittest

from remote_compute import Client, Pool, RemoteError, python_task
from remote_compute.allocation import ACTIVE_GRANT_CONFLICT
from remote_compute.execution import _start_flexible_kernel
from remote_compute.leases import CpuLease
import test_center as fixtures
from test_capability_packages import packaged_worker
from test_center import center
from test_compute import provider, python, scratch, until


class ActiveFloorTests(unittest.TestCase):
    def test_original_runs_once_after_worker_grant_changes_at_launch(self):
        source = """from pathlib import Path
from remote_compute import run_call

def body():
    with Path('native.log').open('a') as marker:
        marker.write('started\\n')
    return 17

def entry():
    return run_call(body, limits={'max_cpus': 3})
"""
        identity = 'a' * 32
        with scratch() as folder, packaged_worker(Path(folder) / 'worker', 'python', cpus=3) as (_, worker):
            with Pool([worker]) as pool, center(pool, Path(folder) / 'center') as (state, operator):
                client = Client(operator.url, operator.token)
                resize = state.resize
                replaced = []

                def move_grant(original, cpus, **options):
                    if options.get('active_cpus', 0) > 1 and not replaced:
                        lower = resize(original, 1, active_cpus=0, min_cpus=0)
                        replaced.append(lower['cpus'])
                    return resize(original, cpus, **options)

                state.resize = move_grant
                task = python_task('fixture:entry', max_cpus=3, min_start_cpus=3, memory_mb=128,
                                   outputs=['native.log'], timeout=12, retries=0)
                result = client.run(task, files={'fixture.py': source}, job_id=identity, keep=True)
                self.assertEqual(result['status'], 'succeeded', (result.get('error'), result.get('stderr')))
                self.assertEqual(replaced, [1])
                self.assertEqual(result['id'], identity)
                self.assertEqual(result['files']['native.log'].splitlines(), [b'started'])
                self.assertEqual(result['attempts'], [])
                self.assertGreater(result['execution_timing']['resource_wait_seconds'], 0)

    def test_kernel_reacquires_a_stale_launch_proposal_before_computing(self):
        class Provider:
            def __init__(self):
                self.calls = []

            def resize(self, identity, cpus, *, active_cpus, **options):
                self.calls.append((identity, cpus, active_cpus))
                if active_cpus == 3:
                    error = RemoteError(ACTIVE_GRANT_CONFLICT)
                    error.status_code = 409
                    raise error
                return {'cpus': 3 if len(self.calls) == 1 else 2, 'memory_mb': 128}

        provider = Provider()
        lease = CpuLease(provider, 'original', max_cpus=4)
        self.assertEqual(_start_flexible_kernel(lease, 4), 2)
        self.assertEqual(provider.calls, [('original', 4, 0), ('original', 3, 3),
                                         ('original', 4, 0), ('original', 2, 2)])

    def test_polling_worker_lease_transports_the_active_floor(self):
        code="""import json,os,sys
sys.path.insert(0,os.environ['REMOTE_COMPUTE_HOME'])
from remote_compute import Client
c=Client(os.environ['REMOTE_COMPUTE_URL'],os.environ.get('REMOTE_COMPUTE_TOKEN',''))
g=c.resize(os.environ['REMOTE_COMPUTE_JOB_ID'],4,active_cpus=1)
print(json.dumps(dict(cpus=g['cpus'],lease=os.environ.get('REMOTE_COMPUTE_LEASE'))))
"""
        with scratch()as root,provider(Path(root)/'worker',cpus=4)as (_,worker):
            with Pool([worker])as pool,center(pool,Path(root)/'center')as (_,client):
                result=client.run(python(code,max_cpus=4,memory_mb=64))
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
        node.health['capabilities']=['resize','resize_active_cpus']
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
        self.assertEqual(state.resize('0',24,active_cpus=4)['cpus'],4)
        # Cross-project fairness returns an empty slot at this owner's boundary;
        # the three active children and other owners' grants keep computing.
        state.jobs['waiting']['task']['priority']=1
        self.assertEqual(state.resize('0',24,active_cpus=3)['cpus'],3)
        self.assertEqual(state.resize('0',3,active_cpus=3)['cpus'],3)
        self.assertEqual(node.reserved,23)
        del state.jobs['waiting']
        for key in list(state.jobs)[1:]:
            node.reserved-=state.jobs[key]['task']['cpus'];node.live_jobs.remove(key);del state.jobs[key]
        self.assertEqual(state.resize('0',24,active_cpus=3)['cpus'],24)
        self.assertEqual(node.reserved,24)

    def test_later_priority_keeps_older_same_project_warmup_and_active_floor(self):
        state=fixtures.AdmissionPlanningTests().center([5]);node=state.pool.nodes[0]
        node.health['capabilities']=['resize','resize_active_cpus']
        node.reserved=2;node.reserved_memory_mb=128;state.stopping=False
        state._publish=lambda job:None;state._save=lambda job:None
        node.client.resize=lambda identity,cpus,**kw:dict(id=identity,cpus=cpus,memory_mb=64)
        for key,priority,submitted in [('older',-1,0),('later',10,1)]:
            node.live_jobs.add(key)
            state.jobs[key]=dict(task=dict(steps=[['echo']],cpus=1,max_cpus=1024,
                memory_mb=64,project='A',priority=priority),elastic_cpus=True,node=node,
                cancel=threading.Event(),desired_cpus=64,
                result=dict(id=key,status='running',cpus=1,memory_mb=64,submitted=submitted))
        self.assertEqual(state.resize('later',64,active_cpus=1)['cpus'],1)
        self.assertEqual(state.resize('older',64,active_cpus=1)['cpus'],4)
        self.assertEqual(node.reserved,5)
        self.assertEqual(state.resize('later',64,active_cpus=1)['cpus'],1)
        self.assertEqual(state.resize('older',1,active_cpus=1)['cpus'],1)
        self.assertEqual(state.resize('later',64,active_cpus=1)['cpus'],4)
        self.assertEqual(node.reserved,5)

    def test_sdk_center_worker_preserve_floor_while_probing_large_estimates(self):
        with scratch()as root,provider(Path(root)/'worker',cpus=4,memory_mb=1024)as (_,worker):
            with Pool([worker])as pool,center(pool,Path(root)/'center')as (_,client):
                hold="from pathlib import Path;import time\nwhile not Path('release').exists():time.sleep(.02)"
                first=client.submit(python(hold,max_cpus=8,project='A',memory_mb=64))['id']
                second=None
                try:
                    until(lambda:client.status(first).get('runtime',{}).get('process_alive') is True)
                    self.assertEqual(client.resize(first,4,active_cpus=1)['cpus'],4)
                    second=client.submit(python(hold,max_cpus=8,project='B',priority=1,memory_mb=64))['id']
                    self.assertEqual(client.resize(first,4,active_cpus=4)['cpus'],4)
                    self.assertEqual(worker.status(first)['cpus'],4)
                    self.assertEqual(client.status(second)['status'],'queued')
                    probe=client.resize(first,4,active_cpus=4,base_memory_mb=128,memory_per_cpu_mb=512)
                    self.assertEqual((probe['cpus'],probe['memory_mb']),(4,1024))
                    self.assertEqual(worker.status(first)['cpus'],4)
                    self.assertEqual(client.resize(first,4,active_cpus=3)['cpus'],3)
                    until(lambda:client.status(second)['status']=='running')
                    self.assertEqual(client.resize(first,4,active_cpus=2)['cpus'],2)
                    with self.assertRaises(RemoteError):client.resize(first,4,active_cpus=3)
                finally:
                    for identity in (first,second):
                        if identity:
                            if client.status(identity)['status']=='running':client.write_job_file(identity,'release','done')
                            else:client.cancel(identity)
                    for identity in (first,second):
                        if identity:client.wait(identity);client.fetch(identity)


if __name__=='__main__':unittest.main()
