"""Public defaults drive actual kernel/process parallelism without caller leases."""
import inspect,json,time,unittest,sys
from pathlib import Path
from remote_compute import Client,Pool,command,python_map,python_call
from remote_compute.worker import validate_task
from test_compute import provider,scratch
from test_center import center

FUNCTIONS='''import os,time
def evaluate(value):
    start=time.time();time.sleep(.35)
    return dict(value=value*2,pid=os.getpid(),started=start,finished=time.time(),threads=os.environ['OMP_NUM_THREADS'])
def kernel():
    return dict(grant=int(os.environ['REMOTE_COMPUTE_CPUS']),threads=int(os.environ['OMP_NUM_THREADS']))
def runtime(value):
    import sys
    time.sleep(.05)
    return bool(sys.flags.dev_mode)
def hold_kernel():
    from pathlib import Path
    Path('started').write_text('yes')
    while not Path('release').exists():time.sleep(.02)
    return 1
def variable(value):
    start=time.time();time.sleep(value)
    return dict(started=start,finished=time.time())
import threading
cache={};guard=threading.Lock()
def shared(value):
    from remote_compute import run_call
    with guard:
        fresh=value not in cache
        if fresh:cache[value]=value*2
    return dict(value=cache[value],new=fresh,pid=os.getpid(),threads=os.environ['REMOTE_COMPUTE_CPUS'],nested=run_call(lambda:1))
'''

class AutomaticExecutionTests(unittest.TestCase):
    def test_multiple_waves_without_a_resource_callback_keep_running(self):
        for backend in ('process','thread'):
            with self.subTest(backend=backend),scratch()as root,provider(Path(root)/'worker',cpus=2)as(worker,client):
                worker.callback={}
                r=client.run(python_map('business:evaluate',range(5),backend=backend,max_cpus=2,timeout=12),files={'business.py':FUNCTIONS})
                self.assertEqual(r['status'],'succeeded',r.get('stderr'))
                self.assertEqual(json.loads(r['files']['.dco-results.json'])['completed'],5)

    def test_map_children_keep_the_frozen_python_prefix(self):
        with scratch()as root,provider(Path(root)/'worker',cpus=2,
                python_runtimes={'cpu':[sys.executable,'-X','dev']})as(_,client):
            r=client.run(python_map('business:runtime',range(4),max_cpus=2),files={'business.py':FUNCTIONS})
            self.assertEqual(r['status'],'succeeded',r.get('stderr'))
            self.assertTrue(all(item['value']for item in json.loads(r['files']['.dco-results.json'])['items']))

    def test_running_kernel_reports_its_actual_active_floor(self):
        from test_compute import until
        with scratch()as root,provider(Path(root)/'worker',cpus=4)as(_,client):
            identity=client.submit(python_call('business:hold_kernel',max_cpus=4),files={'business.py':FUNCTIONS})['id']
            until(lambda:client.status(identity).get('active_cpus')==4)
            self.assertEqual(client.status(identity)['cpus'],4)
            client.write_job_file(identity,'release','yes')
            self.assertEqual(client.wait(identity)['status'],'succeeded');client.fetch(identity)

    def test_public_request_omits_cpu_and_auto_switches(self):
        task=command(['echo'],max_cpus=16)
        self.assertNotIn('cpus',task)
        self.assertNotIn('cpus',inspect.signature(command).parameters)
        self.assertNotIn('cpus',inspect.signature(Client.admission).parameters)
        self.assertEqual(validate_task(task,32,allow_auto=True)['max_cpus'],16)
        with self.assertRaises(TypeError):command(['echo'],cpus=0)

    def test_default_command_expands_argv_and_threads_after_platform_grant(self):
        with scratch()as root,provider(Path(root)/'worker',cpus=4)as(_,client):
            task=command(['${tool:python}','-c',
                'import json,os,sys;print(json.dumps(dict(argv=int(sys.argv[1]),threads=int(os.environ["OMP_NUM_THREADS"]))))','${cpus}'])
            r=client.run(task)
            self.assertEqual(r['status'],'succeeded',r.get('stderr'))
            self.assertEqual(json.loads(r['stdout']),dict(argv=4,threads=4))
            self.assertEqual(r['initial_cpus'],1)
            self.assertEqual(r['peak_cpus'],4)

    def test_map_uses_actual_process_parallelism_without_business_resource_code(self):
        with scratch()as root,provider(Path(root)/'worker',cpus=4)as(_,worker):
            with Pool([worker])as pool,center(pool,Path(root)/'center')as(_,client):
                r=client.run(python_map('business:evaluate',range(8),max_cpus=4),files={'business.py':FUNCTIONS})
                self.assertEqual(r['status'],'succeeded',r.get('stderr')or r.get('error'))
                data=json.loads(r['files']['.dco-results.json'])
                values=[row['value']for row in data['items']]
                self.assertEqual([row['value']for row in values],[i*2 for i in range(8)])
                self.assertEqual({row['threads']for row in values},{'1'})
                concurrency=max(sum(v['started']<=point<v['finished']for v in values)for point in [v['started']for v in values])
                self.assertGreaterEqual(concurrency,2)
                self.assertLessEqual(concurrency,4)
                self.assertEqual(r['peak_cpus'],4)

    def test_call_uses_platform_numerical_kernel_grant(self):
        with scratch()as root,provider(Path(root)/'worker',cpus=4)as(_,client):
            r=client.run(python_call('business:kernel',max_cpus=4),files={'business.py':FUNCTIONS})
            self.assertEqual(r['status'],'succeeded',r.get('stderr'))
            value=json.loads(r['files']['.dco-results.json'])['items'][0]['value']
            self.assertEqual(value,dict(grant=4,threads=4))

    def test_map_expands_when_a_peer_releases_capacity(self):
        with scratch()as root,provider(Path(root)/'worker',cpus=4)as(_,worker):
            with Pool([worker])as pool,center(pool,Path(root)/'center')as(_,client):
                peer=client.submit(command(['${tool:python}','-c','import time;time.sleep(1.2)'],max_cpus=1))['id']
                r=client.run(python_map('business:evaluate',range(16),max_cpus=4),files={'business.py':FUNCTIONS})
                self.assertEqual(r['status'],'succeeded',r.get('stderr'))
                self.assertEqual(r['peak_cpus'],4)
                client.wait(peer);client.fetch(peer)

    def test_tail_returns_unused_cpu_while_the_last_item_keeps_running(self):
        with scratch()as root,provider(Path(root)/'worker',cpus=4)as(_,client):
            identity=client.submit(python_map('business:variable',[.2,.2,.2,3.5],max_cpus=4),files={'business.py':FUNCTIONS})['id']
            deadline=time.monotonic()+8;seen_wide=seen_tail=False
            while time.monotonic()<deadline:
                state=client.status(identity)
                seen_wide|=state.get('peak_cpus',0)==4
                seen_tail|=seen_wide and state.get('active_cpus')==1 and state.get('cpus')==1 and state['status']=='running'
                if state['status']in ('succeeded','failed'):break
                time.sleep(.05)
            result=client.fetch(identity)
            self.assertEqual(result['status'],'succeeded',result.get('stderr'))
            self.assertTrue(seen_tail,'The idle tail CPUs stayed reserved')

    def test_thread_mapping_preserves_shared_business_cache(self):
        with scratch()as root,provider(Path(root)/'worker',cpus=4)as(_,client):
            r=client.run(python_map('business:shared',[1,1,2,2],backend='thread',max_cpus=4),files={'business.py':FUNCTIONS})
            self.assertEqual(r['status'],'succeeded',r.get('stderr'))
            values=[x['value']for x in json.loads(r['files']['.dco-results.json'])['items']]
            self.assertEqual(sum(v['new']for v in values),2)
            self.assertEqual(len({v['pid']for v in values}),1)
            self.assertEqual({v['threads']for v in values},{'1'})

    def test_memory_model_limits_actual_width_and_keeps_future_ram_reserved(self):
        with scratch()as root,provider(Path(root)/'worker',cpus=8,memory_mb=512,memory_headroom_mb=0)as(worker,client):
            task=python_map('business:variable',[1.2]*8,max_cpus=8,base_memory_mb=64,memory_per_cpu_mb=128)
            self.assertEqual(task['memory_mb'],192)
            identity=client.submit(task,files={'business.py':FUNCTIONS})['id']
            deadline=time.monotonic()+10;observed=False
            while time.monotonic()<deadline:
                state=client.status(identity)
                if state['status']=='running'and state.get('cpus')==3:
                    observed=True;self.assertEqual(state['memory_mb'],448)
                    self.assertGreaterEqual(worker.health()['memory_reserved_mb'],448)
                if state['status']in ('succeeded','failed'):break
                time.sleep(.05)
            result=client.fetch(identity)
            self.assertEqual(result['status'],'succeeded',result.get('stderr'))
            self.assertTrue(observed)
            self.assertLessEqual(result['peak_cpus'],3)
