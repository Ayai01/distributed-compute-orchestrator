"""Public defaults drive actual kernel/process parallelism without caller leases."""
import inspect,json,time,unittest,sys
from pathlib import Path
from remote_compute import Client,Pool,command,python_map,python_call,python_task
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
def phased():
    from pathlib import Path
    from remote_compute import map_items
    first=map_items('business:evaluate',[1,2,3],backend='thread')
    print('serial-boundary',flush=True)
    while not Path('release').exists():time.sleep(.02)
    second=map_items('business:evaluate',[4,5,6],backend='thread')
    return [first,second]
def streaming():
    from pathlib import Path
    from remote_compute import iter_items
    with iter_items('business:variable',[.6,.1,.3],backend='thread')as completed:
        first=next(completed)
        print('consumer-paused',flush=True)
        while not Path('release').exists():time.sleep(.02)
        return [first.index]+[item.index for item in completed]
def bounded_stream():
    from pathlib import Path
    from remote_compute import iter_items
    with iter_items('business:evaluate',range(40),backend='thread')as completed:
        first=next(completed)
        print('bounded-consumer-paused',flush=True)
        while not Path('release').exists():time.sleep(.02)
        retained=dict(started=completed.started,queued=completed.queue.qsize(),window=completed.window)
        values=[first]+list(completed)
        return dict(retained=retained,indices=[item.index for item in values])
def abandoned_stream():
    from remote_compute import iter_items
    with iter_items('business:evaluate',range(20),backend='thread')as completed:
        while completed.started<2:time.sleep(.01)
        first=next(completed)
        completed.stop()
        return first.index
def failing_stream_kernel():
    from remote_compute import iter_items,run_call,execution
    with iter_items('business:evaluate',[1],backend='thread')as completed:
        result=next(completed)
        completed.thread.join()
        completed.error=RuntimeError('terminal stream failure')
        return run_call('business:kernel')
def stopped_stream():
    from remote_compute import iter_items
    with iter_items('business:evaluate',list(range(20)),backend='thread')as completed:
        first=next(completed);completed.stop()
        values=[first]+list(completed)
        return dict(indices=[item.index for item in values],started=completed.started)
def checked_kernel():
    from remote_compute import Client
    client=Client(os.environ['REMOTE_COMPUTE_URL'],os.environ.get('REMOTE_COMPUTE_TOKEN',''))
    time.sleep(.3)
    return dict(threads=int(os.environ['OMP_NUM_THREADS']),actual=client.status(os.environ['REMOTE_COMPUTE_JOB_ID'])['cpus'])
def final_stream_kernel():
    from remote_compute import iter_items,run_call,execution
    original=execution._Grants.close
    def delayed_close(grants):time.sleep(.2);original(grants)
    execution._Grants.close=delayed_close
    try:
        count=0
        with iter_items('business:evaluate',[1,2,3],backend='thread')as completed:
            for result in completed:
                count+=1
                if count==3:
                    return run_call('business:checked_kernel',limits={'max_cpus':2,'base_memory_mb':64,'memory_per_cpu_mb':128})
    finally:execution._Grants.close=original
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
def observed_memory():
    from remote_compute import Client
    state=Client(os.environ['REMOTE_COMPUTE_URL'],os.environ.get('REMOTE_COMPUTE_TOKEN','')).status(os.environ['REMOTE_COMPUTE_JOB_ID'])
    return dict(cpus=state['cpus'],memory_mb=state['memory_mb'])
def memory_item(value):
    from pathlib import Path
    state=observed_memory();start=time.time();time.sleep(.45)
    Path('memory-started-'+str(value)).write_text('yes')
    deadline=time.monotonic()+5
    while not all(Path('memory-started-'+str(i)).exists()for i in range(8)):
        if time.monotonic()>deadline:raise TimeoutError('Eight funded memory-phase items did not start')
        time.sleep(.01)
    time.sleep(.1)
    return dict(value=value,state=state,started=start,finished=time.time())
def phased_memory(backend,stream):
    from remote_compute import map_items,iter_items,run_call
    cold=run_call(observed_memory,limits=dict(max_cpus=1,memory_mb=2304))
    limits=dict(max_cpus=8,base_memory_mb=512,memory_per_cpu_mb=896)
    if stream:
        with iter_items('business:memory_item',range(8),backend=backend,limits=limits)as completed:
            values=[item.value for item in completed]
    else:values=map_items('business:memory_item',range(8),backend=backend,limits=limits)
    idle=observed_memory()
    serial=run_call(observed_memory,limits=dict(max_cpus=1,memory_mb=1536))
    return dict(cold=cold,values=values,idle=idle,serial=serial,parent=json.loads(os.environ['REMOTE_COMPUTE_MEMORY_MODEL']))
def expensive_map():
    from remote_compute import map_items
    return map_items('business:unexpected_item',[1],limits=dict(memory_mb=1024))
def capped_map(stream):
    from remote_compute import map_items,iter_items
    options=dict(backend='thread',limits=dict(max_cpus=2,memory_mb=512))
    if stream:
        with iter_items('business:evaluate',range(8),**options)as completed:
            return [item.value for item in completed]
    return map_items('business:evaluate',range(8),**options)
def unexpected_item(value):
    from pathlib import Path
    Path('unexpected-item').write_text('started without phase memory')
    return value
'''

class AutomaticExecutionTests(unittest.TestCase):
    def test_map_continues_after_retained_center_reply_timeout_without_repeating_items(self):
        code=FUNCTIONS+'''
def timeout_map():
    from remote_compute import Client,RemoteError,map_items
    original=Client.resize;seen=[]
    def once(client,*args,**kwargs):
        if not seen:
            seen.append(1)
            error=RemoteError('Center resource reply timed out; existing grant retained')
            error.status_code=409
            raise error
        return original(client,*args,**kwargs)
    Client.resize=once
    try:
        values=map_items('business:evaluate',range(8),limits=dict(max_cpus=4,base_memory_mb=32,memory_per_cpu_mb=32))
        return dict(values=values,timeouts=len(seen))
    finally:Client.resize=original
'''
        with scratch()as root,provider(Path(root)/'worker',cpus=4,memory_mb=4096)as(_,client):
            task=python_task('business:timeout_map',max_cpus=4,base_memory_mb=32,memory_per_cpu_mb=32)
            result=client.run(task,files={'business.py':code})
            self.assertEqual(result['status'],'succeeded',result.get('stderr'))
            value=json.loads(result['files']['.dco-results.json'])['items'][0]['value']
            self.assertEqual(value['timeouts'],1)
            self.assertEqual(sorted(row['value']for row in value['values']),list(range(0,16,2)))

    def test_map_and_stream_replace_cold_memory_before_launch_and_return_tail_ram(self):
        for backend,stream in [('process',False),('thread',False),('process',True)]:
            with self.subTest(backend=backend,stream=stream),scratch()as root,provider(Path(root)/'worker',cpus=8,memory_mb=8192)as(_,client):
                task=python_task('business:phased_memory',kwargs=dict(backend=backend,stream=stream),
                    max_cpus=8,base_memory_mb=512,memory_per_cpu_mb=1792)
                r=client.run(task,files={'business.py':'import json\n'+FUNCTIONS})
                self.assertEqual(r['status'],'succeeded',r.get('stderr'))
                result=json.loads(r['files']['.dco-results.json'])['items'][0]['value']
                self.assertEqual(result['cold'],dict(cpus=1,memory_mb=2304))
                self.assertEqual(result['parent'],dict(base_memory_mb=512,memory_per_cpu_mb=1792))
                self.assertEqual(result['idle'],dict(cpus=1,memory_mb=1408))
                self.assertEqual(result['serial'],dict(cpus=1,memory_mb=1536))
                self.assertEqual(sorted(v['value']for v in result['values']),list(range(8)))
                for value in result['values']:
                    self.assertEqual(value['state']['memory_mb'],512+896*value['state']['cpus'])
                concurrency=max(sum(v['started']<=point<v['finished']for v in result['values'])for point in [v['started']for v in result['values']])
                self.assertEqual(concurrency,8)

    def test_unfunded_map_phase_fails_before_its_first_item(self):
        with scratch()as root,provider(Path(root)/'worker',cpus=2,memory_mb=512)as(_,client):
            task=python_task('business:expensive_map',max_cpus=2,memory_mb=64,outputs=['unexpected-item'])
            r=client.run(task,files={'business.py':FUNCTIONS})
            self.assertEqual(r['status'],'failed')
            self.assertIn('Insufficient memory for resource resize',r['stderr'])
            self.assertNotIn('unexpected-item',r['files'])

    def test_map_and_stream_phase_cap_bounds_real_parallelism(self):
        for stream in (False,True):
            with self.subTest(stream=stream),scratch()as root,provider(Path(root)/'worker',cpus=8)as(_,client):
                task=python_task('business:capped_map',kwargs=dict(stream=stream),max_cpus=8)
                r=client.run(task,files={'business.py':FUNCTIONS})
                self.assertEqual(r['status'],'succeeded',r.get('stderr'))
                self.assertEqual(r['peak_cpus'],2)
                values=json.loads(r['files']['.dco-results.json'])['items'][0]['value']
                concurrency=max(sum(v['started']<=point<v['finished']for v in values)for point in [v['started']for v in values])
                self.assertEqual(concurrency,2)


    def test_memory_resize_rejection_keeps_the_kernel_running_on_its_funded_grant(self):
        from unittest.mock import patch
        from remote_compute.worker import Conflict
        for phase in ('initial','active'):
            with self.subTest(phase=phase),scratch()as root,provider(Path(root)/'worker',cpus=2)as(worker,client):
                original=worker.resize;refused=[]
                def resize(*args,**options):
                    if phase=='initial'and args[1]>1 or phase=='active'and options.get('active_cpus',0):
                        refused.append(True)
                        raise Conflict('Insufficient memory for resource resize; existing grant retained')
                    return original(*args,**options)
                with patch.object(worker,'resize',side_effect=resize):
                    result=client.run(python_call('business:kernel',max_cpus=2),files={'business.py':FUNCTIONS})
                self.assertEqual(result['status'],'succeeded',result.get('stderr'))
                self.assertTrue(refused)
                value=json.loads(result['files']['.dco-results.json'])['items'][0]['value']
                self.assertEqual(value,dict(grant=1 if phase=='initial'else 2,threads=1 if phase=='initial'else 2))
                self.assertEqual(worker.health()['used_cpus'],0)

    def test_hybrid_memory_refusal_preserves_conflict_status_and_funded_work(self):
        from unittest.mock import patch
        from remote_compute.local import LocalClient
        from remote_compute.worker import Conflict
        with scratch()as root,provider(Path(root)/'worker',cpus=2)as(worker,_):
            original=worker.resize;refused=[]
            def resize(*args,**options):
                if options.get('active_cpus',0):
                    refused.append(True)
                    raise Conflict('Insufficient memory for resource resize; existing grant retained')
                return original(*args,**options)
            with Pool([LocalClient(worker)])as pool,center(pool,Path(root)/'center')as(_,client),patch.object(worker,'resize',side_effect=resize):
                r=client.run(python_map('business:evaluate',range(6),max_cpus=2),files={'business.py':FUNCTIONS})
            self.assertEqual(r['status'],'succeeded',r.get('stderr'))
            self.assertTrue(refused)
            self.assertEqual(json.loads(r['files']['.dco-results.json'])['completed'],6)

    def test_slow_consumer_bounds_started_and_buffered_results_and_returns_idle_grant(self):
        from test_compute import until
        with scratch()as root,provider(Path(root)/'worker',cpus=4)as(_,client):
            identity=client.submit(python_task('business:bounded_stream',max_cpus=3),files={'business.py':FUNCTIONS})['id']
            until(lambda:'bounded-consumer-paused'in client.logs(identity)['stdout'],seconds=10)
            until(lambda:client.status(identity)['cpus']==1,seconds=10)
            client.write_job_file(identity,'release','yes')
            client.wait(identity);r=client.fetch(identity)
            self.assertEqual(r['status'],'succeeded',r.get('stderr'))
            value=json.loads(r['files']['.dco-results.json'])['items'][0]['value']
            self.assertEqual(value['retained'],dict(started=3,queued=2,window=3))
            self.assertEqual(sorted(value['indices']),list(range(40)))

    def test_early_exit_preserves_all_started_results_as_declared_recovery_outputs(self):
        with scratch()as root,provider(Path(root)/'worker',cpus=2)as(_,client):
            r=client.run(python_task('business:abandoned_stream',max_cpus=2),files={'business.py':FUNCTIONS})
            self.assertEqual(r['status'],'failed')
            self.assertIn('StreamResultsPending',r['stderr'])
            manifests=[json.loads(value)for name,value in r['files'].items()if name.startswith('.dco-streams/')and name.endswith('manifest.json')]
            self.assertEqual(len(manifests),1,r['files'].keys())
            self.assertEqual(manifests[0]['started'],2)
            self.assertEqual(sorted(manifests[0]['indices']),[0,1])
            # fetch(output_dir) keeps binary recovery records intact.
            binary=[name for name in r['files']if name.endswith('.pkl')]
            self.assertEqual(len(binary),2)

    def test_terminal_stream_failure_blocks_a_kernel_before_the_terminal_marker_is_consumed(self):
        with scratch()as root,provider(Path(root)/'worker',cpus=2)as(_,client):
            r=client.run(python_task('business:failing_stream_kernel',max_cpus=2),files={'business.py':FUNCTIONS})
            self.assertEqual(r['status'],'failed')
            self.assertIn('terminal stream failure',r['stderr'])

    def test_kernel_phase_limit_is_respected_without_a_resource_callback(self):
        from unittest.mock import patch
        from remote_compute.execution import run_call
        with patch.dict('os.environ',{'REMOTE_COMPUTE_WORKER':'1','REMOTE_COMPUTE_CPUS':'8','REMOTE_COMPUTE_MAX_CPUS':'8','REMOTE_COMPUTE_RESIZE':'0','DCO_ITEM_PROCESS':'0'}):
            value=run_call(lambda:int(__import__('os').environ['REMOTE_COMPUTE_CPUS']),limits={'max_cpus':2})
            self.assertEqual(value,2)
            with self.assertRaisesRegex(ValueError,'fixed memory_mb'):
                run_call(lambda:None,limits=dict(memory_mb=256,base_memory_mb=64,memory_per_cpu_mb=128))

    def test_last_completion_waits_for_resource_return_before_a_limited_kernel(self):
        with scratch()as root,provider(Path(root)/'worker',cpus=8)as(_,client):
            r=client.run(python_task('business:final_stream_kernel',max_cpus=8),files={'business.py':FUNCTIONS})
            self.assertEqual(r['status'],'succeeded',r.get('stderr'))
            value=json.loads(r['files']['.dco-results.json'])['items'][0]['value']
            self.assertEqual(value,dict(threads=2,actual=2))
            self.assertEqual(r['cpus'],1)

    def test_completion_stream_keeps_order_and_releases_cores_while_consumer_is_busy(self):
        from test_compute import until
        with scratch()as root,provider(Path(root)/'worker',cpus=4)as(_,client):
            identity=client.submit(python_task('business:streaming',max_cpus=3),files={'business.py':FUNCTIONS})['id']
            until(lambda:'consumer-paused'in client.logs(identity)['stdout'],seconds=10)
            until(lambda:client.status(identity)['cpus']==1,seconds=10)
            client.write_job_file(identity,'release','yes')
            r=client.wait(identity);r=client.fetch(identity)
            self.assertEqual(r['status'],'succeeded',r.get('stderr'))
            order=json.loads(r['files']['.dco-results.json'])['items'][0]['value']
            self.assertEqual(order,[1,2,0])

    def test_stopping_completion_stream_drains_started_items_without_launching_the_rest(self):
        with scratch()as root,provider(Path(root)/'worker',cpus=2)as(_,client):
            r=client.run(python_task('business:stopped_stream',max_cpus=2),files={'business.py':FUNCTIONS})
            self.assertEqual(r['status'],'succeeded',r.get('stderr'))
            value=json.loads(r['files']['.dco-results.json'])['items'][0]['value']
            self.assertLess(value['started'],20)
            self.assertEqual(sorted(value['indices']),list(range(value['started'])))

    def test_business_entry_grows_in_parallel_stages_and_returns_to_one_between_them(self):
        from test_compute import until
        with scratch()as root,provider(Path(root)/'worker',cpus=4)as(_,client):
            identity=client.submit(python_task('business:phased',max_cpus=3),files={'business.py':FUNCTIONS})['id']
            until(lambda:'serial-boundary'in client.logs(identity)['stdout'],seconds=10)
            self.assertEqual(client.status(identity)['cpus'],1)
            client.write_job_file(identity,'release','yes')
            r=client.wait(identity);r=client.fetch(identity)
            self.assertEqual(r['status'],'succeeded',r.get('stderr'))
            self.assertEqual(r['peak_cpus'],3);self.assertEqual(r['cpus'],1)
            stages=json.loads(r['files']['.dco-results.json'])['items'][0]['value']
            for values in stages:
                concurrency=max(sum(v['started']<=point<v['finished']for v in values)for point in [v['started']for v in values])
                self.assertGreaterEqual(concurrency,2)

    def test_memory_resize_rejection_keeps_the_map_running_on_its_existing_grant(self):
        from unittest.mock import patch
        from remote_compute.worker import Conflict
        for phase in ('initial','runtime'):
            with self.subTest(phase=phase),scratch()as root,provider(Path(root)/'worker',cpus=2)as(worker,client):
                original=worker.resize;refused=[]
                def resize(*args,**options):
                    if options.get('active_cpus',0)or phase=='initial'and args[1]>1:
                        refused.append(options['active_cpus'])
                        raise Conflict('Insufficient memory for resource resize; existing grant retained')
                    return original(*args,**options)
                with patch.object(worker,'resize',side_effect=resize):
                    r=client.run(python_map('business:evaluate',range(8),max_cpus=2),files={'business.py':FUNCTIONS})
                self.assertEqual(r['status'],'succeeded',r.get('stderr'))
                self.assertTrue(refused)
                values=[row['value']['value']for row in json.loads(r['files']['.dco-results.json'])['items']]
                self.assertEqual(values,[i*2 for i in range(8)])
                self.assertEqual(worker.health()['used_cpus'],0)

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
                snapshot=worker.health()
                active=next((row for row in snapshot['active_jobs']if row['id']==identity),{})
                state=client.status(identity)
                if active.get('cpus')==3:
                    observed=True;self.assertEqual(active['memory_mb'],448)
                    self.assertGreaterEqual(snapshot['memory_reserved_mb'],448)
                if state['status']in ('succeeded','failed'):break
                time.sleep(.05)
            result=client.fetch(identity)
            self.assertEqual(result['status'],'succeeded',result.get('stderr'))
            self.assertTrue(observed)
            self.assertLessEqual(result['peak_cpus'],3)
