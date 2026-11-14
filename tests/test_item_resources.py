"""Whole lanes have priority; item kernels share one parent grant."""
import json,os,time,threading,unittest
from pathlib import Path
from unittest.mock import patch

from remote_compute import python_map,python_task
from remote_compute.execution import _Grants
from remote_compute.item_resources import ItemResources
from test_compute import provider,scratch,until

BUSINESS='''import os,time
from remote_compute import run_call
def kernel():
    width=int(os.environ['REMOTE_COMPUTE_CPUS'])
    time.sleep(.04)
    return dict(width=width,value=sum(i*i for i in range(32)))
def lane(item):
    rows=[]
    for _ in range(item):rows.append(run_call(kernel,limits={'max_cpus':8}))
    return dict(rows=rows,returned=int(os.environ['REMOTE_COMPUTE_CPUS']))
def failed_kernel():raise ValueError('batch failed')
def recovery(item):
    try:run_call(failed_kernel,limits={'max_cpus':8})
    except ValueError:pass
    return lane(item)
def entry():
    from remote_compute import map_items
    return map_items('business:lane',[2],item_max_cpus=8)
def dynamic(item):
    from pathlib import Path
    values=[]
    for index in range(4):
        Path('ready'+str(index)).write_text('yes')
        while not Path('go'+str(index)).exists():time.sleep(.01)
        deadline=time.monotonic()+4
        while True:
            value=run_call(kernel,limits={'max_cpus':8})
            if value['width']==(1,4,8,1)[index]:break
            if time.monotonic()>deadline:raise TimeoutError('Parent grant change did not become visible')
        values.append(value)
    return values
'''


class ItemResourcesTests(unittest.TestCase):
    def test_available_thread_limits_still_apply_without_controller_api(self):
        import sys,types
        from unittest.mock import Mock
        from remote_compute import execution
        module=types.ModuleType('threadpoolctl');module.threadpool_limits=Mock()
        old=(execution._threadpool_controller,execution._threadpool_legacy_limits,execution._threadpool_module_count,execution._kernel_thread_count)
        execution._threadpool_module_count=-1
        try:
            with patch.dict(sys.modules,threadpoolctl=module),patch.dict(os.environ,{}):
                execution._kernel_threads(4);os.environ['OMP_NUM_THREADS']='8'
                execution._kernel_threads(1,reset=True)
                self.assertEqual([call.kwargs['limits']for call in module.threadpool_limits.call_args_list],[4,1])
                self.assertEqual(os.environ['OMP_NUM_THREADS'],'1')
        finally:(execution._threadpool_controller,execution._threadpool_legacy_limits,execution._threadpool_module_count,execution._kernel_thread_count)=old
    def test_one_core_fast_batches_skip_round_trips_and_see_parent_growth(self):
        from remote_compute.item_resources import ItemChannel
        with patch.dict(os.environ,{'REMOTE_COMPUTE_URL':'','REMOTE_COMPUTE_CPUS':'1'}):
            grants=_Grants(8);resources=ItemResources(grants,8,1)
            resources.reserve(0,1);resources.report(0);channel=ItemChannel(*resources.channel(0))
            try:
                self.assertEqual(channel.request('enter',8),1)
                with grants.condition:self.assertTrue(grants.condition.wait_for(lambda:resources.items[0]['desired']==8,timeout=2))
                channel.request('leave')
                with patch.object(channel.connection,'send',wraps=channel.connection.send)as send:
                    for _ in range(10):
                        self.assertEqual(channel.request('enter',8),1);channel.request('leave')
                    send.assert_not_called()
                with grants.condition:grants.cpus=4;resources._publish()
                self.assertEqual(channel.request('enter',8),4)
                self.assertEqual(grants.active,4)
                channel.request('leave')
                with grants.condition:grants.cpus=1;resources._publish()
                self.assertEqual(channel.request('enter',8),1);channel.request('leave')
            finally:channel.close();resources.finish(0);resources.close();grants.close()
    def test_threadpool_discovery_is_cached_and_refreshes_after_new_import(self):
        import sys,types
        from unittest.mock import Mock
        from remote_compute import execution
        factory=Mock();module=types.ModuleType('threadpoolctl');module.ThreadpoolController=factory
        old=execution._threadpool_controller,execution._threadpool_module_count,execution._kernel_thread_count
        execution._threadpool_controller=None;execution._threadpool_module_count=-1;execution._kernel_thread_count=None
        try:
            with patch.dict(sys.modules,threadpoolctl=module),patch.dict(os.environ,{}):
                execution._kernel_threads(4);execution._kernel_threads(1)
                factory.assert_called_once()
                calls=factory.return_value.limit.call_count
                execution._kernel_threads(1)
                self.assertEqual(factory.return_value.limit.call_count,calls)
                execution._kernel_threads(1,reset=True)
                self.assertEqual(factory.return_value.limit.call_count,calls+1)
                with patch.dict(sys.modules,_dco_new_numeric_module=types.ModuleType('_dco_new_numeric_module')):
                    execution._kernel_threads(2)
                    self.assertEqual(factory.call_count,2)
                    factory.return_value.limit.assert_called_with(limits=2)
        finally:execution._threadpool_controller,execution._threadpool_module_count,execution._kernel_thread_count=old
    def test_pending_shrink_preserves_baselines_without_waiting_for_optional_extras(self):
        from unittest.mock import Mock
        with patch.dict(os.environ,{'REMOTE_COMPUTE_URL':'','REMOTE_COMPUTE_CPUS':'8'}):
            grants=_Grants(8);resources=ItemResources(grants,8,2)
            grants.lease=Mock();resources.reserve(0,2)
            resources.report(0)
            grants.pending=True;grants.pending_floor=2
            results={}
            def enter(index):
                results[index]=resources.stage(index,8)
            threads=[threading.Thread(target=enter,args=(index,))for index in(0,1)]
            for thread in threads:thread.start()
            for thread in threads:thread.join(2);self.assertFalse(thread.is_alive())
            self.assertEqual(results,{0:1,1:1});self.assertEqual(grants.active,2)
            with grants.condition:grants.cpus=4;grants.pending=False;grants.fenced=False
            self.assertEqual(resources.stage(0,8),3)
            self.assertEqual(resources.stage(1,8),1)
            self.assertEqual(grants.active,4)
            resources.release(0);resources.release(1)
            grants.pending=True;grants.pending_floor=2
            self.assertEqual(resources.stage(0,8),1)
            resources.close()
            grants.lease=None;grants.close()

    def test_failed_thread_reset_still_leaves_the_item_stage(self):
        from unittest.mock import Mock
        from remote_compute import execution
        channel=Mock();channel.request.side_effect=[4,1]
        execution._local.item_channel=channel
        try:
            with patch.dict(os.environ,{'REMOTE_COMPUTE_WORKER':'1','REMOTE_COMPUTE_ITEM_MAX_CPUS':'8'}),patch.object(execution,'_kernel_threads',side_effect=[None,ValueError('reset')]):
                with self.assertRaisesRegex(ValueError,'reset'):execution.run_call(lambda:0)
            self.assertEqual([call.args[0]for call in channel.request.call_args_list],['enter','leave'])
        finally:execution._local.item_channel=None

    def test_cancel_while_parent_resize_is_pending(self):
        with scratch()as root,provider(root,cpus=1,cpu_limit=8)as(state,client):
            original=state.resize;pending=threading.Event();release=threading.Event()
            def resize(identity,cpus,**kwargs):
                if kwargs.get('active_cpus',0)and cpus>1:
                    pending.set();release.wait(8)
                return original(identity,cpus,**kwargs)
            with patch.object(state,'resize',side_effect=resize):
                identity=client.submit(python_map('business:lane',[200],item_max_cpus=8,max_cpus=8,timeout=25,retries=0),files={'business.py':BUSINESS})['id']
                try:
                    self.assertTrue(pending.wait(8))
                    client.cancel(identity)
                finally:release.set()
                self.assertEqual(client.wait(identity,wait_timeout=8)['status'],'cancelled')
            self.assertEqual(state.health()['used_cpus'],0)

    def test_finished_whole_map_exits_without_waiting_for_optional_late_ack(self):
        with scratch()as root,provider(root,cpus=1,cpu_limit=8)as(state,client):
            original=state.resize;pending=threading.Event();release=threading.Event()
            def resize(identity,cpus,**kwargs):
                if kwargs.get('active_cpus',0)and cpus>1:
                    pending.set();release.wait(12)
                return original(identity,cpus,**kwargs)
            with patch.object(state,'resize',side_effect=resize):
                identity=client.submit(python_map('business:lane',[2,2,2],item_max_cpus=8,max_cpus=8,timeout=20,retries=0),files={'business.py':BUSINESS})['id']
                try:
                    self.assertEqual(client.wait(identity,wait_timeout=6)['status'],'succeeded')
                    self.assertTrue(pending.is_set())
                    result=client.fetch(identity)
                    self.assertEqual(json.loads(result['files']['.dco-results.json'])['completed'],3)
                finally:release.set()
            self.assertEqual(state.health()['used_cpus'],0)
    def test_confirmed_inherited_grant_survives_the_centers_initial_queued_sample(self):
        from remote_compute import Client,RemoteError
        client=Client();identity='a'*32;error=RemoteError('Insufficient memory for resource resize; existing grant retained');error.status_code=409
        env=dict(REMOTE_COMPUTE_WORKER='1',REMOTE_COMPUTE_JOB_ID=identity,REMOTE_COMPUTE_URL=client.url,REMOTE_COMPUTE_MEMORY_MODEL='{"memory_mb":256}')
        with patch.dict(os.environ,env),patch.object(client,'resize',side_effect=[dict(cpus=2,memory_mb=256),error]),patch.object(client,'status',return_value=dict(status='queued',cpus=2,memory_mb=256,memory_phase={'memory_mb':256})):
            lease=client.cpu_lease(identity,max_cpus=8,cpus=1,memory_mb=256)
            self.assertEqual(lease.refresh(active_cpus=0,ready_cpus=2),2)
            self.assertEqual(lease.refresh(active_cpus=2,ready_cpus=6),2)
        with patch.dict(os.environ,env),patch.object(client,'resize',side_effect=error),patch.object(client,'status',return_value=dict(status='queued',cpus=2,memory_mb=256,memory_phase={'memory_mb':256})):
            with self.assertRaises(RemoteError):client.cpu_lease(identity,max_cpus=8,cpus=2,memory_mb=256).refresh(active_cpus=2,ready_cpus=6)
    def test_declaration_and_queued_lanes_precede_kernel_extras(self):
        task=python_map('business:lane',[1]*10,item_max_cpus=8,max_cpus=80)
        self.assertEqual(task['max_cpus'],80)
        self.assertEqual(task['env']['REMOTE_COMPUTE_ITEM_MAX_CPUS'],'8')
        for bad in(0,-1,True,1.5):
            with self.assertRaises(ValueError):python_map('business:lane',[],item_max_cpus=bad)
        with self.assertRaises(ValueError):python_map('business:lane',[1],item_max_cpus=8,backend='thread')
        with patch.dict(os.environ,{'REMOTE_COMPUTE_URL':'','REMOTE_COMPUTE_CPUS':'8'}):
            grants=_Grants(8);resources=ItemResources(grants,8,10)
            try:
                self.assertEqual(resources.reserve(0,2),2)
                resources.report(8)
                self.assertEqual(resources.stage(0,8),1)
                resources.report(0)
                self.assertEqual(resources.stage(0,8),7)
                self.assertEqual(resources.stage(1,8),1)
                self.assertEqual(grants.active,8)
                resources.release(0);resources.finish(0)
                self.assertEqual(resources.stage(1,8),8)
                self.assertEqual(resources.release(1),1)
                self.assertEqual(grants.active,1)
            finally:resources.close();grants.close()

    def test_fixed_real_grants_one_two_four_eight_and_exception_return(self):
        for cpus in(1,2,4,8):
            with self.subTest(cpus=cpus),scratch()as root,provider(root,cpus=cpus)as(_,client):
                result=client.run(python_map('business:recovery',[3],item_max_cpus=8,max_cpus=8,timeout=30,retries=0),files={'business.py':BUSINESS})
                self.assertEqual(result['status'],'succeeded',result.get('stderr'))
                value=json.loads(result['files']['.dco-results.json'])['items'][0]['value']
                self.assertEqual(max(row['width']for row in value['rows']),cpus,value)
                self.assertTrue(all(1<=row['width']<=cpus for row in value['rows']),value)
                self.assertEqual(value['returned'],1)
                self.assertTrue(all(row['value']==10416 for row in value['rows']))

    def test_ten_lane_tail_expands_under_single_parent_lease(self):
        with scratch()as root,provider(root,cpus=8)as(_,client):
            result=client.run(python_map('business:lane',[1]*9+[30],item_max_cpus=8,max_cpus=80,timeout=45,retries=0),files={'business.py':BUSINESS})
            self.assertEqual(result['status'],'succeeded',result.get('stderr'))
            values=[row['value']for row in json.loads(result['files']['.dco-results.json'])['items']]
            self.assertEqual(len(values),10)
            # Finished peers can lend slots before every earlier index returns.
            # The first lane starts narrow; the long tail uses the whole grant.
            self.assertEqual(values[0]['rows'][0]['width'],1,values[0])
            self.assertTrue(all(1<=row['width']<=8 for value in values for row in value['rows']))
            self.assertEqual(result['peak_cpus'],8)
            self.assertEqual(max(row['width']for row in values[-1]['rows']),8,values[-1])
            self.assertEqual(client.health()['used_cpus'],0)

    def test_entry_map_can_use_multicore_items_without_task_environment_flag(self):
        with scratch()as root,provider(root,cpus=4)as(_,client):
            result=client.run(python_task('business:entry',max_cpus=8,timeout=30,retries=0),files={'business.py':BUSINESS})
            self.assertEqual(result['status'],'succeeded',result.get('stderr'))
            value=json.loads(result['files']['.dco-results.json'])['items'][0]['value']
            self.assertEqual(max(row['width']for row in value[0]['rows']),4)

    def test_same_lane_tracks_one_four_eight_one_at_completed_batch_boundaries(self):
        with scratch()as root,provider(root,cpus=1,cpu_limit=8)as(state,client):
            identity=client.submit(python_map('business:dynamic',[0],item_max_cpus=8,max_cpus=8,timeout=30,retries=0),files={'business.py':BUSINESS})['id']
            for index,cpus in enumerate((1,4,8,1)):
                def ready():
                    return (state.jobs[identity]['directory']/'input'/('ready'+str(index))).exists()
                until(ready)
                state.control(dict(cpus=cpus))
                (state.jobs[identity]['directory']/'input'/('go'+str(index))).write_text('yes')
            result=client.wait(identity,wait_timeout=15)
            self.assertEqual(result['status'],'succeeded',result.get('stderr'))
            result=client.fetch(identity)
            values=json.loads(result['files']['.dco-results.json'])['items'][0]['value']
            self.assertEqual([row['width']for row in values],[1,4,8,1])


if __name__=='__main__':unittest.main()
