"""A real shrink between the capacity snapshot and admission keeps starts funded."""
import json,threading,unittest
from pathlib import Path
from remote_compute import Pool,RemoteError,python_map,python_task
from test_compute import provider,scratch,until
from test_center import center

BUSINESS='''import os,threading,time
from remote_compute import Client
from remote_compute import execution

def hold(value):
    from pathlib import Path
    Path('started-'+str(value)).write_text('yes')
    while not Path('release').exists():time.sleep(.02)
    return value

def evaluate(value):
    start=time.time()
    state=Client(os.environ['REMOTE_COMPUTE_URL'],os.environ.get('REMOTE_COMPUTE_TOKEN','')).status(os.environ['REMOTE_COMPUTE_JOB_ID'])
    time.sleep(.1)
    return dict(value=value,cpus=state['cpus'],started=start,finished=time.time())

def run(backend,scenario):
    original_class=execution._Grants;original_resize=Client.resize
    client=Client(os.environ['REMOTE_COMPUTE_URL'],os.environ.get('REMOTE_COMPUTE_TOKEN',''))
    identity=os.environ['REMOTE_COMPUTE_JOB_ID']
    initial=original_resize(client,identity,4)
    assert initial['cpus']==4,initial
    if scenario=='entry':assert original_resize(client,identity,1)['cpus']==1
    prior_cpu=os.environ.get('REMOTE_COMPUTE_CPUS');os.environ['REMOTE_COMPUTE_CPUS']='4'
    gate=threading.Event();instances=[];snapshots=[]
    rpc_entered=threading.Event();allow_reply=threading.Event();pending_reservations=[]
    class GatedGrants(original_class):
        def __init__(self,*args,**kwargs):
            super().__init__(*args,**kwargs);instances.append(self)
        def _run(self):
            if not gate.wait(5):raise RuntimeError('Capacity snapshot not reached')
            super()._run()
        def reserve(self,requested):
            count=super().reserve(requested)
            if scenario=='pending'and rpc_entered.is_set()and not allow_reply.is_set():
                pending_reservations.append(count)
                assert count==0,'Admitted inputs while a zero-active shrink was pending'
                allow_reply.set()
            return count
    def capped(self,identity,cpus,**options):
        # Real HTTP grants stay at one after the forced shrink. The executor
        # still has four useful inputs, and must honor the acknowledged grant.
        if scenario=='pending'and gate.is_set()and threading.current_thread().name=='dco-executor-resources'and not rpc_entered.is_set():
            rpc_entered.set()
            if not allow_reply.wait(5):raise RuntimeError('No admission attempt reached the pending RPC')
        capped_cpus=1 if scenario=='entry'or gate.is_set()else cpus
        return original_resize(self,identity,min(cpus,capped_cpus),**options)
    def capacity(width):
        if snapshots:return
        snapshots.append(width)
        if scenario=='entry':return
        gate.set();grants=instances[0]
        if scenario=='pending':
            assert rpc_entered.wait(5)
            return
        with grants.condition:
            assert grants.condition.wait_for(lambda:grants.cpus==1 or grants.error,timeout=5)
            if grants.error:raise grants.error
    def started(index):
        if index:return
        gate.set()
        if scenario=='pending':assert allow_reply.is_set(),'Started input before shrink reply'
        done=threading.Event()
        def query():
            instances[0].limit();done.set()
        reader=threading.Thread(target=query);reader.start()
        assert done.wait(2),'Publication callback blocked resource ownership'
        reader.join()
    execution._Grants=GatedGrants;Client.resize=capped
    try:
        values=execution._map_items('business:evaluate',list(range(8)),backend=backend,on_capacity=capacity,on_start=started)
        return dict(values=values,capacity_snapshot=snapshots,initial_cpus=initial['cpus'],pending_reservations=pending_reservations)
    finally:
        gate.set();allow_reply.set();execution._Grants=original_class;Client.resize=original_resize
        if prior_cpu is None:os.environ.pop('REMOTE_COMPUTE_CPUS',None)
        else:os.environ['REMOTE_COMPUTE_CPUS']=prior_cpu
'''


class ExecutorAdmissionTests(unittest.TestCase):
    def test_policy_contraction_preserves_four_started_inputs_until_they_finish(self):
        with scratch()as root,provider(Path(root)/'worker',cpus=4,memory_mb=4096)as(worker,node),Pool([node])as pool:
            with center(pool,Path(root)/'center')as(state,client):
                identity=client.submit(python_map('business:hold',range(5),max_cpus=8,memory_mb=1024,timeout=30),files={'business.py':BUSINESS})['id']
                def started():
                    try:return all(client.read_job_file(identity,'started-'+str(i))==b'yes'for i in range(4))
                    except RemoteError as error:
                        if error.status_code in (404,409):return False
                        raise
                try:
                    until(started,seconds=10)
                    self.assertEqual(client.status(identity)['cpus'],4)
                    original=state.resize;acknowledged=threading.Event();replies=[]
                    def observed(*args,**kwargs):
                        value=original(*args,**kwargs)
                        if kwargs.get('active_cpus')==4:
                            replies.append(value['cpus']);acknowledged.set()
                        return value
                    from unittest.mock import patch
                    with patch.object(state,'resize',side_effect=observed):
                        client.control(node='node1',routing={'limit':1})
                        self.assertTrue(acknowledged.wait(5),'No resource report arrived after policy contraction')
                    self.assertTrue(all(cpus>=4 for cpus in replies))
                    self.assertEqual(worker.health()['used_cpus'],4)
                    self.assertEqual(client.status(identity)['cpus'],4)
                finally:
                    try:client.write_job_file(identity,'release','yes')
                    except RemoteError:pass
                client.wait(identity,wait_timeout=10);result=client.fetch(identity)
                self.assertEqual(result['status'],'succeeded',result.get('stderr'))
                values=json.loads(result['files']['.dco-results.json'])['items']
                self.assertEqual([row['value']for row in values],list(range(5)))
                self.assertEqual(result['cpus'],1)

    def test_shrink_after_snapshot_preserves_all_inputs_and_grant_for_both_backends(self):
      for scenario in ('snapshot','entry','pending'):
        for backend in ('process','thread'):
            with self.subTest(scenario=scenario,backend=backend),scratch()as root,provider(Path(root)/'worker',cpus=4,memory_mb=4096)as(_,client):
                task=python_task('business:run',kwargs=dict(backend=backend,scenario=scenario),max_cpus=4,memory_mb=1024,timeout=30)
                result=client.run(task,files={'business.py':BUSINESS})
                self.assertEqual(result['status'],'succeeded',result.get('stderr'))
                value=json.loads(result['files']['.dco-results.json'])['items'][0]['value']
                self.assertEqual(value['capacity_snapshot'],[1 if scenario=='entry'else 4])
                if scenario=='pending':self.assertEqual(value['pending_reservations'],[0])
                self.assertEqual(value['initial_cpus'],4)
                self.assertEqual([row['value']for row in value['values']],list(range(8)))
                self.assertTrue(all(row['cpus']==1 for row in value['values']))
                concurrency=max(sum(row['started']<=point<row['finished']for row in value['values'])for point in [row['started']for row in value['values']])
                self.assertEqual(concurrency,1)


if __name__=='__main__':unittest.main()
