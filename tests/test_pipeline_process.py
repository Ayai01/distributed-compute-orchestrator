"""Independent client processes share short project state writes and credits."""
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import json,subprocess,sys,threading,time,unittest

from remote_compute import Pool,command
from test_center import center
from test_compute import scratch,provider


def task(number):
    return dict(task=command(['${tool:python}','-c','print('+str(number)+')'],max_cpus=1))


CHILD=r'''import json,sys
from pathlib import Path
from remote_compute import Client
from remote_compute.bundle import atomic_json
p=json.loads(Path(sys.argv[1]).read_bytes());client=Client(p['url'],timeout=5)
if p['mode']=='recovery':
 from remote_compute.pipeline_control import _Call
 original=_Call.recovered
 def recovered(self):
  atomic_json(Path(p['marker']),dict(phase='recovering'))
  sys.stdin.readline()
  return original(self)
 _Call.recovered=recovered
def prepare(spec,identity):
 atomic_json(Path(p['marker']),dict(id=identity,phase='prepare'))
 sys.stdin.readline()
 return spec
def publish(receipt):
 atomic_json(Path(p['marker']),receipt)
 sys.stdin.readline()
results=client.execute(p['plan'],project='process-test',output_dir=p['output'],ready_tasks=p['window'],
 prepare=prepare if p['mode']=='prepare'else None,publish=publish if p['mode']=='publish'else None)
print(json.dumps([dict(id=r['id'],status=r['status'],stdout=r['stdout'])for r in results]),flush=True)
'''


class ProcessPipelineTests(unittest.TestCase):
    @contextmanager
    def child(self,root,client,output,*,mode='',window=2,plan=None):
        output=Path(output);request=Path(root)/('request-'+output.name+'.json')
        marker=Path(root)/('marker-'+output.name+'.json')
        request.write_text(json.dumps(dict(url=client.url,output=str(output),mode=mode,
            window=window,plan=plan or {'one':task(1)},marker=str(marker))),encoding='utf8')
        with subprocess.Popen([sys.executable,'-B','-c',CHILD,str(request)],stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)as child:
            try:yield child,marker
            finally:
                if child.poll()is None:child.terminate()
                child.communicate(timeout=5)

    def marker(self,path,child):
        deadline=time.monotonic()+5
        while not path.exists():
            if child.poll()is not None:self.fail(child.communicate()[1])
            if time.monotonic()>deadline:self.fail('Child did not reach its callback')
            time.sleep(.02)
        return json.loads(path.read_bytes())

    def test_independent_process_finishes_while_peer_publication_waits(self):
        with scratch()as root,provider(Path(root)/'worker',cpus=2)as(_,worker):
            with Pool([worker])as pool,center(pool,Path(root)/'center')as(_,client):
                entered=threading.Event();release=threading.Event()
                def publish(receipt):
                    entered.set()
                    if not release.wait(10):raise RuntimeError('Publication not released')
                with ThreadPoolExecutor(1)as callers:
                    first=callers.submit(client.execute,{'one':task(1)},project='process-test',
                        output_dir=Path(root)/'first',publish=publish,ready_tasks=2)
                    try:
                        self.assertTrue(entered.wait(4))
                        with self.child(root,client,Path(root)/'second',plan={'two':task(2)})as(child,_):
                            out,err=child.communicate(timeout=6)
                            self.assertEqual(child.returncode,0,err)
                            self.assertEqual(json.loads(out)[0]['stdout'].strip(),'2')
                    finally:release.set()
                    self.assertEqual(first.result(timeout=6)[0]['stdout'].strip(),'1')

    def test_existing_controller_recovers_a_peer_that_exits_during_scan(self):
        with scratch()as root,provider(Path(root)/'worker',cpus=2)as(_,worker):
            with Pool([worker])as pool,center(pool,Path(root)/'center')as(_,client):
                entered=threading.Event();release=threading.Event();stop=threading.Event()
                def publish(receipt):
                    if receipt['business_key']=='one':
                        entered.set()
                        if not release.wait(10):raise RuntimeError('Publication not released')
                with ThreadPoolExecutor(1)as callers:
                    active=callers.submit(client.execute,{'one':task(1),'two':dict(spec=task(2),after=['one'])},
                        project='process-test',output_dir=Path(root)/'first',publish=publish,ready_tasks=2,stop_when=stop.is_set)
                    try:
                        self.assertTrue(entered.wait(4))
                        with self.child(root,client,Path(root)/'second',mode='recovery')as(child,marker):
                            self.marker(marker,child);release.set();time.sleep(.1)
                            child.terminate();child.wait(timeout=5)
                            self.assertEqual([r['stdout'].strip()for r in active.result(timeout=6)],['1','2'])
                    finally:release.set();stop.set()

    def test_missing_output_preserves_known_ids_and_clears_recovery_gate(self):
        from hashlib import sha256
        from remote_compute.bundle import atomic_json
        from remote_compute.pipeline_control import _owner
        with scratch()as root,provider(Path(root)/'worker',cpus=1)as(_,worker):
            with Pool([worker])as pool,center(pool,Path(root)/'center')as(_,client):
                key=sha256((client.url+'\0process-test').encode()).hexdigest()
                registry=Path.cwd()/'.local/dco-client'/key/'executions.json'
                missing=_owner(Path(root)/'missing');original='abcde012345678901234567890123456'
                registry.parent.mkdir(parents=True,exist_ok=True)
                atomic_json(registry,dict(project='process-test',ready_tasks=2,
                    directories={missing:dict(pending={original:'0'*64},recovering=True)}))
                deadline=time.monotonic()+4
                result=client.execute({'one':task(1)},project='process-test',output_dir=Path(root)/'first',
                    ready_tasks=2,stop_when=lambda:time.monotonic()>deadline)
                self.assertEqual(result[0]['stdout'].strip(),'1')
                retained=json.loads(registry.read_bytes())['directories'][missing]
                self.assertFalse(retained['recovering']);self.assertIn(original,retained['pending'])

    def test_simultaneous_processes_share_one_preparation_credit(self):
        with scratch()as root,provider(Path(root)/'worker',cpus=2)as(_,worker):
            with Pool([worker])as pool,center(pool,Path(root)/'center')as(_,client):
                with self.child(root,client,Path(root)/'first',mode='prepare',window=1)as(a,ma), \
                     self.child(root,client,Path(root)/'second',mode='prepare',window=1)as(b,mb):
                    deadline=time.monotonic()+5
                    while not(ma.exists()or mb.exists()):
                        if time.monotonic()>deadline:self.fail('No preparation started')
                        time.sleep(.02)
                    time.sleep(.15);self.assertEqual(int(ma.exists())+int(mb.exists()),1)
                    for child in (a,b):child.stdin.write('\n');child.stdin.flush()
                    originals=[]
                    for child in (a,b):
                        out,err=child.communicate(timeout=8);self.assertEqual(child.returncode,0,err)
                        originals.extend(row['id']for row in json.loads(out))
                    self.assertEqual(len(set(originals)),2)
                    self.assertTrue(all(row['status']=='unknown'for row in client.statuses(originals).values()))

    def test_dead_preparation_owner_releases_unused_credit(self):
        with scratch()as root,provider(Path(root)/'worker',cpus=1)as(_,worker):
            with Pool([worker])as pool,center(pool,Path(root)/'center')as(_,client):
                with self.child(root,client,Path(root)/'first',mode='prepare',window=1)as(child,marker):
                    original=self.marker(marker,child)['id'];child.terminate();child.wait(timeout=5)
                deadline=time.monotonic()+4
                results=client.execute({'peer':task(2)},project='process-test',output_dir=Path(root)/'second',
                    ready_tasks=1,stop_when=lambda:time.monotonic()>deadline)
                self.assertEqual(results[0]['stdout'].strip(),'2')
                self.assertEqual(client.statuses([original])[original]['status'],'unknown')

    def test_dead_publication_owner_keeps_paid_original_until_resume(self):
        with scratch()as root,provider(Path(root)/'worker',cpus=2)as(_,worker):
            with Pool([worker])as pool,center(pool,Path(root)/'center')as(_,client):
                output=Path(root)/'first';plan=json.loads(json.dumps({'one':task(1)}))
                with self.child(root,client,output,mode='publish',window=1,plan=plan)as(child,marker):
                    original=self.marker(marker,child)['id'];child.terminate();child.wait(timeout=5)
                prepared=threading.Event();stop=threading.Event()
                def prepare(spec,identity):prepared.set();return spec
                with ThreadPoolExecutor(1)as callers:
                    peer=callers.submit(client.execute,{'peer':task(2)},project='process-test',
                        output_dir=Path(root)/'second',prepare=prepare,ready_tasks=1,stop_when=stop.is_set)
                    try:
                        self.assertFalse(prepared.wait(.2))
                        resumed=client.execute(plan,project='process-test',output_dir=output,ready_tasks=1)
                        self.assertEqual(resumed[0]['id'],original)
                        self.assertEqual(peer.result(timeout=6)[0]['stdout'].strip(),'2')
                    finally:stop.set()
                self.assertEqual(client.statuses([original])[original]['status'],'unknown')

    def test_directory_index_migration_keeps_the_unpublished_original(self):
        from hashlib import sha256
        from remote_compute import ExecutionFailure
        from remote_compute.bundle import atomic_json
        with scratch()as root,provider(Path(root)/'worker',cpus=1)as(_,worker):
            with Pool([worker])as pool,center(pool,Path(root)/'center')as(_,client):
                output=Path(root)/'first';plan={'one':task(1)};paid=[]
                def publish(receipt):paid.append(receipt['id']);raise RuntimeError('Publication interrupted')
                with self.assertRaises(ExecutionFailure):
                    client.execute(plan,project='process-test',output_dir=output,publish=publish,ready_tasks=1)
                key=sha256((client.url+'\0process-test').encode()).hexdigest()
                registry=Path.cwd()/'.local/dco-client'/key/'executions.json'
                atomic_json(registry,dict(project='process-test',directories=[str(output)]))
                with self.child(root,client,Path(root)/'second',window=1)as(child,_):
                    time.sleep(.2);self.assertIsNone(child.poll())
                    resumed=client.execute(plan,project='process-test',output_dir=output,ready_tasks=1)
                    self.assertEqual(resumed[0]['id'],paid[0])
                    _,err=child.communicate(timeout=6);self.assertEqual(child.returncode,0,err)
                self.assertEqual(client.statuses(paid)[paid[0]]['status'],'unknown')

    def test_same_output_directory_keeps_one_process_writer(self):
        with scratch()as root,provider(Path(root)/'worker',cpus=2)as(_,worker):
            with Pool([worker])as pool,center(pool,Path(root)/'center')as(_,client):
                entered=threading.Event();release=threading.Event();output=Path(root)/'first'
                def publish(receipt):
                    entered.set()
                    if not release.wait(10):raise RuntimeError('Publication not released')
                with ThreadPoolExecutor(1)as callers:
                    first=callers.submit(client.execute,{'one':task(1)},project='process-test',
                        output_dir=output,publish=publish,ready_tasks=2)
                    try:
                        self.assertTrue(entered.wait(4))
                        with self.child(root,client,output)as(child,_):
                            _,err=child.communicate(timeout=5)
                            self.assertNotEqual(child.returncode,0);self.assertIn('Another pipeline',err)
                    finally:release.set()
                    self.assertEqual(first.result(timeout=6)[0]['status'],'succeeded')
