"""Independent client processes share short project state writes and credits."""
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import json,subprocess,sys,threading,time,unittest
from unittest.mock import patch

from remote_compute import Client,Pool,command
from test_center import center
from test_compute import scratch,provider


def task(number):
    return dict(task=command(['${tool:python}','-c','print('+str(number)+')'],max_cpus=1))


CHILD=r'''import json,sys
from pathlib import Path
from remote_compute import Client
from remote_compute.bundle import atomic_json
p=json.loads(Path(sys.argv[1]).read_bytes());client=Client(p['url'],timeout=5,state_dir=p['state_dir'])
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


class RegistryTransactionTests(unittest.TestCase):
    def test_default_client_state_root_is_independent_of_cwd(self):
        from remote_compute.bundle import native_path
        with scratch()as folder:
            with patch('remote_compute.client.Path.cwd',return_value=Path(folder)):
                first=Client()
            with patch('remote_compute.client.Path.cwd',return_value=Path(folder)/'other'):
                second=Client()
            self.assertEqual(first.state_dir,second.state_dir)
            self.assertEqual(first.state_dir,native_path(Path(__file__).resolve().parent.parent/'.local/dco-client',force=True))
            self.assertEqual(Client(state_dir=Path(folder)/'client').state_dir,native_path(Path(folder)/'client',force=True))

    def test_registry_reader_and_replacement_share_the_native_lock(self):
        from remote_compute.bundle import atomic_json
        from remote_compute.pipeline_control import _Controller
        with scratch()as folder:
            controller=_Controller(Path(folder),'registry-test')
            atomic_json(controller.registry,dict(project='registry-test',ready_tasks=4,directories={},revision=0))
            opened=threading.Event();release=threading.Event();observed=[]
            original=Path.read_bytes
            def read_bytes(path):
                if path==controller.registry and threading.current_thread()is reader:
                    with path.open('rb')as stream:
                        opened.set()
                        if not release.wait(5):raise RuntimeError('Reader not released')
                        return stream.read()
                return original(path)
            def write():
                with controller.transaction()as value:
                    value['revision']+=1;controller.save(value)
            reader=threading.Thread(target=lambda:observed.append(controller.read()))
            with patch.object(Path,'read_bytes',read_bytes),ThreadPoolExecutor(1)as writers:
                reader.start()
                try:
                    self.assertTrue(opened.wait(3))
                    writing=writers.submit(write)
                    # Longer than atomic_json's full Windows replacement
                    # retry window; an unlocked reader makes that write fail.
                    time.sleep(.8)
                    self.assertFalse(writing.done())
                finally:release.set();reader.join(timeout=3)
                writing.result(timeout=3)
            self.assertEqual(observed[0]['revision'],0)
            self.assertEqual(controller.read()['revision'],1)


class ProcessPipelineTests(unittest.TestCase):
    @contextmanager
    def child(self,root,client,output,*,mode='',window=2,plan=None,cwd=None):
        output=Path(output);request=Path(root)/('request-'+output.name+'.json')
        marker=Path(root)/('marker-'+output.name+'.json')
        request.write_text(json.dumps(dict(url=client.url,output=str(output),mode=mode,
            window=window,plan=plan or {'one':task(1)},marker=str(marker),state_dir=str(client.state_dir))),encoding='utf8')
        script='import sys;sys.path.insert(0,'+repr(str(Path(__file__).resolve().parent.parent))+');'+CHILD
        with subprocess.Popen([sys.executable,'-B','-c',script,str(request)],stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,cwd=cwd)as child:
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
                registry=client.state_dir/key/'executions.json'
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
                registry=client.state_dir/key/'executions.json'
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

    def test_different_cwds_share_pending_credit_and_reject_different_windows(self):
        with scratch()as root,provider(Path(root)/'worker',cpus=2)as(_,worker):
            with Pool([worker])as pool,center(pool,Path(root)/'center')as(_,client):
                entered=threading.Event();release=threading.Event()
                other=Path(root)/'other-cwd';other.mkdir()
                def publish(receipt):
                    entered.set()
                    if not release.wait(10):raise RuntimeError('Publication not released')
                with ThreadPoolExecutor(1)as callers:
                    first=callers.submit(client.execute,{'one':task(1)},project='process-test',
                        output_dir=Path(root)/'first',publish=publish,ready_tasks=1)
                    try:
                        self.assertTrue(entered.wait(4))
                        with self.child(root,client,Path(root)/'conflict',window=2,cwd=other)as(child,_):
                            _,err=child.communicate(timeout=5)
                            self.assertNotEqual(child.returncode,0);self.assertIn('same ready_tasks',err)
                        with self.child(root,client,Path(root)/'second',mode='prepare',window=1,cwd=other)as(child,marker):
                            time.sleep(.2);self.assertFalse(marker.exists());self.assertIsNone(child.poll())
                            release.set();self.assertEqual(self.marker(marker,child)['phase'],'prepare')
                            out,err=child.communicate('\n',timeout=6)
                            self.assertEqual(child.returncode,0,err)
                            self.assertEqual(json.loads(out)[0]['stdout'].strip(),'1')
                    finally:release.set()
                    self.assertEqual(first.result(timeout=6)[0]['stdout'].strip(),'1')

    def test_cwd_index_import_keeps_unpublished_original_and_shared_credit(self):
        from hashlib import sha256
        from remote_compute import ExecutionFailure
        with scratch()as root,provider(Path(root)/'worker',cpus=1)as(_,worker):
            with Pool([worker])as pool,center(pool,Path(root)/'center')as(_,client):
                legacy_cwd=Path(root)/'legacy';legacy_cwd.mkdir()
                legacy=Client(client.url,state_dir=legacy_cwd/'.local/dco-client')
                output=Path(root)/'first';plan={'one':task(1)};original=[]
                def publish(receipt):original.append(receipt['id']);raise RuntimeError('Interrupted publication')
                with self.assertRaises(ExecutionFailure):
                    legacy.execute(plan,project='process-test',output_dir=output,publish=publish,ready_tasks=1)
                with patch('remote_compute.pipeline_control.Path.cwd',return_value=legacy_cwd):
                    with patch.object(client,'submit_bundle')as post:
                        resumed=client.execute(plan,project='process-test',output_dir=output,ready_tasks=1)
                    post.assert_not_called()
                self.assertEqual(resumed[0]['id'],original[0])
                key=sha256((client.url+'\0process-test').encode()).hexdigest()
                registry=json.loads((client.state_dir/key/'executions.json').read_bytes())
                self.assertEqual(len(registry['imported_indexes']),1)
                self.assertFalse(registry['directories'])
