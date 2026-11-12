"""A replaced center reconnects remote computations and queued original IDs."""
import json,socket,subprocess,sys,time,unittest
from pathlib import Path
from remote_compute import Client,python_task
from test_compute import provider,python,scratch,until
from service_fixture import command


SOURCE='''import time
from pathlib import Path
from remote_compute import map_items
def work(x):
 p=Path('start-'+str(x));p.write_text(str(int(p.read_text())+1 if p.exists()else 1))
 while not Path('release').exists():time.sleep(.02)
 return x
def entry():return map_items('hold:work',range(8),backend='thread')
'''


class CenterProcessRecoveryTests(unittest.TestCase):
    def test_replacing_center_preserves_running_body_and_queued_request(self):
        with scratch()as folder,provider(Path(folder)/'worker',cpus=4)as(_,worker):
            root=Path(folder);config=root/'worker.json';config.write_text('{}')
            nodes=root/'nodes.json';nodes.write_text(json.dumps(dict(nodes=[dict(name='remote',url=worker.url)])))
            with socket.socket()as sock:sock.bind(('127.0.0.1',0));port=sock.getsockname()[1]
            url='http://127.0.0.1:'+str(port);client=Client(url,timeout=.5)
            argv=command('--mode','scheduler','--nodes',str(nodes),'--config',str(config),'--root',str(root/'center'),'--port',str(port),'--host','127.0.0.1','--headless','--no-dashboard')
            processes=[]
            with (root/'native.log').open('w+',encoding='utf8')as log:
                def start():
                    process=subprocess.Popen(argv,stdout=log,stderr=subprocess.STDOUT,cwd=Path(__file__).resolve().parents[1]);processes.append(process)
                    def ready():
                        if process.poll()is not None:
                            log.flush();log.seek(0);raise RuntimeError(log.read())
                        try:return client.health()['role']=='scheduler'
                        except Exception:return False
                    until(ready,seconds=12)
                    return process
                try:
                    first=start()
                    a=client.submit(python_task('hold:entry',max_cpus=4,memory_mb=128,project='A',outputs=['start-*']),files={'hold.py':SOURCE})['id']
                    until(lambda:client.status(a)['status']=='running'and worker.status(a).get('active_cpus')==4,seconds=8)
                    b=client.submit(python('print("queued-original")',project='B'))['id']
                    until(lambda:client.status(b)['status']=='queued')
                    before=worker.status(a)
                    first.kill();first.wait(timeout=5)
                    self.assertEqual(worker.status(a)['status'],'running')
                    second=start()
                    self.assertNotEqual(first.pid,second.pid)
                    until(lambda:client.status(a)['status']=='running',seconds=8)
                    after=worker.status(a)
                    self.assertEqual((after['id'],after['started'],after['request_sha256']),(before['id'],before['started'],before['request_sha256']))
                    client.write_job_file(a,'release','done')
                    client.wait(a,wait_timeout=12);client.wait(b,wait_timeout=12)
                    result=client.fetch(a);queued=client.fetch(b)
                    self.assertEqual(result['status'],'succeeded',result.get('stderr'))
                    self.assertEqual([v['value']for v in json.loads(result['files']['.dco-results.json'])['items']][0],list(range(8)))
                    self.assertEqual([result['files']['start-'+str(i)]for i in range(8)],[b'1']*8)
                    self.assertEqual((queued['id'],queued['status'],queued['stdout'].strip()),(b,'succeeded','queued-original'))
                finally:
                    for process in processes:
                        if process.poll()is None:process.kill()
                        process.wait(timeout=5)
