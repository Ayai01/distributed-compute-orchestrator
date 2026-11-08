"""A centre process replacement reattaches live workers and queued originals."""
import json,subprocess,sys,time,unittest,uuid
from pathlib import Path
from remote_compute import Client,RemoteError,python_map,command
from test_compute import provider,scratch,until

CENTRE='''import json,sys,threading
from pathlib import Path
from remote_compute import Pool
from remote_compute.coordinator import Coordinator
from remote_compute.worker import Server
root=Path(sys.argv[1]);config=json.loads((root/'connection.json').read_bytes())
pool=Pool([dict(name='provider',url=config['url'])])
state=Coordinator(pool,root/'state',operator_token='test-operator')
server=Server(('127.0.0.1',config.get('port',0)),state)
(root/'ready.json').write_text(json.dumps(dict(port=server.server_port,pid=__import__('os').getpid())))
server.serve_forever(poll_interval=.05)
'''
BUSINESS='''from pathlib import Path
import time
def one(item):
 marker=Path('once-'+str(item))
 if marker.exists():raise RuntimeError('Original input executed twice')
 marker.write_text('started')
 while not Path('release').exists():time.sleep(.01)
 return item
'''

class CentreHandoverTests(unittest.TestCase):
    def test_remote_calculation_and_queued_original_survive_centre_process_replacement(self):
        with scratch()as folder,provider(Path(folder)/'worker',cpus=2)as(_,remote):
            root=Path(folder)/'centre';root.mkdir();connection=dict(url=remote.url)
            (root/'connection.json').write_text(json.dumps(connection));process=None;client=None;ids=[]
            def start(previous=None):
                child=subprocess.Popen([sys.executable,'-B','-c',CENTRE,str(root)],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
                def ready():
                    if child.poll()is not None:raise RuntimeError(child.stderr.read().decode())
                    try:info=json.loads((root/'ready.json').read_bytes())
                    except (FileNotFoundError,ValueError):return False
                    return info if info['pid']==child.pid else False
                try:
                    until(ready,seconds=12);info=json.loads((root/'ready.json').read_bytes())
                    return child,Client('http://127.0.0.1:'+str(info['port']),operator=True,operator_token='test-operator',timeout=2),info
                except BaseException:
                    if child.poll()is None:child.kill()
                    child.communicate(timeout=5)
                    raise
            try:
                process,client,first=start();connection['port']=first['port'];(root/'connection.json').write_text(json.dumps(connection))
                identity=client.submit(python_map('business:one',range(4),max_cpus=2,memory_mb=128,retries=0,timeout=50),files={'business.py':BUSINESS})['id'];ids.append(identity)
                def started():
                    try:return client.read_job_file(identity,'once-0')==b'started'
                    except RemoteError as error:
                        if error.status_code==404:return False
                        raise
                until(started,seconds=10)
                queued=client.submit(command(['${tool:python}','-c','print("queued-original")'],max_cpus=1,memory_mb=128,retries=0,timeout=20))['id'];ids.append(queued)
                before={i:client.status(i)['request_sha256']for i in ids};worker_pid=remote.health()['pid']
                process.kill();process.communicate(timeout=5)
                process,client,second=start(first['pid'])
                self.assertNotEqual(first['pid'],second['pid'])
                until(lambda:client.status(identity)['status']=='running',seconds=10)
                self.assertEqual(remote.health()['pid'],worker_pid)
                client.write_job_file(identity,'release','yes')
                for i in ids:
                    result=client.wait(i,wait_timeout=20)
                    self.assertEqual(result['status'],'succeeded',result)
                    self.assertEqual(result['request_sha256'],before[i]);self.assertEqual(result['attempts'],[])
                    client.fetch(i,root/i,delete=False);client.delete(i)
                data=json.loads((root/identity/'files/.dco-results.json').read_bytes())
                self.assertEqual([row['value']for row in data['items']],list(range(4)))
            finally:
                if process is not None and process.poll()is None:
                    if client is not None:
                        for i in ids:
                            try:
                                if client.statuses([i])[i]['status']not in ('unknown','succeeded','failed','cancelled','interrupted'):
                                    client.cancel(i);client.wait(i,wait_timeout=10)
                            except RemoteError:pass
                    process.kill();process.communicate(timeout=5)


if __name__=='__main__':unittest.main()
