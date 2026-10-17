"""A crashed center reattaches the original worker process and grants."""
import json
from pathlib import Path
import subprocess
import sys
import unittest

from remote_compute import Client
from test_compute import provider,python,scratch,until


class RecoveryTests(unittest.TestCase):
    def test_center_crash_keeps_original_id_process_and_resource_resize(self):
        with scratch() as temporary,provider(Path(temporary)/'worker',cpus=2) as (worker,node):
            root=Path(temporary);stop=root/'stop'
            code='''import json,sys,time
from pathlib import Path
from remote_compute import Pool
from remote_compute.coordinator import Coordinator
from remote_compute.worker import Server
import threading
root=Path(sys.argv[2])
pool=Pool([dict(name='original',url=sys.argv[1])],refresh=.1)
state=Coordinator(pool,root/'center')
server=Server(('127.0.0.1',0),state)
threading.Thread(target=server.serve_forever,daemon=True).start()
print(json.dumps(dict(url='http://127.0.0.1:'+str(server.server_port))),flush=True)
try:
 while not (root/'stop').exists():time.sleep(.02)
finally:
 state.close();server.shutdown();server.server_close();pool.close()
'''
            def start():
                process=subprocess.Popen([sys.executable,'-B','-c',code,node.url,str(root)],
                    stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,encoding='utf-8')
                return process,Client(json.loads(process.stdout.readline())['url'])
            process,client=start()
            try:
                task=python("import os,time;from pathlib import Path\nPath('pid.txt').write_text(str(os.getpid()))\nwhile not Path('release').exists():time.sleep(.02)\nprint('once')",
                            cpus=0,max_cpus=2,memory_mb=128,outputs=['pid.txt'],project='recovery')
                accepted=client.submit(task);identity=accepted['id']
                until(lambda:identity in worker.jobs and worker.get(identity)['status']=='running')
                until(lambda:(worker.jobs[identity]['directory']/'input/pid.txt').exists())
                original_pid=client.read_job_file(identity,'pid.txt')
                process.kill();process.communicate()
                process,client=start()
                until(lambda:client.status(identity)['status']=='running')
                self.assertEqual(client.read_job_file(identity,'pid.txt'),original_pid)
                health=client.health()
                self.assertEqual((health['used_cpus'],health['projects']['recovery']['used']),(2,2))
                self.assertEqual(health['nodes'][0]['external_cpus'],0)
                grant=client.resize(identity,1,memory_mb=64)
                self.assertEqual((grant['cpus'],grant['memory_mb']),(1,64))
                self.assertEqual(list(worker.jobs),[identity])
                client.write_job_file(identity,'release','go');client.wait(identity)
                result=client.fetch(identity)
                self.assertEqual(result['stdout'].strip(),'once')
                self.assertEqual(result['request_sha256'],accepted['request_sha256'])
                self.assertEqual(client.health()['used_cpus'],0)
            finally:
                stop.write_text('stop')
                try:process.communicate(timeout=10)
                except subprocess.TimeoutExpired:process.kill();process.communicate()
