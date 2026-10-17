"""One legacy service process keeps its running child through the resource fix."""
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
import unittest
import zipfile

from remote_compute import Client, RemoteError, command
from test_compute import scratch, until


@unittest.skipUnless(os.name=='nt' and hasattr(sys,'remote_exec') and shutil.which('git'),
                     'Windows CPython 3.14 and the Git baseline are required')
class LivePatchTests(unittest.TestCase):
    def test_existing_job_and_process_survive_the_resource_patch(self):
        root=Path(__file__).resolve().parents[1]
        archive=subprocess.run(['git','archive','--format=zip','f3d2a0b','remote_compute'],
                               cwd=root,capture_output=True)
        if archive.returncode:self.skipTest('The 0.7.11 baseline is absent in this checkout')
        with scratch() as temporary:
            scope=Path(temporary);legacy=scope/'legacy';legacy.mkdir()
            with zipfile.ZipFile(io.BytesIO(archive.stdout)) as files:files.extractall(legacy)
            host=scope/'host.py'
            host.write_text('''import sys,time,json,threading
from pathlib import Path
sys.path.insert(0,sys.argv[1])
import remote_compute.worker as w
from remote_compute import Pool
from remote_compute.local import LocalClient
from remote_compute.coordinator import Coordinator
w.cpu_benchmark=lambda cpus:dict(name='test',score=1000,unit='kiter/s',threads=cpus)
root=Path(sys.argv[2]); worker=w.Worker(root/'worker',cpus=4,memory_mb=8192)
pool=Pool([LocalClient(worker)]); center=Coordinator(pool,root/'center')
server=w.Server(('127.0.0.1',0),center)
thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
print(json.dumps(dict(pid=__import__('os').getpid(),url='http://127.0.0.1:'+str(server.server_port))),flush=True)
try:
 while not (root/'stop').exists():time.sleep(.02)
finally:
 server.shutdown();server.server_close();center.close();pool.close();worker.close()
''',encoding='utf-8')
            process=subprocess.Popen([sys.executable,'-B',str(host),str(legacy),str(scope)],
                stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,encoding='utf-8')
            try:
                address=json.loads(process.stdout.readline())
                client=Client(address['url'])
                self.assertEqual(client.health()['version'],'0.7.11')
                code="from pathlib import Path;import os,time\nPath('pid.txt').write_text(str(os.getpid()))\nwhile not Path('release').exists():time.sleep(.02)\nprint('completed once')"
                accepted=client.submit(command(['${tool:python}','-u','-c',code],cpus=2,memory_mb=4096,outputs=['pid.txt']))
                identity=accepted['id']
                until(lambda:client.status(identity)['status']=='running')
                def ready():
                    try:return bool(client.read_job_file(identity,'pid.txt'))
                    except RemoteError as error:
                        if getattr(error,'status_code',None)!=404:raise
                        return False
                until(ready)
                child_pid=client.read_job_file(identity,'pid.txt')
                receipt=scope/'patched.json'; script=scope/'patch.py'
                script.write_text("import importlib.util,json\nfrom pathlib import Path\n"+
                    "spec=importlib.util.spec_from_file_location('resource_fix',"+repr(str(root/'remote_compute/live_patch.py'))+")\n"+
                    "module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)\n"+
                    "value=module.apply(source_root="+repr(str(root))+",expected_root="+repr(str(legacy))+",expected_pid="+str(address['pid'])+")\n"+
                    "Path("+repr(str(receipt))+").write_text(json.dumps(value))\n",encoding='utf-8')
                sys.remote_exec(address['pid'],str(script))
                until(receipt.exists,seconds=10)
                applied=json.loads(receipt.read_text())
                self.assertTrue(applied['jobs_unchanged'])
                self.assertEqual((client.health()['pid'],client.health()['version']),(address['pid'],'0.7.13'))
                grant=client.resize(identity,3,memory_mb=256)
                self.assertEqual((grant['cpus'],grant['memory_mb']),(3,256))
                self.assertEqual(client.read_job_file(identity,'pid.txt'),child_pid)
                extra=client.run(command(['${tool:python}','-c','print(42)'],cpus=0,max_cpus=1))
                self.assertEqual(extra['cpus'],1)
                client.write_job_file(identity,'release','go')
                client.wait(identity)
                result=client.fetch(identity)
                self.assertEqual(result['stdout'].strip(),'completed once')
                self.assertEqual(result['request_sha256'],accepted['request_sha256'])
                until(lambda:client.health()['used_cpus']==0)
            finally:
                (scope/'stop').write_text('stop')
                process.wait(15)
                process.stdout.close()
