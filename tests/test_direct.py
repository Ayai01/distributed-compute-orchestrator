"""Direct TLS, one-way resource requests and a detachable worker console."""
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import threading
import unittest
from unittest.mock import patch

from remote_compute import Client,Pool
from remote_compute.worker import Handler,Server
from test_center import center
from test_compute import provider,python,scratch,until


class DirectTests(unittest.TestCase):
    def test_incomplete_tls_peer_does_not_block_new_connections_or_running_task(self):
        openssl=shutil.which('openssl')
        if not openssl and os.name=='nt':openssl='D:/Program Files/Git/usr/bin/openssl.exe'
        if not openssl or not Path(openssl).is_file():self.skipTest('openssl generates a temporary test certificate')
        with scratch() as temporary,provider(Path(temporary)/'worker',cpus=1) as (worker,_):
            root=Path(temporary);cert=root/'node.crt';key=root/'node.key'
            subprocess.run([openssl,'req','-x509','-newkey','rsa:2048','-nodes','-days','1',
                '-subj','/CN=localhost','-addext','subjectAltName=IP:127.0.0.1',
                '-keyout',str(key),'-out',str(cert)],check=True,capture_output=True)
            server=Server(('127.0.0.1',0),worker,certfile=str(cert),keyfile=str(key))
            entered=threading.Event();accept=server.get_request
            def accepting():
                entered.set()
                return accept()
            server.get_request=accepting
            thread=threading.Thread(target=server.serve_forever,kwargs={'poll_interval':.02},daemon=True)
            thread.start();idle=None
            def fresh():return Client('https://127.0.0.1:'+str(server.server_port),ca_file=str(cert),timeout=1)
            try:
                client=fresh()
                identity=client.submit(python("import time;from pathlib import Path\nwhile not Path('release').exists():time.sleep(.02)"))['id']
                until(lambda:worker.get(identity)['status']=='running')
                entered.clear()
                with patch.object(Handler,'idle_timeout',.5),patch.object(Server,'handle_error') as errors:
                    idle=socket.create_connection(server.server_address,timeout=2)
                    self.assertTrue(entered.wait(2))
                    self.assertEqual(fresh().health(brief=True)['role'],'worker')
                    idle.settimeout(2)
                    self.assertEqual(idle.recv(1),b'')
                    idle.close();idle=None
                    with socket.create_connection(server.server_address,timeout=2) as invalid:
                        invalid.sendall(b'GET /health HTTP/1.0\r\n\r\n')
                        self.assertEqual(invalid.recv(1),b'')
                    self.assertEqual(fresh().status(identity,brief=True)['status'],'running')
                    client.write_job_file(identity,'release','done')
                    self.assertEqual(client.wait(identity)['status'],'succeeded')
                    client.fetch(identity)
                    errors.assert_not_called()
            finally:
                if idle:idle.close()
                server.shutdown();thread.join();server.server_close()

    def test_https_and_local_resource_requests_need_no_reverse_connection(self):
        openssl=shutil.which('openssl')
        if not openssl and os.name=='nt':openssl='D:/Program Files/Git/usr/bin/openssl.exe'
        if not openssl or not Path(openssl).is_file():self.skipTest('openssl generates a temporary test certificate')
        with scratch() as temporary,provider(Path(temporary)/'worker',cpus=2) as (worker,_):
            root=Path(temporary);cert=root/'node.crt';key=root/'node.key'
            subprocess.run([openssl,'req','-x509','-newkey','rsa:2048','-nodes','-days','1',
                '-subj','/CN=localhost','-addext','subjectAltName=IP:127.0.0.1',
                '-keyout',str(key),'-out',str(cert)],check=True,capture_output=True)
            server=Server(('127.0.0.1',0),worker,certfile=str(cert),keyfile=str(key))
            thread=threading.Thread(target=server.serve_forever,kwargs={'poll_interval':.05},daemon=True);thread.start()
            try:
                node=Client('https://127.0.0.1:'+str(server.server_port),ca_file=str(cert))
                self.assertEqual(node.health()['role'],'worker')
                with Pool([node],refresh=.1) as pool,center(pool,root/'center') as (state,client):
                    state.callback_address=('unreachable.invalid',26133)
                    code=("import sys,os,json;sys.path.insert(0,os.environ['REMOTE_COMPUTE_HOME']);"
                          "from remote_compute import Client;"
                          "c=Client(os.environ['REMOTE_COMPUTE_URL'],os.environ['REMOTE_COMPUTE_TOKEN']);"
                          "a=c.admission('default',job_id=os.environ['REMOTE_COMPUTE_JOB_ID']);assert a['recommended_cpus']==2;"
                          "print(json.dumps(c.resize(os.environ['REMOTE_COMPUTE_JOB_ID'],1,memory_mb=64)))")
                    result=client.run(python(code,cpus=2,memory_mb=128))
                    self.assertEqual(result['status'],'succeeded',result.get('stderr'))
                    grant=json.loads(result['stdout'])
                    self.assertEqual((grant['cpus'],grant['memory_mb']),(1,64))
                    self.assertEqual(client.health()['used_cpus'],0)
            finally:
                server.shutdown();thread.join();server.server_close()

    def test_worker_console_exit_keeps_the_service_and_existing_task_alive(self):
        with scratch() as temporary,provider(Path(temporary)/'worker') as (worker,client):
            identity=client.submit(python("import time;from pathlib import Path\nwhile not Path('release').exists():time.sleep(.02)"))['id']
            until(lambda:worker.get(identity)['status']=='running')
            from service_fixture import command
            result=subprocess.run(command('--mode','worker','--url',client.url),
                input='status\nquit\n',capture_output=True,text=True,encoding='utf-8',timeout=10)
            self.assertEqual(result.returncode,0,result.stderr)
            self.assertIn('算力>',result.stdout)
            self.assertEqual(worker.get(identity)['status'],'running')
            client.write_job_file(identity,'release','done');client.wait(identity);client.fetch(identity)
