"""Real HTTP regressions for slow and interrupted result transfers."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import hashlib
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
import json
from pathlib import Path
import socket
import threading
import time
import unittest
from unittest.mock import patch

from remote_compute import Client, Pool
from remote_compute.bundle import pack
from remote_compute.client import PoolBusy, _ConnectionPool
from remote_compute.worker import Handler
from test_center import center
from test_compute import provider, python, scratch, until


@contextmanager
def slow_upload_server(*, interrupt=False):
    received=[]
    class Receiver(BaseHTTPRequestHandler):
        def setup(self):
            super().setup()
            self.connection.setsockopt(socket.SOL_SOCKET,socket.SO_RCVBUF,16384)
        def log_message(self,*args):
            pass
        def do_GET(self):
            reply=json.dumps(dict(exists=False)).encode()
            self.send_response(200)
            self.send_header('Content-Length',str(len(reply)))
            self.end_headers()
            self.wfile.write(reply)
        def do_POST(self):
            length=int(self.headers['Content-Length'])
            content=bytearray()
            while len(content)<length:
                chunk=self.rfile.read(min(32768,length-len(content)))
                if not chunk:break
                content.extend(chunk)
                if interrupt and not received and len(content)>=65536:
                    received.append((self.path,bytes(content),self.headers.get('Transfer-Encoding')))
                    self.connection.shutdown(socket.SHUT_RDWR)
                    self.close_connection=True
                    return
                time.sleep(.02)
            received.append((self.path,bytes(content),self.headers.get('Transfer-Encoding')))
            reply=json.dumps(dict(bytes=len(content),sha256=hashlib.sha256(content).hexdigest())).encode()
            try:
                self.send_response(200)
                self.send_header('Content-Length',str(len(reply)))
                self.end_headers()
                self.wfile.write(reply)
            except (BrokenPipeError,ConnectionResetError,ConnectionAbortedError):
                pass
    server=ThreadingHTTPServer(('127.0.0.1',0),Receiver)
    server.daemon_threads=False
    thread=threading.Thread(target=server.serve_forever,kwargs=dict(poll_interval=.02))
    thread.start()
    original=http.client.HTTPConnection.connect
    def connect(connection):
        original(connection)
        connection.sock.setsockopt(socket.SOL_SOCKET,socket.SO_SNDBUF,16384)
    try:
        with patch.object(http.client.HTTPConnection,'connect',connect):
            yield Client('http://127.0.0.1:'+str(server.server_port),timeout=.3),received
    finally:
        server.shutdown();thread.join();server.server_close()


class TransportTests(unittest.TestCase):
    def test_slow_binary_upload_keeps_progress_past_the_socket_timeout(self):
        payload=b'asset-content'*160000
        identity=hashlib.sha256(payload).hexdigest()
        with slow_upload_server() as (client,received):
            started=time.monotonic()
            result=client.request('/assets/'+identity,payload)
            self.assertGreater(time.monotonic()-started,client.timeout)
            self.assertEqual(client.timeout,.3)
            self.assertEqual(result,dict(bytes=len(payload),sha256=identity))
            self.assertEqual(received,[('/assets/'+identity,payload,None)])

    def test_interrupted_binary_upload_restarts_same_hash_from_first_byte(self):
        payload=b'asset-content'*160000
        identity=hashlib.sha256(payload).hexdigest()
        with slow_upload_server(interrupt=True) as (client,received):
            result=client.request('/assets/'+identity,payload)
            self.assertEqual(result,dict(bytes=len(payload),sha256=identity))
            self.assertEqual(len(received),2)
            self.assertEqual(received[0],('/assets/'+identity,payload[:65536],None))
            self.assertEqual(received[1],('/assets/'+identity,payload,None))

    def test_lost_asset_reply_checks_publication_without_reuploading_the_body(self):
        payload=pack({'model.npz':bytes(range(256))*8192})
        identity=hashlib.sha256(payload).hexdigest()
        original=Handler._reply
        requests=[]
        def reply(handler,value,status=200):
            if handler.path=='/assets/'+identity:
                requests.append((handler.command,int(handler.headers.get('Content-Length',0))))
                if handler.command=='POST' and len(requests)==1:
                    self.assertTrue(value['exists'])
                    handler.close_connection=True
                    return
            return original(handler,value,status)
        with scratch() as root,provider(Path(root)/'worker') as (worker,client):
            with patch.object(Handler,'_reply',reply):
                result=client.request('/assets/'+identity,payload)
            self.assertEqual(result,dict(id=identity,exists=True))
            self.assertEqual(requests,[('POST',len(payload)),('GET',0)])
            self.assertEqual((worker.root/'assets'/identity/'model.npz').read_bytes(),bytes(range(256))*8192)

    def test_connection_slots_are_fifo_and_timeout_never_sends_a_request(self):
        pool=_ConnectionPool()
        for _ in range(8):self.assertTrue(pool.acquire(0))
        order=[]
        def waiter(index):
            self.assertTrue(pool.acquire(2))
            order.append(index)
            pool.release()
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures=[]
            for index in range(3):
                futures.append(executor.submit(waiter,index))
                until(lambda:len(pool.waiting)==index+1)
            client=Client(timeout=.01,pool_timeout=.01);client._transport=pool
            with patch.object(client,'_request') as request:
                with self.assertRaises(PoolBusy):client.health()
                request.assert_not_called()
            pool.release()
            for future in futures:future.result()
        self.assertEqual(order,[0,1,2])
        self.assertEqual(pool.available,1)
        self.assertFalse(pool.waiting)
        for _ in range(7):pool.release()
        self.assertEqual(pool.available,8)

    def test_interrupted_binary_get_resumes_and_changed_or_legacy_payload_restarts(self):
        original=Handler._reply
        for mode in ('resume','changed','legacy'):
            with self.subTest(mode=mode),scratch() as root,provider(Path(root)/'worker') as (worker,client):
                identity=client.submit(python("import os; from pathlib import Path; Path('out.bin').write_bytes(os.urandom(300000))",outputs=['out.bin']))['id']
                client.wait(identity)
                expected=(worker.jobs[identity]['directory']/'input'/'out.bin').read_bytes()
                calls=[]
                def reply(handler,value,status=200):
                    if isinstance(value,bytes):
                        calls.append(handler.headers.get('Range'))
                        if len(calls)==1:
                            handler.send_response(200)
                            handler.send_header('Content-Length',str(len(value)))
                            if mode!='legacy':
                                handler.send_header('Accept-Ranges','bytes')
                                tag=hashlib.sha256(value).hexdigest()+('-old' if mode=='changed' else '')
                                handler.send_header('ETag','"'+tag+'"')
                            handler.end_headers()
                            handler.wfile.write(value[:40000])
                            handler.wfile.flush()
                            handler.close_connection=True
                            return
                    return original(handler,value,status)
                with patch.object(Handler,'_reply',reply):
                    result=client.fetch(identity)
                self.assertEqual(result['files']['out.bin'],expected)
                self.assertEqual(calls,[None,'bytes=40000-' if mode!='legacy' else None])
                self.assertEqual(worker.jobs,{})

    def test_slow_transfer_keeps_progress_past_the_socket_timeout(self):
        original=Handler._reply
        with scratch() as root,provider(Path(root)/'worker') as (_,client):
            identity=client.submit(python("import os; from pathlib import Path; Path('out.bin').write_bytes(os.urandom(100000))",outputs=['out.bin']))['id']
            client.wait(identity)
            def reply(handler,value,status=200):
                if not isinstance(value,bytes):return original(handler,value,status)
                handler.send_response(200)
                handler.send_header('Content-Length',str(len(value)))
                handler.end_headers()
                for start in range(0,len(value),10000):
                    handler.wfile.write(value[start:start+10000]);handler.wfile.flush()
                    time.sleep(.02)
            client.timeout=.1
            started=time.monotonic()
            with patch.object(Handler,'_reply',reply):
                raw=client.request('/jobs/'+identity+'/artifacts',binary=True)
            self.assertGreater(time.monotonic()-started,client.timeout)
            import zipfile
            with zipfile.ZipFile(io.BytesIO(raw)) as archive:
                self.assertEqual(len(archive.read('files/out.bin')),100000)
            client.delete(identity)

    def test_fetch_waits_for_remote_headers_without_delaying_status(self):
        with scratch() as root,provider(Path(root)/'worker') as (_,client):
            identity=client.submit(python('pass'))['id'];client.wait(identity)
            entered,release=threading.Event(),threading.Event()
            original=Handler._reply;downloads=[]
            def delayed(handler,value,status=200):
                if isinstance(value,bytes):
                    downloads.append(handler.path);entered.set();release.wait(2)
                return original(handler,value,status)
            client.timeout=.05
            with patch.object(Handler,'_reply',delayed),ThreadPoolExecutor(max_workers=1) as executor:
                result=executor.submit(client.fetch,identity)
                try:
                    self.assertTrue(entered.wait(1))
                    time.sleep(.1)  # Header wait exceeds the status timeout.
                    self.assertEqual(client.health(brief=True)['role'],'worker')
                    self.assertFalse(result.done())
                finally:
                    release.set()
                self.assertEqual(result.result(timeout=2)['status'],'succeeded')
            self.assertEqual(len(downloads),1)

    def test_center_download_uses_long_header_timeout_and_caches_original_zip(self):
        with scratch() as root,provider(Path(root)/'worker') as (worker,node):
            with Pool([node]) as pool,center(pool,Path(root)/'center') as (state,client):
                identity=client.submit(python("print('executed once')"))['id'];client.wait(identity)
                backend=pool.nodes[0].client
                backend.timeout=.05
                control_pool=backend._transport
                entered,release=threading.Event(),threading.Event()
                original=Handler._reply;downloads=[]
                def delayed(handler,value,status=200):
                    if handler.server.worker is worker and handler.path=='/jobs/'+identity+'/artifacts':
                        downloads.append(handler.path);entered.set();release.wait(2)
                    return original(handler,value,status)
                with patch.object(Handler,'_reply',delayed),ThreadPoolExecutor(max_workers=1) as transfers:
                    pending=transfers.submit(client.fetch,identity,delete=False)
                    try:
                        self.assertTrue(entered.wait(1))
                        time.sleep(.1)
                        self.assertEqual(client.health(brief=True)['role'],'scheduler')
                        self.assertEqual(backend.health(brief=True)['role'],'worker')
                        self.assertFalse(pending.done())
                    finally:
                        release.set()
                    result=pending.result(timeout=3)
                    cached=(state.jobs[identity]['directory']/'worker-artifacts.zip').read_bytes()
                    again=client.fetch(identity,delete=False)
                self.assertEqual(len(downloads),1)
                self.assertEqual(result['stdout'].strip(),'executed once')
                self.assertEqual(again,result)
                self.assertEqual((state.jobs[identity]['directory']/'worker-artifacts.zip').read_bytes(),cached)
                self.assertEqual(list(worker.jobs),[identity])
                self.assertEqual(backend.timeout,.05)
                self.assertIs(backend._transport,control_pool)
                self.assertGreaterEqual(backend._result_client.timeout,120)
                self.assertIsNot(backend._result_client._transport,control_pool)
                client.delete(identity)
                self.assertNotIn(identity,worker.jobs)
                self.assertNotIn(identity,state.jobs)

    def test_idle_keep_alive_is_reclaimed_and_the_same_client_reconnects(self):
        with scratch() as root,provider(Path(root)/'worker') as (_,client),patch.object(Handler,'idle_timeout',.05):
            client.health();previous=client._transport.idle[0].sock
            time.sleep(.12)
            self.assertEqual(client.health()['role'],'worker')
            self.assertIsNot(client._transport.idle[0].sock,previous)

    def test_shared_client_bounds_connections_across_many_caller_threads(self):
        with scratch() as root,provider(Path(root)/'worker') as (_,client):
            with ThreadPoolExecutor(max_workers=30) as callers:
                rows=list(callers.map(lambda _:client.health(brief=True),range(60)))
            self.assertTrue(all(row['role']=='worker' for row in rows))
            self.assertLessEqual(len(client._transport.idle),8)
            self.assertTrue(all(c.sock is not None for c in client._transport.idle))

    def test_128_status_callers_wait_without_changing_original_work_or_socket_timeout(self):
        with scratch() as root,provider(Path(root)/'worker') as (worker,client):
            identity=client.submit(python("print('original retained work')"))['id']
            client.wait(identity)
            client.timeout=.5
            lock=threading.Lock()
            counts=dict(active=0,peak=0,requests=0)
            original=Handler._reply
            def delayed(handler,value,status=200):
                if handler.server.worker is not worker or handler.path!='/jobs/'+identity:
                    return original(handler,value,status)
                with lock:
                    counts['active']+=1
                    counts['requests']+=1
                    counts['peak']=max(counts['peak'],counts['active'])
                try:
                    time.sleep(.12)
                    return original(handler,value,status)
                finally:
                    with lock:counts['active']-=1
            def status(_):
                started=time.monotonic()
                row=client.status(identity,brief=True)
                return row,time.monotonic()-started
            with patch.object(Handler,'_reply',delayed),ThreadPoolExecutor(max_workers=128) as callers:
                results=list(callers.map(status,range(128)))
            self.assertTrue(all(row['id']==identity and row['status']=='succeeded' for row,_ in results))
            self.assertGreater(max(elapsed for _,elapsed in results),client.timeout)
            self.assertEqual(counts['requests'],128)
            self.assertLessEqual(counts['peak'],8)
            self.assertEqual(client.timeout,.5)
            self.assertEqual(client._transport.available,8)
            self.assertFalse(client._transport.waiting)
            self.assertEqual(list(worker.jobs),[identity])
            self.assertEqual(client.fetch(identity)['stdout'].strip(),'original retained work')

    def test_running_status_reuses_fresh_worker_snapshot_and_terminal_status_is_fresh(self):
        with scratch() as root,provider(Path(root)/'worker') as (_,client):
            identity=client.submit(python("import time; from pathlib import Path\nwhile not Path('release').exists():time.sleep(.02)"))['id']
            try:
                client.health()
                with patch.object(client,'request',wraps=client.request) as requests:
                    self.assertIn(client.status(identity,brief=True)['status'],('queued','running'))
                    requests.assert_not_called()
                    client.status(identity)
                    self.assertEqual(requests.call_count,1)
                client.write_job_file(identity,'release','done')
                client._status_sample=(0,{})
                client.wait(identity)
                client.health()
                with patch.object(client,'request',wraps=client.request) as requests:
                    self.assertEqual(client.status(identity,brief=True)['status'],'succeeded')
                    self.assertEqual(requests.call_count,1)
            finally:
                client.cancel(identity);client.wait(identity);client.fetch(identity)

    def test_lost_submission_reply_retries_the_original_id_without_running_twice(self):
        original=Handler._reply
        dropped=[]
        def reply(handler,value,status=200):
            if handler.command=='POST' and handler.path.startswith('/jobs/') and not dropped:
                dropped.append(value['id'])
                handler.close_connection=True
                return
            return original(handler,value,status)
        with scratch() as root,provider(Path(root)/'worker') as (worker,client),patch.object(Handler,'_reply',reply):
            identity=client.submit(python("print('once')"))['id']
            result=client.wait(identity)
            self.assertEqual(list(worker.jobs),[identity])
            self.assertEqual(dropped,[identity])
            self.assertEqual(result['stdout'].strip(),'once')
            client.fetch(identity)

    def test_node_downloads_are_serial_and_same_job_is_cached_until_collection(self):
        with scratch() as root,provider(Path(root)/'worker',cpus=2) as (_,node):
            with Pool([node]) as pool,center(pool,Path(root)/'center') as (state,client):
                identities=[client.submit(python('pass'))['id'] for _ in range(2)]
                for identity in identities:client.wait(identity)
                original=pool.nodes[0].client.artifacts
                mutex=threading.Lock();active=peak=0;calls=[]
                def artifacts(identity,*args,**kwargs):
                    nonlocal active,peak
                    with mutex:
                        active+=1;peak=max(peak,active);calls.append(identity)
                    try:
                        time.sleep(.04)
                        return original(identity,*args,**kwargs)
                    finally:
                        with mutex:active-=1
                with patch.object(pool.nodes[0].client,'artifacts',side_effect=artifacts),ThreadPoolExecutor(max_workers=4) as transfers:
                    results=list(transfers.map(state.artifacts,identities*2))
                self.assertEqual(peak,1)
                self.assertEqual(len(calls),2)
                self.assertEqual(results[0],results[2])
                self.assertEqual(results[1],results[3])
                for identity in identities:
                    directory=state.jobs[identity]['directory']
                    self.assertTrue((directory/'worker-artifacts.zip').exists())
                    client.delete(identity)
                    self.assertFalse(directory.exists())

    def test_disconnected_queued_download_does_not_fetch_or_remove_the_original(self):
        with scratch() as root,provider(Path(root)/'worker') as (worker,node):
            with Pool([node]) as pool,center(pool,Path(root)/'center') as (state,client):
                identity=client.submit(python('pass'))['id'];client.wait(identity)
                with patch.object(pool.nodes[0].client,'artifacts',wraps=pool.nodes[0].client.artifacts) as calls:
                    with self.assertRaises(ConnectionAbortedError):state.artifacts(identity,is_connected=lambda:False)
                    calls.assert_not_called()
                self.assertEqual(worker.get(identity)['status'],'succeeded')
                self.assertFalse((state.jobs[identity]['directory']/'worker-artifacts.zip').exists())
                client.fetch(identity)

    def test_slow_remote_delete_does_not_lock_center_health(self):
        with scratch() as root,provider(Path(root)/'worker') as (_,node):
            with Pool([node]) as pool,center(pool,Path(root)/'center') as (_,client):
                identity=client.submit(python('pass'))['id'];client.wait(identity)
                entered,release=threading.Event(),threading.Event()
                original=pool.nodes[0].client.delete
                def delayed(*args):
                    entered.set();release.wait(2);return original(*args)
                with patch.object(pool.nodes[0].client,'delete',side_effect=delayed),ThreadPoolExecutor(max_workers=2) as executor:
                    waiting=executor.submit(Client(client.url).delete,identity)
                    try:
                        self.assertTrue(entered.wait(1))
                        health=executor.submit(Client(client.url).health)
                        self.assertEqual(health.result(timeout=.7)['role'],'scheduler')
                    finally:
                        release.set();waiting.result(timeout=2)
