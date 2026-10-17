"""Real HTTP regressions for slow and interrupted result transfers."""
from concurrent.futures import ThreadPoolExecutor
import hashlib
import io
from pathlib import Path
import threading
import time
import unittest
from unittest.mock import patch

from remote_compute import Client, Pool
from remote_compute.client import PoolBusy, _ConnectionPool
from remote_compute.worker import Handler
from test_center import center
from test_compute import provider, python, scratch, until


class TransportTests(unittest.TestCase):
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
            client=Client(timeout=.01);client._transport=pool
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
                original=pool.nodes[0].client.request
                mutex=threading.Lock();active=peak=0;calls=[]
                def request(path,*args,**kwargs):
                    nonlocal active,peak
                    if not path.endswith('/artifacts'):return original(path,*args,**kwargs)
                    with mutex:
                        active+=1;peak=max(peak,active);calls.append(path)
                    try:
                        time.sleep(.04)
                        return original(path,*args,**kwargs)
                    finally:
                        with mutex:active-=1
                with patch.object(pool.nodes[0].client,'request',side_effect=request),ThreadPoolExecutor(max_workers=4) as transfers:
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
                with patch.object(pool.nodes[0].client,'request',wraps=pool.nodes[0].client.request) as calls:
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
