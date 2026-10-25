"""Real HTTP regressions for slow and interrupted result transfers."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import hashlib
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
import json
import os
from pathlib import Path
import socket
import threading
import time
import unittest
from unittest.mock import patch

from remote_compute import Client, Pool, RemoteError
from remote_compute.bundle import pack, unpack
from remote_compute.client import PoolBusy, RequestCancelled, _ConnectionPool
from remote_compute.worker import Handler
from test_center import center
from test_compute import provider, python, scratch, until


class BulkIOControlTests(unittest.TestCase):
    def test_oversized_request_stops_before_opening_a_connection(self):
        client=Client('http://127.0.0.1:1')
        with patch('remote_compute.client.MAX_BODY',64),patch('remote_compute.client.http.client.HTTPConnection') as connection:
            for body in (b'x'*65,{'value':'x'*64}):
                with self.subTest(binary=isinstance(body,bytes)):
                    with self.assertRaisesRegex(ValueError,'protocol limit is 64 bytes'):
                        client.request('/assets/'+'0'*64,body)
            connection.assert_not_called()

    def test_request_at_body_limit_and_larger_get_response_remain_valid(self):
        with scratch() as root,provider(Path(root)/'worker') as (worker,client):
            payload=pack({'data':b'original immutable input'});identity=hashlib.sha256(payload).hexdigest()
            with patch('remote_compute.client.MAX_BODY',len(payload)):
                client.request('/assets/'+identity,payload)
            with patch('remote_compute.client.MAX_BODY',1):
                self.assertTrue(client.has_asset(identity))
            self.assertTrue((worker.root/'assets'/identity).is_dir())

    def test_shutdown_during_unpack_does_not_publish_or_leave_an_upload(self):
        with scratch() as root,provider(Path(root)/'worker') as (worker,client):
            payload=pack({'model':b'original weights'});asset=hashlib.sha256(payload).hexdigest()
            entered,release=threading.Event(),threading.Event()
            def delayed(data,directory):
                entered.set();release.wait(5);return unpack(data,directory)
            with patch('remote_compute.worker.unpack',side_effect=delayed),ThreadPoolExecutor(max_workers=1) as transfers:
                pending=transfers.submit(client.request,'/assets/'+asset,payload)
                try:
                    self.assertTrue(entered.wait(2));worker.close()
                finally:release.set()
                with self.assertRaisesRegex(RemoteError,'Worker is stopping'):pending.result(timeout=3)
            self.assertFalse((worker.root/'assets'/asset).exists())
            self.assertFalse((worker.root/'assets'/(asset+'.upload')).exists())
            self.assertFalse(worker.asset_writes)

    def test_slow_unpack_does_not_block_other_assets_or_jobs_and_same_id_unpacks_once(self):
        with scratch() as root,provider(Path(root)/'worker') as (worker,client):
            payload=pack({'large':b'original immutable model'});asset=hashlib.sha256(payload).hexdigest()
            entered,release=threading.Event(),threading.Event();unpacks=[]
            def delayed(data,directory):
                if data==payload:
                    unpacks.append(asset);entered.set();release.wait(5)
                return unpack(data,directory)
            with patch('remote_compute.worker.unpack',side_effect=delayed),ThreadPoolExecutor(max_workers=3) as transfers:
                first=transfers.submit(client.request,'/assets/'+asset,payload)
                try:
                    self.assertTrue(entered.wait(2))
                    duplicate=transfers.submit(client.request,'/assets/'+asset,payload)
                    small=transfers.submit(client.sync_files,{'small':b'other input'}).result(timeout=2)
                    identity=transfers.submit(client.submit,python('print("ready")',assets={'data':small})).result(timeout=2)['id']
                    self.assertEqual(client.wait(identity)['status'],'succeeded')
                    self.assertEqual(client.fetch(identity)['stdout'].strip(),'ready')
                    self.assertFalse(first.done());self.assertFalse(duplicate.done())
                finally:
                    release.set();first.result(timeout=3)
                self.assertTrue(duplicate.result(timeout=3)['exists'])
            self.assertEqual(unpacks,[asset]);self.assertFalse(worker.asset_writes)

    def test_waiting_asset_metadata_keeps_a_control_connection_available(self):
        with scratch() as directory, provider(Path(directory)/'worker') as (worker, client):
            entered, release = threading.Event(), threading.Event()
            original_asset=worker.asset
            def metadata(identity,payload=None,**options):
                if identity in {format(i,'064x') for i in range(7)}:
                    release.wait(5)
                return original_asset(identity,payload,**options)
            def slow(*args, **kwargs):
                entered.set()
                if not release.wait(5):raise AssertionError('Unpack was not released')
                return unpack(*args, **kwargs)
            # Saturate metadata connections without depending on a global unpack lock.
            with patch.object(worker,'asset',side_effect=metadata),patch('remote_compute.worker.unpack', side_effect=slow), ThreadPoolExecutor(max_workers=10) as executor:
                upload = executor.submit(client.sync_files, {'data':b'original data'})
                try:
                    self.assertTrue(entered.wait(3))
                    lookups = [executor.submit(client.request, '/assets/'+format(i,'064x')) for i in range(7)]
                    until(lambda:client._transport.bulk_active==7)
                    self.assertEqual(executor.submit(client.health).result(timeout=2)['role'], 'worker')
                finally:
                    release.set()
                upload.result(timeout=5)
                self.assertTrue(all(not lookup.result(timeout=5)['exists'] for lookup in lookups))

    def blocked(self, target, original, action, probe, *, matches=lambda *args: True, concurrent=None):
        entered, release = threading.Event(), threading.Event()
        def slow(*args, **kwargs):
            if matches(*args):
                entered.set()
                if not release.wait(5):
                    raise AssertionError('Blocked I/O was not released')
            return original(*args, **kwargs)
        with patch(target, side_effect=slow), ThreadPoolExecutor(max_workers=3) as executor:
            first = executor.submit(action)
            try:
                self.assertTrue(entered.wait(3), target)
                second = executor.submit(concurrent) if concurrent else None
                executor.submit(probe).result(timeout=2)
                if second:
                    self.assertFalse(second.done(), 'Deletion must wait for the original artifact reader')
            finally:
                release.set()
            result = first.result(timeout=5)
            if second:
                second.result(timeout=5)
            return result

    def test_worker_artifact_pack_preserves_control_and_serializes_delete(self):
        with scratch() as directory, provider(Path(directory)/'worker') as (_, client):
            identity = client.submit(python("from pathlib import Path;Path('answer').write_text('paid result')", outputs=['answer']))['id']
            client.wait(identity)
            result = self.blocked('remote_compute.worker.pack', pack,
                lambda: client.fetch(identity, delete=False),
                lambda: self.assertEqual(client.status(identity)['status'], 'succeeded'),
                concurrent=lambda: client.delete(identity))
            self.assertEqual(result['files']['answer'], b'paid result')

    def test_versioned_file_read_finishes_before_concurrent_delete(self):
        with scratch() as directory, provider(Path(directory)/'worker') as (worker, client):
            identity = client.submit(python("from pathlib import Path;Path('answer').write_text('paid result')", outputs=['answer']))['id']
            client.wait(identity)
            target = worker.root/'jobs'/identity/'input'/'answer'
            original = Path.open
            entered, release = threading.Event(), threading.Event()
            @contextmanager
            def slow(path, *args, **kwargs):
                with original(path, *args, **kwargs) as stream:
                    if path != target:
                        yield stream
                    else:
                        class Reader:
                            def fileno(self):return stream.fileno()
                            def read(self):
                                entered.set()
                                if not release.wait(5):raise AssertionError('Read was not released')
                                return stream.read()
                        yield Reader()
            with patch.object(Path, 'open', slow), ThreadPoolExecutor(max_workers=3) as executor:
                first = executor.submit(client.read_job_file_changed, identity, 'answer')
                try:
                    self.assertTrue(entered.wait(3))
                    deletion = executor.submit(client.delete, identity)
                    self.assertEqual(executor.submit(client.status, identity).result(timeout=2)['status'], 'succeeded')
                    self.assertFalse(deletion.done())
                finally:
                    release.set()
                self.assertEqual(first.result(timeout=5)['data'], b'paid result')
                deletion.result(timeout=5)

    def test_worker_asset_unpack_and_submission_keep_status_and_grants_responsive(self):
        with scratch() as directory, provider(Path(directory)/'worker', cpus=2) as (_, client):
            identity = client.submit(python('import time;time.sleep(30)', cpus=2))['id']
            until(lambda: client.status(identity)['status'] == 'running')
            def probe():
                self.assertEqual(client.status(identity)['status'], 'running')
                self.assertEqual(client.resize(identity, 1)['cpus'], 1)
                self.assertEqual(client.health()['used_cpus'], 1)
            try:
                asset = self.blocked('remote_compute.worker.unpack', unpack,
                    lambda: client.sync_files({'model': b'original model'}), probe)
                submitted = self.blocked('remote_compute.worker.unpack', unpack,
                    lambda: client.submit(python('print("second")', assets={'model': asset})), probe)
                self.assertEqual(client.wait(submitted['id'])['status'], 'succeeded')
                client.fetch(submitted['id'])
            finally:
                client.cancel(identity)
                client.wait(identity)
                client.fetch(identity)

    def test_center_asset_and_request_writes_leave_control_responsive(self):
        with scratch() as directory, provider(Path(directory)/'worker') as (_, node):
            with Pool([node]) as pool, center(pool, Path(directory)/'center') as (state, client):
                identity = client.submit(python('print("retained")'))['id']
                client.wait(identity)
                probe = lambda: self.assertEqual(client.status(identity)['status'], 'succeeded')
                original = Path.write_bytes
                # Path methods need their descriptor binding retained by the patch.
                for kind in ('asset', 'request'):
                    entered, release = threading.Event(), threading.Event()
                    def slow(path, data):
                        if state.root in path.parents and (path.suffix == '.upload' if kind == 'asset' else path.name == 'request.zip'):
                            entered.set()
                            if not release.wait(5):
                                raise AssertionError('Write was not released')
                        return original(path, data)
                    action = (lambda: client.sync_files({'code': b'original code'})) if kind == 'asset' else (lambda: client.submit(python('print("second")')))
                    with patch.object(Path, 'write_bytes', slow), ThreadPoolExecutor(max_workers=2) as executor:
                        first = executor.submit(action)
                        try:
                            self.assertTrue(entered.wait(3))
                            executor.submit(probe).result(timeout=2)
                        finally:
                            release.set()
                        outcome = first.result(timeout=5)
                    if kind == 'request':
                        client.wait(outcome['id']); client.fetch(outcome['id'])
                client.fetch(identity)

    def test_center_existing_asset_metadata_stays_responsive_during_other_writes(self):
        with scratch() as directory, provider(Path(directory)/'worker') as (_, node):
            with Pool([node]) as pool, center(pool, Path(directory)/'center') as (state, client):
                known=client.sync_files({'known':b'published immutable input'})
                original=Path.write_bytes
                for kind in ('asset','request'):
                    entered,release=threading.Event(),threading.Event()
                    def slow(path,data):
                        if state.root in path.parents and (path.suffix=='.upload' if kind=='asset' else path.name=='request.zip'):
                            entered.set()
                            if not release.wait(5):raise AssertionError('Write was not released')
                        return original(path,data)
                    action=(lambda:client.sync_files({'new':b'another immutable input'})) if kind=='asset' else (lambda:client.submit(python('print("ok")',assets={'known':known})))
                    with self.subTest(kind=kind), patch.object(Path,'write_bytes',slow), ThreadPoolExecutor(max_workers=2) as executor:
                        first=executor.submit(action)
                        try:
                            self.assertTrue(entered.wait(3))
                            self.assertTrue(executor.submit(client.request,'/assets/'+known).result(timeout=1)['exists'])
                        finally:
                            release.set()
                            accepted=first.result(timeout=5)
                    if kind=='request':
                        client.wait(accepted['id']);client.fetch(accepted['id'])

    def test_center_acceptance_protects_input_after_producer_releases_its_handoff(self):
        with scratch() as directory, provider(Path(directory)/'worker') as (_, node):
            with Pool([node]) as pool, center(pool, Path(directory)/'center') as (state, client):
                identity='a'*32
                asset=client.sync_files({'code':b'original input'},job_id=identity)
                state.asset_cache.limit=0
                original=Path.write_bytes;entered,release=threading.Event(),threading.Event()
                def slow(path,data):
                    if state.root in path.parents and path.name=='request.zip':
                        entered.set()
                        if not release.wait(5):raise AssertionError('Write was not released')
                    return original(path,data)
                with patch.object(Path,'write_bytes',slow),ThreadPoolExecutor(max_workers=2) as executor:
                    accepted=executor.submit(client.submit,python('print("ok")',assets={'code':asset}),job_id=identity)
                    try:
                        self.assertTrue(entered.wait(3))
                        executor.submit(client.release_assets,identity).result(timeout=1)
                        self.assertTrue(client.request('/assets/'+asset,extra_headers={'X-DCO-Asset-Job':identity})['exists'])
                    finally:release.set()
                    self.assertEqual(accepted.result(timeout=5)['id'],identity)
                self.assertEqual(client.wait(identity)['status'],'succeeded')
                client.fetch(identity)


class OriginalSubmitReceiptTests(unittest.TestCase):
    @staticmethod
    def disconnect(handler):
        handler.connection.shutdown(socket.SHUT_RDWR)
        handler.close_connection=True

    def test_submission_notice_does_not_reopen_the_published_job(self):
        from remote_compute.local import LocalClient
        from remote_compute.records import JobRecords
        for role in ('worker','center','local'):
            with self.subTest(role=role),scratch() as directory,provider(Path(directory)/'worker') as (worker,node):
                with Pool([node]) as pool,center(pool,Path(directory)/'center') as (scheduler,project):
                    state,client=(scheduler,project) if role=='center' else (worker,LocalClient(worker) if role=='local' else node)
                    original_submit,original_read=state.submit,JobRecords.__getitem__
                    accepted_ids=[]
                    def submit(identity,*args,**options):
                        receipt=original_submit(identity,*args,**options)
                        accepted_ids.append(identity)
                        return receipt
                    def read(records,identity):
                        # Archival can remove an active entry between its two
                        # unlocked dictionary reads. Acceptance is already done.
                        if records is state.jobs and identity in accepted_ids and not state.condition._is_owned():
                            raise KeyError(identity)
                        return original_read(records,identity)
                    with patch.object(state,'submit',submit),patch.object(JobRecords,'__getitem__',read):
                        receipt=client.submit(python('print("once")',project='RGCN'),job_id='1'*32)
                    self.assertEqual(accepted_ids,['1'*32])
                    self.assertEqual(receipt['id'],'1'*32)
                    self.assertEqual(receipt['project'],'RGCN')
                    self.assertFalse(state.activity.snapshot()['events'])
                    self.assertEqual(client.wait(receipt['id'])['status'],'succeeded')
                    self.assertEqual(client.fetch(receipt['id'])['stdout'].strip(),'once')

    def test_post_404_after_acceptance_reads_original_without_resending_body(self):
        for role in ('worker','center'):
            with self.subTest(role=role),scratch() as directory,provider(Path(directory)/'worker') as (worker,node):
                with Pool([node]) as pool,center(pool,Path(directory)/'center') as (scheduler,project):
                    state,client=(worker,node) if role=='worker' else (scheduler,project)
                    original=Handler._reply;posts=[];reads=[]
                    def reply(handler,value,status=200,**options):
                        if handler.server.worker is state and handler.path=='/jobs/'+'2'*32:
                            if handler.command=='POST' and status==202:
                                posts.append(value['id'])
                                return original(handler,{'error':'submission notice lost its active record'},404)
                            if handler.command=='GET':reads.append(handler.path)
                        return original(handler,value,status,**options)
                    with patch.object(Handler,'_reply',reply):
                        receipt=client.submit(python('print("original")'),job_id='2'*32)
                    self.assertEqual(posts,['2'*32])
                    self.assertEqual(reads,['/jobs/'+'2'*32])
                    self.assertEqual(receipt['id'],'2'*32)
                    self.assertEqual(client.wait(receipt['id'])['status'],'succeeded')
                    self.assertEqual(client.fetch(receipt['id'])['stdout'].strip(),'original')

    def test_unconfirmed_post_404_is_preserved_without_resending_body(self):
        for exists in (False,True):
            with self.subTest(existing_original=exists),scratch() as directory,provider(Path(directory)/'worker') as (_,client):
                identity='3'*32
                if exists:
                    client.submit(python('print("paid original")'),job_id=identity)
                    client.wait(identity)
                original=Handler.do_POST;posts=[]
                def post(handler):
                    if handler.path=='/jobs/'+identity:
                        posts.append(handler.path)
                        handler.rfile.read(int(handler.headers['Content-Length']))
                        return handler._reply({'error':'original 404'},404)
                    return original(handler)
                with patch.object(Handler,'do_POST',post):
                    with self.assertRaises(RemoteError) as caught:
                        client.submit(python('print("different body")'),job_id=identity)
                self.assertEqual(caught.exception.status_code,404)
                self.assertEqual(posts,['/jobs/'+identity])
                if exists:self.assertEqual(client.fetch(identity)['stdout'].strip(),'paid original')

    def test_lost_submit_reply_reads_original_without_resending_worker_or_center_body(self):
        for role in ('worker','center'):
            with self.subTest(role=role),scratch() as directory,provider(Path(directory)/'worker') as (worker,node):
                with Pool([node]) as pool,center(pool,Path(directory)/'center') as (scheduler,project):
                    state,client=(worker,node) if role=='worker' else (scheduler,project)
                    original=Handler._reply;posts=[];reads=[]
                    def reply(handler,value,status=200,**options):
                        if handler.server.worker is state and handler.path.startswith('/jobs/'):
                            if handler.command=='POST' and status==202:
                                posts.append(value['id']);self.disconnect(handler);return
                            if handler.command=='GET':reads.append(handler.path)
                        return original(handler,value,status,**options)
                    with patch.object(Handler,'_reply',reply):
                        accepted=client.submit(python('print("once")'),job_id='a'*32)
                    self.assertEqual(accepted['id'],'a'*32)
                    self.assertEqual(posts,['a'*32])
                    self.assertEqual(reads,['/jobs/'+'a'*32])
                    self.assertEqual(client.wait(accepted['id'])['status'],'succeeded')
                    self.assertEqual(client.fetch(accepted['id'])['stdout'].strip(),'once')

    def test_missing_original_after_interrupted_upload_retries_only_the_same_id(self):
        with scratch() as directory,provider(Path(directory)/'worker') as (worker,client):
            original=Handler._handle;paths=[]
            def handle(handler):
                if handler.path=='/jobs/'+'b'*32:
                    paths.append(handler.command)
                    if paths==['POST']:
                        self.disconnect(handler);return
                return original(handler)
            with patch.object(Handler,'do_POST',handle),patch.object(Handler,'do_GET',handle):
                accepted=client.submit(python('print("retained")'),job_id='b'*32)
            self.assertEqual(paths,['POST','GET','POST'])
            self.assertEqual(accepted['id'],'b'*32)
            self.assertEqual(client.wait(accepted['id'])['status'],'succeeded')
            client.fetch(accepted['id'])

    def test_center_acceptance_reply_does_not_wait_for_unrelated_asset_cache_io(self):
        with scratch() as directory,provider(Path(directory)/'worker') as (_,node):
            with Pool([node]) as pool,center(pool,Path(directory)/'center') as (state,client):
                state.control({'paused':True})
                saved,cache_locked,release=threading.Event(),threading.Event(),threading.Event()
                original=state._save
                def save(job):
                    original(job);saved.set()
                    if not cache_locked.wait(3):raise AssertionError('Cache operation did not start')
                def cache_io():
                    if not saved.wait(3):raise AssertionError('Request was not persisted')
                    with state.asset_cache.lock:
                        cache_locked.set()
                        if not release.wait(5):raise AssertionError('Cache operation was not released')
                with patch.object(state,'_save',save),ThreadPoolExecutor(max_workers=2) as executor:
                    blocking=executor.submit(cache_io)
                    accepted=executor.submit(client.submit,python('print("queued")'),job_id='e'*32)
                    try:
                        receipt=accepted.result(timeout=2)
                        self.assertEqual(receipt['id'],'e'*32)
                        self.assertEqual(receipt['status'],'queued')
                        self.assertFalse(state.accepting_assets)
                    finally:
                        release.set();blocking.result(timeout=2);accepted.result(timeout=2)
                client.cancel('e'*32);client.fetch('e'*32)

    def test_submit_timeout_after_acceptance_recovers_before_waiting_for_lost_reply(self):
        with scratch() as directory,provider(Path(directory)/'worker') as (_,node):
            with Pool([node]) as pool,center(pool,Path(directory)/'center') as (state,client):
                original=Handler._reply;release=threading.Event();posts=[]
                def reply(handler,value,status=200,**options):
                    if handler.server.worker is state and handler.command=='POST' and status==202:
                        posts.append(value['id']);release.wait(3)
                    return original(handler,value,status,**options)
                client.timeout=.1
                try:
                    with patch.object(Handler,'_reply',reply):
                        accepted=client.submit(python('print("retained")'),job_id='f'*32)
                        self.assertEqual(posts,['f'*32])
                        self.assertEqual(accepted['id'],'f'*32)
                        self.assertFalse(release.is_set())
                finally:release.set()
                self.assertEqual(client.wait(accepted['id'])['status'],'succeeded')
                client.fetch(accepted['id'])

    def test_lost_conflict_reply_never_accepts_a_different_original_body(self):
        with scratch() as directory,provider(Path(directory)/'worker') as (worker,client):
            identity='c'*32
            client.submit(python('print("original")'),job_id=identity)
            original=Handler._reply;conflicts=[]
            def reply(handler,value,status=200,**options):
                if handler.command=='POST' and handler.path=='/jobs/'+identity and status==409:
                    conflicts.append(status)
                    if len(conflicts)==1:self.disconnect(handler);return
                return original(handler,value,status,**options)
            with patch.object(Handler,'_reply',reply):
                with self.assertRaises(RemoteError) as caught:
                    client.submit(python('print("different")'),job_id=identity)
            self.assertEqual(caught.exception.status_code,409)
            self.assertEqual(conflicts,[409,409])
            client.wait(identity)
            self.assertEqual(client.fetch(identity)['stdout'].strip(),'original')

    def test_recovered_cancelled_receipt_keeps_its_actual_outcome(self):
        with scratch() as directory,provider(Path(directory)/'worker') as (worker,client):
            worker.control({'paused':True})
            original=Handler._reply;posts=[]
            def reply(handler,value,status=200,**options):
                if handler.command=='POST' and status==202:
                    posts.append(value['id']);worker.cancel(value['id'])
                    self.disconnect(handler);return
                return original(handler,value,status,**options)
            with patch.object(Handler,'_reply',reply):
                accepted=client.submit(python('raise RuntimeError("must stay cancelled")'),job_id='d'*32)
            self.assertEqual(posts,['d'*32])
            self.assertEqual(accepted['status'],'cancelled')
            self.assertEqual(client.fetch(accepted['id'])['cpu_seconds'],0)


class ArtifactReloadTests(unittest.TestCase):
    def test_legacy_memory_return_only_confirms_an_identical_cpu_grant(self):
        identity='e'*32;url='http://127.0.0.1:12345'
        state=dict(id=identity,status='running',cpus=1,memory_mb=512)
        with patch.dict(os.environ,REMOTE_COMPUTE_LEASE='1',REMOTE_COMPUTE_JOB_ID=identity,
                        REMOTE_COMPUTE_WORKER_JOB_ID=identity,REMOTE_COMPUTE_URL=url):
            for changes,requested in (({'status':'failed'},128),({'status':'cancelled'},128),
                    ({'id':'f'*32},128),({'cpus':2},128),({'memory_mb':64},128),({},768)):
                with self.subTest(changes=changes,requested=requested):
                    client=Client(url);client._capabilities=['resize_memory']
                    failure=RemoteError('min() iterable argument is empty');failure.status_code=409
                    with patch.object(client,'request',side_effect=[failure,dict(state,**changes)]) as request:
                        with self.assertRaises(RemoteError):client.resize(identity,1,memory_mb=requested)
                    self.assertEqual([c.args[0] for c in request.call_args_list],['/jobs/'+identity+'/lease','/jobs/'+identity])
                    self.assertEqual(request.call_args_list[-1].kwargs,dict(brief=True))

    def test_unrelated_or_growing_resize_errors_are_never_hidden_or_replayed(self):
        identity='e'*32;url='http://127.0.0.1:12345'
        for leased,cpus,memory,message in ((False,1,128,'min() iterable argument is empty'),
                (True,0,128,'min() iterable argument is empty'),(True,1,128,'Insufficient memory'),
                (True,1,None,'min() iterable argument is empty')):
            with self.subTest(leased=leased,cpus=cpus,memory=memory,message=message), \
                    patch.dict(os.environ,REMOTE_COMPUTE_LEASE='1' if leased else '0',REMOTE_COMPUTE_JOB_ID=identity,REMOTE_COMPUTE_URL=url):
                client=Client(url);client._capabilities=['resize_memory','resize_auto']
                failure=RemoteError(message);failure.status_code=409
                with patch.object(client,'request',side_effect=failure) as request:
                    with self.assertRaises(RemoteError):client.resize(identity,cpus,memory_mb=memory)
                self.assertEqual(request.call_count,1)

    def test_completed_result_survives_temporary_upstream_outage_without_reexecution(self):
        with scratch() as folder,provider(Path(folder)/'worker') as (_,worker_client):
            with Pool([dict(name='test',client=worker_client)]) as pool,center(pool,Path(folder)/'center') as (_,client):
                identity=client.submit(python("print('executed once')"))['id']
                original=client.wait(identity)
                raw=pool.nodes[0].client.artifacts(identity)
                with patch.object(pool.nodes[0].client,'artifacts',side_effect=[RemoteError('provider reloading'),RemoteError('provider starting'),raw]) as download, \
                        patch.object(client,'submit',side_effect=AssertionError('must not resubmit')):
                    result=client.fetch(identity)
                self.assertEqual(download.call_count,3)
                self.assertEqual(result['id'],identity)
                self.assertEqual(result['cpu_seconds'],original['cpu_seconds'])
                self.assertEqual(result['stdout'].strip(),'executed once')
                with self.assertRaises(RemoteError):client.status(identity)

    def test_unknown_result_is_reported_immediately(self):
        client=Client('http://127.0.0.1:1')
        error=RemoteError('unknown task');error.status_code=404
        with patch.object(Client,'request',side_effect=error) as request, \
                patch('remote_compute.client.time.sleep',side_effect=AssertionError('permanent error must not wait')):
            with self.assertRaises(RemoteError):client.artifacts('0'*32)
        self.assertEqual(request.call_count,1)


@contextmanager
def slow_upload_server(*, interrupt=False, started=None):
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
                if started is not None:started.set()
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


class StatusReadReconnectTests(unittest.TestCase):
    @staticmethod
    def outage(status=None):
        error=RemoteError('temporary status connection loss')
        if status is None:error.transport_retryable=True
        else:error.status_code=status
        return error

    def test_resize_bootstrap_recovers_health_before_one_actual_grant_change(self):
        with scratch() as root,provider(Path(root)/'worker') as (_,client):
            identity=client.submit(python('import time; time.sleep(30)'))['id']
            until(lambda:client.status(identity,brief=True)['status']=='running')
            original=Handler._reply;requests=[]
            def reply(handler,value,status=200):
                requests.append((handler.command,handler.path))
                if handler.path.startswith('/health') and sum(path.startswith('/health') for _,path in requests)<=2:
                    handler.close_connection=True;return
                return original(handler,value,status)
            try:
                with patch.object(Handler,'_reply',reply):
                    grant=client.resize(identity,1,memory_mb=256)
                self.assertEqual((grant['id'],grant['cpus'],grant['memory_mb']),(identity,1,256))
                self.assertEqual(sum(path.startswith('/health') for _,path in requests),3)
                self.assertEqual([path for method,path in requests if method=='POST'],['/jobs/'+identity+'/resize'])
            finally:
                client.cancel(identity);client.wait(identity);client.fetch(identity)

    def test_resize_bootstrap_rejects_permanent_health_error_before_mutation(self):
        client=Client();error=self.outage(403)
        with patch.object(client,'health',side_effect=error) as health,patch.object(client,'request') as request,patch('remote_compute.client.time.sleep') as sleep:
            with self.assertRaises(RemoteError) as caught:client.resize('a'*32,1,memory_mb=256)
        self.assertIs(caught.exception,error)
        self.assertEqual(health.call_count,1);request.assert_not_called();sleep.assert_not_called()

    def test_resize_bootstrap_outage_is_finite_and_never_changes_grant(self):
        client=Client();clock=[0.]
        def sleep(seconds):clock[0]+=seconds
        with patch.object(client,'health',side_effect=self.outage()) as health,patch.object(client,'request') as request,patch('remote_compute.client.time.monotonic',side_effect=lambda:clock[0]),patch('remote_compute.client.time.sleep',side_effect=sleep):
            with self.assertRaises(RemoteError):client.resize('a'*32,1,memory_mb=256)
        self.assertAlmostEqual(clock[0],120.)
        self.assertLess(health.call_count,40);request.assert_not_called()

    def test_unsupported_resize_recovers_only_original_grant_read(self):
        client=Client();client._capabilities=[];identity='b'*32
        state=dict(id=identity,status='running',cpus=1,memory_mb=512)
        with patch.object(client,'status',side_effect=[self.outage(),state]) as status,patch.object(client,'request') as request,patch('remote_compute.client.time.sleep'):
            grant=client.resize(identity,0,memory_mb=256)
        self.assertEqual((grant['id'],grant['cpus'],grant['memory_mb'],grant['supported']),(identity,1,512,False))
        self.assertEqual([call.args for call in status.call_args_list],[(identity,)]*2)
        request.assert_not_called()

    def test_legacy_lease_confirms_original_grant_after_read_outage_without_replaying_lease(self):
        client=Client();client._capabilities=['resize_memory'];identity='c'*32;worker='d'*32
        failure=self.outage(409);failure.args=('min() iterable argument is empty',)
        state=dict(id=worker,status='running',cpus=1,memory_mb=512)
        environment=dict(REMOTE_COMPUTE_LEASE='1',REMOTE_COMPUTE_URL=client.url,
                         REMOTE_COMPUTE_JOB_ID=identity,REMOTE_COMPUTE_WORKER_JOB_ID=worker)
        with patch.dict(os.environ,environment),patch.object(client,'request',side_effect=[failure,self.outage(),state]) as request,patch('remote_compute.client.time.sleep'):
            grant=client.resize(identity,1,memory_mb=256)
        self.assertEqual([call.args[0] for call in request.call_args_list],['/jobs/'+worker+'/lease','/jobs/'+worker,'/jobs/'+worker])
        self.assertEqual(grant['id'],identity)
        self.assertTrue(grant['existing_grant_confirmed']);self.assertFalse(grant['resize_applied'])

    def test_wait_recovers_after_both_http_status_attempts_drop(self):
        with scratch() as root,provider(Path(root)/'worker') as (worker,client):
            identity=client.submit(python("print('paid once')"))['id']
            original=Handler._reply;dropped=[]
            def reply(handler,value,status=200):
                if handler.command=='GET' and handler.path=='/jobs/'+identity and len(dropped)<2:
                    dropped.append(identity);handler.close_connection=True;return
                return original(handler,value,status)
            with patch.object(Handler,'_reply',reply):
                result=client.wait(identity,wait_timeout=5)
            self.assertEqual(dropped,[identity,identity])
            self.assertEqual(list(worker.jobs),[identity])
            self.assertEqual(result['stdout'].strip(),'paid once')
            self.assertEqual(client.fetch(identity)['id'],identity)

    def test_wait_recovers_terminal_receipt_without_changing_id(self):
        client=Client();identity='a'*32
        receipt=dict(id=identity,status='succeeded',cpu_seconds=7)
        with patch.object(client,'status',side_effect=[dict(id=identity,status='succeeded'),self.outage(503),receipt]) as status,patch('remote_compute.client.time.sleep'):
            self.assertEqual(client.wait(identity),dict(receipt,url=client.url))
        self.assertEqual([call.args for call in status.call_args_list],[(identity,)]*3)

    def test_wait_does_not_retry_a_missing_original(self):
        client=Client();error=self.outage(404)
        with patch.object(client,'status',side_effect=error) as status,patch('remote_compute.client.time.sleep') as sleep:
            with self.assertRaises(RemoteError) as caught:client.wait('b'*32)
        self.assertIs(caught.exception,error)
        self.assertEqual(status.call_count,1);sleep.assert_not_called()

    def test_wait_reconnect_preserves_the_callers_deadline(self):
        client=Client();clock=[0.];error=self.outage()
        def sleep(seconds):clock[0]+=seconds
        with patch.object(client,'status',side_effect=error),patch('remote_compute.client.time.monotonic',side_effect=lambda:clock[0]),patch('remote_compute.client.time.sleep',side_effect=sleep):
            with self.assertRaises(RemoteError) as caught:client.wait('c'*32,wait_timeout=.3)
        self.assertIs(caught.exception,error)
        self.assertAlmostEqual(clock[0],.3)

    def test_admission_recovers_only_the_same_readonly_advice(self):
        client=Client();grant=dict(current=3,maximum=8)
        with patch.object(client,'request',side_effect=[self.outage(502),grant]) as request,patch('remote_compute.client.time.sleep'):
            self.assertEqual(client.admission('RGCN',cpus=1,requires=['ng47']),grant)
        self.assertEqual(request.call_count,2)
        self.assertEqual(request.call_args_list[0],request.call_args_list[1])
        self.assertEqual(request.call_args.args[0],'/admission')

    def test_forwarded_admission_reconnect_remains_an_advice_operation(self):
        client=Client();identity='d'*32
        environment=dict(REMOTE_COMPUTE_LEASE='1',REMOTE_COMPUTE_URL=client.url,
                         REMOTE_COMPUTE_JOB_ID='e'*32,REMOTE_COMPUTE_WORKER_JOB_ID=identity)
        with patch.dict(os.environ,environment),patch.object(client,'request',side_effect=[self.outage(),dict(current=2)]) as request,patch('remote_compute.client.time.sleep'):
            self.assertEqual(client.admission('ARH-DSE')['current'],2)
        self.assertEqual(request.call_args_list[0],request.call_args_list[1])
        self.assertEqual(request.call_args.args[0],'/jobs/'+identity+'/lease')
        self.assertEqual(request.call_args.args[1]['_operation'],'admission')

    def test_admission_permanent_error_is_immediate(self):
        client=Client();error=self.outage(403)
        with patch.object(client,'request',side_effect=error) as request,patch('remote_compute.client.time.sleep') as sleep:
            with self.assertRaises(RemoteError):client.admission('RGCN')
        self.assertEqual(request.call_count,1);sleep.assert_not_called()

    def test_admission_outage_has_a_finite_window(self):
        client=Client();clock=[0.]
        def sleep(seconds):clock[0]+=seconds
        with patch.object(client,'request',side_effect=self.outage()) as request,patch('remote_compute.client.time.monotonic',side_effect=lambda:clock[0]),patch('remote_compute.client.time.sleep',side_effect=sleep):
            with self.assertRaises(RemoteError):client.admission('RGCN')
        self.assertAlmostEqual(clock[0],120.)
        self.assertLess(request.call_count,40)

    def test_known_asset_recovers_without_repacking_or_reuploading(self):
        with scratch() as root,provider(Path(root)/'worker') as (worker,client):
            identity=client.sync_files({'model':b'original bytes'},content_id='frozen-model')
            owner='a'*32;original=Handler._reply;requests=[]
            def reply(handler,value,status=200):
                if handler.path=='/assets/'+identity:
                    requests.append((handler.command,handler.headers.get('X-DCO-Asset-Job')))
                    if len(requests)<=2:handler.close_connection=True;return
                return original(handler,value,status)
            with patch.object(Handler,'_reply',reply),patch('remote_compute.client.pack',side_effect=AssertionError('Known model was repacked')):
                self.assertEqual(client.sync_files(lambda: self.fail('Known model was prepared'),content_id='frozen-model',job_id=owner),identity)
            self.assertEqual(requests,[('GET',owner)]*3)
            self.assertEqual(worker.asset_handoffs[owner][0],frozenset({identity}))

    def test_asset_presence_reads_only_metadata_for_existing_and_missing_assets(self):
        with scratch() as root,provider(Path(root)/'worker') as (worker,client):
            identity=client.sync_files({'code.py':b'print(1)'})
            with patch('remote_compute.client.pack',side_effect=AssertionError('Presence check prepared data')):
                self.assertTrue(client.has_asset(identity))
                self.assertFalse(client.has_asset('0'*64))
            self.assertEqual((worker.root/'assets'/identity/'code.py').read_bytes(),b'print(1)')
            self.assertFalse((worker.root/'assets'/('0'*64)).exists())
            self.assertFalse(worker.jobs)

    def test_asset_presence_permanent_error_is_immediate(self):
        client=Client();error=self.outage(403)
        with patch.object(client,'request',side_effect=error) as request,patch('remote_compute.client.time.sleep') as sleep:
            with self.assertRaises(RemoteError):client.has_asset('a'*64)
        self.assertEqual(request.call_count,1);sleep.assert_not_called()

    def test_asset_presence_outage_has_the_same_finite_window(self):
        client=Client();clock=[0.]
        def sleep(seconds):clock[0]+=seconds
        with patch.object(client,'request',side_effect=self.outage()) as request,patch('remote_compute.client.time.monotonic',side_effect=lambda:clock[0]),patch('remote_compute.client.time.sleep',side_effect=sleep):
            with self.assertRaises(RemoteError):client.has_asset('a'*64,job_id='b'*32)
        self.assertAlmostEqual(clock[0],120.)
        self.assertLess(request.call_count,40)
        self.assertTrue(all(call.args==('/assets/'+'a'*64,) and call.kwargs=={'extra_headers':{'X-DCO-Asset-Job':'b'*32}} for call in request.call_args_list))


class TransportTests(unittest.TestCase):
    def test_resource_lease_waits_past_normal_timeout_without_extending_other_requests(self):
        identity = 'a'*32
        calls, deadlines = [], []
        original = http.client.HTTPConnection.request
        def request(connection, method, path, *args, **kwargs):
            deadlines.append((path, connection.timeout))
            return original(connection, method, path, *args, **kwargs)
        def delayed_lease(key, value):
            calls.append((key, value))
            time.sleep(.15)
            return dict(id=key, cpus=1, memory_mb=128)
        with scratch() as root, provider(Path(root)/'worker') as (worker, remote):
            client = Client(remote.url, timeout=.05)
            environment = dict(REMOTE_COMPUTE_LEASE='1', REMOTE_COMPUTE_URL=remote.url,
                               REMOTE_COMPUTE_JOB_ID=identity, REMOTE_COMPUTE_WORKER_JOB_ID=identity)
            with patch.dict('os.environ', environment), patch.object(worker, 'lease', side_effect=delayed_lease), \
                    patch.object(http.client.HTTPConnection, 'request', request):
                result = client.resize(identity, 1, memory_mb=128)
                client.health(brief=True)
            self.assertEqual(result, dict(id=identity, cpus=1, memory_mb=128))
            self.assertEqual(len(calls), 1)
            self.assertIn(('/jobs/'+identity+'/lease', 130), deadlines)
            self.assertEqual(deadlines[-1], ('/health', .05))
            self.assertEqual(client.timeout, .05)

    def test_explicit_short_lease_deadline_never_resubmits_the_pending_request(self):
        identity = 'b'*32
        calls = []
        def delayed_lease(key, value):
            calls.append((key, value))
            time.sleep(.15)
            return dict(id=key, cpus=1, memory_mb=128)
        with scratch() as root, provider(Path(root)/'worker') as (worker, remote):
            client = Client(remote.url, timeout=1, lease_timeout=.05)
            with patch.object(worker, 'lease', side_effect=delayed_lease):
                with self.assertRaisesRegex(RemoteError, 'timed out'):
                    client.request('/jobs/'+identity+'/lease', dict(cpus=1, memory_mb=128))
                self.assertEqual(len(calls), 1)

    def test_direct_resource_resize_uses_the_lease_deadline(self):
        identity = 'c'*32
        calls, deadlines = [], []
        original = http.client.HTTPConnection.request
        def request(connection, method, path, *args, **kwargs):
            deadlines.append((path, connection.timeout))
            return original(connection, method, path, *args, **kwargs)
        def delayed_resize(key, cpus, **options):
            calls.append((key, cpus, options))
            time.sleep(.15)
            return dict(id=key, cpus=cpus, memory_mb=options['memory_mb'])
        with scratch() as root, provider(Path(root)/'worker') as (worker, remote):
            client = Client(remote.url, timeout=.05)
            with patch.object(worker, 'resize', side_effect=delayed_resize), \
                    patch.object(http.client.HTTPConnection, 'request', request):
                result = client.resize(identity, 1, memory_mb=128)
                client.health(brief=True)
            self.assertEqual(result, dict(id=identity, cpus=1, memory_mb=128))
            self.assertEqual(len(calls), 1)
            self.assertIn(('/jobs/'+identity+'/resize', 130), deadlines)
            self.assertEqual(deadlines[-1], ('/health', .05))
            self.assertEqual(client.timeout, .05)

    def test_timed_out_resize_is_not_replayed_and_original_grant_can_be_queried(self):
        identity = 'd'*32
        calls, state = [], dict(id=identity, status='running', cpus=4, memory_mb=512)
        def accepted_resize(key, cpus, **options):
            calls.append(key)
            state.update(cpus=cpus, memory_mb=options['memory_mb'])
            time.sleep(.15)
            return dict(id=key, cpus=state['cpus'], memory_mb=state['memory_mb'])
        with scratch() as root, provider(Path(root)/'worker') as (worker, remote):
            client = Client(remote.url, timeout=1, lease_timeout=.05)
            with patch.object(worker, 'resize', side_effect=accepted_resize), \
                    patch.object(worker, 'get', side_effect=lambda key:dict(state)):
                with self.assertRaisesRegex(RemoteError, 'timed out'):
                    client.resize(identity, 1, memory_mb=128)
                observed = client.status(identity)
                self.assertEqual(observed, dict(id=identity, status='running', cpus=1, memory_mb=128))
                self.assertEqual(calls, [identity])

    def test_retained_job_listing_does_not_block_a_lease_reply(self):
        entered, release = threading.Event(), threading.Event()
        with scratch() as root, provider(Path(root)/'worker') as (worker, client):
            identity = client.submit(python("print('retained')"))['id']
            client.wait(identity)
            original = worker.get
            def slow_copy(key):
                result = original(key)
                entered.set()
                release.wait(3)
                return result
            with patch.object(worker, 'get', side_effect=slow_copy), ThreadPoolExecutor(max_workers=1) as executor:
                listing = executor.submit(client.status)
                try:
                    self.assertTrue(entered.wait(2))
                    control = Client(client.url, timeout=.5)
                    self.assertEqual(control.request('/jobs/'+identity+'/lease',
                        dict(request_id=1, grant=dict(cpus=1, memory_mb=128))), dict(accepted=True))
                finally:
                    release.set()
                self.assertEqual([row['id'] for row in listing.result(timeout=3)], [identity])

    def test_retained_job_listing_tolerates_cleanup_after_its_id_snapshot(self):
        with scratch() as root, provider(Path(root)/'worker') as (worker, client):
            first = client.submit(python("print('first')"))['id']
            second = client.submit(python("print('second')"))['id']
            client.wait(first)
            client.wait(second)
            original = worker.get
            def cleanup_during_copy(identity):
                result = original(identity)
                if identity == first:
                    worker.delete(second)
                return result
            with patch.object(worker, 'get', side_effect=cleanup_during_copy):
                self.assertEqual([row['id'] for row in client.status()], [first])
            self.assertEqual(client.status(first)['status'], 'succeeded')

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

    def test_cancelled_asset_upload_stops_partial_body_without_retry_and_releases_slot(self):
        payload=b'asset-content'*160000
        identity=hashlib.sha256(payload).hexdigest()
        started,cancel=threading.Event(),threading.Event()
        with slow_upload_server(started=started) as (client,received),ThreadPoolExecutor(max_workers=1) as callers:
            upload=callers.submit(client.request,'/assets/'+identity,payload,cancel=cancel.is_set)
            self.assertTrue(started.wait(2));cancel.set()
            with self.assertRaises(RequestCancelled):upload.result(timeout=3)
            until(lambda:bool(received))
            self.assertEqual(len(received),1)
            self.assertGreater(len(received[0][1]),0)
            self.assertLess(len(received[0][1]),len(payload))
            self.assertEqual(client._transport.available,8)
            self.assertEqual(client._transport.bulk_active,0)
            self.assertEqual(client.request('/assets/'+identity),dict(exists=False))

    def test_cancelled_pool_wait_never_sends_or_takes_another_callers_slot(self):
        pool=_ConnectionPool()
        for _ in range(8):self.assertTrue(pool.acquire(0))
        client=Client();client._transport=pool;cancel=threading.Event()
        with patch.object(client,'_request') as request,ThreadPoolExecutor(max_workers=1) as callers:
            waiting=callers.submit(client.request,'/assets/'+'a'*64,b'data',cancel=cancel.is_set)
            until(lambda:bool(pool.waiting));cancel.set()
            with self.assertRaises(RequestCancelled):waiting.result(timeout=1)
            request.assert_not_called()
        self.assertFalse(pool.waiting)
        self.assertEqual(pool.available,0)
        for _ in range(8):pool.release()

    def test_lost_asset_reply_checks_publication_without_reuploading_the_body(self):
        payload=pack({'model.npz':bytes(range(256))*8192})
        identity=hashlib.sha256(payload).hexdigest()
        original=Handler._reply
        requests=[]
        owner='7'*32
        def reply(handler,value,status=200):
            if handler.path=='/assets/'+identity:
                self.assertEqual(handler.headers.get('X-DCO-Asset-Job'),owner)
                requests.append((handler.command,int(handler.headers.get('Content-Length',0))))
                if handler.command=='POST' and len(requests)==1:
                    self.assertTrue(value['exists'])
                    handler.close_connection=True
                    return
            return original(handler,value,status)
        with scratch() as root,provider(Path(root)/'worker') as (worker,client):
            with patch.object(Handler,'_reply',reply):
                result=client.request('/assets/'+identity,payload,extra_headers={'X-DCO-Asset-Job':owner})
            self.assertEqual(result,dict(id=identity,exists=True))
            self.assertEqual(requests,[('POST',len(payload)),('GET',0)])
            self.assertEqual(worker.asset_handoffs[owner][0],frozenset({identity}))
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

    def test_bulk_uploads_leave_a_slot_for_health_and_original_task_grants(self):
        with scratch() as root,provider(Path(root)/'worker') as (worker,client):
            identity=client.submit(python("import time;from pathlib import Path\nwhile not Path('release').exists():time.sleep(.02)"))['id']
            until(lambda:worker.get(identity)['status']=='running')
            client.timeout=5
            release=threading.Event();lock=threading.Lock()
            counts=dict(active=0,peak=0,requests=0)
            original=Handler._reply
            def delayed(handler,value,status=200):
                if handler.server.worker is not worker or handler.command!='POST' or not handler.path.startswith('/assets/'):
                    return original(handler,value,status)
                with lock:
                    counts['active']+=1;counts['requests']+=1
                    counts['peak']=max(counts['peak'],counts['active'])
                try:
                    release.wait(5)
                    return original(handler,value,status)
                finally:
                    with lock:counts['active']-=1
            payloads=[pack({'input.dat':bytes([i])*1024}) for i in range(8)]
            with patch.object(Handler,'_reply',delayed),ThreadPoolExecutor(max_workers=9) as callers:
                uploads=[callers.submit(client.request,'/assets/'+hashlib.sha256(data).hexdigest(),data) for data in payloads]
                try:
                    until(lambda:counts['active']==8 or counts['active']==7 and bool(client._transport.waiting))
                    health=callers.submit(client.health,brief=True).result(timeout=1)
                    self.assertEqual(health['role'],'worker')
                    grant=callers.submit(client.resize,identity,1,memory_mb=128).result(timeout=1)
                    self.assertEqual((grant['id'],grant['cpus'],grant['memory_mb']),(identity,1,128))
                    self.assertLessEqual(counts['peak'],7)
                finally:
                    release.set()
                self.assertTrue(all(f.result(timeout=5)['exists'] for f in uploads))
            self.assertEqual(counts['requests'],8)
            self.assertEqual(client._transport.available,8)
            self.assertFalse(client._transport.waiting)
            self.assertEqual(client.timeout,5)
            self.assertEqual(list(worker.jobs),[identity])
            client.write_job_file(identity,'release','done');client.wait(identity)
            self.assertEqual(client.fetch(identity)['status'],'succeeded')

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
