"""Large ready queues and legacy dispatch envelopes retain their public contract."""
from concurrent.futures import ThreadPoolExecutor
from hashlib import sha256
import json
from pathlib import Path
import threading
import time
from types import SimpleNamespace
import unittest

from remote_compute import Pool
from remote_compute.bundle import atomic_json
from remote_compute.coordinator import Coordinator
from remote_compute.receipts import read_center_receipt
from remote_compute.worker import asset_handoff
from test_compute import scratch,provider,python
from test_center import center


class AssetInterfaceTests(unittest.TestCase):
    def test_large_placement_pass_yields_to_handoffs_and_survives_removed_candidates(self):
        state=Coordinator.__new__(Coordinator)
        state.condition=threading.Condition(threading.RLock())
        state.asset_cache=SimpleNamespace(lock=threading.RLock())
        state.asset_handoffs={};state.stopping=False;state.paused=False
        state.jobs={str(i):dict(task=dict(project='P',cpus=1,priority=0),
            result=dict(status='queued'))for i in range(120)}
        state.queue=list(state.jobs);entered=threading.Event();errors=[]
        def acquire(*args,**kwargs):
            entered.set();time.sleep(.005)
        state.pool=SimpleNamespace(nodes=[],_matching=lambda task:[],_acquire=acquire)
        state._recover_running=lambda:None
        state._poll_observations=lambda:None
        state._fallback_accelerator=lambda job:None
        state.project_resources=lambda:dict(P=dict(used=0,resource_share=1))
        state._growth_headroom=lambda *a,**k:{}
        state._fixed_queue_claims=lambda *a:{}
        state._fixed_queue_headroom=lambda *a:{}
        def schedule():
            try:state._schedule()
            except Exception as error:errors.append(error)
        thread=threading.Thread(target=schedule);thread.start()
        try:
            self.assertTrue(entered.wait(2))
            with ThreadPoolExecutor(1)as calls:
                response=calls.submit(asset_handoff,state,'a'*32,['b'*64]).result(timeout=.25)
                self.assertEqual(response,dict(id='a'*32))
                with state.condition:
                    for key in state.queue[30:]:state.jobs.pop(key)
                    del state.queue[30:]
                    state.condition.notify_all()
                # Continue through a yield with the earlier candidate snapshot.
                time.sleep(.2)
        finally:
            with state.condition:state.stopping=True;state.condition.notify_all()
            thread.join(3)
        self.assertFalse(thread.is_alive());self.assertEqual(errors,[])

    def test_legacy_center_hash_is_qualified_without_mutating_paid_receipt(self):
        with scratch()as root:
            folder=Path(root)/('a'*32);folder.mkdir()
            payload=b'original immutable request';original=sha256(payload).hexdigest()
            raw=dict(id=folder.name,status='succeeded',request_sha256='b'*64,cpu_seconds=1514.94)
            atomic_json(folder/'result.json',raw)
            atomic_json(folder/'admission.json',dict(id=folder.name,request_sha256=original))
            (folder/'request.zip').write_bytes(payload)
            receipt=read_center_receipt(folder/'result.json')
            self.assertEqual(receipt['request_sha256'],original)
            self.assertEqual(receipt['worker_request_sha256'],'b'*64)
            self.assertEqual(receipt['cpu_seconds'],1514.94)
            self.assertEqual(json.loads((folder/'result.json').read_bytes()),raw)
            (folder/'request.zip').write_bytes(b'different request')
            with self.assertRaisesRegex(ValueError,'differs from its admission'):
                read_center_receipt(folder/'result.json')

    def test_center_status_and_collection_keep_original_and_worker_hashes(self):
        with scratch()as root,provider(Path(root)/'worker')as(worker,remote):
            with Pool([remote])as pool,center(pool,Path(root)/'center')as(state,client):
                identity=client.submit(python('print(42)'))['id']
                original=client.wait(identity)
                worker_hash=worker.get(identity)['request_sha256']
                self.assertNotEqual(original['request_sha256'],worker_hash)
                self.assertEqual(original['worker_request_sha256'],worker_hash)
                # Model an old retained receipt, evicted from the hot cache.
                path=state.root/'jobs'/identity/'result.json'
                value=json.loads(path.read_bytes());value['request_sha256']=worker_hash
                value.pop('worker_request_sha256',None);atomic_json(path,value)
                with state.condition:
                    state.jobs.recent.pop(identity,None);state.jobs.live.pop(identity,None)
                self.assertEqual(client.status(identity)['request_sha256'],original['request_sha256'])
                receipt=client.fetch(identity,delete=False)
                self.assertEqual(receipt['request_sha256'],original['request_sha256'])
                self.assertEqual(receipt['worker_request_sha256'],worker_hash)
                self.assertEqual(receipt['cpu_seconds'],original['cpu_seconds'])
                self.assertEqual(receipt['stdout'].strip(),'42')
                client.delete(identity)
