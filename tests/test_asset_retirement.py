"""Explicit operator retirement preserves every live input owner."""
from pathlib import Path
import threading,time,unittest
from unittest.mock import patch

from remote_compute import Client,Pool,RemoteError
from remote_compute.cleanup import AssetCache,remove_owned
from test_compute import provider,python,scratch,until
from test_center import center


class AssetRetirementTests(unittest.TestCase):
    def test_operator_retires_only_selected_closed_asset_on_center_and_worker(self):
        with scratch()as root,provider(Path(root)/'worker')as(worker,node),Pool([node])as pool:
            with center(pool,Path(root)/'center')as(state,client):
                asset=client.sync_files({'data':b'closed input'})
                other=client.sync_files({'data':b'next input'})
                peer_other=node.sync_files({'data':b'other worker input'})
                identity=client.submit(python('print("done")',assets={'data':asset}))['id']
                client.wait(identity);worker.jobs.recent.clear()
                self.assertEqual(client.fetch(identity,delete=False)['stdout'].strip(),'done')
                with self.assertRaises(RemoteError)as denied:
                    Client(client.url, client.token, operator=False).control(retire_asset=asset)
                self.assertEqual(denied.exception.status_code,403)
                for call in (lambda:client.control(retire_asset=asset),
                             lambda:client.control(node=pool.nodes[0].name,worker={'retire_asset':asset})):
                    with self.assertRaises(RemoteError)as held:call()
                    self.assertEqual(held.exception.status_code,409)
                client.delete(identity)
                reply=client.control(retire_asset=asset)
                self.assertTrue(reply['retired']);self.assertGreater(reply['bytes'],0)
                self.assertNotIn(asset,state.legacy_assets)
                self.assertFalse((state.root/'assets'/(asset+'.zip')).exists())
                self.assertTrue((state.root/'assets'/(other+'.zip')).is_file())
                until(lambda: identity not in worker.jobs)
                reply=client.control(node=pool.nodes[0].name,worker={'retire_asset':asset})
                self.assertTrue(reply['retired']);self.assertEqual(reply['bytes'],len(b'closed input'))
                self.assertFalse((worker.root/'assets'/asset).exists())
                self.assertTrue((worker.root/'assets'/peer_other).is_dir())
                self.assertNotIn(asset,worker.asset_cache.entries)
                self.assertFalse(client.control(retire_asset=asset)['retired'])

    def test_producer_handoffs_and_inflight_uploads_keep_the_selected_asset(self):
        with scratch()as root,provider(Path(root)/'worker')as(worker,client):
            identity='a'*32;asset=client.sync_files({'data':b'pending input'},job_id=identity)
            with self.assertRaises(RemoteError)as held:client.control(retire_asset=asset)
            self.assertEqual(held.exception.status_code,409)
            with worker.condition:
                names,_=worker.asset_handoffs[identity]
                worker.asset_handoffs[identity]=(names,time.monotonic()-1)
            transfer=threading.Lock()
            with worker.asset_cache.lock:worker.asset_writes[asset]=transfer
            with self.assertRaises(RemoteError)as held:client.control(retire_asset=asset)
            self.assertEqual(held.exception.status_code,409)
            with transfer:
                with self.assertRaises(RemoteError)as held:client.control(retire_asset=asset)
                self.assertEqual(held.exception.status_code,409)
            del transfer
            self.assertTrue(client.control(retire_asset=asset)['retired'])

    def test_center_accepting_inputs_and_unsubmitted_producers_are_protected(self):
        with scratch()as root,provider(Path(root)/'worker')as(_,node),Pool([node])as pool,center(pool,Path(root)/'center')as(state,client):
            asset=client.sync_files({'data':b'pending input'},job_id='b'*32)
            with self.assertRaises(RemoteError)as held:client.control(retire_asset=asset)
            self.assertEqual(held.exception.status_code,409)
            client.release_assets('b'*32)
            with state.condition:state.accepting_assets.update([asset])
            try:
                with self.assertRaises(RemoteError)as held:client.control(retire_asset=asset)
                self.assertEqual(held.exception.status_code,409)
            finally:
                with state.condition:state.accepting_assets.clear()
            transfer=threading.Lock()
            with state.condition:state.asset_writes[asset]=transfer
            with self.assertRaises(RemoteError)as held:client.control(retire_asset=asset)
            self.assertEqual(held.exception.status_code,409)
            del transfer
            self.assertTrue(client.control(retire_asset=asset)['retired'])

    def test_invalid_or_mixed_control_does_not_change_resources(self):
        with scratch()as root,provider(Path(root)/'worker')as(worker,client):
            budget=worker.capacity
            for request in ({'retire_asset':'../outside'},{'retire_asset':True},
                            {'retire_asset':'a'*64,'cpus':1}):
                with self.subTest(request=request),self.assertRaises(RemoteError):client.control(**request)
            self.assertEqual(worker.capacity,budget)

    def test_slow_recursive_reclaim_does_not_hold_cache_control(self):
        with scratch()as root:
            assets=Path(root)/'assets';assets.mkdir();identity='c'*64
            target=assets/identity;target.mkdir();(target/'raw').write_bytes(b'old')
            other=assets/('d'*64);other.mkdir();(other/'raw').write_bytes(b'keep')
            cache=AssetCache(assets);entered=threading.Event();release=threading.Event()
            def slow(path):
                entered.set()
                if not release.wait(5):raise AssertionError('reclaim not released')
                remove_owned(path)
            with patch('remote_compute.cleanup.remove_owned',side_effect=slow):
                try:
                    self.assertEqual(cache.retire(identity)['bytes'],3)
                    self.assertTrue(entered.wait(2));self.assertFalse(target.exists())
                    self.assertEqual(cache.snapshot()['bytes'],4);self.assertTrue(other.exists())
                    self.assertEqual(cache.retire(identity)['bytes'],0)
                finally:release.set()
                until(lambda:not cache.reclaimer.snapshot()['pending'])
