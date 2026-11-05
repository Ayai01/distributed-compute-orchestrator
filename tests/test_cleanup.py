"""Real process and transport cleanup, including cached-source rehydration."""

from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import os
import hashlib
import json
import threading
import time
import unittest
from unittest.mock import patch

from remote_compute import Pool
from remote_compute.local import LocalClient
from test_center import center
from test_compute import provider, python, scratch, until
from remote_compute.cleanup import AssetCache


class CleanupTests(unittest.TestCase):
    def test_failed_retirement_is_reclaimed_on_the_next_start(self):
        with scratch()as directory:
            root=Path(directory)/'assets';root.mkdir();asset=root/('f'*64);asset.mkdir()
            (asset/'raw').write_bytes(b'discard');cache=AssetCache(root,limit=0)
            with patch('remote_compute.cleanup.remove_owned',side_effect=PermissionError('busy file')):
                cache.prune(set());until(lambda:cache.reclaimer.snapshot()['pending']==0)
            self.assertFalse(asset.exists());self.assertTrue(list((root/'.trash').iterdir()))
            self.assertEqual(len(cache.reclaimer.snapshot()['errors']),1)
            again=AssetCache(root);until(lambda:again.reclaimer.snapshot()['pending']==0)
            self.assertEqual(list((root/'.trash').iterdir()),[])

    def test_corrupt_receipt_keeps_its_diagnostic_scratch(self):
        with scratch()as directory:
            root=Path(directory)/'worker';identity='7'*32
            job=root/'jobs'/identity;job.mkdir(parents=True);(job/'result.json').write_bytes(b'{broken')
            temporary=Path(directory)/'temp';scope=temporary/hashlib.sha256(str(root.resolve()).encode()).hexdigest()[:12]
            diagnostic=scope/identity;diagnostic.mkdir(parents=True);(diagnostic/'raw').write_bytes(b'recovery')
            with provider(root,temp_root=temporary)as(worker,_):
                self.assertIn(identity,worker.startup_recoveries)
                self.assertEqual((diagnostic/'raw').read_bytes(),b'recovery')

    def test_cached_immutable_asset_sizes_skip_tree_walks_on_restart(self):
        with scratch()as directory:
            root=Path(directory)/'assets';root.mkdir();identity='a'*64
            asset=root/identity;asset.mkdir();(asset/'model').write_bytes(b'x'*123)
            first=AssetCache(root)
            self.assertEqual(first.snapshot()['bytes'],123)
            with patch('remote_compute.cleanup.os.walk',side_effect=AssertionError('startup rescanned immutable files')):
                again=AssetCache(root)
            self.assertEqual(again.snapshot()['bytes'],123)
            with again.size_index.open('ab')as stream:stream.write(b'{"id":')
            other=root/('b'*64);other.mkdir();(other/'input').write_bytes(b'new')
            again=AssetCache(root)
            with patch('remote_compute.cleanup.os.walk',side_effect=AssertionError('torn metadata lost a complete size')):
                self.assertEqual(AssetCache(root).snapshot()['bytes'],126)

    def test_slow_asset_reclaim_retires_the_old_name_and_leaves_cache_control_free(self):
        from remote_compute.cleanup import remove_owned
        with scratch()as directory:
            root=Path(directory)/'assets';root.mkdir();identity='c'*64
            asset=root/identity;asset.mkdir();(asset/'data').write_bytes(b'paid')
            cache=AssetCache(root,limit=0);entered=threading.Event();release=threading.Event()
            def slow(path):
                entered.set()
                if not release.wait(5):raise AssertionError('reclaim was not released')
                return remove_owned(path)
            with patch('remote_compute.cleanup.remove_owned',side_effect=slow):
                try:
                    self.assertEqual(cache.prune(set()),[])
                    self.assertTrue(entered.wait(2));self.assertFalse(asset.exists())
                    self.assertEqual(cache.snapshot()['bytes'],0)
                    # The same content can be resynchronized while retired
                    # bytes are still being reclaimed under a private name.
                    asset.mkdir();(asset/'data').write_bytes(b'paid');cache.touch(identity)
                    self.assertEqual(cache.snapshot()['bytes'],4)
                finally:release.set()
                until(lambda:cache.reclaimer.snapshot()['pending']==0)
            self.assertEqual((asset/'data').read_bytes(),b'paid')

    def test_startup_reclaims_only_its_orphan_scratch_and_unfinished_asset_uploads(self):
        with scratch()as directory:
            root=Path(directory)/'worker';assets=root/'assets';assets.mkdir(parents=True)
            upload=assets/('d'*64+'.upload');upload.mkdir();(upload/'partial').write_bytes(b'discard')
            (assets/'operator-note').write_bytes(b'keep')
            temporary=Path(directory)/'temp';scope=temporary/hashlib.sha256(str(root.resolve()).encode()).hexdigest()[:12]
            orphan=scope/('e'*32);orphan.mkdir(parents=True);(orphan/'raw').write_bytes(b'discard')
            other=temporary/'other-owner';other.mkdir();(other/'data').write_bytes(b'keep')
            with provider(root,temp_root=temporary)as(worker,_):
                self.assertFalse(upload.exists());self.assertFalse(orphan.exists())
                until(lambda:worker.asset_cache.reclaimer.snapshot()['pending']==worker.temp_reclaimer.snapshot()['pending']==0)
                self.assertEqual((other/'data').read_bytes(),b'keep')
                self.assertEqual((assets/'operator-note').read_bytes(),b'keep')

    def test_late_terminal_response_cannot_recreate_a_collected_directory(self):
        with scratch() as directory, provider(Path(directory)/'worker') as (_,node):
            with Pool([node]) as pool, center(pool,Path(directory)/'center') as (state,client):
                identity=client.submit(python("print('paid result')"))['id']
                receipt=client.wait(identity)
                job=state.jobs[identity]
                client.fetch(identity)
                with state.condition:
                    state._update(job,receipt)
                    state._save(job)
                    state._retry(job,dict(status='interrupted',error='late worker 404'))
                self.assertNotIn(identity,state.jobs)
                self.assertFalse(job['directory'].exists())

    def test_center_reclaims_scoped_inputs_only_after_all_producers_and_receipts_release(self):
        with scratch() as directory, provider(Path(directory)/'worker') as (_, node):
            with Pool([node]) as pool, center(pool, Path(directory)/'center') as (state, client):
                state.asset_cache.limit = 1
                one, two = '1'*32, '2'*32
                source = {'value': b'original immutable input'}
                asset = client.sync_files(source, job_id=one, content_id='frozen-input')
                self.assertEqual(client.sync_files(source, job_id=two, content_id='frozen-input'), asset)
                task = python("from pathlib import Path;print(Path(r'${asset:data}/value').read_text())", assets={'data':asset})
                # The second producer has not submitted, well beyond the old grace.
                client.submit(task, job_id=one); client.wait(one)
                with state.asset_cache.lock:
                    size, _ = state.asset_cache.entries[asset]
                    state.asset_cache.entries[asset] = (size, time.time()-60)
                client.fetch(one)
                self.assertTrue((state.root/'assets'/(asset+'.zip')).is_file())
                self.assertNotIn(one, state.asset_handoffs)
                self.assertIn(two, state.asset_handoffs)
                client.submit(task, job_id=two); client.wait(two)
                client.release_assets(two)
                self.assertTrue((state.root/'assets'/(asset+'.zip')).is_file(), 'Uncollected receipt owns its input')
                self.assertEqual(client.fetch(two)['stdout'].strip(), 'original immutable input')
                self.assertFalse((state.root/'assets'/(asset+'.zip')).exists())
                self.assertEqual(state.asset_cache.snapshot()['bytes'], 0)

    def test_abandoned_center_preparation_releases_or_expires_without_a_task(self):
        with scratch() as directory, provider(Path(directory)/'worker') as (_, node):
            with Pool([node]) as pool, center(pool, Path(directory)/'center') as (state, client):
                state.asset_cache.limit = 1
                one, two = '3'*32, '4'*32
                first = client.sync_files({'data':b'cancelled preparation'}, job_id=one)
                client.release_assets(one)
                self.assertFalse((state.root/'assets'/(first+'.zip')).exists())
                second = client.sync_files({'data':b'abandoned preparation'}, job_id=two)
                assets, _ = state.asset_handoffs[two]
                state.asset_handoffs[two] = (assets, time.monotonic()-1)
                # Ordinary later upload is sufficient; no timer or extra GC loop.
                client.sync_files({'data':b'current preparation'}, job_id='5'*32)
                self.assertFalse((state.root/'assets'/(second+'.zip')).exists())
                self.assertEqual(len(state.jobs), 0)
                self.assertNotIn(two, state.asset_handoffs)

    def test_legacy_producer_keeps_shared_asset_after_scoped_consumer_is_collected(self):
        with scratch() as directory, provider(Path(directory)/'worker') as (_, node):
            with Pool([node]) as pool, center(pool, Path(directory)/'center') as (state, client):
                state.asset_cache.limit = 1
                source = {'data':b'shared with an older caller'}
                asset = client.sync_files(source)
                identity = '6'*32
                self.assertEqual(client.sync_files(source, job_id=identity), asset)
                client.run(python('print("collected")', assets={'data':asset}), job_id=identity)
                self.assertTrue((state.root/'assets'/(asset+'.zip')).exists())
            self.assertFalse((Path(directory)/'center/assets'/(asset+'.zip')).exists())

    def test_two_collection_cleanups_do_not_delete_the_same_job_twice(self):
        class Together:
            def __init__(self):
                self.ready=threading.Barrier(2);self.lock=threading.Lock()
            def __enter__(self):
                self.ready.wait(timeout=3);self.lock.acquire()
            def __exit__(self,*args):
                self.lock.release()
        with scratch() as directory,provider(Path(directory)/'worker') as (_,node):
            with Pool([node]) as pool,center(pool,Path(directory)/'center') as (state,client):
                identity=client.submit(python("print('once')"))['id'];client.wait(identity)
                state.jobs[identity]['artifact_lock']=Together()
                with ThreadPoolExecutor(max_workers=2) as executor:
                    results=list(executor.map(lambda _:client.delete(identity),range(2)))
                self.assertEqual(results,[{'deleted':identity}]*2)
                self.assertEqual(client.status(),[])

    def test_center_reclaims_old_unreferenced_archives_without_breaking_upload_handoff(self):
        with scratch() as directory,provider(Path(directory)/'worker') as (_,provider_client):
            with Pool([dict(name='test',client=provider_client)]) as pool:
                state=Path(directory)/'center'
                with center(pool,state) as (coordinator,client):
                    coordinator.asset_cache.limit=1
                    first=client.sync_files({'data':b'original model'})
                    # Even an old upload must survive the gap before submission.
                    with patch('remote_compute.coordinator.time.time',return_value=time.time()+60):
                        client.sync_files({'data':b'next model'})
                    task=python("print('retained')",assets={'model':first})
                    identity=client.submit(task)['id'];client.wait(identity)
                    self.assertTrue((state/'assets'/(first+'.zip')).exists())
                # Closing prunes the unreferenced upload, protects the receipt.
                self.assertEqual(list((state/'assets').glob('*.zip')),[state/'assets'/(first+'.zip')])
                self.assertTrue((state/'jobs'/identity/'result.json').exists())
                with center(pool,state) as (_,client):
                    self.assertIn('retained',client.fetch(identity)['stdout'])

    def test_worker_exit_prunes_recent_unreferenced_cache_but_keeps_uncollected_results(self):
        with scratch() as directory:
            root=Path(directory)
            with provider(root/'worker') as (worker,client):
                worker.asset_cache.limit=1
                kept=client.sync_files({'data':b'needed model'})
                identity=client.submit(python("print('saved')",assets={'data':kept}))['id']
                client.wait(identity)
                unused=client.sync_files({'data':b'unused source'})
            self.assertTrue((root/'worker/assets'/kept).exists())
            self.assertFalse((root/'worker/assets'/unused).exists())
            self.assertTrue((root/'worker/jobs'/identity/'result.json').exists())

    @unittest.skipUnless(os.name=='nt','Windows junction cleanup')
    def test_result_cleanup_removes_junction_without_deleting_dependency(self):
        with scratch() as directory, provider(Path(directory)/'worker') as (worker, client):
            dependency=Path(directory)/'dependency'
            dependency.mkdir();(dependency/'keep.dat').write_bytes(b'needed tool')
            code=("from pathlib import Path;import subprocess\n"
                  f"subprocess.run(['cmd.exe','/c','mklink','/J','linked-tools',{str(dependency)!r}],check=True,capture_output=True)\n"
                  "Path('metrics.json').write_text('{}')")
            result=client.run(python(code,outputs=['metrics.json']))
            self.assertEqual(result['status'],'succeeded',result)
            self.assertEqual((dependency/'keep.dat').read_bytes(),b'needed tool')

    def test_finished_and_failed_jobs_keep_only_declared_outputs_before_fetch(self):
        with scratch() as directory, provider(Path(directory)/'worker') as (worker, client):
            for code, status in ((0, 'succeeded'), (4, 'failed')):
                task=python("from pathlib import Path;import sys\n"
                            "Path('raw').mkdir();Path('raw/unused.bin').write_bytes(b'x'*1048576)\n"
                            "Path('metrics.json').write_text('{\"value\":7}')\n"
                            f"sys.exit({code})", outputs=['metrics.json'])
                identity=client.submit(task, files={'request.dat':b'consumed'})['id']
                self.assertEqual(client.wait(identity)['status'], status)
                work=worker.root/'jobs'/identity/'input'
                self.assertEqual(sorted(p.relative_to(work).as_posix() for p in work.rglob('*')), ['metrics.json'])
                self.assertEqual(client.fetch(identity)['files']['metrics.json'],b'{"value":7}')
                self.assertFalse(work.parent.exists())

    def test_queue_cancellation_reclaims_inputs_without_waiting_for_fetch(self):
        with scratch() as directory, provider(Path(directory)/'worker') as (worker, client):
            first=client.submit(python('import time;time.sleep(10)'))['id']
            until(lambda:client.status(first)['status']=='running')
            second=client.submit(python('print("must not run")'),files={'raw.bin':b'x'*1048576})['id']
            self.assertEqual(client.cancel(second)['status'],'cancelled')
            self.assertEqual(list((worker.root/'jobs'/second/'input').iterdir()),[])
            client.fetch(second)
            client.cancel(first);client.wait(first);client.fetch(first)

    def test_worker_lru_protects_uncollected_result_assets_and_center_restores_evicted_source(self):
        with scratch() as directory, provider(Path(directory)/'worker') as (worker, provider_client):
            worker.asset_cache.limit=100
            with Pool([dict(name='test',client=provider_client)],refresh=60) as pool, center(pool,Path(directory)/'center') as (_,client):
                first=client.sync_files({'data':b'a'*80})
                second=client.sync_files({'data':b'b'*80})
                def task(asset):
                    return python("from pathlib import Path;print(Path(r'${asset:model}/data').read_text())",assets={'model':asset})
                one=client.submit(task(first))['id'];client.wait(one)
                two=client.submit(task(second))['id'];client.wait(two)
                self.assertTrue((worker.root/'assets'/first).exists())
                self.assertTrue((worker.root/'assets'/second).exists())
                with patch('remote_compute.worker.time.time',return_value=time.time()+6):
                    client.fetch(two)
                self.assertTrue((worker.root/'assets'/first).exists())
                self.assertTrue((worker.root/'assets'/second).exists())
                client.fetch(one)
                with patch('remote_compute.worker.time.time',return_value=time.time()+60),worker.condition:
                    worker._prune_assets()
                self.assertFalse((worker.root/'assets'/first).exists())
                # Retain the center's stale session cache to exercise the race
                # between worker eviction and the next periodic health sample.
                node=pool.nodes[0]
                node.known_assets.add(first)
                node.asset_session=(node.health.get('pid'),node.health.get('root'),node.health.get('asset_revision'))
                # Previously deployed providers keep the ordinary one-shot
                # resynchronization when the handoff extension is absent.
                node.health['capabilities'].remove('asset_handoff')
                restored=client.run(task(first))
                self.assertEqual(restored['status'],'succeeded',restored)
                self.assertEqual(restored['stdout'].strip(),'a'*80)
                with patch('remote_compute.worker.time.time',return_value=time.time()+6),worker.condition:
                    worker._prune_assets()
                self.assertLessEqual(worker.asset_cache.snapshot()['bytes'],100)

    def test_recent_asset_survives_collection_between_upload_and_submit(self):
        with scratch() as directory, provider(Path(directory)/'worker',cpus=2) as (worker,client):
            worker.asset_cache.limit=100
            active=client.sync_files({'data':b'a'*160})
            first=client.submit(python("import time;time.sleep(20)",assets={'data':active}))['id']
            until(lambda:client.status(first)['status']=='running')
            incoming=client.sync_files({'data':b'b'*80})
            with worker.condition:worker._prune_assets()
            self.assertTrue((worker.root/'assets'/incoming).exists())
            result=client.run(python("from pathlib import Path;print(Path(r'${asset:data}/data').read_text())",assets={'data':incoming}))
            self.assertEqual(result['stdout'].strip(),'b'*80)
            with patch('remote_compute.worker.time.time',return_value=time.time()+6),worker.condition:
                worker._prune_assets()
            self.assertTrue((worker.root/'assets'/incoming).exists())
            replacement=client.sync_files({'data':b'c'*80})
            with patch('remote_compute.worker.time.time',return_value=time.time()+6),worker.condition:
                worker._prune_assets()
            self.assertFalse((worker.root/'assets'/incoming).exists())
            self.assertTrue((worker.root/'assets'/replacement).exists())
            self.assertTrue((worker.root/'assets'/active).exists())
            client.cancel(first);client.wait(first);client.fetch(first)

    def test_first_asset_handoff_survives_collection_without_reupload(self):
        self._slow_asset_handoff()

    def test_private_worker_has_the_same_slow_asset_handoff(self):
        self._slow_asset_handoff(local=True)

    def _slow_asset_handoff(self, *, local=False):
        with scratch() as directory, provider(Path(directory)/'worker') as (worker, provider_client):
            worker.asset_cache.limit=1
            if local:provider_client=LocalClient(worker)
            with Pool([provider_client]) as pool, center(pool,Path(directory)/'center') as (_,client):
                source=client.sync_files({'code.txt':b'source'})
                model=client.sync_files({'data.txt':b'model'})
                original=pool.nodes[0].client.submit_bundle
                original_asset=worker.asset;attempts=[];uploads=[]
                def counted(asset,payload=None,**options):
                    if payload is not None:uploads.append(asset)
                    return original_asset(asset,payload,**options)
                def delayed(identity,payload,**options):
                    # A collector can run after the five-second upload grace,
                    # while this request is still crossing the network.
                    with worker.condition:
                        for key,(size,_used) in tuple(worker.asset_cache.entries.items()):
                            worker.asset_cache.entries[key]=(size,time.time()-6)
                        worker._prune_assets()
                    attempts.append(identity)
                    return original(identity,payload,**options)
                task=python("from pathlib import Path;print(Path(r'${asset:code}/code.txt').read_text()+Path(r'${asset:model}/data.txt').read_text())",
                            assets={'code':source,'model':model},retries=0)
                with patch.object(worker,'asset',side_effect=counted),patch.object(pool.nodes[0].client,'submit_bundle',side_effect=delayed):
                    identity=client.submit(task)['id']
                    result=client.wait(identity)
                self.assertEqual(result['status'],'succeeded',result.get('error'))
                self.assertEqual(set(attempts),{identity})
                self.assertEqual(len(attempts),1)
                self.assertEqual(sorted(uploads),sorted([source,model]))
                self.assertEqual(client.fetch(identity)['stdout'].strip(),'sourcemodel')
                self.assertFalse(worker.asset_handoffs)
                with worker.condition:
                    for key,(size,_used) in tuple(worker.asset_cache.entries.items()):
                        worker.asset_cache.entries[key]=(size,time.time()-6)
                    worker._prune_assets()
                self.assertLessEqual(worker.asset_cache.snapshot()['bytes'],1)

    def test_abandoned_asset_handoffs_expire_and_shared_handoffs_release_independently(self):
        with scratch() as directory, provider(Path(directory)/'worker') as (worker,client):
            worker.asset_cache.limit=1
            asset=client.sync_files({'model':b'model'})
            one,two='a'*32,'b'*32
            for identity in (one,two):
                client.request('/jobs/'+identity+'/asset-handoff',{'assets':[asset]})
            self.assertFalse(worker.jobs)
            with worker.condition:
                worker.asset_cache.entries[asset]=(5,time.time()-6)
                worker._prune_assets()
            self.assertTrue((worker.root/'assets'/asset).exists())
            client.request('/jobs/'+one+'/asset-handoff',method='DELETE')
            with worker.condition:
                worker._prune_assets()
                self.assertTrue((worker.root/'assets'/asset).exists())
                assets,_expires=worker.asset_handoffs[two]
                worker.asset_handoffs[two]=(assets,time.monotonic()-1)
                worker._prune_assets()
            self.assertFalse(worker.asset_handoffs)
            self.assertFalse((worker.root/'assets'/asset).exists())

    def test_failed_submission_releases_its_asset_handoff_and_keeps_the_error(self):
        from remote_compute import RemoteError
        with scratch() as directory,provider(Path(directory)/'worker') as (worker,provider_client):
            with Pool([provider_client]) as pool,center(pool,Path(directory)/'center') as (_,client):
                asset=client.sync_files({'model':b'model'})
                failures=[]
                def reject(identity,payload,**options):
                    failures.append(identity)
                    message='Synchronize missing asset first: '+asset if len(failures)==1 else 'original validation failure'
                    error=RemoteError(message);error.status_code=400
                    raise error
                with patch.object(pool.nodes[0].client,'submit_bundle',side_effect=reject):
                    identity=client.submit(python('pass',assets={'model':asset},retries=0))['id']
                    result=client.wait(identity)
                self.assertIn('original validation failure',result['error'])
                self.assertEqual(failures,[identity,identity])
                self.assertFalse(worker.asset_handoffs)
                self.assertFalse(worker.jobs)
                client.fetch(identity)


if __name__=='__main__':
    unittest.main()
