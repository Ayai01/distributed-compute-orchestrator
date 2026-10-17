"""Real process and transport cleanup, including cached-source rehydration."""

from pathlib import Path
import os
import time
import unittest
from unittest.mock import patch

from remote_compute import Pool
from test_center import center
from test_compute import provider, python, scratch, until


class CleanupTests(unittest.TestCase):
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
                self.assertFalse((worker.root/'assets'/second).exists())
                client.fetch(one)
                # Retain the center's stale session cache to exercise the race
                # between worker eviction and the next periodic health sample.
                node=pool.nodes[0]
                node.known_assets.add(second)
                node.asset_session=(node.health.get('pid'),node.health.get('root'),node.health.get('asset_revision'))
                restored=client.run(task(second))
                self.assertEqual(restored['status'],'succeeded',restored)
                self.assertEqual(restored['stdout'].strip(),'b'*80)
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
            self.assertFalse((worker.root/'assets'/incoming).exists())
            self.assertTrue((worker.root/'assets'/active).exists())
            client.cancel(first);client.wait(first);client.fetch(first)


if __name__=='__main__':
    unittest.main()
