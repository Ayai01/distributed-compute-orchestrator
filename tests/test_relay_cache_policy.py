import hashlib
import os
from pathlib import Path
import threading
import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from remote_compute import Client
from remote_compute.bundle import pack
from remote_compute.cleanup import (
    ASSET_CACHE_IDLE_SECONDS, AssetCache, AssetMaintenance, prune_asset_caches,
)
from remote_compute.client import PoolBusy, _ConnectionPool
from remote_compute.relay_bulk import RelayBulkBudget
from remote_compute.shared_assets import SharedAssets
from test_compute import provider, python, scratch


class CachePolicyTests(unittest.TestCase):
    def test_hybrid_evicts_expired_then_oldest_and_keeps_referenced_inputs(self):
        with scratch() as directory:
            stores = [Path(directory) / name for name in ('center', 'worker')]
            now = time.time()
            for store in stores:
                store.mkdir()
            keys = [char * 64 for char in 'abcdef']
            fixtures = [
                (0, keys[0], 1, ASSET_CACHE_IDLE_SECONDS + 100),
                (0, keys[1], 6, ASSET_CACHE_IDLE_SECONDS + 100),
                (0, keys[2], 4, 200),
                (1, keys[3], 3, 100),
                (1, keys[4], 3, 3),
                (1, keys[5], 1, 1),
            ]
            for index, key, size, age in fixtures:
                path = stores[index] / (key + '.zip')
                path.write_bytes(b'x' * size)
                os.utime(path, (now - age, now - age))
            caches = [AssetCache(store, 11, archives=True) for store in stores]
            owners = (SimpleNamespace(asset_cache=caches[0], _protected_assets=lambda: {keys[1]}),
                      SimpleNamespace(asset_cache=caches[1], _protected_assets=set))
            try:
                self.assertEqual(prune_asset_caches(owners), [])
                self.assertEqual(set(caches[0].entries), {keys[1]})
                self.assertEqual(set(caches[1].entries), {keys[4], keys[5]})
                self.assertEqual(sum(cache.snapshot()['bytes'] for cache in caches), 10)
            finally:
                for cache in caches:
                    cache.reclaimer.close()

    def test_maintenance_runs_without_uploads_or_retirement_requests(self):
        ready = threading.Event()
        maintenance = AssetMaintenance(ready.set, self.fail, interval=.02)
        try:
            self.assertTrue(ready.wait(1))
        finally:
            maintenance.close()

    def test_shared_hit_updates_idle_age_and_cold_factory_recreates_asset(self):
        with scratch() as directory:
            root = Path(directory)
            shared = root / 'shared'
            with provider(root / 'worker', env={'DCO_SHARED_ASSET_CACHE': str(shared)}) as (worker, client):
                owner = worker.shared_assets
                cold = b'cold copy'
                expired = hashlib.sha256(cold).hexdigest()
                owner.publish(expired, cold)
                path = shared / (expired + '.zip')
                stale = time.time() - ASSET_CACHE_IDLE_SECONDS - 60
                os.utime(path, (stale, stale))
                self.assertEqual(owner.read(expired), cold)
                self.assertGreater(path.stat().st_mtime, stale)
                os.utime(path, (stale, stale))
                self.assertEqual(SharedAssets(shared).prune(), [])
                self.assertFalse(path.exists())

                calls = []

                def prepare():
                    calls.append(True)
                    return {'input': b'5'}

                asset = client.sync_files(prepare, content_id='a stable recipe')
                task = python("from pathlib import Path; print(int(Path(r'${asset:data}/input').read_text()) + 2)",
                              assets={'data': asset})
                for _ in range(2):
                    self.assertEqual(client.sync_files(prepare, content_id='a stable recipe'), asset)
                    result = client.run(task)
                    self.assertEqual(result['stdout'].strip(), '7')
                self.assertEqual(len(calls), 1)

    def test_corrupt_shared_archive_falls_back_to_verified_input(self):
        with scratch() as directory:
            root = Path(directory)
            shared = root / 'shared'
            with provider(root / 'worker', env={'DCO_SHARED_ASSET_CACHE': str(shared)}) as (worker, client):
                archive = pack({'model': b'valid'})
                asset = hashlib.sha256(archive).hexdigest()
                shared_file = shared / (asset + '.zip')
                shared_file.write_bytes(b'incomplete')
                self.assertFalse(client.has_asset(asset))
                self.assertTrue(client.request('/assets/' + asset, archive)['exists'])
                self.assertEqual(shared_file.read_bytes(), archive)
                self.assertEqual((worker.asset_cache.root / asset / 'model').read_bytes(), b'valid')


class RelayPolicyTests(unittest.TestCase):
    def test_upload_and_download_have_separate_pacing_when_configured(self):
        budget = RelayBulkBudget([19001], 1000, download_bytes_per_second=1000)
        budget._next = time.monotonic() + 100
        budget.download.pace(1, lambda: None)
        self.assertEqual(budget.snapshot()['upload_paced_bytes'], 0)
        self.assertEqual(budget.snapshot()['download_paced_bytes'], 1)

    def test_three_bulk_transfers_keep_short_dispatch_and_control_ready(self):
        client = Client('http://127.0.0.1:19001', pool_timeout=0)
        client.bulk_limit = 3
        pool = client._transport = _ConnectionPool()
        pool.bulk_limit = 3
        try:
            self.assertTrue(all(pool.acquire(0, bulk=True) for _ in range(3)))
            with patch('remote_compute.client._RELAY_BULK_BUDGET', RelayBulkBudget([19001], 1000)), \
                    patch.object(client, '_request', return_value={'ready': True}):
                self.assertTrue(client.request('/assets/' + 'a' * 64)['ready'])
                self.assertTrue(client.request('/health')['ready'])
                with self.assertRaises(PoolBusy):
                    client.request('/assets/' + 'b' * 64, bytes(300 * 1024))
        finally:
            for _ in range(3):
                pool.release(bulk=True)
