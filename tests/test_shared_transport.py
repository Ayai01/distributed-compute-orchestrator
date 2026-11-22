"""Narrow checks for resumable wire encoding and cold asset publication."""
from concurrent.futures import ThreadPoolExecutor
import io
import json
import lzma
from hashlib import sha256
from pathlib import Path
from random import Random
from tempfile import TemporaryDirectory
import threading
import unittest
from unittest.mock import patch
from zipfile import ZIP_STORED, ZipFile

from remote_compute import Client, Pool, RemoteError
from remote_compute.asset_upload import ASSET_CHUNK_BYTES, ASSET_RESUME_MIN_BYTES, AssetUploads
from remote_compute.bundle import pack
from remote_compute.cleanup import TransportRetirement, UnregisteredRetirementProvider
from remote_compute.pipeline_control import _Controller
from test_center import center
from test_compute import provider, python, until


class SharedTransportTests(unittest.TestCase):
    def test_shared_upload_threshold_includes_64_kib(self):
        self.assertEqual(ASSET_RESUME_MIN_BYTES, 64 * 1024)

    def test_negotiated_xz_recovers_offset_in_original_zip_bytes(self):
        cases = [('compressible', True, True), ('legacy', False, True), ('incompressible', True, False)]
        for name, negotiated, compressible in cases:
            with self.subTest(name=name), TemporaryDirectory(prefix='dco-wire-encoding-') as folder:
                root = Path(folder)
                data = (b'x' * (ASSET_CHUNK_BYTES + 384) if compressible
                        else Random(1042).randbytes(ASSET_CHUNK_BYTES + 384))
                sink = io.BytesIO()
                with ZipFile(sink, 'w', compression=ZIP_STORED) as archive:
                    archive.writestr('asset.dat', data)
                raw = sink.getvalue()
                digest = sha256(raw).hexdigest()
                source = root / 'original.zip'
                source.write_bytes(raw)
                receiver = AssetUploads(root)
                trace = []
                wire = []

                def request(_path, body=None, *, extra_headers=None, **_):
                    if body is None:
                        offered = (extra_headers or {}).get('X-DCO-Accept-Chunk-Encoding')
                        self.assertEqual(offered, 'xz')
                        result = receiver.receive(digest, accept_encoding=offered if negotiated else None)
                        self.assertEqual(result.get('chunk_encoding'), 'xz' if negotiated else None)
                        trace.append(('get', result['offset']))
                        return result
                    headers = extra_headers or {}
                    span = headers.get('Content-Range')
                    if span is None:
                        return receiver.receive(digest, json.dumps(body).encode())
                    start = int(span.split()[1].split('-')[0])
                    encoding = headers.get('X-DCO-Chunk-Encoding')
                    wire.append(len(body))
                    trace.append(('post', start, encoding))
                    plain = lzma.decompress(body) if encoding == 'xz' else body
                    self.assertEqual(plain, raw[start:start + ASSET_CHUNK_BYTES])
                    result = receiver.receive(digest, body, span, encoding=encoding)
                    if start == 0:
                        # Receiver committed the original-byte offset before its reply was lost.
                        error = RemoteError('Simulated lost chunk acknowledgement')
                        error.transport_retryable = True
                        raise error
                    return result

                client = Client()
                metrics = {}
                with patch.object(client, 'request', side_effect=request):
                    result = client.upload_resumable(digest, source, report=metrics)
                self.assertTrue(result['complete'])
                self.assertEqual((root / (digest + '.zip')).read_bytes(), raw)
                self.assertEqual(trace[:3], [('get', 0),
                    ('post', 0, 'xz' if negotiated and compressible else None),
                    ('get', ASSET_CHUNK_BYTES)])
                self.assertEqual([row[1] for row in trace if row[0] == 'post'],
                                 [0, ASSET_CHUNK_BYTES])
                self.assertEqual(metrics['attempted_bytes'], len(raw))
                self.assertEqual(metrics['wire_body_bytes'], sum(wire))
                if negotiated and compressible:
                    self.assertLess(sum(wire), len(raw))
                else:
                    self.assertEqual(sum(wire), len(raw))

    def test_cold_asset_size_does_not_scan_under_cache_lock(self):
        with TemporaryDirectory(prefix='dco-cold-publication-') as folder:
            with provider(Path(folder) / 'worker') as (worker, client):
                files = {'a/file.bin': b'a' * 123, 'b.bin': b'b' * 187}
                payload = pack(files)
                identity = sha256(payload).hexdigest()
                cache = worker.asset_cache
                scan = cache._unpacked_size
                violations = []

                def inspect(path):
                    if cache.lock.locked():
                        violations.append(True)
                    return scan(path)

                with patch.object(cache, '_unpacked_size', side_effect=inspect):
                    result = client.request('/assets/' + identity, payload)
                self.assertTrue(result['exists'])
                self.assertFalse(result['cache_hit'])
                self.assertEqual(violations, [])
                self.assertEqual(cache.entries[identity][0], 310)

    def test_named_retirement_does_not_follow_a_reused_url(self):
        with TemporaryDirectory(prefix='dco-reused-url-') as folder:
            root = Path(folder)
            with provider(root / 'worker') as (_, remote), Pool([remote]) as pool, \
                    center(pool, root / 'center') as (state, _):
                node = pool.nodes[0]
                self.assertFalse(node.detached)
                self.assertFalse(node.error)
                old_name = 'retired-provider'
                self.assertNotIn(old_name, [node.name, *node.aliases])
                transport = 'a' * 32
                target = dict(id=transport, cancel=True, url=node.client.url, node=old_name)
                with patch.object(node.client, 'cancel') as cancel, patch.object(node.client, 'delete') as delete:
                    with self.assertRaises(OSError):
                        state._reclaim_retired('b' * 32, {'targets': [target]})
                    cancel.assert_not_called()
                    delete.assert_not_called()
                    del target['node']
                    state._reclaim_retired('c' * 32, {'targets': [target]})
                    cancel.assert_called_once_with(transport)
                    delete.assert_called_once_with(transport)

    def test_retirement_backoff_is_per_original_and_capped(self):
        unavailable, available = 'a' * 32, 'b' * 32
        expected = [0, 5, 15, 35, 75, 155, 315, 615, 915]
        seen = {unavailable: [], available: []}
        now = 0

        def reclaim(identity, _record):
            seen[identity].append(now)
            if identity == unavailable and len(seen[identity]) < len(expected):
                raise OSError('Synthetic unavailable provider')

        def wait(delay):
            nonlocal now
            now += delay

        with TemporaryDirectory(prefix='dco-retirement-backoff-') as folder:
            queue = TransportRetirement(folder, reclaim)
            for identity in seen:
                queue.mark(identity, {'original': identity})
            # Reload from disk to exercise the persisted queue's initial delay.
            queue = TransportRetirement(folder, reclaim)
            with patch('remote_compute.cleanup.time.monotonic', side_effect=lambda: now), \
                    patch.object(queue.condition, 'wait', side_effect=wait):
                queue._run()
            self.assertEqual(seen, {unavailable: expected, available: [0]})
            self.assertEqual(list(Path(folder).glob('*.json')), [])

    def test_missing_result_does_not_retire_original(self):
        with TemporaryDirectory(prefix='dco-unavailable-artifact-') as folder:
            root = Path(folder)
            with provider(root / 'worker') as (_, remote), Pool([remote]) as pool, \
                    center(pool, root / 'center') as (state, client):
                task = python("from pathlib import Path; Path('answer').write_text('paid')",
                              outputs=['answer'])
                original = client.submit(task)['id']
                self.assertEqual(client.wait(original, wait_timeout=15)['status'], 'succeeded')
                self.assertFalse((state.jobs.root / original / 'worker-artifacts.zip').exists())
                with state.condition:
                    state.jobs[original].pop('node', None)
                with patch.object(pool, 'nodes', []):
                    with self.assertRaises(RemoteError):
                        client.fetch(original, delete=True)
                    self.assertEqual(client.status(original)['status'], 'succeeded')
                    self.assertFalse((state.retirement.root / (original + '.json')).exists())
                self.assertEqual(client.fetch(original, delete=False)['files']['answer'], b'paid')

    def test_durable_retirement_parks_across_restart_and_wakes_on_registration(self):
        with TemporaryDirectory(prefix='dco-retirement-registration-') as folder:
            root = Path(folder)
            with provider(root / 'replacement') as (_, replacement), \
                    provider(root / 'original') as (_, original):
                identity, transport = 'd' * 32, 'e' * 32
                name = 'retired-provider'
                with Pool([replacement]) as pool, center(pool, root / 'center') as (state, _):
                    other = pool.nodes[0].client
                    with patch.object(other, 'cancel') as wrong_cancel, patch.object(other, 'delete') as wrong_delete:
                        state.retirement.mark(identity, {'targets': [
                            dict(id=transport, node=name, url=other.url, cancel=True)]})
                        state.retirement.schedule(identity)
                        until(lambda: state.retirement.status() == {'queued': 0, 'awaiting_route': 1})
                        marker = state.retirement.root / (identity + '.json')
                        self.assertTrue(marker.is_file())
                        wrong_cancel.assert_not_called()
                        wrong_delete.assert_not_called()
                with Pool([replacement]) as pool, center(pool, root / 'center') as (state, operator):
                    until(lambda: state.retirement.status() == {'queued': 0, 'awaiting_route': 1})
                    other = pool.nodes[0].client
                    with patch.object(other, 'cancel') as wrong_cancel, patch.object(other, 'delete') as wrong_delete:
                        calls = []
                        real_cancel, real_delete = Client.cancel, Client.delete

                        def cancel(client, key):
                            if client.url == original.url:
                                calls.append(('cancel', key))
                                return {'id': key, 'status': 'cancelled'}
                            return real_cancel(client, key)

                        def delete(client, key):
                            if client.url == original.url:
                                calls.append(('delete', key))
                                return {'deleted': key}
                            return real_delete(client, key)

                        with patch.object(Client, 'cancel', cancel), patch.object(Client, 'delete', delete):
                            operator.control(add=dict(name=name, url=original.url, token=original.token))
                            until(lambda: not marker.exists())
                        self.assertEqual(calls, [('cancel', transport), ('delete', transport)])
                        wrong_cancel.assert_not_called()
                        wrong_delete.assert_not_called()
                    self.assertEqual(state.retirement.status(), {'queued': 0, 'awaiting_route': 0})

    def test_registration_during_failed_lookup_does_not_lose_wakeup(self):
        identity = 'f' * 32
        checked, registered = threading.Event(), threading.Event()
        seen = []

        def reclaim(key, _record):
            seen.append(key)
            if len(seen) == 1:
                checked.set()
                if not registered.wait(5):
                    raise AssertionError('The test did not register the route')
                raise UnregisteredRetirementProvider('Previous lookup saw no route')

        with TemporaryDirectory(prefix='dco-retirement-route-revision-') as folder:
            queue = TransportRetirement(folder, reclaim)
            queue.mark(identity, {})
            marker = queue.root / (identity + '.json')
            try:
                queue.schedule(identity)
                self.assertTrue(checked.wait(5))
                # The route changes while the stale lookup still owns the intent.
                queue.routes_changed()
                registered.set()
                until(lambda: not marker.exists())
                self.assertEqual(seen, [identity, identity])
                self.assertEqual(queue.status(), {'queued': 0, 'awaiting_route': 0})
            finally:
                registered.set()
                queue.close()

    def test_stage_archive_only_sends_one_body_across_concurrent_callers(self):
        first = pack({'input.txt': b'frozen', 'src/tool.py': b'pass'})
        equivalent = pack({'src/tool.py': b'pass', 'input.txt': b'frozen'})
        self.assertEqual(first, equivalent)
        asset = sha256(first).hexdigest()
        with TemporaryDirectory(prefix='dco-stage-archive-') as folder:
            root = Path(folder)
            with provider(root / 'worker') as (_, remote), Pool([remote]) as pool, \
                    center(pool, root / 'center') as (state, operator):
                client = Client(operator.url, state.token, state_dir=root / 'public-client')
                arrivals = threading.Barrier(6, timeout=5)
                calls = []
                original = state.asset

                def observe(identity, body=None, *, job_id=None):
                    calls.append((identity, body, job_id))
                    return original(identity, body, job_id=job_id)

                def stage(identity):
                    arrivals.wait()
                    return client.stage_archive(first, job_id=identity)

                originals = [f'{i:032x}' for i in range(1, 7)]
                with patch.object(state, 'asset', side_effect=observe), ThreadPoolExecutor(max_workers=6) as callers:
                    receipts = list(callers.map(stage, originals, timeout=10))
                    warm = client.stage_archive(equivalent, job_id=originals[0])
                self.assertEqual([receipt['present_before'] for receipt in receipts].count(False), 1)
                self.assertEqual(warm, dict(asset=asset, archive_bytes=len(first), present_before=True))
                self.assertTrue(all(receipt['asset'] == asset and receipt['archive_bytes'] == len(first)
                                    for receipt in receipts))
                bodies = [(identity, body, job_id) for identity, body, job_id in calls if body is not None]
                self.assertEqual(len(bodies), 1)
                self.assertEqual(bodies[0][:2], (asset, first))
                self.assertIn(bodies[0][2], originals)
                self.assertCountEqual([job_id for _, body, job_id in calls if body is None],
                                      originals + [originals[0]])
                self.assertEqual((state.root / 'assets' / (asset + '.zip')).read_bytes(), first)

    def test_collection_workers_only_changes_local_collection_pool(self):
        with TemporaryDirectory(prefix='dco-collection-workers-') as folder:
            root = Path(folder)
            default = Client(state_dir=root / 'default-state')
            configured = Client(state_dir=root / 'configured-state', collection_workers=8)
            self.assertEqual(default.collection_workers, 2)
            for name, client, collection in [('default', default, 2), ('configured', configured, 8)]:
                with self.subTest(name=name):
                    path = root / name
                    path.mkdir()
                    controller = _Controller(path, 'fixture')
                    try:
                        with patch.object(client, 'window', return_value=object()) as window:
                            controller.initialize(client, 19, str(path))
                        window.assert_called_once_with('fixture', ready_tasks=19)
                        self.assertEqual(tuple(pool._max_workers for pool in controller.pools),
                                         (4, 2, collection))
                    finally:
                        controller.close()
            self.assertEqual(default.collection_workers, 2)
