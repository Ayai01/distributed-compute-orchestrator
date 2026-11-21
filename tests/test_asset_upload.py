"""Regression coverage for checkpointed immutable asset uploads."""

from hashlib import sha256
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
import json
from pathlib import Path
from random import Random
import socket
from tempfile import TemporaryDirectory
import threading
import unittest
from unittest.mock import patch
from zipfile import ZipFile

from remote_compute import Client, Pool, RemoteError
from remote_compute.asset_upload import ASSET_CHUNK_BYTES, ASSET_RESUME_CAPABILITY, ASSET_RESUME_MIN_BYTES, AssetUploads
from remote_compute.bundle import pack
from remote_compute.shared_assets import SharedAssets
from test_center import center
from test_compute import provider, python


class AssetUploadTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.contents = Random(1026).randbytes(2 * ASSET_CHUNK_BYTES + 137)
        cls.payload = pack({'model.bin': cls.contents})
        cls.identity = sha256(cls.payload).hexdigest()
        cls.total = len(cls.payload)

    def _chunk(self, receiver, start):
        stop = min(start + ASSET_CHUNK_BYTES, self.total)
        return receiver.receive(self.identity, self.payload[start:stop],
                                f'bytes {start}-{stop - 1}/{self.total}')

    def _commit(self, receiver):
        return receiver.receive(self.identity, json.dumps({'total': self.total}).encode())

    def _check_published(self, root):
        # An existing consumer uses the ordinary shared-cache API.
        payload = SharedAssets(root).read(self.identity)
        self.assertIsNotNone(payload)
        self.assertEqual(self.identity, sha256(payload).hexdigest())
        self.assertEqual(self.payload, payload)
        with ZipFile(io.BytesIO(payload)) as archive:
            self.assertIsNone(archive.testzip())
            self.assertEqual(self.contents, archive.read('model.bin'))

    def test_shared_receiver_recovers_confirmed_offset_and_overwrites_unconfirmed_tail(self):
        with TemporaryDirectory(prefix='dco-asset-recovery-') as temporary:
            root = Path(temporary)
            first = AssetUploads(root)
            second = AssetUploads(root)
            checkpoint = self._chunk(first, 0)
            self.assertEqual(checkpoint['offset'], ASSET_CHUNK_BYTES)
            self.assertEqual(second.receive(self.identity), checkpoint)
            self.assertIsNone(SharedAssets(root).read(self.identity))

            part = root / ('.upload-' + self.identity)
            with part.open('ab') as stream:
                # The process wrote bytes then died before publishing the new checkpoint.
                stream.write(b'!' * (ASSET_CHUNK_BYTES + 3456))

            self.assertEqual(second.receive(self.identity)['offset'], ASSET_CHUNK_BYTES)
            self.assertEqual(self._chunk(second, ASSET_CHUNK_BYTES)['offset'], 2 * ASSET_CHUNK_BYTES)
            self.assertEqual(part.stat().st_size, 2 * ASSET_CHUNK_BYTES)
            self.assertEqual(self._chunk(first, 2 * ASSET_CHUNK_BYTES)['offset'], self.total)
            self.assertEqual(self._commit(second),
                             dict(id=self.identity, total=self.total, offset=self.total, complete=True))
            self.assertEqual(first.receive(self.identity), self._commit(first))
            self.assertFalse(part.exists())
            self.assertFalse(part.with_name(part.name + '_meta').exists())
            self._check_published(root)

    def test_client_resumes_from_receiver_checkpoint_after_lost_chunk_reply(self):
        with TemporaryDirectory(prefix='dco-asset-reply-loss-') as temporary:
            root = Path(temporary)
            archive = root / 'source.zip'
            archive.write_bytes(self.payload)
            shared = root / 'shared'
            shared.mkdir(mode=0o700)
            reader = AssetUploads(shared)
            protocol = []
            endpoint = f'/assets/{self.identity}/upload'

            class Handler(BaseHTTPRequestHandler):
                protocol_version = 'HTTP/1.1'

                def log_message(self, *_):
                    pass

                def respond(self, body=None):
                    if self.path != endpoint:
                        self.send_error(404)
                        return
                    content_range = self.headers.get('Content-Range')
                    result = reader.receive(endpoint.split('/')[2], body, content_range)
                    protocol.append((self.command, content_range, result['offset'], result['complete']))
                    if content_range and content_range.startswith('bytes 0-'):
                        # Commit to disk, then simulate losing the entire HTTP reply.
                        self.close_connection = True
                        self.connection.shutdown(socket.SHUT_RDWR)
                        return
                    payload = json.dumps(result).encode()
                    self.send_response(200)
                    self.send_header('Content-Length', str(len(payload)))
                    self.send_header('Connection', 'close')
                    self.end_headers()
                    self.wfile.write(payload)

                def do_GET(self):
                    self.respond()

                def do_POST(self):
                    self.respond(self.rfile.read(int(self.headers['Content-Length'])))

            with ThreadingHTTPServer(('127.0.0.1', 0), Handler) as server:
                thread = threading.Thread(target=server.serve_forever, kwargs={'poll_interval': .05})
                thread.start()
                try:
                    client = Client(f'http://127.0.0.1:{server.server_port}', timeout=3)
                    metrics = {}
                    client.upload_resumable(self.identity, archive, report=metrics)
                finally:
                    server.shutdown()
                    thread.join(timeout=3)

            starts = range(0, self.total, ASSET_CHUNK_BYTES)
            self.assertEqual([entry[1] for entry in protocol if entry[1]],
                             [f'bytes {start}-{min(start + ASSET_CHUNK_BYTES, self.total) - 1}/{self.total}'
                              for start in starts])
            first_chunk = next(index for index, entry in enumerate(protocol) if entry[1])
            self.assertEqual(protocol[first_chunk + 1], ('GET', None, ASSET_CHUNK_BYTES, False))
            self.assertEqual([entry for entry in protocol if entry[0] == 'POST' and entry[1] is None],
                             [('POST', None, self.total, True)])
            self.assertEqual(metrics["attempted_bytes"], self.total)
            self.assertEqual(metrics["confirmed_bytes"], self.total)
            self.assertEqual(metrics["reconciliations"], 1)
            self.assertEqual(metrics["resumed_from_bytes"], ASSET_CHUNK_BYTES)
            self._check_published(shared)

    def test_center_uploads_through_shared_receiver_for_selected_legacy_worker(self):
        with TemporaryDirectory(prefix='dco-asset-mixed-workers-') as temporary:
            root = Path(temporary)
            options = {'env': {'DCO_SHARED_ASSET_CACHE': str(root / 'shared')}}
            with provider(root / 'legacy', **options) as (legacy, legacy_client), \
                    provider(root / 'receiver', **options) as (receiver, receiver_client):
                legacy_health = legacy.health

                def legacy_capabilities(**options):
                    health = legacy_health(**options)
                    return dict(health, capabilities=[capability for capability in health['capabilities']
                                                      if capability != ASSET_RESUME_CAPABILITY])

                workers = [{'name': 'legacy', 'client': legacy_client},
                           {'name': 'receiver', 'client': receiver_client}]
                with patch.object(legacy, 'health', side_effect=legacy_capabilities), \
                        patch.object(legacy, 'asset', wraps=legacy.asset) as reads, \
                        patch.object(legacy.shared_assets.uploads, 'receive',
                                     side_effect=AssertionError('Legacy upload endpoint must not be used')), \
                        patch.object(receiver.shared_assets.uploads, 'receive',
                                     wraps=receiver.shared_assets.uploads.receive) as uploads, \
                        patch('remote_compute.coordinator.ASSET_RESUME_MIN_BYTES', 1), \
                        Pool(workers) as pool, center(pool, root / 'center') as (_, operator):
                    operator.request('/assets/' + self.identity, self.payload)
                    task = python(
                        'import hashlib, pathlib, sys; '
                        'print(hashlib.sha256(pathlib.Path(sys.argv[1]).read_bytes()).hexdigest())',
                        node='legacy', assets={'model': self.identity})
                    task['steps'][0].append('${asset:model}/model.bin')
                    original = '1' * 32
                    self.assertEqual(operator.submit(task, job_id=original)['id'], original)
                    operator.wait(original, wait_timeout=15)
                    result = operator.fetch(original, delete=False)

                self.assertEqual(result['id'], original)
                self.assertEqual(result['status'], 'succeeded')
                self.assertEqual(result['assigned_node'], 'legacy')
                self.assertEqual(result['stdout'].strip(), sha256(self.contents).hexdigest())
                self.assertTrue(reads.call_args_list)
                self.assertTrue(all(call.args[0] == self.identity and call.args[1] is None
                                    for call in reads.call_args_list))
                ranges = [call.args[2] for call in uploads.call_args_list if call.args[2] is not None]
                self.assertEqual(ranges,
                                 [f'bytes {start}-{min(start + ASSET_CHUNK_BYTES, self.total) - 1}/{self.total}'
                                  for start in range(0, self.total, ASSET_CHUNK_BYTES)])
                commits = [call for call in uploads.call_args_list
                           if call.args[1] is not None and call.args[2] is None]
                self.assertEqual(len(commits), 1)
                self.assertEqual(json.loads(commits[0].args[1]), {'total': self.total})
                self._check_published(root / 'shared')

    def test_center_switches_receivers_without_replaying_the_job_or_confirmed_prefix(self):
        contents = Random(1028).randbytes(max(ASSET_RESUME_MIN_BYTES, 2 * ASSET_CHUNK_BYTES) + 137)
        payload = pack({'model.bin': contents})
        identity = sha256(payload).hexdigest()
        endpoint = f'/assets/{identity}/upload'
        with TemporaryDirectory(prefix='dco-asset-failover-') as temporary:
            root = Path(temporary)
            options = {'env': {'DCO_SHARED_ASSET_CACHE': str(root / 'shared')}}
            with provider(root / 'compute', **options) as (compute, compute_client), \
                    provider(root / 'primary', **options) as (_, primary), \
                    provider(root / 'backup', **options) as (_, backup):
                original_health = compute.health
                primary_request, backup_request = primary.request, backup.request
                primary_ranges, backup_ranges, backup_queries = [], [], []

                def legacy_health(**options):
                    health = original_health(**options)
                    return dict(health, capabilities=[capability for capability in health['capabilities']
                                                      if capability != ASSET_RESUME_CAPABILITY])

                def disconnect_primary(path, data=None, **options):
                    if path != endpoint:
                        return primary_request(path, data, **options)
                    if len(primary_ranges) == 2:
                        self.fail('Primary receiver was retried before its available peer')
                    receipt = primary_request(path, data, **options)
                    if content_range := (options.get('extra_headers') or {}).get('Content-Range'):
                        primary_ranges.append(content_range)
                        if len(primary_ranges) == 2:
                            error = RemoteError('Primary receiver disconnected after persisting the chunk')
                            error.transport_retryable = True
                            raise error
                    return receipt

                def observe_backup(path, data=None, **options):
                    receipt = backup_request(path, data, **options)
                    if path == endpoint and data is None:
                        backup_queries.append(receipt['offset'])
                        if len(backup_queries) == 1:
                            return dict(receipt, offset=0, total=None)  # Simulate delayed shared-volume visibility.
                    if path == endpoint and (content_range := (options.get('extra_headers') or {}).get('Content-Range')):
                        backup_ranges.append(content_range)
                    return receipt

                workers = [{'name': name, 'client': connection} for name, connection in
                           [('compute', compute_client), ('primary', primary), ('backup', backup)]]
                with patch.object(compute, 'health', side_effect=legacy_health), \
                        patch.object(primary, 'request', side_effect=disconnect_primary), \
                        patch.object(backup, 'request', side_effect=observe_backup), \
                        patch.object(compute_client, 'submit_bundle', wraps=compute_client.submit_bundle) as dispatch, \
                        Pool(workers) as pool, center(pool, root / 'center') as (_, operator):
                    operator.request('/assets/' + identity, payload)
                    task = python(
                        'import hashlib, pathlib, sys; '
                        'print(hashlib.sha256(pathlib.Path(sys.argv[1]).read_bytes()).hexdigest())',
                        node='compute', assets={'model': identity})
                    task['steps'][0].append('${asset:model}/model.bin')
                    original = '2' * 32
                    self.assertEqual(operator.submit(task, job_id=original)['id'], original)
                    operator.wait(original, wait_timeout=20)
                    result = operator.fetch(original, delete=False)

                self.assertEqual(backup_queries, [2 * ASSET_CHUNK_BYTES] * 2)
                self.assertEqual(backup_ranges[0].split('-', 1)[0], f'bytes {backup_queries[0]}')
                dispatch.assert_called_once()
                self.assertEqual(dispatch.call_args.args[0], original)
                self.assertEqual((result['id'], result['status'], result['assigned_node']),
                                 (original, 'succeeded', 'compute'))
                self.assertEqual(result['stdout'].strip(), sha256(contents).hexdigest())
                self.assertGreaterEqual(result['cpu_seconds'], 0)
                metrics = result['asset_transport']['uploads'][identity]
                self.assertEqual(metrics['receiver_failovers'], 1)
                self.assertEqual(metrics['attempted_bytes'], len(payload))
                self.assertEqual(metrics['confirmed_bytes'], len(payload))
                self.assertEqual(metrics['resumed_from_bytes'], 2 * ASSET_CHUNK_BYTES)
