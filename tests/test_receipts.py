"""Crash recovery preserves original identities without re-executing jobs."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import redirect_stderr
import io
import json
from pathlib import Path
import threading
import unittest
from unittest.mock import patch
import zipfile

from remote_compute.bundle import atomic_json, task_bundle
from remote_compute.commands import read_json
from remote_compute.receipts import read_receipt
from remote_compute.records import JobRecords, asset_references
from remote_compute.worker import Conflict
from remote_compute import cli
from test_compute import provider, scratch, python


class ReceiptTests(unittest.TestCase):
    def test_concurrent_writers_do_not_share_a_temporary_file(self):
        with scratch() as folder:
            path = Path(folder) / 'result.json'
            atomic_json(path, {'old': True})
            barrier = threading.Barrier(2)
            replace = Path.replace
            names = []
            def publish(pending, destination):
                if pending not in names:
                    names.append(pending)
                    barrier.wait(timeout=3)
                return replace(pending, destination)
            with patch.object(Path, 'replace', publish), ThreadPoolExecutor(2) as pool:
                list(pool.map(lambda i: atomic_json(path, {'writer': i, 'payload': 'x' * 10000}), range(2)))
            self.assertEqual(len(set(names)), 2)
            self.assertIn(read_json(path)['writer'], (0, 1))
            self.assertEqual(list(Path(folder).iterdir()), [path])

    def test_failed_flush_keeps_previous_document_and_cleans_unpublished_temp(self):
        with scratch() as folder:
            path = Path(folder) / 'result.json'
            path.write_bytes(b'{"old":true}')
            with patch('remote_compute.bundle.os.fsync', side_effect=OSError('disk full')):
                with self.assertRaisesRegex(OSError, 'disk full'):
                    atomic_json(path, {'new': True})
            self.assertEqual(path.read_bytes(), b'{"old":true}')
            self.assertEqual(list(Path(folder).iterdir()), [path])

    def test_brief_windows_reader_conflict_retries_only_replacement(self):
        with scratch() as folder:
            path = Path(folder) / 'result.json'
            path.write_text('{}')
            replace = Path.replace
            attempts = []
            def publish(pending, destination):
                attempts.append(pending)
                if len(attempts) < 3:
                    raise PermissionError('reader holds old receipt')
                return replace(pending, destination)
            with patch.object(Path, 'replace', publish), patch('remote_compute.bundle.time.sleep'):
                atomic_json(path, {'answer': 42})
            self.assertEqual(len(attempts), 3)
            self.assertEqual(len(set(attempts)), 1)
            self.assertEqual(read_json(path), {'answer': 42})

    def test_completed_temporary_receipt_recovers_original_measurements(self):
        with scratch() as folder:
            job = Path(folder) / ('1' * 32)
            job.mkdir()
            (job / 'result.json').write_bytes(b'')
            receipt = {'id': job.name, 'status': 'succeeded', 'cpu_seconds': 123.25,
                       'request_sha256': 'a' * 64, 'assets': {}}
            (job / 'result.tmp').write_text(json.dumps(receipt))
            value = read_receipt(job / 'result.json')
            self.assertEqual(value['status'], 'succeeded')
            self.assertEqual(value['cpu_seconds'], 123.25)
            self.assertNotIn('cost_incomplete', value)
            self.assertEqual(value['receipt_recovery']['recovered_from'], 'result.tmp')
            self.assertEqual((job / value['receipt_recovery']['damaged_file']).read_bytes(), b'')
            self.assertEqual(read_receipt(job / 'result.json'), value)
            self.assertEqual(len(list(job.glob('result.corrupt-*.json'))), 1)

    def test_five_damaged_receipts_allow_worker_and_second_restart(self):
        with scratch() as folder:
            root = Path(folder)
            ids = ['%032x' % i for i in range(1, 6)]
            rows = []
            for identity in ids:
                job = root / 'jobs' / identity
                (job / 'input').mkdir(parents=True)
                (job / 'tmp').mkdir()
                (job / 'input/original.ckpt').write_bytes(b'original checkpoint')
                (job / 'input/answer.txt').write_bytes(b'partial output')
                (job / 'tmp/forensic.txt').write_bytes(b'crash evidence')
                (job / 'result.json').write_bytes(b'')
                atomic_json(job / 'task.json', {'label': 'RGCN original', 'project': 'RGCN',
                            'steps': [['original-command']], 'outputs': ['answer.txt'],
                            'assets': {'model': 'a' * 64}, 'cpus': 1, 'memory_mb': 4096})
                rows.append({'id': identity, 'terminal': False, 'assets': ['a' * 64]})
            (root / 'history.index.jsonl').write_text('null\n[]\n{torn\n' +
                '\n'.join(json.dumps(row) for row in rows) + '\n', encoding='utf-8')
            for restart in range(2):
                with provider(root) as (worker, client):
                    self.assertEqual(list(worker.jobs.active), [])
                    self.assertEqual(set(worker.jobs), set(ids))
                    self.assertEqual(asset_references(worker.jobs), {'a' * 64})
                    for identity in ids:
                        receipt = client.status(identity)
                        self.assertEqual(receipt['status'], 'failed')
                        self.assertEqual(receipt['error_kind'], 'receipt_corrupt')
                        self.assertIsNone(receipt['cpu_seconds'])
                        self.assertTrue(receipt['cost_incomplete'])
                        job = root / 'jobs' / identity
                        self.assertEqual((job / 'input/original.ckpt').read_bytes(), b'original checkpoint')
                        self.assertEqual((job / 'tmp/forensic.txt').read_bytes(), b'crash evidence')
                        self.assertEqual(len(list(job.glob('result.corrupt-*.json'))), 1)
                        with zipfile.ZipFile(io.BytesIO(worker.artifacts(identity))) as archive:
                            self.assertEqual(archive.read('files/answer.txt'), b'partial output')
                            self.assertIn('recovery/receipt-recovery.json', archive.namelist())
                    self.assertEqual(client.run(python("print('worker remains usable')"))['status'], 'succeeded')
                    # Remove only this isolated test job, keeping the original IDs.
                    for identity in set(worker.jobs) - set(ids):
                        worker.delete(identity)

    def test_admission_record_retains_original_request_hash_after_later_corruption(self):
        with scratch() as folder:
            root = Path(folder)
            with provider(root) as (worker, client):
                result = client.run(python("print('original')"), keep=True)
                identity = result['id']
                directory = root / 'jobs' / identity
                admission = read_json(directory / 'admission.json')
                self.assertEqual(admission['request_sha256'], result['request_sha256'])
                (directory / 'result.json').write_bytes(b'\x00\x00')
            with provider(root) as (worker, client):
                value = client.status(identity)
                self.assertEqual(value['request_sha256'], result['request_sha256'])
                self.assertIsNone(value['cpu_seconds'])
                with self.assertRaises(Conflict):
                    worker.submit(identity, b'different input')

    def test_missing_task_uses_index_assets_and_reserves_original_id(self):
        with scratch() as folder:
            root = Path(folder)
            identity = 'a' * 32
            atomic_json(root / 'history.index.jsonl', {})
            (root / 'history.index.jsonl').write_text(json.dumps(
                {'id': identity, 'terminal': False, 'assets': ['b' * 64]}) + '\n')
            with provider(root) as (worker, client):
                self.assertEqual(set(worker.jobs), {identity})
                self.assertEqual(asset_references(worker.jobs), {'b' * 64})
                self.assertTrue(client.status(identity)['cost_incomplete'])

    def test_config_error_names_exact_file_and_location(self):
        with scratch() as folder:
            path = Path(folder) / 'worker.local.json'
            path.write_bytes(b'')
            with self.assertRaisesRegex(ValueError, r'worker.local.json.*line 1 column 1'):
                read_json(path)

    def test_unindexed_original_directory_is_never_deleted_by_resubmission(self):
        with scratch() as folder, provider(Path(folder)) as (worker, _client):
            identity = 'c' * 32
            directory = Path(folder) / 'jobs' / identity
            directory.mkdir()
            original = directory / 'original.ckpt'
            original.write_bytes(b'paid checkpoint')
            with self.assertRaisesRegex(Conflict, 'Original job directory already exists'):
                worker.submit(identity, task_bundle(python("print('do not execute')")))
            self.assertEqual(original.read_bytes(), b'paid checkpoint')

    def test_startup_diagnostic_keeps_traceback_without_redirecting_service_output(self):
        with scratch() as folder:
            root = Path(folder)
            config = root / 'worker.local.json'
            config.write_bytes(b'')
            with patch.object(cli, 'ROOT', root), patch.object(cli, 'require_visible'), redirect_stderr(io.StringIO()):
                code = cli.main(['--mode', 'worker', '--config', str(config), '--no-dashboard'])
            self.assertEqual(code, 2)
            diagnostic = (root / '.local/diagnostics/startup-error.log').read_text(encoding='utf-8')
            self.assertIn('Traceback', diagnostic)
            self.assertIn('worker.local.json', diagnostic)


if __name__ == '__main__':
    unittest.main()
