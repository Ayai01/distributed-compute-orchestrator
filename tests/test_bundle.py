"""Private config publication must not expose credentials through temporary files."""

from contextlib import redirect_stdout
import io
import json
import os
import shutil
from pathlib import Path
import stat
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import zipfile

from remote_compute.bundle import atomic_json,native_path,pack,unpack
from remote_compute import cli
from remote_compute.commands import SchedulerShell
from test_compute import scratch, until


class BundleTests(unittest.TestCase):
    def test_path_members_keep_the_existing_zip_bytes(self):
        with scratch() as directory:
            source = Path(directory) / 'source'
            data = bytes(range(256)) * (9 * 1024**2 // 256 + 1)
            source.write_bytes(data)
            source.chmod(0o755)
            path_mode = stat.S_IMODE(source.stat().st_mode)
            files = {'nested/数据.dat': source, 'stored.npz': source, 'task.json': b'{}'}
            for limit in (zipfile.ZIP64_LIMIT, 2 * 1024**2):
                with self.subTest(zip64_limit=limit), patch.object(zipfile, 'ZIP64_LIMIT', limit):
                    expected = io.BytesIO()
                    with zipfile.ZipFile(expected, 'w') as archive:
                        for name, value in sorted(files.items()):
                            info = zipfile.ZipInfo(name, (1980, 1, 1, 0, 0, 0))
                            info.compress_type = (zipfile.ZIP_STORED if name.endswith('.npz')
                                                  else zipfile.ZIP_DEFLATED)
                            from_file = isinstance(value, Path)
                            mode = path_mode if from_file else 0o644
                            info.external_attr = (stat.S_IFREG | mode) << 16
                            archive.writestr(info, data if from_file else value, compresslevel=1)
                    self.assertEqual(pack(files), expected.getvalue())


class PrivateJsonTests(unittest.TestCase):
    @unittest.skipUnless(os.name=='nt','Windows extended path namespace')
    def test_long_worker_output_is_retained_fetched_and_deleted_by_original_id(self):
        from test_compute import provider
        from remote_compute import command
        with scratch()as directory,provider(Path(directory)/'worker')as(worker,client):
            name='deep/'+'/'.join(['segment-abcdefghijkl']*12)+'/result.bin'
            source="from pathlib import Path;from remote_compute.bundle import native_path;p=native_path("+repr(name)+",force=True);p.parent.mkdir(parents=True);p.write_bytes(b'paid-original');Path('consumed-input').write_text('temporary')"
            identity=client.submit(command(['${tool:python}','-c',source],max_cpus=1,outputs=['deep/**/*'],env={'PYTHONPATH':'${dco}'}))['id']
            client.wait(identity)
            job_root=worker.jobs[identity]['directory']
            receipt=client.fetch(identity,delete=False)
            self.assertEqual(receipt['status'],'succeeded',receipt.get('stderr'))
            self.assertEqual(receipt['id'],identity)
            self.assertEqual(receipt['files'][name],b'paid-original')
            self.assertNotIn('input_cleanup_error',receipt)
            self.assertFalse((job_root/'input/consumed-input').exists())
            self.assertEqual(client.delete(identity), {'deleted': identity})
            self.assertEqual(client.statuses([identity])[identity]['status'],'unknown')
            until(lambda: not native_path(job_root,force=True).exists())

    @unittest.skipUnless(os.name=='nt','Windows extended path namespace')
    def test_long_artifact_and_atomic_state_paths_roundtrip(self):
        with scratch()as directory:
            root=Path(directory)/('a'*80)/('b'*80)/('c'*40)
            try:
                data=pack({'files/scientific-results/'+('d'*64)+'.json.gz':b'paid-original'})
                unpack(data,root)
                target=native_path(root,force=True)/'files/scientific-results'/('d'*64+'.json.gz')
                self.assertGreater(len(str(target)),260)
                self.assertEqual(target.read_bytes(),b'paid-original')
                atomic_json(root/'state.json',{'id':'original'})
                self.assertEqual(json.loads(native_path(root/'state.json').read_bytes())['id'],'original')
            finally:
                if native_path(root,force=True).exists():shutil.rmtree(native_path(root,force=True))

    @unittest.skipUnless(os.name=='nt','Windows extended path namespace')
    def test_fetch_long_artifact_keeps_original_id_and_returns_a_readable_path(self):
        from test_compute import provider
        from remote_compute import command
        with scratch()as directory,provider(Path(directory)/'worker')as(_,client):
            root=Path(directory)/('a'*80)/('b'*80)/('c'*40)
            name='scientific-results/'+('d'*64)+'.json.gz'
            source="from pathlib import Path;p=Path("+repr(name)+");p.parent.mkdir();p.write_bytes(b'paid-original')"
            identity=client.submit(command(['${tool:python}','-c',source],max_cpus=1,outputs=[name]))['id']
            try:
                client.wait(identity)
                with patch.object(client,'submit',side_effect=AssertionError('Paid result must not rerun')):
                    receipt=client.fetch(identity,root,delete=False)
                self.assertEqual(receipt['id'],identity)
                self.assertEqual((Path(receipt['output_dir'])/'files'/name).read_bytes(),b'paid-original')
                client.delete(identity)
            finally:
                if native_path(root,force=True).exists():shutil.rmtree(native_path(root,force=True))

    def test_private_config_uses_exclusive_owner_only_temp_before_replace(self):
        with scratch() as directory:
            root = Path(directory)
            target = root / 'worker.local.json'
            target.write_text('{"cpus":1}', encoding='utf8')
            replace = Path.replace
            published = []

            def publish(pending, destination):
                self.assertEqual(destination, target)
                self.assertEqual(json.loads(target.read_text()), {'cpus': 1})
                self.assertEqual(json.loads(pending.read_text()), {'cpus': 2, 'token': 'test-secret'})
                if os.name != 'nt':
                    self.assertEqual(stat.S_IMODE(pending.stat().st_mode), 0o600)
                published.append(pending)
                return replace(pending, destination)

            with patch('os.open', wraps=os.open) as create, patch.object(Path, 'replace', publish):
                atomic_json(target, {'cpus': 2, 'token': 'test-secret'}, private=True)
            self.assertEqual(len(published), 1)
            self.assertNotEqual(published[0], target.with_suffix('.tmp'))
            self.assertTrue(any(call.args[1] & os.O_EXCL and call.args[2] == 0o600
                                for call in create.call_args_list))
            self.assertEqual(json.loads(target.read_text())['cpus'], 2)
            self.assertEqual(list(root.iterdir()), [target])

    @unittest.skipIf(os.name == 'nt', 'POSIX file modes and umask')
    def test_private_save_repairs_permissive_config_under_permissive_umask(self):
        with scratch() as directory:
            target = Path(directory) / 'worker.local.json'
            target.write_text('{}', encoding='utf8')
            target.chmod(0o664)
            previous = os.umask(0)
            try:
                atomic_json(target, {'token': 'test-secret'}, private=True)
            finally:
                os.umask(previous)
            self.assertEqual(stat.S_IMODE(target.stat().st_mode), 0o600)
            atomic_json(target, {'token': 'test-secret', 'cpus': 2}, private=True)
            self.assertEqual(stat.S_IMODE(target.stat().st_mode), 0o600)

    def test_private_replace_failure_preserves_config_and_cleans_temp(self):
        with scratch() as directory:
            root = Path(directory)
            target = root / 'worker.local.json'
            target.write_bytes(b'{"cpus":1}')
            with patch.object(Path, 'replace', side_effect=OSError('publish failed')):
                with self.assertRaisesRegex(OSError, 'publish failed'):
                    atomic_json(target, {'token': 'test-secret'}, private=True)
            self.assertEqual(target.read_bytes(), b'{"cpus":1}')
            self.assertEqual(list(root.iterdir()), [target])

    def test_invalid_json_leaves_no_private_temp(self):
        with scratch() as directory:
            root = Path(directory)
            with self.assertRaises(ValueError):
                atomic_json(root / 'worker.local.json', {'cpus': float('nan')}, private=True)
            self.assertEqual(list(root.iterdir()), [])

    def test_ordinary_results_flush_unique_temporary_file_before_publication(self):
        with scratch() as directory:
            target = Path(directory) / 'result.json'
            replace = Path.replace
            with patch('remote_compute.bundle.os.fsync', wraps=os.fsync) as flush:
                def publish(pending, destination):
                    flush.assert_called_once()
                    self.assertEqual(json.loads(pending.read_text()), {'answer': 42})
                    return replace(pending, destination)
                with patch.object(Path, 'replace', publish):
                    atomic_json(target, {'answer': 42})
            self.assertEqual(json.loads(target.read_text()), {'answer': 42})
            self.assertEqual(list(Path(directory).iterdir()), [target])

    def test_setup_saves_worker_config_privately_without_starting_service(self):
        with scratch() as directory:
            target = Path(directory) / 'worker.local.json'
            target.write_text('{"cpus":2,"token":"test-secret"}', encoding='utf8')
            with patch('remote_compute.setup_tools.install', return_value={'tools': {}, 'env': {},'python_runtimes':{}}), \
                 patch('remote_compute.cli.atomic_json', wraps=atomic_json) as write, \
                 patch('remote_compute.cli.display'):
                self.assertEqual(cli.main(['--config', str(target), 'setup']), 0)
            self.assertTrue(write.call_args.kwargs['private'])
            self.assertEqual(json.loads(target.read_text())['token'], 'test-secret')

    def test_save_nodes_keeps_token_and_uses_private_publication(self):
        with scratch() as directory:
            root = Path(directory)
            target = root / '.local/nodes.json'
            target.parent.mkdir()
            remote = dict(name='remote', url='https://example.test:26133', weight=1.0,
                          limit=0, memory_limit_mb=0, enabled=True)
            target.write_text(json.dumps({'nodes': [dict(remote, url='https://old.example.test:26133',
                                                        token='test-secret')]}), encoding='utf8')
            shell = SimpleNamespace(pool=SimpleNamespace(nodes=[]),
                                    center=SimpleNamespace(nodes=lambda: dict(policy='adaptive', nodes=[remote]),
                                                           token=''))
            with patch('remote_compute.commands.ROOT', root), \
                 patch('remote_compute.commands.atomic_json', wraps=atomic_json) as write, \
                 redirect_stdout(io.StringIO()):
                SchedulerShell.do_save(shell, '')
            self.assertTrue(write.call_args.kwargs['private'])
            self.assertEqual(write.call_args.args[0], target)
            saved = json.loads(target.read_text())['nodes']
            self.assertEqual(saved, [dict(remote, token='test-secret')])
            self.assertEqual(list(target.parent.iterdir()), [target])
