"""Owned cache defaults, installation reuse and intact state-root relocation."""
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from remote_compute.paths import cache_environment, migrate_worker_state
from remote_compute.setup_tools import _ngspice, reuse_rtl
from remote_compute.worker import Conflict, lock_root


class PathTests(unittest.TestCase):
    def test_valid_shared_ram_marker_needs_no_rewrite_or_writer_privilege(self):
        from remote_compute.ramdisk import _mark_shared,MARKER
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);_mark_shared(root,8*1024**3)
            before=(root/MARKER).read_bytes()
            with patch.object(Path,'write_text',side_effect=PermissionError('reader token')):
                _mark_shared(root,8*1024**3)
                with self.assertRaises(PermissionError):_mark_shared(root,16*1024**3)
            self.assertEqual((root/MARKER).read_bytes(),before)

    def test_execution_derives_cache_paths_after_task_environment_override(self):
        from test_compute import provider, python
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);cache=root/'DCO/.local/cache';model=root/'installed-model'
            with provider(root/'worker',cache_root=cache,env={'HF_HOME':str(root/'device-model')}) as (_,client):
                code="import os,json;print(json.dumps({k:os.environ[k] for k in ('HF_HOME','HUGGINGFACE_HUB_CACHE','NUMBA_CACHE_DIR')}))"
                result=client.run(python(code,env={'HF_HOME':str(model)}))
            value=json.loads(result['stdout'])
            self.assertEqual(value['HF_HOME'],str(model))
            self.assertEqual(value['HUGGINGFACE_HUB_CACHE'],str(model/'hub'))
            self.assertEqual(value['NUMBA_CACHE_DIR'],str(cache/'numba'))

    def test_new_caches_are_managed_and_existing_dependencies_are_reused(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);home=root/'home';cache=root/'DCO/.local/cache'
            installed=home/'.cache/huggingface'
            installed.mkdir(parents=True);(installed/'model').write_bytes(b'installed')
            env={'LOCALAPPDATA':str(home/'local')}
            with patch('remote_compute.paths.Path.home',return_value=home):
                result=cache_environment(env,cache)
            self.assertEqual(result['HF_HOME'],str(installed))
            self.assertEqual(result['NUMBA_CACHE_DIR'],str(cache/'numba'))
            self.assertEqual(result['COURSIER_CACHE'],str(cache/'coursier'))
            self.assertEqual(result['XDG_CACHE_HOME'],str(cache))
            self.assertEqual(env,{'LOCALAPPDATA':str(home/'local')})
            self.assertFalse((cache/'huggingface').exists())

    def test_explicit_paths_keep_priority(self):
        with tempfile.TemporaryDirectory() as folder:
            explicit={'COURSIER_CACHE':'installed-jars','NUMBA_CACHE_DIR':'explicit-jit','HF_HOME':'model-home'}
            result=cache_environment(explicit,folder)
            for key,value in explicit.items():self.assertEqual(result[key],value)

    def test_state_move_preserves_original_receipt_bytes_and_is_resumable(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);old=root/'old-worker';new=root/'DCO/.local/state/worker'
            (old/'jobs/original').mkdir(parents=True)
            raw=b'{"id":"original","cpu_seconds":12.5}'
            (old/'jobs/original/result.json').write_bytes(raw)
            with lock_root(old,'worker'):pass
            migrate_worker_state(old,new,root/'DCO')
            self.assertEqual((new/'jobs/original/result.json').read_bytes(),raw)
            self.assertFalse(old.exists())
            migrate_worker_state(old,new,root/'DCO')

    def test_active_worker_and_unmanaged_destination_are_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);old=root/'old';(old/'jobs').mkdir(parents=True)
            with lock_root(old,'worker'):
                with self.assertRaises(Conflict):
                    migrate_worker_state(old,root/'DCO/.local/state/worker',root/'DCO')
            with self.assertRaises(ValueError):
                migrate_worker_state(old,root/'unmanaged',root/'DCO')
            self.assertTrue(old.exists())

    def test_working_external_ngspice_is_not_copied_or_modified(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);external=root/'installed';exe=external/'bin/ngspice.exe'
            exe.parent.mkdir(parents=True);exe.write_bytes(b'installed')
            destination=root/'DCO/.tools/ngspice'
            with patch('remote_compute.setup_tools._run') as run:
                run.return_value.stdout='ngspice-47\n'
                selected,info=_ngspice(destination,root,external)
            self.assertEqual(selected,exe)
            self.assertTrue(info['reused'])
            self.assertFalse(destination.exists())
            self.assertEqual(exe.read_bytes(),b'installed')

    def test_external_msys_packages_are_referenced_without_a_second_installation(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);external=root/'installed-msys';external.mkdir()
            with patch('remote_compute.setup_tools.rtl_files',return_value=(['bin/tool'],{'gcc':'known'})):
                result=reuse_rtl(external,root/'DCO')
            self.assertEqual(result['env']['MSYS2_ROOT'],external.as_posix())
            self.assertEqual(result['installed_mib'],0)
            self.assertFalse((root/'DCO/.tools/msys64').exists())
            self.assertEqual(list(external.iterdir()),[])


if __name__=='__main__':
    unittest.main()
