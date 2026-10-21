"""Cache synchronization over real center/worker HTTP and Python processes."""

from contextlib import ExitStack
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import shutil
import unittest
from unittest.mock import patch

from remote_compute import Pool
from remote_compute.cache_sync import sync_cache
from remote_compute import cache_arh
from test_cache_arh_bench import seed
from test_center import center
from test_compute import provider, scratch


class CacheTransportTests(unittest.TestCase):
    def test_aop_semantic_units_roundtrip_and_repeat_export_nothing(self):
        installed = Path(__file__).resolve().parents[2] / 'AOP_v2'
        if not (installed / 'analog/cache.py').is_file() or not importlib.util.find_spec('numpy'):
            self.skipTest('Installed AOP producer and NumPy runtime')
        with scratch() as directory, ExitStack() as stack:
            root = Path(directory)
            source, target = root / 'source', root / 'target'
            endpoints = []
            for name, project in (('A-source', source), ('B-target', target)):
                (project / 'utils').mkdir(parents=True)
                shutil.copyfile(installed / 'utils/compat.py', project / 'utils/compat.py')
                _, node = stack.enter_context(provider(root / (name + '-worker'), cpus=1,
                    memory_mb=2048, tools={'aop_python': sys.executable}, shared={'aop': str(project)}))
                endpoints.append(dict(name=name, url=node.url))

            def native(project, body):
                code = ("from analog.cache import CornerCache\n"
                        "from config import get_cache_root\n"
                        "cache=CornerCache(get_cache_root(),'science-v1',2,memory_mb=1)\ntry:\n" +
                        '\n'.join('    ' + line for line in body.splitlines()) +
                        '\nfinally:\n    cache.close()\n')
                environment = dict(os.environ, AOP_CACHE_DIR=str(project / '.aop_cache'),
                                   AOP_RECYCLE_DIR=str(root / 'recycled'))
                return subprocess.check_output([sys.executable, '-B', '-c', code], cwd=installed,
                                               env=environment, text=True, stderr=subprocess.STDOUT, timeout=20)

            native(source, "cache.put_many([('d1','tt',[1.,2.],True,None)])")
            pool = Pool(endpoints)
            stack.callback(pool.close)
            _, client = stack.enter_context(center(pool, root / 'center'))
            first = sync_cache(client, 'aop', kind='research')
            self.assertEqual(first['status'], 'succeeded', first)
            self.assertEqual(first['projects']['aop']['research']['added'], 1)
            self.assertEqual(json.loads(native(target,
                "import json\nprint(json.dumps(cache.get('d1','tt').tolist()))")), [1., 2.])
            second = sync_cache(client, 'aop', kind='research')
            self.assertEqual(second['status'], 'succeeded', second)
            self.assertEqual(second['projects']['aop']['research']['exported_records'], 0)
            self.assertEqual(second['pending_jobs'], {})
            self.assertEqual(client.status(), [])

    def test_arh_cache_only_worker_imports_producer_from_normal_source_asset(self):
        installed = Path(__file__).resolve().parents[2] / 'ARH-DSE-v2'
        if not (installed / 'framework/storage.py').is_file():
            self.skipTest('Installed ARH producer fixture')
        with scratch() as directory, ExitStack() as stack:
            root = Path(directory)
            source, target = root/'source', root/'cache-only'
            (source/'framework').mkdir(parents=True)
            target.mkdir()
            (source/'framework/__init__.py').write_text('', encoding='utf-8')
            for name in ('framework/storage.py', 'framework/artifact_store.py', 'python_compat.py'):
                shutil.copyfile(installed/name, source/name)
            (source/'framework/source_snapshot.py').write_text(
                "def files(root):"
                "\n    return {p.relative_to(root).as_posix():p for p in root.rglob('*.py')}\n",
                encoding='utf-8')
            key = seed(source, 'mac', ('native-measurement',), '{"score":7}')
            endpoints = []
            for name, project in (('A-source', source), ('B-cache', target)):
                _, node = stack.enter_context(provider(root/(name+'-worker'), cpus=1,
                    memory_mb=2048, tools={'arh_python': sys.executable}, shared={'arh': str(project)}))
                endpoints.append(dict(name=name, url=node.url))
            pool = Pool(endpoints)
            stack.callback(pool.close)
            _, client = stack.enter_context(center(pool, root/'center'))
            first = sync_cache(client, 'arh', kind='research')
            self.assertEqual(first['status'], 'succeeded', first)
            self.assertIn(key, cache_arh.manifest(target))
            self.assertFalse((target/'framework').exists())
            self.assertTrue((target/'build/evaluations.sqlite3.arh-cache-owner.json').exists())
            second = sync_cache(client, 'arh', kind='research')
            self.assertEqual(second['status'], 'succeeded', second)
            self.assertEqual(second['projects']['arh']['research']['exported_records'], 0)
            self.assertEqual(second['projects']['arh']['producer_assets']['uploaded_bytes'], 0)
            self.assertEqual(client.status(), [])

    def test_manual_union_asset_prewarm_and_repeat_use_existing_transport(self):
        with scratch() as directory, ExitStack() as stack:
            root = Path(directory)
            endpoints = []
            workers = []
            projects = []
            for name in ('first', 'second'):
                project = root / name
                (project / 'reference').mkdir(parents=True)
                (project / 'reference' / (name + '.npz')).write_bytes(name.encode() * 2000)
                (project / 'bench').mkdir()
                (project / 'bench' / '__init__.py').write_text('', encoding='utf-8')
                (project / 'bench' / 'remote.py').write_text(
                    "def _source_assets(client):\n"
                    "    return {'bench':client.sync_files({'code.py':b'print(42)'}),\n"
                    "            'bench_data':client.sync_files({'data.bin':b'data'*10000})}\n",
                    encoding='utf-8')
                worker, node = stack.enter_context(provider(root / (name + '-worker'),
                    cpus=1, memory_mb=2048, tools={'bench_python': sys.executable},
                    shared={'bench': str(project)}))
                workers.append((worker, node))
                projects.append(project)
                endpoints.append(dict(name=name, url=node.url))
            pool = Pool(endpoints)
            stack.callback(pool.close)
            _, client = stack.enter_context(center(pool, root / 'center'))
            with patch.object(client, 'request', wraps=client.request) as requests:
                first = sync_cache(client, 'bench')
                self.assertEqual(first['status'], 'succeeded', first)
                self.assertEqual(first['projects']['bench']['research']['added'], 2)
                self.assertEqual(first['projects']['bench']['assets']['groups'], 2)
                self.assertGreater(first['projects']['bench']['assets']['uploaded_bytes'], 0)
                self.assertEqual(len(first['projects']['bench']['assets']['warmed']), 2)
                self.assertEqual(first['pending_jobs'], {})
                requests.reset_mock()
                second = sync_cache(client, 'bench')
                self.assertEqual(second['status'], 'succeeded', second)
                self.assertEqual(second['projects']['bench']['research']['exported_records'], 0)
                self.assertEqual(second['projects']['bench']['assets']['uploaded_bytes'], 0)
                self.assertFalse(any(call.args[0].startswith('/assets/') and len(call.args) > 1
                                     for call in requests.call_args_list))
            for project in projects:
                self.assertEqual(sorted(path.name for path in (project / 'reference').iterdir()),
                                 ['first.npz', 'second.npz'])
                self.assertFalse((project / 'remote_op').exists())
            self.assertEqual(client.status(), [])
            for _, node in workers:
                self.assertEqual(node.status(), [])
