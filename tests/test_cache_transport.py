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
from test_cache_arh_bench import arh as cache_arh, dcbfs, snapshot, PROJECTS
from test_cache_arh_bench import seed
from test_center import center
from test_compute import provider, scratch


def register(root, stack, project, source, adapter, **options):
    path = root/'projects.json'
    path.write_text(json.dumps({'projects':{project:dict(root=str(source),adapter=adapter,
                                                       requires=['python'],**options)}}))
    stack.enter_context(patch.dict(os.environ, {'DCO_PROJECTS_FILE':str(path)}))


class CacheTransportTests(unittest.TestCase):
    def test_aop_semantic_units_roundtrip_and_repeat_export_nothing(self):
        installed = Path(os.environ.get('REMOTE_COMPUTE_TEST_PROJECTS_ROOT',Path(__file__).resolve().parents[2])) / 'AOP_v2'
        if not (installed / 'analog/cache.py').is_file() or not importlib.util.find_spec('numpy'):
            self.skipTest('Installed AOP producer and NumPy runtime')
        with scratch() as directory, ExitStack() as stack:
            root = Path(directory)
            source, target = root / 'source', root / 'target'
            register(root, stack, 'aop', installed, 'compute/cache_exchange.py')
            endpoints = []
            for name, project in (('A-source', source), ('B-target', target)):
                project.mkdir()
                _, node = stack.enter_context(provider(root / (name + '-worker'), cpus=1,
                    memory_mb=2048, tools={'python': sys.executable}, shared={'aop': str(project)}))
                endpoints.append(dict(name=name, url=node.url, token=node.token))

            def native(project, body):
                code = ("from analog.cache import CornerCache\n"
                        "from config import get_cache_root\n"
                        "cache=CornerCache(get_cache_root(),'science-v1',2,memory_mb=1,environment="
                        "dict(engine_family='ngspice',major_version=47,execution_protocol='ng47-transistor-v1',"
                        "measurement_protocol='measurements-v1',tolerance_profile='ng47-numeric-v1'))\ntry:\n" +
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
        installed = Path(os.environ.get('REMOTE_COMPUTE_TEST_PROJECTS_ROOT',Path(__file__).resolve().parents[2])) / 'ARH-DSE-v3'
        if not (installed / 'framework/storage.py').is_file():
            self.skipTest('Installed ARH producer fixture')
        with scratch() as directory, ExitStack() as stack:
            root = Path(directory)
            source, target = root/'source', root/'cache-only'
            (source/'framework').mkdir(parents=True)
            register(root, stack, 'arh', source, 'framework/cache_exchange.py', producer_group='arh_source')
            target.mkdir()
            (source/'framework/__init__.py').write_text('', encoding='utf-8')
            for name in ('framework/storage.py', 'framework/artifact_store.py', 'python_compat.py', 'framework/cache_exchange.py'):
                shutil.copyfile(installed/name, source/name)
            (source/'framework/source_snapshot.py').write_text(
                "def files(root):"
                "\n    return {p.relative_to(root).as_posix():p for p in root.rglob('*.py')}\n",
                encoding='utf-8')
            key = seed(source, 'mac', ('native-measurement',), '{"score":7}')
            endpoints = []
            for name, project in (('A-source', source), ('B-cache', target)):
                _, node = stack.enter_context(provider(root/(name+'-worker'), cpus=1,
                    memory_mb=2048, tools={'python': sys.executable}, shared={'arh': str(project)}))
                endpoints.append(dict(name=name, url=node.url, token=node.token))
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

    def test_registered_dcbfs_cache_only_roots_union_and_prewarm_by_hash(self):
        with scratch() as directory, ExitStack() as stack:
            root = Path(directory)
            source = root/'source'
            (source/'src/dcbfs/_runtime').mkdir(parents=True)
            (source/'tools').mkdir()
            shutil.copyfile(PROJECTS/'DCBFS-MO/src/dcbfs/_runtime/cache_exchange.py',
                            source/'src/dcbfs/_runtime/cache_exchange.py')
            (source/'tools/compute.py').write_text(
                "def files():\n    return {'code.py':b'print(42)','data.bin':b'original'*10000}\n")
            register(root, stack, 'dcbfs', source, 'src/dcbfs/_runtime/cache_exchange.py')
            endpoints, workers, projects, keys = [], [], [], []
            for name in ('first', 'second'):
                project = root/name
                keys.append(snapshot(project, name, name.encode()*2000))
                worker, node = stack.enter_context(provider(root/(name+'-worker'), cpus=1,
                    memory_mb=2048, tools={'python':sys.executable}, shared={'dcbfs':str(project)}))
                workers.append((worker,node))
                projects.append(project)
                endpoints.append(dict(name=name,url=node.url,token=node.token))
            pool = Pool(endpoints)
            stack.callback(pool.close)
            _, client = stack.enter_context(center(pool,root/'center'))
            with patch.object(client, 'request', wraps=client.request) as requests:
                first = sync_cache(client, 'dcbfs')
                self.assertEqual(first['status'], 'succeeded', first)
                self.assertEqual(first['projects']['dcbfs']['research']['added'], 2)
                self.assertEqual(first['projects']['dcbfs']['assets']['groups'], 1)
                self.assertGreater(first['projects']['dcbfs']['assets']['uploaded_bytes'], 0)
                self.assertEqual(len(first['projects']['dcbfs']['assets']['warmed']), 2)
                requests.reset_mock()
                second = sync_cache(client, 'dcbfs')
                self.assertEqual(second['status'], 'succeeded', second)
                self.assertEqual(second['projects']['dcbfs']['research']['exported_records'], 0)
                self.assertEqual(second['projects']['dcbfs']['assets']['uploaded_bytes'], 0)
                self.assertFalse(any(call.args[0].startswith('/assets/') and len(call.args)>1
                                     for call in requests.call_args_list))
            for project in projects:
                self.assertEqual(set(dcbfs.manifest(project)), set(keys))
                self.assertFalse((project/'src').exists())
            self.assertEqual(client.status(), [])
            for _, node in workers:
                self.assertEqual(node.status(), [])
