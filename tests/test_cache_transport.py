"""Cache synchronization over real center/worker HTTP and Python processes."""

from contextlib import ExitStack
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

from remote_compute import Pool
from remote_compute.cache_sync import sync_cache
from test_center import center
from test_compute import provider, scratch


class CacheTransportTests(unittest.TestCase):
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
