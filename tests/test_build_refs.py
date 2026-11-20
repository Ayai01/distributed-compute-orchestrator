"""Measure portable output reuse, short dispatch and checkpoint accounting over HTTP."""

from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import statistics
import sys
import time
import unittest
from unittest.mock import patch

from remote_compute import Client, Pool, RemoteError
from test_center import center
from test_compute import provider, python, scratch


PROJECT = 'portable-output-test'
RECIPE = 'a' * 64


def execution_provider(root, cpus=1):
    return provider(root, cpus=cpus, tags=['ml'], python_runtimes={'ml': [sys.executable]})


def record(name, result):
    path = os.environ.get('DCO_ACCEPTANCE_RECEIPTS')
    if path:
        (Path(path) / (name + '.json')).write_text(json.dumps(result, indent=2) + '\n', encoding='utf-8')


class BuildReferenceTests(unittest.TestCase):
    def test_same_build_coalesces_and_eight_consumers_share_one_upload(self):
        with scratch() as folder:
            root = Path(folder)
            with execution_provider(root / 'one', cpus=8) as (a, first), execution_provider(root / 'two', cpus=8) as (b, second):
                with Pool([dict(name='one', client=first), dict(name='two', client=second)], policy='round_robin') as pool:
                    with center(pool, root / 'center') as (state, operator), ThreadPoolExecutor(8) as callers:
                        client = Client(operator.url, operator.token)
                        producer = python("import py_compile;py_compile.compile('compiled.py','compiled.pyc',doraise=True)",
                                          project=PROJECT, outputs=['compiled.pyc'],
                                          produces_artifact_ref=dict(paths=['compiled.pyc'], recipe_sha256=RECIPE),
                                          step_phases=['compile'])
                        submit = lambda _: client.submit(producer, files={'compiled.py': b'print(441)\n'})['id']
                        with patch.object(a, 'submit', wraps=a.submit) as first_submit, patch.object(b, 'submit', wraps=b.submit) as second_submit:
                            originals = set(callers.map(submit, range(8)))
                            self.assertEqual(len(originals), 1)
                            original = originals.pop()
                            built = client.wait(original, wait_timeout=9)
                            self.assertEqual(built['status'], 'succeeded', built)
                            self.assertEqual(first_submit.call_count + second_submit.call_count, 1)
                        ref = built['artifact_ref']
                        duplicate = client.publish_reference(original, project=PROJECT, paths=['compiled.pyc'], recipe_sha256=RECIPE)
                        self.assertEqual(ref, duplicate['id'])
                        client.delete(original)
                        consumer = python(
                            "import os,runpy,pathlib;runpy.run_path(str(pathlib.Path(os.environ['BUILD'])/'compiled.pyc'))",
                            project=PROJECT, env={'BUILD': '${asset:dco_artifact}'}, artifact_ref=ref, affinity_keys=[ref],
                            step_phases=['sim'], short_job=True,
                        )
                        with self.assertRaisesRegex(RemoteError, 'unavailable for this project'):
                            client.submit(dict(consumer, project='different-project'))
                        with self.assertRaisesRegex(RemoteError, 'only contain'):
                            client.submit(dict(consumer, affinity_keys=['b' * 64]))
                        with self.assertRaisesRegex(RemoteError, 'operator-only'):
                            client.publish_reference(original, project=PROJECT, paths=['compiled.pyc'], recipe_sha256=RECIPE,
                                                     legacy_abi=[sys.platform, 'x86_64', ''])
                        with patch.object(a, 'asset', wraps=a.asset) as asset_a, patch.object(b, 'asset', wraps=b.asset) as asset_b:
                            ids = list(callers.map(lambda _: client.submit(consumer)['id'], range(8)))
                            rows = list(callers.map(lambda identity: client.wait(identity, wait_timeout=10), ids))
                            uploads = sum(len(call.args) > 1 and call.args[1] is not None
                                          for asset in (asset_a, asset_b) for call in asset.call_args_list)
                        self.assertEqual(uploads, 1)
                        self.assertEqual({row['stdout'].strip() for row in rows}, {'441'})
                        self.assertEqual(sum(row['cache_hit']['artifact'] for row in rows), 7)
                        for row in rows:
                            self.assertEqual(row['cache_hit']['worker_id'], row['id'])
                            self.assertNotIn('_artifact_abi', row)
                        warm = state.get(ids[0])['assigned_node']
                        warm_provider = next(n for n in pool.nodes if n.name == warm)
                        warm_provider.enabled = False
                        fallback = client.run(consumer)
                        self.assertEqual(fallback['status'], 'succeeded')
                        self.assertFalse(fallback['cache_hit']['artifact'])
                        record('portable-build', dict(producer_original_id=original, reference=ref, cpu_seconds=built['cpu_seconds'],
                            producer_submissions=1, consumers=len(rows), distinct_ids=len(set(ids)), cold_uploads=uploads,
                            cache_hits=7, eligible_warm_assignments=7, eligible_warm_hits=7, fallback_id=fallback['id']))

    def test_fifty_short_jobs_and_receipts(self):
        with scratch() as folder:
            root = Path(folder)
            with execution_provider(root / 'one', cpus=4) as (_, first), execution_provider(root / 'two', cpus=4) as (_, second):
                with Pool([dict(name='one', client=first), dict(name='two', client=second)]) as pool:
                    with center(pool, root / 'center') as (_, operator), ThreadPoolExecutor(4) as callers:
                        client = Client(operator.url, operator.token)
                        def run(index):
                            task = python(f'print({index})', project=PROJECT, short_job=True, step_phases=['sim'])
                            begun = time.perf_counter()
                            original = client.submit(task)['id']
                            row = client.wait(original, poll=.03, wait_timeout=8)
                            outcome = client.fetch(original)
                            wall = time.perf_counter() - begun
                            self.assertEqual(outcome['status'], 'succeeded', row)
                            self.assertEqual(outcome['stdout'].strip(), str(index))
                            native = sum(step['finished'] - step['started'] for step in row['steps'])
                            return dict(id=original, wall_s=wall, execution_wall_s=native, platform_overhead_s=wall - native,
                                        cpu_s=row['cpu_seconds'])
                        rows = list(callers.map(run, range(50)))
                        values = sorted(row['platform_overhead_s'] for row in rows)
                        record('short-jobs', dict(count=len(rows), parallel_requests=4, distinct_ids=len({r['id'] for r in rows}),
                                                  median=statistics.median(values), p95=values[47], max=values[-1], jobs=rows))
                        self.assertEqual(len({row['id'] for row in rows}), 50)
                        self.assertLess(values[-1], 1.0)

    def test_timeout_publishes_verified_checkpoint_and_process_tree_cpu(self):
        long_script = """import os, subprocess, sys, time
from pathlib import Path
from remote_compute import checkpoint_requested, report_checkpoint, report_progress
p = subprocess.Popen([sys.executable, '-B', '-c', 'import time; time.sleep(20)'])
while not checkpoint_requested():
    time.sleep(.02)
saved = {'completed': 7, 'total': 16, 'child_pid': p.pid}
import json
Path('state.json').write_text(json.dumps(saved))
report_checkpoint(['state.json'])
report_progress(phase='sim', completed=7, total=16, unit='cycle')
while True:
    time.sleep(.08)
"""
        short_child = "import subprocess,sys;subprocess.run([sys.executable,'-B','-c','sum(i*i for i in range(400000))'],check=True)"
        with scratch() as folder:
            root = Path(folder)
            with execution_provider(root / 'one') as (_, remote), Pool([remote]) as pool, center(pool, root / 'center') as (_, operator):
                client = Client(operator.url, operator.token)
                task = python(short_child, project=PROJECT, timeout=3, timeout_grace_s=1,
                              outputs=['state.json'], checkpoint_output=dict(paths=['state.json'], recipe_sha256=RECIPE),
                              env={'PYTHONPATH': '${dco}'})
                task['steps'].append(['${tool:python}', '-B', '-c', long_script])
                task['step_phases'] = ['compile', 'sim']
                original = client.submit(task)['id']
                row = client.wait(original, wait_timeout=9)
                self.assertEqual(row['status'], 'timed_out', row)
                ref = row['partial_checkpoint_ref']
                self.assertEqual(len(ref), 64)
                self.assertEqual((row['progress']['completed'], row['progress']['total']), (7, 16))
                cpu = row['cpu_breakdown_s']
                self.assertGreater(cpu['compile'], 0)
                self.assertGreater(cpu['sim'], 0)
                self.assertAlmostEqual(cpu['total'], sum(cpu[stage] for stage in ('compile', 'load_check', 'projection', 'sim')))
                self.assertEqual(cpu['total'], row['cpu_seconds'])
                result = client.fetch(original)
                saved = json.loads(result['files']['state.json'])
                self.assertEqual(saved['completed'], 7)
                if os.name == 'nt':
                    from ctypes import windll
                    handle = windll.kernel32.OpenProcess(0x100000, False, saved['child_pid'])
                    if handle:
                        try:
                            self.assertEqual(windll.kernel32.WaitForSingleObject(handle, 0), 0)
                        finally:
                            windll.kernel32.CloseHandle(handle)
                else:
                    self.assertFalse(Path('/proc', str(saved['child_pid'])).exists())
                consume = python("import json,os,pathlib;print(json.loads((pathlib.Path(os.environ['SNAP'])/'state.json').read_text())['completed'])",
                                 project=PROJECT, env={'SNAP': '${asset:dco_checkpoint}'}, checkpoint_ref=ref)
                resumed = client.run(consume)
                self.assertEqual(resumed['stdout'].strip(), '7')
                record('deadline-checkpoint', dict(id=original, status=row['status'], checkpoint_ref=ref,
                    completed=7, total=16, cpu_breakdown_s=cpu, output_verified=True, descendant_reaped=True,
                    resumed_original_id=resumed['id']))
