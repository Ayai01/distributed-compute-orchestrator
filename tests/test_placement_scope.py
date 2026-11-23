"""Operator placement is a frozen allowlist. Public asset hashes do not rank workers."""

from pathlib import Path
import unittest

from remote_compute import Client, Pool, RemoteError
from remote_compute.packages import package_task
from remote_compute.placement import _matching_pass
from test_center import center
from test_compute import provider, python, scratch


class PlacementScopeTests(unittest.TestCase):
    def test_scope_requires_operator_and_is_never_widened(self):
        with scratch() as directory:
            root = Path(directory)
            with provider(root / 'school') as (_, school), provider(root / 'lab') as (_, lab):
                with Pool([dict(name='school', client=school), dict(name='lab', client=lab)]) as pool:
                    with center(pool, root / 'center') as (owner, operator):
                        project = Client(operator.url, owner.token)
                        scope = {'nodes': ['school'], 'reason': 'Authorized school-only qualification'}
                        task = dict(python('print("scoped")', retries=0), placement_scope=scope)
                        with self.assertRaisesRegex(RemoteError, 'operator'):
                            project.submit(task)
                        with self.assertRaisesRegex(RemoteError, 'outside'):
                            operator.submit(dict(task, node='lab'))
                        operator.control(node='school', routing={'enabled': False})
                        identity = operator.submit(task)['id']
                        self.assertEqual(operator.status(identity)['status'], 'queued')
                        self.assertNotIn('placement_scope', project.status(identity))
                        operator.control(node='school', routing={'enabled': True})
                        result = operator.wait(identity, wait_timeout=12)
                        self.assertEqual(result['assigned_node'], 'school')
                        self.assertEqual(result['placement_scope'], scope)
                        self.assertEqual(result['status'], 'succeeded')

    def test_matching_cache_changes_with_scope_and_public_affinity_is_removed(self):
        with scratch() as directory:
            root = Path(directory)
            with provider(root / 'school') as (_, school), provider(root / 'lab') as (_, lab):
                with Pool([dict(name='school', client=school), dict(name='lab', client=lab)]) as pool:
                    task = dict(python('print(1)'), env={}, requires=['python'], _affinity_assets=('a' * 64,))
                    self.assertNotIn('_affinity_assets', package_task(task, 'python', None))
                    matching = _matching_pass(pool)
                    for label in ('school', 'lab'):
                        scoped = dict(task, placement_scope={'nodes': [label], 'reason': 'Test'})
                        self.assertEqual([node.name for node in matching(scoped)], [label])
