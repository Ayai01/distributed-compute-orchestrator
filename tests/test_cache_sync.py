"""Incremental scientific exchange and RPC lifecycle use only owned jobs."""

import json
from pathlib import Path
import threading
import unittest
from unittest.mock import Mock, patch

from remote_compute import cache_arh as arh, cache_bench as bench
from remote_compute import cache_sync as sync
from remote_compute.client import RemoteError
from test_cache_arh_bench import local_producer, seed, snapshot
from test_compute import scratch


def node(name, root, project='arh', **extra):
    return dict(name=name, tool_root=str(root/'dco'),
                shared_paths={project: str(root)}, tags=[project+'_python', 'python'],
                **extra)


def local_calls(log, before=None):
    """Exercise the real adapter contract while substituting only transport."""
    def call(rpc, target, project, action, **arguments):
        log.append((target['name'], action, arguments))
        if before:
            response = before(target, action, arguments)
            if response is not None:
                return response
        arguments.pop('_assets', None)
        adapter = {'arh': arh, 'bench': bench}[project]
        return getattr(adapter, action)(target['shared_paths'][project], **arguments)
    return call


class ResearchSyncTests(unittest.TestCase):
    def test_arh_union_and_deeper_prefix_converge_then_transfer_zero_records(self):
        with scratch() as directory:
            roots = [Path(directory)/name for name in ('A', 'B', 'C')]
            nodes = [node(root.name, root) for root in roots]
            seed(roots[0], 'mac', ('engine-seed-A',), '{"score":1}')
            seed(roots[1], 'mac', ('engine-seed-B',), '{"score":2}')
            prefix = seed(roots[0], 'conv-prefix', ('sampling-key',),
                          '{"trials":4,"_prefix_state":[1]}', trials=4)
            seed(roots[1], 'conv-prefix', ('sampling-key',),
                 '{"trials":8,"_prefix_state":[1,2]}', trials=8)
            client = Mock()
            client.nodes.return_value = dict(nodes=nodes)
            calls = []
            with patch.object(sync._RPC, 'call', new=local_calls(calls)), \
                    patch.object(sync, '_assets', return_value=dict(
                        status='succeeded', identities={'arh_source': 'fixture-source'})), \
                    patch.object(arh, '_open_store', side_effect=local_producer):
                first = sync.sync_cache(client, 'arh', kind='research')
                inventories = [arh.manifest(root) for root in roots]
                self.assertEqual(first['status'], 'succeeded')
                self.assertEqual(inventories[0], inventories[1])
                self.assertEqual(inventories[1], inventories[2])
                self.assertEqual(len(inventories[0]), 3)
                self.assertEqual(arh.export(roots[0], [prefix])[prefix]['trials'], 8)
                self.assertGreater(first['projects']['arh']['research']['updated'], 0)
                calls.clear()
                second = sync.sync_cache(client, 'arh', kind='research')
            self.assertEqual(second['projects']['arh']['research']['exported_records'], 0)
            self.assertEqual([action for unused, action, args in calls], ['manifest']*3)
            client.machine.assert_not_called()

    def test_bench_complete_snapshots_union_then_no_duplicate_data(self):
        with scratch() as directory:
            roots = [Path(directory)/name for name in ('A', 'B')]
            keys = [snapshot(roots[0], 'class-ab.npz', b'measured-A'),
                    snapshot(roots[1], 'ldo.npz', b'measured-B')]
            client = Mock()
            client.nodes.return_value = dict(nodes=[node(root.name, root, 'bench') for root in roots])
            calls = []
            with patch.object(sync._RPC, 'call', new=local_calls(calls)):
                first = sync.sync_cache(client, 'bench', kind='research')
                self.assertEqual(first['status'], 'succeeded')
                self.assertEqual(set(bench.manifest(roots[0])), set(keys))
                self.assertEqual(bench.manifest(roots[0]), bench.manifest(roots[1]))
                calls.clear()
                second = sync.sync_cache(client, 'bench', kind='research')
            self.assertEqual(second['projects']['bench']['research']['exported_records'], 0)
            self.assertTrue(all(action == 'manifest' for unused, action, args in calls))

    def test_one_registered_consumer_skips_before_any_export(self):
        client = Mock()
        client.nodes.return_value = dict(nodes=[node('only', Path('single'), 'bench')])
        with patch.object(sync._RPC, 'call') as rpc:
            report = sync.sync_cache(client, 'bench', kind='research')
        rpc.assert_not_called()
        self.assertEqual(report['projects']['bench']['research']['status'], 'skipped')

    def test_offline_consumer_does_not_prevent_other_two_merging(self):
        with scratch() as directory:
            roots = [Path(directory)/name for name in ('A', 'B', 'offline')]
            key = snapshot(roots[0], 'class-ab.npz', b'measurement')
            nodes = [node(root.name, root, 'bench') for root in roots]
            nodes[-1]['error'] = 'connection timed out'
            client, calls = Mock(), []
            client.nodes.return_value = dict(nodes=nodes)
            with patch.object(sync._RPC, 'call', new=local_calls(calls)):
                report = sync.sync_cache(client, 'bench', kind='research')
            self.assertEqual(report['status'], 'partial')
            self.assertEqual(report['projects']['bench']['research']['status'], 'succeeded')
            self.assertEqual(report['unavailable'][0]['node'], 'offline')
            self.assertEqual((roots[1]/key).read_bytes(), (roots[0]/key).read_bytes())
            self.assertNotIn('offline', [name for name, action, args in calls])

    def test_manifest_failure_still_syncs_reachable_consumers(self):
        with scratch() as directory:
            roots = [Path(directory)/name for name in ('A', 'B', 'C')]
            key = snapshot(roots[0], 'class-ab.npz', b'measurement')
            client = Mock()
            client.nodes.return_value = dict(nodes=[node(root.name, root, 'bench') for root in roots])
            def before(target, action, arguments):
                if target['name'] == 'B':
                    raise RemoteError('temporarily offline')
            with patch.object(sync._RPC, 'call', new=local_calls([], before)):
                report = sync.sync_cache(client, 'bench', kind='research')
            research = report['projects']['bench']['research']
            self.assertEqual(research['status'], 'partial')
            self.assertEqual(research['errors'][0]['action'], 'manifest')
            self.assertEqual(research['errors'][0]['node'], 'B')
            self.assertEqual((roots[2]/key).read_bytes(), (roots[0]/key).read_bytes())
            self.assertFalse((roots[1]/key).exists())

    def test_failed_receiver_gets_one_batch_while_other_receiver_finishes(self):
        with scratch() as directory:
            roots = [Path(directory)/name for name in ('A', 'B', 'C')]
            for index in range(4):
                snapshot(roots[0], 'snapshot-'+str(index)+'.npz', bytes([index])*600000)
            client, calls = Mock(), []
            client.nodes.return_value = dict(nodes=[node(root.name, root, 'bench') for root in roots])
            def before(target, action, arguments):
                if target['name'] == 'B' and action == 'merge':
                    raise RemoteError('receiver producer is incompatible')
            with patch.object(sync._RPC, 'call', new=local_calls(calls, before)):
                report = sync.sync_cache(client, 'bench', kind='research')
            self.assertEqual(report['status'], 'partial')
            self.assertEqual(sum(name == 'B' and action == 'merge' for name, action, args in calls), 1)
            self.assertEqual(sum(action == 'export' for name, action, args in calls), 4)
            self.assertEqual(bench.manifest(roots[0]), bench.manifest(roots[2]))

    def test_export_digest_supersedes_inventory_when_source_publishes(self):
        with scratch() as directory:
            roots = [Path(directory)/name for name in ('A', 'B')]
            key = snapshot(roots[0], 'class-ab.npz', b'old-measurement')
            original = bench.manifest(roots[0])[key]['digest']
            client, calls = Mock(), []
            client.nodes.return_value = dict(nodes=[node(root.name, root, 'bench') for root in roots])
            def before(target, action, arguments):
                if target['name'] == 'A' and action == 'export':
                    snapshot(roots[0], 'class-ab.npz', b'new-measurement')
            with patch.object(sync._RPC, 'call', new=local_calls(calls, before)):
                report = sync.sync_cache(client, 'bench', kind='research')
            self.assertEqual(report['status'], 'succeeded')
            current = bench.manifest(roots[0])[key]['digest']
            self.assertNotEqual(original, current)
            self.assertEqual(bench.manifest(roots[1])[key]['digest'], current)
            merges = [args for unused, action, args in calls if action == 'merge']
            self.assertEqual(len(merges), 1)
            self.assertEqual(merges[0]['records'][key]['digest'], current)

    def test_disappeared_export_reports_partial_without_inventing_record(self):
        with scratch() as directory:
            roots = [Path(directory)/name for name in ('A', 'B')]
            snapshot(roots[0], 'class-ab.npz', b'old-measurement')
            client, calls = Mock(), []
            client.nodes.return_value = dict(nodes=[node(root.name, root, 'bench') for root in roots])
            def before(target, action, arguments):
                return {} if action == 'export' else None
            with patch.object(sync._RPC, 'call', new=local_calls(calls, before)):
                report = sync.sync_cache(client, 'bench', kind='research')
            self.assertEqual(report['status'], 'partial')
            self.assertEqual(report['projects']['bench']['research']['exported_records'], 0)
            self.assertFalse(any(action == 'merge' for unused, action, args in calls))
            self.assertFalse(roots[1].exists())


class CacheRpcTests(unittest.TestCase):
    def fixture(self, *, fetch_error=None, status='succeeded'):
        client = Mock()
        accepted = []
        client.machine.return_value.python.side_effect = lambda *args, **kwargs: accepted.append(kwargs['job_id'])
        client.status.return_value = dict(status=status)
        client.fetch.return_value = dict(status='succeeded', files={
            'remote_op/result/response.json': json.dumps({'key': {'digest': 'abc', 'bytes': 4}}).encode()})
        if fetch_error:
            client.fetch.side_effect = fetch_error
        stop = threading.Event()
        stop.wait = Mock(return_value=False)
        return client, accepted, sync._RPC(client, None, stop)

    def test_success_fetches_and_deletes_only_own_accepted_id(self):
        client, accepted, rpc = self.fixture()
        response = rpc.call(node('worker', Path('fixture')), 'arh', 'manifest')
        self.assertEqual(response['key']['digest'], 'abc')
        self.assertEqual(len(accepted), 1)
        client.fetch.assert_called_once_with(accepted[0], delete=False)
        client.delete.assert_called_once_with(accepted[0])
        client.cancel.assert_not_called()
        self.assertEqual(rpc.pending, {})
        self.assertEqual(rpc.collected, 1)
        options = client.machine.return_value.python.call_args.kwargs
        self.assertEqual(options['project'], 'DCO-cache')
        self.assertEqual(options['retries'], 0)
        self.assertEqual(options['requires'], ['python_numpy'])
        self.assertEqual(options['tool'], 'python_numpy')

    def test_final_collection_revisits_same_id_after_endpoint_recovers(self):
        client, accepted, rpc = self.fixture()
        rpc.pending['original'] = dict(node='worker', project='arh', action='merge')
        rpc.collect_pending()
        client.fetch.assert_called_once_with('original', delete=False)
        client.delete.assert_called_once_with('original')
        client.machine.assert_not_called()
        client.cancel.assert_not_called()
        self.assertEqual(rpc.pending, {})

    def test_final_collection_retains_unread_receipt_and_active_original_ids(self):
        client, accepted, rpc = self.fixture()
        rpc.pending['active'] = dict(node='worker', project='arh', action='merge')
        rpc.pending['unread'] = dict(node='worker', project='arh', action='manifest')
        client.status.side_effect = lambda identity, **kw: dict(
            status='running' if identity == 'active' else 'succeeded')
        client.fetch.side_effect = RemoteError('still offline')
        rpc.collect_pending()
        self.assertEqual(rpc.pending['active']['status'], 'running')
        self.assertEqual(rpc.pending['unread']['status'], 'awaiting_receipt')
        client.delete.assert_not_called()
        client.cancel.assert_not_called()

    def test_failed_fetch_retains_same_id_and_never_deletes_or_resubmits(self):
        client, accepted, rpc = self.fixture(fetch_error=RemoteError('download timeout'))
        with self.assertRaisesRegex(RemoteError, 'download timeout'):
            rpc.call(node('worker', Path('fixture')), 'arh', 'manifest')
        self.assertEqual(list(rpc.pending), accepted)
        self.assertEqual(rpc.pending[accepted[0]]['action'], 'manifest')
        client.machine.return_value.python.assert_called_once()
        client.delete.assert_not_called()
        client.cancel.assert_not_called()
        self.assertEqual(rpc.collected, 0)
        self.assertEqual(client.fetch.call_count, 3)
        self.assertEqual({call.args[0] for call in client.fetch.call_args_list}, set(accepted))

    def test_stop_collects_only_owned_jobs_and_cancels_only_unfinished(self):
        client, accepted, rpc = self.fixture(fetch_error=RemoteError('download timeout'))
        with self.assertRaises(RemoteError):
            rpc.call(node('worker', Path('fixture')), 'arh', 'manifest')
        completed = accepted[0]
        rpc.pending['own-running'] = dict(node='worker', project='arh', action='merge')
        states = {completed: 'succeeded', 'own-running': 'running'}
        client.status.side_effect = lambda identity, **kwargs: dict(status=states[identity])
        client.cancel.side_effect = lambda identity: states.update({identity: 'cancelled'})
        client.fetch.side_effect = None
        client.reset_mock()
        rpc.cancel_owned()
        client.cancel.assert_called_once_with('own-running')
        self.assertEqual({call.args[0] for call in client.delete.call_args_list}, set(states))
        self.assertEqual({call.args[0] for call in client.fetch.call_args_list}, set(states))
        self.assertEqual(rpc.pending, {})
        self.assertEqual(rpc.collected, 2)
        self.assertEqual({call.args[0] for call in client.status.call_args_list},
                         {completed, 'own-running'})

    def test_stop_preserves_original_ids_when_node_remains_unreachable(self):
        client, accepted, rpc = self.fixture(fetch_error=RemoteError('download timeout'))
        with self.assertRaises(RemoteError):
            rpc.call(node('worker', Path('fixture')), 'arh', 'manifest')
        client.status.side_effect = RemoteError('offline')
        rpc.cancel_owned()
        self.assertEqual(list(rpc.pending), accepted)
        client.delete.assert_not_called()
        client.cancel.assert_not_called()

    def test_lost_submit_reply_queries_and_collects_original_id_once(self):
        client, accepted, rpc = self.fixture()
        on_submit = rpc.on_submit = Mock()
        def accepted_without_reply(*args, **kwargs):
            accepted.append(kwargs['job_id'])
            raise RemoteError('POST reply disappeared after acceptance')
        client.machine.return_value.python.side_effect = accepted_without_reply
        response = rpc.call(node('worker', Path('fixture')), 'arh', 'manifest')
        self.assertEqual(response['key']['digest'], 'abc')
        self.assertEqual(len(accepted), 1)
        client.machine.return_value.python.assert_called_once()
        self.assertEqual({call.args[0] for call in client.status.call_args_list}, set(accepted))
        client.fetch.assert_called_once_with(accepted[0], delete=False)
        client.delete.assert_called_once_with(accepted[0])
        on_submit.assert_called_once_with(client, accepted[0])
        self.assertEqual(rpc.pending, {})

    def test_lost_submit_reply_and_offline_query_keep_original_id_without_second_post(self):
        client, accepted, rpc = self.fixture()
        def accepted_without_reply(*args, **kwargs):
            accepted.append(kwargs['job_id'])
            raise RemoteError('POST reply disappeared')
        client.machine.return_value.python.side_effect = accepted_without_reply
        client.status.side_effect = RemoteError('node disconnected')
        with self.assertRaisesRegex(RemoteError, 'node disconnected'):
            rpc.call(node('worker', Path('fixture')), 'arh', 'merge', records={})
        client.machine.return_value.python.assert_called_once()
        self.assertEqual(client.status.call_count, 3)
        self.assertEqual({call.args[0] for call in client.status.call_args_list}, set(accepted))
        self.assertEqual(list(rpc.pending), accepted)
        client.delete.assert_not_called()
        client.fetch.assert_not_called()

    def test_transient_read_failure_retries_same_id_without_new_execution(self):
        client, accepted, rpc = self.fixture()
        response = client.fetch.return_value
        client.status.side_effect = [RemoteError('status temporarily unavailable'), dict(status='succeeded')]
        client.fetch.side_effect = [RemoteError('first read lost'), RemoteError('second read lost'), response]
        self.assertEqual(rpc.call(node('worker', Path('fixture')), 'arh', 'manifest')['key']['digest'], 'abc')
        client.machine.return_value.python.assert_called_once()
        self.assertEqual(client.fetch.call_count, 3)
        self.assertEqual({call.args[0] for call in client.fetch.call_args_list}, set(accepted))
        client.delete.assert_called_once_with(accepted[0])
        self.assertEqual(rpc.pending, {})

    def test_malformed_response_keeps_fetched_id_for_diagnosis(self):
        client, accepted, rpc = self.fixture()
        client.fetch.return_value = dict(status='succeeded', files={'remote_op/result/response.json': b'not-json'})
        with self.assertRaises(ValueError):
            rpc.call(node('worker', Path('fixture')), 'arh', 'manifest')
        self.assertEqual(list(rpc.pending), accepted)
        client.delete.assert_not_called()


if __name__ == '__main__':
    unittest.main()
