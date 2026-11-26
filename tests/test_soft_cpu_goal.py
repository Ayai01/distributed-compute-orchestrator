"""Preferred CPU width through the placement and live dispatch entries."""

from pathlib import Path
import threading
import unittest
from unittest.mock import patch
from urllib.parse import urlsplit

from remote_compute import Pool
from remote_compute.placement import PlacementSnapshot, _matching_pass
from remote_compute.relay_bulk import RelayBulkBudget
import test_center as fixtures
from remote_compute.worker import Conflict
from test_compute import provider, python, scratch, until


def job(status, node=None, **result):
    return dict(task=dict(steps=[['echo']], cpus=1, max_cpus=8, memory_mb=64, project='A'),
                node=node, elastic_cpus=True, cancel=threading.Event(), result=dict(status=status, **result))


class SoftCpuGoalTests(unittest.TestCase):
    def test_preparing_counts_one_and_fresh_ready_request_counts_its_actual_width(self):
        state = fixtures.AdmissionPlanningTests().center([8])
        node = state.pool.nodes[0]
        node.reserved = 1
        preparing = state.jobs['original'] = job('dispatching', node, dispatch_stage='assets')
        resources = state.project_resources()
        self.assertEqual(resources['A']['demand'], 1)
        self.assertEqual(state._growth_headroom(resources), {})

        preparing['result']['resource_request'] = dict(request_id=9, cpus=2, min_cpus=0, active_cpus=0)
        resources = state.project_resources()
        self.assertEqual(resources['A']['demand'], 2)
        self.assertEqual(state._growth_headroom(resources), {'0': 1})
        preparing['lease_answered'] = 10
        self.assertEqual(state._growth_headroom(state.project_resources()), {})

    def test_idle_slot_can_start_a_new_original_without_consuming_a_fixed_phase(self):
        state = fixtures.AdmissionPlanningTests().center([4])
        node = state.pool.nodes[0]
        node.reserved = 1
        fresh = dict(request_id=4, cpus=4, min_cpus=0, active_cpus=0)
        state.jobs['pending'] = job('running', node, resource_request=fresh)
        state.jobs['entrant'] = job('queued')
        state.queue = ['entrant']
        view = PlacementSnapshot(state)
        self.assertEqual(view._growth_headroom(view._project_resources()), {'0': 3})
        self.assertEqual([(k,h) for k,_,h in view.candidates()], [('entrant',{})])
        asset = 'a' * 64
        state.jobs['entrant']['task']['assets'] = {'input':asset}
        self.assertEqual(list(PlacementSnapshot(state).candidates()), [])
        node.asset_session = (None,None,None)
        node.known_assets.add(asset)
        self.assertTrue(list(PlacementSnapshot(state).candidates()))
        fresh['min_cpus'] = 4
        self.assertEqual(list(PlacementSnapshot(state).candidates()), [])

    def test_planning_does_not_share_matches_between_operator_scopes(self):
        state = fixtures.AdmissionPlanningTests().center([8, 8])
        lookup = _matching_pass(state.pool)
        task = job('queued')['task']
        for name in ('1', '0', '1'):
            scoped = dict(task, placement_scope={'nodes':[name]})
            self.assertEqual([n.name for n in lookup(scoped)], [name])

    def test_started_original_without_a_new_resize_cannot_hold_empty_cpus(self):
        state = fixtures.AdmissionPlanningTests().center([8])
        node = state.pool.nodes[0]
        node.reserved = 2
        for i in range(2):
            current = state.jobs[str(i)] = job('running', node, execution_started=1, requested_cpus=16)
            current['desired_cpus'] = 16
        state.jobs['entrant'] = job('queued')
        state.jobs['entrant']['task']['assets'] = {'input':'a' * 64}
        state.queue = ['entrant']
        view = PlacementSnapshot(state)
        self.assertEqual(view._growth_headroom(view._project_resources()), {})
        self.assertEqual([(i,headroom) for i,_,headroom in view.candidates()], [('entrant',{})])
        state.jobs['0']['result']['resource_request'] = dict(request_id=2,cpus=4,min_cpus=0,active_cpus=0)
        view = PlacementSnapshot(state)
        self.assertGreater(view._growth_headroom(view._project_resources())['0'],0)

    def test_deep_queue_does_not_claim_cores_behind_full_relay_handoffs(self):
        state = fixtures.AdmissionPlanningTests().center([19])
        node = state.pool.nodes[0]
        node.client._endpoint = urlsplit('http://127.0.0.1:27100')
        active = state.jobs['active'] = job('running', node, execution_started=1)
        active['task'].update(max_cpus=16, cpus=4)
        active['desired_cpus'] = 16
        for index in range(4):
            name = f'dispatch-{index}'
            node.pending_dispatches.add(name)
            state.jobs[name] = job('dispatching', node, dispatch_stage='awaiting_worker')
        node.reserved = 8
        for i in range(979):
            key = f'queued-{i}'
            state.jobs[key] = job('queued')
            state.jobs[key]['task']['cpus'] = 0
            state.queue.append(key)

        with patch('remote_compute.client._RELAY_BULK_BUDGET', RelayBulkBudget([27100], None)):
            view = PlacementSnapshot(state)
            self.assertFalse(list(view.candidates()))
            self.assertEqual(view._cooperative_cpu_target('active', 16, view._project_resources()), 15)

            node.pending_dispatches.remove('dispatch-0')
            view = PlacementSnapshot(state)
            self.assertTrue(list(view.candidates()))
            self.assertEqual(view._cooperative_cpu_target('active', 16, view._project_resources()), 14)

            node.health['cpus'] = node.health['cpu_limit'] = 29
            view = PlacementSnapshot(state)
            self.assertEqual(view._cooperative_cpu_target('active', 16, view._project_resources()), 16)

    def test_worker_cold_start_funding_releases_the_centers_larger_estimate(self):
        original = 'c' * 32
        with scratch() as folder, provider(Path(folder)/'worker', cpus=2, memory_mb=768) as (_,worker):
            with Pool([worker]) as pool, fixtures.center(pool,Path(folder)/'center') as (state,client):
                resize = state.resize

                def decline_first(identity, *args, **kwargs):
                    if identity == original:
                        raise Conflict('Insufficient memory for resource resize; existing grant retained')
                    return resize(identity,*args,**kwargs)

                command = python('import time; time.sleep(20)', max_cpus=4, retries=0,
                                 base_memory_mb=2048,memory_per_cpu_mb=512)
                with patch.object(state,'resize',side_effect=decline_first):
                    client.submit(command,job_id=original)
                    try:
                        until(lambda:client.status(original).get('execution_started'))
                        row = client.status(original)
                        self.assertEqual(row['memory_mb'],256)
                        self.assertEqual(row['memory_phase'],{'base_memory_mb':2048,'memory_per_cpu_mb':512})
                        self.assertEqual(pool.nodes[0].reserved_memory_mb,256)
                        entrant = client.submit(python('print("second task reached the worker")',retries=0,
                                                       memory_mb=256))['id']
                        self.assertEqual(client.wait(entrant,wait_timeout=8)['status'],'succeeded')
                        self.assertEqual(client.status(original)['status'],'running')
                    finally:
                        client.cancel(original)
                        client.wait(original,wait_timeout=8)

    def test_second_command_starts_after_first_calibrates_its_funded_memory(self):
        first = 'd' * 32
        with scratch() as folder, provider(Path(folder)/'worker', cpus=2, memory_mb=768) as (_,worker):
            with Pool([worker]) as pool, fixtures.center(pool,Path(folder)/'center') as (_,client):
                client.submit(python('import time; time.sleep(25)',retries=0,
                                     base_memory_mb=512,memory_per_cpu_mb=128),job_id=first)
                try:
                    until(lambda:client.status(first).get('execution_started'))
                    entrant=client.submit(python('print(7)',retries=0,memory_mb=256))['id']
                    self.assertEqual(client.wait(entrant,wait_timeout=10)['status'],'succeeded')
                    self.assertEqual(client.status(first)['status'],'running')
                    self.assertEqual(client.status(first)['memory_phase'],
                                     {'base_memory_mb':512,'memory_per_cpu_mb':128})
                finally:
                    client.cancel(first)
                    client.wait(first,wait_timeout=8)

    def test_missing_input_does_not_delay_another_original_on_the_live_dispatch_entry(self):
        cold = 'a' * 32
        warm = 'b' * 32
        entered, release = threading.Event(), threading.Event()
        with scratch() as folder, provider(Path(folder)/'worker', cpus=4) as (_, worker):
            with Pool([worker]) as pool, fixtures.center(pool, Path(folder)/'center') as (state, client):
                prepare = state._ensure_assets

                def delay_first(node, assets, **kwargs):
                    assets = tuple(assets)
                    if assets:
                        entered.set()
                        if not release.wait(10):
                            raise RuntimeError('Input transfer fixture did not release')
                    return prepare(node, assets, **kwargs)

                source = client.sync_files({'input.txt':b'original missing input'}, job_id=cold)
                descriptor = python('print(12)', project='cpu-goal', max_cpus=4, retries=0,
                                    assets={'payload':source})
                with patch.object(state, '_ensure_assets', side_effect=delay_first):
                    client.submit(descriptor, job_id=cold)
                    try:
                        self.assertTrue(entered.wait(5))
                        client.submit(python('print(14)', project='cpu-goal', retries=0), job_id=warm)
                        ready = client.wait(warm, wait_timeout=8)
                        self.assertEqual((ready['status'], ready['id'],ready['cpus']), ('succeeded',warm,1))
                        self.assertEqual(client.status(cold)['dispatch_stage'], 'assets')
                    finally:
                        release.set()
                    ready = client.wait(cold, wait_timeout=8)
                    self.assertEqual((ready['status'],ready['id']), ('succeeded',cold))
