"""Preferred CPU width through the placement and live dispatch entries."""

from pathlib import Path
import threading
import unittest
from unittest.mock import patch

from remote_compute import Pool
from remote_compute.placement import PlacementSnapshot, _matching_pass
import test_center as fixtures
from test_compute import provider, python, scratch


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
