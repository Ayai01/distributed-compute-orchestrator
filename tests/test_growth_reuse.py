"""A growth pass reuses one unchanged warm plan per project and priority."""
import random
import threading
import unittest
from unittest.mock import patch

from remote_compute import coordinator as module
import test_center as center_fixtures


class UncachedCenter(module.Coordinator):
    def _cooperative_cpu_target(self, identity, desired, resources, *, warm_cache=None):
        return super()._cooperative_cpu_target(identity, desired, resources)


class GrowthReuseTests(unittest.TestCase):
    def test_one_thread_request_has_its_exact_target_without_global_allocation(self):
        state=center_fixtures.AdmissionPlanningTests().center([16])
        node=state.pool.nodes[0];node.reserved=1
        state.jobs['one']=dict(task=dict(steps=[['echo']],cpus=1,max_cpus=16,
            memory_mb=64,project='A'),elastic_cpus=True,node=node,
            cancel=threading.Event(),result=dict(status='running'))
        resources=state.project_resources()
        with patch.object(module,'weighted_node_threads',side_effect=AssertionError('Redundant one-thread allocation')):
            self.assertEqual(state._cooperative_cpu_target('one',1,resources),1)

    def test_cached_warm_plan_matches_independent_per_job_plans(self):
        rng=random.Random(74219)
        fixture=center_fixtures.AdmissionPlanningTests()
        for case in range(320):
            state=fixture.center([rng.randint(4,24) for _ in range(rng.randint(1,4))])
            for node in state.pool.nodes:
                node.health['tags'].append('matrix'+str(int(node.name)%2))
                node.external=rng.randint(0,2)
            for index in range(rng.randint(4,32)):
                node=rng.choice(state.pool.nodes)
                held=node if rng.random()<.8 and node.reserved+node.external<node.capacity else None
                maximum=rng.choice([1,2,3,4,8,16])
                cpus=1 if held else rng.choice([0,1])
                elastic=maximum>1
                task=dict(steps=[['echo']],cpus=cpus,memory_mb=64,
                          max_cpus=maximum,priority=rng.choice([-1,0,2]),
                          project=rng.choice(['A','B','C']),
                          requires=['matrix'+str(int(node.name)%2)] if rng.random()<.35 else [])
                job=dict(task=task,auto_cpus=cpus==0,elastic_cpus=elastic,
                         node=held,cancel=threading.Event(),
                         result=dict(status='running' if held else 'queued',submitted=index))
                if held:node.reserved+=cpus
                if rng.random()<.7:job['desired_cpus']=rng.randint(1,maximum)
                if rng.random()<.05:job['cancel'].set()
                state.jobs[str(index)]=job
            resources=state.project_resources()
            candidate=dict(priority=rng.choice([-1,0,2]))
            actual=state._growth_headroom(candidate,resources)
            state.__class__=UncachedCenter
            expected=state._growth_headroom(candidate,resources)
            self.assertEqual(actual,expected,'scenario '+str(case))

    def test_repeated_peers_do_not_repeat_the_group_allocation(self):
        state=center_fixtures.AdmissionPlanningTests().center([128])
        node=state.pool.nodes[0]
        for index in range(64):
            state.jobs[str(index)]=dict(task=dict(steps=[['echo']],cpus=1,max_cpus=16,
                memory_mb=64,project='A'),elastic_cpus=True,node=node,
                cancel=threading.Event(),result=dict(status='running'))
        node.reserved=64
        resources=state.project_resources()
        with patch.object(module,'weighted_node_threads',wraps=module.weighted_node_threads)as allocation:
            actual=state._growth_headroom({},resources)
            calls=allocation.call_count
        state.__class__=UncachedCenter
        with patch.object(module,'weighted_node_threads',wraps=module.weighted_node_threads)as allocation:
            expected=state._growth_headroom({},resources)
            old_calls=allocation.call_count
        self.assertEqual(actual,expected)
        self.assertEqual(calls,1)
        self.assertEqual(old_calls,192)


if __name__=='__main__':unittest.main()
