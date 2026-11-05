import copy,threading,unittest
from types import SimpleNamespace
from unittest.mock import patch
from remote_compute.client import Node,Pool
from remote_compute.coordinator import _matching_pass

class PlanningPassTests(unittest.TestCase):
    def pool(self):
        pool=Pool.__new__(Pool);pool.condition=threading.Condition();pool.nodes=[]
        for name in ['one','two']:
            node=Node(name,SimpleNamespace(url=name))
            node.health=dict(cpus=4,memory_capacity_mb=256,tags=['ml','ng47'],
                tools={'python':{'available':True},'ng47':{'available':True}},
                shared=['model'],python_backends=['cpu','ml'])
            pool.nodes.append(node)
        pool.nodes[1].health.update(cpus=2,tags=['ml'],shared=[])
        return pool

    def test_distinct_execution_constraints_keep_their_exact_provider_sets(self):
        pool=self.pool();matching=_matching_pass(pool)
        base=dict(cpus=1,memory_mb=128,requires=['ml'],steps=[['${tool:python}','-c','pass']])
        variants=[{},dict(cpus=3),dict(memory_mb=512),dict(node='two'),dict(requires=['ng47']),
            dict(env={'INPUT':'${shared:model}'}),dict(steps=[['${tool:ng47}','-b','input.sp']]),
            dict(accelerator={'kind':'cuda','count':1})]
        for change in variants:
            task=dict(base,**change)
            self.assertEqual([n.name for n in matching(task)],[n.name for n in pool._matching(task)])

    def test_a_new_pass_observes_provider_budget_tools_and_backend_changes(self):
        pool=self.pool();node=pool.nodes[0]
        task=dict(cpus=3,memory_mb=128,requires=['ml'],steps=[['${tool:python}']])
        before=copy.deepcopy(node.health)
        self.assertEqual([n.name for n in _matching_pass(pool)(task)],['one'])
        for changes in [dict(cpus=2),dict(memory_capacity_mb=64),dict(tags=[]),
                dict(tools={'python':{'available':False}}),dict(python_backends=['cpu'])]:
            node.health=dict(copy.deepcopy(before),**changes)
            self.assertEqual(_matching_pass(pool)(task),())

    def test_queue_size_does_not_repeat_provider_scans_for_identical_constraints(self):
        pool=self.pool();tasks=[dict(cpus=1,memory_mb=128,requires=['ml'],
            steps=[['${tool:python}','-c','seed='+str(i)]]) for i in range(512)]
        tasks.extend(dict(task,node='one')for task in tasks[:16])
        with patch.object(pool,'_matching',wraps=pool._matching)as scan:
            matching=_matching_pass(pool)
            rows=[[n.name for n in matching(task)]for task in tasks]
            self.assertEqual(scan.call_count,2)
        self.assertTrue(all(row==['one','two']for row in rows[:512]))
        self.assertTrue(all(row==['one']for row in rows[512:]))
