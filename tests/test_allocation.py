"""Feasible weighted shares across pinned and overlapping node sets."""
import unittest
from remote_compute.allocation import weighted_node_threads


class AllocationTests(unittest.TestCase):
    def check(self,capacities,shares,demands,rooms,expected):
        grants=weighted_node_threads(capacities,shares,demands,rooms)
        self.assertEqual({p:sum(row.values()) for p,row in grants.items()},expected)
        for node,capacity in capacities.items():
            self.assertLessEqual(sum(row.get(node,0) for row in grants.values()),capacity)
        for project,row in grants.items():
            self.assertLessEqual(sum(row.values()),demands[project])
            for node,count in row.items():
                self.assertLessEqual(count,rooms[project][node])

    def test_shared_windows_capacity_cannot_be_spent_again_as_linux_credit(self):
        self.check(dict(PC=12,TX=30,Server=96),dict(ARH=1,Bench=1),dict(ARH=97,Bench=486),
                   dict(ARH=dict(PC=1,TX=30),Bench=dict(PC=12,TX=30,Server=8)),dict(ARH=25,Bench=25))

    def test_flexible_project_moves_to_other_node_to_fill_a_restricted_share(self):
        self.check(dict(one=4,two=4),dict(A=1,B=1),dict(A=8,B=8),
                   dict(A=dict(one=4,two=4),B=dict(one=4)),dict(A=4,B=4))

    def test_a_small_demand_is_counted_once_across_all_eligible_nodes(self):
        self.check(dict(one=8,two=8),dict(A=1,B=1),dict(A=1,B=100),
                   dict(A=dict(one=8,two=8),B=dict(one=8,two=8)),dict(A=1,B=15))

    def test_weighted_integer_shares_and_an_exhausted_project_lend_all_slots(self):
        self.check(dict(one=58),dict(A=2,B=1),dict(A=100,B=100),
                   dict(A=dict(one=58),B=dict(one=58)),dict(A=39,B=19))
        self.check(dict(one=58),dict(A=1,B=1,C=1),dict(A=1,B=100,C=100),
                   {p:dict(one=58) for p in ('A','B','C')},dict(A=1,B=29,C=28))
