"""Zero CPU is authoritative only for a rejection before worker submission."""
import unittest
from pathlib import Path
from unittest.mock import patch

from remote_compute import Pool,RemoteError,command
from remote_compute.coordinator import Coordinator
from remote_compute.worker import missing_task_assets
from test_center import center
from test_compute import provider,scratch


class PreExecutionCostTests(unittest.TestCase):
    def test_missing_asset_has_a_native_zero_cost_receipt_without_worker_rpc(self):
        with scratch()as root,provider(Path(root)/'worker')as(worker,remote):
            with Pool([remote])as pool,center(pool,Path(root)/'center')as(_,client):
                with patch.object(worker,'submit',side_effect=AssertionError('No worker submission'))as submit, \
                     patch.object(worker,'artifacts',side_effect=AssertionError('No worker artifacts'))as artifacts, \
                     patch.object(worker,'delete',side_effect=AssertionError('No worker deletion'))as delete:
                    result=client.run(command(['${tool:python}','-c','raise AssertionError()'],
                        env={'MODEL':'${asset:missing}'},max_cpus=1,retries=0))
                self.assertEqual(result['status'],'failed')
                self.assertEqual(result['error_kind'],'pre_execution')
                self.assertEqual(result['cpu_seconds'],0)
                self.assertFalse(result['task_process_started'])
                self.assertIsNone(result['started'])
                self.assertEqual(result['steps'],[])
                self.assertEqual(client.statuses([result['id']])[result['id']]['status'],'unknown')
                submit.assert_not_called();artifacts.assert_not_called();delete.assert_not_called()

    def test_real_process_failure_keeps_measured_cost_and_execution_evidence(self):
        with scratch()as root,provider(Path(root)/'worker')as(_,remote):
            with Pool([remote])as pool,center(pool,Path(root)/'center')as(_,client):
                code="import time;end=time.process_time()+.04\nwhile time.process_time()<end:pass\nraise KeyError('generic_evaluator')"
                result=client.run(command(['${tool:python}','-c',code],max_cpus=1,retries=0))
                self.assertEqual(result['status'],'failed')
                self.assertGreater(result['cpu_seconds'],0)
                self.assertIsNotNone(result['started'])
                self.assertEqual(len(result['steps']),1)
                self.assertNotIn('rejected_before_submit',result)

    def test_an_unstructured_submission_error_keeps_cpu_unknown(self):
        with scratch()as root,provider(Path(root)/'worker')as(_,remote):
            with Pool([remote])as pool,center(pool,Path(root)/'center')as(_,client):
                error=RemoteError('Unknown remote job');error.status_code=404
                with patch.object(pool.nodes[0].client,'submit_bundle',side_effect=error):
                    result=client.run(command(['${tool:python}','-c','pass'],max_cpus=1,retries=0))
                self.assertEqual(result['status'],'interrupted')
                self.assertNotIn('cpu_seconds',result)
                self.assertNotIn('rejected_before_submit',result)

    def test_reference_check_uses_resolved_fields_and_nested_execution_values(self):
        task=dict(label='${asset:literal-label}',env={'X':'${asset:known}'},assets={'known':'a'*64},
                  execution=dict(kwargs=dict(nested=['${asset:nested}'])))
        self.assertEqual(missing_task_assets(task),['nested'])
        task['execution']={}
        self.assertEqual(missing_task_assets(task),[])

    def test_zero_current_attempt_does_not_erase_paid_or_unknown_attempts(self):
        result=dict(cpu_seconds=0,attempts=[dict(cpu_seconds=7.25)])
        Coordinator._attempt_total(result);self.assertEqual(result['total_cpu_seconds'],7.25)
        result['attempts'].append(dict(cost_incomplete=True))
        Coordinator._attempt_total(result);self.assertIsNone(result['total_cpu_seconds'])
