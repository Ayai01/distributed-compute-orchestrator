"""A cancelled original task publishes its worker's final interrupted receipt."""
from types import SimpleNamespace
import threading
import time
import unittest
from unittest.mock import Mock

from remote_compute.coordinator import Coordinator


class CancelInterruptionTests(unittest.TestCase):
    def test_cancelled_original_keeps_terminal_outcome_and_cost_without_retry(self):
        for cpu in (0.,7.25):
            with self.subTest(cpu_seconds=cpu):
                identity='a'*32
                remote=dict(id=identity,status='interrupted',cpu_seconds=cpu,
                    started=None,finished=time.time(),steps=[],error='Worker stopped before completion')
                client=Mock();client.status.return_value=remote
                node=SimpleNamespace(client=client,detached=False)
                task=dict(cpus=1,memory_mb=512)
                cancel=threading.Event();cancel.set()
                job=dict(node=node,task=task,cancel=cancel,reattach=True,
                    result=dict(id=identity,status='running',submitted=time.time()-10,cpu_seconds=None))
                state=Coordinator.__new__(Coordinator)
                state.condition=threading.Condition();state.stopping=False
                state.pool=SimpleNamespace(_release=Mock());state.jobs={identity:job}
                state.threads={threading.current_thread()};state._save=Mock();state._retry=Mock()
                state._execute(job)
                self.assertEqual(job['result']['id'],identity)
                self.assertEqual(job['result']['status'],'interrupted')
                self.assertEqual(job['result']['cpu_seconds'],cpu)
                self.assertEqual(job['result']['error'],remote['error'])
                client.cancel.assert_called_once_with(identity)
                state._retry.assert_not_called()
                state.pool._release.assert_called_once_with(node,task,remote,job_id=identity)
                self.assertFalse(state.threads)


if __name__=='__main__':unittest.main()
