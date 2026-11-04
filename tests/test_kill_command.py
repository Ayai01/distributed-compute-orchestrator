"""Kill uses current tasks, terminates execution and keeps the original receipt."""
from contextlib import redirect_stdout
import io
from pathlib import Path
import unittest
from unittest.mock import Mock, patch

from remote_compute import Pool
from remote_compute.cli import main
from remote_compute.commands import active_job_identity, SchedulerShell, WorkerShell
from test_center import center
from test_compute import provider, python, scratch, until


class KillCommandTests(unittest.TestCase):
    def test_short_id_reads_live_state_and_rejects_ambiguity(self):
        client=Mock()
        one='abcd'+'1'*28;two='abcd'+'2'*28
        client.health.return_value=dict(nodes=[dict(active_jobs=[dict(id=one)])],queued_jobs=[])
        self.assertEqual(active_job_identity(client,'abcd'),one)
        client.status.assert_not_called();client.status_page.assert_not_called()
        client.health.return_value['queued_jobs']=[dict(id=two)]
        with self.assertRaises(ValueError):active_job_identity(client,'abcd')
        client.cancel.assert_not_called()

    def test_full_terminal_id_reports_already_finished_without_cancel(self):
        client=Mock();client.status.return_value=dict(status='succeeded')
        with self.assertRaisesRegex(ValueError,'任务已结束'):active_job_identity(client,'a'*32)
        client.cancel.assert_not_called()

    def test_cli_kill_terminates_one_original_task_and_preserves_its_receipt(self):
        with scratch()as root,provider(Path(root)/'worker')as(_,worker):
            with Pool([worker])as pool,center(pool,Path(root)/'center')as(state,client):
                identity=client.submit(python('import time;time.sleep(30)',retries=0))['id']
                until(lambda:client.status(identity)['status']=='running')
                original=client.status(identity)
                with redirect_stdout(io.StringIO())as output,patch.dict('os.environ',REMOTE_COMPUTE_OPERATOR_TOKEN=state.operator_token):
                    code=main(['--url',client.url,'kill',identity[:8]])
                self.assertEqual(code,0)
                self.assertIn('cancelled',output.getvalue())
                result=client.wait(identity,wait_timeout=8)
                self.assertEqual(result['status'],'cancelled',result)
                self.assertEqual(result['request_sha256'],original['request_sha256'])
                self.assertEqual(result['id'],identity)
                self.assertIsNotNone(result['cpu_seconds'])
                self.assertEqual(client.fetch(identity)['id'],identity)

    def test_console_kill_queued_and_worker_running_task(self):
        with scratch()as root,provider(Path(root)/'worker')as(state,worker):
            with Pool([worker])as pool,center(pool,Path(root)/'center')as(_,client):
                client.control(paused=True)
                queued=client.submit(python('raise RuntimeError("must not run")'))['id']
                with redirect_stdout(io.StringIO()):SchedulerShell(pool,client).do_kill(queued[:8])
                self.assertEqual(client.status(queued)['status'],'cancelled')
            identity=worker.submit(python('import time;time.sleep(30)'))['id']
            until(lambda:worker.status(identity)['status']=='running')
            with redirect_stdout(io.StringIO()):WorkerShell(state).do_kill(identity[:8])
            self.assertEqual(worker.status(identity)['status'],'cancelled')

    def test_cli_kill_of_an_explicit_worker_url_needs_no_device_name(self):
        with scratch()as root,provider(root)as(_,worker):
            identity=worker.submit(python('import time;time.sleep(30)'))['id']
            until(lambda:worker.status(identity)['status']=='running')
            import socket
            with redirect_stdout(io.StringIO()),patch('remote_compute.cli.socket',wraps=socket)as net:
                net.gethostname.return_value='operator-computer'
                self.assertEqual(main(['--url',worker.url,'kill',identity[:8]]),0)
            self.assertEqual(worker.status(identity)['status'],'cancelled')


if __name__=='__main__':unittest.main()
