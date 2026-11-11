"""Kill uses current tasks, terminates execution and keeps the original receipt."""
from contextlib import redirect_stdout
import io
from pathlib import Path
import unittest
from unittest.mock import Mock, patch

from remote_compute import Pool
from remote_compute.cli import main
from remote_compute.commands import active_job_identity, kill_job,kill_all, SchedulerShell, WorkerShell, RemoteWorker
from test_center import center
from test_compute import provider, python, scratch, until


class KillCommandTests(unittest.TestCase):
    def test_single_unconfirmed_or_timed_out_cancellation_is_incomplete(self):
        from remote_compute import RemoteError
        client=Mock();identity='a'*32;client.status.return_value=dict(id=identity,status='running')
        client.cancel.return_value=dict(id=identity,status='interrupted',remote_cancel_pending=True)
        self.assertEqual(kill_job(client,identity)['pending'],[identity])
        client.cancel.side_effect=RemoteError('control unavailable')
        self.assertEqual(kill_job(client,identity)['status'],'incomplete')
        client.wait.assert_not_called()

    def test_all_initial_status_failure_returns_original_ids_without_cancelling(self):
        from remote_compute import RemoteError
        client=Mock();identity='a'*32
        client.health.return_value=dict(queued_jobs=[dict(id=identity,status='queued')])
        client.statuses.side_effect=RemoteError('status unavailable')
        result=kill_all(client)
        self.assertEqual(result['status'],'incomplete');self.assertEqual(result['pending'],[identity])
        client.cancel.assert_not_called()

    def test_all_snapshots_original_ids_and_does_not_touch_future_or_finished_work(self):
        client=Mock();one='1'*32;two='2'*32;done='3'*32;late='4'*32
        client.health.return_value=dict(queued_jobs=[dict(id=one,status='queued'),dict(id=done,status='succeeded')],
            nodes=[dict(active_jobs=[dict(id=two,status='running'),dict(id=one,status='queued')])])
        client.statuses.return_value={one:dict(id=one,status='queued'),two:dict(id=two,status='running')}
        def cancel(identity):
            client.health.return_value['queued_jobs'].append(dict(id=late,status='queued'))
            return dict(id=identity,status='cancelled')
        client.cancel.side_effect=cancel
        result=kill_all(client)
        self.assertEqual(result['status'],'completed')
        self.assertEqual((result['matched'],result['finished']),(2,2))
        self.assertEqual({call.args[0]for call in client.cancel.call_args_list},{one,two})
        client.health.assert_called_once();client.status_page.assert_not_called()
        client.submit.assert_not_called();client.delete.assert_not_called()

    def test_all_reports_missing_and_unconfirmed_remote_cancellation(self):
        client=Mock();one='1'*32;two='2'*32
        client.health.return_value=dict(active_jobs=[dict(id=one),dict(id=two)])
        client.statuses.return_value={one:dict(id=one,status='running'),two:dict(id=two,status='unknown')}
        client.cancel.return_value=dict(id=one,status='interrupted',remote_cancel_pending=True,cost_incomplete=True)
        result=kill_all(client)
        self.assertEqual(result['status'],'incomplete')
        self.assertEqual(set(result['pending']),{one,two})
        self.assertTrue(result['jobs'][0]['remote_cancel_pending'])
        client.cancel.assert_called_once_with(one)

    def test_all_checks_full_terminal_receipt_and_reconciles_lost_cancel_reply(self):
        from remote_compute import RemoteError
        client=Mock();one='1'*32;two='2'*32
        client.health.return_value=dict(active_jobs=[dict(id=one),dict(id=two)])
        client.statuses.side_effect=[{one:dict(id=one,status='running'),two:dict(id=two,status='running')},
                                    {one:dict(id=one,status='cancelled'),two:dict(id=two,status='interrupted')}]
        client.cancel.side_effect=RemoteError('lost response')
        client.status.side_effect=lambda identity:dict(id=identity,status='cancelled'if identity==one else'interrupted',
                                                     remote_cancel_pending=identity==two)
        result=kill_all(client)
        self.assertEqual(result['pending'],[two])
        self.assertNotIn(one,result['errors'])
        self.assertEqual(client.status.call_count,2)

    def test_all_empty_and_timeout_are_explicit(self):
        client=Mock();client.health.return_value={}
        self.assertEqual(kill_all(client)['matched'],0)
        client.cancel.assert_not_called();client.statuses.assert_not_called()
        identity='a'*32;client.health.return_value=dict(active_jobs=[dict(id=identity)])
        client.statuses.return_value={identity:dict(id=identity,status='running')}
        client.cancel.return_value=dict(id=identity,status='running')
        self.assertEqual(kill_all(client)['pending'],[identity])
        client.wait.assert_not_called()

    def test_cli_all_cancels_running_and_queued_tasks_and_keeps_original_outputs(self):
        with scratch()as root,provider(Path(root)/'worker')as(_,worker):
            with Pool([worker])as pool,center(pool,Path(root)/'center')as(state,client):
                finished=client.submit(python('print("finished")',retries=0))['id']
                self.assertEqual(client.wait(finished)['status'],'succeeded')
                running=client.submit(python('from pathlib import Path;Path("prefix.txt").write_text("kept");print("prefix",flush=True);import time;time.sleep(30)',
                                             outputs=['prefix.txt'],retries=0))['id']
                until(lambda:client.status(running)['status']=='running')
                until(lambda:'prefix'in client.logs(running).get('stdout',''))
                client.control(paused=True)
                queued=client.submit(python('raise RuntimeError("must not run")',retries=0))['id']
                originals={identity:client.status(identity)for identity in(running,queued)}
                with redirect_stdout(io.StringIO())as output,patch.dict('os.environ',REMOTE_COMPUTE_OPERATOR_TOKEN=state.operator_token):
                    self.assertEqual(main(['--url',client.url,'kill','all']),0)
                import json
                result=json.loads(output.getvalue())
                self.assertEqual(result['matched'],2)
                self.assertIn(result['status'],('accepted','completed'))
                for identity in(running,queued):
                    client.wait(identity,wait_timeout=5)
                    receipt=client.fetch(identity,delete=False)
                    self.assertEqual(receipt['id'],identity)
                    self.assertEqual(receipt['status'],'cancelled')
                    self.assertEqual(receipt['request_sha256'],originals[identity]['request_sha256'])
                    if identity==running:
                        self.assertEqual(receipt['files']['prefix.txt'],b'kept')
                self.assertIsNotNone(client.status(running)['cpu_seconds'])
                self.assertEqual(client.status(finished)['status'],'succeeded')
                self.assertTrue(client.health()['paused'])

    def test_cli_all_returns_nonzero_for_unconfirmed_tasks(self):
        with scratch()as root,provider(root)as(_,client):
            with redirect_stdout(io.StringIO()),patch('remote_compute.commands.kill_all',return_value=dict(status='incomplete',pending=['a'*32])):
                self.assertEqual(main(['--url',client.url,'kill','all']),1)

    def test_native_and_remote_worker_all_use_the_same_scope(self):
        for remote in(False,True):
            with self.subTest(remote=remote),scratch()as root,provider(root)as(state,client):
                client.control(paused=True)
                identities=[client.submit(python('raise RuntimeError("must not run")'))['id']for _ in range(2)]
                shell=WorkerShell(RemoteWorker(client)if remote else state)
                with redirect_stdout(io.StringIO()):shell.do_kill('all')
                self.assertTrue(all(client.status(identity)['status']=='cancelled'for identity in identities))

    def test_cancel_is_not_a_second_operator_command_or_completion(self):
        import argparse
        from contextlib import redirect_stderr
        for shell in(object.__new__(SchedulerShell),WorkerShell(Mock())):
            self.assertFalse(hasattr(shell,'do_cancel'))
            self.assertNotIn('cancel',shell.suggest('',0,{})['matches'])
        with redirect_stderr(io.StringIO()),patch.object(argparse.ArgumentParser,'exit',side_effect=ValueError('parse stopped')):
            with self.assertRaisesRegex(ValueError,'parse stopped'):main(['cancel','all'])

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
                self.assertTrue('cancelled' in output.getvalue() or 'cancel_requested' in output.getvalue())
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
            self.assertEqual(worker.wait(identity,wait_timeout=5)['status'],'cancelled')

    def test_cli_kill_of_an_explicit_worker_url_needs_no_device_name(self):
        with scratch()as root,provider(root)as(_,worker):
            identity=worker.submit(python('import time;time.sleep(30)'))['id']
            until(lambda:worker.status(identity)['status']=='running')
            import socket
            with redirect_stdout(io.StringIO()),patch('remote_compute.cli.socket',wraps=socket)as net:
                net.gethostname.return_value='operator-computer'
                self.assertEqual(main(['--url',worker.url,'kill',identity[:8]]),0)
            self.assertEqual(worker.wait(identity,wait_timeout=5)['status'],'cancelled')


if __name__=='__main__':unittest.main()
