"""Queue cleanup owns a dispatch boundary, never a stale client-side guess."""
from contextlib import redirect_stdout
import io
from pathlib import Path
import threading
import unittest
from unittest.mock import patch

from remote_compute import Client,Pool,RemoteError
from remote_compute.cli import main
from remote_compute.commands import SchedulerShell,WorkerShell
from test_center import center
from test_compute import provider,python,scratch,until


class CleanCommandTests(unittest.TestCase):
    def test_center_clean_deletes_queue_but_preserves_running_and_finished_results(self):
        with scratch()as root,provider(Path(root)/'worker')as(_,worker):
            with Pool([worker])as pool,center(pool,Path(root)/'center')as(state,client):
                finished=client.submit(python('print("finished")'))['id']
                client.wait(finished)
                running=client.submit(python('import time;time.sleep(30)'))['id']
                until(lambda:client.status(running)['status']=='running')
                client.control(paused=True)
                queued=[client.submit(python('raise RuntimeError("must not run")'))['id']for _ in range(3)]
                directories=[state.jobs[identity]['directory']for identity in queued]
                with redirect_stdout(io.StringIO()),patch.dict('os.environ',REMOTE_COMPUTE_OPERATOR_TOKEN=state.operator_token):
                    self.assertEqual(main(['--url',client.url,'clean']),0)
                self.assertTrue(all(client.statuses(queued)[identity]['status']=='unknown'for identity in queued))
                self.assertTrue(all(not p.exists()for p in directories))
                self.assertEqual(client.status(running)['status'],'running')
                self.assertEqual(client.status(finished)['status'],'succeeded')
                self.assertTrue(client.health()['paused'])
                client.cancel(running);client.wait(running)

    def test_worker_cleanup_removes_temp_and_private_asset_but_keeps_shared_asset(self):
        with scratch()as root,provider(Path(root)/'worker',temp_root=Path(root)/'temp')as(state,raw):
            client=Client(raw.url,operator=True)
            state.control(dict(paused=True))
            inputs=Path(root)/'model';inputs.mkdir();(inputs/'value').write_text('model')
            private=client.sync(inputs);(inputs/'value').write_text('shared');shared=client.sync(inputs)
            complete=client.submit(python('pass',assets={'model':shared}))['id']
            state.cancel(complete)
            queued=[client.submit(python('pass',assets={'model':asset}))['id']for asset in(private,shared)]
            dirs=[state.jobs[identity]['directory']for identity in queued]
            temp=state._temporary(state.jobs[queued[0]]);temp.mkdir(parents=True);(temp/'debris').write_text('scratch')
            result=client.clean()
            self.assertEqual(set(result['deleted']),set(queued))
            self.assertEqual(result['retired_assets'],[private])
            self.assertEqual(result['shared_assets'],[shared])
            self.assertFalse(temp.exists());self.assertTrue(all(not p.exists()for p in dirs))
            self.assertNotIn(private,state.asset_cache.entries);self.assertIn(shared,state.asset_cache.entries)
            self.assertEqual(client.status(complete)['status'],'cancelled')

    def test_cleanup_releases_dispatch_lock_and_excludes_new_submissions(self):
        with scratch()as root,provider(root)as(state,client):
            state.control(dict(paused=True))
            first=client.submit(python('pass'))['id'];late=[];original=state.delete
            def delete(identity):
                acquired=[]
                def probe():
                    with state.condition:acquired.append(True)
                thread=threading.Thread(target=probe);thread.start();thread.join(2)
                self.assertEqual(acquired,[True])
                late.append(client.submit(python('pass'))['id'])
                return original(identity)
            with patch.object(state,'delete',side_effect=delete),redirect_stdout(io.StringIO()):WorkerShell(state).do_clean('')
            self.assertEqual(client.statuses([first])[first]['status'],'unknown')
            self.assertEqual(client.status(late[0])['status'],'queued')
            self.assertEqual(state.clean()['deleted'],late)
            self.assertEqual(state.clean()['matched'],0)

    def test_cleanup_reports_delete_failure_and_retains_recoverable_receipt(self):
        with scratch()as root,provider(root)as(state,client):
            state.control(dict(paused=True));identity=client.submit(python('pass'))['id']
            with patch.object(state,'delete',side_effect=OSError('disk busy')):
                result=state.clean()
            self.assertEqual(result['status'],'incomplete');self.assertIn(identity,result['errors'])
            self.assertEqual(client.status(identity)['status'],'cancelled')
            state.delete(identity)

    def test_asset_retirement_leaves_the_dispatch_lock_available(self):
        with scratch()as root,provider(root)as(state,raw):
            client=Client(raw.url,operator=True);state.control(dict(paused=True))
            asset=client.sync_files({'model':b'data'})
            client.submit(python('pass',assets={'model':asset}))
            original=state.asset_cache.retire;acquired=[]
            def retire(identity):
                def probe():
                    with state.condition:acquired.append(True)
                thread=threading.Thread(target=probe);thread.start();thread.join(2)
                self.assertEqual(acquired,[True])
                return original(identity)
            with patch.object(state.asset_cache,'retire',side_effect=retire):
                self.assertEqual(client.clean()['status'],'completed')

    def test_endpoint_and_sdk_refuse_nonoperator_cleanup(self):
        with scratch()as root,provider(root)as(_,client):
            with self.assertRaisesRegex(RemoteError,'operator-only'):client.clean()
            with self.assertRaises(RemoteError)as rejected:client.request('/clean',{})
            self.assertEqual(rejected.exception.status_code,403)
            with Pool([client])as pool,center(pool,Path(root)/'center')as(_,operator):
                public=Client(operator.url)
                with self.assertRaises(RemoteError)as rejected:public.request('/clean',{})
                self.assertEqual(rejected.exception.status_code,403)
        # A role header cannot bypass the configured TCP trust boundary.
        with scratch()as root,provider(root,token='private',trusted_networks=[])as(_,raw):
            untrusted=Client(raw.url,operator=True)
            with self.assertRaises(RemoteError)as rejected:untrusted.clean()
            self.assertEqual(rejected.exception.status_code,401)

    def test_shell_clean_rejects_parameters_before_deleting(self):
        with scratch()as root,provider(root)as(state,client):
            with self.assertRaises(ValueError):WorkerShell(state).do_clean('all')
            with Pool([client])as pool,center(pool,Path(root)/'center')as(_,operator):
                shell=SchedulerShell(pool,operator)
                try:
                    with self.assertRaises(ValueError):shell.do_clean('all')
                finally:shell.close()


if __name__=='__main__':unittest.main()
