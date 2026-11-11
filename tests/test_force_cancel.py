"""Force control bypasses blocked polling and interrupts finalization work."""
from pathlib import Path
import os
import sys
import threading
import time
import unittest
from unittest.mock import patch

from remote_compute import Pool
from remote_compute.commands import kill_job
from test_center import center
from test_compute import provider, python, scratch, until


class ForceCancelTests(unittest.TestCase):
    @unittest.skipIf(os.name=='nt','POSIX escaped session ownership')
    def test_force_kill_captures_escaped_descendant_before_parent_reaping(self):
        from remote_compute.process import Process
        with scratch() as directory:
            root=Path(directory)
            child='import os,time;os.setsid();open("escaped.pid","w").write(str(os.getpid()));time.sleep(60)'
            parent='import subprocess,sys,time;subprocess.Popen([sys.executable,"-c",'+repr(child)+']);time.sleep(60)'
            with (root/'out').open('wb') as output:
                process=Process([sys.executable,'-c',parent],root,dict(os.environ),output,output)
                try:
                    until(lambda:(root/'escaped.pid').is_file())
                    pid=int((root/'escaped.pid').read_text())
                    self.assertFalse(process.__dict__.get('_posix_known'))
                    process.kill();process.child.wait(timeout=1)
                    def stopped():
                        try:return Path('/proc',str(pid),'stat').read_text().rsplit(')',1)[1].split()[0]=='Z'
                        except FileNotFoundError:return True
                    until(stopped,seconds=1)
                finally:
                    if (root/'escaped.pid').is_file():
                        try:os.kill(int((root/'escaped.pid').read_text()),9)
                        except ProcessLookupError:pass
                    process.stop();process.close()

    def test_kill_does_not_wait_for_directory_enumeration_and_preserves_paid_output(self):
        enumerating, release = threading.Event(), threading.Event()
        from remote_compute.bundle import tree
        def slow_tree(root, patterns, *, cancel=None):
            if cancel is None:return tree(root,patterns)
            enumerating.set()
            while not release.wait(.01):
                if cancel and cancel():raise InterruptedError('cancelled enumeration')
            return {}
        with scratch() as root, provider(root) as (worker, client):
            with patch('remote_compute.worker.tree', side_effect=slow_tree):
                identity=client.submit(python('from pathlib import Path;Path("paid.txt").write_text("kept")',
                                               outputs=['paid.txt']))['id']
                try:
                    self.assertTrue(enumerating.wait(5))
                    before=client.status(identity)['cpu_seconds'];started=time.monotonic()
                    acknowledgement=kill_job(client,identity)
                    self.assertLess(time.monotonic()-started,1)
                    self.assertIn(acknowledgement['status'],('accepted','cancelled'))
                    until(lambda:client.status(identity)['status']=='cancelled',seconds=2)
                    self.assertEqual(worker.used,0)
                    self.assertEqual(client.status(identity)['cpu_seconds'],before)
                finally:release.set()
            result=client.fetch(identity,delete=False)
            self.assertEqual(result['files']['paid.txt'],b'kept')
            client.delete(identity)

    def test_center_sends_native_kill_while_its_execution_poller_is_blocked(self):
        blocked,release=threading.Event(),threading.Event()
        with scratch() as root,provider(Path(root)/'worker') as (worker,direct):
            with Pool([direct]) as pool,center(pool,Path(root)/'center') as (_,client):
                actual=pool.nodes[0].client.status
                def read(*args,**kwargs):
                    if threading.current_thread().name=='central-job':
                        blocked.set();release.wait(8)
                    return actual(*args,**kwargs)
                with patch.object(pool.nodes[0].client,'status',side_effect=read):
                    identity=client.submit(python('from pathlib import Path;Path("ready").write_text("yes");import time;time.sleep(60)',retries=0))['id']
                    try:
                        self.assertTrue(blocked.wait(5))
                        until(lambda:bool(worker.jobs[identity].get('process')))
                        process=worker.jobs[identity]['process']
                        started=time.monotonic();ack=client.cancel(identity)
                        self.assertTrue(ack['cancel_acknowledged'])
                        self.assertLess(time.monotonic()-started,1)
                        until(lambda:process.child.poll() is not None,seconds=1)
                        self.assertFalse(release.is_set())
                    finally:release.set()
                self.assertEqual(client.wait(identity,wait_timeout=5)['status'],'cancelled')

    def test_cancellation_between_preparation_and_spawn_never_runs_body(self):
        blocked,release=threading.Event(),threading.Event()
        with scratch() as root,provider(root) as (worker,client):
            original=worker._automatic_grant
            def grant(job):blocked.set();release.wait(5);return original(job)
            with patch.object(worker,'_automatic_grant',side_effect=grant):
                identity=client.submit(python('raise RuntimeError("body must never start")'))['id']
                try:
                    self.assertTrue(blocked.wait(5));started=time.monotonic()
                    client.cancel(identity)
                    self.assertLess(time.monotonic()-started,1)
                finally:release.set()
                result=client.wait(identity,wait_timeout=5)
            self.assertEqual(result['status'],'cancelled')
            self.assertFalse(any(step.get('returncode') is not None for step in result['steps']))


if __name__=='__main__':unittest.main()
