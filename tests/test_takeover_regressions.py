"""Admission reservations, process finalization and native memory recovery."""
from contextlib import redirect_stderr,redirect_stdout
import io
import json
from pathlib import Path
import threading
import unittest
from unittest.mock import patch

from remote_compute.bundle import progress_fields
from remote_compute.client import Node
from remote_compute.cli import main
from remote_compute.progress import diagnose
from test_compute import provider,python,scratch,until


class TakeoverRegressions(unittest.TestCase):
    def test_unused_host_ram_does_not_override_funded_worker_reservations(self):
        for accounting in ('rss',None):
            with self.subTest(accounting=accounting):
                node=Node('host',None)
                node.health=dict(memory_capacity_mb=8192,memory_reserved_mb=4096,
                    memory_available_mb=0,host_memory_available_mb=8192,
                    host_commit_available_mb=8192,memory_headroom_mb=1024,
                    memory_accounting=accounting)
                self.assertEqual(node.memory_free,0)
                node.health.update(memory_available_mb=2048,memory_pressure_state=dict(limited=True))
                self.assertEqual(node.memory_free,0)
                node.health['memory_pressure_state']['limited']=False
                self.assertEqual(node.memory_free,2048)

    def test_diagnosis_distinguishes_unstarted_exited_and_missing_processes(self):
        base=dict(status='running',cpus=1,active_cpus=1)
        cases=[(dict(cpu_seconds=0,process_alive=False),'preparing'),
               (dict(cpu_seconds=0,process_alive=False,returncode=0),'finishing'),
               (dict(cpu_seconds=2,process_alive=False,returncode=1),'finishing'),
               (dict(cpu_seconds=2,process_alive=False),'process_lost')]
        for runtime,code in cases:
            job=dict(base,runtime=runtime)
            self.assertEqual(diagnose(job)['code'],code)
            fields=progress_fields(job)
            self.assertEqual(fields['diagnostic']['code'],code)
            self.assertEqual(fields['runtime'],runtime)
        self.assertEqual(diagnose(dict(base,runtime=dict(process_alive=False),resource_request=dict(cpus=4)))['code'],'resource_wait')

    def test_real_exited_process_is_finishing_while_its_outputs_are_retained(self):
        entered=threading.Event();release=threading.Event()
        with scratch()as root,provider(Path(root)/'worker')as(worker,client):
            clean=worker._clean_work
            def hold(job):
                entered.set();release.wait(5);return clean(job)
            with patch.object(worker,'_clean_work',side_effect=hold):
                identity=client.submit(python("print('done')"))['id']
                try:
                    self.assertTrue(entered.wait(5))
                    row=client.status(identity)
                    self.assertEqual(row['status'],'running')
                    self.assertEqual(row['runtime']['returncode'],0)
                    self.assertEqual(row['diagnostic']['code'],'finishing')
                    rows=client.health()['active_jobs']
                    self.assertEqual(next(j for j in rows if j['id']==identity)['diagnostic']['code'],'finishing')
                finally:release.set()
                self.assertEqual(client.wait(identity,wait_timeout=5)['status'],'succeeded')
                self.assertEqual(client.fetch(identity)['stdout'].strip(),'done')
                until(lambda:worker.used==0)

    def test_service_os_memory_errors_reach_the_native_recovery_exit(self):
        with scratch()as root:
            root=Path(root);config=root/'.local/worker.json';config.parent.mkdir(parents=True);config.write_text('{}')
            errors=[MemoryError('allocation'),OSError(12,'allocation')]
            for number in (8,14,1450,1455,5):
                error=OSError('native error');error.winerror=number;errors.append(error)
            for error in errors:
                expected=2 if getattr(error,'winerror',None)==5 else 76
                with self.subTest(error=repr(error)), \
                        patch('remote_compute.cli.ROOT',root), \
                        patch('remote_compute.cli.require_visible'), \
                        patch('remote_compute.cli.handoff_python',return_value=False), \
                        patch('remote_compute.cli.resolve',return_value=None), \
                        patch('remote_compute.cli.run_scheduler',side_effect=error), \
                        redirect_stderr(io.StringIO()),redirect_stdout(io.StringIO()):
                    code=main(['--service-child','--mode','scheduler','--config',str(config),'--port','0'])
                    self.assertEqual(code,expected)
                    self.assertTrue((root/'.local/diagnostics/startup-error.log').is_file())


if __name__=='__main__':unittest.main()
