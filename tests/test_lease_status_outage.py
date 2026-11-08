"""Transient status reads preserve a previously validated lease, not a seed."""
import json
import os
from pathlib import Path
import threading
import time
import unittest
from unittest.mock import patch
from remote_compute import Client, Pool, RemoteError, python_map
from remote_compute.worker import Conflict, Handler
from test_compute import provider, scratch, until
from test_center import center


class LeaseStatusOutageTests(unittest.TestCase):
    phase=dict(base_memory_mb=512,memory_per_cpu_mb=1024)
    identity='a'*32

    def refused(self, timeout=False):
        e=RemoteError('Center resource reply timed out; existing grant retained' if timeout else
                      'Insufficient memory for resource resize; existing grant retained')
        e.status_code=409;return e

    def unavailable(self, code=None):
        e=RemoteError('The read operation timed out');e.status_code=code;e.transport_retryable=code is None
        return e

    def test_cached_confirmed_phase_continues_after_status_read_timeout(self):
        for active in (0,2,4):
            client=Client();initial=dict(cpus=4,memory_mb=128,memory_phase=self.phase)
            with self.subTest(active=active),patch.object(client,'resize',side_effect=[initial,self.refused(),dict(cpus=8,memory_mb=256,memory_phase=self.phase)]), \
                    patch.object(client,'status',side_effect=self.unavailable()),patch('remote_compute.leases.time.monotonic',return_value=10)as clock:
                lease=client.cpu_lease(self.identity,max_cpus=8,**self.phase)
                self.assertEqual(lease.refresh(ready_cpus=8),4)
                clock.return_value=11;self.assertEqual(lease.refresh(active_cpus=active,ready_cpus=8-active),4)
                clock.return_value=12;self.assertEqual(lease.refresh(active_cpus=active,ready_cpus=8-active),8)

    def test_unanswered_resize_keeps_only_its_active_floor_during_read_outage(self):
        for active,expected in ((0,1),(2,2)):
            client=Client()
            with self.subTest(active=active),patch.object(client,'resize',side_effect=[dict(cpus=4,memory_mb=128),self.refused(timeout=True)]), \
                    patch.object(client,'status',side_effect=self.unavailable()),patch('remote_compute.leases.time.monotonic',return_value=10)as clock:
                lease=client.cpu_lease(self.identity,max_cpus=8,**self.phase);lease.refresh(ready_cpus=8)
                clock.return_value=11
                self.assertEqual(lease.refresh(active_cpus=active,ready_cpus=8-active),expected)

    def test_cached_grant_does_not_mask_authorization_or_terminal_errors(self):
        for code in (403,404,409):
            client=Client()
            with self.subTest(code=code),patch.object(client,'resize',side_effect=[dict(cpus=4,memory_mb=128),self.refused()]), \
                    patch.object(client,'status',side_effect=self.unavailable(code)),patch('remote_compute.leases.time.monotonic',return_value=10)as clock:
                lease=client.cpu_lease(self.identity,max_cpus=8,**self.phase);lease.refresh(ready_cpus=8)
                clock.return_value=11
                with self.assertRaises(RemoteError):lease.refresh(active_cpus=2,ready_cpus=6)
        client=Client()
        with patch.object(client,'resize',side_effect=[dict(cpus=4,memory_mb=128),self.refused()]), \
                patch.object(client,'status',return_value=dict(status='cancelled',cpus=4,memory_mb=128,memory_phase=self.phase)), \
                patch('remote_compute.leases.time.monotonic',return_value=10)as clock:
            lease=client.cpu_lease(self.identity,max_cpus=8,**self.phase);lease.refresh(ready_cpus=8)
            clock.return_value=11
            with self.assertRaises(RemoteError):lease.refresh(active_cpus=2,ready_cpus=6)

    def test_proxy_gateway_timeout_retries_observation_without_replaying_resize(self):
        client=Client();snapshot=dict(status='running',cpus=4,memory_mb=128,memory_phase=self.phase)
        with patch.object(client,'resize',side_effect=self.refused())as resize, \
                patch.object(client,'status',side_effect=[self.unavailable(504),self.unavailable(504),snapshot])as status:
            self.assertEqual(client.cpu_lease(self.identity,max_cpus=8,**self.phase).refresh(ready_cpus=8),4)
            self.assertEqual(resize.call_count,1);self.assertEqual(status.call_count,3)

    def test_initial_observation_stops_at_the_original_deadline(self):
        client=Client();env=dict(REMOTE_COMPUTE_JOB_ID=self.identity,REMOTE_COMPUTE_URL=client.url,
                                REMOTE_COMPUTE_DEADLINE=str(time.monotonic()-1))
        with patch.dict(os.environ,env),patch.object(client,'resize',side_effect=self.refused()), \
                patch.object(client,'status',side_effect=self.unavailable()):
            with self.assertRaisesRegex(TimeoutError,'execution deadline reached'):
                client.cpu_lease(self.identity,max_cpus=8,**self.phase).refresh(ready_cpus=8)

    def test_initial_seed_waits_for_a_valid_current_snapshot(self):
        client=Client();env=dict(REMOTE_COMPUTE_JOB_ID=self.identity,REMOTE_COMPUTE_URL=client.url,
                                REMOTE_COMPUTE_MEMORY_MODEL=json.dumps(self.phase))
        snapshot=dict(status='running',cpus=1,memory_mb=256,memory_phase=None)
        with patch.dict(os.environ,env),patch.object(client,'resize',side_effect=self.refused()), \
                patch.object(client,'status',side_effect=[self.unavailable(),snapshot])as status:
            lease=client.cpu_lease(self.identity,max_cpus=8,**self.phase)
            self.assertEqual(lease.refresh(ready_cpus=8),1)
            self.assertEqual(status.call_count,2)

    def test_phase_change_cannot_reuse_a_cached_working_set(self):
        client=Client()
        with patch.object(client,'resize',side_effect=[dict(cpus=4,memory_mb=128),self.refused()]), \
                patch.object(client,'status',side_effect=self.unavailable()), \
                patch.object(client,'_read_reconnecting',return_value=dict(status='running',cpus=4,memory_mb=128,memory_phase=self.phase))as recover, \
                patch('remote_compute.leases.time.monotonic',return_value=10)as clock:
            lease=client.cpu_lease(self.identity,max_cpus=8,**self.phase);lease.refresh(ready_cpus=8)
            lease.memory=dict(memory_mb=8192);clock.return_value=11
            with self.assertRaises(RemoteError):lease.refresh(ready_cpus=8)
            recover.assert_called_once()

    def test_different_phase_can_use_a_fresh_snapshot_that_funds_its_full_model(self):
        client=Client();current=dict(status='running',cpus=4,memory_mb=128,memory_phase=self.phase)
        with patch.object(client,'resize',side_effect=self.refused()),patch.object(client,'status',return_value=current):
            lease=client.cpu_lease(self.identity,max_cpus=8,memory_mb=64)
            self.assertEqual(lease.refresh(ready_cpus=8),4)
            self.assertEqual(lease.confirmed['memory_phase'],dict(memory_mb=64))

    def test_real_map_finishes_after_worker_status_reads_time_out(self):
        for backend in ('process','thread'):
            with self.subTest(backend=backend),scratch()as folder:
                root=Path(folder)
                with provider(root/'worker',cpus=4)as(worker,remote),Pool([remote])as pool,center(pool,root/'center')as(state,client):
                    reject=threading.Event();hits=[];lock=threading.Lock();original_resize=state.resize;original_handle=Handler._handle;original_callback=state._callback
                    def resize(*args,**options):
                        if reject.is_set():raise Conflict('Insufficient memory for resource resize; existing grant retained')
                        return original_resize(*args,**options)
                    def callback(node):return dict(original_callback(node),timeout=.2)
                    def handle(handler):
                        delayed=False
                        if handler.command=='GET'and handler.server.worker is worker and handler.path==('/jobs/'+identity) and handler.headers.get('X-DCO-Brief')!='1' and reject.is_set():
                            with lock:
                                if len(hits)<2:hits.append(time.monotonic());delayed=True
                        if delayed:time.sleep(.5)
                        return original_handle(handler)
                    source='from pathlib import Path\nimport time\ndef one(item):\n Path("started").write_text("yes")\n time.sleep(.6)\n return item\n'
                    identity=''
                    with patch.object(state,'resize',side_effect=resize),patch.object(state,'_callback',side_effect=callback), \
                            patch.object(Handler,'do_GET',handle):
                        identity=client.submit(python_map('outage:one',list(range(20)),backend=backend,max_cpus=4,
                            base_memory_mb=512,memory_per_cpu_mb=1024,retries=0,timeout=35),files={'outage.py':source})['id']
                        try:
                            def started():
                                try:return client.read_job_file(identity,'started')==b'yes'
                                except RemoteError as e:
                                    if e.status_code==404:return False
                                    raise
                            until(started,seconds=8);reject.set()
                            until(lambda:len(hits)==2,seconds=10);reject.clear()
                            result=client.wait(identity,wait_timeout=20)
                            self.assertEqual(result['status'],'succeeded',result)
                            self.assertEqual(result['attempts'],[])
                            result=client.fetch(identity,root/'result')
                            saved=json.loads((root/'result/files/.dco-results.json').read_bytes())
                            self.assertEqual([row['value']for row in saved['items']],list(range(20)))
                        finally:
                            reject.clear()
                            if client.statuses([identity])[identity]['status'] not in ('unknown','succeeded','failed','cancelled','interrupted'):
                                client.cancel(identity);client.wait(identity)


if __name__=='__main__':unittest.main()
