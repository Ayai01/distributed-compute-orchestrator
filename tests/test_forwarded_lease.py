"""Peer TLS EOFs retain funded active work across the lease forwarding hop."""
import json
import ssl
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import Mock,patch

from remote_compute import Client,Pool,RemoteError,python_map
from remote_compute.client import RESOURCE_TRANSPORT_RETAINED
from remote_compute.worker import Conflict
from test_center import center
from test_compute import provider,scratch,until

EOF="https://peer/jobs/task: [SSL: UNEXPECTED_EOF_WHILE_READING] EOF occurred in violation of protocol"


class ForwardedLeaseTests(unittest.TestCase):
    def test_resource_route_normalizes_forwarded_eof_but_keeps_permanent_errors(self):
        client=Client()
        for status,message,retained in ((409,EOF,True),(403,EOF,False),
                (409,'CERTIFICATE_VERIFY_FAILED',False),(409,'Only a running task can resize',False)):
            with self.subTest(status=status,message=message):
                error=RemoteError(message);error.status_code=status
                with patch.object(client,'request',side_effect=error)as request,self.assertRaises(RemoteError)as caught:
                    client.resize('a'*32,4,active_cpus=1)
                self.assertEqual(RESOURCE_TRANSPORT_RETAINED in str(caught.exception),retained)
                self.assertEqual(request.call_count,1)

    def test_tls_eof_is_retryable_and_certificate_failure_is_not(self):
        for kind,expected in ((ssl.SSLEOFError,True),(ssl.SSLCertVerificationError,False)):
            with self.subTest(error=kind):
                connection=Mock();connection.request.side_effect=kind(8,'simulated TLS failure')
                with patch('remote_compute.client.http.client.HTTPConnection',return_value=connection), \
                     patch('remote_compute.client.time.sleep'),self.assertRaises(RemoteError)as caught:
                    Client('http://127.0.0.1:1',timeout=.1).health()
                self.assertEqual(caught.exception.transport_retryable,expected)

    def test_fresh_or_cached_snapshot_cannot_reuse_uncertain_idle_slots(self):
        phase=dict(memory_mb=512)
        for cached in (False,True):
            for active in (0,2):
                with self.subTest(cached=cached,active=active):
                    client=Client();error=RemoteError(RESOURCE_TRANSPORT_RETAINED);error.status_code=409
                    unavailable=RemoteError('local status timed out');unavailable.transport_retryable=True
                    with patch.object(client,'resize',side_effect=[dict(cpus=4,memory_mb=512),error]), \
                         patch.object(client,'status',side_effect=unavailable if cached else None,
                            return_value=dict(status='running',cpus=4,memory_mb=512,memory_phase=phase)), \
                         patch('remote_compute.leases.time.monotonic',return_value=10)as clock:
                        lease=client.cpu_lease('a'*32,max_cpus=8,**phase);lease.refresh(ready_cpus=8)
                        clock.return_value=11
                        self.assertEqual(lease.refresh(active_cpus=active,ready_cpus=8-active),max(1,active))

    def test_real_polling_map_keeps_completed_items_through_forwarded_eof(self):
        source='''import time
from pathlib import Path
def one(index):
 p=Path('called-'+str(index));count=int(p.read_text())+1 if p.exists()else 1;p.write_text(str(count))
 time.sleep(.2)
 return [index,count]
'''
        for typed,backend in ((False,'process'),(True,'thread')):
            with self.subTest(typed=typed,backend=backend),scratch()as folder:
                root=Path(folder)
                with provider(root/'worker',cpus=4)as(worker,remote),Pool([remote])as pool,center(pool,root/'center')as(state,client):
                    trigger=threading.Event();hits=[];original=state.resize
                    def resize(*args,**options):
                        if trigger.is_set()and not hits:
                            hits.append(dict(active=options.get('active_cpus'),requested=options.get('cpus')))
                            if typed:
                                error=RemoteError(EOF);error.transport_retryable=True;raise error
                            raise Conflict(EOF)
                        return original(*args,**options)
                    identity=None
                    with patch.object(state,'resize',side_effect=resize):
                        try:
                            identity=client.submit(python_map('outage:one',list(range(48)),backend=backend,
                                max_cpus=4,memory_mb=512,retries=0,timeout=35),files={'outage.py':source})['id']
                            def prefix():
                                path=root/'worker/jobs'/identity/'input/.dco-results.json'
                                try:return json.loads(path.read_bytes()).get('completed',0)>=5
                                except (FileNotFoundError,PermissionError,ValueError):return False
                            until(prefix,seconds=12);trigger.set();until(lambda:bool(hits),seconds=8)
                            result=client.wait(identity,wait_timeout=30)
                            self.assertEqual(result['status'],'succeeded',result)
                            self.assertEqual(result['attempts'],[])
                            result=client.fetch(identity,root/'result')
                            values=json.loads((root/'result/files/.dco-results.json').read_bytes())['items']
                            self.assertEqual([r['value']for r in values],[[i,1]for i in range(48)])
                            self.assertGreater(result['cpu_seconds'],0)
                        finally:
                            trigger.clear()
                            if identity and client.statuses([identity])[identity]['status']not in ('unknown','succeeded','failed','cancelled','interrupted'):
                                client.cancel(identity);client.wait(identity);client.fetch(identity)
