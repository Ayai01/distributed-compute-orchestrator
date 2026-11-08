"""Lost resource replies recover the same grant operation, never task inputs."""
import json,os,socket,time,unittest
from pathlib import Path
from unittest.mock import patch
from remote_compute import Client,Pool,RemoteError,python_map
from remote_compute.worker import Handler
from test_compute import provider,scratch
from test_center import center


class ResizeReconciliationTests(unittest.TestCase):
    identity='a'*32
    def error(self,status=None,message='The read operation timed out'):
        e=RemoteError(message);e.status_code=status;e.transport_retryable=status is None;return e

    def test_polling_timeout_waits_for_original_request_then_replays_same_grant(self):
        c=Client();worker='b'*32;env=dict(REMOTE_COMPUTE_JOB_ID=self.identity,REMOTE_COMPUTE_WORKER_JOB_ID=worker,
            REMOTE_COMPUTE_URL=c.url,REMOTE_COMPUTE_LEASE='1')
        for original in (self.error(),self.error(409,'A resource request is already pending')):
            with self.subTest(original=str(original)),patch.dict(os.environ,env), \
                    patch.object(c,'request',side_effect=[original,dict(cpus=4,memory_mb=128)])as request, \
                    patch.object(c,'status',side_effect=[dict(status='running',resource_request={'request_id':1}),dict(status='running')])as status:
                self.assertEqual(c.resize(self.identity,4,active_cpus=2,memory_mb=128)['cpus'],4)
                self.assertEqual(request.call_count,2);self.assertEqual(status.call_count,2)
                self.assertEqual(request.call_args_list[0],request.call_args_list[1])
                self.assertEqual(request.call_args.args[0],'/jobs/'+worker+'/lease')

    def test_permanent_errors_and_terminal_observation_stop_resource_replay(self):
        for error in (self.error(403),self.error(409,'Insufficient RAM for active workers; existing grant retained')):
            c=Client()
            with self.subTest(error=str(error)),patch.object(c,'request',side_effect=error)as request,patch.object(c,'status')as status:
                with self.assertRaises(RemoteError):c.resize(self.identity,4)
                self.assertEqual(request.call_count,1);status.assert_not_called()
        c=Client()
        with patch.object(c,'request',side_effect=self.error())as request,patch.object(c,'status',return_value=dict(status='cancelled')):
            with self.assertRaisesRegex(RemoteError,'Only a running'):c.resize(self.identity,4)
            self.assertEqual(request.call_count,1)

    def test_reconnection_respects_original_task_deadline(self):
        c=Client();env=dict(REMOTE_COMPUTE_JOB_ID=self.identity,REMOTE_COMPUTE_URL=c.url,
            REMOTE_COMPUTE_DEADLINE=str(time.monotonic()-1))
        with patch.dict(os.environ,env),patch.object(c,'request',side_effect=self.error())as request:
            with self.assertRaisesRegex(TimeoutError,'execution deadline reached'):c.resize(self.identity,4)
            self.assertEqual(request.call_count,1)

    def test_real_lost_lease_reply_keeps_paid_map_items_and_explicit_zero_retries(self):
        source='''from pathlib import Path
import time
def one(value):
 marker=Path('once-'+str(value))
 if marker.exists():raise RuntimeError('Business input repeated')
 marker.write_text('yes');time.sleep(.1);return value
'''
        for backend in ('process','thread'):
            with self.subTest(backend=backend),scratch()as folder:
                root=Path(folder)
                with provider(root/'worker',cpus=4)as(worker,remote),Pool([remote])as pool,center(pool,root/'center')as(_,client):
                    original=Handler._reply;dropped=[]
                    def reply(handler,value,status=200,**options):
                        if handler.server.worker is worker and handler.command=='POST'and handler.path.endswith('/lease')and isinstance(value,dict)and 'cpus'in value and not dropped:
                            dropped.append(value['cpus']);handler.close_connection=True
                            handler.connection.shutdown(socket.SHUT_RDWR);handler.connection.close();return
                        return original(handler,value,status,**options)
                    with patch.object(Handler,'_reply',reply):
                        identity=client.submit(python_map('business:one',range(12),backend=backend,
                            max_cpus=4,memory_mb=256,retries=0,timeout=30),files={'business.py':source})['id']
                        result=client.wait(identity,wait_timeout=35)
                        self.assertEqual(result['status'],'succeeded',result);self.assertEqual(result['attempts'],[])
                        client.fetch(identity,root/'result')
                        saved=json.loads((root/'result/files/.dco-results.json').read_bytes())
                        self.assertEqual([r['value']for r in saved['items']],list(range(12)))
                        self.assertEqual(len(dropped),1)


if __name__=='__main__':unittest.main()
