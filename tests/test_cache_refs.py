"""Public producer references constrain cache consumers without exposing devices."""
import json
from pathlib import Path
import unittest
from unittest.mock import patch

from remote_compute import Client,Pool,RemoteError,command
from remote_compute.cache_refs import read,register,public
from test_center import center
from test_compute import provider,scratch


class CacheReferenceTests(unittest.TestCase):
    def test_reference_is_durable_immutable_and_redacted(self):
        with scratch()as root:
            row=register(root,'a'*32,project='P',provider='private-machine',content_id='b'*64)
            self.assertEqual(read(root,'a'*32),row)
            self.assertEqual(register(root,'a'*32,project='P',provider='private-machine',content_id='b'*64),row)
            self.assertNotIn('provider',public(row))
            with self.assertRaises(ValueError):register(root,'a'*32,project='P',provider='different',content_id='b'*64)
            with self.assertRaises(ValueError):read(root,'../a')
            with self.assertRaises(ValueError):read(root,'c'*32)

    def test_old_center_fails_before_registration_or_scientific_submission(self):
        c=Client()
        with patch.object(c,'health',return_value={'capabilities':[]}),patch.object(c,'request')as post:
            with self.assertRaisesRegex(RemoteError,'maintenance activation'):c.cache_ref('a'*32)
            post.assert_not_called()

    def test_lost_registration_reply_retries_only_the_same_reference(self):
        c=Client();lost=RemoteError('lost reference reply');lost.transport_retryable=True
        reference=dict(id='a'*32,producer_job='a'*32,project='P',content_id='b'*64)
        with patch.object(c,'health',return_value={'capabilities':['cache_references']}), \
                patch.object(c,'request',side_effect=[lost,reference])as post,patch('remote_compute.client._pause'):
            self.assertEqual(c.cache_ref('a'*32,content_id='b'*64),reference)
            self.assertEqual(post.call_count,2)
            self.assertEqual(post.call_args_list[0],post.call_args_list[1])

    def test_real_http_deleted_producer_keeps_cache_and_public_client_placement(self):
        with scratch()as folder,provider(Path(folder)/'one')as(a,ca),provider(Path(folder)/'two')as(b,cb),Pool([dict(name='producer',client=ca),dict(name='other',client=cb)])as pool:
            root=Path(folder)/'center'
            with center(pool,root)as(state,op):
                client=Client(op.url);task=command(['${tool:python}','-c','print(1)'],max_cpus=1,project='P',node='producer')
                result=op.run(task,keep=True)
                self.assertEqual(result['status'],'succeeded')
                identity=result['id'];reference=client.cache_ref(identity,content_id='d'*64)
                self.assertEqual(set(reference),{'id','producer_job','project','content_id'})
                op.delete(identity)
                self.assertEqual(client.cache_ref(identity),reference)
                consumer=command(['${tool:python}','-c','print(2)'],max_cpus=1,project='P',cache_ref=reference['id'])
                with patch.object(a,'submit',wraps=a.submit)as first,patch.object(b,'submit',wraps=b.submit)as second:
                    used=client.run(consumer)
                    self.assertEqual(used['status'],'succeeded',used.get('stderr'))
                    self.assertEqual(used['cache_ref'],reference['id'])
                    self.assertEqual(first.call_count,1)
                    self.assertEqual(second.call_count,0)
                published=[]
                client.execute({'consume':consumer},project='P',output_dir=Path(folder)/'pipeline',publish=published.append)
                self.assertEqual(len(published),1)
                self.assertEqual(published[0]['cache_ref'],reference['id'])
                with self.assertRaises(RemoteError):client.submit(dict(consumer,project='Q'))
                with self.assertRaises(RemoteError):client.submit(dict(consumer,node='other'))
                with self.assertRaises(RemoteError):client.submit(dict(consumer,platform='linux'))
                with self.assertRaises(RemoteError):client.request('/cache-references/'+identity,{'node':'other'})
                with self.assertRaises(RemoteError):client.cache_ref(identity,content_id='e'*64)
                pool.nodes[0].enabled=False
                with patch.object(b,'submit',wraps=b.submit)as second:
                    waiting=client.submit(consumer)['id']
                    try:outcome=client.wait(waiting,wait_timeout=.3)
                    except RemoteError:
                        outcome=client.status(waiting);client.cancel(waiting);client.wait(waiting)
                    self.assertNotEqual(outcome['status'],'succeeded')
                    self.assertNotEqual(outcome['status'],'running')
                    self.assertNotIn('producer',outcome.get('error',''))
                    self.assertEqual(second.call_count,0)
                    client.fetch(waiting)
                pool.nodes[0].enabled=True
            # A new center reads the registered reference after the job is gone.
            with center(pool,root)as(_,op):
                client=Client(op.url)
                self.assertEqual(client.cache_ref(identity),reference)
                self.assertEqual(client.run(consumer)['status'],'succeeded')

    def test_running_and_failed_producers_and_direct_worker_are_rejected(self):
        with scratch()as folder,provider(Path(folder)/'worker')as(worker,remote),Pool([remote])as pool,center(pool,Path(folder)/'center',paused=True)as(state,op):
            client=Client(op.url)
            queued=client.submit(command(['${tool:python}','-c','print(1)'],project='P'))['id']
            with self.assertRaises(RemoteError):client.cache_ref(queued)
            client.cancel(queued);client.wait(queued);client.fetch(queued)
            state.control({'paused':False})
            failed=client.run(command(['${tool:python}','-c','raise ValueError("synthetic")'],project='P'),keep=True)
            self.assertEqual(failed['status'],'failed')
            with self.assertRaises(RemoteError):client.cache_ref(failed['id'])
            client.delete(failed['id'])
            with self.assertRaises(RemoteError):remote.submit(command(['${tool:python}','-c','print(1)'],project='P',cache_ref='a'*32))
