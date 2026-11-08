"""DELETE confirmation is idempotent across lost and proxied replies."""
from pathlib import Path
import socket
import unittest
from unittest.mock import patch
from remote_compute import Client, Pool, RemoteError
from remote_compute.worker import Handler
from test_compute import provider, python, scratch
from test_center import center


class DeleteConfirmationTests(unittest.TestCase):
    def error(self, status=None):
        error=RemoteError('delete reply lost');error.status_code=status
        error.transport_retryable=status is None
        return error

    def test_lost_reply_reconciles_absent_original_without_redeleting(self):
        client=Client();gone=self.error(404)
        with patch.object(client,'request',side_effect=self.error())as request,patch.object(client,'status',side_effect=gone):
            self.assertEqual(client.delete('a'*32),dict(deleted='a'*32))
            self.assertEqual(request.call_count,1)

    def test_existing_terminal_original_retries_delete_once(self):
        for code in (None,502,503,504):
            with self.subTest(code=code):
                client=Client()
                with patch.object(client,'request',side_effect=[self.error(code),dict(deleted='a'*32)])as request, \
                        patch.object(client,'status',return_value=dict(status='succeeded')):
                    self.assertEqual(client.delete('a'*32),dict(deleted='a'*32))
                    self.assertEqual(request.call_count,2)

    def test_persistent_failure_is_bounded_and_running_or_denied_is_not_deleted(self):
        client=Client();error=self.error()
        with patch.object(client,'request',side_effect=error)as request,patch.object(client,'status',return_value=dict(status='succeeded')):
            with self.assertRaises(RemoteError):client.delete('a'*32)
            self.assertEqual(request.call_count,2)
        with patch.object(client,'request',side_effect=error)as request,patch.object(client,'status',return_value=dict(status='running')):
            with self.assertRaises(RemoteError):client.delete('a'*32)
            self.assertEqual(request.call_count,1)
        for code in (403,409):
            with self.subTest(code=code),patch.object(client,'request',side_effect=self.error(code)),patch.object(client,'status')as status:
                with self.assertRaises(RemoteError):client.delete('a'*32)
                status.assert_not_called()

    def test_real_delete_completed_before_socket_reply_was_dropped(self):
        with scratch()as temporary,provider(temporary)as(worker,client):
            identity=client.submit(python('print("kept")'))['id'];client.wait(identity)
            original=Handler._reply;dropped=[]
            def reply(handler,value,status=200,**options):
                if handler.command=='DELETE'and handler.server.worker is worker and not dropped:
                    dropped.append(identity);handler.close_connection=True
                    handler.connection.shutdown(socket.SHUT_RDWR);handler.connection.close();return
                return original(handler,value,status,**options)
            with patch.object(Handler,'_reply',reply):
                self.assertEqual(client.delete(identity),dict(deleted=identity))
            self.assertEqual(dropped,[identity])
            self.assertEqual(client.statuses([identity])[identity]['status'],'unknown')

    def test_pipeline_publishes_once_and_finishes_when_delete_reply_is_lost(self):
        with scratch()as temporary:
            root=Path(temporary)
            with provider(root/'worker')as(_,remote),Pool([remote])as pool,center(pool,root/'center')as(state,client):
                published=[];original=Handler._reply;dropped=[]
                def reply(handler,value,status=200,**options):
                    if handler.command=='DELETE'and handler.server.worker is state and not dropped:
                        dropped.append(value['deleted']);handler.close_connection=True
                        handler.connection.shutdown(socket.SHUT_RDWR);handler.connection.close();return
                    return original(handler,value,status,**options)
                with patch.object(Handler,'_reply',reply):
                    results=client.execute({'one':dict(task=python('print("one search")'))},
                        project='delete-confirmation',output_dir=root/'execution',publish=lambda row:published.append(row['id']))
                self.assertEqual(len(results),1);self.assertEqual(published,dropped)
                self.assertEqual(client.statuses(published)[published[0]]['status'],'unknown')


if __name__=='__main__':unittest.main()
