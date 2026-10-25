"""Offline expiry and explicit LAN discovery, without touching production nodes."""
from contextlib import redirect_stdout
import io
import json
import os
from pathlib import Path
import time
import threading
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from remote_compute import Client, Pool, RemoteError
from remote_compute.client import Node
from remote_compute.commands import SchedulerShell, save_detached_node
from remote_compute.discovery import scan
from remote_compute.cli import load_nodes, main
from remote_compute.view import format_status, format_notices
from test_compute import provider, scratch


class OfflineTests(unittest.TestCase):
    def test_one_minute_expiry_keeps_original_references_and_skips_ordinary_probes(self):
        with scratch() as root,provider(Path(root)/'worker',cpus=2) as (_,client):
            removed=[]
            with Pool([dict(name='device',client=client,limit=1)],refresh=1000,on_detach=removed.append) as pool:
                pool.stop_event.set();pool.monitor.join()
                node=pool.nodes[0]
                node.last_connected_at=100
                node.error='connection refused'
                node.live_jobs.add('original-id');node.reserved=1
                self.assertEqual(pool._expire_unreachable(159.9),[])
                self.assertTrue(node.enabled)
                self.assertEqual(pool._expire_unreachable(160),[node])
                self.assertTrue(node.detached);self.assertFalse(node.enabled)
                self.assertEqual(node.live_jobs,{'original-id'})
                self.assertEqual(node.reserved,1)
                self.assertEqual(removed,[node])
                with patch.object(client,'health',side_effect=AssertionError('background probe after expiry')):
                    pool.refresh(monitor=True)
                self.assertEqual(pool._expire_unreachable(300),[])
                pool.refresh(nodes=[node])  # Read-only maintenance must not rejoin it.
                self.assertFalse(node.enabled)
                self.assertTrue(node.detached)
                pool.configure('device',enabled=True)
                pool.refresh(nodes=[node])
                self.assertFalse(node.detached);self.assertTrue(node.enabled)
                self.assertFalse(node.error)
                self.assertEqual(node.limit,1)
                node.live_jobs.clear();node.reserved=0

    def test_successful_health_resets_the_one_minute_interval(self):
        with scratch() as root,provider(Path(root)/'worker') as (_,client),Pool([client],refresh=1000) as pool:
            pool.stop_event.set();pool.monitor.join()
            node=pool.nodes[0];node.last_connected_at=time.monotonic()-70
            pool.refresh(nodes=[node])
            self.assertEqual(pool._expire_unreachable(node.last_connected_at+59),[])
            self.assertTrue(node.enabled)

    def test_expired_node_is_hidden_even_with_stale_usage_and_old_error(self):
        node=dict(name='Aya1-ITX',enabled=False,detached=True,reserved=8,external_cpus=8,
                  error='without response',active_jobs=[dict(id='original',cpus=8)])
        value=dict(role='scheduler',node='Aya1-PC',nodes=[node])
        self.assertNotIn('Aya1-ITX','\n'.join(format_status(value,width=110)))
        self.assertFalse(format_notices(value))

    def test_disabled_configuration_keeps_budget_credentials_and_skips_startup_probe(self):
        with scratch() as root:
            path=Path(root)/'nodes.local.json'
            original=dict(name='device',url='http://192.168.1.11:26133',limit=8,token='saved-secret',enabled=True)
            path.write_text(json.dumps({'policy':'adaptive','nodes':[original,dict(name='other',url='http://192.168.1.12:26133',enabled=False)]}))
            node=Node('device',Client(original['url']),enabled=False,detached=True)
            save_detached_node(path,node)
            value=json.loads(path.read_text());self.assertEqual(value['nodes'][0],dict(original,enabled=False,
                reconnect=dict(attempts=0,after=0)))
            with patch('remote_compute.cli.Client',side_effect=AssertionError('probe disabled configuration')):
                self.assertFalse(load_nodes(path)['nodes'][0]['enabled'])

    def test_detached_retry_every_minute_stops_after_five_attempts(self):
        with scratch() as root,provider(Path(root)/'worker') as (_,client),Pool([client],refresh=1000) as pool:
            pool.stop_event.set();pool.monitor.join()
            node=pool.nodes[0];node.last_connected_at=100
            with patch('remote_compute.client.time.time',return_value=1000):pool._expire_unreachable(160)
            self.assertEqual(node.reconnect_after,1060)
            with patch.object(client,'health',side_effect=RemoteError('packet loss')) as health:
                for attempt in range(1,6):
                    now=1000+60*attempt
                    self.assertEqual(pool._retry_detached(now-.1),[])
                    self.assertEqual(pool._retry_detached(now),[node])
                    pool.probe_futures[node.name].result(timeout=2)
                    self.assertEqual(node.reconnect_attempts,attempt)
                    self.assertFalse(node.enabled)
                    self.assertEqual(pool._retry_detached(now),[])
                self.assertEqual(pool._retry_detached(10000),[])
                self.assertEqual(health.call_count,5)
                pool.refresh(monitor=True)
                self.assertEqual(health.call_count,5)
            self.assertEqual(node.reconnect_after,0)

    def test_retry_success_restores_budget_and_original_job_references(self):
        with scratch() as root,provider(Path(root)/'worker',cpus=2) as (worker,client):
            changed=[]
            with Pool([dict(name='device',client=client,limit=1)],refresh=1000,on_reconnect=lambda n:changed.append(n.enabled)) as pool:
                pool.stop_event.set();pool.monitor.join()
                node=pool.nodes[0];node.live_jobs.add('original');node.reserved=1;node.last_connected_at=100
                pool._expire_unreachable(160)
                self.assertEqual(pool._retry_detached(node.reconnect_after),[node])
                pool.probe_futures[node.name].result(timeout=2)
                self.assertTrue(node.enabled);self.assertFalse(node.detached)
                self.assertEqual(node.capacity,1);self.assertEqual(node.live_jobs,{'original'})
                self.assertEqual(node.reserved,1);self.assertEqual(changed,[False,True])
                self.assertEqual((node.reconnect_attempts,node.reconnect_after),(0,0))
                self.assertFalse(worker.jobs)
                node.live_jobs.clear();node.reserved=0

    def test_manual_disable_wins_over_a_late_retry_reply(self):
        with scratch() as root,provider(Path(root)/'worker') as (_,client),Pool([client],refresh=1000) as pool:
            pool.stop_event.set();pool.monitor.join()
            node=pool.nodes[0];value=client.health();node.last_connected_at=100
            pool._expire_unreachable(160)
            entered,released=threading.Event(),threading.Event()
            def delayed():
                entered.set();self.assertTrue(released.wait(2));return value
            with patch.object(client,'health',side_effect=delayed):
                pool._retry_detached(node.reconnect_after)
                self.assertTrue(entered.wait(2))
                pool.configure(node.name,enabled=False)
                released.set();pool.probe_futures[node.name].result(timeout=2)
            self.assertFalse(node.enabled);self.assertFalse(node.detached)
            self.assertEqual(pool._retry_detached(time.time()+1000),[])

    def test_renamed_same_machine_reconnects_without_changing_original_references(self):
        with scratch() as root, provider(Path(root)/'worker', cpus=2) as (_, client):
            path=Path(root)/'nodes.local.json'
            path.write_text(json.dumps({'nodes':[dict(name='original-route',url=client.url,token='retained',limit=1)]}))
            with Pool([dict(name='original-route',client=client,limit=1)],refresh=1000,
                      on_reconnect=lambda n:save_detached_node(path,n)) as pool:
                pool.stop_event.set();pool.monitor.join()
                node=pool.nodes[0];value=dict(node.health)
                value.update(node='new-computer-name',machine_id='new-computer-name:'+value['machine_id'].rsplit(':',1)[-1])
                node.live_jobs.add('original-id');node.reserved=1;node.last_connected_at=100
                pool._expire_unreachable(160)
                with patch.object(client,'health',return_value=value):
                    pool._retry_detached(node.reconnect_after)
                    pool.probe_futures[node.name].result(timeout=2)
                self.assertTrue(node.enabled);self.assertFalse(node.error)
                self.assertEqual(node.live_jobs,{'original-id'});self.assertEqual(node.reserved,1)
                self.assertEqual(node.name,'original-route');self.assertIn(value['node'],node.aliases)
                saved=json.loads(path.read_text())['nodes'][0]
                self.assertEqual(saved['token'],'retained');self.assertEqual(saved['limit'],1)
                self.assertIn(value['node'],saved['aliases'])
                node.live_jobs.clear();node.reserved=0

    def test_different_machine_cannot_rejoin_under_the_previous_computer_name(self):
        with scratch() as root, provider(Path(root)/'worker') as (_,client), Pool([client],refresh=1000) as pool:
            pool.stop_event.set();pool.monitor.join()
            node=pool.nodes[0];value=dict(node.health,machine_id='another-machine:ffffffffffff')
            node.last_connected_at=100;pool._expire_unreachable(160)
            with patch.object(client,'health',return_value=value):
                pool._retry_detached(node.reconnect_after)
                pool.probe_futures[node.name].result(timeout=2)
            self.assertFalse(node.enabled)
            self.assertIn('identity differs',node.error)

    def test_startup_retains_the_previous_name_as_an_alias_for_the_same_endpoint(self):
        with scratch() as root, provider(Path(root)/'worker') as (_,client):
            path=Path(root)/'nodes.local.json'
            path.write_text(json.dumps({'nodes':[dict(name='old-name',url=client.url,aliases=['older-name'])]}))
            loaded=load_nodes(path)['nodes'][0]
            self.assertEqual(loaded['name'],client.health()['node'])
            self.assertEqual(loaded['aliases'],['older-name','old-name'])

    def test_saved_retry_budget_survives_restart_and_manual_off_stays_off(self):
        with scratch() as root,provider(Path(root)/'worker') as (_,client):
            future=time.time()+3600
            rows=[dict(name='lost',client=client,enabled=False,reconnect=dict(attempts=4,after=future)),
                  dict(name='off',url='http://127.0.0.1:1',enabled=False)]
            with Pool(rows,refresh=1000) as pool:
                pool.stop_event.set();pool.monitor.join()
                lost,off=pool.nodes
                self.assertTrue(lost.detached);self.assertFalse(off.detached)
                with patch.object(client,'health',side_effect=RemoteError('offline')) as health:
                    pool._retry_detached(future)
                    pool.probe_futures[lost.name].result(timeout=2)
                    self.assertEqual(health.call_count,1)
                    self.assertEqual(pool._retry_detached(future+600),[])
                self.assertEqual(lost.reconnect_attempts,5)
                path=Path(root)/'nodes.local.json'
                path.write_text(json.dumps({'nodes':[dict(name='lost',url=client.url,enabled=False)]}))
                save_detached_node(path,lost)
                row=json.loads(path.read_text())['nodes'][0]
                self.assertEqual(row['reconnect'],dict(attempts=5,after=0))
                lost.enabled=True;lost.detached=False
                save_detached_node(path,lost)
                row=json.loads(path.read_text())['nodes'][0]
                self.assertTrue(row['enabled']);self.assertNotIn('reconnect',row)

    def test_manual_api_disable_is_saved_before_restart(self):
        from test_center import center
        with scratch() as root,provider(Path(root)/'worker') as (_,remote):
            path=Path(root)/'nodes.local.json'
            row=dict(name='device',url=remote.url,enabled=True,limit=1,token='preserved')
            path.write_text(json.dumps(dict(nodes=[row])))
            with Pool([dict(row,client=remote)],refresh=1000,
                      on_reconnect=lambda n:save_detached_node(path,n)) as pool, \
                 center(pool,Path(root)/'center') as (_,control):
                control.control(node='device',routing=dict(enabled=False))
                saved=json.loads(path.read_text())['nodes'][0]
                self.assertFalse(saved['enabled']);self.assertNotIn('reconnect',saved)
                self.assertEqual(saved['token'],'preserved')
            with patch('remote_compute.cli.Client',side_effect=AssertionError('manual off startup probe')):
                self.assertFalse(load_nodes(path)['nodes'][0]['enabled'])

    def test_cli_save_and_automatic_retry_merge_under_one_configuration_lock(self):
        with scratch() as root:
            path=Path(root)/'nodes.local.json'
            row=dict(name='device',url='http://127.0.0.1:26133',enabled=True,
                     weight=1,limit=52,memory_limit_mb=0,token='preserved')
            path.write_text(json.dumps(dict(nodes=[row])))
            inside,release,automatic_started=threading.Event(),threading.Event(),threading.Event()
            errors=[]
            def snapshot():
                inside.set();self.assertTrue(release.wait(3))
                return dict(policy='adaptive',nodes=[{k:v for k,v in row.items() if k!='token'}])
            shell=SimpleNamespace(center=None,pool=SimpleNamespace(nodes=[],snapshot=snapshot))
            lost=Node('device',Client(row['url']),enabled=False,detached=True,
                      reconnect_attempts=4,reconnect_after=time.time()+60)
            def run(call):
                try:call()
                except Exception as e:errors.append(e)
            def automatic():
                automatic_started.set();save_detached_node(path,lost)
            with patch('remote_compute.commands.ROOT',Path(root)),redirect_stdout(io.StringIO()):
                first=threading.Thread(target=run,args=(lambda:SchedulerShell.do_save(shell,''),));first.start()
                self.assertTrue(inside.wait(3))
                second=threading.Thread(target=run,args=(automatic,));second.start()
                self.assertTrue(automatic_started.wait(3));release.set()
                first.join(3);second.join(3)
                self.assertFalse(first.is_alive());self.assertFalse(second.is_alive())
            self.assertFalse(errors,errors)
            saved=json.loads(path.read_text())['nodes'][0]
            self.assertFalse(saved['enabled']);self.assertEqual(saved['reconnect']['attempts'],4)
            self.assertEqual(saved['token'],'preserved');self.assertEqual(saved['limit'],52)

    def test_unsaved_retry_never_sends_a_network_attempt(self):
        with scratch() as root,provider(Path(root)/'worker') as (_,remote),Pool([remote],refresh=1000) as pool:
            pool.stop_event.set();pool.monitor.join()
            node=pool.nodes[0];node.last_connected_at=100;pool._expire_unreachable(160)
            def cannot_save(_):raise OSError('disk full')
            pool.on_reconnect=cannot_save
            with patch.object(remote,'health',side_effect=AssertionError('unrecorded network attempt')):
                self.assertEqual(pool._retry_detached(node.reconnect_after),[])
            self.assertIn('disk full',node.error)

    def test_cancelled_detached_job_finishes_drain_and_retains_unknown_cost_until_reconnect(self):
        from test_center import center
        from test_compute import python,until
        with scratch() as root,provider(Path(root)/'worker') as (worker,remote):
            with Pool([remote],refresh=1000) as pool,center(pool,Path(root)/'center') as (state,client):
                pool.stop_event.set();pool.monitor.join()
                identity=client.submit(python('import time;time.sleep(30)'))['id']
                until(lambda:client.status(identity,brief=True)['status']=='running')
                node=pool.nodes[0]
                with patch.object(node.client,'status',side_effect=RemoteError('offline')), \
                     patch.object(node.client,'cancel',side_effect=RemoteError('offline')):
                    node.last_connected_at=time.monotonic()-61
                    pool._expire_unreachable()
                    client.cancel(identity)
                    result=client.wait(identity,wait_timeout=3)
                    self.assertEqual(result['status'],'interrupted')
                    self.assertTrue(result['cost_incomplete'])
                    self.assertTrue(result['remote_cancel_pending'])
                    until(lambda:node.reserved==0)
                    # An evicted/reloaded terminal record has only its stored
                    # worker URL, not a live Node object.
                    state.jobs[identity].pop('node',None)
                    with patch.object(node.client,'artifacts',side_effect=AssertionError('offline artifacts RPC')):
                        receipt=client.fetch(identity,delete=False)
                    self.assertEqual(receipt['id'],identity)
                    self.assertTrue((Path(root)/'center/jobs'/identity/'request.zip').exists())
                self.assertEqual(len(worker.jobs),1)
                pool.configure(node.name,enabled=True);pool.refresh(nodes=[node])
                # Cleanup retries only the original cancellation, never submit.
                for _ in range(20):
                    try:client.delete(identity);break
                    except RemoteError:time.sleep(.02)
                else:self.fail('original cancellation did not finish after reconnect')
                self.assertFalse(worker.jobs)

    def test_restored_offline_original_keeps_health_and_cancels_before_reattachment(self):
        from test_center import center
        from test_compute import python
        from remote_compute.bundle import task_bundle
        with scratch() as root:
            state_root=Path(root)/'center';identity='1234'+'a'*28
            directory=state_root/'jobs'/identity;directory.mkdir(parents=True)
            task=python('print(1)',project='AOP',label='original',memory_mb=128)
            request=task_bundle(task);(directory/'request.zip').write_bytes(request)
            original=dict(id=identity,status='running',submitted=time.time(),started=time.time(),
                          assigned_node='offline',worker_url='http://127.0.0.1:1',cpus=1,memory_mb=128)
            (directory/'result.json').write_text(json.dumps(original))
            remote=Client(original['worker_url'],timeout=.1)
            with patch.object(remote,'request',side_effect=AssertionError('offline RPC during recovery cancellation')), \
                 Pool([dict(name='offline',client=remote,enabled=False)],refresh=1000) as pool, \
                 center(pool,state_root) as (state,client):
                self.assertEqual(client.health()['queued'],0)
                result=client.cancel(identity)
                self.assertEqual(result['status'],'interrupted')
                self.assertTrue(result['cost_incomplete'])
                self.assertTrue(result['remote_cancel_pending'])
                receipt=client.fetch(identity,delete=False)
                self.assertEqual(receipt['id'],identity)
                self.assertEqual((directory/'request.zip').read_bytes(),request)
                self.assertEqual(pool.nodes[0].reserved,0)


class DiscoveryTests(unittest.TestCase):
    def test_real_lan_handshake_reads_worker_identity_without_job_submission(self):
        with scratch() as root,provider(Path(root)/'worker') as (worker,client):
            with patch.object(worker,'submit',side_effect=AssertionError('discovery must not submit')):
                result=scan(['127.0.0.1/32'],port=client.endpoint.port,timeout=.3)
            self.assertEqual(result['nodes'],[{'name':worker.health(brief=True)['node'],'url':client.url}])
            self.assertFalse(worker.jobs)

    def test_scan_rejects_public_or_huge_networks_before_any_connection(self):
        with patch('remote_compute.discovery.socket.create_connection',side_effect=AssertionError('unsafe scan')):
            for network in ('8.8.8.0/24','192.168.0.0/16','::1/128'):
                with self.subTest(network=network),self.assertRaises(ValueError):scan([network])

    def test_unavailable_hosts_are_not_rejoined(self):
        with patch('remote_compute.discovery.socket.create_connection',side_effect=OSError('closed')):
            self.assertEqual(scan(['192.168.1.11/32'])['nodes'],[])

    def test_readonly_cli_scan_does_not_start_or_control_a_service(self):
        with patch('remote_compute.cli.Worker',side_effect=AssertionError('scan starts worker')), \
             patch('remote_compute.commands.scan_devices',return_value=[]) as discover,redirect_stdout(io.StringIO()):
            self.assertEqual(main(['scan','192.168.1.0/24']),0)
        discover.assert_called_once()

    def test_scan_add_reuses_existing_readd_contract(self):
        from types import SimpleNamespace
        from unittest.mock import Mock
        row={'name':'device','url':'http://192.168.1.11:26133'}
        shell=SimpleNamespace(do_add=Mock())
        with patch('remote_compute.commands.scan_devices',return_value=[row]):
            SchedulerShell.scan(shell,SimpleNamespace(scan_add=False))
            shell.do_add.assert_not_called()
            SchedulerShell.scan(shell,SimpleNamespace(scan_add=True))
            shell.do_add.assert_called_once_with(row['url'])

    def test_cli_scan_add_uses_existing_center_and_preserves_node_budget(self):
        from test_center import center
        with scratch() as root,provider(Path(root)/'worker',cpus=2) as (worker,remote):
            name=worker.health(brief=True)['node']
            with Pool([dict(name=name,client=remote,enabled=False,limit=1)]) as pool, \
                 center(pool,Path(root)/'center') as (coordinator,client):
                with patch('remote_compute.cli.ROOT',Path(root)),patch('remote_compute.commands.ROOT',Path(root)), \
                     patch('remote_compute.cli.Worker',side_effect=AssertionError('new worker during scan')), \
                     patch('remote_compute.commands.scan_devices',return_value=[{'name':name,'url':remote.url}]), \
                     patch.dict(os.environ,REMOTE_COMPUTE_TOKEN='',REMOTE_COMPUTE_OPERATOR_TOKEN=coordinator.operator_token), \
                     redirect_stdout(io.StringIO()):
                    self.assertEqual(main(['--url',client.url,'scan','--add']),0)
                self.assertTrue(pool.nodes[0].enabled)
                self.assertEqual(pool.nodes[0].limit,1)
                self.assertEqual(len(pool.nodes),1)
