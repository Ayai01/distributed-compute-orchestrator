"""LAN rejoin, external authentication and shared startup configuration."""
from contextlib import contextmanager
import json
import os
from pathlib import Path
import shutil
import subprocess
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from remote_compute import Client, Pool, RemoteError
from remote_compute.auth import in_networks, node_url
from remote_compute.cli import main
from remote_compute.commands import SchedulerShell
from remote_compute.worker import Server
from test_center import center
from test_compute import provider, scratch


@contextmanager
def external_peer(worker, **options):
    server = Server(('127.0.0.1', 0), worker, **options)
    original = server.get_request
    def accept():
        connection, address = original()
        return connection, ('203.0.113.42', address[1])
    server.get_request = accept
    thread = threading.Thread(target=server.serve_forever, kwargs={'poll_interval': .02}, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown();thread.join();server.server_close()


class AccessTests(unittest.TestCase):
    def test_host_shorthand_uses_default_port_and_secure_public_transport(self):
        for address,expected in [
            ('1.191.212.42','https://1.191.212.42:26133'),
            ('1.191.212.42:26133','https://1.191.212.42:26133'),
            ('192.168.1.11','http://192.168.1.11:26133'),
            ('https://192.168.1.11','https://192.168.1.11:26133'),
            ('[::1]:26134','http://[::1]:26134'),
            ('::1','http://[::1]:26133')]:
            self.assertEqual(node_url(address),expected)
        for address in ['', 'https://name/path', 'https://secret@host', 'host:0', 'ftp://host']:
            with self.subTest(address=address),self.assertRaises(ValueError):node_url(address)

    def test_bare_add_reuses_persisted_tls_credentials_and_keeps_budget(self):
        with scratch() as root:
            root=Path(root);url='https://203.0.113.42:26133'
            row=dict(name='remote',url=url,token='node-secret',ca_file='trusted-node.pem',
                     enabled=False,limit=52,timeout=30)
            (root/'nodes.local.json').write_text(json.dumps({'nodes':[row]}))
            control=Mock();control.nodes.return_value=dict(nodes=[row])
            shell=SimpleNamespace(center=control,pool=None,do_nodes=Mock(),do_save=Mock())
            with patch('remote_compute.commands.ROOT',root), \
                 patch('remote_compute.commands.Client') as constructor, \
                 patch.dict(os.environ,{},clear=True):
                constructor.return_value.health.return_value=dict(node='remote')
                SchedulerShell.do_add(shell,'203.0.113.42')
            constructor.assert_called_once_with(url,'node-secret',ca_file='trusted-node.pem',timeout=30,operator=True)
            self.assertEqual(control.control.call_args.kwargs['routing'],
                             dict(url=url,enabled=True,ca_file='trusted-node.pem',timeout=30))
            self.assertNotIn('limit',control.control.call_args.kwargs['routing'])

    def test_bare_lan_address_keeps_a_saved_https_profile(self):
        with scratch() as root:
            root=Path(root);row=dict(name='remote',url='https://192.168.1.12:26133',token='node-secret',ca_file='cert.pem')
            (root/'nodes.local.json').write_text(json.dumps({'nodes':[row]}))
            control=Mock();control.nodes.return_value=dict(nodes=[row])
            shell=SimpleNamespace(center=control,pool=None,do_nodes=Mock(),do_save=Mock())
            with patch('remote_compute.commands.ROOT',root),patch('remote_compute.commands.Client') as constructor, \
                 patch.dict(os.environ,{},clear=True):
                constructor.return_value.health.return_value=dict(node='remote')
                SchedulerShell.do_add(shell,'192.168.1.12')
            self.assertEqual(constructor.call_args.args[0],row['url'])

    def test_save_does_not_assign_center_credentials_to_an_unrelated_node(self):
        with scratch() as root:
            row=dict(name='remote',url='https://203.0.113.42:26133',weight=1,limit=52,memory_limit_mb=0,enabled=True)
            shell=SimpleNamespace(pool=SimpleNamespace(nodes=[]),center=SimpleNamespace(
                nodes=lambda:dict(policy='adaptive',nodes=[row]),token='center-only-secret'))
            with patch('remote_compute.commands.ROOT',Path(root)):
                SchedulerShell.do_save(shell,'')
            self.assertNotIn('token',json.loads((Path(root)/'nodes.local.json').read_text())['nodes'][0])

    def test_lan_worker_admission_needs_no_token(self):
        with scratch() as root, provider(root, token='remote-secret') as (_, remote):
            self.assertEqual(Client(remote.url).health()['role'], 'worker')

    def test_address_classification_does_not_trust_special_or_public_ranges(self):
        for address in ['192.168.1.11', '10.1.2.3', '172.16.1.2', '::1', 'fd01::1', '::ffff:192.168.1.11']:
            self.assertTrue(in_networks(address), address)
        for address in ['203.0.113.42', '100.64.1.2', '8.8.8.8', '::ffff:8.8.8.8']:
            self.assertFalse(in_networks(address), address)

    def test_external_http_rejected_even_with_token_and_forwarded_lan_header(self):
        with scratch() as root, provider(root, token='remote-secret') as (worker, _):
            with external_peer(worker) as server:
                client = Client('http://127.0.0.1:'+str(server.server_port), token='remote-secret')
                with self.assertRaisesRegex(RemoteError, '403.*HTTPS'):
                    client.request('/health', extra_headers={'X-Forwarded-For': '192.168.1.11'})

    def test_client_never_sends_credentials_to_public_http(self):
        with patch('remote_compute.client.http.client.HTTPConnection') as connect:
            with self.assertRaisesRegex(RemoteError, 'require HTTPS'):
                Client('http://203.0.113.42:26133', token='remote-secret').health()
            connect.assert_not_called()

    def test_external_https_requires_valid_token_and_verified_certificate(self):
        openssl = shutil.which('openssl')
        if not openssl and os.name == 'nt':
            openssl = 'D:/Program Files/Git/usr/bin/openssl.exe'
        if not openssl or not Path(openssl).is_file():
            self.skipTest('openssl generates an isolated test certificate')
        with scratch() as temporary, provider(Path(temporary)/'worker', token='remote-secret') as (worker, _):
            root = Path(temporary);cert = root/'node.crt';key = root/'node.key'
            subprocess.run([openssl, 'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '1',
                '-subj', '/CN=localhost', '-addext', 'subjectAltName=IP:127.0.0.1',
                '-keyout', str(key), '-out', str(cert)], check=True, capture_output=True)
            with external_peer(worker, certfile=str(cert), keyfile=str(key)) as server:
                url = 'https://127.0.0.1:'+str(server.server_port)
                for token in ['', 'wrong-token']:
                    with self.subTest(token=token), self.assertRaisesRegex(RemoteError, '401'):
                        Client(url, token=token, ca_file=str(cert)).health()
                with self.assertRaises(RemoteError):
                    Client(url, token='remote-secret').health()
                self.assertEqual(Client(url, token='remote-secret', ca_file=str(cert)).health()['role'], 'worker')
                name=worker.health()['node']
                row=dict(name=name,url=url,token='remote-secret',ca_file=str(cert),enabled=False,limit=1)
                (root/'nodes.local.json').write_text(json.dumps({'nodes':[row]}))
                with Pool([row]) as pool,center(pool,root/'center') as (_,control):
                    shell=SimpleNamespace(center=control,pool=pool,do_nodes=Mock(),do_save=Mock())
                    with patch('remote_compute.commands.ROOT',root),patch.dict(os.environ,{'REMOTE_COMPUTE_TOKEN':'remote-secret'}):
                        SchedulerShell.do_add(shell,'127.0.0.1:'+str(server.server_port))
                    self.assertTrue(pool.nodes[0].enabled)
                    self.assertEqual(pool.nodes[0].limit,1)
                    self.assertEqual(pool.nodes[0].client.ca_file,str(cert))
                worker.token = ''
                with self.assertRaisesRegex(RemoteError, '401'):
                    Client(url, ca_file=str(cert)).health()

    def test_readd_disabled_node_keeps_its_existing_budget(self):
        with scratch() as temporary, provider(Path(temporary)/'worker', cpus=2) as (_, remote):
            name = remote.health()['node']
            with Pool([dict(name=name, url=remote.url, limit=1, enabled=False)]) as pool, \
                    center(pool, Path(temporary)/'center') as (_, control):
                shell = SimpleNamespace(center=control, pool=pool, do_nodes=Mock(), do_save=Mock())
                with patch('remote_compute.commands.ROOT', Path(temporary)), \
                        patch.dict(os.environ, {'REMOTE_COMPUTE_TOKEN': ''}):
                    SchedulerShell.do_add(shell, remote.url)
                self.assertEqual(len(pool.nodes), 1)
                self.assertTrue(pool.nodes[0].enabled)
                self.assertEqual(pool.nodes[0].limit, 1)

    def test_configured_worker_endpoint_and_cli_override_share_one_startup(self):
        for explicit in [False, True]:
            with self.subTest(explicit=explicit), scratch() as temporary:
                root = Path(temporary)
                (root/'worker.local.json').write_text(json.dumps(dict(mode='worker', host='127.0.0.1',
                    port=0 if not explicit else 26134, cpus=1, root=str(root/'state'),
                    temp_root=str(root/'tmp'), trusted_networks=['127.0.0.0/8'])), encoding='utf-8')
                observed = []
                def console(health, url, alive):
                    observed.append(Client(url).health()['role'])
                args = ['--service-child'] + (['--port', '0'] if explicit else [])
                with patch('remote_compute.cli.ROOT', root), \
                        patch('remote_compute.cli.require_visible'), \
                        patch('remote_compute.cli.handoff_python', return_value=False), \
                        patch('remote_compute.cli.resolve', return_value=None) as probe, \
                        patch('remote_compute.cli.WorkerShell.loop', side_effect=console), \
                        patch('remote_compute.worker.cpu_benchmark', return_value={'name':'test', 'score':1000, 'threads':1}):
                    self.assertEqual(main(args), 0)
                self.assertEqual(observed, ['worker'])
                self.assertEqual(probe.call_args.args[:2], ('127.0.0.1', 0))
