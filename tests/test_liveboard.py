import json
import gzip
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch, Mock

from remote_compute import liveboard as live
from remote_compute.liveboard_wire import encode as cbor


class LiveboardTests(unittest.TestCase):
    def test_small_active_project_is_visible_when_large_project_fills_snapshot(self):
        large = [dict(id=str(i), project='large', status='running', cpus=4) for i in range(80)]
        small = [dict(id='native-'+str(i), project='small', status='running', cpus=1) for i in range(2)]
        sample = live.snapshot(self.health(), [self.node(active_jobs=large+small)])
        self.assertEqual(len(sample['jobs']), 32)
        self.assertEqual(sum(j['project']=='small' for j in sample['jobs']),2)
        self.assertEqual(sample['coverage']['jobs_active_total'],82)

    def test_native_snapshot_exposes_measured_cores_and_phase_when_available(self):
        task = dict(id='a'*32, status='running', project='maintenance', cpus=4, max_cpus=4,
                    observe_only=True, phase='prepare', measured_cpus=1.231, cpu_seconds=5.121)
        row = live.snapshot(self.health(), [self.node(active_jobs=[task])])['jobs'][0]
        self.assertEqual((row['cpus'], row['measured_cpus'], row['cpu_seconds'], row['phase']),
                         (4, 1.23, 5.12, 'prepare'))
        del task['measured_cpus']
        self.assertNotIn('measured_cpus',live.snapshot(self.health(),[self.node(active_jobs=[task])])['jobs'][0])

    def test_cbor_matches_rfc8949_typed_value_vectors(self):
        vectors=[(0,'00'),(23,'17'),(24,'1818'),(1000,'1903e8'),(-1,'20'),(-1000,'3903e7'),
                 (2**32,'1b0000000100000000'),(True,'f5'),(False,'f4'),(None,'f6'),
                 (1.1,'fb3ff199999999999a'),('水','63e6b0b4'),([1,2],'820102'),
                 ({'a':1},'a1616101')]
        for value,expected in vectors:
            with self.subTest(value=value):self.assertEqual(cbor(value).hex(),expected)
        for invalid in (float('nan'),float('inf'),2**64,-2**64-1,{1:'not text'},b'bytes'):
            with self.subTest(value=invalid),self.assertRaises(ValueError):cbor(invalid)

    def test_snapshot_separates_maximum_cpu_and_memory_meanings(self):
        n=self.node(base_capacity=96,capacity=96,logical_cpus=104,cpu_overcommit_factor=1.25,
            rss_mb=200,memory_reserved_mb=280,memory_reserve_mb=4096,memory_headroom_mb=2048,
            memory_pressure_state=dict(physical_threshold_mb=640),active_jobs=[dict(id='a',status='running',
                cpus=4,max_cpus=64,memory_mb=280,memory_phase=dict(base_memory_mb=512,memory_per_cpu_mb=1024))])
        value=live.snapshot(self.health(),[n]);row=value['nodes'][0]
        self.assertEqual((row['cpu_base'],row['cpu_capacity'],row['cpu_max_capacity'],row['cpu_total']),(96,96,120,104))
        self.assertEqual((row['memory_actual_mb'],row['memory_reported_mb'],row['memory_reserved_mb']),(200,4608,280))
        self.assertEqual((row['memory_reserve_mb'],row['memory_headroom_mb'],row['host_memory_floor_mb']),(4096,2048,640))
        self.assertEqual(row['host_memory_used_mb'],20000)
        self.assertEqual(value['jobs'][0]['memory_reported_mb'],4608)
        summary=live.snapshot(self.health(),[n,dict(n,name='same-host')])['summary']
        for key in ('memory_actual_mb','memory_reported_mb','memory_reserved_mb','memory_reserve_mb','host_memory_floor_mb'):
            self.assertEqual(summary[key],row[key])

    def node(self, **changes):
        value = dict(name='node', enabled=True, capacity=8, base_capacity=8, reserved=4,
                     external_cpus=0, logical_cpus=8, cpu_percent=50, machine_id='hardware',
                     memory_total_mb=32000, host_memory_available_mb=12000,
                     tags=['ng47','ml','python','secret-device-tag'], url='local://node',
                     root='C:/private', token='NEVER_SEND', error='', active_jobs=[])
        value.update(changes)
        return value

    def health(self):
        return dict(version='0.11.19', used_cpus=4, queued=7, cpu=dict(capacity=8),
                    root='C:/private', token='NEVER_SEND', stdout='SCIENCE_BODY')

    def test_projection_contains_only_public_display_fields_and_deduplicates_physical_ram(self):
        nodes=[self.node(),self.node(name='sibling')]
        value=live.snapshot(self.health(),nodes)
        raw=live.encode(value).decode()
        for forbidden in ('C:/private','NEVER_SEND','SCIENCE_BODY','local://','hardware','secret-device-tag','"url"'):
            self.assertNotIn(forbidden,raw)
        self.assertEqual(value['summary']['memory_total_mb'],32000)
        self.assertEqual(value['summary']['memory_used_mb'],20000)
        self.assertEqual(value['summary']['queued'],7)
        self.assertEqual(live.snapshot(dict(self.health(),cpus=170),nodes)['summary']['cpu_capacity'],170)
        self.assertIsNone(value['summary']['completed'])
        self.assertIsNone(value['source']['uptime_seconds'])
        self.assertEqual(value['nodes'][0]['capabilities'],['ng47'])

    def test_total_threads_are_physical_deduplicated_and_independent_of_grants(self):
        nodes=[self.node(capacity=10,base_capacity=6,logical_cpus=8),
               self.node(name='sibling',capacity=4,base_capacity=2,logical_cpus=8),
               self.node(name='other',machine_id='other',capacity=3,base_capacity=2,logical_cpus=4)]
        value=live.snapshot(self.health(),nodes)
        self.assertEqual(value['summary']['cpu_total'],12)
        self.assertEqual([n['cpu_total']for n in value['nodes']],[8,8,4])
        self.assertEqual(value['nodes'][0]['cpu_base'],6)
        self.assertEqual(value['nodes'][0]['cpu_capacity'],10)
        nodes[0]['capacity']=6
        self.assertEqual(live.snapshot(self.health(),nodes)['summary']['cpu_total'],12)
        nodes[2]['logical_cpus']=None
        self.assertIsNone(live.snapshot(self.health(),nodes)['summary']['cpu_total'])

    def test_task_progress_is_explicit_and_job_width_elapsed_are_real(self):
        jobs=[dict(id='a'*32,label='Task',status='running',cpus=2,max_cpus=64,
                   project='project',started=90,completed=2,total=8,argv=['SECRET']),
              dict(id='b'*32,status='running',cpus=1,max_cpus=54,started=95)]
        value=live.snapshot(self.health(),[self.node(active_jobs=jobs)],now=100)
        self.assertEqual(value['jobs'][0]['progress'],dict(completed=2,total=8,ratio=.25))
        self.assertEqual(value['jobs'][0]['elapsed_seconds'],10)
        self.assertEqual(value['jobs'][0]['max_cpus'],64)
        self.assertIsNone(value['jobs'][1]['progress'])
        self.assertNotIn('SECRET',live.encode(value).decode())

    def test_bounds_hold_for_large_cached_active_sets_without_loading_history(self):
        jobs=[dict(id=str(i),label='任务'*200,project='工程'+str(i),status='running',cpus=1)
              for i in range(200)]
        nodes=[self.node(name='设备'+str(i),active_jobs=jobs)for i in range(20)]
        value=live.snapshot(self.health(),nodes)
        self.assertLessEqual(len(value['jobs']),32)
        self.assertLessEqual(len(value['projects']),16)
        self.assertEqual(len(value['nodes']),len(nodes))
        self.assertLessEqual(len(value['alerts']),8)
        self.assertLessEqual(len(live.encode(dict(data=value))),65536)
        self.assertEqual(value['coverage']['jobs_active_total'],4000)
        self.assertFalse(value['coverage']['history'])

    def test_disabled_historical_nodes_are_hidden_until_they_have_work(self):
        nodes = [self.node(name='online'),
                 self.node(name='retired', enabled=False, reserved=0, error='old connection error'),
                 self.node(name='draining', enabled=False, reserved=1)]
        value = live.snapshot(self.health(), nodes)
        self.assertEqual([row['id'] for row in value['nodes']], ['online', 'draining'])
        self.assertEqual(value['coverage']['nodes_total'], 2)
        self.assertFalse(any(alert.get('node') == 'retired' for alert in value['alerts']))

    def test_terminal_receipts_do_not_contribute_to_active_jobs_projects_or_alerts(self):
        failed = {'id':'finished', 'project':'old', 'status':'failed'}
        health = dict(self.health(), queued=0, recent_failures=[failed])
        offline = self.node(name='offline', error='current transport fault', active_jobs=[failed])
        empty = live.snapshot(health, [offline])
        self.assertEqual(empty['coverage']['jobs_active_total'], 0)
        self.assertEqual(empty['coverage']['projects_active_total'], 0)
        self.assertEqual((empty['summary']['running'], empty['summary']['queued']), (0, 0))
        self.assertEqual(empty['jobs'], [])
        self.assertEqual(empty['projects'], [])
        self.assertEqual([alert['code'] for alert in empty['alerts']], ['node_offline'])

        node = self.node(active_jobs=[dict(id='running', project='current', status='running', cpus=1)])
        queued = dict(id='queued', project='current')
        active = live.snapshot(dict(health, queued=1, queued_jobs=[queued]), [node])
        self.assertEqual(active['coverage']['jobs_active_total'], 2)
        self.assertEqual(active['coverage']['projects_active_total'], 1)
        self.assertEqual((active['summary']['running'], active['summary']['queued']), (1, 1))
        self.assertEqual([job['id'] for job in active['jobs']], ['running', 'queued'])
        self.assertEqual([project['name'] for project in active['projects']], ['current'])

    def test_missing_metrics_and_invalid_progress_stay_unknown(self):
        n=self.node(memory_total_mb=None,cpu_percent=float('nan'),active_jobs=[
            dict(id='a',status='running',completed=3,total=2)])
        value=live.snapshot(self.health(),[n])
        self.assertIsNone(value['summary']['memory_total_mb'])
        self.assertIsNone(value['summary']['cpu_percent'])
        self.assertIsNone(value['jobs'][0]['progress'])
        self.assertIsNone(live.number(True))
        live.encode(value)

    def test_unchanged_data_suppresses_upload_until_heartbeat_and_backoff_is_bounded(self):
        value=live.snapshot(self.health(),[]);other=dict(value,sampled_at='a later time')
        signature=live.fingerprint(value)
        self.assertEqual(signature,live.fingerprint(other))
        self.assertFalse(live.due(signature,signature,59,0))
        self.assertTrue(live.due(signature,signature,60,0))
        self.assertTrue(live.due(signature,None,1,0))
        self.assertEqual([live.backoff(n)for n in (1,2,3,99)],[5,10,20,300])

    def test_collector_uses_one_cached_operator_snapshot_locally(self):
        with tempfile.TemporaryDirectory(prefix='dco-liveboard-test-')as folder:
            token=Path(folder)/'operator.token';token.write_text('private')
            requests=[]
            def read(opener,request,timeout,limit):
                requests.append(request)
                return dict(self.health(),nodes=[self.node()])
            with patch.object(live,'read_json',side_effect=read):
                _,metrics=live.collect(dict(operator_token_file=str(token)))
            self.assertEqual([x.full_url for x in requests],['http://127.0.0.1:26133/health'])
            self.assertEqual(requests[0].get_header('Authorization'),'Bearer private')
            self.assertEqual(metrics['source_queries'],1)
            self.assertGreater(metrics['source_bytes'],0)
            with self.assertRaises(ValueError):live.collect(dict(source_url='http://remote:26133',operator_token_file=str(token)))

    def test_collector_reads_live_health_when_display_sample_is_stale(self):
        with tempfile.TemporaryDirectory(prefix='dco-liveboard-stale-') as folder:
            token=Path(folder)/'operator.token'
            token.write_text('private')
            old=dict(self.health(), nodes=[self.node()], health_sampled_at=1,
                     health_age_seconds=287.7)
            fresh=dict(self.health(), nodes=[self.node()])
            with patch.object(live, 'read_json', side_effect=(old, fresh)) as read:
                value, metrics=live.collect(dict(operator_token_file=str(token)))
            self.assertEqual(metrics['source_queries'], 2)
            headers=[call.args[1] for call in read.call_args_list]
            self.assertEqual(headers[0].get_header('X-dco-health-display'), '1')
            self.assertIsNone(headers[1].get_header('X-dco-health-display'))
            self.assertEqual(headers[1].get_header('Authorization'), 'Bearer private')
            self.assertNotEqual(value['sampled_at'], '1970-01-01T00:00:01Z')

    def test_upload_sends_data_only_and_never_follows_redirects(self):
        with tempfile.TemporaryDirectory(prefix='dco-liveboard-test-')as folder:
            path=Path(folder)/'upload.local.json'
            path.write_text(json.dumps(dict(url='https://example.test/manage/live/id/update/',token='s'*48)))
            value=live.snapshot(self.health(),[])
            with patch.object(live,'read_json',return_value=dict(instance=dict(sequence=3)))as read:
                size,sequence,viewing=live.upload(dict(upload_file=str(path)),value)
            self.assertFalse(viewing)
            request=read.call_args.args[1]
            self.assertEqual(json.loads(request.data),dict(data=value))
            self.assertNotIn('s'*48,request.data.decode())
            self.assertEqual(sequence,3)
            self.assertEqual(size,len(request.data))
            self.assertIsNone(live.NoRedirect().redirect_request(None,None,None,None,None,None))
            for response in ([],None,'string',dict(instance='bad')):
                with patch.object(live,'read_json',return_value=response), self.assertRaises(ValueError):
                    live.upload(dict(upload_file=str(path)),value)
            path.write_text(json.dumps(dict(url='http://example.test/manage/live/id/update/',token='s'*48)))
            with self.assertRaises(ValueError):live.upload(dict(upload_file=str(path)),value)

    def test_upload_uses_gzip_cbor_without_stringifying_numbers(self):
        with tempfile.TemporaryDirectory(prefix='dco-liveboard-test-')as folder:
            path=Path(folder)/'upload.local.json'
            path.write_text(json.dumps(dict(url='https://example.test/manage/live/id/update/',token='s'*48)))
            value=live.snapshot(self.health(),[self.node()])
            with patch.object(live,'read_json',return_value=dict(instance=dict(sequence=4),viewer_active=True))as read:
                size,sequence,viewing=live.upload(dict(upload_file=str(path),transport='cbor-gzip'),value)
            self.assertTrue(viewing)
            request=read.call_args.args[1]
            self.assertEqual(request.get_header('Content-type'),'application/cbor')
            self.assertEqual(request.get_header('Content-encoding'),'gzip')
            self.assertEqual(gzip.decompress(request.data),cbor(dict(data=value)))
            self.assertLess(size,len(live.encode(dict(data=value))))
            self.assertEqual(sequence,4)

    def test_heartbeat_is_small_and_stale_base_recovers_with_current_full_snapshot(self):
        from io import BytesIO
        from urllib.error import HTTPError
        with tempfile.TemporaryDirectory(prefix='dco-liveboard-test-') as folder:
            path = Path(folder)/'upload.local.json'
            url = 'https://example.test/manage/live/id/update/'
            path.write_text(json.dumps(dict(url=url,token='s'*48)))
            config = dict(upload_file=str(path), transport='cbor-gzip')
            value = live.snapshot(self.health(), [self.node()])
            next_sample = dict(value, sampled_at='2026-11-18T00:00:15Z')
            latest = dict(next_sample, sampled_at='2026-11-18T00:00:30Z')
            baseline = {}
            def reply(sequence):
                return dict(instance=dict(sequence=sequence), compact=1)
            with patch.object(live, 'read_json', side_effect=[
                    reply(1), reply(2), HTTPError(url,409,'stale',{},BytesIO()), reply(4)]) as read:
                full, _, _ = live.upload(config, value, baseline=baseline)
                heartbeat, _, _ = live.upload(config, next_sample, baseline=baseline)
                self.assertEqual(baseline['kind'], 'heartbeat')
                self.assertLess(heartbeat, full / 3)
                live.upload(config, latest, baseline=baseline)
            self.assertEqual(baseline['sequence'], 4)
            self.assertEqual(baseline['kind'], 'resync')
            retry = read.call_args.args[1]
            self.assertEqual(gzip.decompress(retry.data), cbor({'data':latest}))

    def test_disabled_or_failed_optional_launcher_preserves_main_service(self):
        with tempfile.TemporaryDirectory(prefix='dco-liveboard-test-')as folder:
            root=Path(folder);(root/'.local').mkdir()
            config=root/'.local/liveboard.local.json';config.write_text('{"enabled":false}')
            with patch.object(live.subprocess,'Popen')as popen:
                with live.supervised_publisher(root):pass
                popen.assert_not_called()
            config.write_text('{"enabled":true}')
            with patch.object(live.subprocess,'Popen',side_effect=OSError('unavailable')):
                with live.supervised_publisher(root):pass
            for bad in ([],None,'string',False):
                config.write_text(json.dumps(bad))
                with patch.object(live.subprocess,'Popen')as popen:
                    with live.supervised_publisher(root):pass
                    popen.assert_not_called()

    def test_transport_absolute_deadline_does_not_accumulate_unsettled_calls(self):
        import threading,time
        release=threading.Event()
        opener=Mock()
        def opening(*args,**kwargs):
            release.wait(1)
            raise TimeoutError('simulated slow connect')
        opener.open.side_effect=opening
        started=time.monotonic()
        with self.assertRaises(TimeoutError):live.read_json(opener,None,.03,100)
        self.assertLess(time.monotonic()-started,.3)
        with self.assertRaises(TimeoutError):live.read_json(opener,None,.03,100)
        self.assertEqual(opener.open.call_count,1)
        release.set();self.assertTrue(live._network_call['done'].wait(1))

    def test_real_slow_drip_and_size_limit_have_bounded_reads(self):
        import threading,time
        from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
        from urllib.request import build_opener,ProxyHandler,Request
        class Handler(BaseHTTPRequestHandler):
            def log_message(self,*_):pass
            def do_GET(self):
                self.send_response(200);self.end_headers()
                try:
                    for part in (b' ',)*100:
                        self.wfile.write(part);self.wfile.flush();time.sleep(.015)
                except OSError:pass
        server=ThreadingHTTPServer(('127.0.0.1',0),Handler);server.daemon_threads=True
        thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        try:
            request=Request('http://127.0.0.1:'+str(server.server_port))
            started=time.monotonic()
            with self.assertRaises(TimeoutError):live.read_json(build_opener(ProxyHandler({})),request,.06,100)
            self.assertLess(time.monotonic()-started,.3)
            self.assertTrue(live._network_call['done'].wait(1))
            with self.assertRaises(ValueError):live.read_json(build_opener(ProxyHandler({})),request,1,2)
        finally:
            server.shutdown();server.server_close();thread.join()

    def test_loaded_health_can_wait_for_planning_without_triggering_retry(self):
        import threading,time
        from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
        health=dict(self.health(),nodes=[])
        class Handler(BaseHTTPRequestHandler):
            def log_message(self,*_):pass
            def do_GET(self):
                if self.path=='/health':time.sleep(2.1)
                data=live.encode(health if self.path=='/health'else dict(nodes=[]))
                self.send_response(200);self.send_header('Content-Length',str(len(data)));self.end_headers()
                self.wfile.write(data)
        server=ThreadingHTTPServer(('127.0.0.1',0),Handler);server.daemon_threads=True
        thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        try:
            with tempfile.TemporaryDirectory(prefix='dco-liveboard-test-')as folder:
                token=Path(folder)/'operator.token';token.write_text('private')
                value,_=live.collect(dict(source_url='http://127.0.0.1:'+str(server.server_port),operator_token_file=str(token)))
                self.assertEqual(value['summary']['queued'],7)
        finally:
            server.shutdown();server.server_close();thread.join()

    def test_source_or_upload_failure_records_only_safe_error_and_no_offline_queue(self):
        with tempfile.TemporaryDirectory(prefix='dco-liveboard-test-')as folder:
            folder=Path(folder);cfg=folder/'liveboard.local.json'
            cfg.write_text(json.dumps(dict(enabled=True,state_file=str(folder/'state.json'),sample_file=str(folder/'sample.json'))))
            with patch.object(live,'collect',side_effect=OSError('SECRET_TOKEN in URL')):
                state=live.run(cfg,once=True)
            self.assertEqual(state['status'],'retrying')
            self.assertEqual(state['error'],'OSError')
            self.assertNotIn('SECRET_TOKEN',(folder/'state.json').read_text())
            self.assertEqual(state['posts'],0)
            self.assertEqual(state['next_interval_seconds'],5)

    def test_fresh_output_directories_and_viewer_driven_interval(self):
        with tempfile.TemporaryDirectory(prefix='dco-liveboard-test-')as folder:
            folder=Path(folder);cfg=folder/'liveboard.local.json'
            cfg.write_text(json.dumps(dict(enabled=True,state_file=str(folder/'current/state.json'),
                sample_file=str(folder/'current/sample.json'))))
            for viewing,interval in ((False,60),(True,15)):
                with patch.object(live,'collect',return_value=(live.snapshot(self.health(),[]),{})), \
                        patch.object(live,'upload',return_value=(100,7,viewing)):
                    state=live.run(cfg,once=True)
                self.assertEqual(state['status'],'online')
                self.assertEqual(state['next_interval_seconds'],interval)
                self.assertTrue((folder/'current/sample.json').is_file())

    def test_real_readonly_collection_and_failed_upload_preserve_running_original(self):
        from remote_compute import Pool, command
        from test_compute import provider, scratch, until
        from test_center import center
        with scratch()as folder, provider(Path(folder)/'worker',cpus=2)as(_,remote),Pool([remote])as pool:
            with center(pool,Path(folder)/'center')as(_,operator):
                identity=operator.submit(command(['${tool:python}','-c','import time;time.sleep(1);print(7)'],
                    max_cpus=1,memory_mb=128,project='probe'))['id']
                until(lambda:operator.status(identity)['status']=='running')
                token=Path(folder)/'operator.token';token.write_text(operator.operator_token)
                cfg=Path(folder)/'display.local.json'
                cfg.write_text(json.dumps(dict(enabled=True,source_url=operator.url,operator_token_file=str(token),
                    sample_file=str(Path(folder)/'sample.json'),state_file=str(Path(folder)/'state.json'))))
                with patch.object(live,'upload',side_effect=OSError('unavailable')):
                    state=live.run(cfg,once=True)
                self.assertEqual(state['status'],'retrying')
                result=operator.wait(identity)
                self.assertEqual(result['id'],identity)
                self.assertEqual(result['status'],'succeeded',result)
                self.assertEqual(result['stdout'].strip(),'7')


if __name__=='__main__':unittest.main()
