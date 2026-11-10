import json
import gzip
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch, Mock

from remote_compute import liveboard as live
from remote_compute.liveboard_wire import encode as cbor


class LiveboardTests(unittest.TestCase):
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
        self.assertIsNone(value['summary']['completed'])
        self.assertIsNone(value['source']['uptime_seconds'])
        self.assertEqual(value['nodes'][0]['capabilities'],['ng47','ml'])

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
        self.assertLessEqual(len(value['nodes']),16)
        self.assertLessEqual(len(value['alerts']),8)
        self.assertLessEqual(len(live.encode(dict(data=value))),65536)
        self.assertEqual(value['coverage']['jobs_active_total'],4000)
        self.assertFalse(value['coverage']['history'])

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

    def test_collector_uses_only_two_cached_read_endpoints_and_operator_header_locally(self):
        with tempfile.TemporaryDirectory(prefix='dco-liveboard-test-')as folder:
            token=Path(folder)/'operator.token';token.write_text('private')
            requests=[]
            def read(opener,request,timeout,limit):
                requests.append(request)
                return self.health()if request.full_url.endswith('/health')else dict(nodes=[self.node()])
            with patch.object(live,'read_json',side_effect=read):
                _,metrics=live.collect(dict(operator_token_file=str(token)))
            self.assertEqual([x.full_url for x in requests],['http://127.0.0.1:26133/health','http://127.0.0.1:26133/nodes'])
            self.assertIsNone(requests[0].get_header('Authorization'))
            self.assertEqual(requests[1].get_header('Authorization'),'Bearer private')
            self.assertGreater(metrics['source_bytes'],0)
            with self.assertRaises(ValueError):live.collect(dict(source_url='http://remote:26133',operator_token_file=str(token)))

    def test_upload_sends_data_only_and_never_follows_redirects(self):
        with tempfile.TemporaryDirectory(prefix='dco-liveboard-test-')as folder:
            path=Path(folder)/'upload.local.json'
            path.write_text(json.dumps(dict(url='https://example.test/manage/live/id/update/',token='s'*48)))
            value=live.snapshot(self.health(),[])
            with patch.object(live,'read_json',return_value=dict(instance=dict(sequence=3)))as read:
                size,sequence=live.upload(dict(upload_file=str(path)),value)
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
            with patch.object(live,'read_json',return_value=dict(instance=dict(sequence=4)))as read:
                size,sequence=live.upload(dict(upload_file=str(path),transport='cbor-gzip'),value)
            request=read.call_args.args[1]
            self.assertEqual(request.get_header('Content-type'),'application/cbor')
            self.assertEqual(request.get_header('Content-encoding'),'gzip')
            self.assertEqual(gzip.decompress(request.data),cbor(dict(data=value)))
            self.assertLess(size,len(live.encode(dict(data=value))))
            self.assertEqual(sequence,4)

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
        health=self.health()
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
