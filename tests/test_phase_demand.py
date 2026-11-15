"""A serial entry's useful ceiling must not fence idle admission slots."""
import json,socket,subprocess,sys,unittest
from pathlib import Path
from remote_compute import Client,Pool,python_task
from test_center import center
from test_compute import provider,scratch,until
from service_fixture import command

SOURCE='''import os,time
from pathlib import Path
def entry():
 Path('pid').write_text(str(os.getpid()))
 while not Path('release').exists():time.sleep(.02)
 return 7
'''


class PhaseDemandTests(unittest.TestCase):
    def test_recovery_distinguishes_retained_parallel_phase_from_fresh_queued_entry(self):
        from remote_compute.coordinator import _restore_phase_demand
        for status,requested,wanted in [('running',1,1),('running',4,4),('running',0,4),('queued',4,1)]:
            job=dict(task=dict(cpus=1,max_cpus=4,execution=dict(mode='entry')),
                     result=dict(status=status,requested_cpus=requested))
            _restore_phase_demand(job)
            self.assertEqual(job['desired_cpus'],wanted)
        job['desired_cpus']=3;_restore_phase_demand(job)
        self.assertEqual(job['desired_cpus'],3)

    def test_serial_entries_fill_all_slots_before_requesting_parallel_phases(self):
        with scratch()as folder,provider(Path(folder)/'worker',cpus=4)as(_,worker),Pool([worker])as pool,center(pool,Path(folder)/'center')as(state,client):
            ids=[client.submit(python_task('hold:entry',max_cpus=4,memory_mb=128,outputs=['pid']),files={'hold.py':SOURCE})['id']for _ in range(4)]
            until(lambda:all(client.status(i)['status']=='running'for i in ids),seconds=12)
            self.assertEqual([state.jobs[i]['desired_cpus']for i in ids],[1]*4)
            self.assertEqual(pool.nodes[0].reserved,4)
            for i in ids:client.write_job_file(i,'release','done')
            for i in ids:
                client.wait(i,wait_timeout=12);self.assertEqual(client.fetch(i)['status'],'succeeded')

    def test_native_reattachment_restores_serial_demand_and_admits_original_queue(self):
        with scratch()as folder,provider(Path(folder)/'worker',cpus=4)as(worker_state,worker):
            root=Path(folder);config=root/'worker.json';config.write_text('{}')
            nodes=root/'nodes.json';nodes.write_text(json.dumps(dict(nodes=[dict(name='remote',url=worker.url)])))
            with socket.socket()as sock:sock.bind(('127.0.0.1',0));port=sock.getsockname()[1]
            client=Client('http://127.0.0.1:'+str(port),timeout=1,operator=True)
            argv=command('--mode','scheduler','--nodes',str(nodes),'--config',str(config),'--root',str(root/'center'),'--port',str(port),'--host','127.0.0.1','--headless','--no-dashboard')
            processes=[]
            with (root/'native.log').open('w+',encoding='utf8')as log:
                def start():
                    p=subprocess.Popen(argv,stdout=log,stderr=subprocess.STDOUT,cwd=Path(__file__).resolve().parents[1]);processes.append(p)
                    def ready():
                        if p.poll()is not None:
                            log.flush();log.seek(0);raise RuntimeError(log.read())
                        try:
                            client.operator_token=(root/'center/scheduler/operator.token').read_text().strip()
                            return client.health()['role']=='scheduler'
                        except Exception:return False
                    until(ready,seconds=12);return p
                try:
                    first=start()
                    task=python_task('hold:entry',max_cpus=4,memory_mb=128,outputs=['pid'],project='phase-recovery')
                    original=client.submit(task,files={'hold.py':SOURCE});identity=original['id']
                    until(lambda:client.status(identity)['status']=='running',seconds=10)
                    until(lambda:(worker_state.jobs[identity]['directory']/'input/pid').exists(),seconds=10)
                    pid=client.read_job_file(identity,'pid');started=client.status(identity)['started']
                    self.assertEqual(client.resize(identity,1,memory_mb=129)['cpus'],1)
                    client.control(paused=True)
                    peers=[client.submit(task,files={'hold.py':SOURCE})for _ in range(3)]
                    first.kill();first.wait(timeout=5);start()
                    ids=[identity]+[r['id']for r in peers]
                    until(lambda:all(client.status(i)['status']=='running'for i in ids),seconds=12)
                    self.assertEqual(client.read_job_file(identity,'pid'),pid)
                    self.assertEqual(client.status(identity)['started'],started)
                    self.assertEqual(client.health()['used_cpus'],4)
                    for i in ids:client.write_job_file(i,'release','done')
                    for accepted in [original]+peers:
                        client.wait(accepted['id'],wait_timeout=12);receipt=client.fetch(accepted['id'])
                        self.assertEqual(receipt['request_sha256'],accepted['request_sha256'])
                        self.assertEqual(receipt['status'],'succeeded');self.assertFalse(receipt['attempts'])
                finally:
                    for p in processes:
                        if p.poll()is None:p.kill()
                        p.wait(timeout=5)
