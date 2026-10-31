"""Real HTTP/process regressions, with optional installed simulator checks."""

from contextlib import contextmanager
import io
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import uuid
from unittest.mock import patch

from remote_compute import Client, Pool, RemoteError, command, ngspice, verilator
from remote_compute.bundle import pack, task_bundle
from remote_compute.cli import ROOT, WorkerShell, table, words
from remote_compute.worker import Conflict, Server, Worker


def scratch():
    base = Path("Z:/DCO-verify") if os.name == "nt" and Path("Z:/").exists() else None
    if base:
        base.mkdir(exist_ok=True)
    return tempfile.TemporaryDirectory(prefix="test-", dir=base)


@contextmanager
def provider(root, cpus=1, *, trusted_networks=None, **options):
    # Transport/accounting tests should not benchmark every fixture or consume
    # a production-sized CPU budget; the actual kernel has its own regression.
    # Host commit pressure from concurrent research belongs to the dedicated
    # capacity regressions, not these isolated HTTP/process fixtures.
    with patch('remote_compute.resources.memory',return_value=(48*1024,40*1024)), \
            patch('remote_compute.resources.commit_memory',return_value=None):
        with patch('remote_compute.worker.cpu_benchmark',side_effect=lambda n,*args:
                   dict(name='test-kernel',score=1000.0,unit='kiter/s',threads=n,seconds=2.0)):
            worker = Worker(root, cpus, **options)
        server = Server(("127.0.0.1", 0), worker, trusted_networks=trusted_networks)
        thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True)
        thread.start()
        try:
            yield worker, Client(f"http://127.0.0.1:{server.server_port}", options.get("token", ""))
        finally:
            server.shutdown()
            thread.join()
            server.server_close()
            worker.close()


def python(code, **options):
    return command(["${tool:python}", "-u", "-c", code], **options)


def until(check, seconds=5):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if check():
            return
        time.sleep(0.02)
    raise AssertionError("Condition did not complete")


class ExecutionTests(unittest.TestCase):
    def test_opt_in_publishes_live_cpu_snapshot_and_final_receipt(self):
        code=("import json,os,time\nfrom pathlib import Path\n"
              "p=Path(os.environ['REMOTE_COMPUTE_JOB_DIR'])/'result.json'\n"
              "end=time.monotonic()+4\n"
              "while time.monotonic()<end:\n"
              " data=json.loads(p.read_text())\n"
              " if data.get('cumulative_cpu_seconds',0)>0:\n"
              "  Path('observed.json').write_text(json.dumps(data));break\n"
              " until=time.process_time()+.005\n"
              " while time.process_time()<until:pass\n"
              " time.sleep(.01)\n"
              "else:raise RuntimeError('No live CPU snapshot')\n")
        task=python(code,env={'DCO_CPU_ACCOUNTING':'1'},outputs=['observed.json'],timeout=6)
        with patch('remote_compute.resources.cpu_frequency',return_value=dict(ghz=4,source='test')):
            result=self.client.run(task)
        self.assertEqual(result['status'],'succeeded',result.get('stderr'))
        observed=json.loads(result['files']['observed.json'])
        self.assertGreater(observed['cumulative_cpu_seconds'],0)
        self.assertEqual(result['cumulative_cpu_seconds'],result['cpu_seconds'])
        self.assertAlmostEqual(result['cpu_work_ghz_seconds'],4*result['cpu_seconds'])
        self.assertEqual(result['cpu_frequency']['source'],'test')

    def test_default_tasks_do_not_probe_frequency_or_add_accounting_fields(self):
        with patch('remote_compute.resources.cpu_frequency',side_effect=AssertionError('Unexpected probe')):
            result=self.client.run(python("print('ordinary task')"))
        self.assertEqual(result['status'],'succeeded')
        self.assertNotIn('cpu_frequency',result)

    def test_live_snapshot_sharing_error_does_not_interrupt_task(self):
        saved=self.worker._save
        blocked=[]
        def save(job):
            if job['result']['status']=='running' and job['result'].get('cumulative_cpu_seconds',0)>0:
                blocked.append(True)
                raise PermissionError('snapshot reader holds the old file')
            saved(job)
        task=python("import time;sum(i*i for i in range(100000));time.sleep(1.3)",
                    env={'DCO_CPU_ACCOUNTING':'1'})
        with patch.object(self.worker,'_save',side_effect=save), \
                patch('remote_compute.resources.cpu_frequency',return_value=dict(ghz=4,source='test')):
            result=self.client.run(task)
        self.assertTrue(blocked)
        self.assertEqual(result['status'],'succeeded',result.get('stderr'))
        self.assertEqual(result['cpu_seconds'],result['cumulative_cpu_seconds'])

    def test_configured_temp_root_is_owned_and_reclaimed_after_exit(self):
        with scratch() as directory:
            root=Path(directory)
            with provider(root/'disk',temp_root=root/'scratch') as (worker,client):
                for exit_code in (0,3):
                    task=python("import os,sys;from pathlib import Path\nPath(os.environ['TMPDIR'],'raw.dat').write_bytes(b'raw')\nprint(os.environ['TMPDIR'],flush=True)\nsys.exit("+str(exit_code)+")")
                    result=client.run(task)
                    temporary=Path(result['stdout'].strip())
                    self.assertEqual(temporary.parent,worker.temp_root)
                    self.assertFalse(temporary.exists())
                    self.assertEqual(result['status'],'succeeded' if exit_code==0 else 'failed')
            self.assertFalse(worker.temp_root.exists())

    def test_exit_keeps_nonempty_scratch_namespace(self):
        with scratch() as directory:
            root=Path(directory)
            with provider(root/'disk',temp_root=root/'scratch') as (worker,_client):
                retained=worker.temp_root/'unfinished.raw'
                retained.write_bytes(b'uncollected native state')
            self.assertEqual(retained.read_bytes(),b'uncollected native state')

    def test_task_temp_is_reclaimed_before_results_are_collected(self):
        for code,expected in ((0,'succeeded'),(3,'failed')):
            with self.subTest(status=expected):
                task=python("from pathlib import Path;import os,sys\n"
                            "Path(os.environ['TEMP'],'unused-raw.bin').write_bytes(b'x'*1024*1024)\n"
                            "Path('answer.txt').write_text('retained result')\n"
                            f"sys.exit({code})",outputs=['answer.txt'])
                identity=self.client.submit(task)['id']
                self.assertEqual(self.client.wait(identity)['status'],expected)
                self.assertFalse((self.worker.root/'jobs'/identity/'tmp').exists())
                self.assertEqual(self.client.fetch(identity)['files']['answer.txt'],b'retained result')

    def test_timeout_reclaims_temp_and_retains_diagnostic_logs(self):
        task=python("from pathlib import Path;import os,time\n"
                    "Path(os.environ['TEMP'],'unfinished.bin').write_bytes(b'x'*1024*1024)\n"
                    "print('ready',flush=True)\ntime.sleep(10)",timeout=.3)
        identity=self.client.submit(task)['id']
        self.assertEqual(self.client.wait(identity)['status'],'timed_out')
        self.assertFalse((self.worker.root/'jobs'/identity/'tmp').exists())
        self.assertIn('ready',self.client.fetch(identity)['stdout'])

    @unittest.skipUnless(os.name=='nt','Windows mandatory labels')
    def test_low_integrity_tool_writes_job_outputs_and_temp_without_changing_parent_acl(self):
        from remote_compute.process import low_integrity_directory
        def permissions(path, *, labels=False):
            raw=subprocess.check_output(['icacls.exe',str(path)],creationflags=subprocess.CREATE_NO_WINDOW).decode(errors='replace')
            return raw if labels else '\n'.join(line for line in raw.splitlines() if '(NW)' not in line)
        low_tool=self.root/'low-tool'
        low_tool.mkdir()
        original=permissions(low_tool)
        low_integrity_directory(low_tool)
        self.assertEqual(permissions(low_tool),original)
        executable=low_tool/'cmd.exe'
        shutil.copyfile(Path(os.environ['SystemRoot'])/'System32/cmd.exe',executable)
        ordinary=self.root/'ordinary'
        ordinary.mkdir()
        denied=subprocess.run([str(executable),'/d','/c','echo denied>denied.txt'],cwd=ordinary,
                              capture_output=True,creationflags=subprocess.CREATE_NO_WINDOW,timeout=5)
        self.assertNotEqual(denied.returncode,0)
        self.assertFalse((ordinary/'denied.txt').exists())
        task=command([str(executable),'/d','/c','run.cmd'],outputs=['value.txt'])
        script=b'@echo off\r\necho result>value.txt\r\necho temp>"%TEMP%\\value.txt"\r\ntype "%TEMP%\\value.txt"\r\n'
        for external in (False,True):
            with self.subTest(external_temp=external), provider(
                    self.root/('external' if external else 'default'),
                    temp_root=self.root/'scratch' if external else None) as (worker,client):
                parents=[worker.root/'jobs', *([worker.temp_root] if external else [])]
                before={parent:permissions(parent,labels=True) for parent in parents}
                identity=client.submit(task,files={'run.cmd':script})['id']
                client.wait(identity)
                temporary=worker._temporary(worker.jobs[identity])
                result=client.fetch(identity)
                self.assertEqual(result['status'],'succeeded',result.get('stderr'))
                self.assertEqual(result['files']['value.txt'].strip(),b'result')
                self.assertEqual(result['stdout'].strip(),'temp')
                self.assertFalse(temporary.exists())
                for parent in parents:
                    self.assertEqual(permissions(parent,labels=True),before[parent])

    def test_explicit_numerical_threads_survive_worker_defaults(self):
        keys=('OMP_NUM_THREADS','OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','NUMEXPR_NUM_THREADS')
        with scratch() as directory, provider(Path(directory)/'worker',cpus=3,env={key:'99' for key in keys}) as (_,client):
            task=python("import json,os;print(json.dumps({k:os.environ[k] for k in " + repr(keys) + "}))",
                        cpus=3,env={'OMP_NUM_THREADS':'1','OPENBLAS_NUM_THREADS':'1','NUMEXPR_NUM_THREADS':'${cpus}'})
            result=client.run(task)
            self.assertEqual(result['status'],'succeeded',result.get('stderr'))
            self.assertEqual(json.loads(result['stdout']),dict(zip(keys,('1','1','3','3'))))

    def setUp(self):
        self.temp = scratch()
        self.root = Path(self.temp.name)
        self.context = provider(self.root / "node")
        self.worker, self.client = self.context.__enter__()

    def tearDown(self):
        self.context.__exit__(None, None, None)
        self.temp.cleanup()

    def test_run_returns_files_and_cleans_node(self):
        task = python("from pathlib import Path; print('hello'); Path('answer.txt').write_text('42')", outputs=["answer.txt"])
        result = self.client.run(task)
        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(result["files"]["answer.txt"], b"42")
        self.assertIn("hello", result["stdout"])
        self.assertEqual(self.client.status(), [])
        self.assertEqual(list((self.worker.root / "jobs").iterdir()), [])

    def test_output_directory_contains_full_logs_and_result(self):
        result = self.client.run(python("print('saved')"), output_dir=self.root / "result")
        self.assertEqual((self.root / "result/stdout.log").read_text().strip(), "saved")
        self.assertEqual(json.loads((self.root / "result/result.json").read_text())["id"], result["id"])

    def test_idempotent_submit_runs_once(self):
        task = python("from pathlib import Path; p=Path('count'); p.write_text(p.read_text()+'1' if p.exists() else '1')", outputs=["count"])
        identity = uuid.uuid4().hex
        first = self.client.submit(task, job_id=identity)
        second = self.client.submit(task, job_id=identity)
        self.assertEqual(first["request_sha256"], second["request_sha256"])
        self.client.wait(identity)
        self.assertEqual(self.client.fetch(identity)["files"]["count"], b"1")

    def test_reusing_id_with_different_input_is_rejected(self):
        identity = uuid.uuid4().hex
        self.client.submit(python("print(1)"), job_id=identity)
        with self.assertRaisesRegex(RemoteError, "409"):
            self.client.submit(python("print(2)"), job_id=identity)

    def test_steps_stop_at_failure(self):
        task = python("import sys; print('failed'); sys.exit(7)")
        task["steps"].append(["${tool:python}", "-c", "raise RuntimeError('should not run')"])
        result = self.client.run(task)
        self.assertEqual(result["status"], "failed")
        self.assertEqual(len(result["steps"]), 1)
        self.assertEqual(result["steps"][0]["returncode"], 7)

    def test_timeout_stops_process(self):
        result = self.client.run(python("import time; time.sleep(30)", timeout=0.15))
        self.assertEqual(result["status"], "timed_out")
        self.assertLess(result["elapsed"], 3)
        self.assertEqual(self.client.health()["used_cpus"], 0)

    def test_cancel_stops_descendants(self):
        code = ("import subprocess,sys,time; from pathlib import Path; "
                "subprocess.Popen([sys.executable,'-c',\"import time; from pathlib import Path; time.sleep(1); Path('leaked').write_text('bad')\"]); "
                "Path('ready').write_text('yes'); time.sleep(30)")
        identity = self.client.submit(python(code))["id"]
        work = self.worker.root / "jobs" / identity / "input"
        until(lambda: (work / "ready").exists())
        self.client.cancel(identity)
        self.assertEqual(self.client.wait(identity)["status"], "cancelled")
        time.sleep(1.05)
        self.assertFalse((work / "leaked").exists())

    def test_priority_pause_and_queued_cancellation(self):
        self.client.control(paused=True)
        identities = []
        for priority in [1, 9, 4]:
            identities.append(self.client.submit(python("import time; time.sleep(.1)", priority=priority))["id"])
        cancelled = self.client.submit(python("raise RuntimeError('cancelled')"))["id"]
        self.client.cancel(cancelled)
        self.assertEqual(self.client.status(cancelled)["status"], "cancelled")
        self.assertTrue(all(self.client.status(x)["status"] == "queued" for x in identities))
        self.client.control(paused=False)
        results = [self.client.wait(x) for x in identities]
        started = sorted(results, key=lambda x: x["started"])
        self.assertEqual([x["id"] for x in started], [identities[1], identities[2], identities[0]])

    def test_cpu_budget_serializes_weighted_jobs_and_live_limit(self):
        self.client.control(cpus=2)
        first = self.client.submit(python("import time; time.sleep(.3)", cpus=2))["id"]
        until(lambda: self.client.status(first)["status"] == "running")
        self.client.control(cpus=1)
        second = self.client.submit(python("print('next')"))["id"]
        one, two = self.client.wait(first), self.client.wait(second)
        self.assertEqual(one["status"], "succeeded")
        self.assertGreaterEqual(two["started"], one["finished"])

    def test_live_log_tail(self):
        identity = self.client.submit(python("import time; print('ready',flush=True); time.sleep(1)"))["id"]
        until(lambda: "ready" in self.client.logs(identity)["stdout"])
        self.client.cancel(identity)
        self.client.wait(identity)

    def test_assets_sync_render_and_shared_roots(self):
        models = self.root / "models"
        models.mkdir()
        (models / "model.txt").write_text("asset-data")
        identity = self.client.sync(models)
        self.assertEqual(identity, self.client.sync(models))
        task = python("from pathlib import Path; p=Path('config.txt').read_text(); print(Path(p).read_text())",
                      assets={"models": identity}, render=["config.txt"])
        result = self.client.run(task, files={"config.txt": "${asset:models}/model.txt"})
        self.assertEqual(result["status"], "succeeded")
        self.assertIn("asset-data", result["stdout"])
        self.assertEqual(len(list((self.worker.root / "assets").iterdir())), 1)

    def test_path_and_task_validation_precede_admission(self):
        identity = uuid.uuid4().hex
        payload = pack({"task.json": json.dumps(python("print(1)")).encode(), "input/ok": b"ok"})
        self.assertEqual(self.client.request("/jobs/" + identity, payload)["id"], identity)
        with self.assertRaises(ValueError):
            task_bundle(python("print(1)"), files={"../../outside": b"bad"})
        for task in [python("print(1)", cpus=3), {"steps": "echo invalid"}, {"steps": [["x"]], "typo": 1}]:
            with self.assertRaises(RemoteError):
                self.client.submit(task)

    def test_root_lock_and_restart_keeps_finished_result(self):
        with self.assertRaises(Conflict):
            Worker(self.worker.root, 1)
        identity = self.client.submit(python("print('persistent')"))["id"]
        self.client.wait(identity)
        saved = self.worker.get(identity)
        self.assertEqual(saved["status"], "succeeded")
        self.assertTrue((self.worker.root / "jobs" / identity / "result.json").exists())


class PoolTests(unittest.TestCase):
    def test_multi_node_order_and_round_robin(self):
        with scratch() as directory, provider(Path(directory) / "one") as (_, one), provider(Path(directory) / "two") as (_, two):
            with Pool([{"name": "one", "url": one.url}, {"name": "two", "url": two.url}], policy="round_robin") as pool:
                tasks = [python(f"import time; time.sleep(.1); print({i})") for i in range(6)]
                results = pool.map(tasks)
                self.assertEqual([x["stdout"].strip() for x in results], list(map(str, range(6))))
                self.assertEqual({x["assigned_node"] for x in results}, {"one", "two"})
                self.assertEqual(one.status(), [])
                self.assertEqual(two.status(), [])
                pool.refresh()
                for node in pool.nodes:
                    self.assertFalse(node.known_jobs)
                    self.assertFalse(node.live_jobs)
                    self.assertEqual((node.reserved,node.external),(0,0))

    def test_adaptive_learns_fast_node_and_controls_route(self):
        with scratch() as directory, provider(Path(directory) / "slow", tags=["spice"], env={"DELAY": ".2"}) as (_, slow), provider(Path(directory) / "fast", tags=["rtl"], env={"DELAY": ".01"}) as (_, fast):
            with Pool([{"name": "slow", "url": slow.url}, {"name": "fast", "url": fast.url}]) as pool:
                task = python("import os,time; time.sleep(float(os.environ['DELAY']))", label="same-batch")
                first = pool.run(task)
                second = pool.run(task)
                third = pool.run(task)
                self.assertEqual({first["assigned_node"], second["assigned_node"]}, {"slow", "fast"})
                self.assertEqual(third["assigned_node"], "fast")
                tagged = dict(task, requires=["spice"])
                self.assertEqual(pool.run(tagged)["assigned_node"], "slow")
                pool.configure("fast", enabled=False)
                self.assertEqual(pool.run(task)["assigned_node"], "slow")
                pool.configure("fast", enabled=True, weight=10, limit=1)
                pool.configure(policy="balanced")
                self.assertEqual(pool.run(task)["assigned_node"], "fast")
                with self.assertRaises(RemoteError):
                    pool.run(dict(task, requires=["gpu"]))

    def test_token_and_shared_reference(self):
        with scratch() as directory:
            shared = Path(directory) / "shared"
            shared.mkdir()
            (shared / "data").write_text("shared-data")
            with provider(Path(directory) / "node", token="test-token", shared={"models": shared}, trusted_networks=[]) as (_, client):
                with self.assertRaisesRegex(RemoteError, "401"):
                    Client(client.url).health()
                result = client.run(python("import os; from pathlib import Path; print(Path(os.environ['DATA']).read_text())",
                                           env={"DATA": "${shared:models}/data"}))
                self.assertIn("shared-data", result["stdout"])

    def test_restart_marks_unfinished_task_and_recovers_artifacts(self):
        with scratch() as directory:
            root = Path(directory) / "node"
            with provider(root) as (_, client):
                client.control(paused=True)
                identity = client.submit(python("print('queued')"))["id"]
                saved = client.status(identity)
            # Reconstruct an interrupted on-disk state as left by an abrupt shutdown.
            result_file = root / "jobs" / identity / "result.json"
            result_file.write_text(json.dumps(saved))
            (root / 'history.index.jsonl').unlink()
            with provider(root) as (_, client):
                self.assertEqual(client.status(identity)["status"], "interrupted")
                self.assertEqual(client.fetch(identity)["status"], "interrupted")


class ConsoleTests(unittest.TestCase):
    def test_hybrid_worker_has_no_http_listener_when_center_listens_on_lan(self):
        from service_fixture import command
        with scratch() as directory:
            config = Path(directory)/'worker.json'
            config.write_text(json.dumps({'host':'0.0.0.0','shared':{},'tools':{}}))
            nodes = Path(directory)/'nodes.json'
            nodes.write_text(json.dumps({'nodes':[]}))
            result = subprocess.run(command('--config',str(config),
                                     '--mode','hybrid','--host','0.0.0.0','--port','0','--cpus','1',
                                     '--nodes',str(nodes),'--root',str(Path(directory)/'service')),
                                    input=b'quit\n',capture_output=True,timeout=15)
            self.assertEqual(result.returncode,0,result.stderr.decode(errors='replace'))
            output = result.stdout.decode('utf8')
            self.assertIn('本机算力（进程内）',output)
            self.assertNotIn('算力提供者 http://',output)
            self.assertIn('调度中心 http://0.0.0.0:',output)

    def test_modes_have_one_entry_and_stable_help(self):
        result = subprocess.run([sys.executable, "-B", str(ROOT / "main.py"), "--help"], capture_output=True)
        self.assertEqual(result.returncode, 0)
        self.assertIn(b"scheduler,hybrid,worker", result.stdout)
        self.assertFalse((ROOT / "scheduler.py").exists())
        self.assertFalse((ROOT / "worker.py").exists())

    def test_worker_and_hybrid_console_accept_commands(self):
        from service_fixture import command
        with scratch() as directory:
            task_path = Path(directory) / "task.json"
            task_path.write_text(json.dumps(python("print('terminal-task')")))
            nodes_path = Path(directory) / 'nodes.json'
            nodes_path.write_text(json.dumps({'nodes':[]}))
            for mode, commands in [("worker", "status\ncpus 1\npause\nresume\njobs\nquit\n"),
                                   ("hybrid", f'nodes\npolicy balanced\nset {__import__("socket").gethostname()} weight=2 limit=1\nrun "{task_path}" --output "{directory}/terminal-result"\nwait\njobs\nquit\n')]:
                result = subprocess.run(command("--mode", mode,
                                         "--cpus", "1", "--port", "0", "--nodes", str(nodes_path),
                                         "--root", str(Path(directory) / mode)),
                                        input=commands.encode(), capture_output=True, timeout=15)
                self.assertEqual(result.returncode, 0, result.stderr.decode(errors="replace"))
                self.assertNotIn("操作失败", result.stdout.decode("utf-8"))
            self.assertEqual((Path(directory) / "terminal-result/stdout.log").read_text().strip(), "terminal-task")

    def test_separate_worker_and_scheduler_processes(self):
        from service_fixture import command
        with scratch() as directory:
            task_path = Path(directory) / "task.json"
            task_path.write_text(json.dumps(python("print('separate-process')")))
            worker = subprocess.Popen(command("--mode", "worker",
                                       "--cpus", "1", "--port", "0", "--root", str(Path(directory) / "node")),
                                      stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            try:
                line = worker.stdout.readline().decode("utf-8")
                url = re.search(r"http://[^ ]+", line)[0]
                result = subprocess.run([sys.executable, "-B", str(ROOT / "main.py"), "--mode", "scheduler",
                                         "--url", url, "run", str(task_path), "--output", str(Path(directory) / "result")],
                                        capture_output=True, timeout=15)
                self.assertEqual(result.returncode, 0, result.stderr.decode(errors="replace"))
                self.assertEqual((Path(directory) / "result/stdout.log").read_text().strip(), "separate-process")
                self.assertEqual(Client(url).status(), [])
            finally:
                worker.communicate(b"quit\n", timeout=10)
            self.assertEqual(worker.returncode, 0)

    def test_console_empty_lines_errors_and_path_quoting(self):
        from contextlib import redirect_stdout
        self.assertEqual(words('run "D:/my project/#name/task.json"'), ["run", "D:/my project/#name/task.json"])
        if os.name == "nt":
            self.assertEqual(words('run "D:\\my project\\task.json"'), ["run", "D:\\my project\\task.json"])
        with io.StringIO() as output, redirect_stdout(output):
            table([], ["id", "status"])
        with scratch() as directory, provider(Path(directory) / "node") as (worker, _):
            shell = WorkerShell(worker)
            with io.StringIO() as output, redirect_stdout(output):
                shell.onecmd("cpus nope")
                shell.onecmd("")
                shell.onecmd("cpus 1")
                self.assertIn("操作失败", output.getvalue())
            self.assertEqual(worker.capacity, 1)


@unittest.skipUnless(os.environ.get("REMOTE_COMPUTE_NATIVE") == "1", "Use --test-native for installed simulators")
class NativeTests(unittest.TestCase):
    def test_real_ngspice_two_nodes_and_verilator(self):
        config_path = os.environ.get("REMOTE_COMPUTE_TEST_CONFIG")
        config = json.loads(Path(config_path).read_text(encoding="utf-8-sig")) if config_path else {}
        options = {key: config[key] for key in ("tools", "shared", "env", "tags") if key in config}
        with scratch() as directory, \
                provider(Path(directory) / "one", temp_root=Path(directory)/'scratch', **options) as (_, one), \
                provider(Path(directory) / "two", temp_root=Path(directory)/'scratch', **options) as (_, two):
            with Pool([one, two], policy="round_robin") as pool:
                results = pool.map([ngspice("divider.spice") for _ in range(4)], inputs=ROOT / "examples/spice")
                self.assertEqual({x["assigned_node"] for x in results}, {"node1", "node2"})
                for result in results:
                    self.assertEqual(result["status"], "succeeded", result.get("stderr"))
                    self.assertNotIn('Error opening code model', result.get('stderr',''))
                    log = result["files"]["ngspice.log"].decode()
                    self.assertNotIn('Error opening code model', log)
                    value = float(re.search(r"vout_at\s*=\s*([\d.eE+-]+)", log)[1])
                    self.assertAlmostEqual(value, 1.2, places=6)
                rtl = pool.run(verilator(["tb.sv"], "tb"), inputs=ROOT / "examples/rtl")
                self.assertEqual(rtl["status"], "succeeded", rtl.get("stderr"))
                self.assertIn("PASS counter 4096", rtl["stdout"])
                self.assertEqual(json.loads(rtl["files"]["metrics.json"]), {"cycles": 4096, "passed": True})
                custom=command(["${tool:verilator}","--cc","adder.sv","--top-module","adder",
                                "--exe","${work}/main.cpp","--build","-j","${cpus}","-o","probe${exe}"],
                               outputs=["metrics.json"])
                custom['steps'].append(["${work}/obj_dir/probe${exe}"])
                result=pool.run(custom,files={
                    'adder.sv':'module adder(input logic [31:0] a,b, output logic [31:0] sum); assign sum=a+b; endmodule\n',
                    'main.cpp':'#include "Vadder.h"\n#include <fstream>\n#include <filesystem>\ndouble sc_time_stamp(){return 0;}\n'
                               'int main(){Vadder top; unsigned long long sum=0;'
                               'for(unsigned i=0;i<100000;i++){top.a=i;top.b=i*3;top.eval();'
                               'if(top.sum!=i*4)return 1;sum+=top.sum;}'
                               'auto path=std::filesystem::u8path(u8"中文 路径.txt");'
                               '{std::ofstream stream(path);stream<<"portable";}'
                               'std::ifstream stream(path);std::string value;stream>>value;'
                               'if(value!="portable")return 2;'
                               'std::ofstream("metrics.json")<<"{\\\"cycles\\\":100000,\\\"checksum\\\":"<<sum<<"}";}\n'})
                self.assertEqual(result['status'],'succeeded',result.get('stderr'))
                self.assertEqual(json.loads(result['files']['metrics.json']),dict(cycles=100000,checksum=19999800000))
                self.assertEqual(one.status(), [])
                self.assertEqual(two.status(), [])


if __name__ == "__main__":
    unittest.main()


@contextmanager
def calibration_environment():
    # CLI lifecycle tests reuse a fixture cache; native calibration has its own tests.
    from remote_compute.benchmark import cpu_identity,NAME
    with scratch() as directory:
        cache=Path(directory)
        for cpus in (1,2):
            value=dict(name=NAME,available=True,score=1,unit='Miter/s',threads=cpus,
                       single=dict(name=NAME,available=True,score=1,unit='Miter/s',threads=1))
            (cache/(str(cpus)+'.json')).write_text(json.dumps(dict(identity=cpu_identity(cpus),benchmark=value)))
        with patch.dict(os.environ,DCO_BENCHMARK_DIR=str(cache)):
            yield
