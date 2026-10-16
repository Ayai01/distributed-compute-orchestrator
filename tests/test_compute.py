"""Real HTTP/process regressions, with optional installed simulator checks."""

from contextlib import contextmanager
import io
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import uuid

from remote_compute import Client, Pool, RemoteError, command, ngspice, verilator
from remote_compute.bundle import pack, task_bundle
from remote_compute.cli import ROOT, WorkerShell, table, words
from remote_compute.worker import Conflict, Server, Worker


def scratch():
    base = Path("Z:/Remote-Compute-verify") if os.name == "nt" and Path("Z:/").exists() else None
    if base:
        base.mkdir(exist_ok=True)
    return tempfile.TemporaryDirectory(prefix="test-", dir=base)


@contextmanager
def provider(root, cpus=1, **options):
    worker = Worker(root, cpus, **options)
    server = Server(("127.0.0.1", 0), worker)
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
            with provider(Path(directory) / "node", token="test-token", shared={"models": shared}) as (_, client):
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
            with provider(root) as (_, client):
                self.assertEqual(client.status(identity)["status"], "interrupted")
                self.assertEqual(client.fetch(identity)["status"], "interrupted")


class ConsoleTests(unittest.TestCase):
    def test_hybrid_worker_has_no_http_listener_when_center_listens_on_lan(self):
        with scratch() as directory:
            config = Path(directory)/'worker.json'
            config.write_text(json.dumps({'host':'0.0.0.0','shared':{},'tools':{}}))
            result = subprocess.run([sys.executable,'-B',str(ROOT/'main.py'),'--config',str(config),
                                     '--mode','hybrid','--host','0.0.0.0','--port','0','--cpus','1',
                                     '--root',str(Path(directory)/'service')],
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
        with scratch() as directory:
            task_path = Path(directory) / "task.json"
            task_path.write_text(json.dumps(python("print('terminal-task')")))
            for mode, commands in [("worker", "status\ncpus 1\npause\nresume\njobs\nquit\n"),
                                   ("hybrid", f'nodes\npolicy balanced\nset {__import__("socket").gethostname()} weight=2 limit=1\nrun "{task_path}" --output "{directory}/terminal-result"\nwait\njobs\nquit\n')]:
                result = subprocess.run([sys.executable, "-B", str(ROOT / "main.py"), "--mode", mode,
                                         "--cpus", "1", "--port", "0", "--root", str(Path(directory) / mode)],
                                        input=commands.encode(), capture_output=True, timeout=15)
                self.assertEqual(result.returncode, 0, result.stderr.decode(errors="replace"))
                self.assertNotIn("操作失败", result.stdout.decode("utf-8"))
            self.assertEqual((Path(directory) / "terminal-result/stdout.log").read_text().strip(), "terminal-task")

    def test_separate_worker_and_scheduler_processes(self):
        with scratch() as directory:
            task_path = Path(directory) / "task.json"
            task_path.write_text(json.dumps(python("print('separate-process')")))
            worker = subprocess.Popen([sys.executable, "-B", str(ROOT / "main.py"), "--mode", "worker",
                                       "--cpus", "1", "--port", "0", "--root", str(Path(directory) / "node")],
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
        with scratch() as directory, provider(Path(directory) / "one", **options) as (_, one), provider(Path(directory) / "two", **options) as (_, two):
            with Pool([one, two], policy="round_robin") as pool:
                results = pool.map([ngspice("divider.spice") for _ in range(4)], inputs=ROOT / "examples/spice")
                self.assertEqual({x["assigned_node"] for x in results}, {"node1", "node2"})
                for result in results:
                    self.assertEqual(result["status"], "succeeded", result.get("stderr"))
                    log = result["files"]["ngspice.log"].decode()
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
                    'main.cpp':'#include "Vadder.h"\n#include <fstream>\ndouble sc_time_stamp(){return 0;}\n'
                               'int main(){Vadder top; unsigned long long sum=0;'
                               'for(unsigned i=0;i<100000;i++){top.a=i;top.b=i*3;top.eval();'
                               'if(top.sum!=i*4)return 1;sum+=top.sum;}'
                               'std::ofstream("metrics.json")<<"{\\\"cycles\\\":100000,\\\"checksum\\\":"<<sum<<"}";}\n'})
                self.assertEqual(result['status'],'succeeded',result.get('stderr'))
                self.assertEqual(json.loads(result['files']['metrics.json']),dict(cycles=100000,checksum=19999800000))
                self.assertEqual(one.status(), [])
                self.assertEqual(two.status(), [])


if __name__ == "__main__":
    unittest.main()
