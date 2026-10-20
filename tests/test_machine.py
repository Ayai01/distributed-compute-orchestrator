"""Host operations use real processes and the same center API as simulations."""

from contextlib import redirect_stdout
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import unittest
import uuid

from remote_compute import Client, Pool, RemoteError, command
from remote_compute.cli import ROOT, SchedulerShell
from test_center import center
from test_compute import provider, scratch, until


class MachineTests(unittest.TestCase):
    def setUp(self):
        self.temp = scratch()
        self.root = Path(self.temp.name)
        self.project = self.root / "project # 中文"
        self.project.mkdir()
        self.provider = provider(self.root / "worker", cpus=2, memory_mb=1024,
                                 shared={"project": str(self.project)}, token="local-test")
        self.worker, self.node = self.provider.__enter__()
        self.pool = Pool([{"name": "device2", "url": self.node.url, "token": "local-test"}])
        self.center = center(self.pool, self.root / "center")
        self.state, self.client = self.center.__enter__()
        self.machine = self.client.machine("device2", cwd="${shared:project}")

    def tearDown(self):
        self.center.__exit__(None, None, None)
        self.pool.close()
        self.provider.__exit__(None, None, None)
        self.temp.cleanup()

    def test_update_project_and_run_with_only_center_connection(self):
        source = self.root / "local"
        source.mkdir()
        (source / "calculation.py").write_text("def answer(): return 6\n", encoding="utf-8")
        (source / "binary.dat").write_bytes(bytes(range(256)))
        self.machine.upload(source, ".")
        first = self.machine.python("import calculation; print(calculation.answer())")
        self.assertEqual(first["status"], "succeeded", first["stderr"])
        self.assertEqual(first["stdout"].strip(), "6")
        self.machine.write_text("calculation.py", "def answer(): return 42\n")
        second = self.machine.python("import calculation; print(calculation.answer())")
        self.assertEqual(second["stdout"].strip(), "42")
        self.assertEqual(second["assigned_node"], "device2")
        self.assertEqual(Path(second["cwd"]), self.project)
        self.assertGreaterEqual(second["peak_rss_mb"], 0)
        self.assertEqual(self.machine.read_file("binary.dat"), bytes(range(256)))
        self.assertEqual(self.machine.read_text("calculation.py"), "def answer(): return 42\n")
        listing = self.machine.listdir()
        self.assertEqual({e["name"] for e in listing["entries"]}, {"calculation.py", "binary.dat"})
        target = self.root / "download"
        manifest = self.machine.download(".", target)
        self.assertEqual(manifest["kind"], "directory")
        self.assertEqual((target / "binary.dat").read_bytes(), bytes(range(256)))
        self.assertEqual(self.client.status(), [])
        self.assertEqual(self.node.status(), [])

    def test_stdin_arguments_and_job_outputs_stay_separate_from_project(self):
        script = self.root / "task 中文.py"
        script.write_text("import os,sys\nfrom pathlib import Path\n"
                          "print(sys.argv[1], sys.stdin.buffer.read().hex(), os.environ['MESSAGE'])\n"
                          "Path(os.environ['OUTPUT']).write_text('collected')\n"
                          "Path('kept.txt').write_text('project file')\n", encoding="utf-8")
        result = self.machine.python(script=script, args=["two words"], stdin=b"\0\xff\n",
                                     outputs=["result.txt"], env={"OUTPUT": "${work}/result.txt", "MESSAGE": "中文"})
        self.assertEqual(result["status"], "succeeded", result["stderr"])
        self.assertIn("two words 00ff0a 中文", result["stdout"])
        self.assertEqual(result["files"]["result.txt"], b"collected")
        self.assertEqual(self.machine.read_text("kept.txt"), "project file")
        self.assertFalse((self.project / "remote_op").exists())
        self.assertFalse((self.project / "result.txt").exists())

    def test_detached_job_reconnect_logs_resources_and_cancel_tree(self):
        code = ("import subprocess,sys,time\nfrom pathlib import Path\n"
                "subprocess.Popen([sys.executable,'-c',\"import time; from pathlib import Path; "
                "time.sleep(1); Path('leaked').write_text('bad')\"])\n"
                "print('ready', flush=True)\ntime.sleep(30)\n")
        accepted = self.machine.python(code, wait=False, memory_mb=64)
        identity = accepted["id"]
        reconnected = Client(self.client.url)
        until(lambda: "ready" in reconnected.logs(identity)["stdout"])
        self.assertEqual(self.node.health()["used_cpus"], 1)
        health=self.node.health()
        self.assertEqual(health['memory_requested_mb'],64)
        self.assertGreater(health['memory_reserved_mb'],0)
        self.assertEqual(health['memory_accounting'],'rss')
        reconnected.cancel(identity)
        self.assertEqual(reconnected.wait(identity)["status"], "cancelled")
        reconnected.fetch(identity)
        time.sleep(1.1)
        self.assertFalse((self.project / "leaked").exists())
        self.assertEqual(self.node.health()["used_cpus"], 0)
        self.assertEqual(self.node.health()["memory_reserved_mb"], 0)

    def test_target_node_selection_and_repeat_submission(self):
        other = self.root / "other"
        other.mkdir()
        with provider(self.root / "second", shared={"project": str(other)}) as (_, second):
            self.client.control(add={"name": "other", "url": second.url})
            identity = uuid.uuid4().hex
            code = "from pathlib import Path; p=Path('count'); p.write_text(p.read_text()+'1' if p.exists() else '1')"
            self.machine.python(code, wait=False, job_id=identity)
            self.machine.python(code, wait=False, job_id=identity)
            self.client.wait(identity)
            self.client.fetch(identity)
            self.assertEqual(self.machine.read_text("count"), "1")
            self.assertEqual(list(other.iterdir()), [])
        row = self.client.nodes()["nodes"][0]
        self.assertIn("machine", row["capabilities"])
        self.assertEqual(Path(row["shared_paths"]["project"]), self.project)

    def test_file_patterns_binary_unicode_and_failed_commands(self):
        binary = bytes(range(256)) * 8192
        manifest = self.machine.write_file("子目录/data.bin", binary)
        self.assertEqual(manifest["sha256"], hashlib.sha256(binary).hexdigest())
        self.machine.write_text("子目录/note.txt", "你好\n")
        downloaded = self.root / "filtered"
        self.machine.download("子目录", downloaded, patterns=["*.txt"])
        self.assertEqual(list(downloaded.iterdir()), [downloaded / "note.txt"])
        self.assertEqual((downloaded / "note.txt").read_text(encoding="utf-8"), "你好\n")
        with self.assertRaises(RemoteError):
            self.machine.read_file("missing")
        failed = self.machine.execute(["${tool:python}", "-c", "import sys; print('bad',file=sys.stderr); sys.exit(7)"])
        self.assertEqual(failed["status"], "failed")
        self.assertEqual(failed["steps"][0]["returncode"], 7)
        self.assertIn("bad", failed["stderr"])
        self.assertEqual(self.client.status(), [])
        with self.assertRaisesRegex(RemoteError, "cwd"):
            self.node.submit(command(["${tool:python}", "-c", "pass"], cwd="relative/path"))

    def test_unified_cli_commands_and_python_source_transfer(self):
        base = [sys.executable, "-B", str(ROOT / "main.py"), "--url", self.client.url]
        def run(*args):
            completed = subprocess.run([*base, *args], capture_output=True, timeout=15,
                                       env=dict(os.environ, PYTHONUTF8="1",
                                                REMOTE_COMPUTE_OPERATOR_TOKEN=self.state.operator_token))
            self.assertEqual(completed.returncode, 0, completed.stderr.decode("utf-8", "replace"))
            return completed.stdout.decode("utf-8")
        result = json.loads(run("exec", "--node", "device2", "--cwd", str(self.project), "--",
                                "${tool:python}", "-c", "print('argv works')"))
        self.assertEqual(result["stdout"].strip(), "argv works")
        source = self.root / "source.py"
        source.write_text("import sys; print('source',sys.argv[1])", encoding="utf-8")
        result = json.loads(run("python", "--node", "device2", "--cwd", str(self.project), str(source), "--", "with spaces"))
        self.assertEqual(result["stdout"].strip(), "source with spaces")
        payload = self.root / "本地.txt"
        payload.write_text("remote text", encoding="utf-8")
        run("put", "--node", "device2", "--cwd", str(self.project), str(payload), "远程.txt")
        self.assertEqual(run("read", "--node", "device2", "--cwd", str(self.project), "远程.txt").strip(), "remote text")
        target = self.root / "回收.txt"
        run("get", "--node", "device2", "--cwd", str(self.project), "远程.txt", str(target))
        self.assertEqual(target.read_text(encoding="utf-8"), "remote text")

    def test_console_commands_return_output_and_keep_accepting_input(self):
        script = self.root / "console.py"
        script.write_text("print('console output')", encoding="utf-8")
        shell = SchedulerShell(self.pool, self.client)
        try:
            with io.StringIO() as output, redirect_stdout(output):
                shell.onecmd("exec --help")
                shell.onecmd("")
                shell.onecmd(f'python --node device2 --cwd "{self.project}" "{script}"')
                shell.do_wait("")
                until(lambda: "console output" in output.getvalue())
                self.assertIn("console output", output.getvalue())
                shell.onecmd("nodes")
                script.write_text("import time; time.sleep(.5); print('detached output')", encoding="utf-8")
                shell.onecmd(f'python --node device2 --cwd "{self.project}" --detach "{script}"')
                identity = self.client.status()[0]["id"]
                shell.close()
                self.assertEqual(self.client.wait(identity)["status"], "succeeded")
                self.assertIn("detached output", self.client.fetch(identity)["stdout"])
        finally:
            shell.close()
        self.assertEqual(self.client.status(), [])


if __name__ == "__main__":
    unittest.main()
