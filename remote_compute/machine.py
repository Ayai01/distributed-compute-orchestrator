"""Host commands and files, transported as ordinary resource-accounted jobs."""

import json
from pathlib import Path

from .bundle import tree, unpack
from .client import RemoteError
from .tasks import command

_HELPER = Path(__file__).with_name("machine_files.py")
_PREFIX = "remote_op"


class Machine:
    def __init__(self, client, node, *, cwd="~"):
        if not isinstance(node, str) or not node:
            raise ValueError("Choose a worker node name for machine operations")
        self.client, self.node, self.cwd = client, node, str(cwd)

    def restart(self, *, wait=False, wait_timeout=None):
        """Reload this worker's code and configuration after its jobs finish."""
        return self.client.restart(self.node,wait=wait,wait_timeout=wait_timeout)

    def execute(self, argv, *, cwd=None, stdin=None, files=None, wait=True, job_id=None,
                output_dir=None, keep=False, on_submit=None, **options):
        """Run an argv on the host; wait=False returns a persistent job id."""
        if isinstance(argv, (str, bytes)):
            raise ValueError("argv requires a list; use python() for a script")
        files = dict(files or {})
        if stdin is not None:
            files[_PREFIX + "/stdin"] = stdin
            options["stdin"] = _PREFIX + "/stdin"
        options.setdefault("label", "machine/exec " + str(argv[0]))
        if getattr(self.client,'_capabilities',None) is None:self.client.health(brief=True)
        if 'retry_jobs' in self.client._capabilities:options.setdefault('retries',0)
        task = command(argv, node=self.node, cwd=self.cwd if cwd is None else cwd, **options)
        accepted = self.client.submit(task, files=files, job_id=job_id)
        identity = accepted["id"]
        if on_submit:
            on_submit(self.client, identity)
        if not wait:
            return {**accepted, "url": self.client.url}
        try:
            self.client.wait(identity)
        except KeyboardInterrupt:
            self.client.cancel(identity)
            raise
        return self.client.fetch(identity, output_dir, delete=not keep)

    def python(self, code=None, *, script=None, args=(), tool="python", files=None, **options):
        """Send Python source or a local script, using the worker's interpreter."""
        if (code is None) == (script is None):
            raise ValueError("Provide either code or a local script")
        files = dict(files or {})
        files[_PREFIX + "/script.py"] = Path(script) if script is not None else code
        loader = ("import os,runpy,sys; sys.path.insert(0,os.getcwd()); sys.argv=sys.argv[1:]; "
                  "runpy.run_path(sys.argv[0],run_name='__main__')")
        return self.execute(["${tool:" + tool + "}", "-u", "-c", loader, "${work}/" + _PREFIX + "/script.py", *args],
                            files=files, **options)

    def _file(self, action, path, *, files=None, patterns=("**/*",)):
        files = dict(files or {})
        files[_PREFIX + "/files.py"] = _HELPER
        result = self.execute(["${tool:python}", "${work}/" + _PREFIX + "/files.py", action, str(path),
                               "--work", "${work}/" + _PREFIX, "--patterns", json.dumps(list(patterns))],
                              files=files, outputs=[_PREFIX + "/result/*"], label="machine/" + action + ' ' + str(path))
        if result["status"] != "succeeded":
            error = RemoteError(result.get("stderr") or result.get("error") or "Remote file operation failed")
            error.result = result
            raise error
        return result["files"]

    def listdir(self, path="."):
        return json.loads(self._file("list", path)[_PREFIX + "/result/manifest.json"])

    def read_file(self, path):
        return self._file("read", path)[_PREFIX + "/result/data"]

    def read_text(self, path, *, encoding="utf-8"):
        return self.read_file(path).decode(encoding)

    def write_file(self, path, data):
        files = self._file("write", path, files={_PREFIX + "/data": data})
        return json.loads(files[_PREFIX + "/result/manifest.json"])

    def write_text(self, path, text, *, encoding="utf-8"):
        return self.write_file(path, text.encode(encoding))

    def upload(self, source, destination, *, patterns=("**/*",)):
        """Copy a file or merge selected directory files; existing files are replaced atomically."""
        source = Path(source).resolve()
        if source.is_file():
            return self.write_file(destination, source)
        if not source.is_dir():
            raise FileNotFoundError(source)
        files = {_PREFIX + "/upload/" + name: path for name, path in tree(source, patterns).items()}
        result = self._file("upload", destination, files=files)
        return json.loads(result[_PREFIX + "/result/manifest.json"])

    def download(self, source, destination, *, patterns=("**/*",)):
        files = self._file("download", source, patterns=patterns)
        manifest = json.loads(files[_PREFIX + "/result/manifest.json"])
        destination = Path(destination).resolve()
        if manifest["kind"] == "file":
            if destination.is_dir():
                destination /= manifest["name"]
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(files[_PREFIX + "/result/data"])
        else:
            unpack(files[_PREFIX + "/result/transfer.zip"], destination)
            destination.mkdir(parents=True, exist_ok=True)
        return {**manifest, "destination": str(destination)}
