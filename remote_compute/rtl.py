"""Portable Verilator invocation, including the existing native MSYS2 toolchain.

The worker runs this helper as an owned child. No project imports or global
toolchain changes are needed; normal Verilator arguments pass through.
"""

import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import sys


def msys_workspace(path):
    """Allow the installed low-integrity MSYS binaries to write this job's files."""
    if __package__:
        from .process import low_integrity_directory
    else:
        from process import low_integrity_directory
    return low_integrity_directory(Path(path).resolve())


def main(arguments=None):
    arguments = list(sys.argv[1:] if arguments is None else arguments)
    if os.name != "nt":
        return subprocess.call([os.environ.get("VERILATOR", "verilator"), *arguments])
    configured = os.environ.get("MSYS2_ROOT")
    bash = str(Path(configured) / "usr/bin/bash.exe") if configured else shutil.which("bash.exe")
    if not bash or not Path(bash).is_file():
        raise RuntimeError("Set MSYS2_ROOT to the existing MSYS2 installation")
    environment = dict(os.environ, MSYSTEM="UCRT64", CHERE_INVOKING="1", MSYS2_PATH_TYPE="inherit",
                       VERILATOR_ROOT="/ucrt64/share/verilator")
    ucrt = Path(bash).resolve().parents[2] / "ucrt64/bin"
    environment["PATH"] = str(ucrt) + os.pathsep + environment.get("PATH", "")
    def run(argv):
        return subprocess.call([bash, "-lc", ' '.join(shlex.quote(arg) for arg in argv)], env=environment)
    binary = "--binary" in arguments
    build = "--build" in arguments and ("--cc" in arguments or "--sc" in arguments)
    if not binary and not build:
        return run(["verilator", *arguments])
    # Scope the label to the private job workspace and job temporary directory.
    owned = os.environ.get("REMOTE_COMPUTE_JOB_DIR")
    if owned:
        directory_root = Path(owned).resolve()
        try:
            Path.cwd().resolve().relative_to(directory_root)
        except ValueError:
            raise ValueError("RTL workspace must be inside its job directory") from None
        msys_workspace(directory_root)
    def option(name, default):
        return arguments[arguments.index(name) + 1] if name in arguments else default
    top = option("--top-module", "tb")
    directory = Path(option("--Mdir", "obj_dir"))
    directory.mkdir(parents=True, exist_ok=True)
    cpus = option("-j", "1")
    generated = [x for x in arguments if x not in {"--binary", "--build"}]
    if binary:
        generated += ["--cc", "--main", "--exe", "--timing", "-CFLAGS", "-DVL_TIME_CONTEXT"]
    if not any('-std=' in value for value in arguments):
        generated += ["-CFLAGS", "-std=c++17"]
    generated += ["-LDFLAGS", "-static-libgcc -static-libstdc++ -static"]
    if "-o" not in generated:
        generated += ["-o", "V" + top + ".exe"]
    code = run(["verilator", *generated])
    if code:
        return code
    makefile = directory / (option("--prefix", "V" + top) + ".mk")
    text = makefile.read_text(encoding="utf-8")
    makefile.write_text(re.sub(r"(?m)^VERILATOR_ROOT\s*=.*$", "VERILATOR_ROOT = /ucrt64/share/verilator", text), encoding="utf-8")
    return run(["make", "-C", directory.resolve().as_posix(), "-f", makefile.name, "-j", cpus])


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, RuntimeError) as error:
        print(str(error), file=sys.stderr)
        raise SystemExit(2)
