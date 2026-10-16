"""Task constructors; callers keep their measurement and scientific semantics."""


def command(argv, *, cpus=1, timeout=300, outputs=(), env=None, assets=None, render=(), label="",
            priority=0, requires=(), node=None):
    return {"steps": [list(argv)], "cpus": cpus, "timeout": timeout,
            "outputs": list(outputs), "env": dict(env or {}), "assets": dict(assets or {}),
            "render": list(render), "label": label, "priority": priority,
            "requires": list(requires), "node": node}


def ngspice(deck="circuit.spice", *, cpus=1, timeout=120, assets=None, render=(), env=None):
    return command(["${tool:ngspice}", "-b", "-o", "ngspice.log", deck],
                   cpus=cpus, timeout=timeout, outputs=["*.log", "*.raw", "*.csv", "*.dat"],
                   assets=assets, render=render, env=env, label="ngspice")


def verilator(sources, top, *, cpus=1, timeout=600, flags=(), run_args=(), outputs=("metrics.json",)):
    """Compile a SystemVerilog testbench, then execute it in the same job."""
    task = command(["${tool:verilator}", "--binary", "--assert", "-j", "${cpus}",
                    "--Mdir", "obj_dir", "--top-module", top, *flags, *sources],
                   cpus=cpus, timeout=timeout, outputs=outputs, label="verilator")
    task["steps"].append(["${work}/obj_dir/V" + top + "${exe}", *run_args])
    return task
