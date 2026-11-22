"""Task constructors; callers keep their measurement and scientific semantics."""
from .policy import canonical_capabilities, validate_platform


def python_map(function, items, *, kwargs=None, backend='process', item_max_cpus=1, **options):
    """Submit independent business inputs to the DCO-owned process executor."""
    task=command(['${tool:python}','-B','${dco}/remote_compute/execution.py','${work}/.dco-execution.json'],
                 **options)
    if type(item_max_cpus)is not int or item_max_cpus<1:raise ValueError('item_max_cpus requires a positive integer')
    if item_max_cpus>1 and backend!='process':raise ValueError('Multicore items require the process backend')
    if item_max_cpus>1:task['env']['REMOTE_COMPUTE_ITEM_MAX_CPUS']=str(item_max_cpus)
    task['execution']=dict(function=function,items=list(items),kwargs=dict(kwargs or {}),mode='map',backend=backend)
    task['outputs']=list(dict.fromkeys([*task['outputs'],'.dco-results.json','.dco-streams/**/*']))
    return task


def python_call(function, *, kwargs=None, **options):
    """Run one native or numerical kernel with its platform-assigned threads."""
    task=python_map(function,[],kwargs=kwargs,**options)
    task['execution']['mode']='call'
    return task


def python_task(function, *, kwargs=None, **options):
    """Run a serial business entry whose parallel stages use DCO map_items."""
    task=python_map(function,[],kwargs=kwargs,**options)
    task['execution']['mode']='entry'
    return task


def command(argv, *, max_cpus=1024, min_start_cpus=None, timeout=300, outputs=(), env=None, assets=None, render=(), label="",
            priority=0, requires=(), node=None, memory_mb=None, base_memory_mb=None,memory_per_cpu_mb=None,memory_limit_mb=None,cwd=None,stdin=None,
            project='', resource_share=1.0, single_thread_weight=None, retries=2,
            accelerator=None, platform=None, cache_ref=None, native_cache=None, resource_recovery=True,
            artifact_ref=None, produces_artifact_ref=None, checkpoint_ref=None, checkpoint_output=None,
            affinity_keys=None, timeout_grace_s=None, step_phases=None, short_job=False,
            executable_ram_mb=None):
    # Older SDK callers can still pass the retired keyword.
    if type(resource_recovery) is not bool:raise ValueError("resource_recovery must be a boolean")
    platform = validate_platform(platform)
    if min_start_cpus is not None:
        from .allocation import validate_min_start_cpus
        validate_min_start_cpus(min_start_cpus, max_cpus)
    model=base_memory_mb is not None or memory_per_cpu_mb is not None or memory_limit_mb is not None
    if model:
        if memory_mb is not None:raise ValueError('Declare fixed memory_mb or a base/per-CPU memory model')
        from .allocation import plan_resize
        base=0 if base_memory_mb is None else base_memory_mb
        per=0 if memory_per_cpu_mb is None else memory_per_cpu_mb
        startup_width = min_start_cpus or 1
        width,memory_mb=plan_resize(startup_width,None,base,per,memory_limit_mb,base+per*startup_width)
        if width!=startup_width or memory_mb is None or memory_limit_mb is not None and memory_mb>memory_limit_mb:
            raise ValueError('Memory model must fit the startup CPU width')
    else:memory_mb=256 if memory_mb is None else memory_mb
    task = {"steps": [list(argv)], "max_cpus": max_cpus, "timeout": timeout,
            "outputs": list(outputs), "env": dict(env or {}), "assets": dict(assets or {}),
            "render": list(render), "label": label, "priority": priority,
            "requires": canonical_capabilities(requires), "node": node, "memory_mb": memory_mb,
            "project": project, "resource_share": resource_share}
    if min_start_cpus is not None:
        task['min_start_cpus'] = min_start_cpus
    if executable_ram_mb is not None:
        from .ramdisk import validate_executable_ram_mb
        task['executable_ram_mb'] = validate_executable_ram_mb(executable_ram_mb)
    if model:task['memory_model']=dict(base_memory_mb=base,memory_per_cpu_mb=per,**({'memory_limit_mb':memory_limit_mb}if memory_limit_mb is not None else{}))
    if accelerator is not None:
        task["accelerator"] = accelerator
    if platform is not None:
        task['platform'] = platform
    if cache_ref is not None:
        from .cache_refs import reference_id
        task['cache_ref']=reference_id(cache_ref)
    if native_cache is not None:
        from .runtimes import validate_native_cache
        task['native_cache'] = validate_native_cache(native_cache)
    if cwd is not None:
        task["cwd"] = str(cwd)
    if stdin is not None:
        task["stdin"] = str(stdin)
    if retries!=2:task['retries']=retries
    if not resource_recovery:task['resource_recovery']=False
    task.update({key: value for key, value in (
        ('artifact_ref', artifact_ref), ('produces_artifact_ref', produces_artifact_ref),
        ('checkpoint_ref', checkpoint_ref), ('checkpoint_output', checkpoint_output),
        ('affinity_keys', affinity_keys), ('timeout_grace_s', timeout_grace_s), ('step_phases', step_phases),
    ) if value is not None})
    if short_job:
        task['short_job'] = short_job
    return task


def ngspice(deck="circuit.spice", *, max_cpus=1024, timeout=120, assets=None, render=(), env=None):
    return command(["${tool:ngspice}", "-b", "-o", "ngspice.log", deck],
                   max_cpus=max_cpus, timeout=timeout, outputs=["*.log", "*.raw", "*.csv", "*.dat"],
                   requires=["ngspice"], assets=assets, render=render, env=env, label="ngspice")


def verilator(sources, top, *, max_cpus=1024, timeout=600, flags=(), run_args=(), outputs=("metrics.json",)):
    """Compile a SystemVerilog testbench, then execute it in the same job."""
    task = command(["${tool:verilator}", "--binary", "--assert", "-j", "${cpus}",
                    "--Mdir", "obj_dir", "--top-module", top, *flags, *sources],
                   max_cpus=max_cpus, timeout=timeout, outputs=outputs, requires=["rtl"], label="verilator")
    task["steps"].append(["${work}/obj_dir/V" + top + "${exe}", *run_args])
    return task
