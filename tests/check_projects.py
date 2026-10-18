"""Opt-in, recordless real-project soak. All run files belong to one temporary scope.

Run with the system Python: python -B tests/check_projects.py --seconds 300
The four checkouts and both workers must already be configured. Nothing is published.
"""

import argparse
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from contextlib import ExitStack
import hashlib
import io
import inspect
import json
import os
from pathlib import Path
import subprocess
import statistics
import sys
import tempfile
import threading
import time
import traceback
import zipfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from remote_compute import Client
from remote_compute.bundle import pack, json_bytes
from remote_compute.ramdisk import scratch

PROJECTS = {'AOP':'AOP_v2', 'RGCN':'RGCN-v2', 'ARH-DSE':'ARH-DSE-v2', 'Analog-Opt-Bench':'Analog-Opt-Bench-v2'}
PREFIX = 'check-3148/长标签·四工程真实联调/'
LIGHT = os.environ.get('DCO_CHECK_LIGHT') == '1'


def write(path, value):
    pending = path.with_suffix('.tmp')
    pending.write_text(json.dumps(value, ensure_ascii=False), encoding='utf-8')
    pending.replace(path)


class Trace:
    def __init__(self, directory):
        self.directory = directory
        self.lock = threading.RLock()
        self.active, self.jobs = {}, {}

    def wrap(self, module):
        original, trace = module.Client, self
        class TracedClient(original):
            def submit_bundle(self, identity, payload, **options):
                with zipfile.ZipFile(io.BytesIO(payload)) as archive:
                    files = {name:archive.read(name) for name in archive.namelist()}
                task = json.loads(files['task.json'])
                if LIGHT:
                    task['cpus'] = 1
                    task['env'].update(BENCH_ADAPTIVE_THREADS='0',BENCH_CPU_MAX='1')
                if not task['label'].startswith(PREFIX):
                    task['label'] = PREFIX + task['label'] + '/真实结果一致性·线程分配与运行退出自动清理验证-long-label-tail'
                files['task.json'] = json_bytes(task)
                with trace.lock:
                    trace.active[identity] = task['label']
                    write(trace.directory/'active.json', trace.active)
                return super().submit_bundle(identity, pack(files), **options)

            def fetch(self, identity, *args, **options):
                result = super().fetch(identity, *args, **options)
                with trace.lock:
                    trace.jobs[identity] = {key:result.get(key) for key in
                        ('status','provider','cpus','peak_cpus','peak_rss_mb','cpu_seconds','elapsed','total_elapsed')}
                return result

            def delete(self, identity):
                result = super().delete(identity)
                with trace.lock:
                    trace.active.pop(identity, None)
                    write(trace.directory/'active.json', trace.active)
                return result
        module.Client = TracedClient

    def cleanup(self, url):
        client = Client(url)
        for identity in list(self.active):
            try:
                client.cancel(identity)
                client.wait(identity, wait_timeout=45)
                client.fetch(identity)
            except Exception as error:
                if getattr(error, 'status_code', None) != 404:
                    raise


def started(directory):
    write(directory/'ready.json', {'ready':True})
    path = directory.parent/'start.json'
    while not path.exists():
        time.sleep(.05)
    return json.loads(path.read_text(encoding='utf-8'))['end']


def stream_until(end, execute, window):
    """Only ready cases enter the project adapter's bounded prefetch window."""
    completed, index = 0, 0
    with ThreadPoolExecutor(max_workers=1 if LIGHT else window.maximum) as executor:
        pending = set()
        while pending or time.time() < end:
            maximum = 1 if LIGHT else window.current()
            while len(pending) < maximum and time.time() < end:
                pending.add(executor.submit(execute, index))
                index += 1
            done, pending = wait(pending, timeout=.2, return_when=FIRST_COMPLETED)
            for future in done:
                future.result()
                completed += 1
    return completed


def aop(root, directory, trace, url):
    import numpy as np
    from application.configuration import runtime_config
    from analog.evaluation import EvaluationSession
    from analog.runtime import RuntimeWorkspace
    from compute import dispatch
    cfg = runtime_config(json.loads((root/'json/AO_cfg_BGR_ngspice.json').read_text(encoding='utf-8')))
    cfg.update(project_root=str(root), compute={'mode':'custom','center':{'url':url}}, resource_share=1.0)
    with RuntimeWorkspace({'mode':'disk','directory':str(directory)}, project_root=directory) as runtime:
        cfg['_runtime_root'] = str(runtime.root)
        with EvaluationSession(cfg, scratch_root=runtime.root/'cases', use_cache=False) as session:
            dispatch.configure(cfg)
            try:
                value=np.full(len(cfg['variables']),.5)
                pair=[]
                for _ in range(2):
                    result=session.evaluate(value)
                    assert result.ok, result.corners
                    pair.append(result.raw)
                assert np.array_equal(*pair,equal_nan=True), pair
                end=started(directory)
                window=Client(url).window('AOP',requires=['aop_python','ngspice'])
                def execute(index):
                    values=value.copy();values[index%len(values)] += ((index%7)-3)*.005
                    result=session.evaluate(values)
                    assert result.ok, result.corners
                count=stream_until(end,execute,window)
                return dict(cases=count,paired_values=len(pair[0]),paired_exact=True)
            finally:
                dispatch.shutdown()


def rgcn(root, directory, trace, url):
    import random
    from src.config import load_config
    from src.eda import resolve_tools
    from src.remote_native import PhysicalRunner, _sdk
    module,_ = _sdk(ROOT)
    trace.wrap(module)
    config=load_config(root/'configs/tsa.json')
    tools=resolve_tools(root)
    from src.simulation_storage import configure_process_scratch
    from src.native_request_identity import unresolved_designs
    from src.dataset_store import design_key
    configure_process_scratch()
    excluded=set(unresolved_designs(root).get(config.circuit.id,[]))
    with ExitStack() as stack:
        runners=[stack.enter_context(PhysicalRunner(root,tools,state_directory=directory/f'state-{i}')) for i in range(2)]
        def compare(index):
            candidate=config.circuit.default_candidate()
            rng=random.Random(42+index)
            for key,spec in config.circuit.parameters.items():
                if spec.step and spec.step<1:
                    slot=round((spec.default-spec.minimum)/spec.step)+rng.randint(-2,2)
                    candidate[key]=min(spec.maximum,max(spec.minimum,spec.minimum+slot*spec.step))
            assert design_key(candidate) not in excluded, 'Existing unresolved geometry is excluded'
            values=[]
            for offset,runner in enumerate(runners):
                mapping=config.to_mapping()
                mapping['resources'].update(workers=0,spice_threads=1)
                mapping['circuit']['pre']['remote_compute']=dict(url=url,client_path=str(ROOT),
                    cpus=1,memory_mb=512)
                values.append(runner.run(load_config(mapping),candidate))
                assert not values[-1].analyses['remote_execution']['reused'], 'Expected a real native task'
            assert values[0].measurements==values[1].measurements, (values[0].measurements,values[1].measurements)
            waves=[{name:v['numeric_sha256'] for name,v in r.analyses.get('waveforms',{}).items()} for r in values]
            assert waves[0]==waves[1], waves
            return len(values[0].measurements)
        size=compare(0)
        end=started(directory)
        window=Client(url).window('RGCN',requires=['rgcn_python','ngspice'])
        count=stream_until(end,lambda i:compare(i+1),window)
        return dict(pairs=count+1,paired_values=size,paired_exact=True)


def bench(root, directory, trace, url):
    import numpy as np
    from config import CONFIG
    from copy import deepcopy
    from bench.remote import run_tasks,select_backend
    config=deepcopy(CONFIG)
    config.update(budget=256,population=32,output_size=32,kernel_workers=0,runtime_workers=0,
        compute_mode='scheduler',analysis={'enabled':False,'population_snapshots':1,'history_points':64},
        remote_compute={'url':url,'task_cpus':1,'memory_mb':1024},resource_share=1.0)
    settings=select_backend(config)
    pair=[]
    for _ in range(2):
        pair.append(next(iter(run_tasks([('class-ab','nsga2',98117,config)],dict(settings),0)))[1])
    fields=('X','F','G','metric_values','physical_X')
    for field in fields:
        assert np.array_equal(pair[0][field],pair[1][field],equal_nan=True),field
    count=0
    end=started(directory)
    while time.time()<end:
        config.update(budget=512 if LIGHT else 4096)
        tasks=[('class-ab','FACE-MO',98118+count+i,config) for i in range(1 if LIGHT else 16)]
        for _,run in run_tasks(tasks,settings,0):
            assert run['evaluations']>0
            count+=1
    return dict(seeds=count,paired_rows=len(pair[0]['X']),paired_exact=True)


def arh(root, directory, trace, url):
    from framework.remote_execution import run_remote
    from framework.registry import get_project
    plugin=get_project('mac')
    config=plugin.validate(dict(algorithm='manual',manual_design=7,seed=71932,
        trials=32,pilot_trials=32,validation_candidates=2,validation_breadth='fixed',search_budget=9,density=.002,
        fault_environment='steady',radiation_range_mode='manual',aggregate_cache_mode='fresh',history_mode='off',
        budget_mode='fixed',memory_budget_mib=1536,resource_share=1.0))
    results=[]
    for i in range(2):
        results.append(run_remote(root,'mac',config,url,artifact_root=directory/f'pair-{i}',transient=True))
    def scientific(value):
        return sorted([dict(row,metrics={key:v for key,v in row['metrics'].items()
                                        if key not in ('evaluation_s','cache_hit')})
                       for row in value['candidates']],key=lambda row:(row['design_id'],row['stage']))
    assert scientific(results[0])==scientific(results[1]), 'ARH numerical result mismatch'
    end=started(directory)
    count=0
    while time.time()<end:
        result=run_remote(root,'mac',dict(config,seed=98118+count),url,
            artifact_root=directory/f'round-{count}',transient=True)
        assert result
        count+=1
    return dict(rounds=count,paired_rows=len(results[0]['candidates']),paired_exact=True)


def project(args):
    directory=Path(args.scratch)
    root=ROOT.parent/PROJECTS[args.project]
    os.chdir(root);sys.path.insert(0,str(root))
    for key in ('TEMP','TMP','TMPDIR','NUMBA_CACHE_DIR','MPLCONFIGDIR'):
        os.environ[key]=str(directory)
    tempfile.tempdir=str(directory)
    import remote_compute
    trace=Trace(directory);trace.wrap(remote_compute)
    result={}
    try:
        result=globals()[{'AOP':'aop','RGCN':'rgcn','ARH-DSE':'arh','Analog-Opt-Bench':'bench'}[args.project]](root,directory,trace,args.url)
        result['passed']=True
    except BaseException:
        result=dict(passed=False,error=traceback.format_exc())
        raise
    finally:
        try:
            trace.cleanup(args.url)
        finally:
            result['jobs']=trace.jobs
            write(directory/'result.json',result)


def protected_reports():
    files=set()
    for name,folder in PROJECTS.items():
        root=ROOT.parent/folder
        patterns=('exports/**/*.html',) if name=='AOP' else ('runs/**/*.html',) if name=='RGCN' else (
            'results/projects/*/report.html','results/projects/*/summary.json','docs/report/**/*.html') if name=='ARH-DSE' else (
            'results/report.html','results/scientific-data/experiments.json','reference/*.npz')
        for pattern in patterns:
            files.update(p for p in root.glob(pattern) if p.is_file())
    return {str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in files}


def console_snapshot(pid):
    import ctypes as c
    from ctypes import wintypes as w
    import re
    class Coord(c.Structure): _fields_=[('X',w.SHORT),('Y',w.SHORT)]
    class Rect(c.Structure): _fields_=[('left',w.SHORT),('top',w.SHORT),('right',w.SHORT),('bottom',w.SHORT)]
    class Info(c.Structure): _fields_=[('size',Coord),('cursor',Coord),('attrs',w.WORD),('window',Rect),('maximum',Coord)]
    class Char(c.Union): _fields_=[('unicode',w.WCHAR),('ascii',c.c_char)]
    class Cell(c.Structure): _fields_=[('char',Char),('attrs',w.WORD)]
    kernel=c.WinDLL('kernel32',use_last_error=True)
    kernel.CreateFileW.argtypes=[w.LPCWSTR,w.DWORD,w.DWORD,c.c_void_p,w.DWORD,w.DWORD,w.HANDLE]
    kernel.CreateFileW.restype=w.HANDLE
    kernel.GetConsoleScreenBufferInfo.argtypes=[w.HANDLE,c.POINTER(Info)]
    kernel.ReadConsoleOutputW.argtypes=[w.HANDLE,c.POINTER(Cell),Coord,Coord,c.POINTER(Rect)]
    kernel.CloseHandle.argtypes=[w.HANDLE]
    kernel.FreeConsole()
    assert kernel.AttachConsole(pid),c.WinError(c.get_last_error())
    handle=kernel.CreateFileW('CONOUT$',0x80000000,3,None,3,0,None)
    try:
        info=Info();assert kernel.GetConsoleScreenBufferInfo(handle,c.byref(info))
        width=info.window.right-info.window.left+1;height=info.window.bottom-info.window.top+1
        cells=(Cell*(width*height))();rect=info.window
        assert kernel.ReadConsoleOutputW(handle,cells,Coord(width,height),Coord(0,0),c.byref(rect))
        ids=[];errors=[];panes=False;long_rows=[]
        for y in range(height):
            text=''.join(cells[y*width+x].char.unicode or ' ' for x in range(width) if not cells[y*width+x].attrs&0x200)
            if '设备资源' in text:panes=True
            if '最近通知' in text or '命令输出' in text:panes=False
            if panes and width>=120 and text.startswith(('+','|')):
                for x in (0,55,width-2):
                    if cells[y*width+x].char.unicode not in ('+','|'):errors.append([y,x])
                if cells[y*width+width-1].char.unicode not in ('',' '):errors.append([y,width-1])
                ids.extend(re.findall(r'\| ([0-9a-f]{4}) \|',text))
                if '...' in text:long_rows.append(text.rstrip())
        return dict(size=[width,height],visible_ids=ids,alignment_errors=errors,long_rows=long_rows[:2])
    finally:
        kernel.CloseHandle(handle);kernel.FreeConsole()


def observations(samples, end):
    busy=[s for s in samples if end-270<=s['time']<=end]
    result=dict(samples=len(samples),peak_threads=max(s['used'] for s in samples),
                mean_threads=round(statistics.mean(s['used'] for s in busy),2),
                peak_queue=max(s['queued'] for s in samples),nodes={})
    for index,node in enumerate(samples[0]['nodes']):
        values=[s['nodes'][index] for s in samples]
        late=[s['nodes'][index]['service']['private_mb'] for s in busy[-15:]]
        early=[s['nodes'][index]['service']['private_mb'] for s in busy[:15]]
        result['nodes'][node['name']]=dict(
            peak_threads=max(v['reserved'] for v in values),
            mean_cpu=round(statistics.mean(s['nodes'][index]['cpu_percent'] or 0 for s in busy),2),
            private_mb_start=values[0]['service']['private_mb'],private_mb_peak=max(v['service']['private_mb'] for v in values),
            private_mb_end=values[-1]['service']['private_mb'],
            steady_private_change_mb=round(statistics.mean(late)-statistics.mean(early),2),
            handles_start=values[0]['service']['handles'],handles_peak=max(v['service']['handles'] for v in values),
            handles_end=values[-1]['service']['handles'],threads_end=values[-1]['service']['threads'],
            scratch_peak_bytes=max(v['scratch_disk']['used'] for v in values),scratch_end_bytes=values[-1]['scratch_disk']['used'],
            assets_end=values[-1]['asset_cache'])
    result['projects']={name:dict(mean_used=round(statistics.mean(s['projects'].get(name,{}).get('used',0) for s in busy),2),
        mean_target=round(statistics.mean(s['projects'].get(name,{}).get('target',0) for s in busy),2),
        max_queue=max(s['projects'].get(name,{}).get('queued',0) for s in busy)) for name in PROJECTS}
    return result


def main(args):
    before=protected_reports()
    client=Client(args.url)
    summary={}
    with tempfile.TemporaryDirectory(prefix='projects-',dir=scratch('Remote-Compute-verify')) as temporary:
        base=Path(temporary)
        processes=[]
        try:
            with ExitStack() as stack:
                for name in PROJECTS:
                    directory=base/name;directory.mkdir()
                    output=stack.enter_context((directory/'console.log').open('wb'))
                    process=subprocess.Popen([sys.executable,'-B',str(Path(__file__).resolve()),
                        '--project',name,'--scratch',str(directory),'--url',args.url],stdout=output,stderr=subprocess.STDOUT,
                        env=dict(os.environ,PYTHONUTF8='1',PYTHONDONTWRITEBYTECODE='1',OPENBLAS_CORETYPE='Haswell',
                                 OPENBLAS_NUM_THREADS='1',OMP_NUM_THREADS='1',MKL_NUM_THREADS='1'))
                    processes.append((name,process))
                deadline=time.monotonic()+300
                while not all((base/name/'ready.json').exists() for name in PROJECTS):
                    for name,p in processes:
                        if p.poll() is not None:
                            raise RuntimeError(name+': '+(base/name/'console.log').read_text(encoding='utf-8',errors='replace')[-5000:])
                    if time.monotonic()>deadline:
                        raise TimeoutError('Project paired preflight exceeded five minutes')
                    time.sleep(.2)
                end=time.time()+args.seconds
                write(base/'start.json',dict(end=end))
                samples=[];last_output=0;checked_console=False
                while any(p.poll() is None for _,p in processes):
                    health=client.health()
                    samples.append(dict(time=time.time(),used=health['used_cpus'],queued=health['queued'],
                        projects=health['projects'],nodes=[{k:n.get(k) for k in
                        ('name','reserved','capacity','cpu_percent','memory_reserved_mb','rss_mb','asset_cache','scratch_disk','service')}
                        for n in health['nodes']]))
                    assert all(n['reserved']<=n['capacity'] for n in health['nodes'])
                    assert sum(v['target'] for v in health['projects'].values())==min(health['cpus'],sum(v['demand'] for v in health['projects'].values()))
                    if not checked_console and time.time()>end-args.seconds/2 and os.name=='nt':
                        local=console_snapshot(health['pid'])
                        remote=client.machine('Aya1-TX',cwd=next(n['tool_root'] for n in health['nodes'] if n['name']=='Aya1-TX')).python(inspect.getsource(console_snapshot)+
                            '\nimport json\nfrom remote_compute import Client\nprint(json.dumps(console_snapshot(Client("http://127.0.0.1:26133").health()["pid"])))',
                            label=PREFIX+'CLI',priority=100,timeout=30,memory_mb=128)
                        distant=(json.loads(remote['stdout']) if remote['status']=='succeeded'
                                 else dict(error=remote.get('stderr','')[-2000:]))
                        for view in (local,distant):
                            if view.get('alignment_errors'):view['error']='Console borders are misaligned'
                        print('NATIVE_CONSOLES',json.dumps(dict(local=local,remote=distant),ensure_ascii=False),flush=True)
                        summary['consoles']=dict(local=local,remote=distant)
                        checked_console=True
                    if time.monotonic()-last_output>20:
                        print(json.dumps(dict(remaining=max(0,round(end-time.time())),**samples[-1]),ensure_ascii=False),flush=True)
                        last_output=time.monotonic()
                    if time.time()>end+120:
                        raise TimeoutError('Project work did not drain')
                    time.sleep(2)
                for _ in range(10):
                    health=client.health()
                    samples.append(dict(time=time.time(),used=health['used_cpus'],queued=health['queued'],projects=health['projects'],
                        nodes=[{k:n.get(k) for k in ('name','reserved','capacity','cpu_percent','asset_cache','scratch_disk','service')} for n in health['nodes']]))
                    time.sleep(2)
                for name,p in processes:
                    summary[name]=json.loads((base/name/'result.json').read_text(encoding='utf-8'))
                    summary[name]['exit_code']=p.returncode
                summary['observations']=observations(samples,end)
        finally:
            # Cancel only IDs created by this check, then let each Python child
            # and the worker's own normal lifecycle release its private files.
            for name,p in processes:
                path=base/name/'active.json'
                for identity in json.loads(path.read_text(encoding='utf-8')) if path.exists() else []:
                    try:
                        client.cancel(identity);client.wait(identity,wait_timeout=45);client.fetch(identity)
                    except Exception as error:
                        if getattr(error,'status_code',None)!=404:print('cleanup',identity,str(error),flush=True)
                if p.poll() is None:
                    p.terminate();p.wait(15)
                p.wait()
        summary['protected_reports_unchanged']=protected_reports()==before
        for name in PROJECTS:
            jobs=summary[name].pop('jobs')
            summary[name]['job_count']=len(jobs)
            summary[name]['provider']='DCO'
            summary[name]['peak_task_cpus']=max(j.get('peak_cpus') or j.get('cpus') or 0 for j in jobs.values())
        print('PROJECT_RESULTS',json.dumps(summary,ensure_ascii=False),flush=True)
    print('OWNED_SCRATCH_REMOVED',not Path(temporary).exists(),'PROTECTED_REPORTS',len(before),flush=True)
    assert all(summary[name]['passed'] for name in PROJECTS),'See PROJECT_RESULTS for project errors'
    assert summary['protected_reports_unchanged'],'Reports or reference files changed during the observation'
    assert not any(view.get('error') for view in summary.get('consoles',{}).values()),'See NATIVE_CONSOLES'


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--seconds',type=int,default=300)
    parser.add_argument('--url',default='http://127.0.0.1:26133')
    parser.add_argument('--project',choices=PROJECTS)
    parser.add_argument('--scratch')
    parser.add_argument('--light',action='store_true',help='at most one 1-CPU task per project')
    args=parser.parse_args()
    if args.light:
        os.environ['DCO_CHECK_LIGHT']='1'
        LIGHT=True
    project(args) if args.project else main(args)
