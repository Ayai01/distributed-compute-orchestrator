"""Approximate native mixed throughput using the installed NumPy/BLAS runtime."""
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time

NAME = 'native-mixed-v4'
BATCH = 32768


class MixedLoad:
    def __init__(self, np):
        self.np = np
        self.volts = np.full(BATCH,0.05)
        self.rhs = np.linspace(0.2,0.3,BATCH)
        self.exponent, self.delta = np.empty(BATCH),np.empty(BATCH)
        self.words = np.arange(BATCH,dtype=np.uint32)+np.uint32(0x12345678)
        self.ram = np.arange(BATCH,dtype=np.uint32)*np.uint32(2654435761)
        self.indices, self.data, self.rotate = (np.empty(BATCH,dtype=np.uint32) for _ in range(3))
        # A native block must outlast Python dispatch even on large CPU budgets.
        self.matrix = np.linspace(-0.2,0.2,512*512).reshape(512,512)
        self.weight = np.full((512,512),0.0001)+np.eye(512)*0.98
        self.product = np.empty_like(self.matrix)

    def block(self):
        np = self.np
        # Bulk nonlinear FP64 updates: no Python loop over circuit nodes.
        for _ in range(3):
            np.divide(self.volts,0.026,out=self.exponent)
            np.exp(self.exponent,out=self.exponent)
            self.exponent *= 0.001
            np.multiply(self.volts,4,out=self.delta)
            self.delta += self.exponent
            self.delta -= 0.001
            self.delta -= self.rhs
            self.exponent /= 0.026
            self.exponent += 4
            self.delta /= self.exponent
            self.volts -= self.delta
        # Fixed-width datapath plus indexed gather/scatter, in native ufuncs.
        np.right_shift(self.words,9,out=self.indices)
        self.indices &= BATCH-1
        np.take(self.ram,self.indices,out=self.data)
        np.left_shift(self.words,5,out=self.rotate)
        self.words >>= 27
        self.words |= self.rotate
        self.words += self.data
        self.words *= np.uint32(1664525)
        self.words += np.uint32(1013904223)
        self.data >>= 1
        self.words ^= self.data
        self.ram[self.indices] = self.words
        # The actual installed BLAS supplies scientific FP64/FMA performance.
        np.matmul(self.matrix,self.weight,out=self.product)
        self.product += 0.0001
        self.matrix,self.product = self.product,self.matrix


def _sample(np, cpus, began, duration=60):
    # Import, allocation and thread warmup are outside the measured intervals.
    serial=MixedLoad(np);serial.block()
    # Keep the two measurements independent and equally long.  ``duration``
    # is the per-mode budget, so the full calibration takes 2*duration.
    started=time.monotonic();deadline=started+duration
    count=0
    while time.monotonic()<deadline:
        serial.block();count+=BATCH
    seconds=time.monotonic()-started
    single=dict(name=NAME,available=True,score=round(count/seconds/1000000,6),
                unit='Miter/s',threads=1,seconds=round(seconds,3),iterations=count)
    bulk_started=deadline=0.0
    def start():
        nonlocal bulk_started,deadline
        bulk_started=time.monotonic();deadline=bulk_started+duration
    ready=threading.Barrier(cpus+1,action=start)
    def lane():
        try:
            load=MixedLoad(np);load.block();ready.wait();count=0
            while time.monotonic()<deadline:
                load.block();count+=BATCH
            return count,time.monotonic()
        except BaseException:
            ready.abort();raise
    with ThreadPoolExecutor(max_workers=cpus,thread_name_prefix='native-benchmark') as executor:
        try:
            futures=[executor.submit(lane) for _ in range(cpus)]
            ready.wait();samples=[future.result() for future in futures]
        finally:ready.abort()
    # The barrier action starts all lanes from one clock edge.  Use the last
    # lane's completion as the denominator so lane startup and teardown never
    # inflate the reported total-thread throughput.
    finished=max(finished for _,finished in samples)
    seconds=max(1e-9,finished-bulk_started)
    count=sum(count for count,_ in samples)
    return dict(name=NAME,available=True,score=round(count/seconds/1000000,6),unit='Miter/s',
                threads=cpus,seconds=round(seconds,3),total_seconds=round(time.monotonic()-began,3),
                iterations=count,numpy=np.__version__,single=single,measured_at=time.time(),
                components=['native-fp64-newton','native-u32-ram','native-blas-matmul'])


def _unavailable(reason):
    return dict(name=NAME,available=False,reason=reason,unit='Miter/s',threads=0)


def measure(cpus, *, executable=None, environment=(), duration=60):
    """One disposable native helper; the service itself stays stdlib-only."""
    began = time.monotonic()
    env = dict(os.environ, **dict(environment))
    for name in ('OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','OMP_NUM_THREADS',
                 'BLIS_NUM_THREADS','VECLIB_MAXIMUM_THREADS'):
        env[name] = '1'  # Outer lanes share the worker budget; no nested BLAS pool.
    with subprocess.Popen([executable or sys.executable,'-I','-B','-u',str(Path(__file__).resolve()),
                           '--sample',str(cpus),str(began),str(duration)],
            env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,encoding='utf-8',
            creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0)) as helper:
        try:
            stdout,stderr = helper.communicate(timeout=duration*2+15)
        except subprocess.TimeoutExpired:
            helper.kill(); helper.communicate()
            return _unavailable('Native estimate exceeded startup budget')
        except BaseException:
            helper.kill(); helper.communicate()
            raise
        if helper.returncode:
            return _unavailable(stderr.strip()[-240:] or 'Native helper exit '+str(helper.returncode))
        result=json.loads(stdout)
        result['python']=executable or sys.executable
        return result


def cpu_identity(cpus):
    import platform
    if os.name=='nt':
        import winreg
        with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE,r'HARDWARE\DESCRIPTION\System\CentralProcessor\0') as key:
            model=winreg.QueryValueEx(key,'ProcessorNameString')[0].strip()
    else:
        model=sorted({line.split(':',1)[1].strip() for line in Path('/proc/cpuinfo').read_text().splitlines() if line.startswith('model name')})
    return dict(kernel=NAME,model=model,architecture=platform.machine(),logical_cpus=os.cpu_count(),
                affinity=sorted(os.sched_getaffinity(0)) if hasattr(os,'sched_getaffinity') else None,threads=cpus)


def calibrated(cpus, *, executable=None, environment=(), cache_file=None, force=False):
    from .bundle import atomic_json
    from .resources import cpu_ticks
    cache=Path(cache_file) if cache_file else Path(os.environ.get('DCO_BENCHMARK_DIR',str(Path(__file__).resolve().parents[1]/'.local/benchmarks')))/(str(cpus)+'.json')
    identity=cpu_identity(cpus)
    if not force:
        try:
            saved=json.loads(cache.read_text(encoding='utf-8'))
            if saved.get('identity')==identity and saved.get('benchmark',{}).get('available'):
                return saved['benchmark']
        except (FileNotFoundError,ValueError):pass
    before=cpu_ticks();time.sleep(.6);after=cpu_ticks()
    total=after[0]-before[0]
    load=100*(1-(after[1]-before[1])/total) if total else 100
    if load>20:return _unavailable('CPU busy; calibration deferred until an idle startup')
    print('DCO: CPU '+str(round(load,1))+'%, calibrating 1/'+str(cpus)+' threads (single 60s + multi 60s)...',flush=True)
    result=measure(cpus,executable=executable,environment=environment)
    if result.get('available'):
        cache.parent.mkdir(parents=True,exist_ok=True)
        atomic_json(cache,dict(identity=identity,benchmark=result))
    return result


if __name__ == '__main__' and sys.argv[1:2] == ['--sample']:
    try:
        import numpy as np
    except ImportError:
        result = _unavailable('Install NumPy with setup; real task timing remains available')
    else:
        result = _sample(np,int(sys.argv[2]),float(sys.argv[3]),float(sys.argv[4]))
    print(json.dumps(result))
