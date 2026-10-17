"""Approximate native mixed throughput using the installed NumPy/BLAS runtime."""
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time

NAME = 'numpy-mixed-v3'
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

    def checksum(self):
        return [int(self.words[0]),float(self.np.sum(self.volts)),float(self.np.sum(self.matrix))]


def _sample(np, cpus, began):
    serial = MixedLoad(np)
    serial.block()
    serial_started = time.monotonic()
    serial_deadline = serial_started + max(0.1,min(0.4,(began+2.0-serial_started)/4))
    serial_iterations = 0
    while time.monotonic()<serial_deadline:
        serial.block()
        serial_iterations += BATCH
    serial_elapsed = time.monotonic()-serial_started
    single = dict(name=NAME,available=True,score=round(serial_iterations/serial_elapsed/1000000,6),
                  unit='Miter/s',threads=1,seconds=round(serial_elapsed,3),iterations=serial_iterations)
    started = deadline = 0.0
    def start():
        nonlocal started,deadline
        started = time.monotonic()
        deadline = started+max(0.25,2.0-(started-began))
    ready = threading.Barrier(cpus+1,action=start)
    def lane():
        try:
            load = MixedLoad(np)
            load.block()
            ready.wait()
            iterations = 0
            while time.monotonic()<deadline:
                load.block()
                iterations += BATCH
            return iterations,time.monotonic(),load.checksum()
        except BaseException:
            ready.abort()
            raise
    with ThreadPoolExecutor(max_workers=cpus,thread_name_prefix='native-benchmark') as executor:
        try:
            futures = [executor.submit(lane) for _ in range(cpus)]
            ready.wait()
            samples = [future.result() for future in futures]
        finally:
            ready.abort()
    elapsed = max(finished for _,finished,_ in samples)-started
    iterations = sum(count for count,_,_ in samples)
    introspect = getattr(getattr(np.lib,'introspect',None),'opt_func_info',None)
    return dict(name=NAME,available=True,score=round(iterations/elapsed/1000000,6),unit='Miter/s',
                threads=cpus,seconds=round(elapsed,3),total_seconds=round(time.monotonic()-began,3),
                iterations=iterations,numpy=np.__version__,
                single=single,
                dispatch=introspect(func_name='^exp$') if introspect else {},
                components=['native-fp64-newton','native-u32-ram','native-blas-matmul'],
                checksums=[checksum for _,_,checksum in samples])


def _unavailable(reason):
    return dict(name=NAME,available=False,reason=reason,unit='Miter/s',threads=0)


def measure(cpus, *, executable=None, environment=()):
    """One disposable native helper; the service itself stays stdlib-only."""
    began = time.monotonic()
    env = dict(os.environ, **dict(environment))
    for name in ('OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','OMP_NUM_THREADS',
                 'BLIS_NUM_THREADS','VECLIB_MAXIMUM_THREADS'):
        env[name] = '1'  # Outer lanes share the worker budget; no nested BLAS pool.
    with subprocess.Popen([executable or sys.executable,'-I','-B','-u',str(Path(__file__).resolve()),
                           '--sample',str(cpus),str(began)],
            env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,encoding='utf-8',
            creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0)) as helper:
        try:
            stdout,stderr = helper.communicate(timeout=5)
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


if __name__ == '__main__' and sys.argv[1:2] == ['--sample']:
    try:
        import numpy as np
    except ImportError:
        result = _unavailable('Install NumPy with setup; real task timing remains available')
    else:
        result = _sample(np,int(sys.argv[2]),float(sys.argv[3]))
    print(json.dumps(result))
