"""Fixed scalar mix, run in short-lived stdlib-only processes at worker startup."""
import json
import math
import os
from pathlib import Path
import subprocess
import sys
import time

NAME = 'mixed-cpu-v1'
BLOCK = 64


class MixedLoad:
    def __init__(self):
        self.volts = [0.0] * 8
        self.vector = [(i+1)/100 for i in range(16)]
        self.matrix = [[((i*7+j*11) % 17-8)/128 for j in range(16)] for i in range(16)]
        self.ram = [(i*2654435761) & 0xffffffff for i in range(1024)]
        self.word, self.ticks = 0x12345678, 0

    def block(self, count=BLOCK):
        volts, vector, matrix, ram = self.volts, self.vector, self.matrix, self.ram
        word = self.word
        drive = 0.25 + 0.02*math.sin(self.ticks/128)
        for tick in range(self.ticks, self.ticks+count):
            # RC ladder with a diode: three Newton updates of one sparse row.
            i = tick & 7
            rhs = 2*volts[i]+volts[(i-1) & 7]+volts[(i+1) & 7]+drive
            v = volts[i]
            for _ in range(3):
                diode = 0.001*math.exp(v/0.026)
                v -= (4*v+diode-0.001-rhs)/(4+diode/0.026)
            volts[i] = v
            # Fixed-width RTL-style state, conditional datapath and indexed RAM.
            data = ram[(word >> 9) & 1023]
            word = (((word << 5) | (word >> 27))+data+tick+3) & 0xffffffff
            word = ((word ^ (data >> 1)) if word & 1 else word*1664525+1013904223) & 0xffffffff
            ram[(word >> 15) & 1023] = word
            # Double-precision matrix row product, reduction and state update.
            i = tick & 15
            total = 0.0
            for a, b in zip(matrix[i], vector):
                total += a*b
            vector[i] = 0.995*vector[i]+0.005*total+0.0001*v
        self.word, self.ticks = word, self.ticks+count


def _lane():
    load = MixedLoad()
    load.block()  # Warm every lane before the common start.
    print('ready', flush=True)
    start = sys.stdin.readline()
    if not start:
        return
    deadline = float(start)
    iterations = 0
    while time.monotonic() < deadline:
        load.block()
        iterations += BLOCK
    print(json.dumps(dict(iterations=iterations, finished=time.monotonic(),
                          checksum=[load.word, sum(load.volts), sum(load.vector)])), flush=True)


def measure(cpus):
    """Budget about two seconds including bootstrap; all lanes share a deadline."""
    # monotonic has a process-shared epoch on supported Windows/Linux versions,
    # including Python 3.7 where Windows perf_counter still had per-process epochs.
    began = time.monotonic()
    lanes = []
    try:
        for _ in range(cpus):
            lanes.append(subprocess.Popen([sys.executable, '-I', '-B', '-u', str(Path(__file__).resolve()), '--lane'],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                text=True, encoding='utf-8', creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0))
        for lane in lanes:
            if lane.stdout.readline().strip() != 'ready':
                raise RuntimeError('CPU benchmark lane failed: '+lane.stderr.read())
        started = time.monotonic()
        deadline = started+max(0.25, 2.0-(started-began))
        for lane in lanes:
            lane.stdin.write(str(deadline)+'\n')
            lane.stdin.flush()
        samples = []
        for lane in lanes:
            stdout, stderr = lane.communicate(timeout=max(1, deadline-time.monotonic()+1))
            if lane.returncode:
                raise RuntimeError('CPU benchmark lane failed: '+stderr)
            samples.append(json.loads(stdout))
        elapsed = max(sample['finished'] for sample in samples)-started
        iterations = sum(sample['iterations'] for sample in samples)
        return dict(name=NAME, score=round(iterations/elapsed/1000000, 6), unit='Miter/s',
                    threads=cpus, seconds=round(elapsed, 3), total_seconds=round(time.monotonic()-began, 3),
                    iterations=iterations, python=sys.version.split()[0],
                    components=['circuit-newton', 'rtl-state-ram', 'scientific-dot'],
                    checksums=[sample['checksum'] for sample in samples])
    finally:
        for lane in lanes:
            if lane.poll() is None:
                lane.kill()
            lane.wait()
            for stream in (lane.stdin, lane.stdout, lane.stderr):
                if stream is not None:
                    stream.close()


if __name__ == '__main__' and sys.argv[1:] == ['--lane']:
    _lane()
