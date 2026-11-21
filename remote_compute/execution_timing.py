"""Wall-clock intervals from the managed executor, separate from CPU billing."""
import math
import os
from pathlib import Path
import threading
import time


class PhaseClock:
    def __init__(self):
        root = os.environ.get('REMOTE_COMPUTE_TIMING_ROOT')
        name = 'phase-%d-%d-%d.log' % (os.getpid(), threading.get_ident(), time.monotonic_ns())
        self.path = Path(root) / name if root else None
        self.phase = None

    def set(self, phase):
        if self.path is None or self.phase == phase:
            return
        self.phase = phase
        try:
            with self.path.open('a', encoding='ascii') as stream:
                stream.write(f'{time.monotonic():.9f} {phase}\n')
        except OSError:
            # Timing is supplemental; preserve task execution and CPU billing.
            self.path = None

    def close(self):
        self.set('idle')


def summarize(root, received, started, ended):
    timing = dict(worker_received_to_process_seconds=None if started is None else max(0., started - received))
    if root is None or started is None:
        return timing

    states = {'native': 0, 'resource': 1, 'exact_grant': 2, 'exact_confirm': 3}
    events = []

    def interval(phase, begin, end):
        begin, end = max(started, begin), min(ended, end)
        if phase in states and end > begin:
            events.extend(((begin, states[phase], 1), (end, states[phase], -1)))

    for path in root.glob('phase-*.log'):
        phase, previous = 'idle', started
        try:
            if path.is_symlink() or not path.is_file():
                continue
            with path.open(encoding='ascii', errors='replace') as stream:
                for line in stream:
                    parts = line.split()
                    if len(parts) != 2 or (parts[1] != 'idle' and parts[1] not in states):
                        continue
                    try:
                        stamp = float(parts[0])
                    except ValueError:
                        continue
                    if previous <= stamp <= ended and math.isfinite(stamp):
                        interval(phase, previous, stamp)
                        previous, phase = stamp, parts[1]
        except OSError:
            continue
        interval(phase, previous, ended)

    live = [0] * len(states)
    totals = [0.] * len(states)
    previous = started
    first_native = None
    exact_seen = False
    for stamp, kind, delta in sorted(events):
        if live[0]:
            totals[0] += stamp - previous
        elif any(live[1:]):
            totals[1] += stamp - previous
            if live[3]:
                totals[3] += stamp - previous
            elif live[2]:
                totals[2] += stamp - previous
        if kind == 0 and delta > 0 and first_native is None:
            first_native = stamp
        exact_seen |= kind >= 2
        live[kind] += delta
        previous = stamp
    timing.update(worker_received_to_native_seconds=None if first_native is None else max(0., first_native - received),
                  process_window_seconds=max(0., ended - started),
                  native_execution_seconds=totals[0] if events else None,
                  resource_wait_seconds=totals[1] if events else None,
                  other_execution_seconds=max(0., ended - started - sum(totals[:2])) if events else None)
    if exact_seen:
        timing.update(exact_grant_wait_seconds=totals[2], exact_launch_confirmation_seconds=totals[3])
    return timing
