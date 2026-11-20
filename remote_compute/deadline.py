"""The worker's execution deadline, shared by its entire process tree."""
import json
import math
import os
import time
from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path


_resource_deadline=ContextVar('dco_resource_deadline',default=None)
_execution_paused=ContextVar('dco_execution_paused',default=False)
PREPARATION_TIMEOUT = 300


def _pause_state(path):
    if path:
        try:
            value = json.loads(Path(path).read_bytes())
            elapsed = float(value['elapsed'])
            started = float(value.get('started', 0))
            expires = float(value.get('expires', 0))
            if all(math.isfinite(number) and number >= 0 for number in (elapsed, started, expires)):
                return elapsed, started, expires
        except (OSError, ValueError, KeyError, TypeError):
            pass
    return 0.0, 0.0, 0.0


def preparation_elapsed(path, now=None):
    elapsed, started, expires = _pause_state(path)
    return elapsed + max(0.0, min(time.monotonic() if now is None else now, expires) - started)


def execution_remaining(deadline, path=None):
    now = time.monotonic()
    return max(0.0, float(deadline) + preparation_elapsed(path, now) - now)


@contextmanager
def preparation_window(seconds=None):
    """Bound a pre-execution resource wait independently of paid execution."""
    path = os.environ.get('REMOTE_COMPUTE_PREPARATION_CLOCK')
    if not path or _execution_paused.get():
        with resource_window(seconds):
            yield
        return
    from .bundle import atomic_json
    seconds = PREPARATION_TIMEOUT if seconds is None else float(seconds)
    if not math.isfinite(seconds) or seconds <= 0:
        raise ValueError('start_timeout requires a finite positive number')
    check_deadline()
    started = time.monotonic()
    elapsed = preparation_elapsed(path, started)
    expires = started + seconds
    atomic_json(Path(path), dict(elapsed=elapsed, started=started, expires=expires), private=True)
    paused = _execution_paused.set(True)
    limit = _resource_deadline.get()
    deadline = _resource_deadline.set(expires if limit is None else min(limit, expires))
    try:
        yield
    finally:
        try:
            atomic_json(Path(path), dict(elapsed=elapsed+max(0.0,min(time.monotonic(),expires)-started)), private=True)
        finally:
            _resource_deadline.reset(deadline)
            _execution_paused.reset(paused)


def resource_remaining_time():
    deadline=_resource_deadline.get()
    return max(0.0,deadline-time.monotonic())if deadline is not None else None


@contextmanager
def resource_window(seconds):
    """Bound a startup handshake without extending the original task deadline."""
    if seconds is None:
        original=os.environ.get('REMOTE_COMPUTE_DEADLINE')
        if not original:
            yield
            return
        deadline=float(original)
    else:
        seconds=float(seconds)
        if not math.isfinite(seconds)or seconds<=0:
            raise ValueError('start_timeout requires a finite positive number')
        deadline=time.monotonic()+seconds
    previous=_resource_deadline.get()
    token=_resource_deadline.set(min(previous,deadline)if previous is not None else deadline)
    try:yield
    finally:_resource_deadline.reset(token)


def remaining_time():
    """Return execution seconds remaining, or None outside a DCO task."""
    deadline=os.environ.get('REMOTE_COMPUTE_DEADLINE')
    original=execution_remaining(deadline,os.environ.get('REMOTE_COMPUTE_PREPARATION_CLOCK')) if deadline and not _execution_paused.get() else None
    window=resource_remaining_time()
    return min(original,window)if original is not None and window is not None else original if window is None else window


def checkpoint_requested():
    """Return true during the configured grace window inside the execution limit."""
    grace = os.environ.get('REMOTE_COMPUTE_TIMEOUT_GRACE')
    remaining = remaining_time()
    return grace is not None and remaining is not None and remaining <= float(grace)


def check_deadline(*,reserve_seconds=0):
    """Raise TimeoutError at a business boundary, leaving time to publish state."""
    reserve=float(reserve_seconds)
    if not math.isfinite(reserve)or reserve<0:
        raise ValueError('reserve_seconds requires a finite nonnegative number')
    remaining=remaining_time()
    if remaining is not None and remaining<=reserve:
        raise TimeoutError('DCO execution deadline reached; %.3fs remaining, %.3fs reserved for publication'%(remaining,reserve))
    return remaining
