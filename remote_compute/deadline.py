"""The worker's execution deadline, shared by its entire process tree."""
import math
import os
import time
from contextlib import contextmanager
from contextvars import ContextVar


_resource_deadline=ContextVar('dco_resource_deadline',default=None)


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
    original=max(0.0,float(deadline)-time.monotonic())if deadline else None
    window=resource_remaining_time()
    return min(original,window)if original is not None and window is not None else original if window is None else window


def check_deadline(*,reserve_seconds=0):
    """Raise TimeoutError at a business boundary, leaving time to publish state."""
    reserve=float(reserve_seconds)
    if not math.isfinite(reserve)or reserve<0:
        raise ValueError('reserve_seconds requires a finite nonnegative number')
    remaining=remaining_time()
    if remaining is not None and remaining<=reserve:
        raise TimeoutError('DCO execution deadline reached; %.3fs remaining, %.3fs reserved for publication'%(remaining,reserve))
    return remaining
