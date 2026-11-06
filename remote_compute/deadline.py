"""The worker's execution deadline, shared by its entire process tree."""
import math
import os
import time


def remaining_time():
    """Return execution seconds remaining, or None outside a DCO task."""
    deadline=os.environ.get('REMOTE_COMPUTE_DEADLINE')
    return max(0.0,float(deadline)-time.monotonic())if deadline else None


def check_deadline(*,reserve_seconds=0):
    """Raise TimeoutError at a business boundary, leaving time to publish state."""
    reserve=float(reserve_seconds)
    if not math.isfinite(reserve)or reserve<0:
        raise ValueError('reserve_seconds requires a finite nonnegative number')
    remaining=remaining_time()
    if remaining is not None and remaining<=reserve:
        raise TimeoutError('DCO execution deadline reached; %.3fs remaining, %.3fs reserved for publication'%(remaining,reserve))
    return remaining
