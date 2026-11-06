"""Cooperative CPU demand at executor boundaries, without a polling thread."""
import time
from .allocation import plan_resize


class CpuLease:
    """Keep an executor's usable grant current while its ready work changes."""

    def __init__(self, client, identity, *, max_cpus=1024, cpus=1, memory_mb=None,
                 base_memory_mb=None, memory_per_cpu_mb=None, memory_limit_mb=None):
        if type(cpus) is not int or type(max_cpus) is not int or not 1 <= cpus <= max_cpus:
            raise ValueError('cpus and max_cpus require 1 <= cpus <= max_cpus')
        self.client, self.identity = client, identity
        self.max_cpus, self.cpus = max_cpus, cpus
        self.memory = {key: value for key, value in dict(memory_mb=memory_mb,
            base_memory_mb=base_memory_mb, memory_per_cpu_mb=memory_per_cpu_mb,
            memory_limit_mb=memory_limit_mb).items() if value is not None}
        self.memory_mb = memory_mb
        self.updated = float('-inf')
        self.demand = self.active_cpus = None

    def refresh(self, *, active_cpus=0, ready_cpus=None):
        """Report in-flight threads and immediately runnable demand; return CPUs.

        Active threads retain their existing authorization. Only idle slots can
        be returned until those threads finish. A rolling executor calls this
        after completions and at least once per second while waiting for them.
        """
        if type(active_cpus) is not int or not 0 <= active_cpus <= self.cpus:
            raise ValueError('active_cpus must count threads within the existing grant')
        if ready_cpus is not None and (type(ready_cpus) is not int or ready_cpus < 0):
            raise ValueError('ready_cpus must be a nonnegative thread demand or None')
        desired = self.max_cpus if ready_cpus is None else min(self.max_cpus, max(1, active_cpus + ready_cpus))
        unchanged = desired == self.demand and active_cpus == self.active_cpus
        # No slot can grow or be returned in a stable fully occupied phase.
        # Other phases still poll: the same demand can borrow a peer's release.
        if unchanged and self.cpus == desired == max(1, active_cpus):
            return self.cpus
        if (desired != self.demand or active_cpus != self.active_cpus
                or time.monotonic() - self.updated >= 1):
            try:
                grant = self.client.resize(self.identity, desired, active_cpus=active_cpus, **self.memory)
            except Exception as error:
                if (getattr(error,'status_code',None)!=409 or
                        not any(message in str(error)for message in (
                            'Insufficient memory for resource resize; existing grant retained',
                            'Center resource reply timed out; existing grant retained'))):
                    raise
                # A refused or unanswered resize leaves paid CPU/RAM usable. Re-read that
                # grant: a new phase may need more memory than it can fund.
                grant=self.client.status(self.identity)
                available=grant.get('memory_mb',0)
                funded,required=plan_resize(min(desired,grant.get('cpus',0)),
                    self.memory.get('memory_mb'),self.memory.get('base_memory_mb'),
                    self.memory.get('memory_per_cpu_mb'),self.memory.get('memory_limit_mb'),available)
                if (grant.get('status')!='running' or funded<max(1,active_cpus) or
                        required is not None and required>available):
                    raise error
                grant={**grant,'cpus':funded}
            self.cpus = grant['cpus']
            self.memory_mb = grant.get('memory_mb', self.memory_mb)
            self.demand, self.active_cpus = desired, active_cpus
            self.updated = time.monotonic()
        return self.cpus
