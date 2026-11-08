"""Cooperative CPU demand at executor boundaries, without a polling thread."""
import json
import os
import time
from .allocation import plan_resize
from .memory import same_phase, plan_memory
from .deadline import check_deadline, remaining_time


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
        self.confirmed = None

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
                            'Insufficient RAM for active workers; existing grant retained',
                            'Center resource reply timed out; existing grant retained'))):
                    raise
                # A refused or unanswered resize leaves paid CPU/RAM usable. Re-read that
                # grant: a new phase may need more memory than it can fund.
                inherited=(self.identity==os.environ.get('REMOTE_COMPUTE_JOB_ID')
                    and self.client.url==os.environ.get('REMOTE_COMPUTE_URL','').rstrip('/'))
                target=(os.environ.get('REMOTE_COMPUTE_WORKER_JOB_ID',self.identity)
                    if inherited and os.environ.get('REMOTE_COMPUTE_LEASE')=='1' else self.identity)
                grant=self._retained_status(target,desired,active_cpus,error,inherited)
                available=grant.get('memory_mb',0)
                if isinstance(grant.get('memory_phase'),dict)and same_phase(self.memory,grant['memory_phase']):
                    # The provider already funded this phase using its actual
                    # working set. Raw startup estimates must not revoke it.
                    funded,required=min(desired,grant.get('cpus',0)),None
                else:
                    funded,required=plan_resize(min(desired,grant.get('cpus',0)),
                        self.memory.get('memory_mb'),self.memory.get('base_memory_mb'),
                        self.memory.get('memory_per_cpu_mb'),self.memory.get('memory_limit_mb'),available)
                    if inherited and grant.get('memory_phase')is None and self._inherited_probe():
                        # Keep a fully funded width; the cold-slot fallback must
                        # not replace an existing multithread grant with one.
                        if active_cpus and grant.get('cpus',0)>=active_cpus and available>0 and (funded<active_cpus or required is not None and required>available):
                            # Retain already running work without funding a new
                            # start. The executor fences completions during RPC.
                            funded,required=active_cpus,None
                        elif not active_cpus and (funded<1 or required is not None and required>available):
                            funded,required=plan_memory(1,available,**self.memory)
                            funded=min(funded,desired,grant.get('cpus',0))
                if (grant.get('status')!='running' or funded<max(1,active_cpus) or
                        required is not None and required>available):
                    raise error
                grant={**grant,'cpus':funded}
            self.cpus = grant['cpus']
            self.memory_mb = grant.get('memory_mb', self.memory_mb)
            self.demand, self.active_cpus = desired, active_cpus
            # Only a successfully negotiated or validated usable grant can
            # survive a read outage. The constructor's CPU seed is not one.
            self.confirmed = (dict(cpus=self.cpus,memory_mb=self.memory_mb,
                memory_phase=dict(self.memory)) if grant.get('supported') is not False
                and self.cpus>0 and (not self.memory or self.memory_mb is not None
                and self.memory_mb>0) else None)
            self.updated = time.monotonic()
        return self.cpus

    def _retained_status(self,target,desired,active,error,inherited):
        try:
            return self.client.status(target)
        except Exception as observed:
            if (getattr(observed,'status_code',None) not in (502,503,504)
                    and not getattr(observed,'transport_retryable',False)):
                raise
        if inherited:check_deadline()
        cached=self.confirmed
        if cached is not None and same_phase(self.memory,cached['memory_phase']):
            width=min(desired,cached['cpus'])
            if 'Center resource reply timed out; existing grant retained' in str(error):
                # The unanswered resize could have returned idle slots. Keep
                # its active floor until an authoritative observation arrives.
                width=min(width,max(1,active))
            return dict(cached,status='running',cpus=width)
        # No new work starts from an unverified seed or a different phase.
        # Retry only observation, within the original execution deadline.
        remaining=remaining_time() if inherited else None
        return self.client._read_reconnecting(lambda:self.client.status(target),
            deadline=time.monotonic()+remaining if remaining is not None else None,
            check=check_deadline if inherited else None)

    def _inherited_probe(self):
        try:parent=json.loads(os.environ.get('REMOTE_COMPUTE_MEMORY_MODEL','null'))
        except ValueError:return False
        return isinstance(parent,dict)and same_phase(self.memory,parent)
