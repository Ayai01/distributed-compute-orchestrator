"""Cooperative CPU demand at executor boundaries, without a polling thread."""
import json
import os
import time
from .allocation import plan_resize
from .memory import same_phase, plan_memory
from .deadline import check_deadline, remaining_time

# Independently loaded SDK modules keep this wire marker local.
RESOURCE_TRANSPORT_RETAINED='Center resource transport interrupted; existing grant retained'


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
        self.phase_pending = False

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
        if unchanged and not self.phase_pending and self.cpus == desired == max(1, active_cpus):
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
                            'Center resource reply timed out; existing grant retained',
                            RESOURCE_TRANSPORT_RETAINED))):
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
                # A worker can run before the center replaces its initial
                # queued/dispatching sample. Only an inherited, previously
                # confirmed grant may bridge that observation lag.
                executing=(grant.get('status')=='running' or inherited
                    and os.environ.get('REMOTE_COMPUTE_WORKER')=='1' and self.confirmed is not None
                    and grant.get('status')in('queued','dispatching'))
                if (not executing or funded<max(1,active_cpus) or
                        required is not None and required>available):
                    probe=(self._retained_downshift(grant,active_cpus) if inherited
                        and os.environ.get('REMOTE_COMPUTE_WORKER')=='1' else None)
                    if probe is not None:grant=probe
                    elif (inherited and executing and not active_cpus
                            and os.environ.get('REMOTE_COMPUTE_WORKER')=='1'
                            and grant.get('cpus',0)>=1 and available>0):
                        # A paid preparation phase can end below the next
                        # phase's first-slot estimate. Negotiate that slot at
                        # this safe boundary before starting any new body.
                        grant=self._wait_for_phase(target)
                    else:raise error
                else:grant={**grant,'cpus':funded}
            self.cpus = grant['cpus']
            self.phase_pending = bool(grant.get('_phase_pending'))
            self.memory_mb = grant.get('memory_mb', self.memory_mb)
            self.demand, self.active_cpus = desired, active_cpus
            # Only a successfully negotiated or validated usable grant can
            # survive a read outage. The constructor's CPU seed is not one.
            self.confirmed = (dict(cpus=self.cpus,memory_mb=self.memory_mb,
                memory_phase=dict(grant['memory_phase'] if self.phase_pending else self.memory)) if grant.get('supported') is not False
                and self.cpus>0 and (not self.memory or self.memory_mb is not None
                and self.memory_mb>0) else None)
            self.updated = time.monotonic()
        return self.cpus

    def _retained_downshift(self,grant,active):
        """Keep one already paid slot when the next estimate decreases."""
        previous=grant.get('memory_phase')
        if (grant.get('status')!='running' or active>1 or grant.get('cpus',0)<1
                or grant.get('memory_mb',0)<=0 or not isinstance(previous,dict)):
            return None
        def first(model):
            return model.get('memory_mb') or ((model.get('base_memory_mb',0)or 0)
                +(model.get('memory_per_cpu_mb',0)or 0))
        needed,prior=first(self.memory),first(previous)
        if not 0<needed<prior or self.memory.get('memory_limit_mb',needed)<needed:
            return None
        if grant['cpus']>1:
            # Return idle slots under the acknowledged phase before starting
            # the single-slot probe. This does not borrow any new CPU or RAM.
            try:reduced=self.client.resize(self.identity,1,active_cpus=active,**previous)
            except Exception as error:
                if (getattr(error,'status_code',None)!=409 or not any(message in str(error)for message in (
                        'Insufficient memory for resource resize; existing grant retained',
                        'Insufficient RAM for active workers; existing grant retained',
                        'Center resource reply timed out; existing grant retained',RESOURCE_TRANSPORT_RETAINED))):raise
                # The original grant still contains this one slot; retry its
                # idle-slot return at the next executor boundary.
            else:
                if reduced.get('cpus')!=1:return None
                grant=reduced
        return dict(grant,cpus=1,memory_phase=dict(previous),_phase_pending=True)

    def _wait_for_phase(self,target):
        from .client import RemoteError
        def waiting():
            error=RemoteError('Waiting for the first funded slot of the new memory phase')
            error.transport_retryable=True
            error.phase_waiting=True
            return error
        def settle():
            current=self.client.status(target)
            if current.get('status')!='running':
                error=RemoteError('Only a running task can change its CPU grant');error.status_code=409
                raise error
            if current.get('resource_request'):raise waiting()
            if (isinstance(current.get('memory_phase'),dict)
                    and same_phase(self.memory,current['memory_phase'])
                    and current.get('cpus',0)>=1 and current.get('memory_mb',0)>0):
                return dict(current,cpus=1)
            try:
                return self.client.resize(self.identity,1,active_cpus=0,**self.memory)
            except RemoteError as error:
                if error.status_code==409 and any(message in str(error)for message in (
                        'Insufficient memory for resource resize; existing grant retained',
                        'Insufficient RAM for active workers; existing grant retained',
                        'Center resource reply timed out; existing grant retained',
                        RESOURCE_TRANSPORT_RETAINED)):
                    raise waiting() from error
                raise
        while True:
            remaining=remaining_time()
            try:
                return self.client._read_reconnecting(settle,
                    deadline=time.monotonic()+remaining if remaining is not None else None,
                    check=check_deadline)
            except RemoteError as error:
                # Resource admission has the task's original deadline. A
                # transport reconnect window must not end that task early.
                if not getattr(error,'phase_waiting',False)or remaining is None:raise
                check_deadline()

    def _retained_status(self,target,desired,active,error,inherited):
        uncertain=any(message in str(error)for message in (
            'Center resource reply timed out; existing grant retained',RESOURCE_TRANSPORT_RETAINED))
        def observe():
            grant=self.client.status(target)
            if uncertain:
                # A late resize may still return idle slots after this read.
                # Preserve the reported active floor, not those unsettled slots.
                grant=dict(grant,cpus=min(grant.get('cpus',0),max(1,active)))
            return grant
        try:
            return observe()
        except Exception as observed:
            if (getattr(observed,'status_code',None) not in (502,503,504)
                    and not getattr(observed,'transport_retryable',False)):
                raise
        if inherited:check_deadline()
        cached=self.confirmed
        if cached is not None and same_phase(self.memory,cached['memory_phase']):
            width=min(desired,cached['cpus'])
            if uncertain:
                # The unanswered resize could have returned idle slots. Keep
                # its active floor until an authoritative observation arrives.
                width=min(width,max(1,active))
            return dict(cached,status='running',cpus=width)
        # No new work starts from an unverified seed or a different phase.
        # Retry only observation, within the original execution deadline.
        remaining=remaining_time() if inherited else None
        return self.client._read_reconnecting(observe,
            deadline=time.monotonic()+remaining if remaining is not None else None,
            check=check_deadline if inherited else None)

    def _inherited_probe(self):
        try:parent=json.loads(os.environ.get('REMOTE_COMPUTE_MEMORY_MODEL','null'))
        except ValueError:return False
        return isinstance(parent,dict)and same_phase(self.memory,parent)
