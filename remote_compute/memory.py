"""Per-command working-set and private-commit envelopes for admission."""
from collections import deque
import math
import os
from pathlib import Path
import threading
import time

PRESSURE_GRACE_SECONDS=2.0
PRESSURE_RECOVERY_SECONDS=2.0
COMMIT_EMERGENCY_MB=1024


def pressure_threshold(host):
    return max(host.get('memory_headroom_mb',0),512,min(2048,int(host.get('memory_total_mb',0)*.02)))


class PressureGuard:
    """Predictions pause growth; actual critical headroom terminates a tree."""
    def __init__(self):
        self.warning_since=None;self.critical_since=None;self.recovered_since=None
        self.state=dict(limited=False,waiting=False,terminate=False)
        self.previous=None
        self.fall_rates=(0,0)

    def update(self,host,now=None):
        now=time.monotonic()if now is None else now
        threshold=pressure_threshold(host)
        physical=host.get('host_memory_available_mb',threshold)
        commit=host.get('host_commit_available_mb',math.inf)
        warning=physical<threshold+512 or commit<COMMIT_EMERGENCY_MB+512
        critical=physical<threshold or commit<COMMIT_EMERGENCY_MB
        predicted=False
        if self.previous is not None:
            at,old_physical,old_commit=self.previous;span=max(.05,now-at)
            if span<=2:
                rates=(max(0,(old_physical-physical)/span),
                       max(0,(old_commit-commit)/span)if math.isfinite(old_commit)and math.isfinite(commit)else 0)
                predicted=(physical-min(rates[0],self.fall_rates[0])<threshold or
                           commit-min(rates[1],self.fall_rates[1])<COMMIT_EMERGENCY_MB)
                self.fall_rates=rates
            else:self.fall_rates=(0,0)
        self.previous=(now,physical,commit)
        warning=warning or predicted
        if warning:
            if self.warning_since is None:self.warning_since=now
            self.recovered_since=None
        elif self.warning_since is not None:
            if self.recovered_since is None:self.recovered_since=now
            if now-self.recovered_since>=PRESSURE_RECOVERY_SECONDS:self.warning_since=None
        if critical:
            if self.critical_since is None:self.critical_since=now
        else:self.critical_since=None
        settling=not warning and self.warning_since is not None
        waiting=settling or warning
        # A short allocation burst can predict a crossing that never happens.
        # Stop admission and new kernels first; keep paid work alive until an
        # actual sample crosses the configured physical/commit protection line.
        terminate=critical
        self.state=dict(limited=warning or settling,waiting=bool(waiting),terminate=bool(terminate),
            warning=warning,predicted=predicted,critical=critical,physical_threshold_mb=threshold,commit_emergency_mb=COMMIT_EMERGENCY_MB,
            warning_seconds=0 if self.warning_since is None else round(now-self.warning_since,3))
        return self.state


_boundary_sample=(None,0.0,False)


def memory_waiting():
    """A local worker signal; stable kernels do no per-batch filesystem I/O."""
    global _boundary_sample
    path=os.environ.get('REMOTE_COMPUTE_MEMORY_WAIT')
    if not path:return False
    sampled,until,waiting=_boundary_sample;now=time.monotonic()
    if sampled!=path or now>=until:
        waiting=Path(path).is_file();_boundary_sample=(path,now+.1,waiting)
    return waiting


def memory_boundary():
    """Wait before starting another item/kernel, within its original deadline."""
    from .deadline import check_deadline
    if not memory_waiting():return
    from .compat import unlink
    root=os.environ.get('REMOTE_COMPUTE_JOB_DIR')
    marker=None;expired=False
    if root:
        folder=Path(root)/'input/.dco-memory-waits';folder.mkdir(parents=True,exist_ok=True)
        marker=folder/('%s-%s'%(os.getpid(),threading.get_ident()));marker.touch()
    try:
        while memory_waiting():
            check_deadline();time.sleep(.05)
    except TimeoutError:
        expired=True;raise
    finally:
        if marker is not None and not expired:unlink(marker)


def startup_memory(task):
    """Fund one cold probe rather than reject an unmeasured large estimate."""
    return min(task.get('memory_mb',256),256*max(1,task.get('cpus',1)))


def same_phase(model,previous):
    clean=lambda value:{k:v for k,v in (value or {}).items()if v is not None}
    return clean(model)==clean(previous)


def calibrated_model(model,previous,active_cpus,physical,commit,calibrated,learned=None,points=None,*,cold_probe=False):
    """Separate the shared working set from observed width-dependent growth."""
    fixed=model.get('memory_mb')is not None
    if not same_phase(model,previous)or not(fixed or model.get('base_memory_mb')is not None):return model
    if not calibrated:return dict(learned)if learned is not None else model
    memory=math.ceil(max(physical,commit))
    if not active_cpus:
        if learned is not None:return dict(learned)
        if fixed:return dict(memory_mb=memory)
        if not cold_probe:return model
        # A stable cold slot funds an unseen lane conservatively with its
        # entire measured envelope, without reporting fictitious active work.
        return dict(base_memory_mb=memory if not model.get('memory_per_cpu_mb')else 0,
                    memory_per_cpu_mb=memory if model.get('memory_per_cpu_mb')else 0,
                    **({'memory_limit_mb':model['memory_limit_mb']}if model.get('memory_limit_mb')is not None else {}))
    samples=dict(points or ());samples[active_cpus]=memory
    samples=sorted((width,value)for width,value in samples.items()if width>0)
    if fixed and len(samples)<2 and (learned is None or learned.get('memory_per_cpu_mb')is None):return dict(memory_mb=memory)
    if len(samples)>1:
        per=max(0,max(math.ceil((b-a)/(wb-wa))for i,(wa,a)in enumerate(samples)for wb,b in samples[i+1:]))
        base=max(0,max(math.ceil(value-per*width)for width,value in samples))
    else:
        if (not fixed and model.get('memory_per_cpu_mb') == 0
                or learned is not None and learned.get('memory_per_cpu_mb') == 0):
            base,per=memory,0
        else:
            # One width cannot identify a shared baseline and a per-worker
            # increment. Charge its whole measured envelope per observed slot
            # until another width measures that split. Subtracting the declared
            # base here can turn a cold interpreter into almost free workers.
            base,per=0,math.ceil(memory/active_cpus)
    result=dict(base_memory_mb=base,memory_per_cpu_mb=per)
    if model.get('memory_limit_mb')is not None:result['memory_limit_mb']=model['memory_limit_mb']
    return result


def plan_memory(cpus,available_mb,*,floor=1,**model):
    """Fit the estimate, with a one-slot probe when only the estimate blocks."""
    from .allocation import plan_resize
    model=dict(dict(memory_mb=None,base_memory_mb=None,memory_per_cpu_mb=None,memory_limit_mb=None),**model)
    width,memory=plan_resize(cpus,available_mb=available_mb,**model)
    if width>=floor and (memory is None or memory<=available_mb):return width,memory
    # Estimates do not form a hard RAM limit. Actual host pressure terminates
    # an offending process tree and keeps its original receipt and paid work.
    needed=(model.get('base_memory_mb',0)or 0)+(model.get('memory_per_cpu_mb',0)or 0)*floor
    needed=model.get('memory_mb')or needed
    probe=min(needed,256*floor)
    if available_mb>=probe:return floor,probe
    return 0,memory


def pressure_victim(jobs,host,*,emergency=False):
    """Newest admitted task first, including children not yet memory-sampled."""
    threshold=pressure_threshold(host)
    physical=host.get('host_memory_available_mb',threshold)
    commit=host.get('host_commit_available_mb',math.inf)
    if not emergency and physical>=threshold and commit>=COMMIT_EMERGENCY_MB:return None
    candidates=[j for j in jobs if j['result']['status']=='running'and not j['cancel'].is_set()
                and j.get('process') is not None]
    return max(candidates,key=lambda j:(j['result'].get('started',0),j['result'].get('submitted',0),j['result'].get('id','')),default=None)


class MemoryObservation:
    """Keep startup funding, then quote recent peaks plus growth room."""
    def __init__(self, now=None):
        self.started = time.monotonic() if now is None else now
        self.samples = deque()
        self.envelope = None
        self.active_cpus = 0

    def observe(self, rss, private=None, now=None, active_cpus=0):
        now = time.monotonic() if now is None else now
        if rss <= 0:
            return
        self.active_cpus=max(self.active_cpus,active_cpus)
        private=rss if private is None else private
        if self.samples and now-self.samples[-1][0] < .25:
            at,peak_rss,peak_private=self.samples[-1]
            self.samples[-1]=(at,max(peak_rss,rss),max(peak_private,private))
            return
        self.samples.append((now, rss, private))
        while len(self.samples)>1 and self.samples[0][0] < now-10:
            self.samples.popleft()
        if self.envelope is None and (now-max(self.started,self.samples[0][0]) < 2 or len(self.samples)<3):
            return
        recent = next((s for s in self.samples if s[0]>=now-2), self.samples[0])
        span = max(.25, now-recent[0])
        self.envelope = tuple(math.ceil(max(
            max(s[column] for s in self.samples)*1.25,
            max(s[column] for s in self.samples)+32,
            max(s[column] for s in self.samples)+2*max(0,(self.samples[-1][column]-recent[column])/span)))
            for column in (1,2))


def reservation(job):
    """Return physical and commit targets without rewriting the request."""
    task = job['task']
    rss = job.get('rss_mb',0)
    private = job.get('private_mb',rss)
    observation = job.get('memory_observation')
    if observation is None or observation.envelope is None:
        initial=job.get('startup_memory_mb',task['memory_mb'])
        return max(initial,rss), max(initial,private)
    physical, commit = observation.envelope
    return max(physical,rss), max(commit,private)
