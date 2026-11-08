"""Per-command working-set and private-commit envelopes for admission."""
from collections import deque
import math
import time


def startup_memory(task):
    """Fund one cold probe rather than reject an unmeasured large estimate."""
    return min(task.get('memory_mb',256),256*max(1,task.get('cpus',1)))


def calibrated_model(model,previous,active_cpus,physical,commit,calibrated):
    """Scale a stable whole-tree envelope; retain new-phase declarations."""
    clean=lambda value:{k:v for k,v in (value or {}).items()if v is not None}
    if not calibrated or not active_cpus or clean(model)!=clean(previous)or 'base_memory_mb'not in model:return model
    result=dict(base_memory_mb=0,memory_per_cpu_mb=max(1,math.ceil(max(physical,commit)/active_cpus)))
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
    needed=(model.get('base_memory_mb',0)or 0)+(model.get('memory_per_cpu_mb',0)or 0)
    needed=model.get('memory_mb')or needed
    probe=min(needed,256*floor)
    if available_mb>=probe:return floor,probe
    return 0,memory


def pressure_victim(jobs,host):
    """Choose one low-priority consumer when real RAM or commit is critical."""
    threshold=max(128,min(1024,int(host.get('memory_total_mb',0)*.02)))
    physical=host.get('host_memory_available_mb',threshold)
    commit=host.get('host_commit_available_mb',threshold)
    if physical>=threshold and commit>=threshold:return None
    private=commit<physical
    candidates=[j for j in jobs if j['result']['status']=='running'and not j['cancel'].is_set()
                and j.get('private_mb'if private else 'rss_mb',j.get('rss_mb',0))>0]
    return min(candidates,key=lambda j:(j['task'].get('priority',0),
        -j.get('private_mb'if private else 'rss_mb',j.get('rss_mb',0))),default=None)


class MemoryObservation:
    """Keep startup funding, then quote recent peaks plus growth room."""
    def __init__(self, now=None):
        self.started = time.monotonic() if now is None else now
        self.samples = deque()
        self.envelope = None

    def observe(self, rss, private=None, now=None):
        now = time.monotonic() if now is None else now
        if rss <= 0:
            return
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
