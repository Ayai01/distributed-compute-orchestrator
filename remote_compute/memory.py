"""Per-command working-set and private-commit envelopes for admission."""
from collections import deque
import math
import time


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
    modeled = job.get('memory_phase_model',task.get('memory_model'))
    if modeled or observation is None or observation.envelope is None:
        return max(task['memory_mb'],rss), max(task['memory_mb'],private)
    physical, commit = observation.envelope
    return max(physical,rss), max(commit,private)
