"""Durable snapshots of jobs whose lifecycle is owned outside the scheduler."""

import json
import re

from .bundle import TERMINAL, atomic_json, progress_fields
from .worker import Conflict, _ID


_KEY = re.compile(r'[A-Za-z0-9_.-]{1,80}\Z')
_TEXT = {'label': 200, 'project': 80, 'phase': 80}
_INTEGERS = {'cpus', 'active_cpus', 'max_cpus', 'memory_mb'}
_NUMBERS = {'submitted', 'started', 'finished', 'elapsed', 'cpu_seconds',
            'cumulative_cpu_seconds', 'cpu_sampled_at', 'measured_cpus', 'rss_mb', 'peak_rss_mb'}
_REQUIRED = {'id', 'source', 'node', 'revision', 'status', 'submitted'}
_METADATA = ('project', 'label', 'cpus', 'max_cpus', 'memory_mb')
_TERMINAL_FIELDS = ('started', 'finished', 'elapsed', 'cpu_seconds', 'cumulative_cpu_seconds')


def observation_batch(value):
    if not isinstance(value, list) or not 1 <= len(value) <= 32:
        raise ValueError('Observations require a list of 1 to 32 complete snapshots')
    result = {}
    for row in value:
        if (not isinstance(row, dict) or not _REQUIRED <= row.keys() or
                row.keys() - (_REQUIRED | _TEXT.keys() | _INTEGERS | _NUMBERS)):
            raise ValueError('Invalid observation fields')
        identity = row['id']
        if not isinstance(identity, str) or not _ID.fullmatch(identity) or identity in result:
            raise ValueError('Observations need distinct 32-digit lowercase hex IDs')
        for name in ('source', 'node'):
            if not isinstance(row[name], str) or not _KEY.fullmatch(row[name]):
                raise ValueError('Invalid observation ' + name)
        if type(row['revision']) is not int or not 0 <= row['revision'] <= 2**63 - 1:
            raise ValueError('Observation revision requires a nonnegative 64-bit integer')
        if not isinstance(row['status'], str) or row['status'] not in TERMINAL | {'queued', 'running'}:
            raise ValueError('Invalid observed job status')
        for key in row.keys() & _TEXT.keys():
            if not isinstance(row[key], str) or len(row[key]) > _TEXT[key] or not row[key].isprintable() and row[key]:
                raise ValueError('Invalid observation ' + key)
        for key in row.keys() & _INTEGERS:
            if type(row[key]) is not int or not 0 <= row[key] <= 2**31 - 1:
                raise ValueError('Invalid observation ' + key)
        for key in row.keys() & _NUMBERS:
            number = row[key]
            if number is None and key in ('started', 'finished'):
                continue
            if type(number) not in (int, float) or not 0 <= number <= 1e16:
                raise ValueError('Invalid observation ' + key)
        stamps = [row[key] for key in ('submitted', 'started', 'finished') if row.get(key) is not None]
        if stamps != sorted(stamps):
            raise ValueError('Observation timestamps are out of order')
        result[identity] = {key: val for key, val in row.items() if val is not None}
    return result


def terminal_observation(previous, result):
    """Accept the original worker's terminal receipt, never a missing job."""
    if (not isinstance(result, dict) or result.get('id') != previous['id'] or
            result.get('status') not in TERMINAL or result.get('submitted') != previous['submitted']):
        return None
    finished = result.get('finished')
    if type(finished) not in (int, float) or not 0 < finished < 9e12:
        return None
    row = {key: previous[key] for key in ('id', 'source', 'node', 'submitted')}
    timing = {key: result[key] for key in _TERMINAL_FIELDS if result.get(key) is not None}
    if 'cumulative_cpu_seconds' not in timing and result.get('total_cpu_seconds') is not None:
        timing['cumulative_cpu_seconds'] = result['total_cpu_seconds']
    row.update(timing, status=result['status'], revision=max(previous['revision']+1, int(finished*1_000_000)+2))
    return observation_batch([row])


class Observations:
    def __init__(self, path):
        self.path = path
        self.rows = json.loads(path.read_bytes()) if path.exists() else {}
        self.active = {identity for identity, row in self.rows.items() if row['status'] not in TERMINAL}
        self.cpu_samples = {}

    def upsert(self, batch, known_nodes, managed):
        """Called under the scheduler condition and per-ID submission locks."""
        updates, replies = {}, []
        for identity, row in batch.items():
            previous = self.rows.get(identity)
            if managed(identity):
                raise Conflict('Job ID already belongs to the scheduler')
            if previous and (previous['source'], previous['node']) != (row['source'], row['node']):
                raise Conflict('Job ID already belongs to another observation source or node')
            action = 'inserted' if previous is None else 'updated'
            if previous:
                row = {key: previous[key] for key in _METADATA if key in previous} | row
                revision = previous['revision']
                if row['revision'] == revision and row != previous:
                    raise Conflict('Observation revision already has different contents')
                if row['revision'] == revision:
                    action = 'unchanged'
                elif (row['revision'] < revision or
                      previous['status'] in TERMINAL and row['status'] != previous['status'] or
                      previous['status'] == 'running' and row['status'] == 'queued'):
                    row, action = previous, 'stale'
            if action in ('inserted', 'updated'):
                if previous is None and row['node'] not in known_nodes:
                    raise ValueError('Unknown observation node: ' + row['node'])
                updates[identity] = row
            replies.append({key: row[key] for key in ('id', 'revision', 'status')} | {'action': action})
        if updates:
            rows = self.rows | updates
            atomic_json(self.path, rows, private=True)
            self.rows = rows
            for identity, row in updates.items():
                if row['status'] in TERMINAL:
                    self.active.discard(identity)
                    self.cpu_samples.pop(identity, None)
                else:
                    self.active.add(identity)
        return {'observations': replies}

    def project(self, row, sample=None):
        value = {key: val for key, val in row.items() if key != 'node'}
        value.update(observe_only=True, assigned_node=row['node'], identity_scope='task')
        if not sample or row['status'] in TERMINAL or sample.get('status') != 'running':
            return value
        value['status'] = 'running'
        for key in ('cpus', 'active_cpus', 'max_cpus', 'memory_mb', 'rss_mb', 'memory_phase'):
            if sample.get(key) is not None:
                value[key] = sample[key]
        runtime = sample.get('runtime') or {}
        stamp, cpu = runtime.get('sampled_at'), runtime.get('cpu_seconds')
        if type(stamp) in (int, float) and type(cpu) in (int, float) and 0 <= cpu and 0 < stamp:
            old_stamp, old_cpu, measured = self.cpu_samples.get(row['id'], (0, 0, None))
            if stamp > old_stamp:
                measured = round((cpu - old_cpu) / (stamp - old_stamp), 3) if old_stamp and cpu >= old_cpu else None
                self.cpu_samples[row['id']] = stamp, cpu, measured
            if stamp >= value.get('cpu_sampled_at', 0):
                value.update(cpu_sampled_at=stamp, cpu_seconds=cpu)
                if measured is not None and stamp >= old_stamp:
                    value['measured_cpus'] = measured
        progress = progress_fields(sample)
        for key in ('progress', 'completed', 'total', 'unit'):
            if key in progress:
                value[key] = progress[key]
        if 'phase' not in value:
            provided = sample.get('progress')
            phase = sample.get('phase') or (provided.get('phase') if isinstance(provided, dict) else None)
            if isinstance(phase, str) and phase:
                value['phase'] = phase
        return value
