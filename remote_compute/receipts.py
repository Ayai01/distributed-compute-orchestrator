"""Isolate a damaged receipt without losing its task identity or inputs."""

import hashlib
import json
from pathlib import Path
import socket
import time

from .bundle import TERMINAL, atomic_json


def _object(path):
    value = json.loads(path.read_text(encoding='utf-8-sig'))
    if not isinstance(value, dict):
        raise ValueError('JSON object required: ' + str(path))
    return value


def _receipt(path, identity):
    value = _object(path)
    if value.get('id') != identity or value.get('status') not in TERMINAL | {'queued', 'running'}:
        raise ValueError('Invalid receipt identity/status: ' + str(path))
    return value


def read_receipt(path, *, assets=()):
    """Return an original receipt or a durable, explicitly incomplete failure.

    An unreadable execution record is not a retry instruction. Its original
    ID remains reserved, outputs remain collectible, and unmeasured costs
    remain unknown. Only the owning caller decides what work to resume.
    """
    path = Path(path)
    identity = path.parent.name
    try:
        return _receipt(path, identity)
    except (ValueError, UnicodeError, FileNotFoundError) as error:
        reason = str(error)
    path.parent.mkdir(parents=True, exist_ok=True)
    recovered = None
    source = None
    candidates = list(path.parent.glob(path.name + '.*.tmp'))
    candidates.append(path.with_suffix('.tmp'))  # Original publisher, before this repair.
    for pending in sorted((p for p in candidates if p.is_file()), key=lambda p: p.stat().st_mtime_ns, reverse=True):
        try:
            recovered = _receipt(pending, identity)
        except (ValueError, UnicodeError, OSError):
            continue
        source = pending.name
        break
    backup = None
    digest = None
    if path.is_file():
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        backup = path.with_name('result.corrupt-' + str(time.time_ns()) + '.json')
        path.replace(backup)
    details = {'reason': reason, 'original_id': identity, 'recovered_at': time.time(),
               'damaged_file': backup.name if backup else None, 'damaged_sha256': digest,
               'recovered_from': source, 'inputs_retained': True, 'automatic_retry': False}
    if recovered is None:
        try:
            task = _object(path.parent / 'task.json')
        except (ValueError, UnicodeError, OSError):
            task = {}
        try:
            admission = _object(path.parent / 'admission.json')
            if admission.get('id') != identity:
                admission = {}
        except (ValueError, UnicodeError, OSError):
            admission = {}
        recovered = {**admission, 'id': identity, 'status': 'failed',
                     'error': 'Job receipt damaged during worker shutdown; original inputs and outputs retained',
                     'error_kind': 'receipt_corrupt', 'cost_incomplete': True,
                     'cpu_seconds': None, 'elapsed': None, 'peak_rss_mb': None,
                     'started': None, 'finished': time.time(), 'steps': [],
                     'stdout': '', 'stderr': '', 'node': socket.gethostname(),
                     'request_sha256': admission.get('request_sha256'),
                     'assets': admission.get('assets') or task.get('assets') or
                               {'retained_' + str(i): value for i, value in enumerate(assets)},
                     'label': admission.get('label', task.get('label', '')),
                     'project': admission.get('project', task.get('project', '')),
                     'cpus': admission.get('initial_cpus', task.get('cpus', 1)),
                     'memory_mb': admission.get('memory_mb', task.get('memory_mb', 0)),
                     'total_steps': admission.get('total_steps', len(task.get('steps', [])))}
    elif recovered['status'] not in TERMINAL:
        # A temporary file can be an older live sample: retain the known cost
        # prefix, but do not pass it off as a completed or retryable execution.
        recovered.update(status='failed', finished=time.time(), cost_incomplete=True,
                         error_kind='receipt_corrupt', error='Recovered incomplete worker receipt; inputs and outputs retained')
    recovered['receipt_recovery'] = details
    atomic_json(path.parent / 'receipt-recovery.json', details)
    atomic_json(path, recovered)
    return recovered
