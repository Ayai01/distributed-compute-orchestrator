"""Task-owned work reports and read-only diagnosis on existing worker samples."""
import json
import math
import os
from pathlib import Path
import tempfile
import threading
import time

SCHEMA = 'dco.progress.v1'
MAX_BYTES = 16384
_UNSET = object()
_lock = threading.Lock()


def _number(value):
    return type(value) in (int, float) and math.isfinite(value) and value >= 0


def work_fraction(job):
    """Return a reported work fraction, keeping command counts separate."""
    p = job.get('progress')
    typed = isinstance(p, dict) and p.get('schema') == SCHEMA
    values = p if typed else {**job, **p} if isinstance(p, dict) else job
    for numerator, denominator in (('completed','total'), ('current','target'), ('nfe','budget')):
        done, total = values.get(numerator), values.get(denominator)
        if not typed and numerator == 'completed' and job.get('step_progress') == dict(completed=done, total=total):
            continue
        if _number(done) and _number(total) and 0 < total and done <= total:
            return done / total
    percent = str(values.get('unit') or '').lower() in ('%', 'percent', 'percentage')
    if isinstance(p, dict):
        for key, scale in (('ratio',1), ('fraction',1), ('percent',100)):
            if _number(p.get(key)) and p[key] <= scale:
                return p[key] / scale
        if percent and _number(p.get('completed')) and p['completed'] <= 100 and p.get('total') is None:
            return p['completed'] / 100
    if _number(p) and p <= 100:
        return p / 100 if percent or p > 1 else p
    return None


def _text(value, limit):
    if not isinstance(value, str) or len(value) > limit or any(ord(c) < 32 for c in value):
        raise ValueError('Progress text must be bounded printable text')
    return value


def normalize(value):
    """Keep a bounded, typed snapshot, without importing arbitrary business data."""
    if not isinstance(value, dict):raise ValueError('Progress must be an object')
    out = dict(schema=SCHEMA)
    if value.get('source') in ('business','executor'):out['source'] = value['source']
    for key, limit in (('phase', 80), ('unit', 24)):
        if value.get(key) is not None:out[key] = _text(value[key], limit)
    for key in ('completed', 'total', 'reported_at', 'last_heartbeat', 'last_advanced', 'stale_after'):
        if value.get(key) is not None:
            if not _number(value[key]):raise ValueError('Invalid progress '+key)
            out[key] = value[key]
    if 'stale_after' in out and out['stale_after']<=0:raise ValueError('stale_after must be positive')
    if out.get('total') is not None:
        if out['total'] <= 0 or out.get('completed') is None or out['completed'] > out['total']:
            raise ValueError('A fixed total requires 0 <= completed <= total and total > 0')
        out['ratio'] = out['completed']/out['total']
    for group in ('counters','metrics'):
        counters = value.get(group, {})
        if not isinstance(counters, dict) or len(counters) > 16:raise ValueError('Expected at most 16 '+group)
        out[group] = {}
        for key, count in counters.items():
            if not _number(count):raise ValueError('Progress counters and metrics must be nonnegative finite numbers')
            out[group][_text(key, 48)] = count
    waiting = value.get('waiting')
    if waiting:
        if isinstance(waiting, str):waiting = dict(kind='business', reason=waiting)
        if not isinstance(waiting, dict):raise ValueError('Invalid waiting report')
        out['waiting'] = {key:_text(waiting[key], limit) for key,limit in (('kind',32),('reason',160)) if key in waiting}
    checkpoint = value.get('checkpoint')
    if checkpoint is not None:
        if not isinstance(checkpoint, dict):raise ValueError('Invalid checkpoint summary')
        item = {}
        for key in ('interval_seconds', 'saved_at'):
            if key in checkpoint:
                if not _number(checkpoint[key]):raise ValueError('Invalid checkpoint '+key)
                item[key] = checkpoint[key]
        if 'available' in checkpoint:
            if type(checkpoint['available']) is not bool:raise ValueError('Checkpoint available must be boolean')
            item['available'] = checkpoint['available']
        if 'resume' in checkpoint:item['resume'] = _text(checkpoint['resume'],160)
        out['checkpoint'] = item
    return out


def report_progress(*, phase=_UNSET, completed=_UNSET, total=_UNSET, unit=_UNSET,
                    counters=_UNSET, metrics=_UNSET, waiting=_UNSET, checkpoint=_UNSET, stale_after=_UNSET):
    """Publish absolute work counters from task code; omitted fields keep their values.

    A call with no arguments is a business heartbeat. Set total=None for unknown
    endpoints and waiting=None to clear a wait. No RPC or monitoring thread starts.
    Returns False outside a DCO task or if the optional report cannot be written.
    """
    updates = dict(phase=phase,completed=completed,total=total,unit=unit,counters=counters,metrics=metrics,
                   waiting=waiting,checkpoint=checkpoint,stale_after=stale_after)
    return _publish({k:v for k,v in updates.items() if v is not _UNSET},source='business')


def _publish(updates, *, source):
    root = os.environ.get('REMOTE_COMPUTE_JOB_DIR')
    if not root:return False
    path = Path(root)/'.dco-progress.json'
    temporary = None
    with _lock:
        try:
            try:
                with path.open('rb') as stream:previous = json.loads(stream.read(MAX_BYTES+1))
            except (OSError,ValueError):previous = {}
            if not isinstance(previous,dict):previous = {}
            if source=='executor' and previous.get('source')=='business':return False
            if previous.get('source')!=source:previous = {}
            value = normalize({**previous, **updates, 'source':source,'reported_at':time.time()})
            raw = json.dumps(value,ensure_ascii=False,allow_nan=False,separators=(',',':')).encode('utf8')
            if len(raw)>MAX_BYTES:raise ValueError('Progress snapshot exceeds byte budget')
            with tempfile.NamedTemporaryFile(prefix='.dco-progress-',dir=str(path.parent),delete=False) as stream:
                temporary = Path(stream.name);stream.write(raw)
            os.replace(str(temporary),str(path));temporary=None
            return True
        except OSError:return False
        finally:
            if temporary is not None:
                try:temporary.unlink()
                except OSError:pass


def report_items(completed, total):
    """Executor fallback; a business report always owns the task's primary view."""
    return _publish(dict(phase='map',completed=completed,total=total,unit='items'),source='executor') if total else False


def sample(job, cpu_seconds, *, now=None, force=False):
    """Read one owned file per second, outside the worker's global state lock."""
    now = time.time() if now is None else now
    if not force and now-job.get('_progress_sampled',0)<1:return {}
    job['_progress_sampled'] = now
    old_runtime = job['result'].get('runtime',{})
    advanced = cpu_seconds > old_runtime.get('cpu_seconds',0)+.01
    runtime = dict(sampled_at=now,process_alive=True,cpu_seconds=cpu_seconds,
                   last_cpu_advanced=now if advanced else old_runtime.get('last_cpu_advanced'))
    updates = dict(runtime=runtime)
    path = job['directory']/'.dco-progress.json'
    try:
        stat = path.stat();signature = (stat.st_ino,stat.st_mtime_ns,stat.st_size)
        if signature != job.get('_progress_signature'):
            with path.open('rb') as stream:raw = stream.read(MAX_BYTES+1)
            if len(raw)>MAX_BYTES:raise ValueError('Progress snapshot exceeds byte budget')
            value = normalize(json.loads(raw))
            previous = job['result'].get('progress') or {}
            if not isinstance(previous,dict):previous = {}
            moved = (not previous or value.get('phase') != previous.get('phase') or
                     value.get('completed',0)>previous.get('completed',0) or
                     any(v>previous.get('counters',{}).get(k,0) for k,v in value['counters'].items()))
            value.update(last_heartbeat=min(now,value.get('reported_at',now)),
                         last_advanced=now if moved else previous.get('last_advanced',now))
            updates.update(progress=value,progress_error=None)
            job['_progress_signature'] = signature
    except FileNotFoundError:pass
    except (OSError,ValueError,TypeError):updates['progress_error'] = 'invalid_or_unreadable_progress'
    return updates


def diagnose(job, *, now=None, offline=False):
    """Explain observations; soft thresholds never cancel or resubmit work."""
    now = time.time() if now is None else now
    state = job.get('status','running');p = job.get('progress') or {}
    p = p if isinstance(p,dict) and p.get('schema')==SCHEMA else {}
    runtime = job.get('runtime') or {}
    progress_error=job.get('progress_error',(job.get('diagnostic') or {}).get('progress_error'))
    age = lambda key: max(0,now-p[key]) if _number(p.get(key)) else None
    heartbeat,advance = age('last_heartbeat'),age('last_advanced')
    if p.get('source')=='executor':heartbeat = None
    threshold = p.get('stale_after',120)
    sample_age = max(0,now-runtime['sampled_at']) if _number(runtime.get('sampled_at')) else None
    cpu_age = max(0,now-runtime['last_cpu_advanced']) if _number(runtime.get('last_cpu_advanced')) else None
    report_stale = heartbeat>threshold if heartbeat is not None else None
    progress_stale = advance>threshold if advance is not None else None
    cpu = _number(runtime.get('last_cpu_advanced')) and now-runtime['last_cpu_advanced']<=15
    code,level = 'running','info'
    if state in ('failed','timed_out','interrupted'):code,level = state,'error'
    elif state in ('succeeded','cancelled'):code = state
    elif state in ('queued','pending'):code = 'queued'
    elif offline:code,level = 'worker_unreachable','warning'
    elif state=='dispatching' and not job.get('worker_status'):
        code = job.get('worker_receive_stage') or job.get('dispatch_stage') or 'preparing'
    elif job.get('worker_status') == 'queued':code = 'worker_accepted'
    elif job.get('preparation_stage') and job['preparation_stage'] != 'complete':
        code = ('resource_wait' if job['preparation_stage'] in ('host_memory_pressure','resource_grant','memory_calibration')
                else 'worker_preparing')
    elif runtime.get('process_alive') is False and runtime.get('returncode') is not None:
        code = 'finishing'
    elif runtime.get('process_alive') is False and (job.get('resource_request') or
            runtime.get('cpu_seconds')==0 or job.get('active_cpus',job.get('cpus'))==0):
        code = 'resource_wait' if job.get('resource_request') else 'preparing'
    elif runtime.get('process_alive') is False:code,level = 'process_lost','warning'
    elif progress_error:code,level = 'progress_invalid','warning'
    elif sample_age is not None and sample_age>max(15,threshold):code,level = 'worker_sample_stale','warning'
    elif job.get('resource_request') or p.get('waiting',{}).get('kind') in ('resource','cpu','memory'):code = 'resource_wait'
    elif p.get('waiting'):code = 'waiting'
    elif report_stale or progress_stale:
        code,level = ('cpu_active','info') if cpu else ('stalled','warning') if cpu_age is not None else ('running','info')
    elif p:code = 'progressing'
    elif cpu:code = 'cpu_active'
    reason=p.get('waiting',{}).get('reason')
    if code=='cpu_active' and (report_stale or progress_stale):reason='暂未报告新进度'
    elif code=='stalled':reason='CPU与业务进度暂未推进'
    elif code=='running' and (report_stale or progress_stale):reason='暂未报告新进度，CPU活动未上报'
    elif code=='resource_wait' and not reason:reason='等待资源授权'
    return dict(code=code,level=level,heartbeat_age_seconds=heartbeat,advanced_age_seconds=advance,
                stale_after_seconds=threshold,cpu_active=bool(cpu),
                worker_sample_age_seconds=sample_age,cpu_advanced_age_seconds=cpu_age,
                process_alive=runtime.get('process_alive'),business_report_stale=report_stale,
                progress_stale=progress_stale,reason=reason,progress_error=progress_error)


LABELS = dict(queued='排队',preparing='准备',finishing='收尾',resource_wait='等资源',waiting='等待',
              bundle='组包',assets='备资产',pacing='等派发',awaiting_worker='等接单',
              unpacking='解包',worker_accepted='已接单',worker_preparing='待开算',
              running='运行',progressing='推进',cpu_active='计算中',stalled='进度停滞',
              heartbeat_lost='业务报告静默',worker_unreachable='连接中断',worker_sample_stale='采样陈旧',
              process_lost='进程丢失',failed='失败',timed_out='超时',interrupted='中断',
              progress_invalid='进度无效',
              succeeded='完成',cancelled='取消')
