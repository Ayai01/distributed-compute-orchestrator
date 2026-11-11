"""Small durable producer references; provider placement stays center-owned."""
import json
import re
import time
from pathlib import Path
from .bundle import atomic_json


def reference_id(value):
    if not isinstance(value,str)or not re.fullmatch('[0-9a-f]{32}',value):
        raise ValueError('cache_ref requires a registered producer job ID')
    return value


def read(root,identity):
    identity=reference_id(identity)
    path=Path(root)/'cache-references'/(identity+'.json')
    try:record=json.loads(path.read_bytes())
    except FileNotFoundError:raise ValueError('Cache reference is unavailable; register the successful producer before deleting it')from None
    if (record.get('id')!=identity or record.get('producer_job')!=identity
            or not isinstance(record.get('project'),str)or not record['project']
            or not isinstance(record.get('provider'),str)or not record['provider']):
        raise ValueError('Invalid retained cache reference')
    return record


def public(record):
    return {k:record.get(k)for k in ('id','producer_job','project','content_id')}


def register(root,identity,*,project,provider,content_id=None,receipt_sha256=None):
    """Caller verifies the successful receipt and its actual producing node."""
    identity=reference_id(identity)
    if not isinstance(project,str)or not project or not isinstance(provider,str)or not provider:
        raise ValueError('A successful named-project producer is required')
    if content_id is not None and(not isinstance(content_id,str)or not re.fullmatch('[0-9a-f]{64}',content_id)):
        raise ValueError('content_id requires a content or model SHA256')
    record=dict(id=identity,producer_job=identity,project=project,provider=provider,
                content_id=content_id,created=time.time())
    if receipt_sha256 is not None:record['receipt_sha256']=receipt_sha256
    folder=Path(root)/'cache-references';folder.mkdir(exist_ok=True)
    path=folder/(identity+'.json')
    if path.exists():
        previous=read(root,identity)
        if any(previous.get(k)!=record[k]for k in ('project','provider','content_id')):
            raise ValueError('A producer cache reference is immutable')
        return previous
    atomic_json(path,record)
    return record
