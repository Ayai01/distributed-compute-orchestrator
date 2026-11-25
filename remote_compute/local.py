"""Private in-process worker transport, owned only by its hybrid scheduler."""

import zipfile
import json
from urllib.parse import unquote

from .client import Client, RemoteError, _check_cancelled
from .worker import Conflict
from .bundle import batch_status, progress
from .policy import validate_status_ids


class LocalClient(Client):
    def __init__(self, worker):
        super().__init__('local://' + worker.health()['node'])
        self.worker = worker

    def cancel(self, identity):
        return self.worker.cancel(identity)

    def cancel_many(self, identities):
        return self.worker.cancel_many(identities)

    def cancel_all(self):
        return self.worker.cancel_all()

    def request(self, path, data=None, *, method=None, binary=False, brief=False, extra_headers=None, cancel=None):
        _check_cancelled(cancel)
        worker = self.worker
        method = method or ('POST' if data is not None else 'GET')
        parts = path.strip('/').split('/')
        worker.activity.access('local')
        try:
            if parts == ['health']:
                return worker.health(brief=brief,sample=(extra_headers or {}).get("X-DCO-Sample")=="1")
            if parts == ['control']:
                result = worker.control(data)
                worker.activity.notify('Control ' + str(data), 'local')
                return result
            if parts == ['clean'] and method == 'POST':
                return worker.clean()
            if parts == ['jobs']:
                with worker.condition:
                    return [(progress(worker.get(identity)) if brief else worker.get(identity)) for identity in worker.jobs]
            if parts == ['jobs','status'] and method == 'POST':
                values={}
                for identity in validate_status_ids(data.get('ids')):
                    try:result=worker.get(identity)
                    except (KeyError,FileNotFoundError):
                        result=dict(id=identity,status='unknown',status_code=404)
                    values[identity]=batch_status(result,compact=data.get('compact') is True)
                return values
            if len(parts) == 2 and parts[0] == 'assets':
                job_id = (extra_headers or {}).get('X-DCO-Asset-Job')
                return worker.asset(parts[1], data, **({'job_id':job_id} if job_id else {}))
            if len(parts) == 3 and parts[0] == 'assets' and parts[2] == 'upload' and method in {'GET', 'POST'}:
                if not worker.shared_assets:
                    raise KeyError('Resumable asset uploads are unavailable')
                headers = extra_headers or {}
                body = json.dumps(data).encode() if isinstance(data, dict) else data
                return worker.shared_assets.uploads.receive(parts[1], body, headers.get('Content-Range'),
                    encoding=headers.get('X-DCO-Chunk-Encoding'), accept_encoding=headers.get('X-DCO-Accept-Chunk-Encoding'))
            if len(parts) >= 2 and parts[0] == 'jobs':
                identity = parts[1]
                if len(parts) == 2:
                    if method == 'POST':
                        callback=json.loads((extra_headers or {}).get('X-DCO-Callback','{}'))
                        result = worker.submit(identity, data,callback=callback)
                        worker.activity.submitted(result, 'local')
                        return result
                    if method == 'DELETE':
                        return worker.delete(identity)
                    result = worker.get(identity)
                    return progress(result) if brief else result
                if parts[2:] == ['cancel']:
                    return worker.cancel(identity)
                if parts[2:] == ['asset-handoff'] and method in {'POST','DELETE'}:
                    return worker.asset_handoff(identity,data['assets'] if method=='POST' else None)
                if parts[2:] == ['resize']:
                    options={k:data[k] for k in ('active_cpus','memory_mb','base_memory_mb','memory_per_cpu_mb','memory_limit_mb','memory_budget_mb') if k in data}
                    return worker.resize(identity,data['cpus'],**options)
                if parts[2:] == ['logs']:
                    return worker.logs(identity)
                if parts[2:] == ['artifacts']:
                    return worker.artifacts(identity)
                if len(parts) >= 4 and parts[2] == 'files':
                    name = unquote('/'.join(parts[3:]))
                    result = worker.job_file(identity, name, data if method == 'POST' else None)
                    if method == 'POST':
                        worker.activity.notify('Job file ' + name + ' [' + identity[:8] + ']', 'local')
                    return result
            raise ValueError('Private worker route unavailable: ' + path)
        except (KeyError, Conflict, FileNotFoundError, ValueError, OSError, zipfile.BadZipFile) as exc:
            error = RemoteError(str(exc))
            error.status_code = 409 if isinstance(exc, Conflict) else 404 if isinstance(exc, (KeyError,FileNotFoundError)) else 400
            raise error from None
