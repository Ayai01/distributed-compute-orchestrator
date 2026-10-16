"""Private in-process worker transport, owned only by its hybrid scheduler."""

import zipfile
from urllib.parse import unquote

from .client import Client, RemoteError
from .worker import Conflict
from .bundle import progress


class LocalClient(Client):
    def __init__(self, worker):
        super().__init__('local://' + worker.health()['node'])
        self.worker = worker

    def request(self, path, data=None, *, method=None, binary=False, brief=False):
        worker = self.worker
        method = method or ('POST' if data is not None else 'GET')
        parts = path.strip('/').split('/')
        worker.activity.access('local')
        try:
            if parts == ['health']:
                return worker.health()
            if parts == ['control']:
                result = worker.control(data)
                worker.activity.notify('Control ' + str(data), 'local')
                return result
            if parts == ['jobs']:
                with worker.condition:
                    return [(progress(worker.get(identity)) if brief else worker.get(identity)) for identity in worker.jobs]
            if len(parts) == 2 and parts[0] == 'assets':
                return worker.asset(parts[1], data)
            if len(parts) >= 2 and parts[0] == 'jobs':
                identity = parts[1]
                if len(parts) == 2:
                    if method == 'POST':
                        result = worker.submit(identity, data)
                        worker.activity.submitted(result, worker.jobs[identity].get('task', {}), 'local')
                        return result
                    if method == 'DELETE':
                        return worker.delete(identity)
                    result = worker.get(identity)
                    return progress(result) if brief else result
                if parts[2:] == ['cancel']:
                    return worker.cancel(identity)
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
