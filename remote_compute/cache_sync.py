"""Manual, incremental exchange through ordinary DCO jobs and asset hashes."""

import hashlib
import json
import threading
import uuid

from .bundle import TERMINAL
from .client import RemoteError
from .policy import canonical_capabilities
from .cache_assets import load_projects, load_adapter, groups
from .bundle import pack

_SCRIPT = """from pathlib import Path
from remote_compute.cache_task import main
main(Path(__file__).with_name('request.json'))
"""


class _Stopped(Exception):
    pass


class _RPC:
    def __init__(self, client, on_submit, stop_event, projects):
        self.client = client
        self.projects = projects
        self.on_submit = on_submit
        self.stop = stop_event or threading.Event()
        self.pending = {}
        self.collected = 0
        self.input_bytes = self.output_bytes = 0

    def check(self):
        if self.stop.is_set():
            raise _Stopped()

    def retry_read(self, operation):
        for attempt in range(3):
            self.check()
            try:
                return operation()
            except RemoteError as error:
                status = getattr(error, 'status_code', None)
                # These reads follow our own submitted ID. Old services can
                # briefly hide it while publishing acceptance or the receipt.
                if attempt == 2 or status is not None and status < 500 and status != 404:
                    raise
                self.stop.wait(attempt + 1)

    def call(self, node, project, action, **arguments):
        self.check()
        assets = arguments.pop('_assets', {})
        registration = self.projects[project]
        assets['cache_adapter'] = registration['_adapter_asset']
        runtime = registration.get('runtime', 'python')
        requires = arguments.pop('_requires', registration['requires'])
        request = dict(project=project, adapter=registration['adapter'], action=action,
                       root=node.get('shared_paths', {}).get(project), **arguments)
        data = json.dumps(request, ensure_ascii=False, allow_nan=False).encode('utf-8')
        identity = uuid.uuid4().hex
        self.pending[identity] = dict(node=node['name'], project=project, action=action)
        machine = self.client.machine(node['name'], cwd=node['tool_root'])
        try:
            machine.python(
                _SCRIPT, tool='python' if action == 'warm' else runtime,
                files={'remote_op/request.json': data},
                outputs=['remote_op/result/*'], wait=False, job_id=identity,
                on_submit=self.on_submit, assets=assets, requires=requires,
                env=dict({'DCO_CACHE_ADAPTER': '${asset:cache_adapter}'},
                         **({'DCO_CACHE_PRODUCER': '${asset:cache_producer}'}
                            if 'cache_producer' in assets else {})),
                max_cpus=1, memory_mb=512, priority=-100, timeout=900,
                retries=0, project='DCO-cache', label='sync_cache/' + project + '/' + action)
        except RemoteError as error:
            status = getattr(error, 'status_code', None)
            if status in (400, 401, 403, 413, 422):
                self.pending.pop(identity, None)
                raise
            # The POST reply can disappear after acceptance. Query the chosen
            # ID; never start another copy of a merge or inventory operation.
            self.retry_read(lambda: self.client.status(identity, brief=True))
            if self.on_submit:
                self.on_submit(self.client, identity)
        self.input_bytes += len(data)
        while True:
            self.check()
            if self.retry_read(lambda: self.client.status(identity, brief=True))['status'] in TERMINAL:
                break
            self.stop.wait(1.0)
        result = self.retry_read(lambda: self.client.fetch(identity, delete=False))
        files = result.get('files', {})
        self.output_bytes += sum(len(value) for value in files.values())
        if result['status'] != 'succeeded':
            error = result.get('stderr') or result.get('error') or result['status']
            self.client.delete(identity)
            self.pending.pop(identity, None)
            self.collected += 1
            raise RemoteError('Cache operation on ' + node['name'] + ': ' + error)
        response = json.loads(files['remote_op/result/response.json'])
        self.client.delete(identity)
        self.pending.pop(identity, None)
        self.collected += 1
        return response

    def collect_pending(self):
        # Other projects can finish while an endpoint recovers. Revisit each
        # original ID once before returning; persistent outages remain explicit.
        for identity in list(self.pending):
            try:
                state = self.client.status(identity, brief=True)['status']
                if state not in TERMINAL:
                    self.pending[identity]['status'] = state
                    continue
                result = self.client.fetch(identity, delete=False)
                if result['status'] == 'succeeded':
                    json.loads(result['files']['remote_op/result/response.json'])
                self.client.delete(identity)
                self.pending.pop(identity, None)
                self.collected += 1
            except (RemoteError, OSError, ValueError, KeyError):
                self.pending[identity]['status'] = 'awaiting_receipt'

    def cancel_owned(self):
        # Failed retrievals keep their original IDs. Only this invocation's
        # unfinished maintenance jobs are cancelled when its caller stops.
        for identity in list(self.pending):
            try:
                status = self.client.status(identity, brief=True)['status']
                if status not in TERMINAL:
                    self.client.cancel(identity)
                for _ in range(5):
                    if self.client.status(identity, brief=True)['status'] in TERMINAL:
                        self.client.fetch(identity, delete=False)
                        self.client.delete(identity)
                        self.pending.pop(identity, None)
                        self.collected += 1
                        break
                    # The stop event is already set: use a fresh bounded wait.
                    threading.Event().wait(0.2)
            except RemoteError:
                pass


def _chunks(keys, manifest):
    batch, size = [], 0
    for key in sorted(keys):
        amount = manifest[key].get('bytes', 0)
        if batch and (len(batch) >= 1024 or size + amount > 1024 * 1024):
            yield batch
            batch, size = [], 0
        batch.append(key)
        size += amount
    if batch:
        yield batch


def _research(rpc, project, nodes, producer_asset=None):
    result = dict(consumers=[node['name'] for node in nodes], added=0, updated=0,
                  unchanged=0, conflicts=0, rejected=0, exported_records=0,
                  errors=[])
    if len(nodes) < 2:
        result.update(status='skipped', reason='Fewer than two registered project consumers')
        return result
    inventories, available = {}, []
    for node in nodes:
        try:
            inventories[node['name']] = rpc.call(node, project, 'manifest')
            available.append(node)
        except (RemoteError, OSError, ValueError, KeyError) as error:
            result['errors'].append(dict(node=node['name'], action='manifest', error=str(error)))
    blocked_targets = set()
    for source in available:
        inventory = inventories[source['name']]
        targets = [node for node in available if node is not source and node['name'] not in blocked_targets]
        keys = [key for key, value in inventory.items() if any(
            inventories[target['name']].get(key, {}).get('digest') != value['digest']
            for target in targets)]
        for keys in _chunks(keys, inventory):
            rpc.check()
            targets = [node for node in targets if node['name'] not in blocked_targets]
            if not targets:
                break
            keys = [key for key in keys if any(
                inventories[target['name']].get(key, {}).get('digest') != inventory[key]['digest']
                for target in targets)]
            if not keys:
                continue
            try:
                records = rpc.call(source, project, 'export', keys=keys)
                # Export may observe new writes after inventory. Its validated
                # digest is authoritative, and vanished rows wait for next run.
                if set(keys) != set(records):
                    result['errors'].append(dict(node=source['name'], action='export',
                                                 error='Source changed during synchronization'))
                result['exported_records'] += len(records)
                for key, record in records.items():
                    inventory[key] = dict(digest=record['digest'],
                                          bytes=inventory.get(key, {}).get('bytes', 0))
            except (RemoteError, OSError, ValueError, KeyError) as error:
                result['errors'].append(dict(node=source['name'], action='export', error=str(error)))
                break
            for target in targets:
                wanted = {key: record for key, record in records.items()
                          if inventories[target['name']].get(key, {}).get('digest') != record['digest']}
                if not wanted:
                    continue
                try:
                    options = {'_assets': {'cache_producer': producer_asset}} if producer_asset else {}
                    merged = rpc.call(target, project, 'merge', records=wanted, **options)
                    for field in ('added', 'updated', 'unchanged', 'conflicts', 'rejected'):
                        result[field] += merged.get(field, 0)
                    for key, digest in merged.get('digests', {}).items():
                        inventories[target['name']][key] = dict(digest=digest,
                            bytes=inventory.get(key, {}).get('bytes', 0))
                except (RemoteError, OSError, ValueError, KeyError) as error:
                    blocked_targets.add(target['name'])
                    result['errors'].append(dict(node=target['name'], action='merge', error=str(error)))
    result['status'] = 'partial' if result['errors'] or result['conflicts'] or result['rejected'] else 'succeeded'
    return result


def _assets(rpc, project, sources, nodes):
    result = dict(groups=0, uploaded_bytes=0, reused_groups=0, warmed=[], identities={},
                  errors=[])
    registration = rpc.projects[project]
    try:
        adapter = load_adapter(registration['root'], registration['adapter'])
        bundles = groups(registration['root'], adapter)
    except (OSError, ValueError, KeyError, ImportError) as error:
        result.update(status='partial', errors=[dict(action='asset_manifest', error=str(error))])
        return result
    present = {}
    for name, group in sorted(bundles.items()):
        rpc.check()
        try:
            data = pack(group['files'])
            digest = hashlib.sha256(data).hexdigest()
            if not rpc.client.request('/assets/' + digest)['exists']:
                rpc.client.request('/assets/' + digest, data)
                result['uploaded_bytes'] += len(data)
            else:
                result['reused_groups'] += 1
            present[name] = dict(digest=digest, requires=group['requires'])
            result['identities'][name] = digest
        except (RemoteError, OSError, ValueError, KeyError) as error:
            result['errors'].append(dict(group=name, error=str(error)))
    result['groups'] = len(bundles)
    for node in nodes:
        matching = {name: group for name, group in present.items()
                    if set(canonical_capabilities(group['requires'])).issubset(
                        canonical_capabilities(node.get('tags', ())))}
        if not matching:
            continue
        try:
            rpc.call(node, project, 'warm',
                     _assets={'cache' + str(i): digest for i, digest in
                              enumerate(sorted({group['digest'] for group in matching.values()}))},
                     _requires=sorted(set(['python'] + [tag for group in matching.values()
                                                        for tag in group['requires']])))
            result['warmed'].append(dict(node=node['name'], groups=len(matching)))
        except (RemoteError, OSError, ValueError, KeyError) as error:
            result['errors'].append(dict(node=node['name'], action='warm', error=str(error)))
    result['status'] = 'partial' if result['errors'] else 'succeeded'
    return result


def sync_cache(client, project='all', *, kind='all', on_submit=None, stop_event=None):
    """Synchronize both cache kinds, or one, only when explicitly invoked."""
    registered = load_projects()
    project = project.lower()
    if project != 'all' and project not in registered:
        raise ValueError('Project is not registered in .local/projects.json: ' + project)
    if kind not in ('all', 'research', 'assets'):
        raise ValueError('Expected kind all/research/assets')
    projects = registered if project == 'all' else (project,)
    rpc = _RPC(client, on_submit, stop_event, registered)
    report = dict(status='succeeded', projects={}, unavailable=[])
    try:
        snapshot = client.nodes()['nodes']
        nodes = []
        for node in snapshot:
            if not node.get('enabled', True):
                continue
            if node.get('error') or node.get('paused'):
                report['unavailable'].append(dict(node=node['name'],
                    reason=node.get('error') or 'paused'))
            else:
                nodes.append(node)
        nodes.sort(key=lambda node: (not node.get('url', '').startswith('local://'), node['name']))
        for name in projects:
            rpc.check()
            registration = registered[name]
            adapter = load_adapter(registration['root'], registration['adapter'])
            registration['_adapter_asset'] = client.sync_files(adapter.source_files(registration['root']))
            sources = [node for node in nodes if node.get('shared_paths', {}).get(name)
                       and set(registration['requires']).issubset(canonical_capabilities(node.get('tags', ())))]
            item = report['projects'][name] = {}
            producer = None
            if registration.get('producer_group') and len(sources) > 1 and kind in ('all', 'research'):
                # Cache roots keep data; producer code comes from its ordinary
                # immutable source asset.
                prepared = _assets(rpc, name, sources, nodes if kind == 'all' else [])
                item['assets' if kind == 'all' else 'producer_assets'] = prepared
                producer = prepared['identities'].get(registration['producer_group'])
                if not producer:
                    item['research'] = dict(status='partial', errors=[dict(
                        action='producer_asset', error='Registered producer group is unavailable')])
                    continue
            if kind in ('all', 'research'):
                item['research'] = _research(rpc, name, sources, producer)
            if kind in ('all', 'assets') and 'assets' not in item:
                item['assets'] = _assets(rpc, name, sources, nodes)
        if report['unavailable'] or any(value['status'] == 'partial'
                for item in report['projects'].values() for value in item.values()):
            report['status'] = 'partial'
        rpc.collect_pending()
    except (_Stopped, KeyboardInterrupt):
        rpc.cancel_owned()
        report.update(status='partial', stopped=True)
    finally:
        report.update(pending_jobs=rpc.pending, collected_jobs=rpc.collected,
                      rpc_input_bytes=rpc.input_bytes, rpc_output_bytes=rpc.output_bytes)
    return report
