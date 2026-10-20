"""Small cache operations run in the owning project's interpreter."""

import hashlib
import importlib
import json
from pathlib import Path
import sys

from .bundle import pack


def execute(request, output):
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    action = request['action']
    if action == 'warm':
        return {'ready': True}
    root = Path(request['root']).resolve()
    if not root.is_dir():
        raise ValueError('Project root is unavailable: ' + str(root))
    project = request['project']
    if project not in ('aop', 'arh', 'bench', 'rgcn'):
        raise ValueError('Unknown cache project')
    sys.path.insert(0, str(root))
    if action.startswith('asset_'):
        from .cache_assets import groups
        bundles = groups(root, project)
        if action == 'asset_manifest':
            result = {}
            for name, group in bundles.items():
                raw = pack(group['files'])
                result[name] = dict(digest=hashlib.sha256(raw).hexdigest(),
                                    bytes=len(raw), requires=group['requires'])
            return result
        if action == 'asset_export':
            group = bundles[request['group']]
            raw = pack(group['files'])
            digest = hashlib.sha256(raw).hexdigest()
            if digest != request['digest']:
                raise ValueError('Asset changed during synchronization; run sync_cache again')
            (output / 'bundle.zip').write_bytes(raw)
            return dict(digest=digest, bytes=len(raw))
    adapter = importlib.import_module('.cache_' + project, __package__)
    if action == 'manifest':
        return adapter.manifest(root)
    if action == 'export':
        return adapter.export(root, request['keys'])
    if action == 'merge':
        return adapter.merge(root, request['records'])
    raise ValueError('Unknown cache operation: ' + action)


def main(path):
    path = Path(path)
    request = json.loads(path.read_text(encoding='utf-8'))
    output = path.parent / 'result'
    result = execute(request, output)
    (output / 'response.json').write_text(
        json.dumps(result, ensure_ascii=False, allow_nan=False), encoding='utf-8')
