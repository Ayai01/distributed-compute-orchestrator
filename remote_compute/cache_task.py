"""Small cache operations run in the owning project's interpreter."""

import json
import os
from pathlib import Path



def execute(request, output):
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    action = request['action']
    if action == 'warm':
        return {'ready': True}
    root = Path(request['root']).resolve()
    if not root.is_dir():
        raise ValueError('Project root is unavailable: ' + str(root))
    from .cache_assets import load_adapter
    source = Path(os.environ['DCO_CACHE_ADAPTER'])
    adapter = load_adapter(source, request['adapter'])
    if action == 'manifest':
        return adapter.manifest(root)
    if action == 'export':
        return adapter.export(root, request['keys'])
    if action == 'merge':
        producer = os.environ.get('DCO_CACHE_PRODUCER')
        options = {'producer_root': producer} if producer else {}
        return adapter.merge(root, request['records'], **options)
    raise ValueError('Unknown cache operation: ' + action)


def main(path):
    path = Path(path)
    request = json.loads(path.read_text(encoding='utf-8'))
    output = path.parent / 'result'
    result = execute(request, output)
    (output / 'response.json').write_text(
        json.dumps(result, ensure_ascii=False, allow_nan=False), encoding='utf-8')
