"""Load project-owned exchange adapters and package their immutable helpers."""

import importlib.util
import json
import os
from pathlib import Path
import re
import sys
import uuid

from .bundle import inside
from .policy import canonical_capabilities


def load_projects(path=None):
    path = Path(path or os.environ.get('DCO_PROJECTS_FILE') or
                Path(__file__).resolve().parents[1]/'.local/projects.json')
    if not path.is_file():
        return {}
    registered = json.loads(path.read_text(encoding='utf-8-sig'))['projects']
    result = {}
    for name, value in registered.items():
        if not re.fullmatch(r'[a-z][a-z0-9_-]*', name) or name == 'all':
            raise ValueError('Invalid cache project ID: ' + str(name))
        entry = dict(value)
        root = Path(entry['root']).expanduser().resolve()
        adapter = inside(root, entry['adapter'])
        if not adapter.is_file():
            raise ValueError('Project adapter is unavailable: ' + str(adapter))
        entry.update(root=str(root), requires=canonical_capabilities(entry.get('requires', ['python'])))
        result[name] = entry
    return result


def load_adapter(root, relative):
    path = inside(Path(root).resolve(), relative)
    spec = importlib.util.spec_from_file_location('_dco_cache_' + uuid.uuid4().hex, path)
    module = importlib.util.module_from_spec(spec)
    original = list(sys.path)
    sys.path.insert(0, str(Path(root).resolve()))
    try:
        spec.loader.exec_module(module)
    finally:
        sys.path[:] = original
    return module


def groups(root, adapter):
    """Preserve each producer's source and model boundaries."""
    original = list(sys.path)
    sys.path.insert(0, str(Path(root).resolve()))
    try:
        return adapter.groups(Path(root).resolve())
    finally:
        sys.path[:] = original
