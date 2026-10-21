"""Small, shared admission policy for project-facing DCO requests.

The scheduler owns placement.  Projects express work in terms of resource
needs and semantic capabilities; these bounds keep optional scheduling hints
from becoming a second placement API.
"""

import math
import re

MAX_RESOURCE_SHARE = 16.0
MAX_QUEUE_RATIO = 4.0
MAX_SINGLE_THREAD_WEIGHT = 8.0
MIN_PRIORITY = -1000
MAX_PRIORITY = 1000
MAX_REQUIREMENTS = 32
MAX_REQUIREMENT_LENGTH = 64
MAX_TOOL_CONSTRAINTS = 32
MAX_ACCELERATORS = 16
MAX_ACCELERATOR_MEMORY_MB = 2 * 1024 * 1024

# Capability names are part of the project-facing contract.  Keep the older
# tool labels accepted at the boundary while publishing compact canonical
# fields to health, admission and the operator view.
CAPABILITY_ALIASES = {
    'circt-translate': 'circt',
    'ngspice47': 'ng47',
    'onnx_cpu': 'onnx',
    'aop_hspice_python': 'aop-hs',
    'rgcn_python_linux': 'rgcn_python',
    'rgcn-linux': 'rgcn_python',
}

_IDENTITY_TAG = re.compile(
    r'(?:^|[_:+.-])(?:sha[_-]?(?:1|224|256|384|512)|md5|hash|digest)(?:$|[_:+.-])'
    r'|(?:^|[_:+.-])[0-9a-f]{32,}(?:$|[_:+.-])', re.I)
_PLATFORM_TAG = re.compile(
    r'(?:^|[_:+.-])(?:win\d*|windows\d*|linux\d*|darwin|macos|posix|nt|ubuntu|debian|centos|rhel|'
    r'x86_64|amd64|aarch64|arm64)(?:$|[_:+.-])', re.I)


def canonical_capability(value, *, registration=False):
    name = CAPABILITY_ALIASES.get(str(value), str(value))
    if (_IDENTITY_TAG.search(name) or _PLATFORM_TAG.search(name)
            or registration and _PLATFORM_TAG.search(str(value))):
        raise ValueError('Capabilities require semantic tool names, without hashes or platform selectors')
    return name


def canonical_capabilities(values):
    return sorted({canonical_capability(value) for value in (values or ())})


def clean_tool_snapshot(result):
    """Remove retired engine identities when loading durable task receipts."""
    changed = False
    if isinstance(result.get('tools'), dict):
        tools = {}
        for name, info in result['tools'].items():
            try:
                name = canonical_capability(name)
            except ValueError:
                continue
            if isinstance(info, dict):
                tools[name] = {key: value for key, value in info.items()
                               if key in ('available', 'argv', 'engine', 'version', 'protocol')}
        if tools != result['tools']:
            result['tools'] = tools
            changed = True
    for attempt in result.get('attempts', ()):
        if isinstance(attempt, dict):
            changed = clean_tool_snapshot(attempt) or changed
    return changed

def normalize_accelerator_requirement(value):
    """Normalize one project accelerator request without exposing a device id."""
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ValueError('accelerator must be an object')
    kind = value.get('kind', 'any')
    count = value.get('count', 1)
    memory_mb = value.get('memory_mb', 0)
    requires = value.get('requires', [])
    if (not isinstance(kind, str) or not kind or len(kind) > 32
            or type(count) is not int or not 1 <= count <= MAX_ACCELERATORS
            or type(memory_mb) is not int or not 0 <= memory_mb <= MAX_ACCELERATOR_MEMORY_MB
            or not isinstance(requires, list) or len(requires) > MAX_REQUIREMENTS
            or any(not isinstance(tag, str) or not tag or len(tag) > MAX_REQUIREMENT_LENGTH
                   for tag in requires)):
        raise ValueError('accelerator requires kind, count 1..16, memory_mb and short capability tags')
    normalized = dict(kind=kind, count=count, memory_mb=memory_mb,
                      requires=canonical_capabilities(requires))
    if 'fallback_after_seconds' in value:
        wait = value['fallback_after_seconds']
        if (isinstance(wait, bool) or not isinstance(wait, (int, float))
                or not math.isfinite(wait) or wait < 0):
            raise ValueError('accelerator fallback_after_seconds must be finite and nonnegative')
        normalized['fallback_after_seconds'] = wait
    return normalized


def normalize_accelerators(value):
    """Normalize provider accelerator profiles; ids stay provider-local."""
    if value is None:
        return []
    if not isinstance(value, list) or len(value) > MAX_ACCELERATORS:
        raise ValueError(f'accelerators must be a list of at most {MAX_ACCELERATORS} profiles')
    result = []
    for profile in value:
        if not isinstance(profile, dict):
            raise ValueError('accelerator profiles must be objects')
        kind = profile.get('kind', 'any')
        count = profile.get('count', 1)
        memory_mb = profile.get('memory_mb', 0)
        tags = profile.get('tags', [])
        ids = profile.get('ids')
        if (not isinstance(kind, str) or not kind or len(kind) > 32
                or type(count) is not int or not 1 <= count <= MAX_ACCELERATORS
                or type(memory_mb) is not int or not 0 <= memory_mb <= MAX_ACCELERATOR_MEMORY_MB
                or not isinstance(tags, list) or len(tags) > MAX_REQUIREMENTS
                or any(not isinstance(tag, str) or not tag or len(tag) > MAX_REQUIREMENT_LENGTH for tag in tags)
                or (ids is not None and (not isinstance(ids, list) or len(ids) != count
                                         or any(not isinstance(item, str) or not item for item in ids)))):
            raise ValueError('accelerator profiles require kind, count, memory_mb, tags and optional ids')
        result.append(dict(kind=kind, count=count, memory_mb=memory_mb,
                           tags=canonical_capabilities(tags), ids=list(ids) if ids is not None else None))
    return result


def aggregate_accelerators(profiles):
    """Aggregate profiles across providers while dropping provider-local ids."""
    merged = {}
    for profile in profiles or []:
        key = (profile.get('kind', 'any'), int(profile.get('memory_mb', 0)),
               tuple(sorted(profile.get('tags', ()))))
        item = merged.setdefault(key, dict(kind=key[0], count=0, memory_mb=key[1], tags=list(key[2])))
        item['count'] += int(profile.get('count', 0))
    return sorted(merged.values(), key=lambda item: (item['kind'], item['memory_mb'], item['tags']))


def public_capabilities():
    """Names of the stable project-facing capability concepts."""
    return [
        "admission",
        "artifact_fetch",
        "max_cpus",
        "resize",
        "resize_auto",
        "resize_memory",
        "resize_memory_model",
        "semantic_requires",
        "single_thread_weight",
        "asset_cache",
        "accelerators",
        "accelerator_fallback",
    ]
