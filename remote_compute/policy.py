"""Small, shared admission policy for project-facing DCO requests.

The scheduler owns placement.  Projects express work in terms of resource
needs and semantic capabilities; these bounds keep optional scheduling hints
from becoming a second placement API.
"""

import math
import re
from functools import lru_cache

MAX_RESOURCE_SHARE = 16.0
MAX_QUEUE_RATIO = 4.0
MAX_READY_TASKS = 4096
MAX_STATUS_BATCH = 128
MAX_SINGLE_THREAD_WEIGHT = 8.0
MIN_PRIORITY = -1000
MAX_PRIORITY = 1000
MAX_REQUIREMENTS = 32
MAX_REQUIREMENT_LENGTH = 64
MAX_TOOL_CONSTRAINTS = 32
MAX_ACCELERATORS = 16
MAX_ACCELERATOR_MEMORY_MB = 2 * 1024 * 1024


def validate_ready_tasks(value):
    if value is not None and (type(value) is not int or not 1 <= value <= MAX_READY_TASKS):
        raise ValueError('ready_tasks requires an integer from 1 to %d' % MAX_READY_TASKS)
    return value


def validate_status_ids(identities):
    if (not isinstance(identities, list) or not 1 <= len(identities) <= MAX_STATUS_BATCH
            or any(not isinstance(identity, str) or not re.fullmatch('[0-9a-f]{32}', identity)
                   for identity in identities)):
        raise ValueError('ids requires 1..%d original 32-character job IDs' % MAX_STATUS_BATCH)
    return identities

# Capability names are part of the project-facing contract.  Keep the older
# tool labels accepted at the boundary while publishing compact canonical
# fields to health, admission and the operator view.
PYTHON_RUNTIME_ALIASES = {
    'aop_python': 'python_numpy',
    'arh_python': 'python_numpy',
    'rgcn_python': 'python_numpy',
    'aop_hspice_python': 'python_numpy',
    'aop-hs': 'python_numpy',
    'bench_python': 'python_scientific',
    'rgcn_train': 'python_torch_pyg',
    'rgcn_fiber': 'python_torch_pyg',
}
CAPABILITY_ALIASES = {
    **PYTHON_RUNTIME_ALIASES,
    'circt-translate': 'circt',
    'ngspice47': 'ng47',
    'onnx_cpu': 'onnx',
    'rgcn_python_linux': 'python_numpy',
    'rgcn-linux': 'python_numpy',
}

_IDENTITY_TAG = re.compile(
    r'(?:^|[_:+.-])(?:sha[_-]?(?:1|224|256|384|512)|md5|hash|digest)(?:$|[_:+.-])'
    r'|(?:^|[_:+.-])[0-9a-f]{32,}(?:$|[_:+.-])', re.I)
_PLATFORM_TAG = re.compile(
    r'(?:^|[_:+.-])(?:win\d*|windows\d*|linux\d*|darwin|macos|posix|nt|ubuntu|debian|centos|rhel|'
    r'x86_64|amd64|aarch64|arm64)(?:$|[_:+.-])', re.I)


def canonical_capability(value, *, registration=False):
    return _canonical_capability(str(value), bool(registration))


@lru_cache(maxsize=256)
def _canonical_capability(value, registration):
    # Scheduling repeatedly matches the same fixed vocabulary across jobs.
    name = CAPABILITY_ALIASES.get(value, value)
    if (_IDENTITY_TAG.search(name) or _PLATFORM_TAG.search(name)
            or registration and _PLATFORM_TAG.search(value)):
        raise ValueError('Capabilities require semantic tool names, without hashes or platform selectors')
    return name


def canonical_capabilities(values):
    return list(_canonical_capabilities_tuple(tuple(str(value) for value in (values or ()))))


@lru_cache(maxsize=256)
def _canonical_capabilities_tuple(values):
    return tuple(sorted({canonical_capability(value) for value in values}))


def clean_tool_snapshot(result):
    """Remove retired engine identities when loading durable task receipts."""
    changed = False
    if isinstance(result.get('tools'), dict):
        tools = {}
        for original, info in result['tools'].items():
            try:
                name = canonical_capability(original)
            except ValueError:
                continue
            # Receipt keys describe bindings accepted by the frozen caller.
            # Keep those paths distinct even when admission shares one profile.
            if original in PYTHON_RUNTIME_ALIASES:
                name = original
            if isinstance(info, dict):
                item = {key: value for key, value in info.items()
                        if key in ('available', 'argv', 'engine', 'version', 'protocol')}
                previous = tools.get(name)
                if (previous is None or item.get('available') and
                        (not previous.get('available') or original == name)):
                    tools[name] = item
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
    devices = set()
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
        for device in ids if ids is not None else map(str, range(count)):
            key = kind, device
            if key in devices:
                raise ValueError('Duplicate accelerator device: ' + kind + '/' + device)
            devices.add(key)
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
        "admission_ready_tasks",
        "batch_status",
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
