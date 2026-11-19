"""Stable execution categories and per-task Python backend selection."""
import re
from functools import lru_cache

from .policy import canonical_capability, canonical_capabilities, select_accelerator_units


SINGLE_CORE_PHASE_HANDOFF = 'single-core-phase-handoff'
EXECUTION_CAPABILITIES = ('ng47','hsp','chisel','ml','cuda','npu','layout','rtl','onnx','onnx-int8','pe64',
                          'elf-aapcs64',SINGLE_CORE_PHASE_HANDOFF)
CHISEL_TOOLS = {'java', 'scala-cli', 'firtool', 'circt', 'verilator', 'cxx', 'make'}
LAYOUT_TOOLS = {'layout', 'magic', 'netgen'}
_TOOLS = re.compile(r'\$\{tool:([A-Za-z_][A-Za-z0-9_+.-]*)\}')
_SHARED = re.compile(r'\$\{shared:([\w-]+)\}')


def native_architecture(value):
    name = (value or '').lower()
    return {'arm64': 'aarch64', 'amd64': 'x86_64', 'x64': 'x86_64'}.get(name, name)


def validate_native_cache(value):
    """A native artifact depends on one shared storage namespace and ABI."""
    if value is None:
        return None
    if (not isinstance(value, dict) or set(value) != {'storage', 'platform', 'architecture'}
            or not all(isinstance(item, str) for item in value.values())
            or not re.fullmatch('[0-9a-f]{64}', value['storage'])
            or any(not re.fullmatch('[a-z0-9_]{1,32}', value[key]) for key in ('platform', 'architecture'))):
        raise ValueError('native_cache requires storage SHA256, platform and architecture')
    return dict(value, architecture=native_architecture(value['architecture']))


def supports_native_cache(health, requirement, configured_architecture=None):
    if requirement is None:
        return True
    architecture = health.get('architecture') or configured_architecture
    return (health.get('shared_asset_cache') == requirement['storage']
            and health.get('platform') == requirement['platform']
            and native_architecture(architecture) == requirement['architecture'])


def execution_capabilities(tags):
    available = set()
    for tag in tags:
        try:available.add(canonical_capability(tag))
        except ValueError:continue  # Retired display labels have no execution role.
    return [name for name in EXECUTION_CAPABILITIES if name in available]


def provider_tags(health):
    """Project capabilities come from the running executor's protocol."""
    tags = set(health.get('tags', ()))
    capabilities = health.get('capabilities', ())
    # The first release implementing the handoff predates its wire flag.
    supported = (SINGLE_CORE_PHASE_HANDOFF in capabilities or health.get('version') == '1.0.20'
                 and {'resize_min_cpus', 'resize_active_cpus'}.issubset(capabilities))
    if supported and 'python' in tags:
        tags.add(SINGLE_CORE_PHASE_HANDOFF)
    else:
        tags.discard(SINGLE_CORE_PHASE_HANDOFF)
    return sorted(tags)


def execution_tool(name):
    canonical=canonical_capability(name)
    return 'python' if canonical in {'python','ml'} else canonical


def execution_bindings(task):
    """Cache immutable request syntax; provider availability stays live."""
    texts=(*task.get('env',{}).values(),task.get('cwd',''),
           *(arg for argv in task.get('steps',())for arg in argv))
    return _execution_bindings(tuple(task.get('requires',())),texts)


@lru_cache(maxsize=256)
def _execution_bindings(requirements,texts):
    required=set(canonical_capabilities(requirements))
    names={canonical_capability(name)for text in texts for name in _TOOLS.findall(text)}
    tools=frozenset('python'if name in {'python','ml'}else name for name in names)
    shared=frozenset(name for text in texts for name in _SHARED.findall(text))
    python='python'in tools or bool({'python','ml'}&required)
    return tools,shared,python,'ml'if 'ml'in required or 'ml'in names else'cpu'


@lru_cache(maxsize=256)
def execution_tool_set(names):
    return frozenset(execution_tool(name)for name in names)


def capability_set(names):
    return _capability_set(tuple(names))


@lru_cache(maxsize=256)
def _capability_set(names):
    return frozenset(canonical_capabilities(names))


def uses_python(task):
    return execution_bindings(task)[2]


def python_backend(task):
    accelerator=task.get('accelerator')
    if accelerator is not None:
        return accelerator.get('kind','any')
    return execution_bindings(task)[3]


def supports_python(task, backends):
    _tools,_shared,python,backend=execution_bindings(task)
    accelerator=task.get('accelerator')
    return not python or ((accelerator.get('kind','any')if accelerator is not None else backend)in backends)


def python_accelerator(task, backends, units, reserved=()):
    """Bind an any-device Python request to one matching execution backend."""
    requirement=task.get('accelerator')
    if requirement is None or requirement.get('kind','any')!='any' or not uses_python(task):return requirement
    for kind in dict.fromkeys(unit[2] for unit in units if unit[2] in backends):
        bound=dict(requirement,kind=kind)
        if select_accelerator_units(units,bound,reserved) is not None:return bound
    return requirement  # An unresolved Python backend fails matching.
