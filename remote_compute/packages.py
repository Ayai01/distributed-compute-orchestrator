"""Fixed project execution packages. Worker probes and placement stay private."""
from dataclasses import dataclass
from pathlib import PurePosixPath, PureWindowsPath

from .policy import (MAX_REQUIREMENTS, MAX_REQUIREMENT_LENGTH, canonical_capabilities,
                     normalize_accelerator_requirement)
from .runtimes import (CHISEL_TOOLS, SINGLE_CORE_PHASE_HANDOFF, capability_set,
                       execution_bindings, execution_tool_set)


@dataclass(frozen=True)
class Package:
    tools: frozenset
    tags: frozenset = frozenset()
    shared: frozenset = frozenset()
    accelerator: str | None = None

    @property
    def requirements(self):
        return sorted(self.tools | self.tags | {'shared:' + name for name in self.shared})


DEFAULT_PACKAGE = 'python'
PYTHON_TOOLS = frozenset({'python'})
PYTHON_TAGS = frozenset({'ml'})
ONNX_TOOLS = PYTHON_TOOLS | {'onnx', 'onnx-int8'}
RTL_TOOLS = ONNX_TOOLS | CHISEL_TOOLS
RTL_TAGS = PYTHON_TAGS | {'rtl', 'chisel'}
RTL_SHARED = frozenset({'arh'})
TIMING_TOOLS = frozenset({'yosys', 'opensta', 'iverilog', 'g++', 'make'})

PACKAGES = {
    'python': Package(PYTHON_TOOLS, PYTHON_TAGS),
    'ng47': Package(PYTHON_TOOLS | {'ng47'}, PYTHON_TAGS),
    'hsp': Package(PYTHON_TOOLS | {'hsp'}, PYTHON_TAGS),
    'rtl': Package(RTL_TOOLS, RTL_TAGS, RTL_SHARED),
    'onnx': Package(ONNX_TOOLS, PYTHON_TAGS),
    'layout': Package(PYTHON_TOOLS | {'layout', 'magic', 'netgen'}, PYTHON_TAGS),
    'timing': Package(PYTHON_TOOLS | TIMING_TOOLS, PYTHON_TAGS),
    'cuda': Package(RTL_TOOLS, RTL_TAGS, RTL_SHARED, 'cuda'),
    'npu': Package(PYTHON_TOOLS, PYTHON_TAGS, accelerator='npu'),
}

# Existing original IDs accepted by 1.0.22 still carry these execution profiles.
# They are used only for already-accepted work.
_RETAINED = {
    'cpu': PACKAGES['onnx'],
    'spice': Package(ONNX_TOOLS | {'ng47', 'ngspice'}, PYTHON_TAGS),
    'hspice': Package(ONNX_TOOLS | {'hsp'}, PYTHON_TAGS),
}
RETAINED_NAMES = {'cpu': 'python', 'spice': 'ng47', 'hspice': 'hsp'}

# The old SDKs can request a known legacy set. It always expands to the
# entire package, so adding or removing aliases cannot refine placement.
_PYTHON_ALIASES = PYTHON_TOOLS | PYTHON_TAGS
_ONNX_ALIASES = ONNX_TOOLS - PYTHON_TOOLS
_RTL_BUILD_HINTS = frozenset({SINGLE_CORE_PHASE_HANDOFF, 'ccache'})
_RTL_ALIASES = frozenset({'rtl', 'chisel', 'shared:arh'}) | _RTL_BUILD_HINTS
_LEGACY = {
    'ng47': frozenset({'spice', 'ng47', 'ngspice'}),
    'hsp': frozenset({'hsp'}),
    'rtl': _RTL_ALIASES,
    'onnx': _ONNX_ALIASES,
    'layout': frozenset({'layout'}),
    'timing': TIMING_TOOLS - {'g++', 'make'} | {'timing'},
    'cuda': frozenset({'cuda'}),
    'npu': frozenset({'npu'}),
}


def project_package(requires, accelerator=None):
    """Resolve one public package and normalize its numeric accelerator needs."""
    if (not isinstance(requires, (list, tuple)) or len(requires) > MAX_REQUIREMENTS
            or any(not isinstance(name, str) or not 0 < len(name) <= MAX_REQUIREMENT_LENGTH for name in requires)):
        raise ValueError('requires must name one DCO capability package')
    names = frozenset(canonical_capabilities(requires))
    if names & _RTL_BUILD_HINTS and not names & (_RTL_ALIASES - _RTL_BUILD_HINTS):
        raise ValueError('Choose one DCO capability package: ' + ', '.join(PACKAGES))
    if accelerator is not None and (
            not isinstance(accelerator, dict) or accelerator.get('requires')
            or set(accelerator) - {'kind', 'count', 'memory_mb', 'requires', 'sharing',
                                  'max_jobs', 'fallback_after_seconds'}):
        raise ValueError('Accelerator requests accept package resource quantities only')

    if names <= _PYTHON_ALIASES:
        name = DEFAULT_PACKAGE
    else:
        name = None
        for candidate, aliases in _LEGACY.items():
            ignored = _PYTHON_ALIASES
            if candidate in ('rtl', 'cuda'):
                ignored = ignored | _ONNX_ALIASES
            if candidate == 'timing':
                ignored = ignored | {'g++', 'make'}
            elif candidate == 'cuda':
                ignored = ignored | _RTL_ALIASES
            if names & aliases and names <= aliases | ignored:
                name = candidate
                break
        if name is None:
            raise ValueError('Choose one DCO capability package: ' + ', '.join(PACKAGES))

    if accelerator is not None:
        kind = accelerator.get('kind', PACKAGES[name].accelerator)
        # Existing auto-CUDA callers use the same combined CUDA package.
        if name in (DEFAULT_PACKAGE, 'onnx', 'rtl') and kind == 'cuda':
            name = 'cuda'
        if name == DEFAULT_PACKAGE and kind == 'npu':
            name = 'npu'
        if not kind or kind != PACKAGES[name].accelerator:
            raise ValueError('Accelerator kind must agree with the DCO capability package')
        accelerator = dict(accelerator, kind=kind)
    elif PACKAGES[name].accelerator:
        accelerator = {'kind': PACKAGES[name].accelerator}
    return name, normalize_accelerator_requirement(accelerator)


def package_task(task, name, accelerator):
    """Expand a project task to a complete, provider-independent execution profile."""
    package = PACKAGES.get(name) or _RETAINED[name]
    if name == 'ng47':
        # Existing SDKs use the unversioned placeholder. The package always
        # executes the verified ng47 registration.
        binding = '${tool:ngspice}'
        ng47 = '${tool:ng47}'
        task = dict(task, steps=[[arg.replace(binding, ng47) for arg in argv] for argv in task['steps']],
                    env={key: value.replace(binding, ng47) for key, value in task['env'].items()})
    tools, shared, _, _ = execution_bindings(task)
    if not tools <= package.tools or not shared <= package.shared:
        raise ValueError('Tool or shared binding is outside the ' + name + ' capability package')
    bindings = ['ml']
    if 'onnx-int8' in tools or 'onnx-int8' in task.get('requires', ()):
        bindings.append('onnx-int8')
    return dict(task, requires=bindings, accelerator=accelerator, _capability_package=name)


def provider_has_package(health, name):
    """Require the same probes for all work in a package, including unused bindings."""
    package = PACKAGES.get(name) or _RETAINED[name]
    if not capability_set(health.get('tags', ())).issuperset(package.requirements):
        return False
    tools = execution_tool_set(tuple(tool for tool, probe in health.get('tools', {}).items()
                                     if probe.get('available')))
    return (package.tools <= tools and package.shared.issubset(health.get('shared', ()))
            and 'ml' in health.get('python_backends', ()))


def provider_environment(task, health):
    """Supply the registered FIRRTL compiler to Chisel on the chosen worker."""
    env = task['env']
    if 'CHISEL_FIRTOOL_PATH' in env:
        return env
    if task.get('_capability_package') not in ('rtl', 'cuda') and 'chisel' not in task['requires']:
        return env
    firtool = health.get('tools', {}).get('firtool', {})
    argv = firtool.get('argv')
    if not firtool.get('available') or not argv:
        return env
    path_type = PureWindowsPath if health.get('platform') == 'win32' else PurePosixPath
    return dict(env, CHISEL_FIRTOOL_PATH=str(path_type(argv[0]).parent))


def package_names(health, *, include_base=True):
    """Project package names justified by a single provider's probe report."""
    tags = health.get('tags', ())
    return [name for name, package in PACKAGES.items() if (include_base or name != DEFAULT_PACKAGE)
            and provider_has_package(health, name)
            and (package.accelerator is None or package.accelerator in tags
                 and package.accelerator in health.get('python_backends', ()))]
