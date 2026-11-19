"""Fixed project execution packages. Worker probes and placement stay private."""
from dataclasses import dataclass

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


DEFAULT_PACKAGE = 'cpu'
CPU_TOOLS = frozenset({'python', 'onnx', 'onnx-int8'})
CPU_TAGS = frozenset({'ml'})
RTL_TOOLS = CPU_TOOLS | CHISEL_TOOLS
RTL_TAGS = CPU_TAGS | {'rtl', 'chisel', SINGLE_CORE_PHASE_HANDOFF}
RTL_SHARED = frozenset({'arh'})
TIMING_TOOLS = frozenset({'yosys', 'opensta', 'iverilog', 'g++', 'make'})

PACKAGES = {
    'cpu': Package(CPU_TOOLS, CPU_TAGS),
    'spice': Package(CPU_TOOLS | {'ng47', 'ngspice'}, CPU_TAGS),
    'hspice': Package(CPU_TOOLS | {'hsp'}, CPU_TAGS),
    'rtl': Package(RTL_TOOLS, RTL_TAGS, RTL_SHARED),
    'layout': Package(CPU_TOOLS | {'layout', 'magic', 'netgen'}, CPU_TAGS),
    'timing': Package(CPU_TOOLS | TIMING_TOOLS, CPU_TAGS),
    'cuda': Package(RTL_TOOLS, RTL_TAGS, RTL_SHARED, 'cuda'),
    'npu': Package(CPU_TOOLS, CPU_TAGS, accelerator='npu'),
}

# The old SDKs can request a known legacy set. It always expands to the
# entire package, so adding or removing aliases cannot refine placement.
_CPU_ALIASES = CPU_TOOLS | CPU_TAGS | {'cpu'}
_RTL_ALIASES = frozenset({'rtl', 'chisel', 'shared:arh', SINGLE_CORE_PHASE_HANDOFF})
_LEGACY = {
    'spice': frozenset({'spice', 'ng47', 'ngspice'}),
    'hspice': frozenset({'hsp'}),
    'rtl': _RTL_ALIASES,
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
    if (SINGLE_CORE_PHASE_HANDOFF in names and not names & {'rtl', 'chisel', 'shared:arh'}
            or 'cpu' in names and not names <= _CPU_ALIASES):
        raise ValueError('Choose one DCO capability package: ' + ', '.join(PACKAGES))
    if accelerator is not None and (
            not isinstance(accelerator, dict) or accelerator.get('requires')
            or set(accelerator) - {'kind', 'count', 'memory_mb', 'requires', 'sharing',
                                  'max_jobs', 'fallback_after_seconds'}):
        raise ValueError('Accelerator requests accept package resource quantities only')

    if names <= _CPU_ALIASES:
        name = DEFAULT_PACKAGE
    else:
        name = None
        for candidate, aliases in _LEGACY.items():
            ignored = _CPU_ALIASES
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
        if name in ('cpu', 'rtl') and kind == 'cuda':
            name = 'cuda'
        if name == 'cpu' and kind == 'npu':
            name = 'npu'
        if not kind or kind != PACKAGES[name].accelerator:
            raise ValueError('Accelerator kind must agree with the DCO capability package')
        accelerator = dict(accelerator, kind=kind)
    elif PACKAGES[name].accelerator:
        accelerator = {'kind': PACKAGES[name].accelerator}
    return name, normalize_accelerator_requirement(accelerator)


def package_task(task, name, accelerator):
    """Expand a project task to a complete, provider-independent execution profile."""
    package = PACKAGES[name]
    tools, shared, _, _ = execution_bindings(task)
    if not tools <= package.tools or not shared <= package.shared:
        raise ValueError('Tool or shared binding is outside the ' + name + ' capability package')
    bindings = ['ml']
    if 'onnx-int8' in task.get('requires', ()):
        bindings.append('onnx-int8')
    return dict(task, requires=bindings, accelerator=accelerator, _capability_package=name)


def provider_has_package(health, name):
    """Require the same probes for all work in a package, including unused bindings."""
    package = PACKAGES[name]
    if not capability_set(health.get('tags', ())).issuperset(package.requirements):
        return False
    tools = execution_tool_set(tuple(tool for tool, probe in health.get('tools', {}).items()
                                     if probe.get('available')))
    return (package.tools <= tools and package.shared.issubset(health.get('shared', ()))
            and 'ml' in health.get('python_backends', ()))


def package_names(health):
    """Project package names justified by a single provider's probe report."""
    tags = health.get('tags', ())
    return [name for name, package in PACKAGES.items() if provider_has_package(health, name)
            and (package.accelerator is None or package.accelerator in tags
                 and package.accelerator in health.get('python_backends', ()))]
