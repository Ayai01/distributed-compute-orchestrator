"""Stable execution categories and per-task Python backend selection."""
import re

from .policy import canonical_capability, canonical_capabilities, select_accelerator_units


EXECUTION_CAPABILITIES = ('ng47','hsp','chisel','ml','cuda','layout','rtl','onnx')
CHISEL_TOOLS = {'java', 'scala-cli', 'firtool', 'circt', 'verilator', 'cxx', 'make'}
LAYOUT_TOOLS = {'layout', 'magic', 'netgen'}


def execution_capabilities(tags):
    available = set()
    for tag in tags:
        try:available.add(canonical_capability(tag))
        except ValueError:continue  # Retired display labels have no execution role.
    return [name for name in EXECUTION_CAPABILITIES if name in available]


def execution_tool(name):
    canonical=canonical_capability(name)
    return 'python' if canonical in {'python','ml'} else canonical


def uses_python(task):
    if {'python','ml'}&set(canonical_capabilities(task.get('requires', ()))):return True
    texts = [*task.get('env', {}).values(), task.get('cwd', ''),
             *(arg for argv in task.get('steps', ()) for arg in argv)]
    return any(execution_tool(name) == 'python' for text in texts
               for name in re.findall(r'\$\{tool:([A-Za-z_][A-Za-z0-9_+.-]*)\}', text))


def python_backend(task):
    accelerator=task.get('accelerator') or {}
    if accelerator:
        return accelerator.get('kind','any')
    if 'ml' in canonical_capabilities(task.get('requires', ())):return 'ml'
    texts=[*task.get('env',{}).values(),task.get('cwd',''),*(arg for argv in task.get('steps',())for arg in argv)]
    if any(canonical_capability(name)=='ml' for text in texts
           for name in re.findall(r'\$\{tool:([A-Za-z_][A-Za-z0-9_+.-]*)\}',text)):return 'ml'
    return 'cpu'


def supports_python(task, backends):
    return not uses_python(task) or python_backend(task) in backends


def python_accelerator(task, backends, units, reserved=()):
    """Bind an any-device Python request to one matching execution backend."""
    requirement=task.get('accelerator')
    if not requirement or requirement.get('kind','any')!='any' or not uses_python(task):return requirement
    for kind in dict.fromkeys(unit[2] for unit in units if unit[2] in backends):
        bound=dict(requirement,kind=kind)
        if select_accelerator_units(units,bound,reserved) is not None:return bound
    return requirement  # An unresolved Python backend fails matching.
