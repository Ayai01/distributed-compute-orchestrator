"""Provision the default PyTorch release for a worker's Python ABI."""

import json
import os
from pathlib import Path
import subprocess
import tempfile


TORCH_VERSION = '2.14.1'
TORCH_BUILDS = {'cpu': None, 'cu130': '13.0'}
WHEEL_INDEX = 'https://download.pytorch.org/whl'

PROBE = '''
import json, sys
import torch
torch.set_num_threads(1)
left = torch.tensor([[1., 2.], [3., 4.]], dtype=torch.float32)
right = torch.tensor([[2., 0.], [1., 2.]], dtype=torch.float32)
product = (left @ right).tolist()
assert product == [[4., 4.], [10., 8.]], product
print(json.dumps(dict(python=sys.executable, torch=torch.__version__,
                      module=torch.__file__, cuda=torch.version.cuda, product=product)))
'''


def _run(executable, *args, env=None, capture=False):
    return subprocess.run([str(executable), *map(str, args)], check=True, text=True,
                          env=env, capture_output=capture,
                          creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))


def _environment(env):
    result = dict(os.environ if env is None else env)
    for key in ('PYTHONHOME', 'PYTHONPATH'):
        result.pop(key, None)
    result.update(PYTHONNOUSERSITE='1', PYTHONDONTWRITEBYTECODE='1',
                  TORCH_DEVICE_BACKEND_AUTOLOAD='0', PIP_CONFIG_FILE=os.devnull)
    return result


def torch_pip_args(build):
    if build not in TORCH_BUILDS:
        raise ValueError('Unsupported PyTorch build: ' + str(build))
    return ['--disable-pip-version-check', '--no-cache-dir', '--no-compile',
            '--only-binary=:all:', '--index-url', f'{WHEEL_INDEX}/{build}',
            f'torch=={TORCH_VERSION}+{build}']


def torch_probe(executable, build='cpu', *, env=None):
    result = json.loads(_run(executable, '-B', '-c', PROBE,
                             env=_environment(env), capture=True).stdout.splitlines()[-1])
    if result['torch'].split('+', 1)[0] != TORCH_VERSION or result['cuda'] != TORCH_BUILDS[build]:
        raise RuntimeError(f"Expected PyTorch {TORCH_VERSION} {build}, found {result['torch']}")
    return result


def prepare_cpu_runtime(executable, directory):
    """Publish a separate CPU venv over the existing read-only science packages."""
    env = _environment(None)
    metadata = ('import json,sys,sysconfig; print(json.dumps(dict('
                'site=sysconfig.get_path("purelib"), abi=sys.implementation.cache_tag)))')
    origin = json.loads(_run(executable, '-B', '-c', metadata, env=env, capture=True).stdout)
    parent = Path(directory)
    parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    target = parent / f'torch-{TORCH_VERSION}-cpu-{origin["abi"]}'
    child = Path('Scripts/python.exe' if os.name == 'nt' else 'bin/python')
    if target.is_dir():
        return torch_probe(target / child, env=env)

    with tempfile.TemporaryDirectory(prefix='.torch-install-', dir=parent) as temp:
        stage = Path(temp) / 'runtime'
        scratch = {**env, 'TMPDIR': temp, 'TEMP': temp, 'TMP': temp}
        _run(executable, '-B', '-m', 'venv', '--without-pip', stage, env=scratch)
        stage_python = stage / child
        location = json.loads(_run(stage_python, '-B', '-c', metadata,
                                   env=scratch, capture=True).stdout)
        site = Path(location['site'])
        (site / 'dco-science-base.pth').write_text(origin['site'] + '\n', encoding='utf-8')
        _run(stage_python, '-B', '-m', 'pip', 'install', '--target', site, *torch_pip_args('cpu'), env=scratch)
        torch_probe(stage_python, env=scratch)
        # Worker tasks use the venv's Python executable directly.
        os.rename(stage, target)
    return torch_probe(target / child, env=env)
