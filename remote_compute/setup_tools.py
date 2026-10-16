"""Install project-local scientific workers; the controller stays stdlib-only."""

import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
from urllib.request import urlopen
import zipfile


ROOT = Path(__file__).resolve().parent.parent
PYTHON_VERSION = '3.12.10'
PYTHON_URL = f'https://www.python.org/ftp/python/{PYTHON_VERSION}/python-{PYTHON_VERSION}-embed-amd64.zip'
PACKAGES = ('numpy==2.3.5', 'scipy==1.18.1', 'numba==0.68.0', 'llvmlite==0.50.0',
            'pymoo==0.6.2', 'moocore==0.3.2', 'threadpoolctl==3.7.0',
            'platformdirs==4.13.0', 'cffi==2.1.1', 'pycparser==3.0',
            'Deprecated==3.0.0', 'wrapt==2.5.0', 'alive-progress==3.3.0',
            'graphemeu==0.7.2', 'about-time==4.2.1', 'matplotlib==3.11.2',
            'python-dateutil==2.9.0.post0', 'six==1.17.0', 'pyparsing==3.3.3',
            'pillow==12.3.0', 'packaging==26.3', 'kiwisolver==1.5.1',
            'fonttools==4.66.1', 'cycler==0.12.1', 'contourpy==1.4.0',
            'cma==4.5.0', 'autograd==1.9.1',
            'opencv-python-headless==4.13.0.92', 'onnx==1.18.0', 'onnxruntime==1.22.1',
            'protobuf==5.29.5', 'sympy==1.14.0', 'mpmath==1.3.0',
            'flatbuffers==25.12.19', 'coloredlogs==15.0.1', 'humanfriendly==10.0',
            'pyreadline3==3.5.6', 'typing-extensions==4.16.0')
NGSPICE_URL = 'https://downloads.sourceforge.net/project/ngspice/ng-spice-rework/47/ngspice-47_64.7z'
SEVENZIP_URL = 'https://github.com/ip7z/7zip/releases/download/26.04/7zr.exe'


def _download(url, target):
    print('Downloading '+target.name, flush=True)
    with urlopen(url, timeout=90) as source, target.open('wb') as output:
        shutil.copyfileobj(source, output, 1024*1024)


def _run(argv, **kwargs):
    return subprocess.run([str(value) for value in argv], check=True,
                          creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0), **kwargs)


def _python(destination, temporary):
    executable = destination/'python.exe'
    if not executable.is_file():
        archive = temporary/'python.zip'
        _download(PYTHON_URL, archive)
        destination.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(archive) as bundle:
            bundle.extractall(destination)
    # Normal module startup must honor the job's PYTHONPATH and cwd. In
    # particular Bench runs -m bench.remote from a content-addressed asset.
    (destination/'python312._pth').unlink(missing_ok=True)
    env = dict(os.environ, PYTHONNOUSERSITE='1', PYTHONDONTWRITEBYTECODE='1', PYTHONUNBUFFERED='1',
               TEMP=str(temporary), TMP=str(temporary), TMPDIR=str(temporary))
    env.pop('PYTHONHOME', None)
    env.pop('PYTHONPATH', None)
    if _run([executable, '-B', '-c', 'import importlib.util; print(bool(importlib.util.find_spec("pip")))'],
            env=env, capture_output=True, text=True).stdout.strip() != 'True':
        bootstrap = temporary/'get-pip.py'
        _download('https://bootstrap.pypa.io/get-pip.py', bootstrap)
        _run([executable, '-B', bootstrap, '--no-cache-dir', '--no-compile'], env=env)
    _run([executable, '-B', '-m', 'pip', 'install', '--disable-pip-version-check',
          '--no-cache-dir', '--no-compile', '--only-binary=:all:', *PACKAGES], env=env)
    probe = ('import json,sys,importlib.metadata as m; import numpy,scipy,numba,pymoo,moocore,cv2,onnx,onnxruntime as ort; '
             'f=numba.njit(lambda x: x*x); assert f(7)==49; '
             'assert cv2.resize(numpy.zeros((2,2),numpy.uint8),(4,4)).shape==(4,4); '
             'h=onnx.helper; t=onnx.TensorProto.FLOAT; '
             'graph=h.make_graph([h.make_node("Add",["x","y"],["z"])],"probe",'
             '[h.make_tensor_value_info("x",t,[2]),h.make_tensor_value_info("y",t,[2])],'
             '[h.make_tensor_value_info("z",t,[2])]); '
             'model=h.make_model(graph,opset_imports=[h.make_opsetid("",13)],ir_version=10); '
             'session=ort.InferenceSession(model.SerializeToString(),providers=["CPUExecutionProvider"]); '
             'result=session.run(None,{"x":numpy.array([1,2],numpy.float32),"y":numpy.array([3,4],numpy.float32)})[0]; '
             'assert numpy.array_equal(result,[4,6]); '
             'print(json.dumps({"python":sys.version.split()[0],"packages":'
             '{n:m.version(n) for n in '+repr([p.split('==')[0] for p in PACKAGES])+'}}))')
    return executable, json.loads(_run([executable, '-B', '-c', probe], env=env,
                                      capture_output=True, text=True).stdout)


def _ngspice(destination, temporary, source=None):
    executable = destination/'bin/ngspice.exe'
    if source:
        source = Path(source).resolve()
        if not (source/'bin/ngspice.exe').is_file():
            raise ValueError('ngspice source must contain bin/ngspice.exe: '+str(source))
        if source != destination.resolve():
            shutil.copytree(source, destination, dirs_exist_ok=True)
    elif not executable.is_file():
        archive, extractor = temporary/'ngspice.7z', temporary/'7zr.exe'
        _download(NGSPICE_URL, archive)
        _download(SEVENZIP_URL, extractor)
        unpacked = temporary/'ngspice-unpacked'
        _run([extractor, 'x', archive, '-o'+str(unpacked), '-y'], capture_output=True)
        found = next(unpacked.rglob('ngspice.exe'))
        shutil.copytree(found.parent.parent, destination, dirs_exist_ok=True)
    # The Windows distribution's spinit contains installation-specific paths.
    # Refresh just those paths so copied installations also work on another PC.
    spinit = destination/'share/ngspice/scripts/spinit'
    if spinit.is_file():
        text = spinit.read_text(encoding='utf-8-sig')
        def relocate(match):
            name = match[2].strip().strip('"').replace('\\', '/').rsplit('/', 1)[-1]
            return match[1]+'"'+(destination/'lib/ngspice'/name).as_posix()+'"'
        spinit.write_text(re.sub(r'(?m)^(\s*(?:codemodel|osdi)\s+)([^\r\n]+)', relocate, text), encoding='utf8')
    version = _run([executable, '--version'], capture_output=True, text=True).stdout
    return executable, {'version':next((line.strip(' *') for line in version.splitlines() if 'ngspice-' in line), version.strip()),
                        'sha256':hashlib.sha256(executable.read_bytes()).hexdigest()}


def install(root=ROOT, *, ngspice_source=None, temporary_root=None):
    """Return tools/env for worker config; install no packages outside .tools."""
    if os.name != 'nt':
        raise ValueError('This installer supplies Windows x64 workers; use native Python/ngspice on other systems')
    from .ramdisk import scratch
    root = Path(root).resolve()
    tools = root/'.tools'
    tools.mkdir(exist_ok=True)
    temporary_root = Path(temporary_root) if temporary_root else scratch('Remote-Compute')
    temporary_root.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='install-science-', dir=temporary_root) as directory:
        temporary = Path(directory)
        python, runtime = _python(tools/'python', temporary)
        ngspice, simulator = _ngspice(tools/'ngspice', temporary, ngspice_source)
    result = {'tools':{name:python.as_posix() for name in ('aop_python','rgcn_python','bench_python','arh_python')},
              'env':{'PYTHONNOUSERSITE':'1', 'ARH_INFERENCE_PYTHON':python.as_posix()},
              'runtime':runtime, 'ngspice':simulator}
    result['tools']['ngspice'] = ngspice.as_posix()
    msys = tools/'msys64'
    if (msys/'usr/bin/bash.exe').is_file():
        for name in ('tmp','dev/shm','dev/mqueue','home'):
            (msys/name).mkdir(parents=True, exist_ok=True)
        result['env']['MSYS2_ROOT'] = msys.as_posix()
    result['installed_mib'] = {name:round(sum(p.stat().st_size for p in (tools/name).rglob('*') if p.is_file())/1024**2, 1)
                               for name in ('python','ngspice')}
    (tools/'science.json').write_text(json.dumps(result, ensure_ascii=False, indent=2)+'\n', encoding='utf8')
    return result


def rtl_files(source):
    """Select the installed GCC/Verilator package closure, excluding docs/cache."""
    source = Path(source).resolve()
    def sections(path):
        return {name:body.strip().splitlines()
                for name,body in re.findall(r'%([^%]+)%\n([^%]*)', path.read_text(encoding='utf8'))}
    packages, providers = {}, {}
    for folder in (source/'var/lib/pacman/local').iterdir():
        if not (folder/'desc').is_file():
            continue
        metadata = sections(folder/'desc')
        name = metadata['NAME'][0]
        packages[name] = folder, metadata
        for value in metadata.get('PROVIDES', []):
            providers[re.split('[<>=]', value, maxsplit=1)[0]] = name
    pending = ['msys2-runtime','bash','make','sed','coreutils','grep','findutils','filesystem','which',
               'mingw-w64-ucrt-x86_64-gcc','mingw-w64-ucrt-x86_64-verilator']
    selected, files = {}, set()
    while pending:
        name = pending.pop()
        name = name if name in packages else providers[name]
        if name in selected:
            continue
        folder, metadata = packages[name]
        selected[name] = metadata['VERSION'][0]
        pending.extend(re.split('[<>=]', value, maxsplit=1)[0] for value in metadata.get('DEPENDS', []))
        for name in sections(folder/'files').get('FILES', []):
            if ((source/name).is_file() and not
                    {'doc','man','locale','info','post-install','__pycache__'}.intersection(Path(name).parts)):
                files.add(name)
    return sorted(files), dict(sorted(selected.items()))


def copy_rtl(source, root=ROOT):
    """Copy an existing MSYS2 runtime's actual compiler dependency closure."""
    source, root = Path(source).resolve(), Path(root).resolve()
    destination = root/'.tools/msys64'
    if source == destination:
        raise ValueError('Select the existing full MSYS2 installation as the source')
    files, packages = rtl_files(source)
    for name in files:
        target = destination/name
        target.parent.mkdir(parents=True, exist_ok=True)
        original = source/name
        if not target.is_file() or (target.stat().st_size,target.stat().st_mtime_ns) != (original.stat().st_size,original.stat().st_mtime_ns):
            shutil.copy2(original, target)
    for name in ('tmp','dev/shm','dev/mqueue','home'):
        (destination/name).mkdir(parents=True, exist_ok=True)
    result = {'env':{'MSYS2_ROOT':destination.as_posix()}, 'packages':packages,
              'files':len(files), 'installed_mib':round(sum((destination/name).stat().st_size for name in files)/1024**2, 1)}
    (root/'.tools/rtl.json').write_text(json.dumps(result, indent=2)+'\n', encoding='utf8')
    return result
