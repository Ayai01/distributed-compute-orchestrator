"""Build the same content groups used by each project's normal task submission.

The caller runs one project per Python process with its root on sys.path. This
keeps project imports and their configuration isolated from the DCO service.
"""

import hashlib
import importlib
import json
from pathlib import Path
import re


class _Capture:
    def __init__(self):
        self.files = {}

    def sync_files(self, files, **_options):
        identity = str(len(self.files))
        self.files[identity] = dict(files() if callable(files) else files)
        return identity


def _group(files, *requires):
    return dict(files=files, requires=list(requires))


def _aop(root):
    # Center.__init__ uses Paths here; preserve executable-mode metadata too.
    sources = {'config.py': root/'config.py'}
    for package in ('analog', 'compute', 'utils'):
        sources.update((path.relative_to(root).as_posix(), path)
                       for path in (root/package).rglob('*.py'))
    result = {'aop_code': _group(sources, 'aop_python')}
    simulation_jobs = importlib.import_module('analog.simulators').simulation_jobs
    model_bundle = importlib.import_module('compute.models').model_bundle
    token = re.compile(r'__AOP_MODEL_([0-9a-f]{64})__')
    seen = set()
    for path in sorted((root/'json').glob('AO_cfg_*.json')):
        config = json.loads(path.read_text(encoding='utf-8-sig'))
        config.setdefault('project_root', str(root))
        backend = str(config.get('simulator', 'hspice')).lower()
        engine = 'ngspice47' if backend == 'ngspice' else backend
        for index, job in enumerate(simulation_jobs(config)):
            _, files, blobs = model_bundle(job['netlist'], config)
            if not files:
                continue
            # Keep Center.execute's cooked include names and bytes identical.
            models = {name+'.lib': token.sub(lambda match: match[1]+'.lib',
                       blobs[digest].decode('latin-1')).encode('latin-1')
                      for name, digest in files.items()}
            fingerprint = (engine, tuple(sorted((name, hashlib.sha256(data).digest())
                                                for name, data in models.items())))
            if fingerprint not in seen:
                seen.add(fingerprint)
                result['aop_models_'+path.stem+'_'+str(index)] = _group(models, 'aop_python', engine)
    return result


def _rgcn(root):
    native = importlib.import_module('src.remote_native')
    code = native.native_code(root)
    result = {'rgcn_native': _group(code, 'rgcn_python', native.NATIVE_TOOL)}
    # RemoteStudy.__init__ extends the native source with current configs and
    # their explicitly referenced validation decks, never experiment history.
    research = dict(code)
    for path in (root/'configs').glob('*.json'):
        research[path.relative_to(root).as_posix()] = path.read_bytes()
        pre = json.loads(path.read_text(encoding='utf-8')).get('circuit', {}).get('pre', {})
        for field in ('netlist', 'reference_netlist'):
            if pre.get(field):
                template = (root/pre[field]).resolve()
                research[template.relative_to(root).as_posix()] = template.read_bytes()
    research['tools/research_worker.py'] = (root/'tools/research_worker.py').read_bytes()
    result['rgcn_research'] = _group(research, 'rgcn_train')
    pdk = importlib.import_module('src.eda').resolve_tools(root).get('pdk_root')
    if pdk:
        models = native.native_models(pdk)
        result['rgcn_pdk'] = _group(models, 'rgcn_python', native.NATIVE_TOOL)
        result['rgcn_research_pdk'] = _group(models, 'rgcn_train', native.NATIVE_TOOL)
    return result


def groups(root, project):
    """Return explicit source/model groups without uploading or running work."""
    root = Path(root).resolve()
    if project == 'aop':
        return _aop(root)
    if project == 'rgcn':
        return _rgcn(root)
    if project == 'bench':
        capture = _Capture()
        assets = importlib.import_module('bench.remote')._source_assets(capture)
        return {name: _group(capture.files[identity], 'bench_python')
                for name, identity in assets.items()}
    if project == 'arh':
        files = importlib.import_module('framework.source_snapshot').files(root)
        return {'arh_source': _group(files, 'arh_python', 'shared:arh')}
    raise ValueError('Unknown cache project: '+str(project))
