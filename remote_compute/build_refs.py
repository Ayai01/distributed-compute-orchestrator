"""Portable job outputs backed by the existing content-addressed asset store."""

import hashlib
import io
import json
import os
from functools import lru_cache
from pathlib import Path, PurePosixPath
import re
import stat
import sys
import zipfile

from .bundle import MAX_BODY, atomic_json, inside, json_bytes
from .runtimes import native_architecture


MOUNTS = {'artifact_ref': 'dco_artifact', 'checkpoint_ref': 'dco_checkpoint'}
REFERENCE_FIELDS = frozenset((*MOUNTS, 'produces_artifact_ref', 'checkpoint_output', 'affinity_keys'))
CHECKPOINT_MANIFEST = '.dco-checkpoint.json'


def _digest(value):
    if not isinstance(value, str) or not re.fullmatch(r'[a-f0-9]{64}', value):
        raise ValueError('Artifact reference and recipe keys require a SHA256 digest')
    return value


def output_path(value):
    if not isinstance(value, str) or not value or '\\' in value or value != PurePosixPath(value).as_posix():
        raise ValueError('A published output requires a relative POSIX file path')
    inside(Path.cwd(), value)
    return value


def declaration(value, *, kind='artifact'):
    if not isinstance(value, dict) or set(value) != {'paths', 'recipe_sha256'}:
        raise ValueError('A produced reference requires paths and recipe_sha256')
    paths = value['paths']
    if not isinstance(paths, list) or not 1 <= len(paths) <= 32:
        raise ValueError('A produced reference requires 1..32 output file paths')
    paths = sorted({output_path(path) for path in paths})
    if kind not in ('artifact', 'checkpoint'):
        raise ValueError('Unknown reference kind')
    return dict(paths=paths, recipe_sha256=_digest(value['recipe_sha256']))


def validate_fields(task):
    for field in MOUNTS.keys() & task.keys():
        if task[field] is not None:
            _digest(task[field])
    for field, kind in (('produces_artifact_ref', 'artifact'), ('checkpoint_output', 'checkpoint')):
        if field not in task:
            continue
        task[field] = declaration(task[field], kind=kind)
        for path in task[field]['paths']:
            if path not in task['outputs']:
                raise ValueError('Published files must be listed as exact outputs')
            if path == CHECKPOINT_MANIFEST:
                raise ValueError('The DCO checkpoint manifest path is reserved')
    if 'affinity_keys' in task:
        keys = task['affinity_keys']
        if not isinstance(keys, list) or len(keys) > len(MOUNTS):
            raise ValueError('affinity_keys requires an array of the task’s references')
        allowed = {task[field] for field in MOUNTS if task.get(field) is not None}
        if any(_digest(key) not in allowed for key in keys):
            raise ValueError('affinity_keys may only contain this task’s artifact_ref and checkpoint_ref')
    if task.keys() & REFERENCE_FIELDS and not task.get('project'):
        raise ValueError('Portable references require a named project')


@lru_cache(maxsize=4)
def worker_abi(architecture):
    libc = os.confstr('CS_GNU_LIBC_VERSION') if sys.platform.startswith('linux') and 'CS_GNU_LIBC_VERSION' in os.confstr_names else None
    return [sys.platform, native_architecture(architecture), libc or '']


def provider_abi(health):
    abi = health.get('artifact_abi')
    if (not isinstance(abi, list) or len(abi) != 3 or not all(isinstance(item, str) for item in abi)
            or not abi[1] or (abi[0].startswith('linux') and not abi[2])):
        return None
    return tuple(abi)


def _record_path(root, project, ref):
    if not isinstance(project, str) or not project:
        raise ValueError('Portable references require a named project')
    scope = hashlib.sha256(project.encode()).hexdigest()
    return root / 'artifact-references' / scope / (_digest(ref) + '.json')


def resolve(root, project, ref, kind):
    try:
        record = json.loads(_record_path(root, project, ref).read_bytes())
    except FileNotFoundError:
        raise ValueError('Published ' + kind + ' reference is unavailable for this project') from None
    if record.get('id') != ref or record.get('project') != project or record.get('kind') != kind:
        raise ValueError('Artifact reference does not match the requested project and kind')
    return record


def bind(root, task):
    records = {name: resolve(root, task['project'], ref, field.removesuffix('_ref'))
               for field, name in MOUNTS.items() if (ref := task.get(field)) is not None}
    if not records:
        return task
    assets = dict(task['assets'])
    abi = {tuple(record['abi']) for record in records.values()}
    if len(abi) != 1:
        raise ValueError('Artifact and checkpoint require incompatible worker ABIs')
    for name, record in records.items():
        if name in assets:
            raise ValueError('Duplicate reserved artifact mount: ' + name)
        assets[name] = record['asset']
    keys = task.get('affinity_keys', [record['id'] for record in records.values()])
    return dict(task, assets=assets, _artifact_abi=next(iter(abi)),
                _artifact_mounts={name: record['asset'] for name, record in records.items()},
                _affinity_assets=tuple(sorted({record['asset'] for record in records.values() if record['id'] in keys})))


def output_archive(raw, identity, paths, *, checkpoint=False):
    """Select immutable receipt files while preserving executable permissions."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(io.BytesIO(raw)) as source, zipfile.ZipFile(buffer, 'w') as target:
        receipt = json.loads(source.read('result.json'))
        if receipt.get('id') != identity:
            raise ValueError('Publication receipt differs from producer')
        checksums = None
        if checkpoint:
            try:
                manifest = source.getinfo('files/' + CHECKPOINT_MANIFEST)
            except KeyError:
                return None
            if manifest.file_size > 8192:
                raise ValueError('Checkpoint manifest exceeds its size limit')
            checksums = json.loads(source.read(manifest))
            if not isinstance(checksums, dict) or set(checksums) != set(paths):
                raise ValueError('Checkpoint receipt does not match the declared outputs')
        entries = []
        for path in paths:
            name = 'files/' + path
            try:
                original = source.getinfo(name)
            except KeyError:
                raise ValueError('Produced output is missing: ' + path) from None
            if stat.S_IFMT(original.external_attr >> 16) not in (0, stat.S_IFREG):
                raise ValueError('Published output must be a regular file')
            entries.append((path, original))
        if sum(original.file_size for _, original in entries) > MAX_BODY:
            raise ValueError('Published outputs exceed the asset size limit')
        for path, original in entries:
            entry = zipfile.ZipInfo(path)
            entry.external_attr = original.external_attr
            entry.create_system = original.create_system
            entry.compress_type = original.compress_type
            data = source.read(original)
            if checksums is not None and hashlib.sha256(data).hexdigest() != checksums[path]:
                raise ValueError('Checkpoint output digest changed after publication')
            target.writestr(entry, data, compresslevel=1)
    if buffer.tell() > MAX_BODY:
        raise ValueError('Published asset exceeds the asset size limit')
    return buffer.getvalue()


def report_checkpoint(paths):
    """Commit the digests of complete job outputs for checkpoint publication."""
    base = os.environ.get('REMOTE_COMPUTE_CHECKPOINT_ROOT')
    if not base:
        raise RuntimeError('This DCO task has no declared checkpoint output')
    root = Path(base)
    completed = {}
    for name in paths:
        name = output_path(name)
        with inside(root, name).open('rb') as stream:
            completed[name] = hashlib.file_digest(stream, 'sha256').hexdigest()
    atomic_json(root / CHECKPOINT_MANIFEST, completed, private=True)


def register(center, *, project, producer_id, kind, spec, abi, archive):
    asset = hashlib.sha256(archive).hexdigest()
    binding = dict(project=project, kind=kind, recipe_sha256=spec['recipe_sha256'],
                   paths=spec['paths'], asset=asset, abi=list(abi))
    ref = hashlib.sha256(json_bytes(binding)).hexdigest()
    path = _record_path(center.root, project, ref)
    with center.submission_lock.hold(ref):
        center.asset(asset, archive)
        if not path.is_file():
            path.parent.mkdir(parents=True, exist_ok=True)
            atomic_json(path, dict(binding, id=ref, producer_id=producer_id))
        return resolve(center.root, project, ref, kind)


def public(record):
    return {key: record[key] for key in ('id', 'kind', 'paths', 'producer_id', 'recipe_sha256')}
