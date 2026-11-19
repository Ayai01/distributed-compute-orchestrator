"""Bind registered alternative input archives after provider selection."""

import json
import re

from .runtimes import native_architecture


def load_asset_variants(path):
    if not path.is_file():
        return ()
    groups = json.loads(path.read_bytes())
    if not isinstance(groups, list):
        raise ValueError('Asset variants require a list of platform-to-digest maps')
    used = set()
    for group in groups:
        if (not isinstance(group, dict) or len(group) < 2
                or any(not re.fullmatch(r'[a-z0-9_]+/[a-z0-9_]+', target)
                       or not isinstance(digest, str) or not re.fullmatch(r'[a-f0-9]{64}', digest)
                       for target, digest in group.items())
                or len(set(group.values())) != len(group) or used.intersection(group.values())):
            raise ValueError('Each asset variant group requires distinct platform and content identities')
        used.update(group.values())
    return tuple(groups)


def freeze_asset_variants(task, groups):
    """Qualify complete groups mounted only as standalone environment values."""
    matched = []
    env = task['env']
    for group in groups:
        identities = set(group.values())
        names = [(digest, name) for name, digest in task['assets'].items() if digest in identities]
        if len(names) != len(identities) or {digest for digest, _ in names} != identities:
            continue
        placeholders = {'${asset:' + name + '}' for _, name in names}
        if not placeholders.issubset(env.values()):
            continue
        other_env = {name: value for name, value in env.items() if value not in placeholders}
        other_fields = json.dumps(dict(task, env=other_env))
        if any(binding in other_fields for binding in placeholders):
            continue
        by_identity = dict(names)
        matched.append({target: by_identity[digest] for target, digest in group.items()})
    return matched


def bind_asset_variants(task, variants, health):
    target = str(health.get('platform', '')) + '/' + native_architecture(health.get('architecture'))
    omit = set()
    for group in variants:
        if target in group:
            omit.update(set(group.values()) - {group[target]})
    if not omit:
        return task
    placeholders = {'${asset:' + name + '}' for name in omit}
    return dict(task, assets={name: digest for name, digest in task['assets'].items() if name not in omit},
                env={name: value for name, value in task['env'].items() if value not in placeholders})
