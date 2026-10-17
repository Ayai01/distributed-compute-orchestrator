"""Small stdlib operations shared by the supported Python 3.7–3.15 range."""

import hashlib


def is_relative_to(path, root):
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def sha256(stream):
    digest = hashlib.sha256()
    for block in iter(lambda: stream.read(1024 * 1024), b''):
        digest.update(block)
    return digest.hexdigest()


def unlink(path):
    try:
        path.unlink()
    except FileNotFoundError:
        pass
