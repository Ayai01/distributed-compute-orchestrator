"""Small stdlib operations shared by the supported Python 3.7–3.15 range."""

def is_relative_to(path, root):
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def unlink(path):
    try:
        path.unlink()
    except FileNotFoundError:
        pass
