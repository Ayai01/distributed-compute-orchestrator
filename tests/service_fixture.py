"""Isolate terminal discovery in subprocess tests of other service behavior.

The real entry point is exercised without this fixture by test_startup. Copies
use the same shim across reloads; production has no visibility override flag.
"""
from pathlib import Path
import sys


ENTRY = '''import sys
from unittest.mock import patch
from remote_compute.cli import main
with patch('remote_compute.cli.require_visible'):
    raise SystemExit(main())
'''


def command(*args):
    return [sys.executable, '-B', '-c', ENTRY, '--service-child', *args]


def write_entry(root):
    Path(root, 'main.py').write_text(ENTRY, encoding='utf-8')
