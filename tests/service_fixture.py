"""Isolate terminal discovery in subprocess tests of other service behavior.

The real entry point is exercised without this fixture by test_startup. Copies
use the same shim across reloads; production has no visibility override flag.
"""
from pathlib import Path
import sys


ENTRY = '''import sys
import tempfile
from contextlib import nullcontext
from pathlib import Path
from unittest.mock import patch
from remote_compute.cli import main
with tempfile.TemporaryDirectory(prefix='dco-service-config-') as directory:
    if sys.argv[0]=='-c' and '--config' not in sys.argv:
        path=Path(directory)/'worker.json'
        path.write_text('{}',encoding='utf8')
        sys.argv[1:1]=['--config',str(path)]
    with patch('remote_compute.cli.require_visible'), patch('remote_compute.cli.console_close',return_value=nullcontext()):
        raise SystemExit(main())
'''


def command(*args):
    return [sys.executable, '-B', '-c', ENTRY, '--service-child', *args]


def write_entry(root):
    Path(root, 'main.py').write_text(ENTRY, encoding='utf-8')
