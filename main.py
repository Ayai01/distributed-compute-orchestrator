"""Unified Python entry point for scheduling and compute provision."""
import sys

sys.dont_write_bytecode = True
for stream in (sys.stdout, sys.stderr):
    if hasattr(stream, "reconfigure"):
        stream.reconfigure(encoding="utf-8", errors="replace")

from remote_compute.cli import main

if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except MemoryError:
        import os
        os._exit(76)  # Native supervisor waits for real memory recovery.
