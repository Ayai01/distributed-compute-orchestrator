#!/bin/sh
set -eu
cd -- "$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
export PYTHONUTF8=1 PYTHONDONTWRITEBYTECODE=1
if command -v python3.14 >/dev/null 2>&1; then
    exec python3.14 -B main.py "$@"
elif command -v python3 >/dev/null 2>&1; then
    exec python3 -B main.py "$@"
elif command -v python >/dev/null 2>&1; then
    exec python -B main.py "$@"
else
    exec .tools/python/bin/python3 -B main.py "$@"
fi
