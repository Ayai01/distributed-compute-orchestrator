#!/bin/sh
set -eu
cd -- "$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
if [ -x .tools/python/bin/python3 ]; then
    exec .tools/python/bin/python3 -B main.py "$@"
elif command -v python3 >/dev/null 2>&1; then
    exec python3 -B main.py "$@"
else
    exec python -B main.py "$@"
fi
