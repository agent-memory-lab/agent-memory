#!/usr/bin/env sh
set -eu

if [ -n "${PYTHON_BIN:-}" ]; then
  if ! "$PYTHON_BIN" -c 'import sys; raise SystemExit(sys.version_info < (3, 11))'; then
    printf '%s\n' "PYTHON_BIN must point to Python 3.11 or newer." >&2
    exit 1
  fi
else
  PYTHON_BIN=""
  for candidate in python3.13 python3.12 python3.11 python3; do
    if command -v "$candidate" >/dev/null 2>&1 \
      && "$candidate" -c 'import sys; raise SystemExit(sys.version_info < (3, 11))'; then
      PYTHON_BIN="$candidate"
      break
    fi
  done
  if [ -z "$PYTHON_BIN" ]; then
    printf '%s\n' "Python 3.11 or newer is required. Install it or set PYTHON_BIN=/path/to/python3.11." >&2
    exit 1
  fi
fi

printf '%s\n' "Using Python: $($PYTHON_BIN --version)"
"$PYTHON_BIN" -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e '.[dev]'

if [ "${1:-}" = "--all" ]; then
  python -m pip install -e packages/evolution
  python -m pip install -e packages/langgraph
  python -m pip install -e packages/python-sdk
  python -m pip install -e packages/mcp-server
  python -m pip install -e packages/postgres
fi

printf '%s\n' "Environment ready. Activate it with: . .venv/bin/activate"
