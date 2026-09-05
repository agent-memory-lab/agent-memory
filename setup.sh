#!/usr/bin/env sh
set -eu

PYTHON_BIN="${PYTHON_BIN:-python3}"
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

