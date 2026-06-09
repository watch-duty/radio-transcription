#!/usr/bin/env bash
set -euo pipefail

MODEL_DIR="${ASR_MODEL_DIR:-/workspace/model}"
WORKSPACE_DIR="${ASR_WORKSPACE_DIR:-/workspace}"

git config --global --add safe.directory "${WORKSPACE_DIR}"

if [ -f "${MODEL_DIR}/pyproject.toml" ]; then
  /opt/conda/bin/python -m pip install --no-cache-dir -e "${MODEL_DIR}[scoring,vertex]"
else
  echo "warning: ${MODEL_DIR}/pyproject.toml not found; skipping editable model package install" >&2
fi

exec "$@"
