#!/usr/bin/env bash
set -euo pipefail

MODEL_DIR="${ASR_MODEL_DIR:-/workspace/model}"

if [ -f "${MODEL_DIR}/pyproject.toml" ]; then
  /opt/conda/bin/python -m pip install --no-cache-dir -e "${MODEL_DIR}[scoring,vertex]"
else
  echo "warning: ${MODEL_DIR}/pyproject.toml not found; skipping editable common install" >&2
fi

exec "$@"
