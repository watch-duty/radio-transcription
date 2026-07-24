#!/usr/bin/env python3
"""One-time next-round SFT dataset reconstruction entry point."""

# Import order is deliberately established only after model/src is pinned.
# ruff: noqa: I001

from __future__ import annotations

import pathlib
import sys


RUN_ROOT = pathlib.Path(__file__).resolve().parent
MODEL_SRC = RUN_ROOT.parents[3] / "src"
for import_root in (RUN_ROOT, MODEL_SRC):
    while str(import_root) in sys.path:
        sys.path.remove(str(import_root))
sys.path.insert(0, str(RUN_ROOT))
sys.path.insert(0, str(MODEL_SRC))

from dataset_run import cli  # noqa: E402


if __name__ == "__main__":
    raise SystemExit(cli.main())
