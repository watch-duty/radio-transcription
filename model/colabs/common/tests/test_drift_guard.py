"""Drift guard for shared Gemini prompt/request plumbing.

The scripts prompt module is imported at runtime so comments or dead strings cannot
satisfy the guard. The notebook is parsed as code cells and checked for real import
statements instead of raw string matches.
"""

from __future__ import annotations

import ast
import importlib.util
import json
import sys
import unittest
from pathlib import Path

# tests/ -> common/ -> colabs/ -> model/
_MODEL_DIR = Path(__file__).resolve().parents[3]
_COLABS_DIR = _MODEL_DIR / "colabs"
_SCRIPTS_PROMPTS = _MODEL_DIR / "scripts" / "sft" / "prompts.py"
_NOTEBOOK = _MODEL_DIR / "colabs" / "gemini_transcribe_audio.ipynb"


def _load_scripts_prompts_module():
    spec = importlib.util.spec_from_file_location(
        "scripts_sft_prompts_for_drift_guard", _SCRIPTS_PROMPTS
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load {_SCRIPTS_PROMPTS}")

    module = importlib.util.module_from_spec(spec)
    old_sys_path = sys.path[:]
    try:
        sys.path.insert(0, str(_COLABS_DIR))
        spec.loader.exec_module(module)
    finally:
        sys.path[:] = old_sys_path
    return module


def _notebook_imports() -> set[tuple[str | None, str]]:
    notebook = json.loads(_NOTEBOOK.read_text())
    imports: set[tuple[str | None, str]] = set()
    for cell in notebook.get("cells", []):
        if cell.get("cell_type") != "code":
            continue
        source = "".join(cell.get("source", []))
        python_source = "\n".join(
            line
            for line in source.splitlines()
            if not line.lstrip().startswith(("!", "%"))
        )
        if not python_source.strip():
            continue
        try:
            tree = ast.parse(python_source)
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                for alias in node.names:
                    imports.add((node.module, alias.name))
    return imports


class TestDriftGuard(unittest.TestCase):
    def test_scripts_prompts_reexports_runtime_common_prompts(self) -> None:
        """scripts/sft/prompts.py must source prompts from common.prompts at runtime."""
        from common.prompts import (
            GEMINI_TRANSCRIBE_SYSTEM_PROMPT,
            GEMINI_TRANSCRIBE_USER_PROMPT,
        )

        module = _load_scripts_prompts_module()

        self.assertIs(
            module.PIPELINE_SYSTEM_PROMPT,
            GEMINI_TRANSCRIBE_SYSTEM_PROMPT,
        )
        self.assertIs(
            module.PIPELINE_USER_PROMPT,
            GEMINI_TRANSCRIBE_USER_PROMPT,
        )

    def test_notebook_imports_canonical_prompt_symbols(self) -> None:
        """The eval notebook must import canonical prompt symbols from common."""
        imports = _notebook_imports()

        self.assertIn(
            ("common.prompts", "GEMINI_TRANSCRIBE_SYSTEM_PROMPT"),
            imports,
        )
        self.assertIn(
            ("common.prompts", "GEMINI_TRANSCRIBE_USER_PROMPT"),
            imports,
        )

    def test_notebook_imports_canonical_vertex_helpers(self) -> None:
        """The eval notebook must use common.vertex for request and batch calls."""
        imports = _notebook_imports()

        self.assertIn(("common.vertex", "build_request"), imports)
        self.assertIn(("common.vertex", "submit_batch_inference"), imports)


if __name__ == "__main__":
    unittest.main()
