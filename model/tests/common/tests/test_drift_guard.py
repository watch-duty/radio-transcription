"""Drift guard for shared Gemini prompt/request plumbing."""

from __future__ import annotations

import ast
import json
import tempfile
import unittest
from pathlib import Path

from common.gemini.prompts import (
    GEMINI_TRANSCRIBE_SYSTEM_PROMPT,
    GEMINI_TRANSCRIBE_USER_PROMPT,
)
from gemini_sft.config import load_run_config

_MODEL_DIR = Path(__file__).resolve().parents[3]
_NOTEBOOK = _MODEL_DIR / "colabs" / "gemini_transcribe_audio.ipynb"
_SRC_DIR = _MODEL_DIR / "src"
_SCRIPTS_DIR = _MODEL_DIR / "scripts"


def _notebook_imports() -> set[tuple[str | None, str]]:
    notebook = json.loads(_NOTEBOOK.read_text(encoding="utf-8"))
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


def _python_imports(path: Path) -> set[tuple[str | None, str]]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    imports: set[tuple[str | None, str]] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            for alias in node.names:
                imports.add((node.module, alias.name))
        elif isinstance(node, ast.Import):
            for alias in node.names:
                imports.add((None, alias.name))
    return imports


class TestDriftGuard(unittest.TestCase):
    def test_gemini_sft_config_defaults_to_runtime_common_prompts(self) -> None:
        """SFT config defaults must source prompts from common.gemini.prompts."""
        with tempfile.TemporaryDirectory() as tmp:
            cfg_path = Path(tmp) / "run.toml"
            cfg_path.write_text(
                """
round_id = "round"
dataset = "wd"
inference_dataset_slug = "echo/eval"
train_manifest_uri = "gs://bucket/train.jsonl"
validation_manifest_uri = "gs://bucket/validation.jsonl"
eval_manifest_uri = "gs://bucket/eval.jsonl"

[gcp]
project = "project"
bucket = "bucket"
location = "us-central1"

[sft]
base_model = "gemini-3.1-flash-lite"
epoch_count = 4
adapter_size = "FOUR"
learning_rate_multiplier = 1.0
""",
                encoding="utf-8",
            )
            cfg = load_run_config(cfg_path)

        self.assertEqual(cfg.system_prompt, GEMINI_TRANSCRIBE_SYSTEM_PROMPT)
        self.assertEqual(cfg.user_prompt, GEMINI_TRANSCRIBE_USER_PROMPT)

    def test_notebook_imports_canonical_prompt_symbols(self) -> None:
        """The eval notebook must import canonical prompt symbols from common.gemini."""
        imports = _notebook_imports()

        self.assertIn(
            ("common.gemini.prompts", "GEMINI_TRANSCRIBE_SYSTEM_PROMPT"),
            imports,
        )
        self.assertIn(
            ("common.gemini.prompts", "GEMINI_TRANSCRIBE_USER_PROMPT"),
            imports,
        )

    def test_notebook_imports_canonical_vertex_helpers(self) -> None:
        """The eval notebook must use common.gemini.vertex for request and batch calls."""
        imports = _notebook_imports()

        self.assertIn(("common.gemini.vertex", "build_request"), imports)
        self.assertIn(
            ("common.gemini.vertex", "submit_batch_inference"), imports
        )

    def test_packaged_eval_uses_shared_context_builder(self) -> None:
        imports = _python_imports(_SRC_DIR / "gemini_sft" / "evaluate.py")

        self.assertIn(
            ("common.gemini.context", "build_context_histories"), imports
        )

    def test_target_execution_uses_shared_vertex_request_helpers(self) -> None:
        imports = _python_imports(
            _SRC_DIR / "gemini_sft" / "target_execution.py"
        )

        self.assertIn(("common.gemini.vertex", "build_request"), imports)
        self.assertIn(
            ("common.gemini.vertex", "GEMINI_GENERATION_CONFIG"), imports
        )
        self.assertIn(
            ("common.gemini.vertex", "GEMINI_SAFETY_SETTINGS"), imports
        )

    def test_tuning_data_uses_shared_context_prompt_helpers(self) -> None:
        imports = _python_imports(
            _SRC_DIR / "common" / "gemini" / "tuning_data.py"
        )

        self.assertIn(
            ("common.gemini.context", "build_prior_text_user_turn"), imports
        )
        self.assertIn(
            ("common.gemini.context", "build_transcript_context_prompt"),
            imports,
        )
        self.assertIn(
            (
                "common.gemini.context",
                "build_vapo_p3_transcript_context_prompt",
            ),
            imports,
        )

    def test_checkpoint_scorer_uses_packaged_online_executor(self) -> None:
        imports = _python_imports(
            _SCRIPTS_DIR / "sft" / "score_gemini_sft_checkpoints_online.py"
        )

        self.assertIn(
            ("gemini_sft.target_execution", "run_online_target_inference"),
            imports,
        )
