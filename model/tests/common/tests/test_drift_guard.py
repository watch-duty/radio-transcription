"""Drift guard for shared Gemini prompt/request plumbing."""

from __future__ import annotations

import ast
import json
import re
import subprocess
import tempfile
import unittest
from pathlib import Path

from common.gemini.prompts import (
    GEMINI_TRANSCRIBE_SYSTEM_PROMPT,
    GEMINI_TRANSCRIBE_USER_PROMPT,
)
from gemini_sft import reporting
from gemini_sft.config import load_run_config

_MODEL_DIR = Path(__file__).resolve().parents[3]
_REPO_ROOT = _MODEL_DIR.parent
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

[eval.model]
label = "base"
model = "gemini-3.1-flash-lite"
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

    def test_sft_example_config_uses_singular_eval_model(self) -> None:
        text = (_SCRIPTS_DIR / "sft" / "run_config.example.toml").read_text(
            encoding="utf-8"
        )

        self.assertIn("[eval.model]", text)
        self.assertIn("one model per config", text)
        self.assertIn("[eval.execution]", text)
        self.assertIn("max_retries = 3", text)
        self.assertNotIn("[[eval.models]]", text)

    def test_sft_operator_metric_docs_track_report_columns(self) -> None:
        text = (
            _SCRIPTS_DIR / "sft" / "docs" / "metrics.md"
        ).read_text(encoding="utf-8")

        documented_columns = []
        in_column_table = False
        for line in text.splitlines():
            if line == "| Column | Meaning |":
                in_column_table = True
                continue
            if in_column_table and not line.startswith("|"):
                break
            if not in_column_table or not line.startswith("| `"):
                continue
            parts = line.split("|")
            documented_columns.append(parts[1].strip().strip("`"))

        self.assertEqual(list(reporting.REPORT_COLUMNS), documented_columns)
        self.assertFalse(
            set(documented_columns)
            & {
                "empty_rate",
                "hallucination_rate",
                "hits",
                "correct_words",
            }
        )

    def test_sft_operator_hygiene_docs_and_gitignore_cover_local_artifacts(
        self,
    ) -> None:
        hygiene_text = (
            _SCRIPTS_DIR / "sft" / "docs" / "hygiene.md"
        ).read_text(encoding="utf-8")
        runbook_text = (
            _SCRIPTS_DIR / "sft" / "docs" / "runbook.md"
        ).read_text(encoding="utf-8")
        gitignore_text = (_REPO_ROOT / ".gitignore").read_text(
            encoding="utf-8"
        )

        expected_hygiene_terms = (
            ".local.toml",
            "results/",
            "model/scripts/sft/results/**/*.jsonl",
            "model/scripts/sft/results/**/*.jsonl.gz",
            "model/data/inference_manifests/*.jsonl",
            "online_predictions.jsonl",
            "git status --short --ignored",
            "git diff --cached --name-only",
        )
        for term in expected_hygiene_terms:
            with self.subTest(term=term):
                self.assertIn(term, hygiene_text)

        gitignore_lines = {
            line.strip()
            for line in gitignore_text.splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        }

        expected_gitignore_terms = (
            "*.local.toml",
            "/results/",
            "model/scripts/sft/results/**/*.jsonl",
            "model/scripts/sft/results/**/*.jsonl.gz",
            "model/data/inference_manifests/*.jsonl",
        )
        for term in expected_gitignore_terms:
            with self.subTest(term=term):
                self.assertIn(term, gitignore_lines)

        self.assertNotIn("results/", gitignore_lines)

        hygiene_patterns = re.findall(r"rg '([^']+)'", hygiene_text)
        runbook_patterns = re.findall(r"rg '([^']+)'", runbook_text)
        self.assertEqual(1, len(hygiene_patterns))
        self.assertEqual(hygiene_patterns, runbook_patterns)
        staged_artifact_pattern = re.compile(hygiene_patterns[0])

        blocked_paths = (
            "results/run/output.jsonl",
            "model/scripts/sft/results/run/output.jsonl",
            "model/scripts/sft/results/run/output.jsonl.gz",
            "model/data/inference_manifests/base.jsonl",
            "scratch.local.toml",
            "online_predictions.jsonl",
            "batch_predictions_output.jsonl",
        )
        for path in blocked_paths:
            with self.subTest(staged_path=path):
                self.assertRegex(path, staged_artifact_pattern)

        allowed_paths = (
            "model/scripts/sft/results/run/config.json",
            "model/scripts/sft/results/run/status.json",
            "model/scripts/sft/results/run/wer_summary.md",
            "model/scripts/sft/results/run/ledger.md",
        )
        for path in allowed_paths:
            with self.subTest(staged_path=path):
                self.assertNotRegex(path, staged_artifact_pattern)

        ignored_paths = (
            "results/run/output.jsonl",
            "model/scripts/sft/results/run/output.jsonl",
            "model/scripts/sft/results/run/output.jsonl.gz",
            "model/data/inference_manifests/base.jsonl",
            "model/data/inference_manifests/base.jsonl.gz",
            "scratch.local.toml",
        )
        for path in ignored_paths:
            with self.subTest(ignored_path=path):
                ignored_check = subprocess.run(
                    [
                        "git",
                        "-C",
                        str(_REPO_ROOT),
                        "check-ignore",
                        "--no-index",
                        path,
                    ],
                    capture_output=True,
                    check=False,
                    text=True,
                )
                self.assertEqual(0, ignored_check.returncode, ignored_check.stderr)

        for path in allowed_paths:
            with self.subTest(trackable_path=path):
                trackable_check = subprocess.run(
                    [
                        "git",
                        "-C",
                        str(_REPO_ROOT),
                        "check-ignore",
                        "--no-index",
                        path,
                    ],
                    capture_output=True,
                    check=False,
                    text=True,
                )
                self.assertEqual(1, trackable_check.returncode, trackable_check.stderr)
