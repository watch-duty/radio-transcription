"""Drift guards for shared Gemini plumbing and operator documentation."""

from __future__ import annotations

import ast
import json
import pathlib
import re
import subprocess
import tempfile
import unittest

from common.gemini import prompts
from gemini_sft import config as config_lib
from gemini_sft import reporting

_MODEL_DIR = pathlib.Path(__file__).resolve().parents[3]
_REPO_ROOT = _MODEL_DIR.parent
_NOTEBOOK = _MODEL_DIR / "colabs" / "gemini_transcribe_audio.ipynb"
_SRC_DIR = _MODEL_DIR / "src"
_SCRIPTS_DIR = _MODEL_DIR / "scripts"
_BACKEND_PROMPT = (
    _REPO_ROOT
    / "backend"
    / "pipeline"
    / "transcription"
    / "transcribers"
    / "prompts.py"
)


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


def _python_calls(path: pathlib.Path) -> set[tuple[str, str]]:
    """Collect direct module-alias method calls from one Python source file.

    Args:
        path: Python source file to parse.

    Returns:
        Pairs containing the base name and called attribute.
    """
    tree = ast.parse(path.read_text(encoding="utf-8"))
    calls: set[tuple[str, str]] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        function = node.func
        if not isinstance(function, ast.Attribute):
            continue
        if isinstance(function.value, ast.Name):
            calls.add((function.value.id, function.attr))
    return calls


def _module_constant(path: pathlib.Path, name: str) -> str | None:
    """Read a module-level string constant's value via AST, no import.

    Args:
        path: Python source file to parse.
        name: Module-level assignment name to read.

    Returns:
        The constant's string value, or None if the name is not assigned.
    """
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == name
            for target in node.targets
        ):
            return ast.literal_eval(node.value)
    return None


class TestDriftGuard(unittest.TestCase):
    def test_backend_transcriber_prompt_matches_canonical_system_prompt(
        self,
    ) -> None:
        """Backend GEMINI_PROMPT must match the canonical SFT system prompt.

        The served model is fine-tuned with the canonical prompt, so backend
        inference must send byte-identical text or it drifts from training. A
        companion guard lives in the transcription package tests because CI
        path-filters the lanes: a backend-only edit skips this model lane.
        """
        backend_prompt = _module_constant(_BACKEND_PROMPT, "GEMINI_PROMPT")
        self.assertIsNotNone(backend_prompt)
        self.assertEqual(
            backend_prompt, prompts.GEMINI_TRANSCRIBE_SYSTEM_PROMPT
        )

    def test_gemini_sft_config_defaults_to_runtime_common_prompts(self) -> None:
        """SFT config defaults must source prompts from common.gemini.prompts."""
        with tempfile.TemporaryDirectory() as tmp:
            cfg_path = pathlib.Path(tmp) / "run.toml"
            cfg_path.write_text(
                """
round_id = "round"
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
            cfg = config_lib.load_run_config(cfg_path)

        self.assertEqual(
            cfg.system_prompt,
            prompts.GEMINI_TRANSCRIBE_SYSTEM_PROMPT,
        )
        self.assertEqual(
            cfg.user_prompt,
            prompts.GEMINI_TRANSCRIBE_USER_PROMPT,
        )

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

    def test_packaged_eval_uses_prediction_only_rolling_schedule(self) -> None:
        """Packaged eval must use the transcript-free rolling data flow."""
        evaluate_calls = _python_calls(_SRC_DIR / "gemini_sft" / "evaluate.py")
        target_calls = _python_calls(
            _SRC_DIR / "gemini_sft" / "target_execution.py"
        )

        self.assertIn(
            ("artifacts_lib", "eval_rows_for_inference_from_entries"),
            evaluate_calls,
        )
        self.assertIn(
            ("context", "build_strict_causal_schedule"),
            target_calls,
        )
        self.assertNotIn(
            ("context", "build_training_reference_histories"),
            evaluate_calls,
        )

    def test_target_execution_uses_shared_vertex_request_helpers(self) -> None:
        """Online target execution must call shared Vertex helpers."""
        calls = _python_calls(_SRC_DIR / "gemini_sft" / "target_execution.py")

        self.assertIn(("vertex", "build_request"), calls)
        self.assertIn(("vertex", "resource_location"), calls)

    def test_tuning_data_uses_shared_content_builder(self) -> None:
        """Tuning examples must call the shared content builder."""
        calls = _python_calls(_SRC_DIR / "common" / "gemini" / "tuning_data.py")

        self.assertIn(
            ("context", "build_training_transcription_contents"),
            calls,
        )

    def test_vertex_request_uses_shared_content_builder(self) -> None:
        """Batch requests must call the shared content builder."""
        calls = _python_calls(_SRC_DIR / "common" / "gemini" / "vertex.py")

        self.assertIn(
            ("context", "build_evaluation_transcription_contents"),
            calls,
        )

    def test_sft_example_config_uses_singular_eval_model(self) -> None:
        """The committed example must expose one eval target and controls."""
        text = (_SCRIPTS_DIR / "sft" / "run_config.example.toml").read_text(
            encoding="utf-8"
        )

        self.assertIn("[eval.model]", text)
        self.assertIn("one model per config", text)
        self.assertIn("[eval.execution]", text)
        self.assertIn("max_retries = 3", text)

    def test_sft_operator_metric_docs_track_report_columns(self) -> None:
        """The metric glossary table must track public report columns."""
        text = (_SCRIPTS_DIR / "sft" / "docs" / "metrics.md").read_text(
            encoding="utf-8"
        )

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

    def test_sft_operator_docs_cover_eval_only_and_batch_resume(self) -> None:
        """Operator docs must cover eval-only and resumable batch flows."""
        configs = (_SCRIPTS_DIR / "sft" / "docs" / "configs.md").read_text(
            encoding="utf-8"
        )
        runbook = (_SCRIPTS_DIR / "sft" / "docs" / "runbook.md").read_text(
            encoding="utf-8"
        )
        artifacts = (_SCRIPTS_DIR / "sft" / "docs" / "artifacts.md").read_text(
            encoding="utf-8"
        )
        example = (_SCRIPTS_DIR / "sft" / "run_config.example.toml").read_text(
            encoding="utf-8"
        )

        self.assertIn("eval-only", configs)
        self.assertIn("batch_job.meta.json", runbook)
        self.assertIn("batch_job.meta.json", artifacts)
        self.assertIn("Training rounds additionally", artifacts)
        self.assertIn("prepared eval-only config", example)

    def test_sft_metric_docs_describe_unintelligible_case_folding(self) -> None:
        """The glossary must describe case-insensitive token matching."""
        text = (_SCRIPTS_DIR / "sft" / "docs" / "metrics.md").read_text(
            encoding="utf-8"
        )

        self.assertIn("case-insensitive", text)

    def test_sft_operator_hygiene_docs_and_gitignore_cover_local_artifacts(
        self,
    ) -> None:
        """Docs and ignore rules must agree on local experiment artifacts."""
        hygiene_text = (_SCRIPTS_DIR / "sft" / "docs" / "hygiene.md").read_text(
            encoding="utf-8"
        )
        runbook_text = (_SCRIPTS_DIR / "sft" / "docs" / "runbook.md").read_text(
            encoding="utf-8"
        )
        gitignore_text = (_REPO_ROOT / ".gitignore").read_text(encoding="utf-8")

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
                self.assertEqual(
                    0, ignored_check.returncode, ignored_check.stderr
                )

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
                self.assertEqual(
                    1, trackable_check.returncode, trackable_check.stderr
                )
