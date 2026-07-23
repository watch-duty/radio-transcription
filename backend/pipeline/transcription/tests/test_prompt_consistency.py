"""Guard: backend transcription prompt must match the canonical SFT prompt.

The served Gemini model is fine-tuned with
``common.gemini.prompts.GEMINI_TRANSCRIBE_SYSTEM_PROMPT`` (in the separate
``radio-transcription-model`` package); backend inference sends
``GEMINI_PROMPT``. These must stay byte-identical or inference drifts from
training. The canonical copy is not importable here, so it is read by path.

This mirrors a guard in the model drift-guard test. Both exist because CI
path-filters the two lanes: a backend-only edit runs the backend tests but not
the model tests, and vice versa.
"""

from __future__ import annotations

import ast
import pathlib
import unittest

from backend.pipeline.transcription.transcribers.prompts import GEMINI_PROMPT

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
_CANONICAL_PROMPT = (
    _REPO_ROOT / "model" / "src" / "common" / "gemini" / "prompts.py"
)


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


class TestPromptConsistency(unittest.TestCase):
    def test_backend_prompt_matches_canonical_system_prompt(self) -> None:
        """GEMINI_PROMPT must match the canonical system prompt."""
        canonical = _module_constant(
            _CANONICAL_PROMPT, "GEMINI_TRANSCRIBE_SYSTEM_PROMPT"
        )
        self.assertIsNotNone(canonical)
        self.assertEqual(GEMINI_PROMPT, canonical)


if __name__ == "__main__":
    unittest.main()
