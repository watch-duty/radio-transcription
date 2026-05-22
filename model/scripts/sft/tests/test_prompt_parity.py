"""Drift-guard: PIPELINE_SYSTEM_PROMPT must stay byte-identical to the notebook prompt.

If this test fails, either:
  (a) Someone edited prompts.py without updating the notebook, or
  (b) Someone edited the notebook without updating prompts.py.

Fix: re-seed prompts.py from the notebook (source of truth per D-06).
"""
from __future__ import annotations

import json
import re
import sys
import unittest
from pathlib import Path

# Path from scripts/sft/tests/ -> sft/ -> scripts/ -> model/ -> colabs/
NOTEBOOK_PATH = (
    Path(__file__).resolve().parent.parent.parent.parent  # tests/ -> sft/ -> scripts/ -> model/
    / "colabs"
    / "gemini_transcribe_audio.ipynb"
)


def _extract_notebook_system_prompt() -> str:
    """Load the notebook and extract the inline SYSTEM_PROMPT from Cell 5."""
    with open(NOTEBOOK_PATH, encoding="utf-8") as f:
        nb = json.load(f)
    for cell in nb["cells"]:
        if cell["cell_type"] != "code":
            continue
        src = "".join(cell["source"])
        if "SYSTEM_PROMPT" not in src or "Evaluate all" not in src:
            continue
        m = re.search(r'SYSTEM_PROMPT = """(.+?)"""', src, re.DOTALL)
        if m:
            return m.group(1).strip()
    raise ValueError(
        f"Could not find SYSTEM_PROMPT in {NOTEBOOK_PATH}. "
        "Has the notebook cell been restructured?"
    )


class TestPromptParity(unittest.TestCase):
    def test_pipeline_system_prompt_matches_notebook(self) -> None:
        """D-06 hard constraint: PIPELINE_SYSTEM_PROMPT == notebook SYSTEM_PROMPT."""
        # Allow running from model/scripts/sft/ or from model/scripts/sft/tests/
        sft_dir = str(Path(__file__).resolve().parent.parent)
        if sft_dir not in sys.path:
            sys.path.insert(0, sft_dir)
        from prompts import PIPELINE_SYSTEM_PROMPT

        notebook_prompt = _extract_notebook_system_prompt()
        self.assertEqual(
            PIPELINE_SYSTEM_PROMPT,
            notebook_prompt,
            msg=(
                "PIPELINE_SYSTEM_PROMPT in scripts/sft/prompts.py does NOT match "
                "the inline SYSTEM_PROMPT in gemini_transcribe_audio.ipynb. "
                "Re-seed prompts.py from the notebook (D-06 source of truth)."
            ),
        )

    def test_pipeline_user_prompt_is_expected_value(self) -> None:
        sft_dir = str(Path(__file__).resolve().parent.parent)
        if sft_dir not in sys.path:
            sys.path.insert(0, sft_dir)
        from prompts import PIPELINE_USER_PROMPT

        expected = (
            "Transcribe this emergency radio communication segment verbatim per the rules above."
        )
        self.assertEqual(PIPELINE_USER_PROMPT, expected)


if __name__ == "__main__":
    unittest.main()
