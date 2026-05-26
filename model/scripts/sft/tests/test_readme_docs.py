"""Documentation regression tests for the Gemini SFT pipeline README."""

from __future__ import annotations

import unittest
from pathlib import Path

_SFT_DIR = Path(__file__).resolve().parent.parent
_README = _SFT_DIR / "README.md"


class TestGeminiSftReadme(unittest.TestCase):
    def test_subcommands_are_described_as_gemini_sft(self) -> None:
        readme = _README.read_text()

        self.assertIn(
            "python pipeline.py build   Build Gemini SFT JSONL",
            readme,
        )
        self.assertIn(
            "python pipeline.py tune    Submit Vertex AI Gemini SFT tuning job",
            readme,
        )

    def test_cost_estimate_links_pricing_basis(self) -> None:
        readme = _README.read_text()

        self.assertIn("$90-290", readme)
        self.assertIn("training tokens = dataset tokens x epochs", readme)
        self.assertIn("Gemini 2.5 Flash supervised fine-tuning", readme)
        self.assertIn("$5 per 1M training tokens", readme)
        self.assertIn(
            "https://cloud.google.com/vertex-ai/generative-ai/pricing",
            readme,
        )

    def test_docker_runtime_is_documented_as_default(self) -> None:
        readme = _README.read_text()

        self.assertIn("Default local runtime", readme)
        self.assertLess(readme.index("## Runtime"), readme.index("## Local"))


if __name__ == "__main__":
    unittest.main()
