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

        self.assertIn("$55-175", readme)
        self.assertIn("training tokens = dataset tokens x epochs", readme)
        self.assertIn("Gemini 3.1 Flash-Lite supervised fine-tuning", readme)
        self.assertIn("$3 per 1M training tokens", readme)
        self.assertIn(
            "https://cloud.google.com/vertex-ai/generative-ai/pricing",
            readme,
        )

    def test_cost_section_mentions_confirm_for_automation(self) -> None:
        readme = _README.read_text()

        self.assertIn("non-interactive automation", readme)
        self.assertIn("--confirm", readme)

    def test_docker_runtime_is_documented_as_default(self) -> None:
        readme = _README.read_text()

        self.assertIn("Default local runtime", readme)
        self.assertLess(readme.index("## Runtime"), readme.index("## Local"))

    def test_config_driven_tune_contract_is_documented(self) -> None:
        readme = _README.read_text()

        self.assertIn(
            "python pipeline.py tune --config /path/to/run.toml --confirm",
            readme,
        )
        self.assertIn("gs://<bucket>/sft/runs/<round-id>/", readme)
        self.assertIn("local results/<round-id>/ is a mirror/cache", readme)
        self.assertIn("all --config", readme)
        self.assertIn("ONE, TWO, FOUR, EIGHT, SIXTEEN", readme)


if __name__ == "__main__":
    unittest.main()
