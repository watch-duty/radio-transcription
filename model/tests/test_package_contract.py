from __future__ import annotations

import importlib.metadata
import unittest


class TestModelPackageContract(unittest.TestCase):
    def test_distribution_exposes_expected_packages(self) -> None:
        self.assertEqual(
            importlib.metadata.metadata("radio-transcription-model")["Name"],
            "radio-transcription-model",
        )

        import common
        import common.gemini.prompts
        import common.gemini.tuning_data
        import common.gemini.vertex
        import gemini_sft.cli

        self.assertTrue(common.__doc__)
        self.assertIn("/model/src/common/", str(common.__file__))
        self.assertTrue(common.gemini.prompts.GEMINI_TRANSCRIBE_SYSTEM_PROMPT)
        self.assertTrue(
            callable(common.gemini.tuning_data.build_audio_tuning_example)
        )
        self.assertTrue(
            callable(common.gemini.tuning_data.validate_audio_tuning_example)
        )
        self.assertTrue(callable(common.gemini.vertex.build_request))
        self.assertTrue(callable(gemini_sft.cli.main))

    def test_distribution_exposes_gemini_sft_console_script(self) -> None:
        scripts = {
            entry_point.name: entry_point.value
            for entry_point in importlib.metadata.entry_points(
                group="console_scripts"
            )
        }

        self.assertEqual(scripts["gemini-sft"], "gemini_sft.cli:main")


if __name__ == "__main__":
    unittest.main()
