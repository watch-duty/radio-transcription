import os
import subprocess
import sys
import unittest
from pathlib import Path

from common.gemini.context import ContextTurn
from common.gemini.tuning_data import (
    build_audio_tuning_example,
    validate_audio_tuning_example,
)

# model/src must be on PYTHONPATH in subprocess calls so `import common` resolves.
_SRC_DIR = str(Path(__file__).resolve().parents[3] / "src")


def _first_file_part(turn: dict) -> dict:
    return next(part for part in turn["parts"] if "fileData" in part)


class TestBuildExample(unittest.TestCase):
    def test_round_trips_audio_uri(self) -> None:
        example = build_audio_tuning_example(
            audio_uri="gs://bucket/seg001.flac",
            gt_text="Engine 41 copy",
            system_prompt="You are a transcriber.",
            user_prompt="Transcribe this.",
        )
        file_parts = [
            p for p in example["contents"][0]["parts"] if "fileData" in p
        ]
        self.assertEqual(
            file_parts[0]["fileData"]["fileUri"], "gs://bucket/seg001.flac"
        )
        self.assertEqual(file_parts[0]["fileData"]["mimeType"], "audio/flac")

    def test_system_instruction_is_sibling_of_contents(self) -> None:
        example = build_audio_tuning_example(
            "gs://b/s.flac", "copy", "sys", "user"
        )
        self.assertIn("systemInstruction", example)
        self.assertIn("contents", example)
        # systemInstruction must NOT be nested inside any turn in contents
        for turn in example["contents"]:
            self.assertNotIn("systemInstruction", turn)

    def test_contents_has_user_then_model_turns(self) -> None:
        example = build_audio_tuning_example(
            "gs://b/s.flac", "copy", "sys", "user"
        )
        self.assertEqual(len(example["contents"]), 2)
        self.assertEqual(example["contents"][0]["role"], "user")
        self.assertEqual(example["contents"][1]["role"], "model")

    def test_model_turn_carries_ground_truth_text(self) -> None:
        example = build_audio_tuning_example(
            audio_uri="gs://bucket/seg001.flac",
            gt_text="Engine 41 copy",
            system_prompt="You are a transcriber.",
            user_prompt="Transcribe this.",
        )
        self.assertEqual(
            example["contents"][1]["parts"][0]["text"], "Engine 41 copy"
        )

    def test_user_turn_carries_user_prompt_text(self) -> None:
        example = build_audio_tuning_example(
            audio_uri="gs://b/s.flac",
            gt_text="copy",
            system_prompt="sys",
            user_prompt="Transcribe this.",
        )
        text_parts = [p for p in example["contents"][0]["parts"] if "text" in p]
        self.assertTrue(
            any(p["text"] == "Transcribe this." for p in text_parts),
            msg=f"Expected user_prompt text not found in user turn parts: {example['contents'][0]['parts']}",
        )

    def test_history_is_encoded_as_prior_audio_model_turn_pairs(self) -> None:
        example = build_audio_tuning_example(
            audio_uri="gs://bucket/current.flac",
            gt_text="current text",
            system_prompt="sys",
            user_prompt="current prompt",
            history=[
                ContextTurn("gs://bucket/prev-1.flac", "first"),
                ContextTurn("gs://bucket/prev-2.flac", "second"),
            ],
        )

        self.assertEqual(
            [turn["role"] for turn in example["contents"]],
            ["user", "model", "user", "model", "user", "model"],
        )
        self.assertEqual(
            _first_file_part(example["contents"][0])["fileData"]["fileUri"],
            "gs://bucket/prev-1.flac",
        )
        self.assertEqual(example["contents"][1]["parts"][0]["text"], "first")
        self.assertEqual(
            _first_file_part(example["contents"][2])["fileData"]["fileUri"],
            "gs://bucket/prev-2.flac",
        )
        self.assertEqual(example["contents"][3]["parts"][0]["text"], "second")
        self.assertEqual(
            example["contents"][4]["parts"][0]["text"],
            "current prompt",
        )
        self.assertEqual(
            _first_file_part(example["contents"][4])["fileData"]["fileUri"],
            "gs://bucket/current.flac",
        )

    def test_transcript_history_mode_keeps_one_audio_part(self) -> None:
        example = build_audio_tuning_example(
            audio_uri="gs://bucket/current.flac",
            gt_text="current text",
            system_prompt="sys",
            user_prompt="current prompt",
            history=[
                ContextTurn("gs://bucket/prev-1.flac", "first"),
                ContextTurn("gs://bucket/prev-2.flac", "second"),
            ],
            history_mode="transcript",
        )

        self.assertEqual(len(example["contents"]), 2)
        self.assertEqual(
            [turn["role"] for turn in example["contents"]],
            ["user", "model"],
        )
        self.assertEqual(
            _first_file_part(example["contents"][0])["fileData"]["fileUri"],
            "gs://bucket/current.flac",
        )
        user_text = example["contents"][0]["parts"][0]["text"]
        self.assertIn("Prior same-source transcripts", user_text)
        self.assertIn("1. first", user_text)
        self.assertIn("2. second", user_text)
        self.assertIn("current prompt", user_text)

    def test_guarded_transcript_block_mode_uses_exact_template(self) -> None:
        example = build_audio_tuning_example(
            audio_uri="gs://bucket/current.flac",
            gt_text="current text",
            system_prompt="sys",
            user_prompt="IMPORTANT: current prompt",
            history=[
                ContextTurn("gs://bucket/prev-1.flac", " first   transcript "),
                ContextTurn("gs://bucket/prev-2.flac", "second transcript"),
            ],
            history_mode="guarded_transcript_block",
        )

        self.assertEqual(len(example["contents"]), 2)
        self.assertTrue(validate_audio_tuning_example(example))
        self.assertEqual(
            _first_file_part(example["contents"][0])["fileData"]["fileUri"],
            "gs://bucket/current.flac",
        )
        self.assertEqual(
            example["contents"][0]["parts"][0]["text"],
            "\n".join(
                [
                    "The following prior same-source transcripts are for "
                    "situational awareness only.",
                    "Do not re-transcribe them. Do not continue them.",
                    "Transcribe exclusively the current audio clip.",
                    "",
                    "Prior transcripts, oldest to newest:",
                    "1. first transcript",
                    "2. second transcript",
                    "",
                    "IMPORTANT: current prompt",
                ]
            ),
        )

    def test_guarded_transcript_block_mode_includes_no_history_sentence(
        self,
    ) -> None:
        example = build_audio_tuning_example(
            audio_uri="gs://bucket/current.flac",
            gt_text="current text",
            system_prompt="sys",
            user_prompt="IMPORTANT: current prompt",
            history=[],
            history_mode="guarded_transcript_block",
        )

        self.assertEqual(
            example["contents"][0]["parts"][0]["text"],
            "\n".join(
                [
                    "The following prior same-source transcripts are for "
                    "situational awareness only.",
                    "Do not re-transcribe them. Do not continue them.",
                    "Transcribe exclusively the current audio clip.",
                    "",
                    "Prior transcripts, oldest to newest:",
                    "There are no prior transcripts for this original "
                    "recording.",
                    "",
                    "IMPORTANT: current prompt",
                ]
            ),
        )

    def test_text_turn_history_mode_uses_prior_user_model_turns_with_one_audio(
        self,
    ) -> None:
        user_prompt = "current prompt"
        example = build_audio_tuning_example(
            audio_uri="gs://bucket/current.flac",
            gt_text="current text",
            system_prompt="sys",
            user_prompt=user_prompt,
            history=[
                ContextTurn("gs://bucket/prev-1.flac", "first"),
                ContextTurn("gs://bucket/prev-2.flac", "second"),
            ],
            history_mode="text_turns",
        )

        self.assertEqual(
            [turn["role"] for turn in example["contents"]],
            ["user", "model", "user", "model", "user", "model"],
        )
        audio_parts = [
            part
            for turn in example["contents"]
            for part in turn["parts"]
            if "fileData" in part
        ]
        self.assertEqual(len(audio_parts), 1)
        self.assertEqual(
            audio_parts[0]["fileData"]["fileUri"],
            "gs://bucket/current.flac",
        )
        self.assertEqual(
            example["contents"][0]["parts"][0]["text"], user_prompt
        )
        self.assertEqual(example["contents"][1]["parts"][0]["text"], "first")
        self.assertEqual(
            example["contents"][2]["parts"][0]["text"], user_prompt
        )
        self.assertEqual(example["contents"][3]["parts"][0]["text"], "second")
        self.assertEqual(
            example["contents"][4]["parts"][0]["text"], user_prompt
        )


class TestValidateExample(unittest.TestCase):
    def test_accepts_well_formed_example(self) -> None:
        ex = build_audio_tuning_example("gs://b/s.flac", "copy", "sys", "user")
        self.assertTrue(validate_audio_tuning_example(ex))

    def test_rejects_legacy_input_output_shape(self) -> None:
        self.assertFalse(
            validate_audio_tuning_example(
                {"input_text": "x", "output_text": "y"}
            )
        )

    def test_rejects_flat_prompt_response_shape(self) -> None:
        self.assertFalse(
            validate_audio_tuning_example({"prompt": "x", "response": "y"})
        )

    def test_rejects_non_gs_uri(self) -> None:
        ex = build_audio_tuning_example("gs://b/s.flac", "copy", "sys", "user")
        _first_file_part(ex["contents"][0])["fileData"]["fileUri"] = (
            "s3://bucket/file.flac"
        )
        self.assertFalse(validate_audio_tuning_example(ex))

    def test_rejects_null_file_uri(self) -> None:
        ex = build_audio_tuning_example("gs://b/s.flac", "copy", "sys", "user")
        _first_file_part(ex["contents"][0])["fileData"]["fileUri"] = None
        # Must return False, not raise AttributeError on None.startswith(...)
        self.assertFalse(validate_audio_tuning_example(ex))

    def test_rejects_wrong_mime_type(self) -> None:
        ex = build_audio_tuning_example("gs://b/s.flac", "copy", "sys", "user")
        _first_file_part(ex["contents"][0])["fileData"]["mimeType"] = (
            "audio/wav"
        )
        self.assertFalse(validate_audio_tuning_example(ex))

    def test_rejects_empty_model_text(self) -> None:
        ex = build_audio_tuning_example("gs://b/s.flac", "   ", "sys", "user")
        self.assertFalse(validate_audio_tuning_example(ex))

    def test_rejects_contents_with_wrong_turn_count(self) -> None:
        ex = build_audio_tuning_example("gs://b/s.flac", "copy", "sys", "user")
        # Remove the model turn so only one turn remains
        ex["contents"] = ex["contents"][:1]
        self.assertFalse(validate_audio_tuning_example(ex))

    def test_accepts_well_formed_text_turn_history_turns(self) -> None:
        ex = build_audio_tuning_example(
            "gs://b/current.flac",
            "current",
            "sys",
            "user",
            history=[ContextTurn("gs://b/prior.flac", "prior")],
            history_mode="text_turns",
        )

        self.assertTrue(validate_audio_tuning_example(ex))

    def test_rejects_audio_history_turns_with_multiple_audio_parts(
        self,
    ) -> None:
        ex = build_audio_tuning_example(
            "gs://b/current.flac",
            "current",
            "sys",
            "user",
            history=[ContextTurn("gs://b/prior.flac", "prior")],
            history_mode="audio",
        )

        self.assertFalse(validate_audio_tuning_example(ex))


class TestImportIsolation(unittest.TestCase):
    """TEST-03: `import common.prompts` (the light core) must load no heavy deps."""

    def _run_isolated(self, code: str) -> subprocess.CompletedProcess:
        """Run code in a subprocess with model/src on PYTHONPATH."""
        env = {**os.environ, "PYTHONPATH": _SRC_DIR}
        return subprocess.run(
            [sys.executable, "-c", code],
            capture_output=True,
            text=True,
            env=env,
            check=False,
        )

    def test_import_common_prompts_loads_no_heavy_deps(self) -> None:
        code = (
            "import common.prompts; "
            "import sys; "
            "forbidden = ['nemo_text_processing', 'torch', 'datasets']; "
            "found = [m for m in forbidden if m in sys.modules]; "
            "assert not found, f'Heavy deps leaked into the light core: {found}'"
        )
        result = self._run_isolated(code)
        self.assertEqual(
            result.returncode,
            0,
            msg=f"Import isolation failed (stdout={result.stdout!r} stderr={result.stderr!r})",
        )

    def test_import_common_gemini_tuning_data_loads_no_heavy_deps(self) -> None:
        code = (
            "import common.gemini.tuning_data; "
            "import sys; "
            "forbidden = ['nemo_text_processing', 'torch', 'datasets']; "
            "found = [m for m in forbidden if m in sys.modules]; "
            "assert not found, f'Heavy deps leaked into the light core: {found}'"
        )
        result = self._run_isolated(code)
        self.assertEqual(
            result.returncode,
            0,
            msg=f"Import isolation failed (stdout={result.stdout!r} stderr={result.stderr!r})",
        )


if __name__ == "__main__":
    unittest.main()
