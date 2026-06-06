from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

_SFT_DIR = str(Path(__file__).resolve().parent.parent)
_COLABS_DIR = str(
    Path(__file__).resolve().parent.parent.parent.parent / "colabs"
)
if _SFT_DIR not in sys.path:
    sys.path.insert(0, _SFT_DIR)
if _COLABS_DIR not in sys.path:
    sys.path.insert(0, _COLABS_DIR)

from run_config import RunConfigError, load_run_config  # noqa: E402


class TestRunConfig(unittest.TestCase):
    def _write_config(self, body: str) -> Path:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        path = Path(tmp.name) / "run.toml"
        path.write_text(body, encoding="utf-8")
        return path

    def _valid_toml(self, **replacements: str) -> str:
        values = {
            "round_id": '"round"',
            "dataset": '"wd-internal"',
            "train_manifest_uri": '"gs://source/manifests/train.jsonl"',
            "validation_manifest_uri": '"gs://source/manifests/validation.jsonl"',
            "eval_manifest_uri": '"gs://source/manifests/eval.jsonl"',
            "project": '"project-id"',
            "bucket": '"bucket"',
            "location": '"us-central1"',
            "base_model": '"gemini-3.1-flash-lite"',
            "epoch_count": "6",
            "adapter_size": '"SIXTEEN"',
            "learning_rate_multiplier": "1.0",
            "prompts": "",
        }
        values.update(replacements)
        return f"""
round_id = {values["round_id"]}
dataset = {values["dataset"]}
train_manifest_uri = {values["train_manifest_uri"]}
validation_manifest_uri = {values["validation_manifest_uri"]}
eval_manifest_uri = {values["eval_manifest_uri"]}

[gcp]
project = {values["project"]}
bucket = {values["bucket"]}
location = {values["location"]}

[sft]
base_model = {values["base_model"]}
epoch_count = {values["epoch_count"]}
adapter_size = {values["adapter_size"]}
learning_rate_multiplier = {values["learning_rate_multiplier"]}
{values["prompts"]}
"""

    def test_valid_minimal_toml_resolves_required_fields_and_paths(self) -> None:
        cfg = load_run_config(self._write_config(self._valid_toml()))

        self.assertEqual(cfg.round_id, "round")
        self.assertEqual(cfg.dataset, "wd-internal")
        self.assertEqual(cfg.paths.gcs_prefix, "gs://bucket/sft/runs/round")
        self.assertEqual(
            cfg.paths.gemini_validation_uri,
            "gs://bucket/sft/runs/round/model_inputs/gemini/validation.jsonl",
        )
        record = cfg.to_record_dict()
        self.assertEqual(record["datasets"], ["wd-internal"])
        self.assertEqual(record["combined_train_uri"], cfg.paths.gemini_train_uri)
        self.assertEqual(record["combined_val_uri"], cfg.paths.gemini_validation_uri)

    def test_missing_validation_manifest_uri_raises(self) -> None:
        body = self._valid_toml(validation_manifest_uri='""')

        with self.assertRaisesRegex(RunConfigError, "validation_manifest_uri"):
            load_run_config(self._write_config(body))

    def test_inline_prompts_override_defaults(self) -> None:
        body = self._valid_toml(
            prompts="""
[prompts]
system = "custom system"
user = "custom user"
"""
        )

        cfg = load_run_config(self._write_config(body))

        self.assertEqual(cfg.system_prompt, "custom system")
        self.assertEqual(cfg.user_prompt, "custom user")

    def test_prompt_file_keys_are_rejected(self) -> None:
        body = self._valid_toml(
            prompts="""
[prompts]
system_file = "system.txt"
"""
        )

        with self.assertRaisesRegex(RunConfigError, "system_file"):
            load_run_config(self._write_config(body))

    def test_at_file_prompt_values_are_rejected(self) -> None:
        body = self._valid_toml(
            prompts="""
[prompts]
system = "@prompt.txt"
"""
        )

        with self.assertRaisesRegex(RunConfigError, "@|inline"):
            load_run_config(self._write_config(body))

    def test_gcs_bucket_must_be_bucket_name(self) -> None:
        body = self._valid_toml(bucket='"gs://bucket/path"')

        with self.assertRaisesRegex(RunConfigError, "bucket"):
            load_run_config(self._write_config(body))

    def test_adapter_size_two_is_accepted(self) -> None:
        cfg = load_run_config(
            self._write_config(self._valid_toml(adapter_size='"TWO"'))
        )

        self.assertEqual(cfg.adapter_size, "TWO")

    def test_unknown_adapter_size_is_rejected(self) -> None:
        with self.assertRaisesRegex(RunConfigError, "adapter_size"):
            load_run_config(
                self._write_config(self._valid_toml(adapter_size='"THREE"'))
            )


if __name__ == "__main__":
    unittest.main()
