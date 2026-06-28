from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

_SRC_DIR = str(Path(__file__).resolve().parents[2] / "src")
if _SRC_DIR not in sys.path:
    sys.path.insert(0, _SRC_DIR)

from gemini_sft.config import (  # noqa: E402
    RunConfigError,
    load_eval_run_config,
    load_run_config,
    require_config_eval_models,
)


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
            "inference_dataset_slug": '"echo/eval"',
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
            "context": "",
            "prompts": "",
            "eval_section": "",
        }
        values.update(replacements)
        return f"""
round_id = {values["round_id"]}
dataset = {values["dataset"]}
inference_dataset_slug = {values["inference_dataset_slug"]}
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
{values["context"]}
{values["prompts"]}
{values["eval_section"]}
"""

    def _without_manifest_lines(self, body: str, *keys: str) -> str:
        prefixes = tuple(f"{key} =" for key in keys)
        return "\n".join(
            line for line in body.splitlines() if not line.startswith(prefixes)
        )

    def _eval_models_section(self, *targets: tuple[str, str]) -> str:
        lines = ["[eval]"]
        for label, model in targets:
            lines.extend(
                [
                    "",
                    "[[eval.models]]",
                    f'label = "{label}"',
                    f'model = "{model}"',
                ]
            )
        return "\n".join(lines)

    def test_valid_minimal_toml_resolves_required_fields_and_paths(
        self,
    ) -> None:
        cfg = load_run_config(self._write_config(self._valid_toml()))

        self.assertEqual(cfg.round_id, "round")
        self.assertEqual(cfg.dataset, "wd-internal")
        self.assertEqual(cfg.inference_dataset_slug, "echo/eval")
        self.assertEqual(cfg.paths.gcs_prefix, "gs://bucket/sft/runs/round")
        self.assertEqual(
            cfg.paths.gemini_validation_uri,
            "gs://bucket/sft/runs/round/model_inputs/gemini/validation.jsonl",
        )
        record = cfg.to_record_dict()
        self.assertEqual(record["dataset"], "wd-internal")
        self.assertEqual(record["prior_context_count"], 0)
        self.assertEqual(record["prior_context_mode"], "text_turns")
        self.assertNotIn("continuous_tuned_model_name", record)
        self.assertNotIn("continuous_checkpoint_id", record)
        self.assertEqual(record["inference_dataset_slug"], "echo/eval")
        self.assertEqual(record["gemini_train_uri"], cfg.paths.gemini_train_uri)
        self.assertEqual(
            record["gemini_validation_uri"], cfg.paths.gemini_validation_uri
        )
        self.assertEqual(cfg.eval_models, ())
        self.assertNotIn("eval_models", record)
        legacy_aliases = {
            "datasets",
            "epochs",
            "lr_multiplier",
            "combined_train_uri",
            "combined_val_uri",
        }
        self.assertFalse(legacy_aliases & set(record))

    def test_continuous_tuning_config_serializes_source_checkpoint(
        self,
    ) -> None:
        body = self._valid_toml(
            context="""
[continuous_tuning]
tuned_model_name = "projects/p/locations/us/models/123@1"
checkpoint_id = "7"
""",
        )

        cfg = load_run_config(self._write_config(body))

        self.assertEqual(
            cfg.continuous_tuned_model_name,
            "projects/p/locations/us/models/123@1",
        )
        self.assertEqual(cfg.continuous_checkpoint_id, "7")
        record = cfg.to_record_dict()
        self.assertEqual(
            record["continuous_tuned_model_name"],
            "projects/p/locations/us/models/123@1",
        )
        self.assertEqual(record["continuous_checkpoint_id"], "7")

    def test_continuous_tuning_checkpoint_requires_source_model(self) -> None:
        body = self._valid_toml(
            context="""
[continuous_tuning]
checkpoint_id = "7"
""",
        )

        with self.assertRaisesRegex(RunConfigError, "tuned_model_name"):
            load_run_config(self._write_config(body))

    def test_missing_validation_manifest_uri_raises(self) -> None:
        body = self._valid_toml(validation_manifest_uri='""')

        with self.assertRaisesRegex(RunConfigError, "validation_manifest_uri"):
            load_run_config(self._write_config(body))

    def test_eval_config_requires_explicit_eval_models(
        self,
    ) -> None:
        body = self._without_manifest_lines(
            self._valid_toml(),
            "train_manifest_uri",
            "validation_manifest_uri",
        )

        with self.assertRaisesRegex(
            RunConfigError,
            r"\[\[eval\.models\]\].*label.*model.*base_model/endpoint",
        ):
            load_eval_run_config(self._write_config(body))

    def test_eval_config_with_models_allows_missing_training_manifests(
        self,
    ) -> None:
        body = self._without_manifest_lines(
            self._valid_toml(
                eval_section=self._eval_models_section(
                    ("base", "gemini-3.1-flash-lite"),
                    (
                        "checkpoint_6",
                        "projects/p/locations/us-central1/endpoints/123",
                    ),
                )
            ),
            "train_manifest_uri",
            "validation_manifest_uri",
        )

        cfg = load_eval_run_config(self._write_config(body))

        self.assertIsNone(cfg.train_manifest_uri)
        self.assertIsNone(cfg.validation_manifest_uri)
        self.assertEqual(
            cfg.eval_manifest_uri,
            "gs://source/manifests/eval.jsonl",
        )
        self.assertEqual(
            cfg.paths.config_uri,
            "gs://bucket/sft/runs/round/config.json",
        )
        self.assertEqual(
            cfg.to_record_dict()["eval_models"],
            [
                {"label": "base", "model": "gemini-3.1-flash-lite"},
                {
                    "label": "checkpoint_6",
                    "model": "projects/p/locations/us-central1/endpoints/123",
                },
            ],
        )

    def test_masked_and_unmasked_eval_configs_are_separate_runs(
        self,
    ) -> None:
        eval_section = self._eval_models_section(
            ("base", "gemini-3.1-flash-lite")
        )
        unmasked_body = self._without_manifest_lines(
            self._valid_toml(
                round_id='"round-unmasked"',
                eval_manifest_uri='"gs://source/manifests/eval.jsonl"',
                inference_dataset_slug='"radio/eval"',
                eval_section=eval_section,
            ),
            "train_manifest_uri",
            "validation_manifest_uri",
        )
        masked_body = self._without_manifest_lines(
            self._valid_toml(
                round_id='"round-masked"',
                eval_manifest_uri=(
                    '"gs://source/manifests/masked_v2/eval.jsonl"'
                ),
                inference_dataset_slug='"radio/masked_v2/eval"',
                eval_section=eval_section,
            ),
            "train_manifest_uri",
            "validation_manifest_uri",
        )

        unmasked_config = load_eval_run_config(
            self._write_config(unmasked_body)
        )
        masked_config = load_eval_run_config(self._write_config(masked_body))

        self.assertEqual(unmasked_config.round_id, "round-unmasked")
        self.assertEqual(masked_config.round_id, "round-masked")
        self.assertEqual(
            unmasked_config.eval_manifest_uri,
            "gs://source/manifests/eval.jsonl",
        )
        self.assertEqual(
            masked_config.eval_manifest_uri,
            "gs://source/manifests/masked_v2/eval.jsonl",
        )
        self.assertEqual(
            unmasked_config.inference_dataset_slug,
            "radio/eval",
        )
        self.assertEqual(
            masked_config.inference_dataset_slug,
            "radio/masked_v2/eval",
        )
        self.assertEqual(unmasked_config.eval_models, masked_config.eval_models)

        unmasked_record = unmasked_config.to_record_dict()
        masked_record = masked_config.to_record_dict()
        self.assertEqual(unmasked_record["round_id"], "round-unmasked")
        self.assertEqual(masked_record["round_id"], "round-masked")
        self.assertEqual(
            unmasked_record["eval_manifest_uri"],
            "gs://source/manifests/eval.jsonl",
        )
        self.assertEqual(
            masked_record["eval_manifest_uri"],
            "gs://source/manifests/masked_v2/eval.jsonl",
        )
        self.assertEqual(
            unmasked_record["inference_dataset_slug"],
            "radio/eval",
        )
        self.assertEqual(
            masked_record["inference_dataset_slug"],
            "radio/masked_v2/eval",
        )
        for record in (unmasked_record, masked_record):
            self.assertNotIn("eval_label", record)
            self.assertNotIn("masked", record)

    def test_run_config_serializes_optional_eval_models(self) -> None:
        body = self._valid_toml(
            eval_section=self._eval_models_section(
                ("base", "gemini-3.1-flash-lite"),
                (
                    "checkpoint_6",
                    "projects/p/locations/us-central1/endpoints/123",
                ),
            )
        )

        cfg = load_run_config(self._write_config(body))

        self.assertEqual(
            [target.to_record_dict() for target in cfg.eval_models],
            [
                {"label": "base", "model": "gemini-3.1-flash-lite"},
                {
                    "label": "checkpoint_6",
                    "model": "projects/p/locations/us-central1/endpoints/123",
                },
            ],
        )
        self.assertEqual(
            cfg.to_record_dict()["eval_models"],
            [
                {"label": "base", "model": "gemini-3.1-flash-lite"},
                {
                    "label": "checkpoint_6",
                    "model": "projects/p/locations/us-central1/endpoints/123",
                },
            ],
        )

    def test_eval_config_rejects_non_string_optional_manifest_uri(self) -> None:
        body = self._valid_toml(
            train_manifest_uri="123",
            eval_section=self._eval_models_section(
                ("base", "gemini-3.1-flash-lite")
            ),
        )

        with self.assertRaisesRegex(RunConfigError, "train_manifest_uri"):
            load_eval_run_config(self._write_config(body))

    def test_eval_config_requires_eval_manifest_uri(self) -> None:
        body = self._without_manifest_lines(
            self._valid_toml(
                eval_section=self._eval_models_section(
                    ("base", "gemini-3.1-flash-lite")
                )
            ),
            "eval_manifest_uri",
        )

        with self.assertRaisesRegex(RunConfigError, "eval_manifest_uri"):
            load_eval_run_config(self._write_config(body))

    def test_eval_model_targets_reject_invalid_labels(self) -> None:
        for label in (
            "",
            ".",
            "..",
            "bad label",
            "nested/label",
            "base.jsonl",
        ):
            with self.subTest(label=label):
                body = self._valid_toml(
                    eval_section=self._eval_models_section(
                        (label, "gemini-3.1-flash-lite")
                    )
                )

                with self.assertRaisesRegex(
                    RunConfigError, r"eval\.models\[0\]\.label"
                ):
                    load_run_config(self._write_config(body))

    def test_eval_model_targets_reject_duplicate_labels(self) -> None:
        body = self._valid_toml(
            eval_section=self._eval_models_section(
                ("base", "gemini-3.1-flash-lite"),
                ("base", "projects/p/locations/us-central1/endpoints/123"),
            )
        )

        with self.assertRaisesRegex(RunConfigError, "duplicate"):
            load_run_config(self._write_config(body))

    def test_eval_model_targets_reject_invalid_model_strings(self) -> None:
        invalid_sections = {
            "empty": """
[eval]

[[eval.models]]
label = "base"
model = "   "
""",
            "non-string": """
[eval]

[[eval.models]]
label = "base"
model = 123
""",
        }
        for name, eval_section in invalid_sections.items():
            with self.subTest(name=name):
                body = self._valid_toml(eval_section=eval_section)

                with self.assertRaisesRegex(
                    RunConfigError, r"eval\.models\[0\]\.model"
                ):
                    load_run_config(self._write_config(body))

    def test_eval_model_targets_reject_non_table_entries(self) -> None:
        body = self._valid_toml(
            eval_section="""
[eval]
models = ["not-a-table"]
"""
        )

        with self.assertRaisesRegex(RunConfigError, "TOML table"):
            load_run_config(self._write_config(body))

    def test_eval_model_targets_reject_unsupported_fields(self) -> None:
        for field_name in ("type", "description"):
            with self.subTest(field_name=field_name):
                body = self._valid_toml(
                    eval_section=f"""
[eval]

[[eval.models]]
label = "base"
model = "gemini-3.1-flash-lite"
{field_name} = "unsupported"
"""
                )

                with self.assertRaisesRegex(RunConfigError, field_name):
                    load_run_config(self._write_config(body))

    def test_require_config_eval_models_returns_valid_targets(self) -> None:
        targets = require_config_eval_models(
            {
                "eval_models": [
                    {"label": "base", "model": " gemini-3.1-flash-lite "},
                    {
                        "label": "checkpoint_6",
                        "model": "projects/p/locations/us/endpoints/123",
                    },
                ]
            }
        )

        self.assertEqual(
            [target.to_record_dict() for target in targets],
            [
                {"label": "base", "model": "gemini-3.1-flash-lite"},
                {
                    "label": "checkpoint_6",
                    "model": "projects/p/locations/us/endpoints/123",
                },
            ],
        )

    def test_require_config_eval_models_requires_eval_models(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"config\.json missing required eval_models.*"
            r"\[\[eval\.models\]\].*label.*model.*"
            r"base_model/endpoint fallback is not supported",
        ):
            require_config_eval_models(
                {
                    "base_model": "gemini-3.1-flash-lite",
                    "endpoint": "projects/p/locations/us/endpoints/123",
                }
            )

    def test_require_config_eval_models_rejects_duplicate_labels(self) -> None:
        with self.assertRaisesRegex(ValueError, "duplicate"):
            require_config_eval_models(
                {
                    "eval_models": [
                        {"label": "base", "model": "gemini-3.1-flash-lite"},
                        {
                            "label": "base",
                            "model": "projects/p/locations/us/endpoints/123",
                        },
                    ]
                }
            )

    def test_require_config_eval_models_rejects_invalid_labels(self) -> None:
        for label in ("", ".", "..", "bad label", "nested/label", "base.jsonl"):
            with self.subTest(label=label):
                with self.assertRaisesRegex(ValueError, "eval_models\\[0\\]"):
                    require_config_eval_models(
                        {
                            "eval_models": [
                                {
                                    "label": label,
                                    "model": "gemini-3.1-flash-lite",
                                }
                            ]
                        }
                    )

    def test_require_config_eval_models_rejects_empty_model(self) -> None:
        with self.assertRaisesRegex(ValueError, r"eval_models\[0\]\.model"):
            require_config_eval_models(
                {"eval_models": [{"label": "base", "model": "   "}]}
            )

    def test_require_config_eval_models_rejects_unsupported_fields(
        self,
    ) -> None:
        with self.assertRaisesRegex(ValueError, "unsupported: type"):
            require_config_eval_models(
                {
                    "eval_models": [
                        {
                            "label": "base",
                            "model": "gemini-3.1-flash-lite",
                            "type": "publisher",
                        }
                    ]
                }
            )

    def test_round_id_must_be_safe_single_path_component(self) -> None:
        for round_id in (
            "../escape",
            "/absolute",
            "nested/round",
            "space round",
        ):
            with self.subTest(round_id=round_id):
                body = self._valid_toml(round_id=f'"{round_id}"')

                with self.assertRaisesRegex(RunConfigError, "round_id"):
                    load_run_config(self._write_config(body))

    def test_inference_dataset_slug_must_be_safe_relative_path(self) -> None:
        for slug in ("", "/absolute", "echo/../eval", "echo//eval"):
            with self.subTest(slug=slug):
                body = self._valid_toml(inference_dataset_slug=f'"{slug}"')

                with self.assertRaisesRegex(
                    RunConfigError, "inference_dataset_slug"
                ):
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

    def test_context_prior_turn_count_is_recorded(self) -> None:
        body = self._valid_toml(
            context="""
[context]
prior_turn_count = 8
prior_context_mode = "text_turns"
"""
        )

        cfg = load_run_config(self._write_config(body))

        self.assertEqual(cfg.prior_context_count, 8)
        self.assertEqual(cfg.prior_context_mode, "text_turns")
        self.assertEqual(cfg.to_record_dict()["prior_context_count"], 8)
        self.assertEqual(
            cfg.to_record_dict()["prior_context_mode"], "text_turns"
        )

    def test_vapo_p3_context_mode_is_accepted(self) -> None:
        body = self._valid_toml(
            context="""
[context]
prior_turn_count = 8
prior_context_mode = "vapo_p3_transcript"
"""
        )

        cfg = load_run_config(self._write_config(body))

        self.assertEqual(cfg.prior_context_count, 8)
        self.assertEqual(cfg.prior_context_mode, "vapo_p3_transcript")
        self.assertEqual(
            cfg.to_record_dict()["prior_context_mode"],
            "vapo_p3_transcript",
        )

    def test_context_prior_turn_count_must_be_nonnegative_int(self) -> None:
        for value in ("-1", "true"):
            with self.subTest(value=value):
                body = self._valid_toml(
                    context=f"""
[context]
prior_turn_count = {value}
"""
                )

                with self.assertRaisesRegex(RunConfigError, "prior_turn_count"):
                    load_run_config(self._write_config(body))

    def test_context_prior_context_mode_must_be_supported(self) -> None:
        body = self._valid_toml(
            context="""
[context]
prior_context_mode = "audio"
"""
        )

        with self.assertRaisesRegex(RunConfigError, "prior_context_mode"):
            load_run_config(self._write_config(body))

    def test_prompt_file_keys_are_rejected(self) -> None:
        body = self._valid_toml(
            prompts="""
[prompts]
system_file = "system.txt"
"""
        )

        with self.assertRaisesRegex(
            RunConfigError, "intentionally not supported"
        ):
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
