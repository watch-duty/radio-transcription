from __future__ import annotations

import pathlib
import tempfile
import unittest

from common.gemini import prompts as prompt_defaults
from gemini_sft import config as config_module


class TestRunConfig(unittest.TestCase):
    def _write_config(self, body: str) -> pathlib.Path:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        path = pathlib.Path(tmp.name) / "run.toml"
        path.write_text(body, encoding="utf-8")
        return path

    def _valid_toml(self, **replacements: str) -> str:
        values = {
            "round_id": '"round"',
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

    def _eval_model_section(
        self,
        label: str = "base",
        model: str = "gemini-3.1-flash-lite",
    ) -> str:
        return f"""
[eval]

[eval.model]
label = "{label}"
model = "{model}"
"""

    def test_valid_minimal_toml_resolves_required_fields_and_paths(
        self,
    ) -> None:
        cfg = config_module.load_run_config(
            self._write_config(self._valid_toml())
        )

        self.assertEqual(cfg.round_id, "round")
        self.assertEqual(cfg.inference_dataset_slug, "echo/eval")
        self.assertEqual(cfg.paths.gcs_prefix, "gs://bucket/sft/runs/round")
        self.assertEqual(
            cfg.paths.gemini_validation_uri,
            "gs://bucket/sft/runs/round/model_inputs/gemini/validation.jsonl",
        )
        record = cfg.to_record_dict()
        self.assertEqual(record["prior_context_count"], 0)
        self.assertEqual(record["prior_context_mode"], "text_turns")
        self.assertEqual(record["inference_dataset_slug"], "echo/eval")
        self.assertEqual(record["gemini_train_uri"], cfg.paths.gemini_train_uri)
        self.assertEqual(
            record["gemini_validation_uri"], cfg.paths.gemini_validation_uri
        )
        self.assertIsNone(cfg.eval_model)

    def test_committed_example_is_valid_production_parity_config(self) -> None:
        example_path = (
            pathlib.Path(__file__).resolve().parents[2]
            / "scripts"
            / "sft"
            / "run_config.example.toml"
        )

        cfg = config_module.load_run_config(example_path)

        self.assertEqual(cfg.prior_context_count, 0)
        self.assertEqual(cfg.user_prompt, "")
        self.assertEqual(
            cfg.system_prompt,
            prompt_defaults.GEMINI_TRANSCRIBE_SYSTEM_PROMPT,
        )

    def test_missing_validation_manifest_uri_raises(self) -> None:
        body = self._valid_toml(validation_manifest_uri='""')

        with self.assertRaisesRegex(
            config_module.RunConfigError, "validation_manifest_uri"
        ):
            config_module.load_run_config(self._write_config(body))

    def test_prepare_config_accepts_complete_training_pair(self) -> None:
        cfg = config_module.load_prepare_run_config(
            self._write_config(self._valid_toml())
        )

        self.assertIsNotNone(cfg.train_manifest_uri)
        self.assertIsNotNone(cfg.validation_manifest_uri)
        self.assertIn("gemini_train_uri", cfg.to_record_dict())

    def test_prepare_config_accepts_eval_only_target(self) -> None:
        body = self._without_manifest_lines(
            self._valid_toml(
                eval_section=self._eval_model_section(
                    "checkpoint_6",
                    "projects/p/locations/us-central1/endpoints/123",
                )
            ),
            "train_manifest_uri",
            "validation_manifest_uri",
        )

        cfg = config_module.load_prepare_run_config(self._write_config(body))
        record = cfg.to_record_dict()

        self.assertIsNone(cfg.train_manifest_uri)
        self.assertIsNone(cfg.validation_manifest_uri)
        self.assertEqual(cfg.eval_model.label, "checkpoint_6")
        for key in (
            "canonical_train_uri",
            "canonical_validation_uri",
            "gemini_train_uri",
            "gemini_validation_uri",
        ):
            self.assertNotIn(key, record)

    def test_prepare_config_rejects_partial_training_pair(self) -> None:
        body = self._without_manifest_lines(
            self._valid_toml(eval_section=self._eval_model_section()),
            "validation_manifest_uri",
        )

        with self.assertRaisesRegex(
            config_module.RunConfigError,
            "both train_manifest_uri and validation_manifest_uri",
        ):
            config_module.load_prepare_run_config(self._write_config(body))

    def test_prepare_config_requires_target_without_training_pair(
        self,
    ) -> None:
        body = self._without_manifest_lines(
            self._valid_toml(),
            "train_manifest_uri",
            "validation_manifest_uri",
        )

        with self.assertRaisesRegex(
            config_module.RunConfigError,
            r"eval-only prepare.*\[eval\.model\]",
        ):
            config_module.load_prepare_run_config(self._write_config(body))

    def test_sft_epoch_count_rejects_bool(self) -> None:
        body = self._valid_toml(epoch_count="true")

        with self.assertRaisesRegex(
            config_module.RunConfigError, "epoch_count"
        ):
            config_module.load_run_config(self._write_config(body))

    def test_eval_config_requires_explicit_eval_model(
        self,
    ) -> None:
        body = self._without_manifest_lines(
            self._valid_toml(),
            "train_manifest_uri",
            "validation_manifest_uri",
        )

        with self.assertRaisesRegex(
            config_module.RunConfigError,
            r"\[eval\.model\].*label.*model",
        ):
            config_module.load_eval_run_config(self._write_config(body))

    def test_eval_config_with_model_allows_missing_training_manifests(
        self,
    ) -> None:
        body = self._without_manifest_lines(
            self._valid_toml(
                eval_section=self._eval_model_section(
                    "checkpoint_6",
                    "projects/p/locations/us-central1/endpoints/123",
                )
            ),
            "train_manifest_uri",
            "validation_manifest_uri",
        )

        cfg = config_module.load_eval_run_config(self._write_config(body))

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
            cfg.eval_model.to_record_dict(),
            {
                "label": "checkpoint_6",
                "model": "projects/p/locations/us-central1/endpoints/123",
            },
        )
        self.assertEqual(
            cfg.to_record_dict()["eval_model"],
            {
                "label": "checkpoint_6",
                "model": "projects/p/locations/us-central1/endpoints/123",
            },
        )

    def test_masked_and_unmasked_eval_configs_are_separate_runs(
        self,
    ) -> None:
        eval_section = self._eval_model_section()
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

        unmasked_config = config_module.load_eval_run_config(
            self._write_config(unmasked_body)
        )
        masked_config = config_module.load_eval_run_config(
            self._write_config(masked_body)
        )

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
        self.assertEqual(unmasked_config.eval_model, masked_config.eval_model)

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
            self.assertEqual(
                set(record["eval_model"]),
                {"label", "model"},
            )

    def test_run_config_serializes_optional_eval_model(self) -> None:
        body = self._valid_toml(
            eval_section=self._eval_model_section(
                "base",
                "gemini-3.1-flash-lite",
            )
        )

        cfg = config_module.load_run_config(self._write_config(body))

        self.assertEqual(
            cfg.eval_model.to_record_dict(),
            {"label": "base", "model": "gemini-3.1-flash-lite"},
        )
        self.assertEqual(
            cfg.to_record_dict()["eval_model"],
            {"label": "base", "model": "gemini-3.1-flash-lite"},
        )

    def test_eval_execution_config_serializes_defaults(self) -> None:
        cfg = config_module.load_run_config(
            self._write_config(self._valid_toml())
        )

        self.assertIsNone(cfg.eval_execution.backend)
        self.assertIsNone(cfg.eval_execution.limit)
        self.assertEqual(cfg.eval_execution.concurrency, 16)
        self.assertEqual(cfg.eval_execution.max_retries, 3)
        self.assertEqual(
            cfg.to_record_dict()["eval_execution"],
            {"concurrency": 16, "max_retries": 3},
        )

    def test_eval_execution_config_serializes_optional_fields(self) -> None:
        body = self._without_manifest_lines(
            self._valid_toml(
                eval_section="""
[eval]

[eval.execution]
backend = "online"
limit = 100
concurrency = 8
max_retries = 4

[eval.model]
label = "checkpoint_6"
model = "projects/p/locations/us-central1/endpoints/123"
"""
            ),
            "train_manifest_uri",
            "validation_manifest_uri",
        )

        cfg = config_module.load_eval_run_config(self._write_config(body))

        self.assertEqual(cfg.eval_execution.backend, "online")
        self.assertEqual(cfg.eval_execution.limit, 100)
        self.assertEqual(cfg.eval_execution.concurrency, 8)
        self.assertEqual(cfg.eval_execution.max_retries, 4)
        self.assertEqual(
            cfg.to_record_dict()["eval_execution"],
            {
                "backend": "online",
                "limit": 100,
                "concurrency": 8,
                "max_retries": 4,
            },
        )

    def test_eval_execution_config_rejects_invalid_values(self) -> None:
        cases = {
            "unsupported": 'unknown = "value"',
            "auto": 'backend = "auto"',
            "non-string-backend": "backend = 123",
            "zero-limit": "limit = 0",
            "bool-limit": "limit = true",
            "zero-concurrency": "concurrency = 0",
            "zero-max-retries": "max_retries = 0",
        }
        for name, execution_body in cases.items():
            with self.subTest(name=name):
                body = self._valid_toml(
                    eval_section=f"""
[eval]

[eval.execution]
{execution_body}

[eval.model]
label = "base"
model = "gemini-3.1-flash-lite"
"""
                )

                with self.assertRaisesRegex(
                    config_module.RunConfigError, r"eval\.execution"
                ):
                    config_module.load_run_config(self._write_config(body))

    def test_prepare_rejects_batch_backend_with_prior_context(self) -> None:
        body = self._valid_toml(
            context="""
[context]
prior_turn_count = 8
prior_context_mode = "text_turns"
""",
            eval_section="""
[eval]

[eval.execution]
backend = "batch"

[eval.model]
label = "base"
model = "gemini-3.1-flash-lite"
""",
        )

        with self.assertRaisesRegex(
            config_module.RunConfigError,
            "predicted-history evaluation requires the online backend",
        ):
            config_module.load_prepare_run_config(self._write_config(body))

    def test_eval_config_rejects_non_string_optional_manifest_uri(self) -> None:
        body = self._valid_toml(
            train_manifest_uri="123",
            eval_section=self._eval_model_section(),
        )

        with self.assertRaisesRegex(
            config_module.RunConfigError, "train_manifest_uri"
        ):
            config_module.load_eval_run_config(self._write_config(body))

    def test_eval_config_requires_eval_manifest_uri(self) -> None:
        body = self._without_manifest_lines(
            self._valid_toml(eval_section=self._eval_model_section()),
            "eval_manifest_uri",
        )

        with self.assertRaisesRegex(
            config_module.RunConfigError, "eval_manifest_uri"
        ):
            config_module.load_eval_run_config(self._write_config(body))

    def test_eval_model_target_rejects_invalid_labels(self) -> None:
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
                    eval_section=self._eval_model_section(label)
                )

                with self.assertRaisesRegex(
                    config_module.RunConfigError, r"eval\.model\.label"
                ):
                    config_module.load_run_config(self._write_config(body))

    def test_eval_model_target_rejects_invalid_model_strings(self) -> None:
        invalid_sections = {
            "empty": """
[eval]

[eval.model]
label = "base"
model = "   "
""",
            "non-string": """
[eval]

[eval.model]
label = "base"
model = 123
""",
        }
        for name, eval_section in invalid_sections.items():
            with self.subTest(name=name):
                body = self._valid_toml(eval_section=eval_section)

                with self.assertRaisesRegex(
                    config_module.RunConfigError, r"eval\.model\.model"
                ):
                    config_module.load_run_config(self._write_config(body))

    def test_eval_model_target_rejects_non_table_entry(self) -> None:
        body = self._valid_toml(
            eval_section="""
[eval]
model = "not-a-table"
"""
        )

        with self.assertRaisesRegex(config_module.RunConfigError, "TOML table"):
            config_module.load_run_config(self._write_config(body))

    def test_eval_model_target_rejects_unsupported_fields(self) -> None:
        for field_name in ("type", "description"):
            with self.subTest(field_name=field_name):
                body = self._valid_toml(
                    eval_section=f"""
[eval]

[eval.model]
label = "base"
model = "gemini-3.1-flash-lite"
{field_name} = "unsupported"
"""
                )

                with self.assertRaisesRegex(
                    config_module.RunConfigError, field_name
                ):
                    config_module.load_run_config(self._write_config(body))

    def test_require_config_eval_model_returns_valid_target(self) -> None:
        target = config_module.require_config_eval_model(
            {
                "eval_model": {
                    "label": "base",
                    "model": " gemini-3.1-flash-lite ",
                }
            }
        )

        self.assertEqual(
            target.to_record_dict(),
            {"label": "base", "model": "gemini-3.1-flash-lite"},
        )

    def test_require_config_eval_model_requires_eval_model(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"config\.json missing required eval_model.*"
            r"\[eval\.model\].*label.*model",
        ):
            config_module.require_config_eval_model(
                {
                    "base_model": "gemini-3.1-flash-lite",
                    "endpoint": "projects/p/locations/us/endpoints/123",
                }
            )

    def test_require_config_eval_model_rejects_invalid_labels(self) -> None:
        for label in ("", ".", "..", "bad label", "nested/label", "base.jsonl"):
            with self.subTest(label=label):
                with self.assertRaisesRegex(ValueError, "eval_model"):
                    config_module.require_config_eval_model(
                        {
                            "eval_model": {
                                "label": label,
                                "model": "gemini-3.1-flash-lite",
                            }
                        }
                    )

    def test_require_config_eval_model_rejects_empty_model(self) -> None:
        with self.assertRaisesRegex(ValueError, r"eval_model\.model"):
            config_module.require_config_eval_model(
                {"eval_model": {"label": "base", "model": "   "}}
            )

    def test_require_config_eval_model_rejects_non_object(self) -> None:
        with self.assertRaisesRegex(TypeError, "eval_model must be an object"):
            config_module.require_config_eval_model(
                {"eval_model": "not-an-object"}
            )

    def test_require_config_eval_model_rejects_unsupported_fields(
        self,
    ) -> None:
        with self.assertRaisesRegex(ValueError, "unsupported: type"):
            config_module.require_config_eval_model(
                {
                    "eval_model": {
                        "label": "base",
                        "model": "gemini-3.1-flash-lite",
                        "type": "publisher",
                    }
                }
            )

    def test_require_config_eval_execution_rejects_missing_field(self) -> None:
        with self.assertRaisesRegex(TypeError, "eval_execution"):
            config_module.require_config_eval_execution({})

    def test_require_config_eval_execution_returns_valid_config(self) -> None:
        execution = config_module.require_config_eval_execution(
            {
                "eval_execution": {
                    "backend": "batch",
                    "limit": 25,
                    "concurrency": 4,
                    "max_retries": 2,
                }
            }
        )

        self.assertEqual(execution.backend, "batch")
        self.assertEqual(execution.limit, 25)
        self.assertEqual(execution.concurrency, 4)
        self.assertEqual(execution.max_retries, 2)

    def test_require_config_eval_execution_rejects_invalid_config(
        self,
    ) -> None:
        cases = (
            {"unexpected": "value"},
            {"concurrency": 16},
            {"max_retries": 3},
            {"backend": "auto", "concurrency": 16, "max_retries": 3},
            {"backend": 123, "concurrency": 16, "max_retries": 3},
            {"limit": 0, "concurrency": 16, "max_retries": 3},
            {"limit": True, "concurrency": 16, "max_retries": 3},
            {"concurrency": 0, "max_retries": 3},
            {"concurrency": 16, "max_retries": 0},
        )
        for execution in cases:
            with self.subTest(execution=execution):
                with self.assertRaisesRegex(
                    (TypeError, ValueError),
                    "config.json field eval_execution",
                ):
                    config_module.require_config_eval_execution(
                        {"eval_execution": execution}
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

                with self.assertRaisesRegex(
                    config_module.RunConfigError, "round_id"
                ):
                    config_module.load_run_config(self._write_config(body))

    def test_inference_dataset_slug_must_be_safe_relative_path(self) -> None:
        for slug in ("", "/absolute", "echo/../eval", "echo//eval"):
            with self.subTest(slug=slug):
                body = self._valid_toml(inference_dataset_slug=f'"{slug}"')

                with self.assertRaisesRegex(
                    config_module.RunConfigError, "inference_dataset_slug"
                ):
                    config_module.load_run_config(self._write_config(body))

    def test_inline_prompts_override_defaults(self) -> None:
        body = self._valid_toml(
            prompts="""
[prompts]
system = "custom system"
user = "custom user"
"""
        )

        cfg = config_module.load_run_config(self._write_config(body))

        self.assertEqual(cfg.system_prompt, "custom system")
        self.assertEqual(cfg.user_prompt, "custom user")

    def test_omitted_user_prompt_resolves_default(self) -> None:
        cfg = config_module.load_run_config(
            self._write_config(self._valid_toml())
        )

        self.assertEqual(
            cfg.user_prompt,
            prompt_defaults.GEMINI_TRANSCRIBE_USER_PROMPT,
        )

    def test_empty_system_prompt_is_rejected(self) -> None:
        body = self._valid_toml(
            prompts="""
[prompts]
system = ""
"""
        )

        with self.assertRaisesRegex(
            config_module.RunConfigError,
            "prompts.system must be a non-empty string",
        ):
            config_module.load_run_config(self._write_config(body))

    def test_whitespace_only_user_prompt_is_rejected(self) -> None:
        body = self._valid_toml(
            prompts="""
[prompts]
user = "   "
"""
        )

        with self.assertRaisesRegex(
            config_module.RunConfigError,
            "prompts.user must be a non-empty string",
        ):
            config_module.load_run_config(self._write_config(body))

    def test_explicit_empty_user_prompt_is_preserved(self) -> None:
        body = self._valid_toml(
            prompts="""
[prompts]
user = ""
"""
        )

        cfg = config_module.load_run_config(self._write_config(body))

        self.assertEqual(cfg.user_prompt, "")
        self.assertEqual(cfg.to_record_dict()["user_prompt"], "")

    def test_empty_user_prompt_rejects_prior_context(self) -> None:
        body = self._valid_toml(
            context="""
[context]
prior_turn_count = 1
""",
            prompts="""
[prompts]
user = ""
""",
        )

        with self.assertRaisesRegex(
            config_module.RunConfigError,
            "prior_turn_count.*non-empty user prompt",
        ):
            config_module.load_run_config(self._write_config(body))

    def test_require_config_str_allows_exact_empty_only_with_opt_in(
        self,
    ) -> None:
        self.assertEqual(
            config_module.require_config_str(
                {"user_prompt": ""},
                "user_prompt",
                allow_empty=True,
            ),
            "",
        )
        for config in (
            {},
            {"user_prompt": None},
            {"user_prompt": 1},
            {"user_prompt": ""},
        ):
            with self.subTest(config=config):
                with self.assertRaisesRegex(ValueError, "user_prompt"):
                    config_module.require_config_str(config, "user_prompt")

    def test_require_config_str_rejects_whitespace_with_or_without_opt_in(
        self,
    ) -> None:
        for allow_empty in (False, True):
            with self.subTest(allow_empty=allow_empty):
                with self.assertRaisesRegex(ValueError, "user_prompt"):
                    config_module.require_config_str(
                        {"user_prompt": "   "},
                        "user_prompt",
                        allow_empty=allow_empty,
                    )

    def test_context_prior_turn_count_is_recorded(self) -> None:
        body = self._valid_toml(
            context="""
[context]
prior_turn_count = 8
prior_context_mode = "text_turns"
"""
        )

        cfg = config_module.load_run_config(self._write_config(body))

        self.assertEqual(cfg.prior_context_count, 8)
        self.assertEqual(cfg.prior_context_mode, "text_turns")
        self.assertEqual(cfg.to_record_dict()["prior_context_count"], 8)
        self.assertEqual(
            cfg.to_record_dict()["prior_context_mode"], "text_turns"
        )

    def test_guarded_transcript_block_context_mode_is_accepted(self) -> None:
        body = self._valid_toml(
            context="""
[context]
prior_turn_count = 8
prior_context_mode = "guarded_transcript_block"
"""
        )

        cfg = config_module.load_run_config(self._write_config(body))

        self.assertEqual(cfg.prior_context_count, 8)
        self.assertEqual(cfg.prior_context_mode, "guarded_transcript_block")
        self.assertEqual(
            cfg.to_record_dict()["prior_context_mode"],
            "guarded_transcript_block",
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

                with self.assertRaisesRegex(
                    config_module.RunConfigError, "prior_turn_count"
                ):
                    config_module.load_run_config(self._write_config(body))

    def test_prompt_file_keys_are_rejected(self) -> None:
        body = self._valid_toml(
            prompts="""
[prompts]
system_file = "system.txt"
"""
        )

        with self.assertRaisesRegex(
            config_module.RunConfigError, "intentionally not supported"
        ):
            config_module.load_run_config(self._write_config(body))

    def test_at_file_prompt_values_are_rejected(self) -> None:
        body = self._valid_toml(
            prompts="""
[prompts]
system = "@prompt.txt"
"""
        )

        with self.assertRaisesRegex(config_module.RunConfigError, "@|inline"):
            config_module.load_run_config(self._write_config(body))

    def test_gcs_bucket_must_be_bucket_name(self) -> None:
        body = self._valid_toml(bucket='"gs://bucket/path"')

        with self.assertRaisesRegex(config_module.RunConfigError, "bucket"):
            config_module.load_run_config(self._write_config(body))

    def test_adapter_size_two_is_accepted(self) -> None:
        cfg = config_module.load_run_config(
            self._write_config(self._valid_toml(adapter_size='"TWO"'))
        )

        self.assertEqual(cfg.adapter_size, "TWO")

    def test_unknown_adapter_size_is_rejected(self) -> None:
        with self.assertRaisesRegex(
            config_module.RunConfigError, "adapter_size"
        ):
            config_module.load_run_config(
                self._write_config(self._valid_toml(adapter_size='"THREE"'))
            )


if __name__ == "__main__":
    unittest.main()
