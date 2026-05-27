from __future__ import annotations

import sys
import unittest
from pathlib import Path

_SFT_DIR = str(Path(__file__).resolve().parent.parent)
if _SFT_DIR not in sys.path:
    sys.path.insert(0, _SFT_DIR)

from dataset_versioning.config import (  # noqa: E402
    ConfigValidationError,
    parse_dataset_version_config_toml,
)


def _valid_config(**overrides: str) -> str:
    values = {
        "dataset_version_id": "radio-v1",
        "random_seed": "1234",
        "train_ratio": "0.8",
        "eval_ratio": "0.2",
        "output_gcs_prefix": '"gs://wd-transcription-data/sft/radio-v1"',
        "name": "calls",
        "family": "bcfy_calls",
        "manifest_uri": '"gs://bucket/manifests/calls.jsonl"',
        "source_strategy": "bcfy_calls",
        "source_map_uri": '"gs://bucket/source_maps/calls.jsonl"',
    }
    values.update(overrides)
    source_map_line = ""
    if values["source_map_uri"]:
        source_map_line = f"source_map_uri = {values['source_map_uri']}\n"

    return f"""
dataset_version_id = "{values["dataset_version_id"]}"
random_seed = {values["random_seed"]}
train_ratio = {values["train_ratio"]}
eval_ratio = {values["eval_ratio"]}
output_gcs_prefix = {values["output_gcs_prefix"]}

[[datasets]]
name = "{values["name"]}"
family = "{values["family"]}"
manifest_uri = {values["manifest_uri"]}
source_strategy = "{values["source_strategy"]}"
{source_map_line}
"""


class TestDatasetVersionConfig(unittest.TestCase):
    def test_valid_config_requires_explicit_source_strategy(self) -> None:
        config = parse_dataset_version_config_toml(_valid_config())

        self.assertEqual(config.dataset_version_id, "radio-v1")
        self.assertEqual(config.random_seed, 1234)
        self.assertEqual(config.train_ratio, 0.8)
        self.assertEqual(config.eval_ratio, 0.2)
        self.assertEqual(
            config.output_gcs_prefix,
            "gs://wd-transcription-data/sft/radio-v1",
        )
        self.assertEqual(len(config.datasets), 1)
        dataset = config.datasets[0]
        self.assertEqual(dataset.name, "calls")
        self.assertEqual(dataset.family, "bcfy_calls")
        self.assertEqual(dataset.source_strategy, "bcfy_calls")
        self.assertEqual(
            dataset.source_map_uri,
            "gs://bucket/source_maps/calls.jsonl",
        )

    def test_missing_source_strategy_fails(self) -> None:
        text = _valid_config().replace('source_strategy = "bcfy_calls"\n', "")

        with self.assertRaisesRegex(
            ConfigValidationError, "source_strategy"
        ):
            parse_dataset_version_config_toml(text)

    def test_rejects_non_gs_manifest_uri(self) -> None:
        text = _valid_config(manifest_uri='"s3://bucket/calls.jsonl"')

        with self.assertRaisesRegex(ConfigValidationError, "manifest_uri"):
            parse_dataset_version_config_toml(text)

    def test_rejects_non_gs_source_map_uri(self) -> None:
        text = _valid_config(source_map_uri='"./source-map.jsonl"')

        with self.assertRaisesRegex(ConfigValidationError, "source_map_uri"):
            parse_dataset_version_config_toml(text)

    def test_rejects_duplicate_dataset_names(self) -> None:
        text = _valid_config() + """
[[datasets]]
name = "calls"
family = "bcfy_feeds"
manifest_uri = "gs://bucket/manifests/feeds.jsonl"
source_strategy = "bcfy_feeds"
"""

        with self.assertRaisesRegex(ConfigValidationError, "duplicate"):
            parse_dataset_version_config_toml(text)

    def test_rejects_unknown_family(self) -> None:
        text = _valid_config(family="unknown")

        with self.assertRaisesRegex(ConfigValidationError, "family"):
            parse_dataset_version_config_toml(text)

    def test_rejects_train_eval_ratio_sum_failure(self) -> None:
        text = _valid_config(eval_ratio="0.3")

        with self.assertRaisesRegex(ConfigValidationError, "train_ratio"):
            parse_dataset_version_config_toml(text)


if __name__ == "__main__":
    unittest.main()
