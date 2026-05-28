from __future__ import annotations

import sys
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

from dataset_split.config import (  # noqa: E402
    DatasetVersionConfig,
    InputDatasetConfig,
)
from dataset_split.validate import (  # noqa: E402
    DatasetValidationError,
    load_and_validate_datasets,
    load_source_map,
    validate_dataset,
)


class FakeTextReader:
    def __init__(self, values: dict[str, str]) -> None:
        self.values = values

    def read_text(self, uri: str) -> str:
        return self.values[uri]


def _config(*datasets: InputDatasetConfig) -> DatasetVersionConfig:
    return DatasetVersionConfig(
        dataset_version_id="radio-v1",
        train_ratio=0.8,
        eval_ratio=0.2,
        output_gcs_prefix="gs://bucket/out",
        datasets=datasets,
    )


def _dataset(
    name: str,
    family: str,
    manifest_uri: str,
    *,
    source_map_uri: str | None = None,
) -> InputDatasetConfig:
    return InputDatasetConfig(
        name=name,
        family=family,
        manifest_uri=manifest_uri,
        source_strategy=family,
        source_map_uri=source_map_uri,
    )


class TestDatasetVersionValidate(unittest.TestCase):
    def test_validation_summarizes_loaded_valid_and_excluded_rows(self) -> None:
        config = _config(
            _dataset("calls", "bcfy_calls", "gs://manifests/calls.jsonl"),
            _dataset("feeds", "bcfy_feeds", "gs://manifests/feeds.jsonl"),
        )
        reader = FakeTextReader(
            {
                "gs://manifests/calls.jsonl": (
                    '{"audio_filepath": "gs://bucket/123_1700000000.mp3", '
                    '"text": "alpha", "duration": 1.0}\n'
                    '{"audio_filepath": "gs://bucket/123_1700000001.mp3", '
                    '"text": "  "}\n'
                ),
                "gs://manifests/feeds.jsonl": (
                    '{"url": "https://archives.broadcastify.com/119/20260114/'
                    '202601141312-685630-119.mp3", "text": "bravo", '
                    '"duration": 1.0}\n'
                ),
            }
        )

        summaries = validate_dataset(config, reader=reader)

        self.assertEqual(len(summaries), 2)
        self.assertEqual(summaries[0].dataset_name, "calls")
        self.assertEqual(summaries[0].loaded_rows, 2)
        self.assertEqual(summaries[0].valid_rows, 1)
        self.assertEqual(summaries[0].excluded_empty_text, 1)
        self.assertEqual(summaries[1].dataset_name, "feeds")
        self.assertEqual(summaries[1].valid_rows, 1)

    def test_source_map_loading(self) -> None:
        uri = "gs://audio/echo.mp3"
        source_map = load_source_map(
            "gs://maps/echo.jsonl",
            FakeTextReader(
                {
                    "gs://maps/echo.jsonl": (
                        f'{{"audio_uri": "{uri}", "area_code": "ca_chico", '
                        '"echo_name": "Tehama_Sheriff_Disp"}\n'
                    )
                }
            ),
        )

        self.assertEqual(
            source_map[uri],
            {"area_code": "ca_chico", "echo_name": "Tehama_Sheriff_Disp"},
        )

    def test_validation_uses_source_map(self) -> None:
        audio_uri = "gs://audio/echo.mp3"
        config = _config(
            _dataset(
                "echo",
                "echo",
                "gs://manifests/echo.jsonl",
                source_map_uri="gs://maps/echo.jsonl",
            )
        )
        reader = FakeTextReader(
            {
                "gs://manifests/echo.jsonl": (
                    f'{{"audio_filepath": "{audio_uri}", "text": "alpha", '
                    '"duration": 1.0}\n'
                ),
                "gs://maps/echo.jsonl": (
                    f'{{"audio_filepath": "{audio_uri}", '
                    '"area_code": "ca_red_bluff", '
                    '"echo_name": "Tehama_Sheriff_Disp"}\n'
                ),
            }
        )

        summaries = validate_dataset(config, reader=reader)

        self.assertEqual(summaries[0].valid_rows, 1)

    def test_validation_precomputes_echo_ambiguity_policy(self) -> None:
        config = _config(_dataset("echo", "echo", "gs://manifests/echo.jsonl"))
        reader = FakeTextReader(
            {
                "gs://manifests/echo.jsonl": (
                    '{"audio_filepath": "gs://audio/Lake_Co_Red_20250326_07.wav", '
                    '"text": "alpha", "duration": 1.0}\n'
                    '{"audio_filepath": "gs://audio/Wahkiakum_Fire_Disp_20250326_07.wav", '
                    '"text": "bravo", "duration": 1.0}\n'
                )
            }
        )

        result = load_and_validate_datasets(
            config,
            reader=reader,
            echo_registry={
                "Lake_Co_Red": {"ca_petaluma", "ca_willits"},
                "Wahkiakum_Fire_Disp": {"wa_cathlamet"},
            },
        )

        self.assertEqual(
            [segment.source_group for segment in result.segments],
            ["echo:Lake_Co_Red", "echo:wa_cathlamet/Wahkiakum_Fire_Disp"],
        )

    def test_source_identity_error_includes_dataset_manifest_and_strategy(
        self,
    ) -> None:
        config = _config(
            _dataset("feeds", "bcfy_feeds", "gs://manifests/feeds.jsonl")
        )
        reader = FakeTextReader(
            {
                "gs://manifests/feeds.jsonl": (
                    '{"url": "https://archives.broadcastify.com/bad.mp3", '
                    '"text": "alpha"}\n'
                )
            }
        )

        with self.assertRaisesRegex(
            DatasetValidationError,
            "dataset=feeds .*manifest=gs://manifests/feeds.jsonl "
            ".*source_strategy=bcfy_feeds .*bcfy_feeds",
        ):
            validate_dataset(config, reader=reader)

    def test_zero_valid_examples_is_hard_failure(self) -> None:
        config = _config(
            _dataset("calls", "bcfy_calls", "gs://manifests/calls.jsonl")
        )
        reader = FakeTextReader(
            {
                "gs://manifests/calls.jsonl": (
                    '{"audio_filepath": "gs://bucket/123_1700000000.mp3", '
                    '"text": "  "}\n'
                )
            }
        )

        with self.assertRaisesRegex(
            DatasetValidationError,
            "dataset=calls .*manifest=gs://manifests/calls.jsonl "
            ".*source_strategy=bcfy_calls .*produced zero valid examples",
        ):
            validate_dataset(config, reader=reader)

    def test_all_four_source_strategies_are_represented(self) -> None:
        config = _config(
            _dataset("bcfy_calls", "bcfy_calls", "gs://m/calls.jsonl"),
            _dataset("bcfy_feeds", "bcfy_feeds", "gs://m/feeds.jsonl"),
            _dataset("echo", "echo", "gs://m/echo.jsonl"),
            _dataset(
                "fire_notifications",
                "fire_notifications",
                "gs://m/fire.jsonl",
            ),
        )
        reader = FakeTextReader(
            {
                "gs://m/calls.jsonl": (
                    '{"audio_filepath": "gs://bucket/123_1700000000.mp3", '
                    '"text": "alpha", "duration": 1.0}\n'
                ),
                "gs://m/feeds.jsonl": (
                    '{"url": "https://archives.broadcastify.com/119/20260114/'
                    '202601141312-685630-119.mp3", "text": "bravo", '
                    '"duration": 1.0}\n'
                ),
                "gs://m/echo.jsonl": (
                    '{"area_code": "ca_chico", '
                    '"echo_name": "Tehama_Sheriff_Disp", '
                    '"audio_filepath": "gs://bucket/echo.mp3", '
                    '"text": "charlie", "duration": 1.0}\n'
                ),
                "gs://m/fire.jsonl": (
                    '{"location": "TEXAS/AL-JEFFERSON-ADAMSVILLE-DISP", '
                    '"url": "https://player.textmefires.info/a.mp3", '
                    '"text": "delta", "duration": 1.0}\n'
                ),
            }
        )

        summaries = validate_dataset(config, reader=reader)

        self.assertEqual(
            [summary.dataset_name for summary in summaries],
            ["bcfy_calls", "bcfy_feeds", "echo", "fire_notifications"],
        )


if __name__ == "__main__":
    unittest.main()
