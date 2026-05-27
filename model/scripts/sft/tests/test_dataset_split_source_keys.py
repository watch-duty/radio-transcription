from __future__ import annotations

import sys
import unittest
from pathlib import Path

_SFT_DIR = str(Path(__file__).resolve().parent.parent)
if _SFT_DIR not in sys.path:
    sys.path.insert(0, _SFT_DIR)

from dataset_split.source_keys import resolve_source_group  # noqa: E402
from dataset_split.types import SourceIdentityError  # noqa: E402


class TestDatasetVersionSourceKeys(unittest.TestCase):
    def test_bcfy_calls_explicit_group_id(self) -> None:
        source_group = resolve_source_group(
            {"groupId": 12345},
            dataset_family="bcfy_calls",
            source_strategy="bcfy_calls",
        )

        self.assertEqual(source_group, "bcfy_calls:12345")

    def test_bcfy_calls_filename_fallback(self) -> None:
        source_group = resolve_source_group(
            {"audio_filepath": "gs://bucket/random/67890_1760000000.mp3"},
            dataset_family="bcfy_calls",
            source_strategy="bcfy_calls",
        )

        self.assertEqual(source_group, "bcfy_calls:67890")

    def test_bcfy_calls_missing_group_fails(self) -> None:
        with self.assertRaisesRegex(SourceIdentityError, "bcfy_calls"):
            resolve_source_group(
                {"url": "https://cdn.example.com/audio.mp3"},
                dataset_family="bcfy_calls",
                source_strategy="bcfy_calls",
            )

    def test_bcfy_feeds_archive_url_fallback(self) -> None:
        source_group = resolve_source_group(
            {
                "url": (
                    "https://archives.broadcastify.com/119/20260114/"
                    "202601141312-685630-119.mp3"
                )
            },
            dataset_family="bcfy_feeds",
            source_strategy="bcfy_feeds",
        )

        self.assertEqual(source_group, "bcfy_feeds:119")

    def test_bcfy_feeds_archive_url_mismatch_fails(self) -> None:
        with self.assertRaisesRegex(SourceIdentityError, "disagree"):
            resolve_source_group(
                {
                    "url": (
                        "https://archives.broadcastify.com/119/20260114/"
                        "202601141312-685630-120.mp3"
                    )
                },
                dataset_family="bcfy_feeds",
                source_strategy="bcfy_feeds",
            )

    def test_bcfy_feeds_missing_feed_fails(self) -> None:
        with self.assertRaisesRegex(SourceIdentityError, "bcfy_feeds"):
            resolve_source_group(
                {"url": "https://archives.broadcastify.com/not-an-id.mp3"},
                dataset_family="bcfy_feeds",
                source_strategy="bcfy_feeds",
            )

    def test_echo_explicit_area_and_name(self) -> None:
        source_group = resolve_source_group(
            {"area_code": "ca_chico", "echo_name": "Tehama_Sheriff_Disp"},
            dataset_family="echo",
            source_strategy="echo",
        )

        self.assertEqual(source_group, "echo:ca_chico/Tehama_Sheriff_Disp")

    def test_echo_uri_fallback(self) -> None:
        source_group = resolve_source_group(
            {
                "s3_url": (
                    "https://echo-recordings.s3.us-east-1.amazonaws.com/"
                    "ca_chico/20260331/"
                    "Tehama_Sheriff_Disp_20260331_12.mp3"
                )
            },
            dataset_family="echo",
            source_strategy="echo",
        )

        self.assertEqual(source_group, "echo:ca_chico/Tehama_Sheriff_Disp")

    def test_echo_source_map_fallback(self) -> None:
        uri = "gs://bucket/echo/file.mp3"
        source_group = resolve_source_group(
            {"audio_filepath": uri},
            dataset_family="echo",
            source_strategy="echo",
            source_map={
                uri: {
                    "area_code": "ca_red_bluff",
                    "echo_name": "Tehama_Sheriff_Disp",
                }
            },
        )

        self.assertEqual(
            source_group, "echo:ca_red_bluff/Tehama_Sheriff_Disp"
        )

    def test_echo_source_map_uses_audio_uri_field(self) -> None:
        uri = "gs://bucket/echo/file.mp3"
        source_group = resolve_source_group(
            {"audio_uri": uri},
            dataset_family="echo",
            source_strategy="echo",
            source_map={
                uri: {
                    "area_code": "ca_red_bluff",
                    "echo_name": "Tehama_Sheriff_Disp",
                }
            },
        )

        self.assertEqual(
            source_group, "echo:ca_red_bluff/Tehama_Sheriff_Disp"
        )

    def test_echo_ambiguous_name_without_area_fails(self) -> None:
        with self.assertRaisesRegex(SourceIdentityError, "ambiguous echo_name"):
            resolve_source_group(
                {"echo_name": "Tehama_Sheriff_Disp"},
                dataset_family="echo",
                source_strategy="echo",
                echo_registry={
                    "Tehama_Sheriff_Disp": {"ca_chico", "ca_red_bluff"}
                },
            )

    def test_echo_missing_identity_fails(self) -> None:
        with self.assertRaisesRegex(SourceIdentityError, "echo"):
            resolve_source_group(
                {"echo_name": "Unknown"},
                dataset_family="echo",
                source_strategy="echo",
                echo_registry={},
            )

    def test_fire_notifications_uses_location_not_group_id(self) -> None:
        source_group = resolve_source_group(
            {
                "location": "TEXAS/AL-JEFFERSON-ADAMSVILLE-DISP",
                "group_id": "sampling-day-uuid",
            },
            dataset_family="fire_notifications",
            source_strategy="fire_notifications",
        )

        self.assertEqual(
            source_group,
            "fire_notifications:TEXAS/AL-JEFFERSON-ADAMSVILLE-DISP",
        )

    def test_fire_notifications_original_path_fallback(self) -> None:
        source_group = resolve_source_group(
            {
                "original_path": (
                    "COLORADO/CO-BOULDER-DISP/Archive/20260331/file.mp3"
                )
            },
            dataset_family="fire_notifications",
            source_strategy="fire_notifications",
        )

        self.assertEqual(
            source_group, "fire_notifications:COLORADO/CO-BOULDER-DISP"
        )

    def test_fire_notifications_missing_stream_fails(self) -> None:
        with self.assertRaisesRegex(SourceIdentityError, "fire_notifications"):
            resolve_source_group(
                {"group_id": "sampling-day-uuid"},
                dataset_family="fire_notifications",
                source_strategy="fire_notifications",
            )


if __name__ == "__main__":
    unittest.main()
