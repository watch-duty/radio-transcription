from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import patch

_SFT_DIR = str(Path(__file__).resolve().parent.parent)
_COLABS_DIR = str(
    Path(__file__).resolve().parent.parent.parent.parent / "colabs"
)
if _SFT_DIR not in sys.path:
    sys.path.insert(0, _SFT_DIR)
if _COLABS_DIR not in sys.path:
    sys.path.insert(0, _COLABS_DIR)

from dataset_split.artifacts import DatasetArtifactLayout  # noqa: E402
from dataset_split.audio import (  # noqa: E402
    AUDIO_ACTIONS,
    EXTERNAL_DOWNLOAD_CHUNK_SIZE,
    EXTERNAL_DOWNLOAD_MAX_BYTES,
    EXTERNAL_DOWNLOAD_TIMEOUT,
    AudioDerivationError,
    AudioProbe,
    plan_audio_actions,
    probe_audio,
    stage_source_audio,
)
from dataset_split.types import LabeledSegment  # noqa: E402


class FakeDownloadedBlob:
    def __init__(self, content: bytes = b"fake audio") -> None:
        self.content = content
        self.downloads: list[str] = []

    def download_to_filename(self, filename: str, **_: object) -> None:
        self.downloads.append(filename)
        Path(filename).write_bytes(self.content)


class FakeBucket:
    def __init__(self) -> None:
        self.blobs: dict[str, FakeDownloadedBlob] = {}

    def blob(self, path: str) -> FakeDownloadedBlob:
        blob = self.blobs.setdefault(path, FakeDownloadedBlob())
        return blob


class FakeStorageClient:
    def __init__(self) -> None:
        self.buckets: dict[str, FakeBucket] = {}

    def bucket(self, bucket_name: str) -> FakeBucket:
        bucket = self.buckets.setdefault(bucket_name, FakeBucket())
        return bucket


class FakeResponse:
    def __init__(self, chunks: tuple[bytes, ...]) -> None:
        self.chunks = chunks
        self.chunk_sizes: list[int] = []
        self.closed = False

    def __enter__(self) -> FakeResponse:
        return self

    def __exit__(self, *_: object) -> None:
        self.closed = True

    def raise_for_status(self) -> None:
        return None

    def iter_content(self, chunk_size: int) -> Any:
        self.chunk_sizes.append(chunk_size)
        yield from self.chunks


def _segment(
    audio_uri: str = "gs://source-bucket/audio/example.flac",
    *,
    offset: float = 0.0,
    duration: float = 10.0,
    row_index: int = 3,
    split: str = "train",
) -> LabeledSegment:
    return LabeledSegment(
        dataset_name="calls",
        dataset_family="bcfy_calls",
        source_strategy="bcfy_calls",
        source_group="bcfy_calls:feed-a",
        audio_uri=audio_uri,
        original_audio_uri=audio_uri,
        text="engine 41 copy",
        row_index=row_index,
        offset=offset,
        duration=duration,
        timestamp="2026-05-27T12:00:00Z",
        example_id=f"example-{row_index}",
        segment_id=f"segment-{row_index}",
        split=split,
    )


def _layout() -> DatasetArtifactLayout:
    return DatasetArtifactLayout.for_dataset_version("dv-001")


def _ffprobe_runner(
    *,
    duration: float,
    codec_name: str = "flac",
    channels: int = 2,
    sample_rate: int = 44100,
    format_name: str = "flac",
    calls: list[dict[str, object]] | None = None,
):
    def runner(args: list[str], **kwargs: object) -> subprocess.CompletedProcess:
        if calls is not None:
            calls.append({"args": args, "kwargs": kwargs})
        payload = {
            "format": {
                "duration": str(duration),
                "format_name": format_name,
            },
            "streams": [
                {
                    "codec_name": codec_name,
                    "channels": channels,
                    "sample_rate": str(sample_rate),
                }
            ],
        }
        return subprocess.CompletedProcess(
            args=args, returncode=0, stdout=json.dumps(payload), stderr=""
        )

    return runner


def _plan(
    segment: LabeledSegment,
    *,
    probe_duration: float,
    codec_name: str = "flac",
    format_name: str = "flac",
):
    with tempfile.TemporaryDirectory() as scratch_dir:
        return plan_audio_actions(
            FakeStorageClient(),
            layout=_layout(),
            segments=(segment,),
            scratch_dir=scratch_dir,
            runner=_ffprobe_runner(
                duration=probe_duration,
                codec_name=codec_name,
                format_name=format_name,
            ),
        )[0]


class TestAudioActionPlanning(unittest.TestCase):
    def test_duration_less_than_or_equal_to_zero_fails(self) -> None:
        with tempfile.TemporaryDirectory() as scratch_dir:
            with self.assertRaisesRegex(
                AudioDerivationError,
                "dataset_name=calls.*row_index=3.*duration=0.0",
            ):
                plan_audio_actions(
                    FakeStorageClient(),
                    layout=_layout(),
                    segments=(_segment(duration=0.0),),
                    scratch_dir=scratch_dir,
                    runner=_ffprobe_runner(duration=10.0),
                )

    def test_probe_audio_called_before_action_selection(self) -> None:
        plan = _plan(_segment(duration=10.0), probe_duration=20.0)

        self.assertEqual(plan.action, "derived")
        self.assertIsInstance(plan.probe, AudioProbe)
        self.assertEqual(plan.probe.duration, 20.0)

    def test_supported_standalone_gcs_clip_is_reused(self) -> None:
        plan = _plan(
            _segment("gs://source-bucket/audio/example.flac", duration=10.0),
            probe_duration=10.2,
        )

        self.assertEqual(plan.action, "reused")
        self.assertEqual(plan.source_uri, plan.segment.audio_uri)
        self.assertIsNone(plan.destination_uri)
        self.assertEqual(plan.output_suffix, ".flac")

    def test_positive_offset_selects_derived(self) -> None:
        plan = _plan(
            _segment(offset=2.0, duration=6.0),
            probe_duration=30.0,
        )

        self.assertEqual(plan.action, "derived")
        self.assertIsNotNone(plan.destination_uri)
        self.assertIn("audio/derived/", plan.destination_uri)
        self.assertEqual(plan.output_suffix, ".flac")

    def test_offset_duration_beyond_source_duration_fails(self) -> None:
        with self.assertRaisesRegex(
            AudioDerivationError,
            (
                "dataset_name=calls.*row_index=3.*audio_uri=gs://source-bucket"
                "/audio/example.flac.*offset=9.0.*duration=5.0.*split=train"
                ".*source_group=bcfy_calls:feed-a"
            ),
        ):
            _plan(_segment(offset=9.0, duration=5.0), probe_duration=10.0)

    def test_audio_action_vocabulary_is_exact(self) -> None:
        self.assertEqual(
            AUDIO_ACTIONS, ("reused", "copied", "derived", "transcoded")
        )

    @patch("dataset_split.audio.requests.get")
    def test_supported_standalone_non_gcs_clip_is_copied(
        self, mock_get: Any
    ) -> None:
        mock_get.return_value = FakeResponse((b"fake",))

        plan = _plan(
            _segment("https://example.test/audio/source.mp3", duration=8.0),
            probe_duration=8.1,
            codec_name="mp3",
            format_name="mp3",
        )

        self.assertEqual(plan.action, "copied")
        self.assertIn("audio/copied/", plan.destination_uri or "")
        self.assertEqual(plan.output_suffix, ".mp3")

    def test_unsupported_standalone_source_selects_transcoded(self) -> None:
        plan = _plan(
            _segment("gs://source-bucket/audio/example.wav", duration=8.0),
            probe_duration=8.1,
            codec_name="pcm_s16le",
            format_name="wav",
        )

        self.assertEqual(plan.action, "transcoded")
        self.assertIn("audio/transcoded/", plan.destination_uri or "")
        self.assertEqual(plan.output_suffix, ".flac")


class TestAudioCommands(unittest.TestCase):
    def test_probe_audio_uses_json_argv_without_shell(self) -> None:
        calls: list[dict[str, object]] = []

        probe = probe_audio(
            Path("/tmp/input.flac"),
            runner=_ffprobe_runner(duration=4.25, calls=calls),
        )

        self.assertEqual(probe.duration, 4.25)
        args = calls[0]["args"]
        kwargs = calls[0]["kwargs"]
        self.assertEqual(args[0], "ffprobe")
        self.assertIn("-of", args)
        self.assertIn("json", args)
        self.assertNotIn("shell", kwargs)

    def test_derive_command_downmixes_to_mono_without_resampling(self) -> None:
        from dataset_split import audio as audio_module

        derive_audio_clip = getattr(audio_module, "derive_audio_clip", None)
        if derive_audio_clip is None:
            self.skipTest("derive_audio_clip is implemented in Task 3")

        calls: list[dict[str, object]] = []

        def runner(
            args: list[str], **kwargs: object
        ) -> subprocess.CompletedProcess:
            calls.append({"args": args, "kwargs": kwargs})
            return subprocess.CompletedProcess(args=args, returncode=0)

        derive_audio_clip(
            Path("/tmp/input.wav"),
            Path("/tmp/output.flac"),
            offset=1.25,
            duration=2.5,
            runner=runner,
        )

        args = calls[0]["args"]
        self.assertEqual(args[0], "ffmpeg")
        self.assertIn("-ac", args)
        self.assertEqual(args[args.index("-ac") + 1], "1")
        self.assertIn("-c:a", args)
        self.assertEqual(args[args.index("-c:a") + 1], "flac")
        self.assertNotIn("-ar", args)
        self.assertNotIn("apad", args)
        self.assertNotIn("adelay", args)
        self.assertNotIn("shell", calls[0]["kwargs"])

    def test_transcode_command_downmixes_without_clipping_or_padding(
        self,
    ) -> None:
        from dataset_split import audio as audio_module

        transcode_audio_file = getattr(
            audio_module, "transcode_audio_file", None
        )
        if transcode_audio_file is None:
            self.skipTest("transcode_audio_file is implemented in Task 3")

        calls: list[dict[str, object]] = []

        def runner(
            args: list[str], **kwargs: object
        ) -> subprocess.CompletedProcess:
            calls.append({"args": args, "kwargs": kwargs})
            return subprocess.CompletedProcess(args=args, returncode=0)

        transcode_audio_file(
            Path("/tmp/input.wav"),
            Path("/tmp/output.flac"),
            runner=runner,
        )

        args = calls[0]["args"]
        self.assertIn("-ac", args)
        self.assertEqual(args[args.index("-ac") + 1], "1")
        self.assertIn("-c:a", args)
        self.assertEqual(args[args.index("-c:a") + 1], "flac")
        self.assertNotIn("-ss", args)
        self.assertNotIn("-t", args)
        self.assertNotIn("-ar", args)
        self.assertNotIn("apad", args)
        self.assertNotIn("adelay", args)
        self.assertNotIn("shell", calls[0]["kwargs"])


class TestAudioPreparation(unittest.TestCase):
    @patch("dataset_split.audio.requests.get")
    def test_external_url_staging_uses_streaming_download_controls(
        self, mock_get: Any
    ) -> None:
        response = FakeResponse((b"abc", b"", b"def"))
        mock_get.return_value = response
        with tempfile.TemporaryDirectory() as scratch_dir:
            local_path = stage_source_audio(
                FakeStorageClient(),
                "https://example.test/audio/source.mp3",
                scratch_dir,
            )

        mock_get.assert_called_once_with(
            "https://example.test/audio/source.mp3",
            stream=True,
            timeout=EXTERNAL_DOWNLOAD_TIMEOUT,
        )
        self.assertEqual(response.chunk_sizes, [EXTERNAL_DOWNLOAD_CHUNK_SIZE])
        self.assertEqual(Path(local_path).suffix, ".mp3")
        self.assertTrue(response.closed)

    @patch("dataset_split.audio.EXTERNAL_DOWNLOAD_MAX_BYTES", 3)
    @patch("dataset_split.audio.requests.get")
    def test_external_url_staging_enforces_max_bytes(
        self, mock_get: Any
    ) -> None:
        self.assertEqual(EXTERNAL_DOWNLOAD_MAX_BYTES, 512 * 1024 * 1024)
        mock_get.return_value = FakeResponse((b"ab", b"cd"))

        with tempfile.TemporaryDirectory() as scratch_dir:
            with self.assertRaisesRegex(
                AudioDerivationError, "exceeded maximum"
            ):
                stage_source_audio(
                    FakeStorageClient(),
                    "https://example.test/audio/source.mp3",
                    scratch_dir,
                )


if __name__ == "__main__":
    unittest.main()
