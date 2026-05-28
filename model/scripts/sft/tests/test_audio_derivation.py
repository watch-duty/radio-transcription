from __future__ import annotations

import json
import math
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
    AUDIO_UPLOAD_MAX_WORKERS,
    EXTERNAL_DOWNLOAD_CHUNK_SIZE,
    EXTERNAL_DOWNLOAD_MAX_BYTES,
    EXTERNAL_DOWNLOAD_TIMEOUT,
    FFMPEG_TIMEOUT_SECONDS,
    FFPROBE_TIMEOUT_SECONDS,
    GENERATED_OUTPUT_DURATION_TOLERANCE_SECONDS,
    PROGRAM_VERSION_TIMEOUT_SECONDS,
    AudioDerivationError,
    AudioPreparationResult,
    AudioProbe,
    AudioUploadTask,
    plan_audio_actions,
    prepare_audio_for_publication,
    probe_audio,
    stage_source_audio,
    upload_prepared_audio,
)
from dataset_split.types import LabeledSegment  # noqa: E402


class FakeDownloadedBlob:
    def __init__(self, content: bytes = b"fake audio") -> None:
        self.content = content
        self.downloads: list[str] = []
        self.uploads: list[dict[str, object]] = []

    def download_to_filename(self, filename: str, **_: object) -> None:
        self.downloads.append(filename)
        Path(filename).write_bytes(self.content)

    def upload_from_filename(
        self,
        filename: str,
        *,
        content_type: str,
        if_generation_match: int,
    ) -> None:
        self.uploads.append(
            {
                "filename": filename,
                "content_type": content_type,
                "if_generation_match": if_generation_match,
            }
        )


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


def _all_uploads(client: FakeStorageClient) -> list[dict[str, object]]:
    return [
        upload
        for bucket in client.buckets.values()
        for blob in bucket.blobs.values()
        for upload in blob.uploads
    ]


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
    def runner(
        args: list[str], **kwargs: object
    ) -> subprocess.CompletedProcess:
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


class FakeAudioRunner:
    def __init__(self, probes: tuple[AudioProbe, ...]) -> None:
        self.probes = list(probes)
        self.calls: list[dict[str, object]] = []

    def __call__(
        self, args: list[str], **kwargs: object
    ) -> subprocess.CompletedProcess:
        self.calls.append({"args": args, "kwargs": kwargs})
        if args == ["ffprobe", "-version"]:
            return subprocess.CompletedProcess(
                args=args,
                returncode=0,
                stdout="ffprobe version 7.0.2-test\n",
                stderr="",
            )
        if args == ["ffmpeg", "-version"]:
            return subprocess.CompletedProcess(
                args=args,
                returncode=0,
                stdout="ffmpeg version 7.0.2-test\n",
                stderr="",
            )
        if args[0] == "ffprobe":
            probe = self.probes.pop(0)
            return subprocess.CompletedProcess(
                args=args,
                returncode=0,
                stdout=json.dumps(
                    {
                        "format": {
                            "duration": str(probe.duration),
                            "format_name": probe.format_name,
                        },
                        "streams": [
                            {
                                "codec_name": probe.codec_name,
                                "channels": probe.channels,
                                "sample_rate": str(probe.sample_rate),
                            }
                        ],
                    }
                ),
                stderr="",
            )
        if args[0] == "ffmpeg":
            Path(args[-1]).write_bytes(b"generated audio")
            return subprocess.CompletedProcess(
                args=args, returncode=0, stdout="", stderr=""
            )
        raise AssertionError(f"unexpected command: {args}")

    def ffmpeg_transform_commands(self) -> list[list[str]]:
        return [
            call["args"]
            for call in self.calls
            if call["args"][0] == "ffmpeg"
            and call["args"] != ["ffmpeg", "-version"]
        ]


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

    def test_non_finite_span_values_fail_before_ffmpeg(self) -> None:
        for offset, duration in (
            (math.nan, 10.0),
            (math.inf, 10.0),
            (0.0, math.nan),
            (0.0, math.inf),
        ):
            with self.subTest(offset=offset, duration=duration):
                with self.assertRaisesRegex(
                    AudioDerivationError, "must be finite"
                ):
                    _plan(
                        _segment(offset=offset, duration=duration),
                        probe_duration=10.0,
                    )

    @patch("dataset_split.audio.requests.get")
    @patch(
        "dataset_split.audio.socket.getaddrinfo",
        return_value=[(None, None, None, None, ("93.184.216.34", 443))],
    )
    def test_supported_standalone_non_gcs_clip_is_copied(
        self, _mock_getaddrinfo: Any, mock_get: Any
    ) -> None:
        mock_get.return_value = FakeResponse((b"fake",))

        plan = _plan(
            _segment(
                "https://archives.broadcastify.com/119/20260114/source.mp3",
                duration=8.0,
            ),
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
        self.assertEqual(kwargs["timeout"], FFPROBE_TIMEOUT_SECONDS)
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
        self.assertEqual(calls[0]["kwargs"]["timeout"], FFMPEG_TIMEOUT_SECONDS)
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
        self.assertEqual(calls[0]["kwargs"]["timeout"], FFMPEG_TIMEOUT_SECONDS)
        self.assertNotIn("shell", calls[0]["kwargs"])


class TestAudioPreparation(unittest.TestCase):
    def setUp(self) -> None:
        self.upload_many_calls: list[dict[str, object]] = []
        self._upload_many_patcher = patch(
            "dataset_split.audio.transfer_manager.upload_many",
            side_effect=self._fake_upload_many,
        )
        self._upload_many_patcher.start()
        self.addCleanup(self._upload_many_patcher.stop)

    def _fake_upload_many(
        self,
        file_blob_pairs: list[tuple[str, FakeDownloadedBlob]],
        *,
        upload_kwargs: dict[str, object],
        raise_exception: bool,
        worker_type: str,
        max_workers: int,
    ) -> list[Exception | None]:
        pairs = tuple(file_blob_pairs)
        self.upload_many_calls.append(
            {
                "pairs": pairs,
                "upload_kwargs": dict(upload_kwargs),
                "raise_exception": raise_exception,
                "worker_type": worker_type,
                "max_workers": max_workers,
            }
        )
        results: list[Exception | None] = []
        for filename, blob in pairs:
            try:
                blob.upload_from_filename(
                    filename,
                    content_type=str(upload_kwargs["content_type"]),
                    if_generation_match=int(
                        upload_kwargs["if_generation_match"]
                    ),
                )
            except Exception as exc:
                results.append(exc)
            else:
                results.append(None)
        return results

    def test_prepare_audio_enriches_segments_with_transformation_metadata(
        self,
    ) -> None:
        runner = FakeAudioRunner(
            (
                AudioProbe(10.0, "flac", 2, 44100, "flac"),
                AudioProbe(30.0, "flac", 2, 44100, "flac"),
                AudioProbe(8.0, "pcm_s16le", 2, 48000, "wav"),
                AudioProbe(6.0, "flac", 1, 44100, "flac"),
                AudioProbe(8.0, "flac", 1, 48000, "flac"),
            )
        )
        segments = (
            _segment(
                "gs://source-bucket/audio/reused.flac",
                duration=10.0,
                row_index=1,
            ),
            _segment(
                "gs://source-bucket/audio/source.flac",
                offset=2.0,
                duration=6.0,
                row_index=2,
            ),
            _segment(
                "gs://source-bucket/audio/source.wav",
                duration=8.0,
                row_index=3,
            ),
        )

        with tempfile.TemporaryDirectory() as scratch_dir:
            result = prepare_audio_for_publication(
                FakeStorageClient(),
                layout=_layout(),
                segments=segments,
                scratch_dir=scratch_dir,
                runner=runner,
            )

        self.assertEqual(
            result.action_counts,
            {"reused": 1, "copied": 0, "derived": 1, "transcoded": 1},
        )
        self.assertEqual(len(result.uploaded_audio_uris), 2)
        reused, derived, transcoded = result.segments
        self.assertEqual(
            reused.model_ready_audio_uri,
            "gs://source-bucket/audio/reused.flac",
        )
        self.assertIn("audio/derived/", derived.model_ready_audio_uri or "")
        self.assertIn(
            "audio/transcoded/", transcoded.model_ready_audio_uri or ""
        )
        metadata = derived.transformation_metadata or {}
        for key in (
            "action",
            "original_audio_uri",
            "source_audio_uri",
            "offset",
            "duration",
            "source_duration",
            "output_duration",
            "source_format",
            "output_format",
            "source_channels",
            "output_channels",
            "mixed_to_mono",
            "resampled",
            "padded",
            "split",
            "source_group",
            "ffprobe",
            "ffmpeg",
            "ffprobe_version",
            "ffmpeg_version",
        ):
            self.assertIn(key, metadata)
        self.assertEqual(metadata["action"], "derived")
        self.assertEqual(metadata["source_channels"], 2)
        self.assertEqual(metadata["output_channels"], 1)
        self.assertTrue(metadata["mixed_to_mono"])
        self.assertFalse(metadata["resampled"])
        self.assertFalse(metadata["padded"])
        self.assertIn("ffmpeg -hide_banner", str(metadata["ffmpeg"]))
        self.assertEqual(
            metadata["ffprobe_version"], "ffprobe version 7.0.2-test"
        )
        self.assertEqual(
            metadata["ffmpeg_version"], "ffmpeg version 7.0.2-test"
        )

    def test_derived_sets_derived_audio_uri_only_for_clipped_spans(
        self,
    ) -> None:
        runner = FakeAudioRunner(
            (
                AudioProbe(10.0, "flac", 1, 16000, "flac"),
                AudioProbe(30.0, "flac", 1, 16000, "flac"),
                AudioProbe(8.0, "pcm_s16le", 1, 16000, "wav"),
                AudioProbe(6.0, "flac", 1, 16000, "flac"),
                AudioProbe(8.0, "flac", 1, 16000, "flac"),
            )
        )
        segments = (
            _segment(
                "gs://source-bucket/audio/reused.flac",
                duration=10.0,
                row_index=1,
            ),
            _segment(
                "gs://source-bucket/audio/source.flac",
                offset=2.0,
                duration=6.0,
                row_index=2,
            ),
            _segment(
                "gs://source-bucket/audio/source.wav",
                duration=8.0,
                row_index=3,
            ),
        )

        with tempfile.TemporaryDirectory() as scratch_dir:
            result = prepare_audio_for_publication(
                FakeStorageClient(),
                layout=_layout(),
                segments=segments,
                scratch_dir=scratch_dir,
                runner=runner,
            )

        self.assertIsNone(result.segments[0].derived_audio_uri)
        self.assertEqual(
            result.segments[1].derived_audio_uri,
            result.segments[1].model_ready_audio_uri,
        )
        self.assertIsNone(result.segments[2].derived_audio_uri)

    @patch("dataset_split.audio.requests.get")
    @patch(
        "dataset_split.audio.socket.getaddrinfo",
        return_value=[(None, None, None, None, ("93.184.216.34", 443))],
    )
    def test_copied_action_uploads_staged_file_without_ffmpeg(
        self, _mock_getaddrinfo: Any, mock_get: Any
    ) -> None:
        mock_get.return_value = FakeResponse((b"mp3 bytes",))
        runner = FakeAudioRunner((AudioProbe(8.0, "mp3", 2, 44100, "mp3"),))

        with tempfile.TemporaryDirectory() as scratch_dir:
            result = prepare_audio_for_publication(
                FakeStorageClient(),
                layout=_layout(),
                segments=(
                    _segment(
                        "https://archives.broadcastify.com/119/20260114/source.mp3",
                        duration=8.0,
                    ),
                ),
                scratch_dir=scratch_dir,
                runner=runner,
            )

        self.assertEqual(result.action_counts["copied"], 1)
        segment = result.segments[0]
        self.assertIn("audio/copied/", segment.model_ready_audio_uri or "")
        self.assertIsNone(segment.derived_audio_uri)
        self.assertEqual(
            (segment.transformation_metadata or {})["output_channels"], 2
        )
        self.assertFalse(
            (segment.transformation_metadata or {})["mixed_to_mono"]
        )
        self.assertEqual(runner.ffmpeg_transform_commands(), [])

    def test_generated_output_duration_mismatch_fails_before_upload(
        self,
    ) -> None:
        client = FakeStorageClient()
        runner = FakeAudioRunner(
            (
                AudioProbe(30.0, "flac", 1, 16000, "flac"),
                AudioProbe(2.0, "flac", 1, 16000, "flac"),
            )
        )

        with tempfile.TemporaryDirectory() as scratch_dir:
            with self.assertRaisesRegex(
                AudioDerivationError, "generated audio duration mismatch"
            ):
                prepare_audio_for_publication(
                    client,
                    layout=_layout(),
                    segments=(
                        _segment(
                            "gs://source-bucket/audio/source.flac",
                            offset=2.0,
                            duration=6.0,
                        ),
                    ),
                    scratch_dir=scratch_dir,
                    runner=runner,
                )

        self.assertEqual(_all_uploads(client), [])

    def test_near_eof_generated_output_matches_available_source_duration(
        self,
    ) -> None:
        client = FakeStorageClient()
        runner = FakeAudioRunner(
            (
                AudioProbe(10.0, "flac", 1, 16000, "flac"),
                AudioProbe(0.3, "flac", 1, 16000, "flac"),
            )
        )

        with tempfile.TemporaryDirectory() as scratch_dir:
            result = prepare_audio_for_publication(
                client,
                layout=_layout(),
                segments=(
                    _segment(
                        "gs://source-bucket/audio/source.flac",
                        offset=9.7,
                        duration=0.6,
                    ),
                ),
                scratch_dir=scratch_dir,
                runner=runner,
                upload=False,
            )

        metadata = result.segments[0].transformation_metadata or {}
        self.assertEqual(metadata["output_duration"], 0.3)
        self.assertEqual(_all_uploads(client), [])

    def test_transcoded_standalone_output_matches_source_duration(
        self,
    ) -> None:
        client = FakeStorageClient()
        runner = FakeAudioRunner(
            (
                AudioProbe(10.2, "pcm_s16le", 1, 16000, "wav"),
                AudioProbe(10.2, "flac", 1, 16000, "flac"),
            )
        )

        with tempfile.TemporaryDirectory() as scratch_dir:
            result = prepare_audio_for_publication(
                client,
                layout=_layout(),
                segments=(
                    _segment(
                        "gs://source-bucket/audio/source.wav",
                        offset=0.0,
                        duration=10.0,
                    ),
                ),
                scratch_dir=scratch_dir,
                runner=runner,
                upload=False,
            )

        metadata = result.segments[0].transformation_metadata or {}
        self.assertEqual(metadata["action"], "transcoded")
        self.assertEqual(metadata["output_duration"], 10.2)
        self.assertEqual(_all_uploads(client), [])

    def test_prepare_audio_can_defer_uploads(self) -> None:
        client = FakeStorageClient()
        runner = FakeAudioRunner(
            (
                AudioProbe(30.0, "flac", 1, 16000, "flac"),
                AudioProbe(6.0, "flac", 1, 16000, "flac"),
            )
        )

        with tempfile.TemporaryDirectory() as scratch_dir:
            result = prepare_audio_for_publication(
                client,
                layout=_layout(),
                segments=(
                    _segment(
                        "gs://source-bucket/audio/source.flac",
                        offset=2.0,
                        duration=6.0,
                    ),
                ),
                scratch_dir=scratch_dir,
                runner=runner,
                upload=False,
            )
            self.assertEqual(result.uploaded_audio_uris, ())
            self.assertEqual(len(result.upload_tasks), 1)
            self.assertTrue(result.upload_tasks[0].local_path.exists())
            self.assertEqual(_all_uploads(client), [])

            uploaded = upload_prepared_audio(client, result)

        self.assertEqual(uploaded, (result.upload_tasks[0].uri,))
        self.assertEqual(len(_all_uploads(client)), 1)

    def test_upload_prepared_audio_uses_transfer_manager_create_only(
        self,
    ) -> None:
        client = FakeStorageClient()
        with tempfile.TemporaryDirectory() as scratch_dir:
            flac_path = Path(scratch_dir) / "one.flac"
            mp3_path = Path(scratch_dir) / "two.mp3"
            flac_path.write_bytes(b"flac")
            mp3_path.write_bytes(b"mp3")
            result = AudioPreparationResult(
                segments=(),
                plans=(),
                uploaded_audio_uris=(),
                action_counts=dict.fromkeys(AUDIO_ACTIONS, 0),
                upload_tasks=(
                    AudioUploadTask(
                        uri="gs://bucket/audio/one.flac",
                        local_path=flac_path,
                        content_type="audio/flac",
                    ),
                    AudioUploadTask(
                        uri="gs://bucket/audio/two.mp3",
                        local_path=mp3_path,
                        content_type="audio/mpeg",
                    ),
                ),
            )

            uploaded = upload_prepared_audio(client, result)

        self.assertEqual(
            uploaded,
            ("gs://bucket/audio/one.flac", "gs://bucket/audio/two.mp3"),
        )
        self.assertEqual(len(self.upload_many_calls), 2)
        self.assertEqual(
            {
                call["upload_kwargs"]["content_type"]  # type: ignore[index]
                for call in self.upload_many_calls
            },
            {"audio/flac", "audio/mpeg"},
        )
        for call in self.upload_many_calls:
            self.assertEqual(
                call["upload_kwargs"]["if_generation_match"],
                0,  # type: ignore[index]
            )
            self.assertFalse(call["raise_exception"])
            self.assertEqual(call["worker_type"], "thread")
            self.assertEqual(call["max_workers"], AUDIO_UPLOAD_MAX_WORKERS)

    def test_upload_prepared_audio_fails_when_any_transfer_fails(self) -> None:
        client = FakeStorageClient()
        with tempfile.TemporaryDirectory() as scratch_dir:
            local_path = Path(scratch_dir) / "one.flac"
            local_path.write_bytes(b"flac")
            result = AudioPreparationResult(
                segments=(),
                plans=(),
                uploaded_audio_uris=(),
                action_counts=dict.fromkeys(AUDIO_ACTIONS, 0),
                upload_tasks=(
                    AudioUploadTask(
                        uri="gs://bucket/audio/one.flac",
                        local_path=local_path,
                        content_type="audio/flac",
                    ),
                ),
            )

            with patch(
                "dataset_split.audio.transfer_manager.upload_many",
                return_value=[RuntimeError("network down")],
            ):
                with self.assertRaisesRegex(
                    AudioDerivationError,
                    "failed to upload audio count=1 "
                    "first_uri=gs://bucket/audio/one.flac",
                ):
                    upload_prepared_audio(client, result)

    def test_upload_prepared_audio_fails_before_partial_upload_for_missing_file(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as scratch_dir:
            missing_path = Path(scratch_dir) / "does-not-exist.flac"
            result = AudioPreparationResult(
                segments=(),
                plans=(),
                uploaded_audio_uris=(),
                action_counts=dict.fromkeys(AUDIO_ACTIONS, 0),
                upload_tasks=(
                    AudioUploadTask(
                        uri="gs://bucket/audio/missing.flac",
                        local_path=missing_path,
                        content_type="audio/flac",
                    ),
                ),
            )

            with self.assertRaisesRegex(
                AudioDerivationError, "missing prepared audio file"
            ):
                upload_prepared_audio(FakeStorageClient(), result)

        self.assertEqual(self.upload_many_calls, [])

    def test_deferred_upload_requires_caller_owned_scratch_dir(self) -> None:
        with self.assertRaisesRegex(
            AudioDerivationError, "scratch_dir is required"
        ):
            prepare_audio_for_publication(
                FakeStorageClient(),
                layout=_layout(),
                segments=(),
                upload=False,
            )

    def test_generated_output_duration_uses_tight_tolerance(self) -> None:
        self.assertEqual(GENERATED_OUTPUT_DURATION_TOLERANCE_SECONDS, 0.05)
        client = FakeStorageClient()
        runner = FakeAudioRunner(
            (
                AudioProbe(30.0, "flac", 1, 16000, "flac"),
                AudioProbe(6.4, "flac", 1, 16000, "flac"),
            )
        )

        with tempfile.TemporaryDirectory() as scratch_dir:
            with self.assertRaisesRegex(
                AudioDerivationError, "generated audio duration mismatch"
            ):
                prepare_audio_for_publication(
                    client,
                    layout=_layout(),
                    segments=(
                        _segment(
                            "gs://source-bucket/audio/source.flac",
                            offset=2.0,
                            duration=6.0,
                        ),
                    ),
                    scratch_dir=scratch_dir,
                    runner=runner,
                )

        self.assertEqual(_all_uploads(client), [])

    def test_program_version_empty_stdout_is_unavailable(self) -> None:
        from dataset_split import audio as audio_module

        calls: list[dict[str, object]] = []

        def runner(
            args: list[str], **kwargs: object
        ) -> subprocess.CompletedProcess:
            calls.append({"args": args, "kwargs": kwargs})
            return subprocess.CompletedProcess(
                args=args, returncode=0, stdout="", stderr=""
            )

        version = audio_module._program_version("ffmpeg", runner=runner)

        self.assertEqual(version, "unavailable")
        self.assertEqual(
            calls[0]["kwargs"]["timeout"], PROGRAM_VERSION_TIMEOUT_SECONDS
        )

    def test_local_materialization_failure_happens_before_any_upload(
        self,
    ) -> None:
        client = FakeStorageClient()
        ffmpeg_calls = 0
        ffprobe_calls = 0

        def runner(
            args: list[str], **kwargs: object
        ) -> subprocess.CompletedProcess:
            nonlocal ffmpeg_calls, ffprobe_calls
            if args == ["ffprobe", "-version"]:
                return subprocess.CompletedProcess(
                    args=args,
                    returncode=0,
                    stdout="ffprobe version 7.0.2-test\n",
                    stderr="",
                )
            if args == ["ffmpeg", "-version"]:
                return subprocess.CompletedProcess(
                    args=args,
                    returncode=0,
                    stdout="ffmpeg version 7.0.2-test\n",
                    stderr="",
                )
            if args[0] == "ffmpeg":
                ffmpeg_calls += 1
                if ffmpeg_calls == 2:
                    raise subprocess.CalledProcessError(1, args)
                Path(args[-1]).write_bytes(b"generated audio")
                return subprocess.CompletedProcess(
                    args=args, returncode=0, stdout="", stderr=""
                )
            if args[0] == "ffprobe":
                ffprobe_calls += 1
                duration = "30.0" if ffprobe_calls <= 2 else "6.0"
                payload = {
                    "format": {"duration": duration, "format_name": "flac"},
                    "streams": [
                        {
                            "codec_name": "flac",
                            "channels": 1,
                            "sample_rate": "16000",
                        }
                    ],
                }
                return subprocess.CompletedProcess(
                    args=args,
                    returncode=0,
                    stdout=json.dumps(payload),
                    stderr="",
                )
            raise AssertionError(f"unexpected command: {args}")

        with tempfile.TemporaryDirectory() as scratch_dir:
            with self.assertRaisesRegex(
                AudioDerivationError, "failed to run ffmpeg"
            ):
                prepare_audio_for_publication(
                    client,
                    layout=_layout(),
                    segments=(
                        _segment(
                            "gs://source-bucket/audio/source-a.flac",
                            offset=1.0,
                            duration=6.0,
                            row_index=1,
                        ),
                        _segment(
                            "gs://source-bucket/audio/source-b.flac",
                            offset=1.0,
                            duration=6.0,
                            row_index=2,
                        ),
                    ),
                    scratch_dir=scratch_dir,
                    runner=runner,
                )

        self.assertEqual(_all_uploads(client), [])

    @patch("dataset_split.audio.requests.get")
    @patch(
        "dataset_split.audio.socket.getaddrinfo",
        return_value=[(None, None, None, None, ("93.184.216.34", 443))],
    )
    def test_external_url_staging_uses_streaming_download_controls(
        self, _mock_getaddrinfo: Any, mock_get: Any
    ) -> None:
        response = FakeResponse((b"abc", b"", b"def"))
        mock_get.return_value = response
        with tempfile.TemporaryDirectory() as scratch_dir:
            local_path = stage_source_audio(
                FakeStorageClient(),
                "https://archives.broadcastify.com/119/20260114/source.mp3",
                scratch_dir,
            )

        mock_get.assert_called_once_with(
            "https://archives.broadcastify.com/119/20260114/source.mp3",
            stream=True,
            timeout=EXTERNAL_DOWNLOAD_TIMEOUT,
            allow_redirects=False,
        )
        self.assertEqual(response.chunk_sizes, [EXTERNAL_DOWNLOAD_CHUNK_SIZE])
        self.assertEqual(Path(local_path).suffix, ".mp3")
        self.assertTrue(response.closed)

    @patch("dataset_split.audio.EXTERNAL_DOWNLOAD_MAX_BYTES", 3)
    @patch("dataset_split.audio.requests.get")
    @patch(
        "dataset_split.audio.socket.getaddrinfo",
        return_value=[(None, None, None, None, ("93.184.216.34", 443))],
    )
    def test_external_url_staging_enforces_max_bytes(
        self, _mock_getaddrinfo: Any, mock_get: Any
    ) -> None:
        self.assertEqual(EXTERNAL_DOWNLOAD_MAX_BYTES, 512 * 1024 * 1024)
        mock_get.return_value = FakeResponse((b"ab", b"cd"))

        with tempfile.TemporaryDirectory() as scratch_dir:
            with self.assertRaisesRegex(
                AudioDerivationError, "exceeded maximum"
            ):
                stage_source_audio(
                    FakeStorageClient(),
                    "https://archives.broadcastify.com/119/20260114/source.mp3",
                    scratch_dir,
                )

    def test_external_url_staging_rejects_plain_http(self) -> None:
        with tempfile.TemporaryDirectory() as scratch_dir:
            with self.assertRaisesRegex(AudioDerivationError, "must use https"):
                stage_source_audio(
                    FakeStorageClient(),
                    "http://archives.broadcastify.com/119/20260114/source.mp3",
                    scratch_dir,
                )

    def test_external_url_staging_rejects_unknown_hosts(self) -> None:
        with tempfile.TemporaryDirectory() as scratch_dir:
            with self.assertRaisesRegex(
                AudioDerivationError, "host is not allowed"
            ):
                stage_source_audio(
                    FakeStorageClient(),
                    "https://example.test/audio/source.mp3",
                    scratch_dir,
                )

    @patch(
        "dataset_split.audio.socket.getaddrinfo",
        return_value=[(None, None, None, None, ("127.0.0.1", 443))],
    )
    def test_external_url_staging_rejects_private_hosts(
        self, _mock_getaddrinfo: Any
    ) -> None:
        with tempfile.TemporaryDirectory() as scratch_dir:
            with self.assertRaisesRegex(
                AudioDerivationError, "must resolve only to global"
            ):
                stage_source_audio(
                    FakeStorageClient(),
                    "https://archives.broadcastify.com/119/20260114/source.mp3",
                    scratch_dir,
                )


if __name__ == "__main__":
    unittest.main()
