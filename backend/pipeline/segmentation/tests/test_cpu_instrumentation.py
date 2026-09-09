"""Tests for segmentation CPU metric boundaries."""

import concurrent.futures
import datetime
import threading
from contextlib import nullcontext
from unittest.mock import MagicMock, patch

import numpy as np
from opentelemetry import context as otel_context

from backend.pipeline.segmentation.datatypes import BufferedChunk
from backend.pipeline.segmentation.transforms.stateless import (
    UploadRawSegmentFn,
)


@patch(
    "backend.pipeline.segmentation.transforms.stateless."
    "cpu_time.read_thread_cpu_ns",
    side_effect=[1_000, 5_000, 10_000, 18_000],
)
def test_stitch_and_encode_have_separate_cpu_scopes(
    _mock_thread_cpu: MagicMock,
) -> None:
    """Accounts for PCM stitching separately from FLAC encoding."""
    fn = UploadRawSegmentFn("staging-bucket", "project")
    fn.post_flush_stitch_thread_cpu_us = MagicMock()
    fn.post_flush_encode_thread_cpu_us = MagicMock()
    fn.stitch_latency_ms = MagicMock()

    chunk = BufferedChunk(gcs_uri="gs://bucket/chunk.flac", timestamp_ms=0)
    request = MagicMock()
    request.time_range.start_ms = 0
    request.time_range.end_ms = 100
    request.contributing_chunks = [chunk]
    request.sample_rate = 16_000
    decoded_chunks = {
        chunk.gcs_uri: (np.zeros(1_600, dtype=np.int16), 16_000, 0)
    }

    with patch.object(fn, "_pcm_to_flac", return_value=b"flac"):
        result = fn._stitch_audio_segments(
            request,
            decoded_chunks,
            MagicMock(),
        )

    assert result == b"flac"
    fn.post_flush_stitch_thread_cpu_us.update.assert_called_once_with(4)
    fn.post_flush_encode_thread_cpu_us.update.assert_called_once_with(8)


@patch(
    "backend.pipeline.segmentation.transforms.stateless."
    "cpu_time.read_thread_cpu_ns",
    side_effect=[2_000, 9_000],
)
def test_upload_has_its_own_cpu_scope(
    _mock_thread_cpu: MagicMock,
) -> None:
    """Accounts for GCS upload client work without wall-clock wait time."""
    fn = UploadRawSegmentFn("staging-bucket", "project")
    fn.gcs_client = MagicMock()
    fn.post_flush_upload_thread_cpu_us = MagicMock()
    fn.stitched_segments_uploaded = MagicMock()
    fn.upload_latency_ms = MagicMock()
    request = MagicMock(feed_id="feed", segment_id="segment")
    start_datetime = datetime.datetime(
        2026,
        8,
        7,
        tzinfo=datetime.UTC,
    )

    uri = fn._upload_stitched_audio(
        request,
        b"flac",
        start_datetime,
        MagicMock(),
    )

    assert uri.endswith("/segment.flac")
    fn.post_flush_upload_thread_cpu_us.update.assert_called_once_with(7)


@patch(
    "backend.pipeline.segmentation.transforms.stateless.sf.read",
    return_value=(np.zeros(1_600, dtype=np.int16), 16_000),
)
def test_download_cpu_is_reported_from_processing_thread(
    _mock_read: MagicMock,
) -> None:
    """Moves executor measurements back to the Beam processing thread."""
    fn = UploadRawSegmentFn("staging-bucket", "project")
    fn.gcs_client = MagicMock()
    fn.gcs_chunks_downloaded = MagicMock()
    fn.download_latency_ms = MagicMock()
    update_threads: list[str] = []
    fn.post_flush_download_decode_thread_cpu_us = MagicMock()
    fn.post_flush_download_decode_thread_cpu_us.update.side_effect = (
        lambda _value: update_threads.append(threading.current_thread().name)
    )
    request = MagicMock()
    request.contributing_chunks = [
        BufferedChunk(gcs_uri="gs://bucket/one.flac", timestamp_ms=0),
        BufferedChunk(gcs_uri="gs://bucket/two.flac", timestamp_ms=1_000),
    ]
    main_thread_name = threading.current_thread().name

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        fn._executor = executor
        decoded = fn._download_contributing_chunks(
            request,
            MagicMock(),
            otel_context.get_current(),
        )

    assert set(decoded) == {
        "gs://bucket/one.flac",
        "gs://bucket/two.flac",
    }
    assert update_threads == [main_thread_name, main_thread_name]


@patch(
    "backend.pipeline.segmentation.transforms.stateless."
    "cpu_time.read_thread_cpu_ns",
    side_effect=[3_000, 14_000],
)
@patch("backend.pipeline.segmentation.transforms.stateless.inject_otel_context")
@patch(
    "backend.pipeline.segmentation.transforms.stateless.with_tracer_context",
    return_value=nullcontext(),
)
def test_message_construction_has_its_own_cpu_scope(
    _mock_trace_context: MagicMock,
    _mock_inject: MagicMock,
    _mock_thread_cpu: MagicMock,
) -> None:
    """Accounts for protobuf creation and serialization before yielding."""
    fn = UploadRawSegmentFn("staging-bucket", "project")
    proto = MagicMock()
    proto.SerializeToString.return_value = b"serialized"
    fn.post_flush_message_thread_cpu_us = MagicMock()
    fn.segmentation_success = MagicMock()
    fn.segmentation_error = MagicMock()
    request = MagicMock(
        feed_id="feed",
        session_id="session",
        traceparent=None,
        baggage=None,
    )
    request.time_range.start_ms = 0

    with (
        patch.object(
            fn,
            "_upload_raw_audio",
            return_value="gs://bucket/segment.flac",
        ),
        patch.object(
            fn,
            "_build_segmented_audio_proto",
            return_value=proto,
        ),
    ):
        output = list(fn.process(("feed", (1, request))))

    assert output[0][1][1]["data"] == b"serialized"
    fn.post_flush_message_thread_cpu_us.update.assert_called_once_with(11)
