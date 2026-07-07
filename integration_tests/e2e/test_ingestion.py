import os
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest
from google.cloud import storage

from backend.pipeline.ingestion.collectors.echo import main as echo_main
from integration_tests.feed_utils import (
    create_test_bcfy_feed,  # noqa: F401
    create_test_echo_feed,  # noqa: F401
    create_test_fire_notifications_feed,  # noqa: F401
    create_test_polling_feed,  # noqa: F401
)
from integration_tests.test_utils import (
    verify_audio_segments_via_api,
    verify_multiple_audio_segments_via_api,
)


def test_ingestion_integration(test_bcfy_feed: tuple[str, str]) -> None:
    """Tests that audio ingestion service picks up the test feed and results in a transcript."""
    feed_id, _ = test_bcfy_feed

    # Broadcastify feeds are continuous audio streams, so they should NOT have an external ID
    verify_audio_segments_via_api(
        feed_id, lambda s: s.get("external_audio_segment_id") is None
    )


def test_ingestion_api_polling(test_polling_feed: tuple[str, str]) -> None:
    """Tests that audio ingestion service picks up a feed from API polling and results in a transcript."""
    feed_id, _ = test_polling_feed

    # Broadcastify calls should have an external ID representing the full audio URL.
    # We verify that at least 2 segments are generated, proving that the collector's
    # single connection_session_id does not cause segment_id collisions.
    verify_multiple_audio_segments_via_api(
        feed_id,
        lambda s: (
            isinstance(ext_id := s.get("external_audio_segment_id"), str)
            and ext_id.startswith(
                "http://mock-audio-server:8090/broadcastify_calls/2912/"
            )
        ),
        min_count=2,
    )


def test_ingestion_echo(test_echo_feed: tuple[str, str]) -> None:
    """Tests that audio ingestion service picks up a feed from Echo and results in a transcript."""
    feed_id, source_feed_id = test_echo_feed

    # Read in test audio file
    audio_bytes = b"dummy mp3 audio"
    try:
        with open(
            "/app/backend/pipeline/segmentation/tests/test_data/test_bcfy.flac",
            "rb",
        ) as f:
            audio_bytes = f.read()
    except Exception:
        pytest.fail(
            "Echo Ingestion Setup Failure: Unable to find test audio file"
        )

    channel_name = source_feed_id
    now = datetime.now(UTC)
    date_str = now.strftime("%Y%m%d")
    time_str = now.strftime("%H%M%S")
    filename = f"{channel_name}_{date_str}_{time_str}.mp3"
    gcs_path = f"{channel_name}/{date_str}/{filename}"

    gcs_client = storage.Client()
    source_bucket_name = os.environ.get(
        "ECHO_RECORDINGS_BUCKET", "echo-recordings-test"
    )
    blob = gcs_client.bucket(source_bucket_name).blob(gcs_path)
    blob.upload_from_string(audio_bytes, content_type="audio/mpeg")

    event = MagicMock()
    event.data = {"name": gcs_path, "bucket": source_bucket_name}

    # Mock this dependency call so we don't have to install it.
    with patch.object(echo_main, "get_audio_duration", return_value=15000):
        # Simulate new audio available event trigger on Echo
        echo_main.handle_notification(event)

    # Verify external ID propagation
    verify_audio_segments_via_api(
        feed_id,
        lambda s: (
            s.get("external_audio_segment_id")
            == f"{source_bucket_name}/{gcs_path}"
        ),
    )


def test_ingestion_fire_notifications_timestamp_override(
    test_fire_notifications_feed: tuple[str, str],
) -> None:
    """Tests that audio ingestion service picks up a feed from Fire Notifications and results in a transcript with correct timezone."""
    feed_id, _ = test_fire_notifications_feed

    # The mock file is "SAN-JOSE-DISP 2026-06-15 17-45-43.mp3".
    # Timezone is set to America/Los_Angeles (PDT in June is UTC-7).
    # Local time: 2026-06-15 17:45:43
    # Expected UTC time: 2026-06-16 00:45:43 UTC.
    expected_start_utc = "2026-06-16T00:45:43"

    def match_segment(segment: dict) -> bool:
        start_time_str = segment.get("start_timestamp")
        if not start_time_str:
            return False
        return start_time_str.startswith(expected_start_utc)

    verify_audio_segments_via_api(
        feed_id,
        match_segment,
    )
