import logging
import os
from unittest.mock import MagicMock, patch

import pytest
from google.cloud import storage

from backend.pipeline.ingestion.collectors.echo import main as echo_main
from integration_tests.feed_utils import (
    create_test_bcfy_feed,  # noqa: F401
    create_test_echo_feed,  # noqa: F401
    create_test_polling_feed,  # noqa: F401
)
from integration_tests.test_utils import (
    verify_transcript_in_db,
)

logger = logging.getLogger(__name__)


def test_ingestion_integration(test_bcfy_feed: tuple[str, str]) -> None:
    """Tests that audio ingestion service picks up the test feed and results in a transcript."""
    feed_id, _ = test_bcfy_feed
    verify_transcript_in_db(feed_id)


def test_ingestion_api_polling(test_polling_feed: tuple[str, str]) -> None:
    """Tests that audio ingestion service picks up a feed from API polling and results in a transcript."""
    feed_id, _ = test_polling_feed
    verify_transcript_in_db(feed_id)


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
    date_str = "20260326"
    time_str = "143022"
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

    verify_transcript_in_db(feed_id)
