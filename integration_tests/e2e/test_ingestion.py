import logging

from integration_tests.feed_utils import create_test_feed  # noqa: F401
from integration_tests.test_utils import (
    verify_transcript_in_db,
)

logger = logging.getLogger(__name__)


def test_ingestion_integration(test_feed: tuple[str, str]) -> None:
    """Tests that audio ingestion service picks up seeded feed and results in a transcript."""
    feed_id, _ = test_feed
    verify_transcript_in_db(feed_id)
