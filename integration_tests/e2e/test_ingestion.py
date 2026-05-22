import logging

from integration_tests.feed_utils import (
    create_test_bcfy_feed,  # noqa: F401
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
