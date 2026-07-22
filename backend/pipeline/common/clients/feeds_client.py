import logging

import httpx

from backend.pipeline.common.clients.session_helper import authenticated_get
from backend.services.feeds.models import Tag

logger = logging.getLogger(__name__)


class FeedsClient:
    """Client for interacting with the Feeds API."""

    def __init__(self, base_url: str) -> None:
        if not base_url:
            msg = "Feeds API base URL must be provided."
            raise ValueError(msg)
        self.base_url = base_url.rstrip("/")
        self.client = httpx.Client()

    def close(self) -> None:
        """Closes the underlying HTTP client session connection pool."""
        self.client.close()

    def get_feed_tags(self, feed_id: str) -> list[Tag] | None:
        """Fetches tags for a given feed_id from the feeds API.

        Args:
            feed_id: The ID of the feed.

        Returns:
            A list of Tag objects if successful, otherwise None.
        """
        url = f"{self.base_url}/v1/feeds/{feed_id}"

        try:
            response = authenticated_get(self.client, self.base_url, url)
        except httpx.HTTPError:
            logger.exception("Error fetching feed %s from feeds API", feed_id)
            return None

        try:
            data = response.json()
            tags_data = data.get("tags") or []
            return [Tag(**t) for t in tags_data]
        except (ValueError, TypeError):
            logger.exception(
                "Error parsing response from feeds API for feed %s", feed_id
            )
            return None
