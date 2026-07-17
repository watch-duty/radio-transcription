import logging

import httpx
from tenacity import Retrying

from backend.pipeline.common import auth_client, env
from backend.pipeline.common.clients.session_helper import (
    get_httpx_retry_config,
)
from backend.pipeline.common.tracing_utils import get_current_traceparent
from backend.services.feeds.models import Tag

logger = logging.getLogger(__name__)


class FeedsClient:
    """Client for interacting with the Feeds API."""

    def __init__(self, base_url: str) -> None:
        if not base_url:
            msg = "Feeds API base URL must be provided."
            raise ValueError(msg)
        self.base_url = base_url.rstrip("/")
        self.client = httpx.Client(http2=True)

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
        headers = {}

        traceparent = get_current_traceparent()
        if traceparent:
            headers["traceparent"] = traceparent

        if env.is_gcp_env():
            token = auth_client.get_id_token(self.base_url)
            headers["Authorization"] = f"Bearer {token}"

        try:
            for attempt in Retrying(
                **get_httpx_retry_config(
                    total_attempts=4,
                    multiplier=0.5,
                    min_seconds=0.5,
                    max_seconds=2.0,
                )
            ):
                with attempt:
                    response = self.client.get(url, headers=headers, timeout=5)
                    response.raise_for_status()
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
