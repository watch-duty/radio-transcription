import logging
import time

import requests

from backend.pipeline.common import auth_client, env
from backend.pipeline.common.clients.session_helper import (
    create_resilient_session,
)
from backend.pipeline.common.tracing_utils import get_current_traceparent
from backend.services.feeds.models import Tag

logger = logging.getLogger(__name__)


class FeedsClient:
    """Client for interacting with the Feeds API."""

    def __init__(
        self,
        base_url: str,
        *,
        cache_ttl_seconds: float | None = 600.0,
        cache_max_size: int = 1000,
    ) -> None:
        if not base_url:
            msg = "Feeds API base URL must be provided."
            raise ValueError(msg)
        self.base_url = base_url.rstrip("/")
        self.session = create_resilient_session()
        self._cache_ttl_seconds = cache_ttl_seconds
        self._cache_max_size = cache_max_size
        # Cache format: {feed_id: (expiry_timestamp, tags_list)}
        self._cache: dict[str, tuple[float, list[Tag]]] = {}

    def get_feed_tags(self, feed_id: str) -> list[Tag] | None:
        """Fetches tags for a given feed_id from the feeds API, using cache if available.

        Args:
            feed_id: The ID of the feed.

        Returns:
            A list of Tag objects if successful, otherwise None.
        """
        now = time.time()
        cache_enabled = (
            self._cache_ttl_seconds is not None and self._cache_ttl_seconds > 0
        )

        if cache_enabled:
            if feed_id in self._cache:
                expiry, tags = self._cache[feed_id]
                if now < expiry:
                    logger.info("Returning cached tags for feed %s", feed_id)
                    return list(tags)
                # Evict expired entry
                del self._cache[feed_id]

        try:
            tags = self._fetch_feed_tags(feed_id)
        except requests.exceptions.RequestException:
            logger.warning("Failed to fetch tags for feed %s", feed_id)
            return None
        except (ValueError, TypeError):
            logger.warning("Failed to parse tags for feed %s", feed_id)
            return None

        if cache_enabled:
            if len(self._cache) >= self._cache_max_size:
                # Evict the oldest/first inserted key (FIFO-ish via dict insertion order)
                oldest_key = next(iter(self._cache))
                del self._cache[oldest_key]
            self._cache[feed_id] = (now + self._cache_ttl_seconds, tags)

        return list(tags)

    def _fetch_feed_tags(self, feed_id: str) -> list[Tag]:
        url = f"{self.base_url}/v1/feeds/{feed_id}"
        headers = {}

        traceparent = get_current_traceparent()
        if traceparent:
            headers["traceparent"] = traceparent

        if env.is_gcp_env():
            token = auth_client.get_id_token(self.base_url)
            headers["Authorization"] = f"Bearer {token}"

        try:
            response = self.session.get(url, headers=headers, timeout=5)
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                logger.warning("Feed %s not found in feeds API", feed_id)
                return []
            logger.exception(
                "HTTP error fetching feed %s from feeds API", feed_id
            )
            raise
        except requests.exceptions.RequestException:
            logger.exception("Error fetching feed %s from feeds API", feed_id)
            raise

        try:
            data = response.json()
            tags_data = data.get("tags") or []
            return [Tag(**t) for t in tags_data]
        except (ValueError, TypeError):
            logger.exception(
                "Error parsing response from feeds API for feed %s", feed_id
            )
            raise
