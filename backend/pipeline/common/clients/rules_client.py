import logging

import httpx

from backend.pipeline.common.clients.session_helper import authenticated_get
from backend.services.feeds.models import Tag

logger = logging.getLogger(__name__)


class RulesClient:
    """Client for interacting with the Rules Management API."""

    def __init__(self, base_url: str) -> None:
        if not base_url:
            msg = "Rules API base URL must be provided."
            raise ValueError(msg)
        self.base_url = base_url.rstrip("/")
        self.client = httpx.Client()

    def close(self) -> None:
        """Closes the underlying HTTP client session connection pool."""
        self.client.close()

    def get_rule_tags(self, rule_ids: list[str]) -> list[Tag] | None:
        """Fetches the de-duplicated union of tags for the given rule IDs.

        Args:
            rule_ids: IDs of the rules a transmission triggered.

        Returns:
            The combined tags across those rules (each key/value once, in
            first-seen order), an empty list when no rule IDs are given, or None
            if the API call fails.
        """
        if not rule_ids:
            return []

        url = f"{self.base_url}/v1/rules"

        try:
            response = authenticated_get(
                self.client, self.base_url, url, params={"rule_ids": rule_ids}
            )
        except httpx.HTTPError:
            logger.exception("Error fetching rules %s from rules API", rule_ids)
            return None

        try:
            rules = response.json()
            seen: set[tuple[str, str]] = set()
            tags: list[Tag] = []
            for rule in rules:
                for tag in rule.get("tags") or []:
                    identity = (tag["key"], tag["value"])
                    if identity not in seen:
                        seen.add(identity)
                        tags.append(Tag(**tag))
        except (ValueError, TypeError, KeyError):
            logger.exception(
                "Error parsing response from rules API for rules %s", rule_ids
            )
            return None
        else:
            return tags
