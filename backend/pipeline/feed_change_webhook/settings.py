"""Runtime configuration for the Feed Change Notification webhook relay."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import TYPE_CHECKING
from urllib.parse import urlparse

if TYPE_CHECKING:
    from collections.abc import Mapping

WEBHOOK_URL_ENV = "FEED_CHANGE_WEBHOOK_URL"


class SettingsError(ValueError):
    """Raised when relay runtime configuration is invalid."""


@dataclass(frozen=True)
class FeedChangeWebhookSettings:
    webhook_url: str
    webhook_api_key: str = field(repr=False)


def load_settings(
    env: Mapping[str, str] | None = None,
) -> FeedChangeWebhookSettings:
    source = os.environ if env is None else env
    webhook_url = _require_env("FEED_CHANGE_WEBHOOK_URL", env=source)
    api_key = _require_env("FEED_CHANGE_WEBHOOK_API_KEY", env=source)

    parsed = urlparse(webhook_url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        msg = f"{WEBHOOK_URL_ENV} must be an absolute HTTP(S) URL"
        raise SettingsError(msg)

    return FeedChangeWebhookSettings(
        webhook_url=webhook_url,
        webhook_api_key=api_key,
    )


def _require_env(name: str, *, env: Mapping[str, str] | None = None) -> str:
    source = os.environ if env is None else env
    value = source.get(name)
    if value is None or not value.strip():
        msg = f"{name} is required"
        raise SettingsError(msg)
    return value.strip()
