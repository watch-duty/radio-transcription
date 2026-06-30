"""Runtime configuration for the Feed Change Notification webhook relay."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import TYPE_CHECKING
from urllib.parse import urlparse

if TYPE_CHECKING:
    from collections.abc import Mapping

WD_BACKEND_BASE_URL_ENV = "WD_BACKEND_BASE_URL"
WD_BACKEND_API_KEY_ENV = "WD_BACKEND_API_KEY"
WD_FEED_CHANGE_WEBHOOK_PATH = (
    "/api/v1/echo/radio_transcription/internal/audit/webhook/"
)


class SettingsError(ValueError):
    """Raised when relay runtime configuration is invalid."""


@dataclass(frozen=True)
class FeedChangeWebhookSettings:
    wd_backend_base_url: str
    wd_backend_api_key: str = field(repr=False)

    @property
    def wd_feed_change_webhook_url(self) -> str:
        return f"{self.wd_backend_base_url}{WD_FEED_CHANGE_WEBHOOK_PATH}"


def load_settings(
    env: Mapping[str, str] | None = None,
) -> FeedChangeWebhookSettings:
    source = os.environ if env is None else env
    base_url = _required_env_value(source, WD_BACKEND_BASE_URL_ENV)
    api_key = _required_env_value(source, WD_BACKEND_API_KEY_ENV)

    if base_url.endswith("/"):
        msg = f"{WD_BACKEND_BASE_URL_ENV} must not end with a slash"
        raise SettingsError(msg)

    parsed = urlparse(base_url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        msg = f"{WD_BACKEND_BASE_URL_ENV} must be an absolute HTTP(S) URL"
        raise SettingsError(msg)

    return FeedChangeWebhookSettings(
        wd_backend_base_url=base_url,
        wd_backend_api_key=api_key,
    )


def _required_env_value(env: Mapping[str, str], name: str) -> str:
    value = env.get(name)
    if value is None or not value.strip():
        msg = f"{name} is required"
        raise SettingsError(msg)
    return value.strip()
