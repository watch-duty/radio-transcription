from __future__ import annotations

import pytest

from backend.pipeline.feed_change_webhook import settings


def test_load_settings_builds_webhook_url() -> None:
    loaded = settings.load_settings(
        {
            "FEED_CHANGE_WEBHOOK_URL": (
                " https://webhook.example.test/feed-change/ "
            ),
            "FEED_CHANGE_WEBHOOK_API_KEY": " secret-value ",
        }
    )

    assert loaded.webhook_url == "https://webhook.example.test/feed-change/"
    assert loaded.webhook_api_key == "secret-value"


@pytest.mark.parametrize(
    ("env", "expected_message"),
    [
        ({}, "FEED_CHANGE_WEBHOOK_URL is required"),
        (
            {
                "FEED_CHANGE_WEBHOOK_URL": "",
                "FEED_CHANGE_WEBHOOK_API_KEY": "key",
            },
            "FEED_CHANGE_WEBHOOK_URL is required",
        ),
        (
            {
                "FEED_CHANGE_WEBHOOK_URL": "webhook.local/feed-change",
                "FEED_CHANGE_WEBHOOK_API_KEY": "key",
            },
            "FEED_CHANGE_WEBHOOK_URL must be an absolute HTTP(S) URL",
        ),
        (
            {
                "FEED_CHANGE_WEBHOOK_URL": "https://webhook.example.test/feed-change"
            },
            "FEED_CHANGE_WEBHOOK_API_KEY is required",
        ),
        (
            {
                "FEED_CHANGE_WEBHOOK_URL": "https://webhook.example.test/feed-change",
                "FEED_CHANGE_WEBHOOK_API_KEY": " ",
            },
            "FEED_CHANGE_WEBHOOK_API_KEY is required",
        ),
    ],
)
def test_load_settings_rejects_invalid_config(
    env: dict[str, str],
    expected_message: str,
) -> None:
    with pytest.raises(settings.SettingsError) as exc_info:
        settings.load_settings(env)
    assert str(exc_info.value) == expected_message
