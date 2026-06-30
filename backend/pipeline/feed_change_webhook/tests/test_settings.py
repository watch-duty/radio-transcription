from __future__ import annotations

import pytest

from backend.pipeline.feed_change_webhook import settings


def test_load_settings_builds_webhook_url() -> None:
    loaded = settings.load_settings(
        {
            "WD_BACKEND_BASE_URL": " https://backend.watchduty.org ",
            "WD_BACKEND_API_KEY": " secret-value ",
        }
    )

    assert loaded.wd_backend_base_url == "https://backend.watchduty.org"
    assert loaded.wd_backend_api_key == "secret-value"
    assert (
        loaded.wd_feed_change_webhook_url == "https://backend.watchduty.org"
        "/api/v1/echo/radio_transcription/internal/audit/webhook/"
    )


@pytest.mark.parametrize(
    ("env", "expected_message"),
    [
        ({}, "WD_BACKEND_BASE_URL is required"),
        (
            {"WD_BACKEND_BASE_URL": "", "WD_BACKEND_API_KEY": "key"},
            "WD_BACKEND_BASE_URL is required",
        ),
        (
            {
                "WD_BACKEND_BASE_URL": "watchduty.local",
                "WD_BACKEND_API_KEY": "key",
            },
            "WD_BACKEND_BASE_URL must be an absolute HTTP(S) URL",
        ),
        (
            {
                "WD_BACKEND_BASE_URL": "https://watchduty.local/",
                "WD_BACKEND_API_KEY": "key",
            },
            "WD_BACKEND_BASE_URL must not end with a slash",
        ),
        (
            {"WD_BACKEND_BASE_URL": "https://watchduty.local"},
            "WD_BACKEND_API_KEY is required",
        ),
        (
            {
                "WD_BACKEND_BASE_URL": "https://watchduty.local",
                "WD_BACKEND_API_KEY": " ",
            },
            "WD_BACKEND_API_KEY is required",
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
