from __future__ import annotations

import logging
import os
from unittest import mock

import pytest

from backend.pipeline.common import actor_identity


def setup_function() -> None:
    actor_identity._reset_runtime_service_actor_cache_for_tests()


def teardown_function() -> None:
    actor_identity._reset_runtime_service_actor_cache_for_tests()


def test_google_user_actor_from_sub() -> None:
    actor_id = actor_identity.actor_id_from_google_sub(" admin-sub-123 ")

    assert actor_id == "user:google:admin-sub-123"


@pytest.mark.parametrize("sub", ["", "   ", "admin sub", "admin\nsub"])
def test_google_user_actor_rejects_blank_or_whitespace_sub(sub: str) -> None:
    with pytest.raises(ValueError, match="Google user sub"):
        actor_identity.actor_id_from_google_sub(sub)


def test_google_user_actor_rejects_too_long_sub() -> None:
    prefix_len = len(actor_identity.GOOGLE_USER_ACTOR_PREFIX)
    sub = "x" * (actor_identity.MAX_ACTOR_ID_LENGTH - prefix_len + 1)

    with pytest.raises(ValueError, match="too long"):
        actor_identity.actor_id_from_google_sub(sub)


@pytest.mark.parametrize(
    ("actor_id", "expected"),
    [
        ("user:google:admin-sub-123", True),
        ("user:google:", False),
        ("user:google:admin sub", False),
        ("user:google:" + ("x" * 502), False),
        ("service_account:gcp:1234567890", False),
        ("service:collector-runtime", False),
    ],
)
def test_google_user_actor_validator(actor_id: str, expected) -> None:
    assert (
        actor_identity.is_well_formed_google_user_actor_id(actor_id) is expected
    )


@pytest.mark.parametrize(
    ("actor_id", "expected"),
    [
        ("service_account:gcp:1234567890", True),
        ("service_account:gcp:unresolved", True),
        ("service_account:local:development", True),
        ("user:google:admin-sub-123", True),
        ("", False),
        ("   ", False),
        ("service_account:gcp:bad value", False),
        ("service_account:gcp:bad\nvalue", False),
        (None, False),
        ("x" * (actor_identity.MAX_ACTOR_ID_LENGTH + 1), False),
    ],
)
def test_generic_actor_id_validator(
    actor_id: str | None,
    expected: bool,
) -> None:
    assert actor_identity.is_well_formed_actor_id(actor_id) is expected


def test_runtime_service_actor_uses_local_fallback_outside_gcp() -> None:
    with (
        mock.patch.object(actor_identity, "is_gcp_env", return_value=False),
        mock.patch.dict(os.environ, {}, clear=True),
    ):
        actor_id = actor_identity.runtime_service_actor_id()

    assert actor_id == "service_account:local:development"


def test_runtime_service_actor_uses_local_fallback_with_bad_env_outside_gcp(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with (
        caplog.at_level(logging.ERROR),
        mock.patch.object(actor_identity, "is_gcp_env", return_value=False),
        mock.patch.dict(
            os.environ,
            {actor_identity.CONFIGURED_SERVICE_ACTOR_ENV: "bad value"},
            clear=True,
        ),
    ):
        actor_id = actor_identity.runtime_service_actor_id()

    assert actor_id == "service_account:local:development"
    assert "feed_audit_actor_unresolved" not in caplog.text


def test_runtime_service_actor_uses_configured_gcp_actor() -> None:
    configured_actor_id = "service_account:gcp:1234567890"

    with (
        mock.patch.object(actor_identity, "is_gcp_env", return_value=True),
        mock.patch.dict(
            os.environ,
            {actor_identity.CONFIGURED_SERVICE_ACTOR_ENV: configured_actor_id},
            clear=True,
        ),
    ):
        actor_id = actor_identity.runtime_service_actor_id()
        repeated_actor_id = actor_identity.runtime_service_actor_id()

    assert actor_id == configured_actor_id
    assert repeated_actor_id == configured_actor_id


def test_runtime_service_actor_returns_unresolved_when_gcp_config_missing(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with (
        caplog.at_level(logging.ERROR),
        mock.patch.object(actor_identity, "is_gcp_env", return_value=True),
        mock.patch.dict(os.environ, {}, clear=True),
    ):
        actor_id = actor_identity.runtime_service_actor_id()
        repeated_actor_id = actor_identity.runtime_service_actor_id()

    assert actor_id == "service_account:gcp:unresolved"
    assert repeated_actor_id == "service_account:gcp:unresolved"
    unresolved_records = [
        record
        for record in caplog.records
        if record.message == "feed_audit_actor_unresolved"
    ]
    assert len(unresolved_records) == 1
    assert unresolved_records[0].event == "feed_audit_actor_unresolved"
    assert unresolved_records[0].gcp_runtime_detected is True
    assert unresolved_records[0].reason == "missing"
    assert (
        unresolved_records[0].fallback_actor
        == "service_account:gcp:unresolved"
    )


def test_runtime_service_actor_returns_unresolved_when_gcp_config_malformed(
    caplog: pytest.LogCaptureFixture,
) -> None:
    bad_actor_id = "service_account:gcp:bad value"

    with (
        caplog.at_level(logging.ERROR),
        mock.patch.object(actor_identity, "is_gcp_env", return_value=True),
        mock.patch.dict(
            os.environ,
            {actor_identity.CONFIGURED_SERVICE_ACTOR_ENV: bad_actor_id},
            clear=True,
        ),
    ):
        actor_id = actor_identity.runtime_service_actor_id()
        repeated_actor_id = actor_identity.runtime_service_actor_id()

    assert actor_id == "service_account:gcp:unresolved"
    assert repeated_actor_id == "service_account:gcp:unresolved"
    assert bad_actor_id not in caplog.text
    unresolved_records = [
        record
        for record in caplog.records
        if record.message == "feed_audit_actor_unresolved"
    ]
    assert len(unresolved_records) == 1
    assert unresolved_records[0].reason == "malformed"
    assert (
        unresolved_records[0].fallback_actor
        == "service_account:gcp:unresolved"
    )
