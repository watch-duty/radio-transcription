from __future__ import annotations

import logging
import os
from typing import Any, cast
from unittest import mock

import pytest

from backend.pipeline.common import actor_identity


def test_google_user_actor_from_email() -> None:
    actor_id = actor_identity.actor_id_from_google_email(" Admin@Example.com ")

    assert actor_id == "user:google:admin@example.com"


@pytest.mark.parametrize(
    "email",
    ["", "   ", "admin @example.com", "admin\n@example.com"],
)
def test_google_user_actor_rejects_blank_or_whitespace_email(
    email: str,
) -> None:
    with pytest.raises(ValueError, match="Google user email"):
        actor_identity.actor_id_from_google_email(email)


def test_google_user_actor_rejects_too_long_email() -> None:
    prefix_len = len(actor_identity.GOOGLE_USER_ACTOR_PREFIX)
    email = "x" * (actor_identity.MAX_ACTOR_ID_LENGTH - prefix_len + 1)

    with pytest.raises(ValueError, match="too long"):
        actor_identity.actor_id_from_google_email(email)


@pytest.mark.parametrize(
    ("actor_id", "expected"),
    [
        ("user:google:admin@example.com", True),
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
        (
            "service_account:gcp:feeds-service@example.iam.gserviceaccount.com",
            True,
        ),
        (
            "service_account:gcp:"
            "feeds-service@example.iam.gserviceaccount.com?uid=1234567890",
            True,
        ),
        ("service_account:gcp:unresolved", True),
        ("service_account:gcp:", False),
        ("service_account:gcp:feeds-service", True),
        ("service_account:gcp:bad value", False),
        ("service_account:gcp:bad\nvalue", False),
        ("service_account:gcp:１２３", True),
        ("service_account:gcp:²³", True),
        ("service_account:local:development", False),
        ("user:google:admin@example.com", False),
        ("service_account:gcp:" + ("1" * 493), False),
    ],
)
def test_gcp_service_account_actor_validator(
    actor_id: str,
    expected,
) -> None:
    assert (
        actor_identity.is_well_formed_gcp_service_account_actor_id(actor_id)
        is expected
    )


def test_runtime_service_actor_uses_local_fallback_outside_gcp() -> None:
    with (
        mock.patch.object(actor_identity, "is_gcp_env", return_value=False),
        mock.patch.dict(os.environ, {}, clear=True),
    ):
        actor_id = actor_identity.resolve_runtime_service_actor_id()

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
        actor_id = actor_identity.resolve_runtime_service_actor_id()

    assert actor_id == "service_account:local:development"
    assert "feed_audit_actor_unresolved" not in caplog.text


@pytest.mark.parametrize(
    "configured_actor_id",
    [
        "service_account:gcp:1234567890",
        "service_account:gcp:feeds-service@example.iam.gserviceaccount.com",
        (
            "service_account:gcp:"
            "feeds-service@example.iam.gserviceaccount.com?uid=1234567890"
        ),
    ],
)
def test_runtime_service_actor_uses_configured_gcp_actor(
    configured_actor_id: str,
) -> None:
    with (
        mock.patch.object(actor_identity, "is_gcp_env", return_value=True),
        mock.patch.dict(
            os.environ,
            {actor_identity.CONFIGURED_SERVICE_ACTOR_ENV: configured_actor_id},
            clear=True,
        ),
    ):
        actor_id = actor_identity.resolve_runtime_service_actor_id()
        repeated_actor_id = actor_identity.resolve_runtime_service_actor_id()

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
        actor_id = actor_identity.resolve_runtime_service_actor_id()

    assert actor_id == "service_account:gcp:unresolved"
    unresolved_records = [
        record
        for record in caplog.records
        if record.message == "feed_audit_actor_unresolved"
    ]
    assert len(unresolved_records) == 1
    record = cast("Any", unresolved_records[0])
    assert record.event == "feed_audit_actor_unresolved"
    assert record.gcp_runtime_detected is True
    assert record.reason == "missing"
    assert record.fallback_actor == "service_account:gcp:unresolved"


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
        actor_id = actor_identity.resolve_runtime_service_actor_id()

    assert actor_id == "service_account:gcp:unresolved"
    assert bad_actor_id not in caplog.text
    unresolved_records = [
        record
        for record in caplog.records
        if record.message == "feed_audit_actor_unresolved"
    ]
    assert len(unresolved_records) == 1
    record = cast("Any", unresolved_records[0])
    assert record.reason == "malformed"
    assert record.fallback_actor == "service_account:gcp:unresolved"


def test_runtime_service_actor_returns_unresolved_when_gcp_config_has_whitespace(
    caplog: pytest.LogCaptureFixture,
) -> None:
    malformed_actor_id = (
        "service_account:gcp:"
        "feeds-service@example.iam.gserviceaccount.com?uid=bad value"
    )

    with (
        caplog.at_level(logging.ERROR),
        mock.patch.object(actor_identity, "is_gcp_env", return_value=True),
        mock.patch.dict(
            os.environ,
            {actor_identity.CONFIGURED_SERVICE_ACTOR_ENV: malformed_actor_id},
            clear=True,
        ),
    ):
        actor_id = actor_identity.resolve_runtime_service_actor_id()

    assert actor_id == "service_account:gcp:unresolved"
    assert malformed_actor_id not in caplog.text
    unresolved_records = [
        record
        for record in caplog.records
        if record.message == "feed_audit_actor_unresolved"
    ]
    assert len(unresolved_records) == 1
    record = cast("Any", unresolved_records[0])
    assert record.reason == "malformed"
