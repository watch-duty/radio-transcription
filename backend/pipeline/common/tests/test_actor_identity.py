from __future__ import annotations

import logging
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
        ("service_account:gcp:test@example.iam.gserviceaccount.com", False),
        ("service:collector-runtime", False),
    ],
)
def test_google_user_actor_validator(actor_id: str, expected) -> None:
    assert (
        actor_identity.is_well_formed_google_user_actor_id(actor_id) is expected
    )


def test_runtime_service_actor_uses_local_fallback_outside_gcp() -> None:
    with (
        mock.patch.object(actor_identity, "is_gcp_env", return_value=False),
        mock.patch.object(
            actor_identity,
            "_fetch_metadata_service_account_email",
        ) as fetch_metadata,
    ):
        actor_id = actor_identity.runtime_service_actor_id()

    assert actor_id == "service_account:local:development"
    fetch_metadata.assert_not_called()


def test_runtime_service_actor_caches_gcp_metadata_email() -> None:
    with (
        mock.patch.object(actor_identity, "is_gcp_env", return_value=True),
        mock.patch.object(
            actor_identity,
            "_fetch_metadata_service_account_email",
            return_value="test-sa@example.iam.gserviceaccount.com",
        ) as fetch_metadata,
    ):
        actor_id = actor_identity.runtime_service_actor_id()
        repeated_actor_id = actor_identity.runtime_service_actor_id()

    assert (
        actor_id
        == "service_account:gcp:test-sa@example.iam.gserviceaccount.com"
    )
    assert repeated_actor_id == actor_id
    fetch_metadata.assert_called_once_with()


def test_runtime_service_actor_returns_unresolved_when_gcp_metadata_fails(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with (
        caplog.at_level(logging.ERROR),
        mock.patch.object(
            actor_identity,
            "_fetch_metadata_service_account_email",
            side_effect=RuntimeError("metadata unavailable"),
        ) as fetch_metadata,
        mock.patch.object(actor_identity, "is_gcp_env", return_value=True),
    ):
        actor_id = actor_identity.runtime_service_actor_id()
        repeated_actor_id = actor_identity.runtime_service_actor_id()

    assert actor_id == "service_account:gcp:unresolved"
    assert repeated_actor_id == "service_account:gcp:unresolved"
    assert "Failed to resolve GCP service-account actor" in caplog.text
    fetch_metadata.assert_called_once_with()
