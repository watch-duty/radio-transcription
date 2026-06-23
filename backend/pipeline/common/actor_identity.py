from __future__ import annotations

import logging
import os

from backend.pipeline.common.env import is_gcp_env

logger = logging.getLogger(__name__)

GOOGLE_USER_ACTOR_PREFIX = "user:google:"
GCP_SERVICE_ACCOUNT_ACTOR_PREFIX = "service_account:gcp:"
LOCAL_SERVICE_ACCOUNT_ACTOR_ID = "service_account:local:development"
UNRESOLVED_GCP_SERVICE_ACCOUNT_ACTOR_ID = "service_account:gcp:unresolved"
MAX_ACTOR_ID_LENGTH = 512
CONFIGURED_SERVICE_ACTOR_ENV = "FEED_AUDIT_ACTOR_ID"

_cached_runtime_service_actor_id: str | None = None


def actor_id_from_google_sub(sub: str) -> str:
    normalized_sub = sub.strip()
    if not normalized_sub:
        msg = "Google user sub is required"
        raise ValueError(msg)
    if _contains_whitespace(normalized_sub):
        msg = "Google user sub must not contain whitespace"
        raise ValueError(msg)

    actor_id = f"{GOOGLE_USER_ACTOR_PREFIX}{normalized_sub}"
    if len(actor_id) > MAX_ACTOR_ID_LENGTH:
        msg = "Google user actor ID is too long"
        raise ValueError(msg)

    return actor_id


def is_well_formed_google_user_actor_id(actor_id: str) -> bool:
    if len(actor_id) > MAX_ACTOR_ID_LENGTH:
        return False
    if not actor_id.startswith(GOOGLE_USER_ACTOR_PREFIX):
        return False

    google_sub = actor_id[len(GOOGLE_USER_ACTOR_PREFIX) :]
    return bool(google_sub) and not _contains_whitespace(google_sub)


def is_well_formed_gcp_service_account_actor_id(actor_id: str) -> bool:
    if len(actor_id) > MAX_ACTOR_ID_LENGTH:
        return False
    if not actor_id.startswith(GCP_SERVICE_ACCOUNT_ACTOR_PREFIX):
        return False

    service_account_unique_id = actor_id[
        len(GCP_SERVICE_ACCOUNT_ACTOR_PREFIX) :
    ]
    return service_account_unique_id.isdigit()


def is_well_formed_actor_id(actor_id: str | None) -> bool:
    if actor_id is None:
        return False

    if not actor_id:
        return False

    if len(actor_id) > MAX_ACTOR_ID_LENGTH:
        return False

    return not _contains_whitespace(actor_id)


def runtime_service_actor_id() -> str:
    global _cached_runtime_service_actor_id  # noqa: PLW0603

    if _cached_runtime_service_actor_id is not None:
        return _cached_runtime_service_actor_id

    _cached_runtime_service_actor_id = _resolve_runtime_service_actor_id()
    return _cached_runtime_service_actor_id


def _resolve_runtime_service_actor_id() -> str:
    if not is_gcp_env():
        return LOCAL_SERVICE_ACCOUNT_ACTOR_ID

    configured_actor_id = os.environ.get(CONFIGURED_SERVICE_ACTOR_ENV)
    if (
        configured_actor_id is not None
        and is_well_formed_gcp_service_account_actor_id(
            configured_actor_id,
        )
    ):
        return configured_actor_id

    reason = "missing" if configured_actor_id is None else "malformed"
    _log_unresolved_gcp_actor_config(reason)
    return UNRESOLVED_GCP_SERVICE_ACCOUNT_ACTOR_ID


def _log_unresolved_gcp_actor_config(reason: str) -> None:
    logger.error(
        "feed_audit_actor_unresolved",
        extra={
            "event": "feed_audit_actor_unresolved",
            "gcp_runtime_detected": True,
            "reason": reason,
            "fallback_actor": UNRESOLVED_GCP_SERVICE_ACCOUNT_ACTOR_ID,
        },
    )


def _contains_whitespace(value: str) -> bool:
    return any(char.isspace() for char in value)


def _reset_runtime_service_actor_cache_for_tests() -> None:
    global _cached_runtime_service_actor_id  # noqa: PLW0603

    _cached_runtime_service_actor_id = None
