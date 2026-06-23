from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from urllib import request

from backend.pipeline.common.env import is_gcp_env

logger = logging.getLogger(__name__)

GOOGLE_USER_ACTOR_PREFIX = "user:google:"
GCP_SERVICE_ACCOUNT_ACTOR_PREFIX = "service_account:gcp:"
LOCAL_SERVICE_ACCOUNT_ACTOR_ID = "service_account:local:development"
UNRESOLVED_GCP_SERVICE_ACCOUNT_ACTOR_ID = "service_account:gcp:unresolved"

_METADATA_SERVICE_ACCOUNT_EMAIL_URL = (
    "http://metadata.google.internal/computeMetadata/v1/"
    "instance/service-accounts/default/email"
)
_METADATA_HEADERS = {"Metadata-Flavor": "Google"}
_METADATA_TIMEOUT_SECONDS = 1.0
_METADATA_RETRY_SECONDS = 60.0


@dataclass
class _RuntimeActorState:
    cached_gcp_actor_id: str | None = None
    refresh_in_flight: bool = False
    next_refresh_monotonic: float = 0.0


_runtime_actor_state = _RuntimeActorState()
_runtime_actor_lock = threading.Lock()


def actor_id_from_google_sub(sub: str) -> str:
    normalized_sub = sub.strip()
    if not normalized_sub:
        msg = "Google user sub is required"
        raise ValueError(msg)
    if _contains_whitespace(normalized_sub):
        msg = "Google user sub must not contain whitespace"
        raise ValueError(msg)

    return f"{GOOGLE_USER_ACTOR_PREFIX}{normalized_sub}"


def is_well_formed_google_user_actor_id(actor_id: str) -> bool:
    if not actor_id.startswith(GOOGLE_USER_ACTOR_PREFIX):
        return False

    google_sub = actor_id[len(GOOGLE_USER_ACTOR_PREFIX) :]
    return bool(google_sub) and not _contains_whitespace(google_sub)


def runtime_service_actor_id() -> str:
    if not is_gcp_env():
        return LOCAL_SERVICE_ACCOUNT_ACTOR_ID

    with _runtime_actor_lock:
        if _runtime_actor_state.cached_gcp_actor_id is not None:
            return _runtime_actor_state.cached_gcp_actor_id

        _schedule_metadata_refresh_locked(now=time.monotonic())

    return UNRESOLVED_GCP_SERVICE_ACCOUNT_ACTOR_ID


def _contains_whitespace(value: str) -> bool:
    return any(char.isspace() for char in value)


def _schedule_metadata_refresh_locked(*, now: float) -> None:
    if _runtime_actor_state.refresh_in_flight:
        return
    if now < _runtime_actor_state.next_refresh_monotonic:
        return

    _runtime_actor_state.refresh_in_flight = True
    try:
        _start_metadata_refresh_thread()
    except Exception:
        _runtime_actor_state.refresh_in_flight = False
        _runtime_actor_state.next_refresh_monotonic = (
            now + _METADATA_RETRY_SECONDS
        )
        logger.exception(
            "Failed to start GCP service-account actor refresh; using actor_id=%s",
            UNRESOLVED_GCP_SERVICE_ACCOUNT_ACTOR_ID,
        )


def _start_metadata_refresh_thread() -> None:
    thread = threading.Thread(
        target=_refresh_metadata_actor_cache,
        name="gcp-service-account-actor-refresh",
        daemon=True,
    )
    thread.start()


def _refresh_metadata_actor_cache() -> None:
    try:
        service_account_email = _fetch_metadata_service_account_email()
    except Exception:
        logger.exception(
            "Failed to resolve GCP service-account actor from metadata; "
            "using actor_id=%s",
            UNRESOLVED_GCP_SERVICE_ACCOUNT_ACTOR_ID,
        )
        next_refresh_monotonic = time.monotonic() + _METADATA_RETRY_SECONDS
        with _runtime_actor_lock:
            _runtime_actor_state.refresh_in_flight = False
            _runtime_actor_state.next_refresh_monotonic = next_refresh_monotonic
        return

    actor_id = f"{GCP_SERVICE_ACCOUNT_ACTOR_PREFIX}{service_account_email}"
    with _runtime_actor_lock:
        _runtime_actor_state.cached_gcp_actor_id = actor_id
        _runtime_actor_state.refresh_in_flight = False
        _runtime_actor_state.next_refresh_monotonic = 0.0


def _fetch_metadata_service_account_email() -> str:
    metadata_request = request.Request(  # noqa: S310
        _METADATA_SERVICE_ACCOUNT_EMAIL_URL,
        headers=_METADATA_HEADERS,
    )

    # Fixed GCP metadata endpoint; no user-controlled URL is opened here.
    with request.urlopen(  # noqa: S310
        metadata_request,
        timeout=_METADATA_TIMEOUT_SECONDS,
    ) as response:
        service_account_email = response.read().decode("utf-8").strip()

    if not _is_valid_metadata_service_account_email(service_account_email):
        msg = "Invalid service-account email from metadata server"
        raise ValueError(msg)

    return service_account_email


def _is_valid_metadata_service_account_email(email: str) -> bool:
    return bool(email) and "@" in email and not _contains_whitespace(email)


def _reset_runtime_service_actor_cache_for_tests() -> None:
    with _runtime_actor_lock:
        _runtime_actor_state.cached_gcp_actor_id = None
        _runtime_actor_state.refresh_in_flight = False
        _runtime_actor_state.next_refresh_monotonic = 0.0
