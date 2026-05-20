"""Self-contained Broadcastify Calls + Common HTTP client for the framing-context A/B.

JWT logic is copy-adapted from radio-transcription/model/data/broadcastify/
bcfy_api.py (the experiment spec forbids modifying main-repo code). Adds the
Calls-API methods that bcfy_api.py does not expose: live calls (with
historical pos), group_get, node_get, groups_ctid, group_archives -- plus a
few Common-API methods (states, counties_in_state) used by the global
talkgroup enumeration in build_context_records.

Rate limiting (per Broadcastify docs):
- live: "should be polled no more than once every 5 seconds" — enforced here
  with a 5s throttle on the `live` endpoint.
- Other Calls / Common endpoints have no documented rate, but we apply a
  defensive 1s minimum-spacing throttle to avoid surprising the upstream.

Env vars (preferred -> fallback):
- BROADCASTIFY_JWT            preferred: pre-generated user-authenticated JWT
                              (Secret Manager `broadcastify-jwt-prod`). The
                              Calls endpoints require user authentication
                              (sub + utk in the payload), not app-only --
                              match production by using the same rotated JWT.
- BROADCASTIFY_APP_ID         (default "6797c432dc150")
- BROADCASTIFY_API_KEY_ID     fallback path: generate app-only JWT (only
- BROADCASTIFY_API_TOKEN      works for some Feeds endpoints, NOT Calls).
"""

from __future__ import annotations

import base64
import hmac
import json
import logging
import os
import time
from dataclasses import dataclass
from hashlib import sha256
from typing import Any

import requests

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BCFY_BASE = "https://api.bcfy.io"
TIMEOUT_S = 15

# Calls endpoints
LIVE_URL = f"{BCFY_BASE}/calls/v1/live/"
GROUP_GET_URL = BCFY_BASE + "/calls/v1/group_get/{group_id}"
NODE_GET_URL = BCFY_BASE + "/calls/v1/node_get/{node_id}"
GROUPS_CTID_URL = BCFY_BASE + "/calls/v1/groups_ctid/{ctid}"
GROUP_ARCHIVES_URL = (
    BCFY_BASE + "/calls/v1/group_archives/{group_id}/{start}/{end}"
)

# Common endpoints (used by global enumeration)
STATES_URL = BCFY_BASE + "/common/v1/states/{coid}"
COUNTIES_URL = BCFY_BASE + "/common/v1/counties/{stid}"

# Preferred: pre-generated user-auth JWT (matches production; required by Calls endpoints).
JWT = os.getenv("BROADCASTIFY_JWT")

# Fallback: app-only JWT generation (works for some Feeds endpoints, NOT Calls).
APP_ID = os.getenv("BROADCASTIFY_APP_ID", "6797c432dc150")
KEY_ID = os.getenv("BROADCASTIFY_API_KEY_ID")
TOKEN = os.getenv("BROADCASTIFY_API_TOKEN")

LIVE_THROTTLE_S = 5.0   # per Broadcastify spec
CALL_THROTTLE_S = 1.0   # defensive default for other endpoints


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class MissingTokenError(ValueError):
    """BROADCASTIFY_API_KEY_ID / BROADCASTIFY_API_TOKEN not set in env."""


# ---------------------------------------------------------------------------
# JWT
# ---------------------------------------------------------------------------


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _generate_jwt() -> str:
    if not KEY_ID or not TOKEN:
        raise MissingTokenError(
            "set BROADCASTIFY_API_KEY_ID and BROADCASTIFY_API_TOKEN env vars"
        )
    header = {"alg": "HS256", "typ": "JWT", "kid": KEY_ID}
    now = int(time.time())
    payload = {"iss": APP_ID, "iat": now, "exp": now + 3600}
    eh = _b64url(json.dumps(header, separators=(",", ":")).encode())
    ep = _b64url(json.dumps(payload, separators=(",", ":")).encode())
    sig = hmac.new(TOKEN.encode(), f"{eh}.{ep}".encode(), sha256).digest()
    return f"{eh}.{ep}.{_b64url(sig)}"


def _auth_headers() -> dict[str, str]:
    """Return the Authorization header.

    Prefer the pre-generated user-authenticated JWT in `BROADCASTIFY_JWT`
    (the Calls endpoints require user auth, not app-only). Fall back to
    app-only JWT generation when only KEY_ID/TOKEN are available.
    """
    if JWT:
        return {"Authorization": f"Bearer {JWT}"}
    return {"Authorization": f"Bearer {_generate_jwt()}"}


# ---------------------------------------------------------------------------
# Per-endpoint throttle
# ---------------------------------------------------------------------------

_last_ts: dict[str, float] = {}


def _throttle(key: str, interval_s: float) -> None:
    """Sleep so the time since the last call tagged with `key` is >= interval_s."""
    now = time.monotonic()
    wait = interval_s - (now - _last_ts.get(key, 0.0))
    if wait > 0:
        log.debug("throttle %s: sleeping %.2fs", key, wait)
        time.sleep(wait)
    _last_ts[key] = time.monotonic()


# ---------------------------------------------------------------------------
# Calls API methods
# ---------------------------------------------------------------------------


def live(sid: int, pos: int | None = None) -> list[dict[str, Any]]:
    """GET /calls/v1/live/?sid={sid}&pos={pos}

    Each returned call dict carries `ts`, `groupId`, `grouping`, `tag`, etc.
    With `pos` set to a unix timestamp, asks for calls *after* that timestamp.
    Historical-pos support is what M1 recovery depends on — the M1 probe in
    build_context_records.py validates it.

    Throttled per Broadcastify spec (5s minimum between live calls).
    """
    _throttle("live", LIVE_THROTTLE_S)
    params: dict[str, Any] = {"sid": sid}
    if pos is not None:
        params["pos"] = pos
    r = requests.get(
        LIVE_URL, headers=_auth_headers(), params=params, timeout=TIMEOUT_S
    )
    r.raise_for_status()
    body = r.json()
    if isinstance(body, dict):
        return body.get("calls", [])
    return body


def group(group_id: str) -> dict[str, Any]:
    """GET /calls/v1/group_get/{groupId}"""
    _throttle("call", CALL_THROTTLE_S)
    r = requests.get(
        GROUP_GET_URL.format(group_id=group_id),
        headers=_auth_headers(),
        timeout=TIMEOUT_S,
    )
    r.raise_for_status()
    return r.json()


def node(node_id: int) -> dict[str, Any]:
    """GET /calls/v1/node_get/{nodeId}"""
    _throttle("call", CALL_THROTTLE_S)
    r = requests.get(
        NODE_GET_URL.format(node_id=node_id),
        headers=_auth_headers(),
        timeout=TIMEOUT_S,
    )
    r.raise_for_status()
    return r.json()


def groups_ctid(ctid: int, secs_ago: int = 86400 * 365) -> list[dict[str, Any]]:
    """POST /calls/v1/groups_ctid/{ctid} — talkgroups recently active in a county.

    `secs_ago` filters to groups seen in the last N seconds. Default 1 year
    (the global-enumeration path needs old-but-active talkgroups visible).
    """
    _throttle("call", CALL_THROTTLE_S)
    r = requests.post(
        GROUPS_CTID_URL.format(ctid=ctid),
        headers=_auth_headers(),
        data={"secs_ago": secs_ago},
        timeout=TIMEOUT_S,
    )
    r.raise_for_status()
    return r.json()


def group_archives(group_id: str, start: int, end: int) -> dict[str, Any]:
    """GET /calls/v1/group_archives/{groupId}/{start}/{end} (max 8 hours)."""
    if end - start > 28800:
        raise ValueError(
            f"group_archives window must be <=28800s; got {end - start}"
        )
    _throttle("call", CALL_THROTTLE_S)
    r = requests.get(
        GROUP_ARCHIVES_URL.format(group_id=group_id, start=start, end=end),
        headers=_auth_headers(),
        timeout=TIMEOUT_S,
    )
    r.raise_for_status()
    return r.json()


# ---------------------------------------------------------------------------
# Common API methods (used by global enumeration)
# ---------------------------------------------------------------------------


def states(coid: int = 1) -> list[dict[str, Any]]:
    """GET /common/v1/states/{coid} -- list of states in a country (default US)."""
    _throttle("call", CALL_THROTTLE_S)
    r = requests.get(
        STATES_URL.format(coid=coid),
        headers=_auth_headers(),
        timeout=TIMEOUT_S,
    )
    r.raise_for_status()
    return r.json()


def counties_in_state(stid: int) -> list[dict[str, Any]]:
    """GET /common/v1/counties/{stid} -- list of counties in a state."""
    _throttle("call", CALL_THROTTLE_S)
    r = requests.get(
        COUNTIES_URL.format(stid=stid),
        headers=_auth_headers(),
        timeout=TIMEOUT_S,
    )
    r.raise_for_status()
    return r.json()


# ---------------------------------------------------------------------------
# Probe outcome (consumed by build_context_records.py)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ProbeResult:
    """Outcome of the M1 historical-pos probe."""

    works: bool
    calls_returned: int
    note: str
