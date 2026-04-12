import base64
import hmac
import json
import logging
import os
import time
from hashlib import sha256

import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from config import BCFY_API_BASE

logger = logging.getLogger(__name__)

_BCFY_APP_ID = os.getenv("BROADCASTIFY_APP_ID", "6797c432dc150")
_BCFY_API_KEY_ID = os.getenv("BROADCASTIFY_API_KEY_ID")
_BCFY_API_KEY = os.getenv("BCFY_API_KEY")

_TIMEOUT = 15


def _b64url(data):
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def _generate_jwt():
    if not _BCFY_API_KEY:
        msg = "BCFY_API_KEY env var must be set"
        raise ValueError(msg)
    if not _BCFY_API_KEY_ID:
        msg = "BROADCASTIFY_API_KEY_ID env var must be set"
        raise ValueError(msg)

    header = _b64url(json.dumps(
        {"alg": "HS256", "typ": "JWT", "kid": _BCFY_API_KEY_ID}
    ).encode())
    now = int(time.time())
    payload = _b64url(json.dumps(
        {"iss": _BCFY_APP_ID, "iat": now, "exp": now + 60}
    ).encode())
    sig = _b64url(hmac.new(
        _BCFY_API_KEY.encode(), f"{header}.{payload}".encode(), sha256
    ).digest())
    return f"{header}.{payload}.{sig}"


@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
def get_auth_headers():
    jwt = _generate_jwt()
    return {"Authorization": f"Bearer {jwt}"}


@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
def bcfy_get(url, rate_limiter, params=None):
    rate_limiter.wait()
    headers = get_auth_headers()
    resp = requests.get(url, headers=headers, params=params, timeout=_TIMEOUT)
    resp.raise_for_status()
    if resp.status_code == 204:
        return []
    return resp.json()


@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
def bcfy_post(url, rate_limiter, data=None):
    rate_limiter.wait()
    headers = get_auth_headers()
    resp = requests.post(url, headers=headers, data=data or {}, timeout=_TIMEOUT)
    resp.raise_for_status()
    if resp.status_code == 204:
        return []
    return resp.json()
