import base64
import hmac
import json
import logging
import os
import time
from hashlib import sha256

import requests
from bcfy_types import BroadcastifyFeed, BroadcastifyFeedGenre

BROADCASTIFY_API_BASE_URL = "https://api.bcfy.io"
BROADCASTIFY_FETCH_TIMEOUT_SECONDS = 15

ALL_FEEDS_URL = f"{BROADCASTIFY_API_BASE_URL}/feeds/v1/feeds/"
NEAR_FEEDS_URL = f"{BROADCASTIFY_API_BASE_URL}/feeds/v1/near/"
ARCHIVES_URL = BROADCASTIFY_API_BASE_URL + "/feeds/v1/archives/{feed_id}"
ARCHIVES_FILES_URL = BROADCASTIFY_API_BASE_URL + "/feeds/v1/archives/{feed_id}"

BROADCASTIFY_APP_ID = os.getenv("BROADCASTIFY_APP_ID", "6797c432dc150")
BROADCASTIFY_API_KEY_ID = os.getenv("BROADCASTIFY_API_KEY_ID", None)
BROADCASTIFY_API_TOKEN = os.getenv("BROADCASTIFY_API_TOKEN", None)

logger = logging.getLogger(__name__)


class MissingTokenError(ValueError):
    """Raised when the Broadcastify API token is missing."""

    def __init__(self) -> None:
        super().__init__("BROADCASTIFY_API_TOKEN must be set")


def _base64url_encode(data: bytes) -> str:
    padding = b"="
    return base64.urlsafe_b64encode(data).rstrip(padding).decode("utf-8")


def _create_header() -> dict:
    return {"alg": "HS256", "typ": "JWT", "kid": BROADCASTIFY_API_KEY_ID}


def _create_payload() -> dict:
    now = int(time.time())
    return {"iss": BROADCASTIFY_APP_ID, "iat": now, "exp": now + 60}


def _create_signature(encoded_header: str, encoded_payload: str) -> str:
    if BROADCASTIFY_API_TOKEN is None:
        raise MissingTokenError
    message = f"{encoded_header}.{encoded_payload}".encode()
    signature = hmac.new(
        BROADCASTIFY_API_TOKEN.encode("utf-8"),
        message,
        sha256,
    ).digest()
    return _base64url_encode(signature)


def _generate_token() -> str:
    """Generate a JWT token for the Broadcastify API"""
    header = _create_header()
    encoded_header = _base64url_encode(json.dumps(header).encode("utf-8"))

    payload = _create_payload()
    encoded_payload = _base64url_encode(json.dumps(payload).encode("utf-8"))

    signature = _create_signature(encoded_header, encoded_payload)

    return f"{encoded_header}.{encoded_payload}.{signature}"


def fetch_all_feeds(genre: BroadcastifyFeedGenre) -> list[BroadcastifyFeed]:
    jwt = _generate_token()

    response = requests.get(
        ALL_FEEDS_URL,
        headers={"Authorization": f"Bearer {jwt}"},
        params={"genreId": int(genre.value)},
        timeout=BROADCASTIFY_FETCH_TIMEOUT_SECONDS,
    )
    if response.status_code == 204:
        return []

    response.raise_for_status()
    feeds = response.json()
    return [BroadcastifyFeed(**feed) for feed in feeds]


def fetch_archive_days(feed_id: int) -> list[str]:
    jwt = _generate_token()

    response = requests.get(
        ARCHIVES_URL.format(feed_id=feed_id),
        headers={"Authorization": f"Bearer {jwt}"},
        timeout=BROADCASTIFY_FETCH_TIMEOUT_SECONDS,
    )
    if response.status_code == 204:
        return []

    response.raise_for_status()
    archives = response.json()
    return [a["day"] for a in archives["days"]]


def fetch_archive_files(feed_id: int) -> list:
    jwt = _generate_token()

    response = requests.get(
        ARCHIVES_FILES_URL.format(feed_id=feed_id),
        headers={"Authorization": f"Bearer {jwt}"},
        timeout=BROADCASTIFY_FETCH_TIMEOUT_SECONDS,
    )
    if response.status_code == 204:
        return []

    response.raise_for_status()
    archives = response.json()
    return [a["url"] for a in archives["archives"]]
