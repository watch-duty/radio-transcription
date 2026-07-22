from __future__ import annotations

import httpx
import requests
from requests.adapters import HTTPAdapter
from tenacity import (
    Retrying,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)
from urllib3.util import Retry

from backend.pipeline.common import auth_client, env
from backend.pipeline.common.tracing_utils import get_current_traceparent

DEFAULT_STATUS_FORCELIST = [429, 500, 502, 503, 504]


def create_resilient_session(
    max_retries: int = 3,
    backoff_factor: float = 0.5,
    status_forcelist: list[int] | None = None,
    *,
    raise_on_status: bool = False,
) -> requests.Session:
    """
    Creates a requests.Session configured with exponential backoff retries
    for transient network and gateway faults.

    Args:
        max_retries: The maximum number of retries. If <= 0, retries are disabled.
        backoff_factor: Exponential backoff factor.
        status_forcelist: List of HTTP status codes to retry. Defaults to [502, 503, 504].
        raise_on_status: Whether to raise an exception immediately on matched status codes.
            Must be passed as a keyword argument.

    Returns:
        A requests.Session instance.
    """
    session = requests.Session()
    if max_retries > 0:
        retries = Retry(
            total=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist
            if status_forcelist is not None
            else DEFAULT_STATUS_FORCELIST,
            raise_on_status=raise_on_status,
        )
        adapter = HTTPAdapter(max_retries=retries)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
    return session


def is_transient_httpx_error(e: BaseException) -> bool:
    """Checks if an exception is a transient HTTP/transport error suitable for retries."""
    if isinstance(e, httpx.HTTPStatusError):
        return e.response.status_code in {429, 500, 502, 503, 504}
    return isinstance(e, (httpx.TransportError, httpx.TimeoutException))


def get_httpx_retry_config(
    total_attempts: int = 4,
    multiplier: float = 0.5,
    min_seconds: float = 0.5,
    max_seconds: float = 10.0,
) -> dict:
    """Returns tenacity retry configuration arguments."""
    return {
        "stop": stop_after_attempt(total_attempts),
        "wait": wait_exponential(
            multiplier=multiplier, min=min_seconds, max=max_seconds
        ),
        "retry": retry_if_exception(is_transient_httpx_error),
        "reraise": True,
    }


def authenticated_get(
    client: httpx.Client,
    base_url: str,
    url: str,
    *,
    params: dict | None = None,
    timeout: float = 5,
    total_attempts: int = 4,
    multiplier: float = 0.5,
    min_seconds: float = 0.5,
    max_seconds: float = 2.0,
) -> httpx.Response:
    """Performs a GET against an internal API with traceparent propagation, GCP
    auth, and retries on transient errors.

    Raises the last ``httpx.HTTPError`` if every retry attempt is exhausted.
    """
    headers: dict[str, str] = {}

    traceparent = get_current_traceparent()
    if traceparent:
        headers["traceparent"] = traceparent

    if env.is_gcp_env():
        token = auth_client.get_id_token(base_url)
        headers["Authorization"] = f"Bearer {token}"

    for attempt in Retrying(
        **get_httpx_retry_config(
            total_attempts=total_attempts,
            multiplier=multiplier,
            min_seconds=min_seconds,
            max_seconds=max_seconds,
        )
    ):
        with attempt:
            response = client.get(
                url, params=params, headers=headers, timeout=timeout
            )
            response.raise_for_status()
    return response
