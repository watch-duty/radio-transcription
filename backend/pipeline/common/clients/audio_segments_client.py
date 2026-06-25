from __future__ import annotations

import asyncio
import logging
import threading
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

import httpx
import requests.auth
from tenacity import (
    AsyncRetrying,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from backend.pipeline.common.auth_client import get_id_token
from backend.pipeline.common.clients.session_helper import (
    create_resilient_session,
)
from backend.pipeline.common.env import is_gcp_env
from backend.pipeline.common.tracing_utils import get_current_traceparent

if TYPE_CHECKING:
    from backend.services.audio_segments.models import AnnotationType

logger = logging.getLogger(__name__)


class AudioSegmentsClient:
    """
    Resilient synchronous client for interacting with the Audio Segments API.
    """

    def __init__(self, api_url: str, max_retries: int = 3) -> None:
        """
        Initializes the AudioSegmentsClient with retry-resilient sessions.

        Args:
            api_url: The base URL of the Audio Segments API.
            max_retries: The maximum number of retries for transient network errors.
        """
        self.api_url = api_url.rstrip("/")
        self.session = create_resilient_session(
            max_retries=max_retries,
            backoff_factor=0.5,  # [0.5s, 1.0s, 2.0s]
            raise_on_status=False,
        )
        if is_gcp_env():
            self.session.auth = GCPMetadataAuth(self.api_url)

    def add_audio_segment_annotation(
        self,
        audio_segment_id: str,
        annotation_type: AnnotationType,
        data: dict,
    ) -> None:
        """
        Adds an annotation to a specific audio segment.

        Args:
            audio_segment_id: The ID of the audio segment.
            annotation_type: The type of annotation (e.g. TRANSCRIPT, EVALUATION).
            data: The annotation data payload.

        Raises:
            requests.exceptions.HTTPError: If the request fails.
        """
        headers = {}
        traceparent = get_current_traceparent()
        if traceparent:
            headers["traceparent"] = traceparent

        payload = {
            "type": str(annotation_type),
            "data": data,
        }

        response = self.session.post(
            f"{self.api_url}/v1/audio_segments/{audio_segment_id}/annotations",
            json=payload,
            headers=headers,
            timeout=10,
        )
        response.raise_for_status()

    def add_audio_segment(self, segment: dict) -> None:
        """
        Saves a single audio segment.

        Args:
            segment: The audio segment data to add.

        Raises:
            requests.exceptions.HTTPError: If the request fails.
        """
        headers = {}
        traceparent = get_current_traceparent()
        if traceparent:
            headers["traceparent"] = traceparent

        response = self.session.post(
            f"{self.api_url}/v1/audio_segments",
            json=segment,
            headers=headers,
            timeout=10,
        )
        response.raise_for_status()


class GCPMetadataAuth(requests.auth.AuthBase):
    """Custom requests authentication class that fetches GCP ID tokens."""

    def __init__(self, audience: str) -> None:
        self.audience = audience

    def __call__(self, r: requests.PreparedRequest) -> requests.PreparedRequest:
        token = get_id_token(self.audience)
        if r.headers is not None:
            r.headers["Authorization"] = f"Bearer {token}"
        return r


# Async OIDC token cache mapping audience to (token, expire_timestamp)
_async_token_cache: dict[str, tuple[str, float]] = {}
_async_locks_lock = threading.Lock()
_async_audience_locks: dict[str, asyncio.Lock] = {}

# Keep cached tokens for 45 minutes (2700 seconds)
ASYNC_CACHE_TTL_SECONDS = 2700


class GCPMetadataAsyncAuth(httpx.Auth):
    """Custom httpx authentication class that fetches GCP ID tokens asynchronously."""

    def __init__(self, audience: str) -> None:
        self.audience = audience

    async def _fetch_id_token_async(self) -> str:
        """Asynchronously fetches OIDC ID token directly from GCP Metadata Server."""
        url = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/identity"
        headers = {"Metadata-Flavor": "Google"}
        params = {"audience": self.audience}

        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=headers, params=params, timeout=5.0)
            response.raise_for_status()
            return response.text.strip()

    async def async_auth_flow(
        self, request: httpx.Request
    ) -> AsyncGenerator[httpx.Request, httpx.Response]:
        now = time.monotonic()

        # 1. Quick check: read from cache
        if self.audience in _async_token_cache:
            token, expire_at = _async_token_cache[self.audience]
            if now < expire_at:
                request.headers["Authorization"] = f"Bearer {token}"
                yield request
                return

        # 2. Get or create a Lock specific to this audience
        with _async_locks_lock:
            if self.audience not in _async_audience_locks:
                _async_audience_locks[self.audience] = asyncio.Lock()
            audience_lock = _async_audience_locks[self.audience]

        # 3. Synchronize only for this audience during the fetch
        async with audience_lock:
            # Double check cache inside the lock
            if self.audience in _async_token_cache:
                token, expire_at = _async_token_cache[self.audience]
                if now < expire_at:
                    request.headers["Authorization"] = f"Bearer {token}"
                    yield request
                    return

            token = await self._fetch_id_token_async()
            _async_token_cache[self.audience] = (token, now + ASYNC_CACHE_TTL_SECONDS)

        request.headers["Authorization"] = f"Bearer {token}"
        yield request


def is_transient_error(e: BaseException) -> bool:
    """Retries on all network errors and transient 429/5xx status codes."""
    if isinstance(e, httpx.HTTPStatusError):
        return e.response.status_code in {429, 500, 502, 503, 504}
    return isinstance(e, (httpx.TransportError, httpx.TimeoutException))


class AsyncAudioSegmentsClient:
    """
    Resilient asynchronous client for interacting with the Audio Segments API.
    """

    def __init__(self, api_url: str, max_retries: int = 3) -> None:
        """
        Initializes the AsyncAudioSegmentsClient.

        Args:
            api_url: The base URL of the Audio Segments API.
            max_retries: The maximum number of retries for transient network errors.
        """
        self.api_url = api_url.rstrip("/")
        self.max_retries = max_retries
        transport = httpx.AsyncHTTPTransport(retries=0)
        auth = GCPMetadataAsyncAuth(self.api_url) if is_gcp_env() else None
        self.client = httpx.AsyncClient(transport=transport, auth=auth)

    async def close(self) -> None:
        """Closes the underlying HTTP client session connection pool."""
        await self.client.aclose()

    async def add_audio_segment_annotation(
        self,
        audio_segment_id: str,
        annotation_type: AnnotationType,
        data: dict,
    ) -> None:
        """
        Adds an annotation to a specific audio segment asynchronously.

        Args:
            audio_segment_id: The ID of the audio segment.
            annotation_type: The type of annotation (e.g. TRANSCRIPT, EVALUATION).
            data: The annotation data payload.

        Raises:
            httpx.HTTPStatusError: If the request fails.
        """
        headers = {}
        traceparent = get_current_traceparent()
        if traceparent:
            headers["traceparent"] = traceparent

        payload = {
            "type": str(annotation_type),
            "data": data,
        }

        async for attempt in AsyncRetrying(
            retry=retry_if_exception(is_transient_error),
            stop=stop_after_attempt(self.max_retries),
            wait=wait_exponential(multiplier=0.5, min=0.5, max=2.0),
            reraise=True,
        ):
            with attempt:
                response = await self.client.post(
                    f"{self.api_url}/v1/audio_segments/{audio_segment_id}/annotations",
                    json=payload,
                    headers=headers,
                    timeout=10.0,
                )
                response.raise_for_status()

    async def add_audio_segment(self, segment: dict) -> None:
        """
        Saves a single audio segment asynchronously.

        Args:
            segment: The audio segment data to add.

        Raises:
            httpx.HTTPStatusError: If the request fails.
        """
        headers = {}
        traceparent = get_current_traceparent()
        if traceparent:
            headers["traceparent"] = traceparent

        async for attempt in AsyncRetrying(
            retry=retry_if_exception(is_transient_error),
            stop=stop_after_attempt(self.max_retries),
            wait=wait_exponential(multiplier=0.5, min=0.5, max=2.0),
            reraise=True,
        ):
            with attempt:
                response = await self.client.post(
                    f"{self.api_url}/v1/audio_segments",
                    json=segment,
                    headers=headers,
                    timeout=10.0,
                )
                response.raise_for_status()
