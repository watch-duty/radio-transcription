from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

import httpx
import requests.auth
from tenacity import AsyncRetrying

from backend.pipeline.common.auth_client import get_id_token
from backend.pipeline.common.clients.session_helper import (
    create_resilient_session,
    get_httpx_retry_config,
)
from backend.pipeline.common.env import is_gcp_env
from backend.pipeline.common.tracing_utils import get_current_traceparent

if TYPE_CHECKING:
    from backend.services.audio_segments.models import AnnotationType


class AudioSegmentsClient:
    """
    Resilient synchronous client for interacting with the Audio Segments API.
    """

    def __init__(
        self,
        api_url: str,
        max_retries: int = 3,
        default_timeout: float = 30.0,
    ) -> None:
        """
        Initializes the AudioSegmentsClient with retry-resilient sessions.

        Args:
            api_url: The base URL of the Audio Segments API.
            max_retries: The maximum number of retries for transient network errors.
            default_timeout: Default timeout in seconds for HTTP requests.
        """
        self.api_url = api_url.rstrip("/")
        self.default_timeout = default_timeout
        self.session = create_resilient_session(
            max_retries=max_retries,
            backoff_factor=0.5,  # [0.5s, 1.0s, 2.0s]
            raise_on_status=False,
            allowed_methods=None,
        )
        if is_gcp_env():
            self.session.auth = GCPMetadataAuth(self.api_url)

    def add_audio_segment_annotation(
        self,
        audio_segment_id: str,
        annotation_type: AnnotationType,
        data: dict,
        timeout: float | None = None,
    ) -> None:
        """
        Adds an annotation to a specific audio segment.

        Args:
            audio_segment_id: The ID of the audio segment.
            annotation_type: The type of annotation (e.g. TRANSCRIPT, EVALUATION).
            data: The annotation data payload.
            timeout: Optional request timeout override in seconds.

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
            timeout=timeout if timeout is not None else self.default_timeout,
        )
        response.raise_for_status()

    def add_audio_segment(
        self, segment: dict, timeout: float | None = None
    ) -> None:
        """
        Saves a single audio segment.

        Args:
            segment: The audio segment data to add.
            timeout: Optional request timeout override in seconds.

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
            timeout=timeout if timeout is not None else self.default_timeout,
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


class GCPMetadataAsyncAuth(httpx.Auth):
    """Custom httpx authentication class that fetches GCP ID tokens asynchronously."""

    def __init__(self, audience: str) -> None:
        self.audience = audience

    async def async_auth_flow(
        self, request: httpx.Request
    ) -> AsyncGenerator[httpx.Request, httpx.Response]:
        token = await asyncio.to_thread(get_id_token, self.audience)
        request.headers["Authorization"] = f"Bearer {token}"
        yield request


class AsyncAudioSegmentsClient:
    """
    Resilient asynchronous client for interacting with the Audio Segments API.
    """

    def __init__(
        self,
        api_url: str,
        max_retries: int = 3,
        default_timeout: float = 30.0,
    ) -> None:
        """
        Initializes the AsyncAudioSegmentsClient.

        Args:
            api_url: The base URL of the Audio Segments API.
            max_retries: The maximum number of retries for transient network errors.
            default_timeout: Default timeout in seconds for HTTP requests.
        """
        self.api_url = api_url.rstrip("/")
        self.max_retries = max_retries
        self.default_timeout = default_timeout
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
        timeout: float | None = None,  # noqa: ASYNC109
    ) -> None:
        """
        Adds an annotation to a specific audio segment asynchronously.

        Args:
            audio_segment_id: The ID of the audio segment.
            annotation_type: The type of annotation (e.g. TRANSCRIPT, EVALUATION).
            data: The annotation data payload.
            timeout: Optional request timeout override in seconds.

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

        timeout_val = timeout if timeout is not None else self.default_timeout

        async for attempt in AsyncRetrying(
            **get_httpx_retry_config(
                total_attempts=self.max_retries,
                multiplier=0.5,
                min_seconds=0.5,
                max_seconds=2.0,
            )
        ):
            with attempt:
                response = await self.client.post(
                    f"{self.api_url}/v1/audio_segments/{audio_segment_id}/annotations",
                    json=payload,
                    headers=headers,
                    timeout=timeout_val,
                )
                response.raise_for_status()

    async def add_audio_segment(
        self,
        segment: dict,
        timeout: float | None = None,  # noqa: ASYNC109
    ) -> None:
        """
        Saves a single audio segment asynchronously.

        Args:
            segment: The audio segment data to add.
            timeout: Optional request timeout override in seconds.

        Raises:
            httpx.HTTPStatusError: If the request fails.
        """
        headers = {}
        traceparent = get_current_traceparent()
        if traceparent:
            headers["traceparent"] = traceparent

        timeout_val = timeout if timeout is not None else self.default_timeout

        async for attempt in AsyncRetrying(
            **get_httpx_retry_config(
                total_attempts=self.max_retries,
                multiplier=0.5,
                min_seconds=0.5,
                max_seconds=2.0,
            )
        ):
            with attempt:
                response = await self.client.post(
                    f"{self.api_url}/v1/audio_segments",
                    json=segment,
                    headers=headers,
                    timeout=timeout_val,
                )
                response.raise_for_status()
