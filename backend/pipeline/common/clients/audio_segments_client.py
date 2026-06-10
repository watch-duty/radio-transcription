from __future__ import annotations

import logging
from typing import TYPE_CHECKING

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
    Resilient client for interacting with the Audio Segments API.
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

        if is_gcp_env():
            token = get_id_token(self.api_url)
            self.session.headers.update({"Authorization": f"Bearer {token}"})

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

        if is_gcp_env():
            token = get_id_token(self.api_url)
            self.session.headers.update({"Authorization": f"Bearer {token}"})

        response = self.session.post(
            f"{self.api_url}/v1/audio_segments",
            json=segment,
            headers=headers,
            timeout=10,
        )
        response.raise_for_status()
