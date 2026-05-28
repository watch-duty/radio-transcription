from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from backend.pipeline.common.auth_client import get_id_token
from backend.pipeline.common.env import is_gcp_env

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
        self.session = requests.Session()

        if max_retries > 0:
            # Configure exponential backoff retries for transient gateway/network faults (502, 503, 504)
            retries = Retry(
                total=max_retries,
                backoff_factor=0.5,  # [0.5s, 1.0s, 2.0s]
                status_forcelist=[502, 503, 504],
                raise_on_status=False,
            )
            adapter = HTTPAdapter(max_retries=retries)
            self.session.mount("http://", adapter)
            self.session.mount("https://", adapter)

    def add_audio_segment_annotation(
        self,
        audio_segment_id: str,
        annotation_type: AnnotationType | str,
        data: dict,
    ) -> None:
        """
        Adds an annotation to a specific audio segment.

        Args:
            audio_segment_id: The ID of the audio segment.
            annotation_type: The type of annotation (e.g. TRANSCRIPT, EVALUATION).
            data: The annotation data payload.

        Raises:
            ValueError: If any inputs fail boundary validation.
            requests.exceptions.HTTPError: If the request fails.
        """

        def _raise(msg: str) -> None:
            raise ValueError(msg)

        # Fail-Fast Local input boundary validations
        if not audio_segment_id or not audio_segment_id.strip():
            _raise("audio_segment_id cannot be empty or whitespace")
        if not annotation_type:
            _raise("annotation_type cannot be empty")
        if not data:
            _raise("annotation data payload cannot be empty")

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
            timeout=10,
        )
        response.raise_for_status()

    def bulk_add_audio_segments(self, segments: list[dict]) -> int:
        """
        Saves multiple audio segments in bulk.

        Args:
            segments: A list of audio segment data to add.

        Returns:
            The count of successfully inserted segments.

        Raises:
            ValueError: If any inputs fail boundary validation.
            requests.exceptions.HTTPError: If the request fails.
        """

        def _raise(msg: str) -> None:
            raise ValueError(msg)

        # Fail-Fast Local input boundary validation
        if not segments:
            _raise("segments list cannot be empty")

        if is_gcp_env():
            token = get_id_token(self.api_url)
            self.session.headers.update({"Authorization": f"Bearer {token}"})

        response = self.session.post(
            f"{self.api_url}/v1/audio_segments",
            json={"audio_segments": segments},
            timeout=10,
        )
        response.raise_for_status()
        return response.json().get("inserted_count", 0)
