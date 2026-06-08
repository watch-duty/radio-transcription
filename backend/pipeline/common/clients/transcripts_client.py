from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import requests
from google.protobuf import json_format

from backend.pipeline.common.auth_client import get_id_token
from backend.pipeline.common.env import is_gcp_env
from backend.pipeline.common.exceptions import AlreadyExistsError
from backend.pipeline.common.tracing_utils import get_current_traceparent

if TYPE_CHECKING:
    from backend.pipeline.schema_types import (
        evaluated_transcribed_audio_pb2 as evaluated_pb2,
    )

logger = logging.getLogger(__name__)


class TranscriptsClient:
    """
    Client for interacting with the Transcripts API.
    """

    def __init__(self, api_url: str) -> None:
        """
        Initializes the TranscriptsClient.

        Args:
            api_url: The base URL of the Transcripts API.
        """
        self.api_url = api_url.rstrip("/")
        self.session = requests.Session()

    def create_transcript(
        self, payload: evaluated_pb2.EvaluatedTranscribedAudio
    ) -> None:
        """
        Sends the evaluated transcript to the Transcripts API.

        Args:
            payload: The evaluated transcript payload.

        Raises:
            requests.exceptions.HTTPError: If the request fails.
        """

        def _raise(msg: str) -> None:
            raise ValueError(msg)

        if not payload.segment_id:
            _raise("segment_id is required")
        if not payload.feed_id:
            _raise("feed_id is required")
        if not payload.transcript:
            _raise("transcript is required")
        if not payload.source_audio_uris:
            _raise("source_audio_uris is required")

        data = json_format.MessageToDict(
            payload,
            preserving_proto_field_name=True,
            always_print_fields_with_no_presence=True,
        )
        if "segment_id" in data:
            data["transmission_id"] = data.pop("segment_id")

        headers = {}
        traceparent = get_current_traceparent()
        if traceparent:
            headers["traceparent"] = traceparent

        if is_gcp_env():
            token = get_id_token(self.api_url)
            self.session.headers.update({"Authorization": f"Bearer {token}"})

        response = self.session.post(
            f"{self.api_url}/v1/transcripts",
            json=data,
            headers=headers,
            timeout=10,
        )

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            if response.status_code == 409:
                raise AlreadyExistsError(payload.segment_id) from e
            raise
