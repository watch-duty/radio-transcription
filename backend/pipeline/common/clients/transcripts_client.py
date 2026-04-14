from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import requests
from google.protobuf import json_format

from backend.pipeline.common.auth import get_id_token
from backend.pipeline.common.env import is_gcp_env
from backend.pipeline.common.exceptions import AlreadyExistsError

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
        data = json_format.MessageToDict(
            payload, preserving_proto_field_name=True
        )

        if is_gcp_env():
            token = get_id_token(self.api_url)
            self.session.headers.update({"Authorization": f"Bearer {token}"})

        response = self.session.post(
            f"{self.api_url}/v1/transcripts",
            json=data,
            timeout=10,
        )
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            if response.status_code == 409:
                raise AlreadyExistsError(payload.transmission_id) from e
            raise
