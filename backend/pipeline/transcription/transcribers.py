"""
Pluggable Transcription API Architecture.

This module defines the abstract interface for audio transcription services,
allowing the Beam pipeline to dynamically swap between different engines
(e.g., Google Cloud Speech-to-Text, Whisper, Custom Models) via configuration.
"""

import abc
import logging
from dataclasses import dataclass, field

import tenacity
from google.api_core.exceptions import GoogleAPIError, RetryError
from google.cloud import speech_v2 as cloud_speech
from google.cloud.speech_v2 import SpeechClient

from backend.pipeline.transcription.constants import BYTES_PER_SECOND_16KHZ_MONO
from backend.pipeline.transcription.enums import TranscriberType
from backend.pipeline.transcription.utils import JsonConfigMixin

logger = logging.getLogger(__name__)


class Transcriber(abc.ABC):
    """
    Abstract interface for handling audio transcription.
    Allows hot-swapping different external transcription APIs or models.
    """

    @abc.abstractmethod
    def setup(self) -> None:
        """
        Called once per Beam worker upon initialization.
        Use this to spin up clients, establish connections, or load models
        that cannot be pickled/serialized across processes.
        """

    @abc.abstractmethod
    def transcribe(
        self,
        *,
        audio_data: bytes,
    ) -> str:
        """
        Transcribes the raw audio bytes and returns the text transcript.
        """


@dataclass(frozen=True)
class ChirpConfig(JsonConfigMixin):
    """Strongly typed configuration for the Google Chirp V3 Transcriber."""

    location: str = "us"
    recognizer: str = "_"
    model: str = "chirp_3"
    language_codes: list[str] = field(default_factory=lambda: ["en-US"])
    enable_automatic_punctuation: bool = True
    enable_word_time_offsets: bool = False


class GoogleChirpV3Transcriber(Transcriber):
    """
    Transcriber implementation using Google Cloud Speech-to-Text V2 API
    with the 'chirp_3' model.
    """

    def __init__(self, project_id: str, config_json: str) -> None:
        self.project_id = project_id
        self.config_json = config_json

        self.client: SpeechClient | None = None
        self.config: ChirpConfig | None = None

    def setup(self) -> None:
        self.client = SpeechClient()
        self.config = ChirpConfig.from_json(self.config_json)

    @tenacity.retry(
        wait=tenacity.wait_exponential(multiplier=1, max=10),
        stop=tenacity.stop_after_attempt(5),
        retry=tenacity.retry_if_exception_type((GoogleAPIError, RetryError)),
        reraise=True,
    )
    def transcribe(
        self,
        *,
        audio_data: bytes,
    ) -> str:

        if not self.client or not self.config:
            msg = "Transcriber client used before setup() was called."
            raise RuntimeError(msg)

        duration_sec = len(audio_data) / BYTES_PER_SECOND_16KHZ_MONO

        logger.info(
            "Transcribing %.3fs of audio",
            duration_sec,
        )

        request = cloud_speech.RecognizeRequest(
            recognizer=f"projects/{self.project_id}/locations/{self.config.location}/recognizers/{self.config.recognizer}",
            config=cloud_speech.RecognitionConfig(
                auto_decoding_config=cloud_speech.AutoDetectDecodingConfig(),
                model=self.config.model,
                language_codes=self.config.language_codes,
                features=cloud_speech.RecognitionFeatures(
                    enable_automatic_punctuation=self.config.enable_automatic_punctuation,
                    enable_word_time_offsets=self.config.enable_word_time_offsets,
                ),
            ),
            content=audio_data,
        )

        response = self.client.recognize(request=request)
        transcript = ""
        for result in response.results:
            if result.alternatives:
                # Chirp v3 is prompted to emit [BACKGROUND] when no speech is detected.
                # We strip this string explicitly across all chunks.
                chunk_text = (
                    result.alternatives[0]
                    .transcript.replace("[BACKGROUND]", "")
                    .strip()
                )
                if chunk_text:
                    transcript += chunk_text + " "

        transcript = transcript.rstrip()

        if not transcript:
            msg = "Transcription returned [BACKGROUND] only or was completely empty (no discernable speech)."
            logger.warning(msg)
            raise ValueError(msg)

        return transcript


def get_transcriber(
    transcriber_type: TranscriberType, project_id: str, config_json: str
) -> Transcriber:
    if transcriber_type == TranscriberType.GOOGLE_CHIRP_V3:
        return GoogleChirpV3Transcriber(project_id, config_json)
    msg = f"Unknown transcriber type: {transcriber_type}"
    raise ValueError(msg)
