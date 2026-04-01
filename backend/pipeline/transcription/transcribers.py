"""Pluggable Transcription API Architecture.

This module defines the abstract interface for audio transcription services,
allowing the Beam pipeline to dynamically swap between different engines
(e.g., Google Cloud Speech-to-Text, Whisper, Custom Models) via configuration.
"""

import abc
import logging
import pathlib

import pydantic
import tenacity
from google.api_core import client_options
from google.api_core.exceptions import GoogleAPIError, RetryError
from google.cloud import speech_v2 as cloud_speech
from google.cloud.speech_v2 import SpeechClient

from backend.pipeline.common.constants import BYTES_PER_SECOND_16KHZ_MONO
from backend.pipeline.transcription.constants import (
    DEFAULT_MAX_RETRIES,
    DEFAULT_RETRY_MAX_SECONDS,
)
from backend.pipeline.transcription.enums import TranscriberType
from backend.pipeline.transcription.utils import ConfigBase

logger = logging.getLogger(__name__)


class Transcriber(abc.ABC):
    """Abstract interface for handling audio transcription.

    Allows hot-swapping different external transcription APIs or models.
    """

    @abc.abstractmethod
    def setup(self) -> None:
        """Called once per Beam worker upon initialization.

        Use this to spin up clients, establish connections, or load models
        that cannot be pickled/serialized across processes.
        """

    @abc.abstractmethod
    def transcribe(
        self,
        *,
        audio_data: bytes,
    ) -> str | None:
        """Transcribes the raw audio bytes and returns the text transcript."""


class KeywordItem(pydantic.BaseModel):
    """A single keyword/phrase entry with an optional per-phrase boost override."""

    phrase: str
    boost: float | None = None


class ChirpConfig(ConfigBase):
    """Strongly typed configuration for the Google Chirp V3 Transcriber."""

    location: str = "us-central1"
    recognizer: str = "_"
    model: str = "chirp_3"
    language_codes: list[str] = ["en-US"]
    enable_automatic_punctuation: bool = True
    enable_word_time_offsets: bool = False
    # Path to a JSON file containing KeywordItem entries. Each entry may specify
    # its own boost; entries without one fall back to phrase_boost. Optional —
    # if omitted no file is loaded.
    keywords_file_path: str | None = None
    # Inline phrase hints as a convenience alternative (or complement) to a
    # keywords file. Useful for per-job overrides without a container rebuild.
    phrase_hints: list[str] = []
    # Default boost applied to inline phrase_hints and to any KeywordItem that
    # does not specify its own boost value.
    phrase_boost: float = 10.0


class GoogleChirpV3Transcriber(Transcriber):
    """Transcriber implementation using Google Cloud Speech-to-Text V2 API
    with the 'chirp_3' model.
    """

    def __init__(
        self,
        project_id: str,
        config: ChirpConfig,
    ) -> None:
        """Binds the GCP Project ID and parsed Chirp configuration."""
        self.project_id = project_id
        self.config = config

        self.client: SpeechClient | None = None
        self.keywords_list: list[KeywordItem] = []

    def _init_client(self) -> SpeechClient:
        opts = client_options.ClientOptions(
            api_endpoint=f"{self.config.location}-speech.googleapis.com"
        )
        return SpeechClient(client_options=opts)

    def setup(self) -> None:
        """Instantiates the Speech-to-Text API gRPC client and loads keywords if configured."""
        self.client = self._init_client()

        if self.config.keywords_file_path:
            p = pathlib.Path(self.config.keywords_file_path)
            if not p.exists():
                msg = f"Keywords file {self.config.keywords_file_path} does not exist"
                raise FileNotFoundError(msg)
            try:
                with p.open("r") as f:
                    self.keywords_list = pydantic.TypeAdapter(
                        list[KeywordItem]
                    ).validate_json(f.read())
                    logger.info(
                        "Loaded %d keywords from %s",
                        len(self.keywords_list),
                        self.config.keywords_file_path,
                    )
            except pydantic.ValidationError as e:
                msg = f"Failed to parse keywords file {self.config.keywords_file_path}: {e}"
                raise ValueError(msg) from e

    def _build_adaptation(self) -> cloud_speech.SpeechAdaptation | None:
        """Builds a SpeechAdaptation combining file-loaded keywords and inline phrase hints.

        File-loaded KeywordItems may specify a per-phrase boost; those without
        one fall back to phrase_boost. Inline phrase_hints always use phrase_boost.
        Returns None when no keywords or hints are configured.
        """
        phrases = [
            cloud_speech.PhraseSet.Phrase(
                value=kw.phrase,
                boost=kw.boost if kw.boost is not None else self.config.phrase_boost,
            )
            for kw in self.keywords_list
            if kw.phrase
        ] + [
            cloud_speech.PhraseSet.Phrase(
                value=hint,
                boost=self.config.phrase_boost,
            )
            for hint in self.config.phrase_hints
        ]

        if not phrases:
            return None

        return cloud_speech.SpeechAdaptation(
            phrase_sets=[
                cloud_speech.SpeechAdaptation.AdaptationPhraseSet(
                    inline_phrase_set=cloud_speech.PhraseSet(phrases=phrases)
                )
            ]
        )

    @tenacity.retry(
        wait=tenacity.wait_exponential(
            multiplier=1, max=DEFAULT_RETRY_MAX_SECONDS
        ),
        stop=tenacity.stop_after_attempt(DEFAULT_MAX_RETRIES),
        retry=tenacity.retry_if_exception_type((GoogleAPIError, RetryError)),
        reraise=True,
    )
    def transcribe(
        self,
        *,
        audio_data: bytes,
    ) -> str | None:
        """Transcribes the given audio payload."""
        if not self.client:
            msg = "Transcriber client used before setup() was called."
            raise RuntimeError(msg)

        duration_sec = len(audio_data) / BYTES_PER_SECOND_16KHZ_MONO

        logger.info(
            "Transcribing %.3fs of audio",
            duration_sec,
        )

        request = cloud_speech.RecognizeRequest(
            recognizer=cloud_speech.SpeechClient.recognizer_path(
                self.project_id, self.config.location, self.config.recognizer
            ),
            config=cloud_speech.RecognitionConfig(
                auto_decoding_config=cloud_speech.AutoDetectDecodingConfig(),
                model=self.config.model,
                language_codes=self.config.language_codes,
                adaptation=self._build_adaptation(),
                features=cloud_speech.RecognitionFeatures(
                    enable_automatic_punctuation=self.config.enable_automatic_punctuation,
                    enable_word_time_offsets=self.config.enable_word_time_offsets,
                ),
            ),
            content=audio_data,
        )

        response = self.client.recognize(request=request)
        return self._parse_response(response)

    def _parse_response(
        self,
        response: cloud_speech.RecognizeResponse,
    ) -> str | None:
        """Extracts and joins transcript text from a RecognizeResponse.

        Strips Chirp v3's [BACKGROUND] marker (emitted when no speech is
        detected) and returns None if no meaningful text remains.
        """
        chunks = []
        for result in response.results:
            if not result.alternatives:
                continue

            chunk_text = (
                result.alternatives[0]
                .transcript.replace("[BACKGROUND]", "")
                .strip()
            )
            if chunk_text:
                chunks.append(chunk_text)

        transcript = " ".join(chunks).strip()

        if not transcript:
            logger.info(
                "Transcription returned [BACKGROUND] only or was completely empty (no discernable speech)."
            )
            return None

        return transcript


def get_transcriber(
    transcriber_type: TranscriberType,
    project_id: str,
    config_json: str,
) -> Transcriber:
    """A factory method instantiating the requested Transcriber implementation based on the enum type."""
    if transcriber_type == TranscriberType.GOOGLE_CHIRP_V3:
        return GoogleChirpV3Transcriber(project_id, ChirpConfig.from_json(config_json))
    msg = f"Unknown transcriber type: {transcriber_type}"
    raise ValueError(msg)
