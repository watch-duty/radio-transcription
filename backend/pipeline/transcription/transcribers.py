"""Pluggable Transcription API Architecture.

This module defines the abstract interface for audio transcription services,
allowing the Beam pipeline to dynamically swap between different engines
(e.g., Google Cloud Speech-to-Text, Whisper, Custom Models) via configuration.
"""

import abc
import logging
import pathlib

import pydantic
from google.api_core import client_options
from google.api_core.retry import Retry
from google.cloud import speech_v2 as cloud_speech
from google.cloud.speech_v2 import SpeechClient

from backend.pipeline.common.constants import BYTES_PER_SECOND_16KHZ_MONO
from backend.pipeline.transcription.constants import (
    DEFAULT_CHIRP_LANGUAGE_CODES,
    DEFAULT_CHIRP_LOCATION,
    DEFAULT_CHIRP_MODEL,
    DEFAULT_CHIRP_RECOGNIZER,
    DEFAULT_KEYWORDS_FILE_PATH,
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
    """A single keyword/phrase entry loaded from the keywords JSON file."""

    phrase: str
    boost: float | None = None


class ChirpConfig(ConfigBase):
    """Strongly typed configuration for the Google Chirp V3 Transcriber."""

    location: str = DEFAULT_CHIRP_LOCATION
    recognizer: str = DEFAULT_CHIRP_RECOGNIZER
    model: str = DEFAULT_CHIRP_MODEL
    language_codes: list[str] = DEFAULT_CHIRP_LANGUAGE_CODES
    enable_automatic_punctuation: bool = True
    enable_word_time_offsets: bool = False
    enable_denoiser: bool = True
    # Path to a JSON file containing KeywordItem entries for phrase adaptation.
    # Defaults to the packaged file in the container image; explicitly set to
    # None to disable adaptation (e.g. in tests or non-container environments).
    keywords_file_path: str | None = DEFAULT_KEYWORDS_FILE_PATH


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
        """Builds a SpeechAdaptation from the loaded keywords list.

        Returns None when no keywords are loaded, skipping adaptation entirely.
        """
        if not self.keywords_list:
            return None

        phrases = [
            cloud_speech.PhraseSet.Phrase(value=kw.phrase, boost=kw.boost)
            for kw in self.keywords_list
            if kw.phrase
        ]

        return cloud_speech.SpeechAdaptation(
            phrase_sets=[
                cloud_speech.SpeechAdaptation.AdaptationPhraseSet(
                    inline_phrase_set=cloud_speech.PhraseSet(phrases=phrases)
                )
            ]
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
                denoiser_config=cloud_speech.DenoiserConfig(
                    denoise_audio=self.config.enable_denoiser
                ),
            ),
            content=audio_data,
        )

        retry_policy = Retry(
            initial=1.0,
            maximum=float(DEFAULT_RETRY_MAX_SECONDS),
            multiplier=2.0,
            deadline=float(DEFAULT_RETRY_MAX_SECONDS * DEFAULT_MAX_RETRIES),
        )
        response = self.client.recognize(request=request, retry=retry_policy)
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
        return GoogleChirpV3Transcriber(
            project_id, ChirpConfig.from_json(config_json)
        )
    msg = f"Unknown transcriber type: {transcriber_type}"
    raise ValueError(msg)
