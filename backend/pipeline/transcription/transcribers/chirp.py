"""Google Cloud Speech-to-Text Chirp V3 transcriber implementation."""

import pathlib

import pydantic
from google.api_core import client_options
from google.api_core.retry import Retry
from google.cloud import speech_v2 as cloud_speech
from google.cloud.speech_v2 import SpeechClient

from backend.pipeline.common.logging import get_task_logger
from backend.pipeline.common.utils import ConfigBase
from backend.pipeline.transcription.transcribers.base import Transcriber

# Default paths to the packaged prompt and phrase hints configuration text assets.
DEFAULT_PHRASE_HINTS_FILE_PATH = (
    "/app/backend/pipeline/transcription/transcribers/chirp_phrase_hints.txt"
)
DEFAULT_CHIRP_PROMPT_FILE_PATH = (
    "/app/backend/pipeline/transcription/transcribers/chirp_prompt.txt"
)

# Marker defined in the prompt to indicate when the model detects no intelligible speech.
CHIRP_UNINTELLIGIBLE_MARKER = "[UNINTELLIGIBLE]"

# Chirp model configuration defaults
DEFAULT_CHIRP_LOCATION = "us"
DEFAULT_CHIRP_RECOGNIZER = "_"
DEFAULT_CHIRP_MODEL = "chirp_3"
DEFAULT_CHIRP_LANGUAGE_CODES = ["en-US"]

# API limits and retry defaults
MAX_SYNCHRONOUS_TRANSCRIPTION_DURATION_MS = 60000
DEFAULT_MAX_RETRIES = 5
DEFAULT_RETRY_MAX_SECONDS = 10

logger = get_task_logger(
    __name__, {"system": "transcription", "component": "chirp"}
)


def load_default_phrase_hints() -> list[str]:
    """Loads phrase hints from the default packaged text file asset if it exists."""
    p = pathlib.Path(DEFAULT_PHRASE_HINTS_FILE_PATH)
    if not p.exists():
        return []
    with p.open("r") as f:
        return [
            line.strip()
            for line in f
            if line.strip() and not line.startswith("#")
        ]


def load_default_prompt() -> str | None:
    """Loads the custom transcription prompt from the default packaged text file asset if it exists."""
    p = pathlib.Path(DEFAULT_CHIRP_PROMPT_FILE_PATH)
    return p.read_text() if p.exists() else None


class ChirpConfig(ConfigBase):
    """Strongly typed configuration for the Google Chirp V3 Transcriber."""

    location: str = DEFAULT_CHIRP_LOCATION
    recognizer: str = DEFAULT_CHIRP_RECOGNIZER
    model: str = DEFAULT_CHIRP_MODEL
    language_codes: list[str] = DEFAULT_CHIRP_LANGUAGE_CODES
    enable_automatic_punctuation: bool = True
    enable_word_time_offsets: bool = False
    enable_denoiser: bool = True

    # Custom transcription prompt content. Defaults to the packaged custom prompt file.
    custom_prompt: str | None = pydantic.Field(
        default_factory=load_default_prompt
    )

    # List of phrase hints for Chirp adaptation. Defaults to the packaged phrase hints file.
    phrase_hints: list[str] = pydantic.Field(
        default_factory=load_default_phrase_hints
    )


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

    def _init_client(self) -> SpeechClient:
        opts = client_options.ClientOptions(
            api_endpoint=f"{self.config.location}-speech.googleapis.com"
        )
        return SpeechClient(client_options=opts)

    def setup(self) -> None:
        """Instantiates the Speech-to-Text API gRPC client."""
        self.client = self._init_client()

    def _build_adaptation(self) -> cloud_speech.SpeechAdaptation | None:
        """Builds a SpeechAdaptation from the configured phrase hints."""
        if not self.config.phrase_hints:
            return None

        phrases = [
            cloud_speech.PhraseSet.Phrase(value=phrase)
            for phrase in self.config.phrase_hints
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
        audio_data: bytes | None = None,
        uri: str | None = None,
        duration_ms: int,
    ) -> str | None:
        """Transcribes the given audio payload using GCP Speech V2 API."""
        if not self.client:
            msg = "Transcriber client used before setup() was called."
            raise RuntimeError(msg)

        if duration_ms > MAX_SYNCHRONOUS_TRANSCRIPTION_DURATION_MS:
            msg = f"Audio payload too long for synchronous API: {duration_ms / 1000:.2f}s"
            raise ValueError(msg)

        logger.info(
            "Transcribing %.3fs of audio %s",
            duration_ms / 1000,
            f"from GCS URI: {uri}" if uri else "from in-memory bytes",
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
                    custom_prompt_config=cloud_speech.CustomPromptConfig(
                        custom_prompt=self.config.custom_prompt
                    )
                    if self.config.custom_prompt
                    else None,
                ),
                denoiser_config=cloud_speech.DenoiserConfig(
                    denoise_audio=self.config.enable_denoiser
                ),
            ),
            content=audio_data,
            uri=uri,
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

            chunk_text = result.alternatives[0].transcript.strip()
            if chunk_text:
                chunks.append(chunk_text)

        transcript = " ".join(chunks).strip()

        if not transcript:
            logger.info(
                "Transcription returned [UNINTELLIGIBLE] only or was completely empty (no discernable speech)."
            )
            return None

        return transcript
