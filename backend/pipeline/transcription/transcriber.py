import logging
import os
from abc import ABC, abstractmethod

import google.cloud.logging
from google import genai
from google.genai import types
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

if not os.environ.get("LOCAL_DEV"):
    client = google.cloud.logging.Client()
    client.setup_logging()
else:
    # If local, just print normally to the Docker console
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    logger.addHandler(handler)
    logger.info("Running in LOCAL_DEV mode. Logs will print here.")

SYSTEM_PROMPT = (
    "You are a universal Emergency Dispatch Logger. "
    "Identify speakers by analyzing their acoustic profile and radio protocol.\n\n"
    "UNIVERSAL RULES:\n"
    "1. Calling Order: The second name mentioned is usually the caller. "
    "   Example: 'Engine 5, Base' -> Unit: Dispatch; 'Base, Engine 5' -> "
    "Unit: Engine 5.\n"
    "2. Acoustic Profiles: 'Dispatch' is usually clear and quiet. "
    "'Units' have background noise/static.\n"
    "3. Role Mapping:\n"
    "   - is_dispatch=True: Base, Metro, Control, Center, Station, KMA.\n"
    "   - is_dispatch=False: Engines, Rescues, Trucks, Battalions, Units, Medics.\n\n"
    "FEW-SHOT EXAMPLES:\n"
    "- 'Control, Engine 12 on scene' -> Unit: 'Engine 12', is_dispatch: false\n"
    "- 'Engine 12, Control copy' -> Unit: 'Dispatch', is_dispatch: true\n"
    "- 'Truck 4, respond to the medical' -> Unit: 'Dispatch', is_dispatch: true"
)


class DispatchEvent(BaseModel):
    unit: str = Field(description="The call sign or unit ID, e.g., 'Engine 122'")
    message: str = Field(description="The exact words spoken")
    is_dispatch: bool = Field(
        description="True if the speaker is the base/metro station"
    )


class DispatchLog(BaseModel):
    events: list[DispatchEvent]


class BaseTranscriber(ABC):
    """Abstract base class for all transcriber implementations."""

    @abstractmethod
    def transcribe(self, wav_data: bytes) -> str | None:
        """Transcribe audio data and return a JSON string or None on failure."""


class GeminiTranscriber(BaseTranscriber):
    """Gemini-based implementation of audio transcription."""

    def __init__(self, api_key: str | None) -> None:
        self.api_key = api_key
        self._client = None
        if api_key:
            self._client = genai.Client(api_key=api_key)

    def transcribe(self, wav_data: bytes) -> str | None:
        """Transcribe audio using Gemini AI."""
        if self._client is None:
            logger.warning("No Gemini API key provided")
            return None
        model = os.getenv("GEMINI_MODEL", "gemini-3-flash-preview")
        try:
            response = self._client.models.generate_content(
                model=model,
                config=types.GenerateContentConfig(
                    system_instruction=SYSTEM_PROMPT,
                    response_mime_type="application/json",
                    response_schema=DispatchLog,
                    # Thinking config enables Chain-of-Thought reasoning for complex logic (like speaker ID).
                    # Levels: LOW (faster/coarse), MEDIUM (balanced), HIGH (deep/slower).
                    # Documentation: https://ai.google.dev/gemini-api/docs/thinking#thinking-levels
                    # MEDIUM as it provides a solid balance for the reasoning required to identify speakers
                    # and roles from radio protocol without excessively increasing latency.
                    thinking_config=types.ThinkingConfig(thinking_level=types.ThinkingLevel.MEDIUM),
                    temperature=0.0,
                    candidate_count=1,
                ),
                contents=[types.Part.from_bytes(data=wav_data, mime_type="audio/wav")],
            )
        except Exception:
            logger.exception("Error calling gemini")
            return None
        else:
            return response.text
