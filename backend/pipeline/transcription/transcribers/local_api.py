import requests
import logging
from backend.pipeline.transcription.transcribers.base import Transcriber

logger = logging.getLogger(__name__)

class LocalApiTranscriber(Transcriber):
    """Transcriber that calls a local HTTP API for transcription."""

    def __init__(self, api_url: str = "http://local-whisper:8095/transcribe"):
        self.api_url = api_url

    def setup(self) -> None:
        logger.info(f"LocalApiTranscriber setup pointing to {self.api_url}")
        try:
            health_url = self.api_url.replace("/transcribe", "/health")
            resp = requests.get(health_url, timeout=2)
            if resp.status_code == 200:
                logger.info("Successfully connected to local whisper API health check.")
            else:
                logger.warning(f"Local whisper API health check returned {resp.status_code}")
        except Exception as e:
            logger.warning(f"Could not connect to local whisper API during setup: {e}")

    def transcribe(
        self,
        *,
        audio_data: bytes | None = None,
        uri: str | None = None,
        duration_ms: int,
    ) -> str | None:
        try:
            if uri:
                # Prefer sending URI if available, let the API download it
                logger.info(f"Calling local whisper API with URI: {uri}")
                params = {"uri": uri}
                resp = requests.post(self.api_url, params=params, timeout=60)
            elif audio_data:
                logger.info("Calling local whisper API with raw audio data")
                files = {"file": ("audio.flac", audio_data, "audio/flac")}
                resp = requests.post(self.api_url, files=files, timeout=60)
            else:
                logger.warning("No audio_data or uri provided to LocalApiTranscriber.")
                return None
                
            if resp.status_code == 200:
                result = resp.json()
                return result.get("text")
            else:
                logger.error(f"Local whisper API returned error {resp.status_code}: {resp.text}")
                return None
        except Exception as e:
            logger.error(f"Error calling local whisper API: {e}")
            return None
