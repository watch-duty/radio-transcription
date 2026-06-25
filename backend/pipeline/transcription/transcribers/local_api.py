import asyncio
import logging
import os
import time

import requests

from backend.pipeline.transcription.transcribers.base import Transcriber

logger = logging.getLogger(__name__)


class LocalApiTranscriber(Transcriber):
    """Transcriber that calls a local HTTP API for transcription."""

    def __init__(self, api_url: str | None = None) -> None:
        self.api_url = api_url or os.environ.get("LOCAL_ASR_API_URL")

    def setup(self) -> None:
        if not self.api_url:
            msg = "LocalApiTranscriber setup failed: LOCAL_ASR_API_URL environment variable is not set."
            raise ValueError(msg)

        logger.info(f"LocalApiTranscriber setup pointing to {self.api_url}")
        health_url = self.api_url.replace("/transcribe", "/health")

        max_retries = 5
        for attempt in range(1, max_retries + 1):
            try:
                resp = requests.get(health_url, timeout=2)
                if resp.status_code == 200:
                    logger.info(
                        "Successfully connected to local asr API health check."
                    )
                    return
                logger.warning(
                    f"Local asr API health check returned {resp.status_code} (attempt {attempt}/{max_retries})"
                )
            except Exception as e:
                logger.warning(
                    f"Could not connect to local asr API (attempt {attempt}/{max_retries}): {e}"
                )

            if attempt < max_retries:
                time.sleep(1)

        msg = f"Failed to connect to local asr API health check at {health_url} after {max_retries} attempts."
        raise RuntimeError(msg)

    async def transcribe(
        self,
        *,
        audio_data: bytes | None = None,
        uri: str | None = None,
        duration_ms: int,
    ) -> str | None:
        def _call_api() -> requests.Response | None:
            if uri:
                logger.info(f"Calling local whisper API with URI: {uri}")
                params = {"uri": uri}
                return requests.post(self.api_url, params=params, timeout=60)
            if audio_data:
                logger.info("Calling local whisper API with raw audio data")
                files = {"file": ("audio.flac", audio_data, "audio/flac")}
                return requests.post(self.api_url, files=files, timeout=60)
            logger.warning(
                "No audio_data or uri provided to LocalApiTranscriber."
            )
            return None

        resp = await asyncio.to_thread(_call_api)
        if resp is None:
            return None

        try:
            resp.raise_for_status()
        except requests.HTTPError:
            logger.exception(
                f"Local whisper API returned error {resp.status_code}: {resp.text}"
            )
            raise

        try:
            result = resp.json()
        except Exception:
            logger.exception(
                "Failed to parse JSON response from local whisper API"
            )
            raise

        text = result.get("text")
        if text is not None:
            text = text.strip()
            if not text:
                return None
        return text
