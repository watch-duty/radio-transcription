"""Centralized worker-node singleton resource registry for Apache Beam state management."""

import logging
import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from apache_beam.utils.shared import Shared

from backend.pipeline.transcription.enums import TranscriberType, VadType
from backend.pipeline.transcription.transcribers import Transcriber
from backend.pipeline.transcription.vads import VoiceActivityDetector

logger = logging.getLogger(__name__)

# The unified process-level token for Beam garbage collection pooling
SHARED_RESOURCE_HANDLE = Shared()


@dataclass
class SharedResources:
    """A strictly singleton dataclass mapping heavyweight machine learning and API clients.

    Wrapped uniquely via `apache_beam.utils.shared.Shared`, this container ensures that expensive
    machine learning models (like TenVAD) are loaded into memory exactly once per worker machine,
    and HTTP/GRPC API connections (GCS, Google Speech) are persistently pooled and reused. This
    eliminates the latency and CPU overhead of repeatedly initializing heavy resources across bundles.
    """

    vad: VoiceActivityDetector | None = None
    gcs_client: Any | None = None
    transcriber: Transcriber | None = None

    _vad_lock: threading.Lock = field(default_factory=threading.Lock)
    _gcs_lock: threading.Lock = field(default_factory=threading.Lock)
    _transcriber_lock: threading.Lock = field(default_factory=threading.Lock)

    def get_vad(
        self,
        factory: Callable[[VadType, str], VoiceActivityDetector],
        vad_type: VadType,
        config_json: str,
    ) -> VoiceActivityDetector:
        """Lazily initialize and return the VAD plugin object."""
        if self.vad is None:
            with self._vad_lock:
                if self.vad is None:
                    self.vad = factory(vad_type, config_json)
        return self.vad

    def get_gcs(self, factory: Callable[[], Any]) -> Any:
        """Lazily initialize and return the Google Cloud Storage client."""
        if self.gcs_client is None:
            with self._gcs_lock:
                if self.gcs_client is None:
                    self.gcs_client = factory()
        return self.gcs_client

    def get_transcriber(
        self,
        factory: Callable[[TranscriberType, str, str], Transcriber],
        transcriber_type: TranscriberType,
        project_id: str,
        config_json: str,
    ) -> Transcriber:
        """Lazily initialize and return the Transcriber instance."""
        if self.transcriber is None:
            with self._transcriber_lock:
                if self.transcriber is None:
                    self.transcriber = factory(
                        transcriber_type,
                        project_id,
                        config_json,
                    )
                    # Invoke the underlying engine's setup logic precisely once organically
                    self.transcriber.setup()
        return self.transcriber
