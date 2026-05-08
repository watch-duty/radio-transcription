"""Centralized worker-node singleton resource registry for Apache Beam state management."""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from apache_beam.utils.shared import Shared

if TYPE_CHECKING:
    from collections.abc import Callable

    from google.cloud import storage

    from backend.pipeline.transcription.audio import vad as vad_module
    from backend.pipeline.transcription.services import transcribers

from backend.pipeline.transcription.common import enums, logging

logger = logging.get_logger(
    __name__, {"system": "transcription", "component": "resources"}
)

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

    vad: vad_module.VoiceActivityDetector | None = None
    gcs_client: storage.Client | None = None
    transcriber: transcribers.Transcriber | None = None

    _vad_lock: threading.Lock = field(default_factory=threading.Lock)
    _gcs_lock: threading.Lock = field(default_factory=threading.Lock)
    _transcriber_lock: threading.Lock = field(default_factory=threading.Lock)

    def get_vad(
        self,
        factory: Callable[[str], vad_module.VoiceActivityDetector],
        config_json: str,
    ) -> vad_module.VoiceActivityDetector:
        """Lazily initialize and return the VoiceActivityDetector engine."""
        if self.vad is None:
            with self._vad_lock:
                if self.vad is None:
                    self.vad = factory(config_json)
        return self.vad

    def get_gcs(self, factory: Callable[[], storage.Client]) -> storage.Client:
        """Lazily initialize and return the Google Cloud Storage client."""
        if self.gcs_client is None:
            with self._gcs_lock:
                if self.gcs_client is None:
                    self.gcs_client = factory()
        return self.gcs_client

    def get_transcriber(
        self,
        factory: Callable[
            [enums.TranscriberType, str, str], transcribers.Transcriber
        ],
        transcriber_type: enums.TranscriberType,
        project_id: str,
        config_json: str,
    ) -> transcribers.Transcriber:
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
