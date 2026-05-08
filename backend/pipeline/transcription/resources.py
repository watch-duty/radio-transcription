import threading
from collections.abc import Callable
from dataclasses import dataclass, field

from apache_beam.utils.shared import Shared
from google.cloud import storage

from backend.pipeline.transcription.audio import vad
from backend.pipeline.transcription.common import logging
from backend.pipeline.transcription.services import transcribers

logger = logging.get_logger(
    __name__, {"system": "transcription", "component": "resources"}
)

# The unified process-level token for Beam garbage collection pooling
SHARED_RESOURCE_HANDLE = Shared()


@dataclass
class SharedResources:
    """A strictly singleton dataclass mapping ML and API clients.

    Wrapped uniquely via `apache_beam.utils.shared.Shared`, this
    container ensures that models are loaded into memory exactly once
    per worker machine, and HTTP/GRPC API connections (GCS, Google
    Speech) are persistently pooled and reused. This eliminates the
    latency and CPU overhead of repeatedly initializing heavy
    resources across bundles.
    """

    vad_engine: vad.VoiceActivityDetector | None = None
    gcs_client: storage.Client | None = None
    transcriber_client: transcribers.Transcriber | None = None

    _vad_lock: threading.Lock = field(default_factory=threading.Lock)
    _gcs_lock: threading.Lock = field(default_factory=threading.Lock)
    _transcriber_lock: threading.Lock = field(default_factory=threading.Lock)

    def get_vad(
        self,
        factory: Callable[[str], vad.VoiceActivityDetector],
        config_json: str,
    ) -> vad.VoiceActivityDetector:
        """Lazily initialize and return the VoiceActivityDetector engine."""
        if self.vad_engine is None:
            with self._vad_lock:
                if self.vad_engine is None:
                    self.vad_engine = factory(config_json)
        return self.vad_engine

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
            [type[transcribers.Transcriber], str, str],
            transcribers.Transcriber,
        ],
        transcriber_type: type[transcribers.Transcriber],
        project_id: str,
        config_json: str,
    ) -> transcribers.Transcriber:
        """Lazily initialize and return the Transcriber instance."""
        if self.transcriber_client is None:
            with self._transcriber_lock:
                if self.transcriber_client is None:
                    self.transcriber_client = factory(
                        transcriber_type,
                        project_id,
                        config_json,
                    )
        return self.transcriber_client
