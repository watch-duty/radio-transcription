"""Centralized worker-node singleton resource registry for Apache Beam state management."""

import logging
import threading
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from apache_beam.utils.shared import Shared

from backend.pipeline.transcription.enums import TranscriberType
from backend.pipeline.transcription.transcribers import Transcriber

logger = logging.getLogger(__name__)

# The unified process-level token for Beam garbage collection pooling
SHARED_RESOURCE_HANDLE = Shared()


@dataclass
class SharedResources:
    """A strictly singleton dataclass mapping heavyweight machine learning and API clients.

    Wrapped uniquely via `apache_beam.utils.shared.Shared`, this container ensures that expensive
    machine learning models (like TenVAD or Sherpa-ONNX) are loaded into memory exactly once per worker machine,
    and HTTP/GRPC API connections (GCS, Google Speech) are persistently pooled and reused. This
    eliminates the latency and CPU overhead of repeatedly initializing heavy resources across bundles.
    """

    _vads: OrderedDict[str, Any] = field(default_factory=OrderedDict)
    _denoisers: OrderedDict[str, Any] = field(default_factory=OrderedDict)
    gcs_client: Any | None = None
    transcriber: Transcriber | None = None

    max_cache_size: int = 20

    _vad_lock: threading.Lock = field(default_factory=threading.Lock)
    _denoiser_lock: threading.Lock = field(default_factory=threading.Lock)
    _gcs_lock: threading.Lock = field(default_factory=threading.Lock)
    _transcriber_lock: threading.Lock = field(default_factory=threading.Lock)

    def get_vad(
        self,
        session_id: str,
        factory: Callable[[str], Any],
        config_json: str,
    ) -> Any:
        """Lazily initialize and return the VAD object for a session using LRU cache."""
        with self._vad_lock:
            if session_id in self._vads:
                self._vads.move_to_end(session_id)
                return self._vads[session_id]

            vad = factory(config_json)
            self._vads[session_id] = vad
            if len(self._vads) > self.max_cache_size:
                self._vads.popitem(last=False)
            return vad

    def get_denoiser(
        self,
        session_id: str,
        factory: Callable[[], Any],
    ) -> Any:
        """Lazily initialize and return the Speech Denoiser object for a session using LRU cache."""
        with self._denoiser_lock:
            if session_id in self._denoisers:
                self._denoisers.move_to_end(session_id)
                return self._denoisers[session_id]

            denoiser = factory()
            self._denoisers[session_id] = denoiser
            if len(self._denoisers) > self.max_cache_size:
                self._denoisers.popitem(last=False)
            return denoiser

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
