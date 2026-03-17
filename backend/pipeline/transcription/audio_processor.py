import io
import logging
import urllib.parse
from typing import Any

from pydub import AudioSegment, effects

from backend.pipeline.transcription.constants import (
    HIGHPASS_FILTER_FREQ,
    LOWPASS_FILTER_FREQ,
)
from backend.pipeline.shared_constants import (
    AUDIO_FORMAT,
    NUM_AUDIO_CHANNELS,
    SAMPLE_RATE_HZ,
)
from backend.pipeline.transcription.datatypes import AudioChunkData
from backend.pipeline.transcription.enums import VadType
from backend.pipeline.transcription.metadata import (
    get_gcs_client,
    read_sed_segments_from_blob,
)
from backend.pipeline.transcription.vads import VoiceActivityDetector, get_vad_plugin

logger = logging.getLogger(__name__)


class AudioProcessor:
    """
    An acoustic manipulation module.
    Responsible for downloading/parsing audio, applying VAD, and applying bandpass filters.
    While mostly stateless, it holds per-worker initialized state (`self.vad`) from `setup()`.
    All streaming orchestration and API integrations are handled upstream.
    """

    def __init__(
        self, vad_type: VadType = VadType.TEN_VAD, vad_config: str = "{}"
    ) -> None:
        self.vad_type = vad_type
        self.vad_config = vad_config
        self.vad: VoiceActivityDetector | None = None
        self.gcs_client: Any | None = None

    def setup(self) -> None:
        """Initializes the VAD plugin and GCS client once per worker."""
        self.vad = get_vad_plugin(self.vad_type, self.vad_config)
        self.gcs_client = get_gcs_client()

    def download_audio_and_sed(self, gcs_path: str) -> AudioChunkData:
        """Downloads FLAC bytes and SED metadata from GCS, returning an AudioChunkData."""
        if not self.gcs_client:
            msg = "GCS client not initialized. Call setup() first."
            raise RuntimeError(msg)

        parsed_uri = urllib.parse.urlparse(gcs_path)
        bucket_name = parsed_uri.netloc
        blob_name = parsed_uri.path.lstrip("/")

        blob = self.gcs_client.bucket(bucket_name).get_blob(blob_name)
        if not blob:
            err_msg = f"GCS object not found: {gcs_path}"
            logger.error(err_msg)
            raise FileNotFoundError(err_msg)

        in_mem_file = io.BytesIO()
        blob.download_to_file(in_mem_file)
        in_mem_file.seek(0)

        full_audio_segment = AudioSegment.from_file(in_mem_file, format=AUDIO_FORMAT)

        file_start_ms, speech_segments = read_sed_segments_from_blob(blob)

        return AudioChunkData(
            start_ms=file_start_ms,
            audio=full_audio_segment,
            speech_segments=speech_segments,
            gcs_uri=gcs_path,
        )

    def check_vad(self, audio_buffer: AudioSegment) -> bool:
        """Evaluates audio buffer with TenVAD and returns True if speech is detected."""
        if self.vad is None:
            msg = "VAD plugin not initialized. Call setup() first."
            raise RuntimeError(msg)

        audio_16k = audio_buffer.set_frame_rate(SAMPLE_RATE_HZ).set_channels(
            NUM_AUDIO_CHANNELS
        )
        pcm_bytes = audio_16k.raw_data

        return self.vad.evaluate(pcm_bytes, sample_rate=SAMPLE_RATE_HZ)

    def preprocess_audio(self, audio_buffer: AudioSegment) -> AudioSegment:
        """Applies native bandpass filtering to remove rumble and static."""
        audio = effects.high_pass_filter(audio_buffer, HIGHPASS_FILTER_FREQ)
        return effects.low_pass_filter(audio, LOWPASS_FILTER_FREQ)

    def export_flac(self, audio_buffer: AudioSegment) -> bytes:
        """Exports an AudioSegment to FLAC bytes."""
        buf = io.BytesIO()
        audio_buffer.export(buf, format=AUDIO_FORMAT)
        return buf.getvalue()
