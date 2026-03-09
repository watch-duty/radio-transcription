import io
import logging
import urllib.parse

from constants import (
    AUDIO_CHANNELS,
    AUDIO_FORMAT,
    SAMPLE_RATE_HZ,
)
from enums import VadType
from pydub import AudioSegment, effects
from utils import get_gcs_client, read_sad_segments_from_gcs
from vads import VoiceActivityDetector, get_vad_plugin

logger = logging.getLogger(__name__)


class AudioProcessor:
    """
    A purely stateless acoustic manipulation module.
    Responsible for downloading/parsing audio, applying VAD, and applying bandpass filters.
    All streaming orchestration, state management, and API integrations are handled upstream.
    """

    def __init__(
        self, vad_type: VadType = VadType.TEN_VAD, vad_config: str = "{}"
    ) -> None:
        self.vad_type = vad_type
        self.vad_config = vad_config
        self.vad: VoiceActivityDetector | None = None

    def setup(self) -> None:
        """Initializes the VAD plugin once per worker."""
        self.vad = get_vad_plugin(self.vad_type, self.vad_config)

    def download_audio_and_sad(
        self, gcs_path: str
    ) -> tuple[AudioSegment, list[tuple[float, float]]]:
        """Downloads FLAC bytes and SAD metadata from GCS, returning a parsed AudioSegment."""
        gcs_client = get_gcs_client()
        parsed_uri = urllib.parse.urlparse(gcs_path)
        bucket_name = parsed_uri.netloc
        blob_name = parsed_uri.path.lstrip("/")

        blob = gcs_client.bucket(bucket_name).get_blob(blob_name)
        if not blob:
            err_msg = f"GCS object not found: {gcs_path}"
            logger.error(err_msg)
            raise FileNotFoundError(err_msg)

        in_mem_file = io.BytesIO()
        blob.download_to_file(in_mem_file)
        in_mem_file.seek(0)

        full_audio_segment = AudioSegment.from_file(in_mem_file, format=AUDIO_FORMAT)

        sad_path = gcs_path.replace(f".{AUDIO_FORMAT}", ".sad.pb")
        speech_segments = read_sad_segments_from_gcs(sad_path)

        return full_audio_segment, speech_segments

    def check_vad(self, audio_buffer: AudioSegment) -> bool:
        """Evaluates audio buffer with TenVAD and returns True if speech is detected."""
        if not self.vad:
            msg = "VAD plugin not initialized. Call setup() first."
            raise RuntimeError(msg)

        audio_16k = audio_buffer.set_frame_rate(SAMPLE_RATE_HZ).set_channels(
            AUDIO_CHANNELS
        )
        pcm_bytes = audio_16k.raw_data

        return self.vad.evaluate(pcm_bytes, sample_rate=SAMPLE_RATE_HZ)

    def preprocess_audio(self, audio_buffer: AudioSegment) -> AudioSegment:
        """Applies native 300Hz-3000Hz bandpass filtering to remove rumble and static."""
        audio = effects.high_pass_filter(audio_buffer, 300)
        return effects.low_pass_filter(audio, 3000)

    def export_flac(self, audio_buffer: AudioSegment) -> bytes:
        """Exports an AudioSegment to FLAC bytes."""
        buf = io.BytesIO()
        audio_buffer.export(buf, format=AUDIO_FORMAT)
        return buf.getvalue()
