"""Constants shared across the radio transcription pipeline."""

# Audio processing shared constants
CHUNK_DURATION_SECONDS = 15
SAMPLE_RATE_HZ = 16000
NUM_AUDIO_CHANNELS = 1
AUDIO_FORMAT = "flac"
SAMPLE_WIDTH_16BIT = 2  # 16-bit PCM sample width in bytes
FLAC_COMPRESSION_LEVEL = "5"
M4A_BITRATE = "32k"

# Google Cloud shared constants
GCS_METADATA_SIZE_LIMIT = 8 * 1024  # 8 KiB in bytes
GCS_DOWNLOAD_TIMEOUT_SEC = 30

# Time Conversion shared constants
MS_PER_SECOND = 1000
MICROSECONDS_PER_MS = 1000
NANOS_PER_MS = 1_000_000
NANOS_PER_SECOND = 1_000_000_000
