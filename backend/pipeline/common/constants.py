"""Constants shared across the radio transcription pipeline."""

# Audio processing shared constants
CHUNK_DURATION_SECONDS = 15
SAMPLE_RATE_HZ = 16000
NUM_AUDIO_CHANNELS = 1
AUDIO_FORMAT = "flac"

# Google Cloud shared constants
GCS_METADATA_SIZE_LIMIT = 8 * 1024  # 8 KiB in bytes

# Time Conversion shared constants
MS_PER_SECOND = 1000
MICROSECONDS_PER_MS = 1000
NANOS_PER_MS = 1_000_000
NANOS_PER_SECOND = 1_000_000_000

# 16-bit PCM = 2 bytes per sample
BYTES_PER_SECOND_16KHZ_MONO = SAMPLE_RATE_HZ * NUM_AUDIO_CHANNELS * 2
