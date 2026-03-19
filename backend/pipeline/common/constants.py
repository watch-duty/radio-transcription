"""Constants shared across the radio transcription pipeline."""

# Audio processing shared constants
CHUNK_DURATION_SECONDS = 15
AUDIO_SAMPLE_RATE = 16_000  # 16 kHz

# Google Cloud shared constants
GCS_METADATA_SIZE_LIMIT = 8 * 1024  # 8 KiB in bytes
