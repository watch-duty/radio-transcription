"""
Constants shared across the pipeline and tests.
"""

DEAD_LETTER_QUEUE_TAG = "transcription_dlq"

# Audio Constraints
SAMPLE_RATE_HZ = 16000
AUDIO_CHANNELS = 1
MS_PER_SECOND = 1000

# 16-bit PCM = 2 bytes per sample
BYTES_PER_SECOND_16KHZ_MONO = SAMPLE_RATE_HZ * AUDIO_CHANNELS * 2

# Pipeline Defaults
DEFAULT_SIGNIFICANT_GAP_SEC = 0.5
DEFAULT_STALE_TIMEOUT_SEC = 60.0
MAIN_TAG = "main"

# File Formats
AUDIO_FORMAT = "flac"
