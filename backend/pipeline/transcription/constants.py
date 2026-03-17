"""
Constants shared across the pipeline and tests.
"""

DEAD_LETTER_QUEUE_TAG = "transcription_dlq"

# Pipeline Defaults
DEFAULT_SIGNIFICANT_GAP_MS = 500
DEFAULT_STALE_TIMEOUT_MS = 60000
DEFAULT_MAX_TRANSMISSION_DURATION_MS = 600000
DEFAULT_OUT_OF_ORDER_TIMEOUT_MS = 5000
MAIN_TAG = "main"

# Telemetry Metrics
GCP_METRIC_PREFIX = "custom.googleapis.com/radio_transcription"
GCP_DURATION_METRIC_NAME = "transcription_time"
GCP_STITCHING_METRIC_NAME = "stitching_time"

# Time Conversions
MS_PER_SECOND = 1000
MICROSECONDS_PER_MS = 1000
NANOS_PER_MS = 1_000_000

from backend.pipeline.shared_constants import NUM_AUDIO_CHANNELS, SAMPLE_RATE_HZ

# 16-bit PCM = 2 bytes per sample
BYTES_PER_SECOND_16KHZ_MONO = SAMPLE_RATE_HZ * NUM_AUDIO_CHANNELS * 2

# Audio Filter Parameters
HIGHPASS_FILTER_FREQ = 300
LOWPASS_FILTER_FREQ = 3000

# Voice Activity Detection Defaults
DEFAULT_TENVAD_THRESHOLD = 0.8
DEFAULT_TENVAD_HOP_SIZE = 256
DEFAULT_TENVAD_MIN_SPEECH_MS = 250


