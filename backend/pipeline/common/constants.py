"""Constants shared across the radio transcription pipeline."""

# Audio processing shared constants
CHUNK_DURATION_SECONDS = 15
SAMPLE_RATE_HZ = 16000
NUM_AUDIO_CHANNELS = 1
AUDIO_FORMAT = "flac"
FLAC_COMPRESSION_LEVEL = "5"
M4A_BITRATE = "32k"

GCS_DOWNLOAD_TIMEOUT_SEC = 30
GCS_UPLOAD_TIMEOUT_SEC = 60
GCS_RETRY_MAX_ATTEMPTS = 5
GCS_RETRY_MIN_WAIT_SEC = 1.0
GCS_RETRY_MAX_WAIT_SEC = 15.0

# Time Conversion shared constants
MS_PER_SECOND = 1000
MICROSECONDS_PER_MS = 1000
NANOS_PER_MS = 1_000_000
NANOS_PER_SECOND = 1_000_000_000

# Transcription fallback constants
UNINTELLIGIBLE_MARKER = "[UNINTELLIGIBLE]"

# Source types published as ContinuousAudio, so only these reach Dataflow
# segmentation. Slugs rather than SourceType so Dataflow workers don't import
# the ingestion registry (and asyncpg with it); SourceRuntimeSpec.topic_kind is
# still the authority and a test in the ingestion suite pins this to it.
CONTINUOUS_SOURCE_TYPES = frozenset({"bcfy_feeds", "generic_icecast"})
