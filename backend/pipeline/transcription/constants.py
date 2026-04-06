"""Constants shared across the pipeline and tests."""

DEAD_LETTER_QUEUE_TAG = "transcription_dlq"

# Default path to the packaged Chirp keyword/phrase hints file in the container image.
DEFAULT_KEYWORDS_FILE_PATH = (
    "/app/backend/pipeline/transcription/chirp_keywords.json"
)

# Chirp model configuration defaults
DEFAULT_CHIRP_LOCATION = "us-central1"
DEFAULT_CHIRP_RECOGNIZER = "_"
DEFAULT_CHIRP_MODEL = "chirp_3"
DEFAULT_CHIRP_LANGUAGE_CODES = ["en-US"]
DEFAULT_KEYWORD_BOOST = 10.0

# Pipeline Defaults
DEFAULT_SIGNIFICANT_GAP_MS = 800
DEFAULT_STALE_TIMEOUT_MS = 45000
DEFAULT_MAX_TRANSMISSION_DURATION_MS = 600000
DEFAULT_OUT_OF_ORDER_TIMEOUT_MS = 15000
DEFAULT_FLOAT_TOLERANCE_MS = 500
MAIN_TAG = "main"

# API Retry Configuration
DEFAULT_MAX_RETRIES = 5
DEFAULT_RETRY_MAX_SECONDS = 10

# Telemetry Metrics
GCP_METRIC_PREFIX = "custom.googleapis.com/radio_transcription"
GCP_DURATION_METRIC_NAME = "transcription_time"
GCP_STITCHING_METRIC_NAME = "stitching_time"

# Voice Activity Detection Defaults
DEFAULT_VAD_PRE_ROLL_MS = 200
DEFAULT_VAD_POST_ROLL_MS = 200
DEFAULT_VAD_THRESHOLD = 0.15


MS_PER_SEC = 1000

