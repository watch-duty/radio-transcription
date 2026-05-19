"""Constants shared across the pipeline and tests."""

from typing import Final

DEAD_LETTER_QUEUE_TAG: Final = "transcription_dlq"

# Default path to the packaged Chirp phrase hints file in the container image.
DEFAULT_PHRASE_HINTS_FILE_PATH: Final = (
    "/app/backend/pipeline/transcription/chirp_phrase_hints.txt"
)

# Default path to the packaged Chirp custom prompt file in the container image.
DEFAULT_CHIRP_PROMPT_FILE_PATH: Final = (
    "/app/backend/pipeline/transcription/chirp_prompt.txt"
)

# Marker defined in the prompt to indicate when the model detects no intelligible speech.
CHIRP_UNINTELLIGIBLE_MARKER: Final = "[UNINTELLIGIBLE]"

# Chirp model configuration defaults
DEFAULT_CHIRP_LOCATION: Final = "us"
DEFAULT_CHIRP_RECOGNIZER: Final = "_"
DEFAULT_CHIRP_MODEL: Final = "chirp_3"
DEFAULT_CHIRP_LANGUAGE_CODES: Final = ["en-US"]

# Pipeline Defaults
DEFAULT_SIGNIFICANT_GAP_MS: Final = 800
DEFAULT_STALE_TIMEOUT_MS: Final = 75000
DEFAULT_SEGMENTED_STALE_TIMEOUT_MS: Final = 5000
DEFAULT_MAX_TRANSMISSION_DURATION_MS: Final = 60000
DEFAULT_CONTINUOUS_OUT_OF_ORDER_TIMEOUT_MS: Final = 60000
DEFAULT_SEGMENTED_OUT_OF_ORDER_TIMEOUT_MS: Final = 10000
DEFAULT_BACKFILL_LATENESS_THRESHOLD_MS: Final = 300000
MAX_SYNCHRONOUS_TRANSCRIPTION_DURATION_MS: Final = 60000
OVERLAPPING_TRANSMISSION_TOLERANCE_MS: Final = 100
DEFAULT_FLOAT_TOLERANCE_MS: Final = 500
MAIN_TAG: Final = "main"

# API Retry Configuration
DEFAULT_MAX_RETRIES: Final = 5
DEFAULT_RETRY_MAX_SECONDS: Final = 10


# Audio Filter Parameters
HIGHPASS_FILTER_FREQ: Final = 300
LOWPASS_FILTER_FREQ: Final = 3000

# Voice Activity Detection Defaults
VAD_DEFAULT_HIGHPASS_HZ: Final = 300.0
VAD_DEFAULT_LOWPASS_HZ: Final = 4000.0
VAD_DEFAULT_BLEND_RATIO: Final = 0.9
VAD_DEFAULT_BOOST_FREQ_HZ: Final = 2500.0
VAD_DEFAULT_BOOST_GAIN_DB: Final = 10.0
VAD_DEFAULT_PEAK_FILTER_Q: Final = 1.0
VAD_DEFAULT_THRESHOLD_ONSET: Final = 0.35
VAD_DEFAULT_THRESHOLD_OFFSET: Final = 0.10
VAD_DEFAULT_MIN_SPEECH_DURATION_MS: Final = 100
VAD_DEFAULT_MIN_SILENCE_DURATION_MS: Final = 500
VAD_DEFAULT_PAD_SEC: Final = 0.0
VAD_DEFAULT_PRIMING_SEC: Final = 6.0
DEFAULT_VAD_PRE_ROLL_MS: Final = 300
DEFAULT_VAD_POST_ROLL_MS: Final = 500

# Audio Signal Processing Boundaries
INT16_MAX_FLOAT: Final = 32768.0

# DSP Mathematical Heuristic Defaults
DEFAULT_SED_FFT_SIZE: Final = 2048
DEFAULT_SED_HOP_SIZE: Final = 512
VAD_RMS_SILENCE_THRESHOLD: Final = 0.005
