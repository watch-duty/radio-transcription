"""Constants for the segmentation Apache Beam streaming pipeline."""

from typing import Final

from backend.pipeline.common.utils import get_optimal_thread_pool_size

DEAD_LETTER_QUEUE_TAG: Final = "segmentation_dlq"
MAIN_TAG: Final = "main"

# Pipeline Defaults
DEFAULT_SIGNIFICANT_GAP_MS: Final = 800


# Maximum wall-clock duration (in seconds) a worker can spend inside a single Dataflow
# bundle. Sized to 1/5th of Google Cloud Windmill's hard 300-second RPC commit lease
# limit, leaving a generous 240-second safety margin for GCS uploads and state checkpointing.
MAX_WINDMILL_BUNDLE_DURATION_SEC: Final = 60.0

# Memory & GCS prefetch backstop / active per-bundle cap: Maximum number of chunks popped
# and prefetched per bundle during backfills, acting alongside the wall-clock budget
# (MAX_WINDMILL_BUNDLE_DURATION_SEC) as a hard item-count processing limit. Sized to
# ~50 minutes of audio (~300 chunks), which takes roughly ~8 seconds to compute, preventing
# instantaneous heap unrolls from flooding memory with thousands of in-flight GCS futures.
MAX_CHUNKS_PER_WINDMILL_BUNDLE: Final = 300

# Number of chunks to prefetch ahead in the sliding window to bound background task queue
# length and prevent connection pool exhaustion and task duplicates when bundles are clamped.
PREFETCH_WINDOW_SIZE: Final = 20

# Maximum waiting tasks allowed in the shared thread pool queue before we bypass prefetching
# to apply global backpressure and prevent pool starvation across multiple active feeds.
MAX_PREFETCH_QUEUE_DEPTH: Final = 15


# Resilient Runner V2 Gate: Minimum timer advancement (in seconds) to satisfy Dataflow Streaming
# Engine forward-progression invariants. In Apache Beam, scheduling a self-chaining recursive timer
# at un-advanced Event-Time (`timestamp + 0`) risks triggering un-progressed circular watermark
# dependencies, resulting in CommitStatus: NOT_FOUND commit loops or work-item evictions.
# We beautifully advance self-chaining timers by the true physical Event-Time duration of the audio
# successfully emitted in the bundle (e.g., ~375s), while maintaining this 1ms epsilon as an absolute
# bulletproof safety lower-bound to guarantee forward progress across every edge case. See PR #727.
WINDMILL_TIMER_MIN_ADVANCE_SECS: Final = 0.001
GCS_DOWNLOAD_TIMEOUT_SEC: Final = 30
DEFAULT_STALE_TIMEOUT_MS: Final = 75000
DEFAULT_MAX_TRANSMISSION_DURATION_MS: Final = 60000
DEFAULT_CONTINUOUS_OUT_OF_ORDER_TIMEOUT_MS: Final = 30000
DEFAULT_SEGMENTED_OUT_OF_ORDER_TIMEOUT_MS: Final = 10000
DEFAULT_BACKFILL_LATENESS_THRESHOLD_MS: Final = 300000
OVERLAPPING_TRANSMISSION_TOLERANCE_MS: Final = 100
DEFAULT_MIN_RAM_RESOURCE_HINT: Final = "16GB"
DEFAULT_FLOAT_TOLERANCE_MS: Final = 500
UPSTREAM_GAP_DRIFT_TOLERANCE_MS: Final = 50


SHARED_DOWNLOAD_POOL_SIZE: Final = get_optimal_thread_pool_size(
    "SEGMENTATION_DOWNLOAD_POOL_SIZE"
)
# Scaled to 1.5x max download thread pool to provide sufficient HTTP connection
# pool headroom without triggering urllib3 connection pool eviction.
GCS_CONNECTION_POOL_MULTIPLIER: Final = 1.5
GCS_CONNECTION_POOL_SIZE: Final = int(
    SHARED_DOWNLOAD_POOL_SIZE * GCS_CONNECTION_POOL_MULTIPLIER
)
GCS_CONNECTION_MAX_RETRIES: Final = 3

# Structured watermark and FSM recovery configurations
DEFAULT_VAD_POST_ROLL_MS: Final = 500

# Audio Filter Parameters

# Voice Activity Detection Defaults
VAD_DEFAULT_HIGHPASS_HZ: Final = 300.0
VAD_DEFAULT_LOWPASS_HZ: Final = 4000.0
VAD_DEFAULT_BLEND_RATIO: Final = 0.80
VAD_DEFAULT_BOOST_FREQ_HZ: Final = 2500.0
VAD_DEFAULT_BOOST_GAIN_DB: Final = 10.0
VAD_DEFAULT_PEAK_FILTER_Q: Final = 1.0
VAD_DEFAULT_THRESHOLD_ONSET: Final = 0.20
# Raised to 0.20 to close trailing silent segments faster
VAD_DEFAULT_THRESHOLD_OFFSET: Final = 0.20
VAD_DEFAULT_MIN_SPEECH_DURATION_MS: Final = 150
# Extended to 750ms to prevent whisper/dispatcher dropouts from prematurely splitting dispatches
VAD_DEFAULT_MIN_SILENCE_DURATION_MS: Final = 750
VAD_DEFAULT_PAD_SEC: Final = 0.3
# Absolute lower bound (in seconds) for padded audio segment start offsets
VAD_MIN_AUDIO_OFFSET_SEC: Final = 0.0
# Divisor used to split silence gaps between adjacent speech bursts into equal halves for midpoint padding clamping
VAD_GAP_MIDPOINT_DIVISOR: Final = 2.0

# VAD Priming Terminology Glossary:
# 1. prior_audio_tail (VAD_DEFAULT_PRIMING_SEC = 6.0s):
#    The duration of the trailing audio we extract from the end of a chunk and store in the
#    persistent state (Dataflow/Windmill) to pass to the next chunk. We store a larger buffer (6.0s)
#    in the state so that we can adjust the active warmup window (below) in the future via config
#    changes without needing a breaking state schema migration.
VAD_DEFAULT_PRIMING_SEC: Final = 3.5

# 2. warmup_sec (VAD_DEFAULT_WARMUP_SEC = 3.5s):
#    The active window of the prior_audio_tail that the VAD actually runs on to warm up its denoiser
#    and RNN states. We use 3.5s because empirical testing showed this value provides the optimal
#    balance between quiet speech onset sensitivity and preserving VAD F1 accuracy on benchmark static.
VAD_DEFAULT_WARMUP_SEC: Final = 3.5

# 3. fallback_priming (VAD_DEFAULT_FALLBACK_PRIMING_SEC = 1.0s):
#    The duration of synthetic comfort noise generated to prime the denoiser at the very start of a
#    stream (when no prior_audio_tail exists). Silero VAD is bypassed on this fallback noise to prevent
#    biasing it toward silence.
VAD_DEFAULT_FALLBACK_PRIMING_SEC: Final = 1.0
VAD_DEFAULT_DITHER_RMS: Final = 1e-6
VAD_NORMALIZATION_TARGET_PEAK: Final = 0.95
VAD_NORMALIZATION_MIN_PEAK: Final = 0.01
VAD_DEFAULT_SPIKINESS_RATIO_THRESHOLD: Final = 15.0
VAD_DEFAULT_MIN_RMS_THRESHOLD: Final = 0.001
VAD_DEFAULT_SEED: Final = 2147483647
MAX_AUDIO_CHUNK_DURATION_SEC: Final = 300


# Signaling Tone Detection Defaults
# Parameters for identifying and rejecting alert/paging tones (e.g., Quik-Call II)
TONE_STFT_FRAME_LENGTH: Final = 1024
TONE_STFT_HOP_LENGTH: Final = 512
TONE_MIN_POWER_THRESHOLD: Final = 1e-10
TONE_ACTIVE_FRAME_POWER_RATIO: Final = 0.01
TONE_PEAK_NEIGHBORHOOD_RADIUS: Final = 3
TONE_FRAME_MIN_CONCENTRATION_RATIO: Final = 0.85
TONE_SEGMENT_MIN_TONE_FRAME_RATIO: Final = 0.75


PRIMARY_AUDIO_STREAM_INDEX: Final = 0
MONO_CHANNEL_COUNT: Final = 1

# DSP Mathematical Heuristic Defaults
# Voice Activity Detection Formant Spectral Gating Defaults
VAD_SPECTRAL_MIN_TOTAL_ENERGY: Final = 1e-10
VAD_VOCAL_ENERGY_MIN_FREQ_HZ: Final = 200.0
VAD_VOCAL_ENERGY_MAX_FREQ_HZ: Final = 3000.0
# Empirically tuned: genuine speech (including whispers) typically maintains >0.40 energy ratio
# in the 200-3000Hz vocal formant band; 72Hz electrical flicker and low-frequency rumble maintain ~0.02.
# A 0.15 threshold provides a highly robust safety margin for whisper-heavy streams while rejecting interference.
VAD_VOCAL_ENERGY_MIN_RATIO: Final = 0.15


# Integration Suite Automated Testing Tone Definitions
TONE_QUIK_CALL_II_FREQ1_HZ: Final = 600.9
TONE_QUIK_CALL_II_FREQ2_HZ: Final = 742.5
TONE_EAS_FREQ1_HZ: Final = 853.0
TONE_EAS_FREQ2_HZ: Final = 960.0
VAD_TEST_SUBAUDIBLE_RUMBLE_FREQ_HZ: Final = 75.0
