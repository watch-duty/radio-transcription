"""Constants for the segmentation Apache Beam streaming pipeline."""

import os
from typing import Final

from backend.pipeline.common.env import is_gcp_env

DEAD_LETTER_QUEUE_TAG: Final = "segmentation_dlq"
MAIN_TAG: Final = "main"

# Pipeline Defaults
DEFAULT_SIGNIFICANT_GAP_MS: Final = 800

# Operational Sizing Sweet Spot: Maximum number of chunks emitted per
# Dataflow bundle by our stateful ordering DoFns. Why exactly 10 chunks
# (~150 seconds of raw audio)?
#
# 1. Multiprocessing Work-Stealing Parallelism: Python enforces the Global
#    Interpreter Lock (GIL). If we computed 50+ chunks at a time, a single
#    worker VM would entirely monopolize its CPU core, causing severe Head-of-
#    Line starvation across our remaining autoscaled worker machines.
#    Clamping at 10 chunks keeps steady-state DAG compute time at ~6 seconds,
#    enabling central Windmill servers to instantly work-steal and distribute
#    subsequent chained bundles to completely different idle VMs.
#
# 2. Transaction Rollback "Blast Radius": If a sudden GCP network glitch or
#    downstream database exception occurs, Windmill must entirely roll back
#    the active bundle transaction. A 10-chunk clamp limits your re-drive
#    compute penalty perfectly to just a few seconds of compute effort.
#
# 3. Synchronous Checkpointing: Committing SSD state Checkpoints every ~6
#    seconds guarantees that downstream windowed joins, active database
#    deduplication FSMs, and live custom Stackdriver user execution
#    counters (`chunks_received`) remain 100% active, live, and real-time.
#
# 4. Resilient Backlog Recovery: Under heavy backlog catch-up load, GIL
#    contention and GCS latency can slow down sequential chunk processing
#    by ~30x (up to 18.5s per chunk). A 10-chunk clamp guarantees that even
#    under worst-case load, the bundle completes in ~185s, keeping it safely
#    below the hard 300-second Windmill lease limit and preventing infinite
#    timeout rollback loops.
MAX_CHUNKS_PER_WINDMILL_BUNDLE: Final = 10

# Resilient Runner V2 Gate: Minimum timer advancement (in seconds) to satisfy Dataflow Streaming
# Engine forward-progression invariants. In Apache Beam, scheduling a self-chaining recursive timer
# at un-advanced Event-Time (`timestamp + 0`) risks triggering un-progressed circular watermark
# dependencies, resulting in CommitStatus: NOT_FOUND commit loops or work-item evictions.
# We beautifully advance self-chaining timers by the true physical Event-Time duration of the audio
# successfully emitted in the bundle (e.g., ~150s), while maintaining this 1ms epsilon as an absolute
# bulletproof safety lower-bound to guarantee forward progress across every edge case. See PR #727.
WINDMILL_TIMER_MIN_ADVANCE_SECS: Final = 0.001
GCS_DOWNLOAD_TIMEOUT_SEC: Final = 30
DEFAULT_STALE_TIMEOUT_MS: Final = 75000
DEFAULT_MAX_TRANSMISSION_DURATION_MS: Final = 59000
DEFAULT_CONTINUOUS_OUT_OF_ORDER_TIMEOUT_MS: Final = 30000
DEFAULT_SEGMENTED_OUT_OF_ORDER_TIMEOUT_MS: Final = 10000
DEFAULT_BACKFILL_LATENESS_THRESHOLD_MS: Final = 300000
OVERLAPPING_TRANSMISSION_TOLERANCE_MS: Final = 100
DEFAULT_FLOAT_TOLERANCE_MS: Final = 500
UPSTREAM_GAP_DRIFT_TOLERANCE_MS: Final = 50


def _get_download_pool_size() -> int:
    """Dynamically determine thread pool size based on container CPU core count or env override."""
    env_val = os.getenv("SEGMENTATION_DOWNLOAD_POOL_SIZE")
    if env_val:
        try:
            return max(1, int(env_val))
        except ValueError:
            pass

    if not is_gcp_env():
        return 16

    try:
        core_count = len(os.sched_getaffinity(0))  # type: ignore[attr-defined]
    except (AttributeError, NotImplementedError):
        core_count = os.cpu_count() or 4

    return max(4, min(32, core_count * 4))


SHARED_DOWNLOAD_POOL_SIZE: Final = _get_download_pool_size()
GCS_CONNECTION_POOL_SIZE: Final = SHARED_DOWNLOAD_POOL_SIZE + 4
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
VAD_DEFAULT_THRESHOLD_ONSET: Final = 0.25
# Raised to 0.20 to close trailing silent segments faster
VAD_DEFAULT_THRESHOLD_OFFSET: Final = 0.20
VAD_DEFAULT_MIN_SPEECH_DURATION_MS: Final = 200
# Extended to 750ms to prevent whisper/dispatcher dropouts from prematurely splitting dispatches
VAD_DEFAULT_MIN_SILENCE_DURATION_MS: Final = 750
VAD_DEFAULT_PAD_SEC: Final = 0.3

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
VAD_DEFAULT_COMP_THRESHOLD_DB: Final = -30.0
VAD_DEFAULT_COMP_RATIO: Final = 6.0
VAD_DEFAULT_COMP_ATTACK_MS: Final = 2.0
VAD_DEFAULT_COMP_RELEASE_MS: Final = 150.0
VAD_DEFAULT_COMP_PEAK_THRESHOLD: Final = 0.55
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
