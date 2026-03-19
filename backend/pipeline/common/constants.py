"""Shared constants for the pipeline.

These values are imported by pipeline stages that need consistent audio
processing parameters and storage-related limits.
"""

CHUNK_DURATION_SECONDS = 15
AUDIO_SAMPLE_RATE = 16_000  # 16 kHz
GCS_METADATA_SIZE_LIMIT = 8 * 1024  # 8 KiB in bytes
