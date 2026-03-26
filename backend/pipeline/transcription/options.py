"""Custom Apache Beam PipelineOptions mapping for environment and run configurations."""

import argparse

from apache_beam.options.pipeline_options import PipelineOptions

from backend.pipeline.transcription.constants import (
    DEFAULT_MAX_TRANSMISSION_DURATION_MS,
    DEFAULT_OUT_OF_ORDER_TIMEOUT_MS,
    DEFAULT_SIGNIFICANT_GAP_MS,
    DEFAULT_STALE_TIMEOUT_MS,
    DEFAULT_VAD_POST_ROLL_MS,
    DEFAULT_VAD_PRE_ROLL_MS,
)
from backend.pipeline.transcription.enums import (
    MetricsExporterType,
    TranscriberType,
    VadType,
)


class TranscriptionOptions(PipelineOptions):
    """CLI pipeline configuration options mapping to Beam's PipelineOptions."""

    @classmethod
    def _add_argparse_args(cls, parser: argparse.ArgumentParser) -> None:
        """Registers pipeline CLI parameters to enable interactive flag passing via Dataflow."""
        parser.add_argument(
            "--input_topic",
            type=str,
            required=True,
            help="Pub/Sub topic to read from",
        )
        parser.add_argument(
            "--output_topic",
            type=str,
            required=True,
            help="Pub/Sub topic to write to",
        )
        parser.add_argument(
            "--dlq_topic",
            type=str,
            required=False,
            help="Pub/Sub topic to route Dead-Letter Queue (DLQ) processing failures. Defaults to output_topic appended with '-dlq'.",
        )
        parser.add_argument(
            "--id_label",
            type=str,
            required=False,
            default="chunk_uri",
            help="Pub/Sub attribute to use for strictly exactly-once deduplication.",
        )
        parser.add_argument(
            "--project_id",
            type=str,
            required=True,
            help="GCP Project ID for Speech-to-Text API.",
        )
        parser.add_argument(
            "--transcriber_type",
            type=str,
            choices=[e.value for e in TranscriberType],
            default=TranscriberType.GOOGLE_CHIRP_V3.value,
            help="Type of transcription model to use.",
        )
        parser.add_argument(
            "--transcriber_config",
            type=str,
            default="{}",
            help="JSON string of transcriber-specific configuration.",
        )
        parser.add_argument(
            "--vad_type",
            type=str,
            choices=[e.value for e in VadType],
            default=VadType.TEN_VAD.value,
            help="Type of VAD model to use.",
        )
        parser.add_argument(
            "--vad_config",
            type=str,
            default="{}",
            help="JSON string of VAD-specific configuration.",
        )
        parser.add_argument(
            "--metrics_exporter_type",
            type=str,
            default=MetricsExporterType.NONE.value,
            help="Comma-separated metrics platforms (e.g. 'gcp').",
        )
        parser.add_argument(
            "--metrics_config",
            type=str,
            default="{}",
            help="JSON string of metrics-specific configuration.",
        )
        parser.add_argument(
            "--significant_gap_ms",
            type=int,
            default=DEFAULT_SIGNIFICANT_GAP_MS,
            help="Silence gap required to flush a transmission.",
        )
        parser.add_argument(
            "--stale_timeout_ms",
            type=int,
            default=DEFAULT_STALE_TIMEOUT_MS,
            help="Milliseconds before an incomplete transmission is flushed.",
        )
        parser.add_argument(
            "--out_of_order_timeout_ms",
            type=int,
            default=DEFAULT_OUT_OF_ORDER_TIMEOUT_MS,
            help="Milliseconds to wait for missing chunks before accepting a gap.",
        )
        parser.add_argument(
            "--vad_pre_roll_ms",
            type=int,
            default=DEFAULT_VAD_PRE_ROLL_MS,
            help="Milliseconds of audio to include before the first spoken segment to provide a background noise floor.",
        )
        parser.add_argument(
            "--vad_post_roll_ms",
            type=int,
            default=DEFAULT_VAD_POST_ROLL_MS,
            help="Milliseconds of audio to include after the last spoken segment to provide a background noise floor.",
        )
        parser.add_argument(
            "--max_transmission_duration_ms",
            type=int,
            default=DEFAULT_MAX_TRANSMISSION_DURATION_MS,
            help="Absolute maximum duration of a single continuous transmission.",
        )
        parser.add_argument(
            "--route_to_dlq",
            action=argparse.BooleanOptionalAction,
            default=True,
            help="If false, exceptions will be raised immediately instead of routing to the Dead Letter Queue. Useful for tests.",
        )
        parser.add_argument(
            "--stitched_audio_bucket",
            type=str,
            required=False,
            help="GCS bucket name for storing clean, stitched audio. If omitted, audio is not persisted to GCS.",
        )
