import argparse

from apache_beam.options.pipeline_options import PipelineOptions

from backend.pipeline.transcription.constants import (
    DEFAULT_SIGNIFICANT_GAP_SEC,
    DEFAULT_STALE_TIMEOUT_SEC,
)
from backend.pipeline.transcription.enums import (
    MetricsExporterType,
    TranscriberType,
    VadType,
)


class TranscriptionOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser: argparse.ArgumentParser) -> None:
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
            "--project_id",
            type=str,
            required=True,
            help="GCP Project ID for Speech-to-Text API.",
        )
        parser.add_argument(
            "--transcriber_type",
            type=TranscriberType,
            choices=list(TranscriberType),
            default=TranscriberType.GOOGLE_CHIRP_V3,
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
            type=VadType,
            choices=list(VadType),
            default=VadType.TEN_VAD,
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
            "--significant_gap_sec",
            type=float,
            default=DEFAULT_SIGNIFICANT_GAP_SEC,
            help="Silence gap required to flush a transmission.",
        )
        parser.add_argument(
            "--stale_timeout_sec",
            type=float,
            default=DEFAULT_STALE_TIMEOUT_SEC,
            help="Seconds before an incomplete transmission is flushed.",
        )
