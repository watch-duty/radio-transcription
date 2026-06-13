"""Custom Apache Beam PipelineOptions mapping for environment and run configurations."""

import argparse

from apache_beam.options.pipeline_options import PipelineOptions

from backend.pipeline.segmentation.constants import (
    DEFAULT_CONTINUOUS_OUT_OF_ORDER_TIMEOUT_MS,
    DEFAULT_MAX_TRANSMISSION_DURATION_MS,
    DEFAULT_SIGNIFICANT_GAP_MS,
    DEFAULT_STALE_TIMEOUT_MS,
)


class SegmentationOptions(PipelineOptions):
    """CLI pipeline configuration options mapping to Beam's PipelineOptions."""

    @classmethod
    def _add_argparse_args(cls, parser: argparse.ArgumentParser) -> None:
        """Registers pipeline CLI parameters to enable interactive flag passing via Dataflow."""
        parser.add_argument(
            "--input_subscription",
            "--continuous_input_subscription",
            type=str,
            required=True,
            help="Pub/Sub ordered subscription for audio feeds.",
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
            default="gcs_uri",
            help="Pub/Sub attribute to use for strictly exactly-once deduplication.",
        )

        parser.add_argument(
            "--vad_config",
            type=str,
            default="{}",
            help="JSON string of VAD-specific configuration.",
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
            "--continuous_out_of_order_timeout_ms",
            type=int,
            default=None,
            help=f"Milliseconds to wait for missing chunks before accepting a logical gap for audio feeds. Default: {DEFAULT_CONTINUOUS_OUT_OF_ORDER_TIMEOUT_MS}ms.",
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
            "--staging_audio_bucket",
            type=str,
            required=False,
            help="GCS bucket name for storing temporary raw audio segments before segmentation.",
        )


class DataflowSystemOptions(PipelineOptions):
    """Dummy options class to absorb Dataflow-injected camelCase arguments and quiet warnings.

    Dataflow often passes internal system flags to workers in camelCase (e.g., --autoscalingAlgorithm),
    while the Python SDK expects snake_case. This class registers them so the parser recognizes them
    and stops logging 'Discarding unparseable args' warnings.
    """

    @classmethod
    def _add_argparse_args(cls, parser: argparse.ArgumentParser) -> None:
        parser.add_argument("--autoscalingAlgorithm", type=str, required=False)
        parser.add_argument("--dataflowJobId", type=str, required=False)
        parser.add_argument("--jobId", type=str, required=False)
        parser.add_argument("--numWorkers", type=str, required=False)
        parser.add_argument("--maxNumWorkers", type=str, required=False)
        parser.add_argument("--pipelineUrl", type=str, required=False)
        parser.add_argument("--gcpTempLocation", type=str, required=False)
        parser.add_argument("--tempLocation", type=str, required=False)
        parser.add_argument(
            "--direct_runner_use_stacked_bundle",
            action="store_true",
            required=False,
        )
        parser.add_argument(
            "--pipeline_type_check", action="store_true", required=False
        )
