"""The canonical Apache Beam DAG definition and pipeline orchestration mapping.

This module composes the individual DoFns into a complete streaming topology.
It is separated from the CLI entry point to improve testability and modularity.
"""

import json

import apache_beam as beam
from apache_beam.io.gcp.pubsub import (
    PubsubMessage,
    ReadFromPubSub,
    WriteToPubSub,
)
from apache_beam.options.pipeline_options import (
    GoogleCloudOptions,
    PipelineOptions,
    StandardOptions,
)

from backend.pipeline.common.log_helper import get_task_logger
from backend.pipeline.segmentation import coders as trans_coders
from backend.pipeline.segmentation.constants import (
    DEAD_LETTER_QUEUE_TAG,
    DEFAULT_CONTINUOUS_OUT_OF_ORDER_TIMEOUT_MS,
    DEFAULT_MAX_TRANSMISSION_DURATION_MS,
    DEFAULT_SIGNIFICANT_GAP_MS,
    DEFAULT_STALE_TIMEOUT_MS,
    MAIN_TAG,
)
from backend.pipeline.segmentation.datatypes import (
    OrderRestorerConfig,
    StitchAudioConfig,
)
from backend.pipeline.segmentation.options import SegmentationOptions
from backend.pipeline.segmentation.transforms.stateful import (
    OrderedStitchAudioFn,
)
from backend.pipeline.segmentation.transforms.stateless import (
    ParseAndKeyFn,
    UploadRawSegmentFn,
)

logger = get_task_logger(
    __name__, {"system": "transcription", "component": "orchestration"}
)


def format_dlq_message(element: dict) -> PubsubMessage:
    """Formats the dlq message."""
    feed_id = element.get("feed_id", "unknown")
    payload = json.dumps(element).encode("utf-8")
    return PubsubMessage(
        data=payload,
        attributes={"feed_id": feed_id, "error_type": "pipeline_failure"},
        ordering_key=feed_id,
    )


def get_pipeline(
    pipeline_options: PipelineOptions,
) -> beam.Pipeline:
    """Constructs the Apache Beam pipeline DAG and returns the pipeline object."""
    trans_coders.register_custom_coders()
    # Require streaming mode since we handle unbounded logical streams from Pub/Sub
    standard_options = pipeline_options.view_as(StandardOptions)
    standard_options.streaming = True
    options = pipeline_options.view_as(SegmentationOptions)
    project = pipeline_options.view_as(GoogleCloudOptions).project

    # Validate logical pipeline timeout configuration rules
    ooo_timeout = (
        options.out_of_order_timeout_ms
        if options.out_of_order_timeout_ms is not None
        else DEFAULT_CONTINUOUS_OUT_OF_ORDER_TIMEOUT_MS
    )

    stale_timeout = options.stale_timeout_ms or DEFAULT_STALE_TIMEOUT_MS

    if not options.staging_audio_bucket:
        err_msg = (
            "Invalid pipeline configuration: staging_audio_bucket must be set. "
            "This bucket is required to stage raw segmented audio chunks."
        )
        raise ValueError(err_msg)

    if ooo_timeout >= stale_timeout:
        err_msg = (
            f"Invalid pipeline configuration: stale_timeout_ms ({stale_timeout}) must be strictly "
            f"greater than out_of_order_timeout_ms ({ooo_timeout}) to prevent fragmented audio stitching."
        )
        raise ValueError(err_msg)

    pipeline = beam.Pipeline(options=pipeline_options)

    # Note: DirectRunner's dummy PubSub emulator natively rejects id_label.
    # To run locally, explicitly pass --id_label "" to bypass exact-once deduplication.
    messages = pipeline | "ReadFromPubSub" >> ReadFromPubSub(
        subscription=options.input_subscription,
        id_label=options.id_label or None,
        with_attributes=True,
        timestamp_attribute="timestamp_ms",
    )

    # Parse and key stream
    parsed = messages | "ParseAndKey" >> beam.ParDo(
        ParseAndKeyFn(is_continuous=True)
    ).with_outputs(DEAD_LETTER_QUEUE_TAG, main=MAIN_TAG)

    dlq_list = []

    stitch_config = StitchAudioConfig(
        project_id=project,
        vad_config=options.vad_config,
        significant_gap_ms=options.significant_gap_ms
        or DEFAULT_SIGNIFICANT_GAP_MS,
        stale_timeout_ms=stale_timeout,
        max_transmission_duration_ms=options.max_transmission_duration_ms
        or DEFAULT_MAX_TRANSMISSION_DURATION_MS,
        route_to_dlq=options.route_to_dlq
        if options.route_to_dlq is not None
        else True,
    )

    stitching = parsed[MAIN_TAG] | "OrderedStitchAudio" >> beam.ParDo(
        OrderedStitchAudioFn(
            order_config=OrderRestorerConfig(
                out_of_order_timeout_ms=ooo_timeout,
            ),
            stitch_config=stitch_config,
        )
    ).with_resource_hints(min_ram="8GB").with_outputs(
        DEAD_LETTER_QUEUE_TAG, main=MAIN_TAG
    )

    dlq_list.append(stitching[DEAD_LETTER_QUEUE_TAG])
    dlq_list.append(parsed[DEAD_LETTER_QUEUE_TAG])

    stitching_main = stitching.main

    # Statelessly upload the raw PCM buffer as a WAV file and produce SegmentedAudio claim-check
    uploaded_segments = stitching_main | "UploadRawSegment" >> beam.ParDo(
        UploadRawSegmentFn(
            staging_audio_bucket=options.staging_audio_bucket,
            project_id=project,
        )
    ).with_outputs(DEAD_LETTER_QUEUE_TAG, main=MAIN_TAG)

    uploaded_segments.main | "WriteToPubSub" >> WriteToPubSub(
        topic=options.output_topic,
        with_attributes=True,
    )

    # Route all DLQ outputs to a dedicated topic
    dlq_list.append(uploaded_segments[DEAD_LETTER_QUEUE_TAG])

    dlq_combined = tuple(dlq_list) | "FlattenDlqs" >> beam.Flatten()

    dlq_messages = dlq_combined | "FormatDlq" >> beam.Map(format_dlq_message)
    dlq_messages | "WriteDlqToPubSub" >> WriteToPubSub(
        topic=options.dlq_topic or f"{options.output_topic}-dlq",
        with_attributes=True,
    )

    return pipeline
