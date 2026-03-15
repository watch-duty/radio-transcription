"""
Radio Transcription Pipeline Orchestration

This module contains the Apache Beam pipeline definition and DAG construction.
It is separated from the CLI entry point to improve testability and modularity.
"""

import json
import logging

import apache_beam as beam
from apache_beam.io.gcp.pubsub import ReadFromPubSub, WriteToPubSub
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

from backend.pipeline.transcription.constants import (
    DEAD_LETTER_QUEUE_TAG,
    DEFAULT_MAX_TRANSMISSION_DURATION_MS,
    DEFAULT_OUT_OF_ORDER_TIMEOUT_MS,
    DEFAULT_SIGNIFICANT_GAP_MS,
    DEFAULT_STALE_TIMEOUT_MS,
    MAIN_TAG,
)
from backend.pipeline.transcription.datatypes import (
    OrderRestorerConfig,
    StitchAudioConfig,
    TranscribeAudioConfig,
)
from backend.pipeline.transcription.options import TranscriptionOptions
from backend.pipeline.transcription.stitcher import StitchAudioFn, TranscribeAudioFn
from backend.pipeline.transcription.transforms import (
    AddEventTimestamp,
    ParseAndKeyFn,
    RestoreOrderFn,
    SerializeToPubSubMessageFn,
)

logger = logging.getLogger(__name__)


def get_pipeline(
    pipeline_options: PipelineOptions,
) -> beam.Pipeline:
    """
    Constructs the Apache Beam pipeline DAG and returns the pipeline object.
    """
    # Require streaming mode since we handle unbounded logical streams from Pub/Sub
    pipeline_options.view_as(StandardOptions).streaming = True
    options = pipeline_options.view_as(TranscriptionOptions)

    pipeline = beam.Pipeline(options=pipeline_options)
    messages = pipeline | "ReadFromPubSub" >> ReadFromPubSub(
        topic=options.input_topic, with_attributes=True
    )
    # Group incoming messages into Key-Value pairs: (feed_id, gs://uri/to/audio)
    parsed = messages | "ParseAndKey" >> beam.ParDo(ParseAndKeyFn()).with_outputs(
        DEAD_LETTER_QUEUE_TAG, main=MAIN_TAG
    )

    timestamped = parsed[MAIN_TAG] | "AddTimestamp" >> beam.ParDo(AddEventTimestamp())

    restored = timestamped | "RestoreOrder" >> beam.ParDo(
        RestoreOrderFn(
            config=OrderRestorerConfig(
                out_of_order_timeout_ms=options.out_of_order_timeout_ms
                or DEFAULT_OUT_OF_ORDER_TIMEOUT_MS,
            )
        )
    )

    # Core pipeline logic: State buffers audio across multiple chunks, flushing only on silence or timeout.
    stitching_results = restored | "StitchAudio" >> beam.ParDo(
        StitchAudioFn(
            config=StitchAudioConfig(
                project_id=options.project_id,
                vad_type=options.vad_type,
                vad_config=options.vad_config,
                metrics_exporter_type=options.metrics_exporter_type,
                metrics_config=options.metrics_config,
                significant_gap_ms=options.significant_gap_ms
                or DEFAULT_SIGNIFICANT_GAP_MS,
                stale_timeout_ms=options.stale_timeout_ms or DEFAULT_STALE_TIMEOUT_MS,
                max_transmission_duration_ms=options.max_transmission_duration_ms
                or DEFAULT_MAX_TRANSMISSION_DURATION_MS,
                route_to_dlq=options.route_to_dlq
                if options.route_to_dlq is not None
                else True,
            )
        )
    ).with_outputs(DEAD_LETTER_QUEUE_TAG, main=MAIN_TAG)

    transcripts = stitching_results.main | "TranscribeAudio" >> beam.ParDo(
        TranscribeAudioFn(
            config=TranscribeAudioConfig(
                project_id=options.project_id,
                transcriber_type=options.transcriber_type,
                transcriber_config=options.transcriber_config,
                vad_type=options.vad_type,
                vad_config=options.vad_config,
                metrics_exporter_type=options.metrics_exporter_type,
                metrics_config=options.metrics_config,
                route_to_dlq=options.route_to_dlq
                if options.route_to_dlq is not None
                else True,
            )
        )
    ).with_outputs(DEAD_LETTER_QUEUE_TAG, main=MAIN_TAG)

    # Convert the native TranscriptionResult into a serialized Protobuf and wrap in a Pub/Sub message
    serialized = transcripts.main | "Serialize" >> beam.ParDo(
        SerializeToPubSubMessageFn()
    )
    serialized | "WriteToPubSub" >> WriteToPubSub(
        topic=options.output_topic,
        with_attributes=True,
    )

    # Route all DLQ (Dead Letter Queue) outputs from intermediate steps to a dedicated topic
    dlq_combined = (
        parsed[DEAD_LETTER_QUEUE_TAG],
        stitching_results[DEAD_LETTER_QUEUE_TAG],
        transcripts[DEAD_LETTER_QUEUE_TAG],
    ) | "FlattenDlqs" >> beam.Flatten()

    dlq_json = dlq_combined | "FormatDlqAsJson" >> beam.Map(
        lambda x: json.dumps(x).encode("utf-8")
    )
    dlq_json | "WriteDlqToPubSub" >> WriteToPubSub(topic=f"{options.output_topic}-dlq")

    return pipeline
