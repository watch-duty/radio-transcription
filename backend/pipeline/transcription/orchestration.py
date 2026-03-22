"""The canonical Apache Beam DAG definition and pipeline orchestration mapping.

This module composes the individual DoFns into a complete streaming topology.
It is separated from the CLI entry point to improve testability and modularity.
"""

import json
import logging

import apache_beam as beam
from apache_beam.io.gcp.pubsub import PubsubMessage, ReadFromPubSub, WriteToPubSub
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
    DownloadAudioFn,
    ParseAndKeyFn,
    RestoreOrderFn,
    SerializeToPubSubMessageFn,
)

logger = logging.getLogger(__name__)

def format_dlq_message(element: dict) -> PubsubMessage:
    """Formats the dlq message."""
    feed_id = element.get("feed_id", "unknown")
    payload = json.dumps(element).encode("utf-8")
    return PubsubMessage(
        data=payload,
        attributes={"feed_id": feed_id, "error_type": "pipeline_failure"},
    )

def get_pipeline(
    pipeline_options: PipelineOptions,
) -> beam.Pipeline:
    """Constructs the Apache Beam pipeline DAG and returns the pipeline object."""
    # Require streaming mode since we handle unbounded logical streams from Pub/Sub
    pipeline_options.view_as(StandardOptions).streaming = True
    options = pipeline_options.view_as(TranscriptionOptions)

    pipeline = beam.Pipeline(options=pipeline_options)
    messages = pipeline | "ReadFromPubSub" >> ReadFromPubSub(
        topic=options.input_topic, with_attributes=True, id_label="chunk_uri"
    )
    # Group incoming messages into Key-Value pairs: (feed_id, gs://uri/to/audio)
    parsed = messages | "ParseAndKey" >> beam.ParDo(ParseAndKeyFn()).with_outputs(
        DEAD_LETTER_QUEUE_TAG, main=MAIN_TAG
    )

    timestamped = parsed[MAIN_TAG] | "AddTimestamp" >> beam.ParDo(AddEventTimestamp())

    # Order chunks based on exact 15,000ms chunk duration expectations
    restored = timestamped | "RestoreOrder" >> beam.ParDo(
        RestoreOrderFn(
            config=OrderRestorerConfig(
                out_of_order_timeout_ms=options.out_of_order_timeout_ms
                or DEFAULT_OUT_OF_ORDER_TIMEOUT_MS,
            )
        )
    )

    # Claim-Check: Download the raw bytes for ordered chunks currently just passing as URIs
    download_config = StitchAudioConfig(
        project_id=options.project_id,
        vad_type=options.vad_type,
        vad_config=options.vad_config,
        metrics_exporter_type=options.metrics_exporter_type,
        metrics_config=options.metrics_config,
        significant_gap_ms=options.significant_gap_ms or DEFAULT_SIGNIFICANT_GAP_MS,
        stale_timeout_ms=options.stale_timeout_ms or DEFAULT_STALE_TIMEOUT_MS,
        max_transmission_duration_ms=options.max_transmission_duration_ms
        or DEFAULT_MAX_TRANSMISSION_DURATION_MS,
        route_to_dlq=options.route_to_dlq if options.route_to_dlq is not None else True,
    )
    downloaded_chunks = restored | "DownloadAudio" >> beam.ParDo(
        DownloadAudioFn(config=download_config)
    ).with_outputs(DEAD_LETTER_QUEUE_TAG, main=MAIN_TAG)

    # Core pipeline logic: State buffers audio across multiple chunks, flushing only on silence or timeout.
    stitching_results = downloaded_chunks.main | "StitchAudio" >> beam.ParDo(
        StitchAudioFn(config=download_config)
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
        downloaded_chunks[DEAD_LETTER_QUEUE_TAG],
        stitching_results[DEAD_LETTER_QUEUE_TAG],
        transcripts[DEAD_LETTER_QUEUE_TAG],
    ) | "FlattenDlqs" >> beam.Flatten()

    dlq_messages = dlq_combined | "FormatDlq" >> beam.Map(format_dlq_message)
    dlq_messages | "WriteDlqToPubSub" >> WriteToPubSub(
        topic=f"{options.output_topic}-dlq",
        with_attributes=True,
    )

    return pipeline
