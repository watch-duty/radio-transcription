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
    DEFAULT_SIGNIFICANT_GAP_SEC,
    DEFAULT_STALE_TIMEOUT_SEC,
    MAIN_TAG,
)
from backend.pipeline.transcription.options import TranscriptionOptions
from backend.pipeline.transcription.transforms import (
    AddEventTimestamp,
    ParseAndKeyFn,
    SerializeToPubSubMessageFn,
    StitchAndTranscribeConfig,
    StitchAndTranscribeFn,
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

    # Core pipeline logic: State buffers audio across multiple chunks, flushing only on silence or timeout.
    transcripts = timestamped | "StitchAndTranscribe" >> beam.ParDo(
        StitchAndTranscribeFn(
            config=StitchAndTranscribeConfig(
                project_id=options.project_id,
                transcriber_type=options.transcriber_type,
                transcriber_config=options.transcriber_config,
                vad_type=options.vad_type,
                vad_config=options.vad_config,
                metrics_exporter_type=options.metrics_exporter_type,
                metrics_config=options.metrics_config,
                significant_gap_sec=options.significant_gap_sec
                or DEFAULT_SIGNIFICANT_GAP_SEC,
                stale_timeout_sec=options.stale_timeout_sec
                or DEFAULT_STALE_TIMEOUT_SEC,
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
        transcripts[DEAD_LETTER_QUEUE_TAG],
    ) | "FlattenDlqs" >> beam.Flatten()

    dlq_json = dlq_combined | "FormatDlqAsJson" >> beam.Map(json.dumps)
    dlq_json | "WriteDlqToPubSub" >> WriteToPubSub(topic=f"{options.output_topic}-dlq")

    return pipeline
