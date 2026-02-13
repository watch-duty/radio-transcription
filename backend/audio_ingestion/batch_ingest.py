"""
Apache Beam pipeline for audio file processing from Google Cloud Storage.

This module implements a distributed audio processing pipeline using Apache Beam
that reads audio files from a specified Google Cloud Storage (GCS) path, processes
the audio data, and writes metadata to a Pub/Sub topic.

The pipeline supports both local execution (DirectRunner) and distributed execution
on Google Cloud Dataflow (DataflowRunner).

Example:
    Run the pipeline locally with the following command:

    $ python batch_ingest.py \
        --input_path gs://wd-radio-test/raw_data/*.wav \
        --project_id automatic-hawk-481415-m9 \
        --region us-central1 \
        --temp_location gs://wd-radio-test/temp/ \
        --staging_location gs://wd-radio-test/staging/ \
        --topic_id watch-duty-ingestion-test \
        --run_local

Attributes:
    logger (logging.Logger): Logger instance for this module.

Functions:
    run_audio_pipeline: Configures and executes the audio processing pipeline.
    process_audio_data: Processes individual audio files and extracts metadata.

"""

import argparse
import json
import logging
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

import apache_beam as beam
from apache_beam.io import WriteToPubSub, fileio
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import storage

logger = logging.getLogger(__name__)

# Suppress verbose logging from Beam options about external arguments
logging.getLogger("apache_beam.options.pipeline_options").setLevel(logging.ERROR)


@dataclass
class PipelineConfig:
    """Configuration for the audio processing pipeline."""

    input_gcs_path: str
    project_id: str
    region: str
    temp_location: str
    staging_location: str
    topic_id: str
    run_local: bool = False


def run_audio_pipeline(config: PipelineConfig) -> None:
    """
    Configures and executes the audio processing pipeline.

    Args:
        config: PipelineConfig object containing all pipeline parameters.

    """
    runner = "DirectRunner" if config.run_local else "DataflowRunner"
    pipeline_options = PipelineOptions(
        runner=runner,
        project=config.project_id,
        region=config.region,
        temp_location=config.temp_location,
        staging_location=config.staging_location,
        allow_unknown_args=True,
    )

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "Match Audio Files" >> fileio.MatchFiles(config.input_gcs_path)
            | "Read Audio Contents" >> fileio.ReadMatches()
            | "ProcessAudio" >> beam.ParDo(ProcessAudioDataDoFn())
            | "WriteProcessedData"
            >> WriteToPubSub(f"projects/{config.project_id}/topics/{config.topic_id}")
        )


class ProcessAudioDataDoFn(beam.DoFn):
    def setup(self) -> None:
        self.storage_client = storage.Client()

    def process(
        self, element: fileio.ReadableFile, *args: Any, **kwargs: Any
    ) -> Iterable[bytes]:
        try:
            if not element.metadata or not element.metadata.path:
                logger.warning("File metadata or path is missing. Skipping file.")
                return []

            # get the GCS blob to access user-defined metadata fields
            file_name = element.metadata.path
            path_no_gs = file_name.replace("gs://", "", 1)
            path_parts = path_no_gs.split("/", 1)
            if len(path_parts) != 2:
                logger.warning(
                    f"Unexpected file path format: {file_name}. Skipping file."
                )
                return []
            bucket_name, blob_name = path_parts
            bucket = self.storage_client.bucket(bucket_name)
            blob = bucket.get_blob(blob_name)

            if blob and blob.metadata:
                metadata_fields = {
                    "byte_length": element.metadata.size_in_bytes,
                    "location": (blob.metadata.get("location", "Unknown")),
                    "feed": (blob.metadata.get("feed", "Unknown")),
                    "source": (blob.metadata.get("source", "Unknown")),
                }
            else:
                metadata_fields = {
                    "byte_length": element.metadata.size_in_bytes,
                }
            json_string = json.dumps(metadata_fields)
            return [json_string.encode("utf-8")]
        except Exception as e:
            logger.exception(
                f"Error processing file {element.metadata.path if element.metadata else 'unknown'}: {e}"
            )
            return []


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run Apache Beam audio processing pipeline"
    )
    parser.add_argument(
        "--input_path",
        type=str,
        required=True,
        help="GCS input path for audio files",
    )
    parser.add_argument(
        "--project_id",
        type=str,
        required=True,
        help="GCP project ID",
    )
    parser.add_argument(
        "--region",
        type=str,
        required=True,
        help="GCP region",
    )
    parser.add_argument(
        "--temp_location",
        type=str,
        required=True,
        help="GCS temp bucket location",
    )
    parser.add_argument(
        "--staging_location",
        type=str,
        required=True,
        help="GCS staging bucket location",
    )
    parser.add_argument(
        "--topic_id",
        type=str,
        required=True,
        help="Pub/Sub topic ID",
    )
    parser.add_argument(
        "--run_local",
        action=argparse.BooleanOptionalAction,
        help="Whether to run the pipeline locally (DirectRunner) or on Dataflow",
    )

    args = parser.parse_args()

    config = PipelineConfig(
        input_gcs_path=args.input_path,
        project_id=args.project_id,
        region=args.region,
        temp_location=args.temp_location,
        staging_location=args.staging_location,
        topic_id=args.topic_id,
        run_local=args.run_local,
    )
    run_audio_pipeline(config)
