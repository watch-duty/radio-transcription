"""
Apache Beam pipeline for audio file processing from Google Cloud Storage.

This module implements a distributed audio processing pipeline using Apache Beam
that reads audio files from a specified Google Cloud Storage (GCS) path, processes
the audio data, and writes metadata to another GCS path.

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
        --output_path gs://wd-radio-test/processed_data/ \
        --run_local

Attributes:
    logger (logging.Logger): Logger instance for this module.

Functions:
    run_audio_pipeline: Configures and executes the audio processing pipeline.
    process_audio_data: Processes individual audio files and extracts metadata.

"""

import argparse
import datetime
import logging
from dataclasses import dataclass

import apache_beam as beam
from apache_beam.io import fileio
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
    output_gcs_path: str
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
        disk_size_gb=50,
        allow_unknown_args=True,
    )

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "Match Audio Files" >> fileio.MatchFiles(config.input_gcs_path)
            | "Read Audio Contents" >> fileio.ReadMatches()
            | "ProcessAudio" >> beam.Map(process_audio_data)
            | "WriteProcessedData"
            >> beam.io.WriteToText(
                config.output_gcs_path,
                file_name_suffix=f"_{datetime.datetime.now().strftime('%H:%M')}",
            )
        )


def process_audio_data(file_info: fileio.ReadableFile) -> str:
    file_name = file_info.metadata.path
    audio_content_bytes = file_info.read()

    # Metadata extraction from GCS blob
    bucket_name = file_name.split("/")[2]
    blob_name = "/".join(file_name.split("/")[3:])
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.get_blob(blob_name)

    # Process audio bytes here
    metadata_fields = {
        "all_file_attributes": file_info.metadata.__dict__,
        "blob_metadata": blob.metadata,
    }
    return (
        f"Processed file {file_name} with {len(audio_content_bytes)} bytes. "
        f"Metadata: {metadata_fields}."
    )


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
        "--output_path",
        type=str,
        required=True,
        help="GCS output path",
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
        output_gcs_path=args.output_path,
        run_local=args.run_local,
    )
    run_audio_pipeline(config)
