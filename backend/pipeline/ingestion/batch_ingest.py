"""
Apache Beam pipeline for audio file processing from Google Cloud Storage.

This module implements a distributed audio processing pipeline using Apache Beam
that reads audio files from urls specified in a manifest.txt file, processes
the audio data, and writes metadata to a Pub/Sub topic.

The pipeline supports both local execution (DirectRunner) and distributed execution
on Google Cloud Dataflow (DataflowRunner).

Example:
    Run the pipeline locally with the following command:

    python batch_ingest.py \
    --txt_file https://watchduty-radio-transcription-data.s3.us-east-1.amazonaws.com/echo/manifest.txt \
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
    fetch_url_content: Fetches the content of a URL.
    get_metadata_fields: Retrieves metadata fields of the audio file from the URL.

"""

import argparse
import json
import logging
import urllib.error
import urllib.request
from dataclasses import dataclass

import apache_beam as beam
import requests
from apache_beam.io import WriteToPubSub
from apache_beam.options.pipeline_options import PipelineOptions

logger = logging.getLogger(__name__)

# Suppress verbose logging from Beam options about external arguments
logging.getLogger("apache_beam.options.pipeline_options").setLevel(logging.ERROR)


@dataclass
class PipelineConfig:
    """Configuration for the audio processing pipeline."""

    txt_file: str
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
            | "Read Manifest" >> beam.Create([config.txt_file])
            | "Read Lines" >> beam.Map(fetch_url_content)
            | "Split Lines" >> beam.FlatMap(lambda content: content.splitlines())
            | "Placeholder Filter Some Out" >> beam.Filter(lambda line: len(line) > 115)
            | "Get File Size Info" >> beam.Map(get_metadata_fields)
            | "Serialize to JSON Bytes"
            >> beam.Map(lambda data: json.dumps(data).encode("utf-8"))
            | "WriteProcessedData"
            >> WriteToPubSub(f"projects/{config.project_id}/topics/{config.topic_id}")
        )


def fetch_url_content(url: str) -> str:
    """Fetches the content of a URL."""
    if not url.startswith(("http:", "https:")):
        msg = "URL must start with 'http:' or 'https:'"
        raise ValueError(msg)

    try:
        with urllib.request.urlopen(url, timeout=10) as response:  # noqa: S310
            return response.read().decode("utf-8")

    except urllib.error.HTTPError as e:
        # Handles 404, 500, etc.
        return f"HTTP Error: {e.code}"
    except urllib.error.URLError as e:
        # Handles DNS failures or refused connections
        return f"Connection Error: {e.reason}"
    except UnicodeDecodeError:
        # Handles cases where the content isn't valid UTF-8
        return "Error: Content could not be decoded as UTF-8"
    except Exception as e:
        # Catch-all for unexpected issues
        return f"Unexpected Error: {e!s}"


def get_metadata_fields(file_path: str) -> dict:
    try:
        response = requests.get(file_path, timeout=10)
        response.raise_for_status()
        audio_bytes = response.content

        return {
            "file_path": file_path,
            "byte_length": len(audio_bytes),
            "source": "Echo",
        }
    except requests.exceptions.Timeout:
        logger.exception(f"Timeout fetching {file_path}")
        raise
    except requests.exceptions.HTTPError as e:
        logger.exception(f"HTTP error for {file_path}: {e.response.status_code}")
        raise
    except requests.exceptions.RequestException as e:
        logger.exception(f"Request failed for {file_path}: {e}")
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run Apache Beam audio processing pipeline"
    )
    parser.add_argument(
        "--txt_file",
        type=str,
        required=True,
        help="Path to the text file containing the list of audio file paths",
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
        txt_file=args.txt_file,
        project_id=args.project_id,
        region=args.region,
        temp_location=args.temp_location,
        staging_location=args.staging_location,
        topic_id=args.topic_id,
        run_local=args.run_local,
    )
    run_audio_pipeline(config)
