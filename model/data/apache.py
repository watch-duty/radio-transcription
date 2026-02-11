import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam.io.fileio as fileio
import argparse
from google.cloud import storage
import logging
import datetime

logger = logging.getLogger(__name__)

# Suppress verbose logging from Beam options
logging.getLogger("apache_beam.options.pipeline_options").setLevel(logging.ERROR)


# This pipeline reads audio files from a specified GCS path, processes the audio data,
# and writes the metadata to another GCS path. The processing step can be customized to
# perform any desired operations on the audio data.
#
# Example command to run the pipeline:
# python apache.py \
#   --input_path gs://wd-radio-test/raw_data/*.wav \
#   --project_id automatic-hawk-481415-m9 \
#   --region us-central1 \
#   --temp_location gs://wd-radio-test/temp/ \
#   --staging_location gs://wd-radio-test/staging/ \
#   --output_path gs://wd-radio-test/processed_data/ \
#   --run_local
def run_audio_pipeline(
    input_gcs_path: str,
    project_id: str,
    region: str,
    temp_location: str,
    staging_location: str,
    output_gcs_path: str,
    run_local=False,
):
    runner = "DirectRunner" if run_local else "DataflowRunner"
    pipeline_options = PipelineOptions(
        runner=runner,
        project=project_id,
        region=region,
        temp_location=temp_location,
        staging_location=staging_location,
        disk_size_gb=50,
        allow_unknown_args=True,
    )

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "Match Audio Files" >> fileio.MatchFiles(input_gcs_path)
            | "Read Audio Contents" >> fileio.ReadMatches()
            | "ProcessAudio" >> beam.Map(process_audio_data)
            | "WriteProcessedData"
            >> beam.io.WriteToText(
                output_gcs_path,
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

    # TODO: Process audio bytes here
    metadata_fields = {
        "all_file_attributes": file_info.metadata.__dict__,
        "blob_metadata": blob.metadata,
    }
    processed_data = (
        f"Processed file {file_name} with {len(audio_content_bytes)} bytes. "
        f"Metadata: {metadata_fields}."
    )

    return processed_data


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

    run_audio_pipeline(
        input_gcs_path=args.input_path,
        project_id=args.project_id,
        region=args.region,
        temp_location=args.temp_location,
        staging_location=args.staging_location,
        output_gcs_path=args.output_path,
        run_local=args.run_local,
    )
