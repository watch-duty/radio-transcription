import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam.io.fileio as fileio
import argparse


# This pipeline reads audio files from a specified GCS path, processes the audio data, and writes the metadata to another GCS path.
# The processing step can be customized to perform any desired operations on the audio data.
# Example command to run the pipeline:
# python apache.py \
#   --input_path gs://wd-radio-test/raw_data/* \
#   --project_id automatic-hawk-481415-m9 \
#   --region us-central1 \
#   --temp_location gs://wd-radio-test/temp/ \
#   --staging_location gs://wd-radio-test/staging/ \
#   --output-path gs://wd-radio-test/processed_data/
def run_audio_pipeline(
    input_gcs_path, project_id, region, temp_location, staging_location, output_gcs_path
):
    pipeline_options = PipelineOptions(
        runner="DataflowRunner",
        project=project_id,
        region=region,
        temp_location=temp_location,
        staging_location=staging_location,
        disk_size_gb=50,  # Adjust as needed based on your audio file sizes
        allow_unknown_args=True,
        # Add other options as needed, e.g., service_account_email
    )

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "Match Audio Files" >> fileio.MatchFiles(input_gcs_path)
            | "Read Audio Contents" >> fileio.ReadMatches()
            | "ProcessAudio" >> beam.Map(process_audio_data)
            | "WriteProcessedData" >> beam.io.WriteToText(output_gcs_path)
        )


def process_audio_data(file_info: fileio.ReadableFile):
    file_name = file_info.metadata.path
    audio_content_bytes = file_info.read()

    # Process audio bytes here
    metadata_fields = {
        "path": file_info.metadata.path,
        "size_in_bytes": file_info.metadata.size_in_bytes,
        "last_updated_in_seconds": file_info.metadata.last_updated_in_seconds,
        "all_attributes": file_info.metadata.__dict__,
    }
    print(f"Metadata fields: {metadata_fields}")
    processed_data = f"Processed file {file_name} with {len(audio_content_bytes)} bytes. Metadata: {metadata_fields}"

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
        "--output-path",
        type=str,
        required=True,
        help="GCS output path",
    )

    args = parser.parse_args()

    run_audio_pipeline(
        input_gcs_path=args.input_path,
        project_id=args.project_id,
        region=args.region,
        temp_location=args.temp_location,
        staging_location=args.staging_location,
        output_gcs_path=args.output_path,
    )
