# Audio Ingestion Pipeline

## Streaming version: TODO

Stream from a URL feed and write output to a Pub/Sub topic.

## Batch version: batch_ingest.py

Apache Beam pipeline for audio file processing from URLs listed in a manifest file.

This module implements a distributed audio processing pipeline using Apache Beam
that reads audio files from URLs specified in a `manifest.txt` file, processes
the audio data, and writes metadata to a Pub/Sub topic.

The pipeline supports both local execution (DirectRunner) and distributed execution
on Google Cloud Dataflow (DataflowRunner).

### Example

Run the pipeline locally with the following command:

```bash
python batch_ingest.py \
--txt_file https://YOUR_MANIFEST_URL/manifest.txt \
--project_id YOUR_PROJECT_ID \
--region us-central1 \
--temp_location gs://YOUR_TEMP_BUCKET/temp/ \
--staging_location gs://YOUR_STAGING_BUCKET/staging/ \
--topic_id YOUR_TOPIC_ID \
--run_local
```

### Components

- **run_audio_pipeline**: Configures and executes the audio processing pipeline.
- **fetch_url_content**: Fetches the content of a URL.
- **get_metadata_fields**: Retrieves metadata fields of the audio file from the URL.