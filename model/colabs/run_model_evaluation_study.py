import os
import json
import requests
from google.cloud import storage

def _parse_gcs_uri(gcs_uri: str) -> tuple[str, str]:
    """Parses a GCS URI into bucket name and blob path."""
    if not gcs_uri.startswith("gs://"):
        raise ValueError("GCS URI must start with 'gs://'")
    parts = gcs_uri[len("gs://"):].split('/', 1)
    bucket_name = parts[0]
    blob_path = parts[1] if len(parts) > 1 else ''
    return bucket_name, blob_path

def download_blob_to_file(storage_client, bucket_name: str, blob_path: str, destination_file_name: str):
    """Downloads a blob from GCS to a local file."""
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.download_to_filename(destination_file_name)
    print(f"Blob {blob_path} downloaded from bucket {bucket_name} to {destination_file_name}.")

def upload_file_to_blob(storage_client, bucket_name: str, blob_path: str, source_file_name: str):
    """Uploads a local file to a GCS blob."""
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.upload_from_filename(source_file_name)
    print(f"File {source_file_name} uploaded to gs://{bucket_name}/{blob_path}.")

def download_jsonl_manifest(storage_client, gcs_manifest_uri: str) -> list[dict]:
    """Downloads and parses a JSONL manifest from GCS."""
    bucket_name, blob_path = _parse_gcs_uri(gcs_manifest_uri)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    content = blob.download_as_text()
    
    manifest_entries = []
    for line in content.strip().split('\n'):
        if line:
            manifest_entries.append(json.loads(line))
    print(f"Downloaded and parsed {len(manifest_entries)} entries from {gcs_manifest_uri}.")
    return manifest_entries

def transcribe_audio_with_model(audio_file_path: str, endpoint_url: str, prompt: str = None) -> str:
    """Sends an audio file to a self-contained STT model FastAPI endpoint."""
    with open(audio_file_path, "rb") as f:
        data = {"prompt": prompt} if prompt else None
        response = requests.post(endpoint_url, files={"file": f}, data=data)
    if response.status_code == 200:
        # Try getting "transcription" first, default to generic "text" if not present
        res_json = response.json()
        return res_json.get("transcription") or res_json.get("text") or ""
    else:
        raise RuntimeError(f"FastAPI inference failed with status code {response.status_code}: {response.text}")

def run_pipeline(gcp_project_id: str, gcs_manifest_uri: str, gcs_bucket: str, project_name: str, experiment_name: str, model_endpoints: dict[str, str], prompt: str = None):
    """
    Runs the transcription evaluation pipeline concurrently for multiple models.
    Outputs will be saved dynamically under:
    gs://{GCS_BUCKET}/transcripts/{PROJECT_NAME}_audio/{MODEL_NAME}/{EXPERIMENT_NAME}/
    """
    storage_client = storage.Client(project=gcp_project_id)
    manifest_data = download_jsonl_manifest(storage_client, gcs_manifest_uri)
    print("\n--- Processing each audio entry ---")

    for i, entry in enumerate(manifest_data):
        audio_gcs_uri = entry["audio_filepath"]
        print(f"\nProcessing audio: {audio_gcs_uri}")

        audio_bucket, audio_blob_path = _parse_gcs_uri(audio_gcs_uri)
        local_audio_file_name = os.path.basename(audio_blob_path)
        local_audio_path = f"./temp_audio_{local_audio_file_name}"

        print(f"Downloading {audio_blob_path} from GCS to {local_audio_path}")
        download_blob_to_file(storage_client, audio_bucket, audio_blob_path, local_audio_path)

        for model_name, endpoint_url in model_endpoints.items():
            print(f"Transcribing with {model_name}: {local_audio_file_name}...")
            try:
                transcription_result = transcribe_audio_with_model(local_audio_path, endpoint_url, prompt)
            except Exception as e:
                print(f"Error during transcription for {model_name}: {e}")
                transcription_result = "[ERROR]"

            local_result_file_name = f"{os.path.splitext(local_audio_file_name)[0]}.txt"
            local_result_path = f"./{local_result_file_name}"
            with open(local_result_path, "w") as f:
                f.write(transcription_result)

            result_gcs_blob_path = f"transcripts/{project_name}_audio/{model_name}/{experiment_name}/{local_result_file_name}"
            print(f"Uploading {model_name} result from {local_result_path} to gs://{gcs_bucket}/{result_gcs_blob_path}")
            upload_file_to_blob(storage_client, gcs_bucket, result_gcs_blob_path, local_result_path)

            os.remove(local_result_path)
            print(f"Cleaned up local result file: {local_result_path}")

        os.remove(local_audio_path)
        print(f"Cleaned up local audio file: {local_audio_path}")

    print("\n--- All entries processed for all models ---")
