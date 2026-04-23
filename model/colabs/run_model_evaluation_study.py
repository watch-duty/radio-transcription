import os
import json
import requests
from google.cloud import storage
import logging
from pydub import AudioSegment

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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
    logger.info(f"Blob {blob_path} downloaded from bucket {bucket_name} to {destination_file_name}.")

def upload_file_to_blob(storage_client, bucket_name: str, blob_path: str, source_file_name: str):
    """Uploads a local file to a GCS blob."""
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.upload_from_filename(source_file_name)
    logger.info(f"File {source_file_name} uploaded to gs://{bucket_name}/{blob_path}.")

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
    logger.info(f"Downloaded and parsed {len(manifest_entries)} entries from {gcs_manifest_uri}.")
    return manifest_entries

def splice_audio(audio_segment: AudioSegment, offset: float, duration: float, output_path: str) -> str:
    """Splices an AudioSegment and saves it to a file."""
    start_ms = int(offset * 1000)
    end_ms = int((offset + duration) * 1000)
    segment = audio_segment[start_ms:end_ms]
    segment.export(output_path, format="wav")
    return output_path

def transcribe_audio_with_model(audio_file_path: str, endpoint_url: str, prompt: str = None, verbose: bool = False) -> str:
    """Sends an audio file to a self-contained STT model FastAPI endpoint."""
    with open(audio_file_path, "rb") as f:
        data = {"prompt": prompt} if prompt else None
        response = requests.post(endpoint_url, files={"file": f}, data=data)
        
    if verbose:
        logger.info(f"Response Status: {response.status_code}")
        logger.info(f"Response Body: {response.text}")
        
    if response.status_code == 200:
        res_json = response.json()
        return res_json.get("transcription") or res_json.get("text") or ""
    else:
        raise RuntimeError(f"FastAPI inference failed with status code {response.status_code}: {response.text}")

def run_pipeline(gcp_project_id: str, gcs_manifest_uri: str, gcs_bucket: str, project_name: str, experiment_name: str, model_endpoints: dict[str, str], selected_model: str, prompt: str = None, verbose: bool = False, no_upload: bool = False, limit: int = None):
    """
    Runs the transcription evaluation pipeline for a selected model.
    Outputs will be saved dynamically under:
    gs://{GCS_BUCKET}/transcripts/{PROJECT_NAME}_audio/{MODEL_NAME}/{EXPERIMENT_NAME}/
    """
    storage_client = storage.Client(project=gcp_project_id)
    manifest_data = download_jsonl_manifest(storage_client, gcs_manifest_uri)
    
    if limit is not None:
        logger.info(f"Limiting processing to first {limit} entries (mostly used for testing).")
        manifest_data = manifest_data[:limit]
        
    logger.info(f"--- Starting processing of {len(manifest_data)} entries ---")

    if selected_model not in model_endpoints:
        raise ValueError(f"Selected model '{selected_model}' not found in model_endpoints.")

    endpoint_url = model_endpoints[selected_model]
    model_name = selected_model

    current_audio_gcs_uri = None
    local_audio_path = None
    full_audio = None

    for i, entry in enumerate(manifest_data):
        audio_gcs_uri = entry["audio_filepath"]
        offset = entry.get("offset", 0.0)
        duration = entry.get("duration", 0.0)
        
        logger.info(f"[{i+1}/{len(manifest_data)}] Processing segment from: {audio_gcs_uri} (offset: {offset}, duration: {duration})")

        audio_bucket, audio_blob_path = _parse_gcs_uri(audio_gcs_uri)
        audio_file_name = os.path.basename(audio_blob_path)

        # Cache check
        if audio_gcs_uri != current_audio_gcs_uri:
            # Clean up previous file if it exists
            if local_audio_path and os.path.exists(local_audio_path):
                os.remove(local_audio_path)
                if verbose:
                    logger.info(f"Cleaned up previous cached audio: {local_audio_path}")

            local_audio_path = f"./temp_full_{audio_file_name}"
            
            if verbose:
                logger.info(f"Downloading new file {audio_blob_path} from GCS...")
            download_blob_to_file(storage_client, audio_bucket, audio_blob_path, local_audio_path)
            
            # Load full audio for splicing
            if verbose:
                logger.info(f"Loading full audio into memory for splicing...")
            full_audio = AudioSegment.from_file(local_audio_path)
            current_audio_gcs_uri = audio_gcs_uri
        else:
            if verbose:
                logger.info(f"Using cached audio file for {audio_file_name}")

        # Splice audio using utility function
        segment_path = f"./temp_seg_{i}_{audio_file_name}"
        if verbose:
            logger.info(f"Splicing segment: {offset}s to {offset+duration}s")
        splice_audio(full_audio, offset, duration, segment_path)

        if verbose:
            logger.info(f"Transcribing with {model_name}...")
        try:
            transcription_result = transcribe_audio_with_model(segment_path, endpoint_url, prompt, verbose=verbose)
        except Exception as e:
            logger.error(f"Error during transcription for {model_name}: {e}")
            transcription_result = "[ERROR]"

        # Save result
        local_result_file_name = f"{os.path.splitext(audio_file_name)[0]}_seg_{i}.txt"
        local_result_path = f"./{local_result_file_name}"
        with open(local_result_path, "w") as f:
            f.write(transcription_result)

        result_gcs_blob_path = f"transcripts/{project_name}_audio/{model_name}/{experiment_name}/{local_result_file_name}"
        
        if not no_upload:
            if verbose:
                logger.info(f"Uploading result to gs://{gcs_bucket}/{result_gcs_blob_path}")
            upload_file_to_blob(storage_client, gcs_bucket, result_gcs_blob_path, local_result_path)
        else:
            if verbose:
                logger.info(f"Skipping upload for {local_result_file_name} (no_upload=True)")

        # Cleanup segment
        os.remove(local_result_path)
        os.remove(segment_path)

    # Final cleanup of cached file
    if local_audio_path and os.path.exists(local_audio_path):
        os.remove(local_audio_path)
        if verbose:
            logger.info(f"Cleaned up final cached audio: {local_audio_path}")

    logger.info(f"--- All entries processed for model {model_name} ---")
