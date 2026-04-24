import os
import json
import requests
from google.cloud import storage
import logging

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
    """Downloads and parses a JSON or JSONL manifest from GCS."""
    bucket_name, blob_path = _parse_gcs_uri(gcs_manifest_uri)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    content = blob.download_as_text().strip()
    
    # Try parsing as standard JSON first (list of dicts)
    try:
        data = json.loads(content)
        if isinstance(data, list):
            logger.info(f"Downloaded and parsed {len(data)} entries from JSON file {gcs_manifest_uri}.")
            return data
    except json.JSONDecodeError:
        pass # Fall back to JSONL
        
    # Fallback to JSONL
    manifest_entries = []
    for line in content.split('\n'):
        if line.strip():
            manifest_entries.append(json.loads(line))
    logger.info(f"Downloaded and parsed {len(manifest_entries)} entries from JSONL file {gcs_manifest_uri}.")
    return manifest_entries

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
    Outputs a JSONL file with predictions, omitting ground truth.
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

    # Track segment index per audio file
    segment_counters = {}
    results_list = []

    for i, entry in enumerate(manifest_data):
        audio_gcs_uri = entry["audio_filepath"]
        offset = entry.get("offset", 0.0)
        duration = entry.get("duration", 0.0)
        
        # Initialize or increment counter for this file
        if audio_gcs_uri not in segment_counters:
            segment_counters[audio_gcs_uri] = 0
        else:
            segment_counters[audio_gcs_uri] += 1
            
        seg_index = segment_counters[audio_gcs_uri]
        seg_str = f"seg{seg_index:03d}"

        audio_bucket, audio_blob_path = _parse_gcs_uri(audio_gcs_uri)
        file_name = os.path.splitext(os.path.basename(audio_blob_path))[0]
        
        segmented_blob_path = f"segmented_audio/{project_name}_audio/{file_name}/{file_name}__{seg_str}.flac"
        
        logger.info(f"[{i+1}/{len(manifest_data)}] Fetching segment: {segmented_blob_path}")

        local_segment_path = f"./temp_{file_name}__{seg_str}.flac"
        
        try:
            if verbose:
                logger.info(f"Downloading segment from GCS...")
            download_blob_to_file(storage_client, audio_bucket, segmented_blob_path, local_segment_path)
            
            if verbose:
                logger.info(f"Transcribing with {model_name}...")
            transcription_result = transcribe_audio_with_model(local_segment_path, endpoint_url, prompt, verbose=verbose)
            
            if verbose:
                logger.info(f"Transcription result: {transcription_result}")
        except Exception as e:
            logger.error(f"Error during processing/transcription for {segmented_blob_path}: {e}")
            transcription_result = "[ERROR]"

        # Construct result row WITHOUT ground truth 'text'
        result_row = {
            "audio_filepath": audio_gcs_uri,
            "offset": offset,
            "duration": duration,
            f"pred_text_{selected_model}": transcription_result
        }
        results_list.append(result_row)

        # Cleanup local segment
        if os.path.exists(local_segment_path):
            os.remove(local_segment_path)

    # Write all results to a single JSONL file
    local_jsonl_path = f"./{project_name}_{selected_model}_{experiment_name}.jsonl"
    with open(local_jsonl_path, "w", encoding="utf-8") as f:
        for row in results_list:
            f.write(json.dumps(row) + "\n")
            
    logger.info(f"Wrote {len(results_list)} results to {local_jsonl_path}")

    # Upload JSONL file
    if not no_upload:
        result_gcs_blob_path = f"transcripts/{project_name}_audio/{selected_model}/{experiment_name}/{project_name}_{selected_model}_{experiment_name}.jsonl"
        logger.info(f"Uploading combined JSONL to gs://{gcs_bucket}/{result_gcs_blob_path}")
        upload_file_to_blob(storage_client, gcs_bucket, result_gcs_blob_path, local_jsonl_path)
    else:
        logger.info(f"Skipping upload for {local_jsonl_path} (no_upload=True)")
        logger.info("--- Final JSONL Content Start ---")
        with open(local_jsonl_path, "r", encoding="utf-8") as f:
            for line in f:
                logger.info(line.strip())
        logger.info("--- Final JSONL Content End ---")
    logger.info(f"--- All entries processed for model {model_name} ---")
