import os
import json
import logging
from typing import Any
from google.cloud import storage

logger = logging.getLogger(__name__)

def parse_gcs_uri(gcs_uri: str) -> tuple[str, str]:
    """Parses a GCS URI into bucket name and blob path."""
    if not gcs_uri.startswith("gs://"):
        raise ValueError("GCS URI must start with 'gs://'")
    parts = gcs_uri[len("gs://"):].split('/', 1)
    bucket_name = parts[0]
    blob_path = parts[1] if len(parts) > 1 else ''
    return bucket_name, blob_path

def download_blob_to_file(storage_client: storage.Client, bucket_name: str, blob_path: str, destination_file_name: str) -> None:
    """Downloads a blob from GCS to a local file."""
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.download_to_filename(destination_file_name)
    logger.info(f"Downloaded {blob_path} to {destination_file_name}")

def upload_file_to_blob(storage_client: storage.Client, bucket_name: str, blob_path: str, source_file_name: str) -> None:
    """Uploads a local file to a GCS blob."""
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.upload_from_filename(source_file_name)
    logger.info(f"File {source_file_name} uploaded to gs://{bucket_name}/{blob_path}.")

def download_jsonl_manifest(storage_client: storage.Client, gcs_manifest_uri: str) -> list[dict[str, Any]]:
    """Downloads and parses a JSONL manifest from GCS."""
    bucket_name, blob_path = parse_gcs_uri(gcs_manifest_uri)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    content = blob.download_as_text().strip()
    
    manifest_entries = []
    for line in content.split('\n'):
        if line.strip():
            manifest_entries.append(json.loads(line))
    logger.info(f"Downloaded {len(manifest_entries)} entries from {gcs_manifest_uri}")
    return manifest_entries

def upload_inference_results(storage_client: storage.Client, bucket_name: str, project_name: str, model_name: str, experiment_name: str, results_list: list[dict[str, Any]]) -> str:
    """
    Uploads inference results directly from memory to GCS using the standard path structure.
    """
    blob_path = f"inference_manifests/{project_name}/{model_name}/{experiment_name}/{project_name}_{model_name}_{experiment_name}.jsonl"
    
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    
    # Convert list of dicts to JSONL string in memory
    jsonl_content = "\n".join(json.dumps(row) for row in results_list) + "\n"
    
    blob.upload_from_string(jsonl_content, content_type="application/jsonl")
    
    logger.info(f"Uploaded results to gs://{bucket_name}/{blob_path}")
    return f"gs://{bucket_name}/{blob_path}"
