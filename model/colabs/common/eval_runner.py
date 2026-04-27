import os
import json
import torch
import logging
from .gcs_utils import parse_gcs_uri, download_blob_to_file

logger = logging.getLogger(__name__)

def run_batch_evaluation(
    model, 
    manifest_data, 
    prompt_fn, 
    inference_fn,
    decode_fn, 
    storage_client,
    project_name,
    selected_model,
    batch_size=4,
    limit=None
):
    """
    Runs a batch evaluation for a model.
    
    Args:
        model: The loaded model instance.
        manifest_data: List of manifest entries.
        prompt_fn: Callable(entry, local_path) -> prompt structure.
        inference_fn: Callable(model, prompts) -> list of raw outputs.
        decode_fn: Callable(output, model) -> str (transcription).
        storage_client: GCS storage client.
        project_name: Name of the project for path derivation.
        selected_model: Name of the model (used for output keys).
        batch_size: Number of files to process in parallel.
        limit: Limit the number of entries to process.
    """
    if limit:
        manifest_data = manifest_data[:limit]
        
    results_list = []
    segment_counters = {}
    
    logger.info(f"--- Starting batch processing of {len(manifest_data)} entries ---")
    
    for i in range(0, len(manifest_data), batch_size):
        batch = manifest_data[i:i+batch_size]
        prompts = []
        local_files = []
        batch_entries = []
        
        # Prepare batch
        for entry in batch:
            audio_gcs_uri = entry["audio_filepath"]
            
            if audio_gcs_uri not in segment_counters:
                segment_counters[audio_gcs_uri] = 0
            else:
                segment_counters[audio_gcs_uri] += 1
                
            seg_index = segment_counters[audio_gcs_uri]
            seg_str = f"seg{seg_index:03d}"
    
            audio_bucket, audio_blob_path = parse_gcs_uri(audio_gcs_uri)
            file_name = os.path.splitext(os.path.basename(audio_blob_path))[0]
            
            segmented_blob_path = f"segmented_audio/{project_name}_audio/{file_name}/{file_name}__{seg_str}.flac"
            local_path = f"./temp_{file_name}__{seg_str}.flac"
            
            try:
                download_blob_to_file(storage_client, audio_bucket, segmented_blob_path, local_path)
                local_files.append(local_path)
                batch_entries.append(entry)
                
                # Generate prompt for this model
                prompts.append(prompt_fn(entry, local_path))
            except Exception as e:
                logger.error(f"Failed to process {segmented_blob_path}: {e}")
                continue
            
        if not prompts:
            continue
            
        # Run inference
        logger.info(f"Processing batch {i//batch_size + 1}...")
        with torch.no_grad():
            # Use the provided inference function
            outputs = inference_fn(model, prompts)
            
        # Decode and store results
        for j, ans in enumerate(outputs):
            transcript = decode_fn(ans, model)
            result_row = dict(batch_entries[j])
            result_row[f"pred_text_{selected_model}"] = transcript.strip()
            results_list.append(result_row)
            
        # Cleanup local files
        for local_path in local_files:
            if os.path.exists(local_path):
                os.remove(local_path)
                
    logger.info(f"Processed {len(results_list)} results.")
    return results_list
