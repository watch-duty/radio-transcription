import os
import json
import torch
import logging
from typing import Callable, Any, Optional
from google.cloud import storage
from .gcs_utils import parse_gcs_uri, download_blob_to_file, upload_file_to_blob

logger = logging.getLogger(__name__)

def run_inference_pipeline(
    model: Any, 
    manifest_data: list[dict[str, Any]], 
    prompt_fn: Callable[[dict[str, Any], str], Any], 
    inference_fn: Callable[[Any, list[Any]], list[Any]],
    decode_fn: Callable[[Any, Any], str], 
    storage_client: storage.Client,
    project_name: str,
    selected_model: str,
    batch_size: int = 4,
    limit: Optional[int] = None,
    preprocess_fn: Optional[Callable[[str, str], bool]] = None
) -> list[dict[str, Any]]:
    """
    Runs a batch inference for a model.
    
    Args:
        model: The loaded model instance. This function is model-agnostic and can be used with various model types (e.g., NeMo models like SALM, Whisper, etc.) as it delegates inference and decoding to the provided callables.
        manifest_data: List of manifest entries. Assumes 'audio_filepath' points to a separate audio file already segmented.
        prompt_fn: Callable(entry, local_path) -> prompt structure.
        inference_fn: Callable(model, prompts) -> list of raw outputs.
        decode_fn: Callable(output, model) -> str (transcription).
        storage_client: GCS storage client.
        project_name: Name of the project for path derivation.
        selected_model: Name of the model (used for output keys).
        batch_size: Number of files to process in parallel.
        limit: Limit the number of entries to process.
        preprocess_fn: Optional callable to preprocess audio file before prompting. Callable(input_path, output_path) -> bool.
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
            audio_bucket, audio_blob_path = parse_gcs_uri(audio_gcs_uri)
            # Use the file name directly for local storage
            file_name = os.path.splitext(os.path.basename(audio_blob_path))[0]
            local_path = f"./temp_{file_name}.flac"
            try:
                download_blob_to_file(storage_client, audio_bucket, audio_blob_path, local_path)
                local_files.append(local_path)
                
                current_path = local_path
                if preprocess_fn:
                    preprocessed_path = f"./temp_prep_{file_name}.wav"
                    if preprocess_fn(local_path, preprocessed_path):
                        current_path = preprocessed_path
                        local_files.append(preprocessed_path)
                    else:
                        logger.warning(f"Preprocessing failed for {local_path}, using original.")
                
                batch_entries.append(entry)
                # Generate prompt for this model
                prompts.append(prompt_fn(entry, current_path))
            except Exception as e:
                logger.error(f"Failed to process {audio_blob_path}: {e}")
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


def run_test_baseline_inference_evaluation(
    model,
    prompt_fn,
    inference_fn,
    decode_fn,
    dataset_name="librispeech_asr",
    dataset_config="clean",
    split="test",
    num_examples=20,
    batch_size=4,
):
    """Runs a baseline evaluation on a public dataset (e.g. Librispeech) using streaming and batching."""
    from datasets import load_dataset
    from evaluate import load
    import tempfile
    import soundfile as sf

    logger.info(f"Loading dataset {dataset_name} in streaming mode...")
    dataset = load_dataset(
        dataset_name, dataset_config, split=split, streaming=True
    )
    wer = load("wer")

    if num_examples and num_examples > 0:
        dataset = dataset.take(num_examples)

    predictions = []
    references = []

    batch_prompts = []
    batch_refs = []
    batch_temp_paths = []

    logger.info(f"Running inference on {num_examples} examples with batch size {batch_size}...")

    def process_current_batch():
        nonlocal batch_prompts, batch_refs, batch_temp_paths, predictions, references
        if not batch_prompts:
            return
        try:
            with torch.no_grad():
                outputs = inference_fn(model, batch_prompts)

            for j, out in enumerate(outputs):
                if out != "[ERROR]":
                    pred = decode_fn(out, model)
                    predictions.append(pred.strip())
                    references.append(batch_refs[j].strip())
                else:
                    logger.error(f"Error processing example in batch")

        except Exception as e:
            logger.error(f"Failed processing batch: {e}")
        finally:
            # Cleanup temp files for this batch
            for tp in batch_temp_paths:
                if os.path.exists(tp):
                    os.remove(tp)
            
            # Reset batch accumulators
            batch_prompts = []
            batch_refs = []
            batch_temp_paths = []

    for i, example in enumerate(dataset):
        audio_array = example["audio"]["array"]
        sampling_rate = example["audio"]["sampling_rate"]
        reference_text = example["text"]

        with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as temp_file:
            temp_path = temp_file.name
            sf.write(temp_path, audio_array, sampling_rate)
            
        batch_temp_paths.append(temp_path)
        batch_prompts.append(prompt_fn(None, temp_path))
        batch_refs.append(reference_text)

        if len(batch_prompts) == batch_size:
            process_current_batch()
            if i % (batch_size * 2) == 0:
                 logger.info(f"Processed {i+1} examples...")

    # Process any remaining items in the last batch
    process_current_batch()

    wer_score = None
    if predictions:
        wer_score = wer.compute(predictions=predictions, references=references)
        logger.info(f"WER on {len(predictions)} examples: {wer_score}")

    return wer_score, predictions, references