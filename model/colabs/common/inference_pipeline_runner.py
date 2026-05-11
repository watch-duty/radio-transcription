import os
import json
import torch
import logging
from typing import Callable, Any, Optional
from google.cloud import storage
from common.gcs_utils import (
    parse_gcs_uri,
    download_blob_to_file,
    upload_file_to_blob,
)

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
    preprocess_fn: Optional[Callable[[str, str], bool]] = None,
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
    cached_downloads = {}

    logger.info(
        f"--- Starting batch processing of {len(manifest_data)} entries ---"
    )

    for i in range(0, len(manifest_data), batch_size):
        batch = manifest_data[i : i + batch_size]
        prompts = []
        batch_entries = []

        # Prepare batch
        for entry in batch:
            audio_gcs_uri = entry["audio_filepath"]
            if audio_gcs_uri in cached_downloads:
                current_path = cached_downloads[audio_gcs_uri]
                batch_entries.append(entry)
                prompts.append(prompt_fn(entry, current_path))
            else:
                audio_bucket, audio_blob_path = parse_gcs_uri(audio_gcs_uri)
                file_name = os.path.splitext(os.path.basename(audio_blob_path))[
                    0
                ]
                local_path = f"/tmp/temp_{file_name}.flac"
                try:
                    download_blob_to_file(
                        storage_client,
                        audio_bucket,
                        audio_blob_path,
                        local_path,
                    )

                    current_path = local_path
                    if preprocess_fn:
                        preprocessed_path = f"/tmp/temp_prep_{file_name}.wav"
                        if preprocess_fn(local_path, preprocessed_path):
                            current_path = preprocessed_path
                        else:
                            logger.warning(
                                f"Preprocessing failed for {local_path}, using original."
                            )

                    cached_downloads[audio_gcs_uri] = current_path
                    batch_entries.append(entry)
                    prompts.append(prompt_fn(entry, current_path))
                except Exception as e:
                    logger.error(f"Failed to process {audio_blob_path}: {e}")
                    # Cleanup raw flac if download succeeded but preprocessing failed
                    if os.path.exists(local_path):
                        os.remove(local_path)
                    continue

        if not prompts:
            continue

        # Run inference
        logger.info(f"Processing batch {i // batch_size + 1}...")
        with torch.no_grad():
            # Use the provided inference function
            outputs = inference_fn(model, prompts)

        # Decode and store results
        for j, ans in enumerate(outputs):
            transcript = decode_fn(ans, model)
            result_row = dict(batch_entries[j])
            result_row[f"pred_text_{selected_model}"] = transcript.strip()
            results_list.append(result_row)

    # Cleanup all cached local downloads at the very end of the entire run
    for local_path in cached_downloads.values():
        if os.path.exists(local_path):
            os.remove(local_path)
        # Also clean up the raw FLAC file if preprocessing created a WAV file
        if "_prep_" in local_path:
            raw_flac_path = local_path.replace("_prep_", "").replace(
                ".wav", ".flac"
            )
            if os.path.exists(raw_flac_path):
                os.remove(raw_flac_path)

    logger.info(f"Processed {len(results_list)} results.")
    return results_list


def run_huggingface_inference_pipeline(
    model: Any,
    processor: Any,
    manifest_data: list[dict[str, Any]],
    storage_client: storage.Client,
    project_name: str,
    selected_model: str,
    batch_size: int = 4,
    limit: Optional[int] = None,
    preprocess_fn: Optional[Callable[[str, str], bool]] = None,
    max_new_tokens: int = 256,
    text_prompt: str = "transcribe",
    processor_kwargs: Optional[dict] = None,
) -> list[dict[str, Any]]:
    """
    Runs a highly-optimized parallel GPU batch inference specifically for Hugging Face Speech models.
    Handles GCS audio downloading, resampling, multi-modal padding, and parallel decoding natively.
    """
    import torchaudio
    import torch

    if limit:
        manifest_data = manifest_data[:limit]

    results_list = []
    logger.info(
        f"--- Starting parallel Hugging Face ASR pipeline for {len(manifest_data)} entries ---"
    )

    for i in range(0, len(manifest_data), batch_size):
        batch = manifest_data[i : i + batch_size]
        local_files = []
        batch_entries = []
        audios = []

        # Download and preprocess in parallel
        for entry in batch:
            audio_gcs_uri = entry["audio_filepath"]
            audio_bucket, audio_blob_path = parse_gcs_uri(audio_gcs_uri)
            file_name = os.path.splitext(os.path.basename(audio_blob_path))[0]
            local_path = f"/tmp/temp_{file_name}.flac"
            try:
                download_blob_to_file(
                    storage_client, audio_bucket, audio_blob_path, local_path
                )
                local_files.append(local_path)

                current_path = local_path
                if preprocess_fn:
                    preprocessed_path = f"/tmp/temp_prep_{file_name}.wav"
                    if preprocess_fn(local_path, preprocessed_path):
                        current_path = preprocessed_path
                        local_files.append(preprocessed_path)
                    else:
                        logger.warning(
                            f"Preprocessing failed for {local_path}, using original."
                        )

                # Load audio waveform at 16kHz using torchaudio
                waveform, sr = torchaudio.load(current_path)
                if sr != 16000:
                    import torchaudio.transforms as T

                    waveform = T.Resample(orig_freq=sr, new_freq=16000)(
                        waveform
                    )

                # Averaging stereo/multi-channel to mono
                if waveform.shape[0] > 1:
                    waveform = torch.mean(waveform, dim=0, keepdim=True)

                audios.append(waveform.squeeze(0).numpy())
                batch_entries.append(entry)
            except Exception as e:
                logger.error(
                    f"Failed to download or preprocess {audio_blob_path}: {e}"
                )
                continue

        if not audios:
            continue

        try:
            # 1. Format multi-modal inputs with padding enabled
            inputs = processor(
                audio=audios,
                sampling_rate=16000,
                text=[text_prompt] * len(audios),
                padding=True,
                return_tensors="pt",
                **(processor_kwargs or {}),
            )
            inputs.to(model.device, dtype=model.dtype)

            # 2. Execute parallel forward pass on the GPU
            logger.info(
                f"Processing parallel GPU batch {i // batch_size + 1}..."
            )
            with torch.no_grad():
                outputs = model.generate(
                    **inputs, max_new_tokens=max_new_tokens
                )

            # 3. Decode the entire batch together (slicing prompt tokens if present)
            if "input_ids" in inputs:
                new_tokens = outputs[:, inputs["input_ids"].shape[-1] :]
            else:
                new_tokens = outputs
            transcripts = processor.batch_decode(
                new_tokens, skip_special_tokens=True
            )

            # 4. Store results
            for j, row in enumerate(batch_entries):
                result_row = dict(row)
                result_row[f"pred_text_{selected_model}"] = transcripts[
                    j
                ].strip()
                results_list.append(result_row)

        except Exception as e:
            logger.warning(
                f"Batch parallel GPU execution failed: {e}. "
                f"Gracefully falling back to sequential GPU inference (batch size: {len(batch_entries)}) to prevent batch crash."
            )
            # Sequential fallback to ensure the batch never crashes the run
            for j, row in enumerate(batch_entries):
                try:
                    inputs = processor(
                        audio=audios[j],
                        sampling_rate=16000,
                        text=text_prompt,
                        return_tensors="pt",
                        **(processor_kwargs or {}),
                    )
                    inputs.to(model.device, dtype=model.dtype)
                    with torch.no_grad():
                        out = model.generate(
                            **inputs, max_new_tokens=max_new_tokens
                        )
                    if "input_ids" in inputs:
                        new_tokens = out[0, inputs["input_ids"].shape[-1] :]
                    else:
                        new_tokens = out[0]
                    pred = processor.decode(
                        new_tokens, skip_special_tokens=True
                    )
                    result_row = dict(row)
                    result_row[f"pred_text_{selected_model}"] = pred.strip()
                    results_list.append(result_row)
                except Exception as ex:
                    logger.error(
                        f"Sequential fallback failed for row {j}: {ex}"
                    )

        # Cleanup local files
        for local_path in local_files:
            if os.path.exists(local_path):
                os.remove(local_path)

    logger.info(f"Processed {len(results_list)} parallel results.")
    return results_list


def run_test_baseline_inference_evaluation(
    model,
    prompt_fn,
    inference_fn,
    decode_fn,
    normalize_fn=None,
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
    import re

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

    logger.info(
        f"Running inference on {num_examples} examples with batch size {batch_size}..."
    )

    # Use default normalizer if none provided
    if normalize_fn is None:

        def default_normalize(text):
            text = text.upper()
            text = re.sub(r"[^A-Z\s]", "", text)
            return re.sub(r"\s+", " ", text).strip()

        normalize_fn = default_normalize

    def process_current_batch():
        nonlocal \
            batch_prompts, \
            batch_refs, \
            batch_temp_paths, \
            predictions, \
            references
        if not batch_prompts:
            return
        try:
            with torch.no_grad():
                outputs = inference_fn(model, batch_prompts)

            for j, out in enumerate(outputs):
                if out != "[ERROR]":
                    pred = decode_fn(out, model)

                    # Apply normalization
                    pred_norm = normalize_fn(pred)
                    ref_norm = normalize_fn(batch_refs[j])

                    predictions.append(pred_norm)
                    references.append(ref_norm)
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

        with tempfile.NamedTemporaryFile(
            suffix=".wav", delete=False
        ) as temp_file:
            temp_path = temp_file.name
            sf.write(temp_path, audio_array, sampling_rate)

        batch_temp_paths.append(temp_path)
        batch_prompts.append(prompt_fn(None, temp_path))
        batch_refs.append(reference_text)

        if len(batch_prompts) == batch_size:
            process_current_batch()
            if i % (batch_size * 2) == 0:
                logger.info(f"Processed {i + 1} examples...")

    # Process any remaining items in the last batch
    process_current_batch()

    wer_score = None
    if predictions:
        wer_score = wer.compute(predictions=predictions, references=references)
        logger.info(f"WER on {len(predictions)} examples: {wer_score}")

    return wer_score, predictions, references
