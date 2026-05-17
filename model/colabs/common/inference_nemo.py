"""NeMo-based ASR batch inference pipeline. Requires the [hf] extra (torch)."""

import os
import json
import logging
from typing import Callable, Any, Optional
from google.cloud import storage
from common.gcs_utils import (
    parse_gcs_uri,
    download_blob_to_file,
)

logger = logging.getLogger(__name__)

try:
    import torch
except ImportError as _e:
    _TORCH_MISSING = _e
else:
    _TORCH_MISSING = None


def _require_torch() -> None:
    """Raise a clear error if torch is not installed."""
    if _TORCH_MISSING:
        raise ImportError(
            "inference_nemo requires torch: pip install 'common[hf]'"
        ) from _TORCH_MISSING


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
    """Run a batch inference pipeline for a NeMo-compatible model.

    Moved verbatim from ``inference_pipeline_runner.py`` lines 16-130. Signature is
    preserved exactly to avoid breaking notebook imports (Pitfall 7).

    Args:
        model: The loaded model instance. Model-agnostic — delegates inference and
            decoding to the provided callables (NeMo SALM, Whisper, etc.).
        manifest_data: List of manifest entries. Assumes 'audio_filepath' points to a
            GCS URI for an already-segmented audio file.
        prompt_fn: Callable(entry, local_path) -> prompt structure.
        inference_fn: Callable(model, prompts) -> list of raw outputs.
        decode_fn: Callable(output, model) -> str (transcription).
        storage_client: GCS storage client.
        project_name: Name of the project for path derivation.
        selected_model: Name of the model (used for output keys).
        batch_size: Number of files to process in parallel.
        limit: Limit the number of entries to process.
        preprocess_fn: Optional callable to preprocess audio before prompting.
            Callable(input_path, output_path) -> bool.

    Returns:
        List of result dicts, each being a copy of the manifest entry with an added
        ``pred_text_{selected_model}`` key.
    """
    _require_torch()

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
                    # Cleanup prep wav if preprocess_fn already wrote it
                    preprocessed_path = f"/tmp/temp_prep_{file_name}.wav"
                    if os.path.exists(preprocessed_path):
                        os.remove(preprocessed_path)
                    continue

        if not prompts:
            continue

        # Run inference
        logger.info(f"Processing batch {i // batch_size + 1}...")
        with torch.no_grad():
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
            name = os.path.basename(local_path)  # temp_prep_<stem>.wav
            stem = name.removeprefix("temp_prep_").removesuffix(".wav")
            raw_flac_path = f"/tmp/temp_{stem}.flac"
            if os.path.exists(raw_flac_path):
                os.remove(raw_flac_path)

    logger.info(f"Processed {len(results_list)} results.")
    return results_list
