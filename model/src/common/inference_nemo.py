"""NeMo SALM x GCS-manifest ASR inference.

Iterates a JSONL manifest of GCS-hosted audio segments and runs a NeMo SALM
model batch by batch. The caller passes the loaded SALM model in (it ships in
the NeMo container). Requires torch (declared via the [hf] extra).
"""

import logging
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

from google.cloud import storage

from common.gcs_utils import download_to_scratch

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
        msg = (
            "inference_nemo requires torch: "
            "pip install 'radio-transcription-model[hf]'"
        )
        raise ImportError(msg) from _TORCH_MISSING


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
    limit: int | None = None,
    preprocess_fn: Callable[[str, str], bool] | None = None,
) -> list[dict[str, Any]]:
    """Run a batch inference pipeline for a NeMo-compatible model.

    The public signature is kept stable so the evaluation notebooks that
    import this function keep working unchanged.

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
    logger.info(
        f"--- Starting batch processing of {len(manifest_data)} entries ---"
    )

    for i in range(0, len(manifest_data), batch_size):
        batch = manifest_data[i : i + batch_size]
        with tempfile.TemporaryDirectory(prefix="asr_dl_") as scratch_dir:
            prompts = []
            batch_entries = []

            for entry in batch:
                audio_gcs_uri = entry["audio_filepath"]
                try:
                    local_path = download_to_scratch(
                        storage_client, audio_gcs_uri, scratch_dir
                    )

                    current_path = local_path
                    if preprocess_fn:
                        path = Path(local_path)
                        preprocessed_path = str(
                            path.with_name(f"{path.stem}_prep.wav")
                        )
                        if preprocess_fn(local_path, preprocessed_path):
                            current_path = preprocessed_path
                        else:
                            logger.warning(
                                f"Preprocessing failed for {local_path}, "
                                f"using original."
                            )

                    batch_entries.append(entry)
                    prompts.append(prompt_fn(entry, current_path))
                except Exception as e:
                    logger.exception(f"Failed to process {audio_gcs_uri}: {e}")
                    continue

            if not prompts:
                continue

            logger.info(f"Processing batch {i // batch_size + 1}...")
            with torch.no_grad():
                outputs = inference_fn(model, prompts)

            for j, ans in enumerate(outputs):
                transcript = decode_fn(ans, model)
                result_row = dict(batch_entries[j])
                result_row[f"pred_text_{selected_model}"] = transcript.strip()
                results_list.append(result_row)
        # scratch_dir and every file in it are removed here — each iteration,
        # including when inference_fn raises (the exception then propagates,
        # preserving today's fail-loud behavior).

    logger.info(f"Processed {len(results_list)} results.")
    return results_list
