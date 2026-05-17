"""Hugging Face ASR parallel GPU batch inference pipeline. Requires the [hf] extra."""

import os
import logging
from typing import Any, Optional, Callable
from google.cloud import storage
from common.gcs_utils import parse_gcs_uri, download_blob_to_file
from common.audio_utils import preprocess_audio_for_model

logger = logging.getLogger(__name__)

try:
    import torch
    import torchaudio
    import torchaudio.transforms as T
except ImportError as _e:
    _HF_MISSING = _e
else:
    _HF_MISSING = None


def _require_hf() -> None:
    """Raise a clear error if the [hf] extra is not installed."""
    if _HF_MISSING:
        raise ImportError(
            "inference_hf requires the [hf] extra: pip install 'common[hf]'"
        ) from _HF_MISSING


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
    """Run a parallel GPU batch inference pipeline for Hugging Face Speech models.

    Moved verbatim from ``inference_pipeline_runner.py`` lines 133-294. Signature is
    preserved exactly to avoid breaking notebook imports (Pitfall 7). The inline
    torchaudio resample+mono block (original lines 191-203) is replaced with a call to
    ``preprocess_audio_for_model`` from ``common.audio_utils`` (D-05 deduplication).

    Handles GCS audio downloading, resampling, multi-modal padding, and parallel
    decoding natively. Falls back to sequential inference per item if the batched
    forward pass fails.

    Args:
        model: Loaded Hugging Face speech model.
        processor: Corresponding Hugging Face processor.
        manifest_data: List of manifest entries with 'audio_filepath' GCS URIs.
        storage_client: GCS storage client.
        project_name: Name of the project for path derivation.
        selected_model: Model name used for output key construction.
        batch_size: Number of audio files per parallel GPU batch.
        limit: Optional cap on total entries to process.
        preprocess_fn: Optional callable(input_path, output_path) -> bool for audio
            pre-processing before loading waveform.
        max_new_tokens: Maximum tokens to generate per transcription.
        text_prompt: Text prompt passed to the processor.
        processor_kwargs: Additional keyword arguments for the processor call.

    Returns:
        List of result dicts, each being a copy of the manifest entry with an added
        ``pred_text_{selected_model}`` key.
    """
    _require_hf()

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

                # Load audio waveform at 16kHz — uses preprocess_audio_for_model
                # for resample+mono (D-05: deduplicates inline torchaudio block)
                resampled_path = f"/tmp/temp_resampled_{file_name}.wav"
                if preprocess_audio_for_model(
                    current_path, resampled_path, target_sr=16000
                ):
                    waveform, sr = torchaudio.load(resampled_path)
                    local_files.append(resampled_path)
                else:
                    # Fallback: load and resample inline if preprocessing failed
                    waveform, sr = torchaudio.load(current_path)
                    if sr != 16000:
                        waveform = T.Resample(orig_freq=sr, new_freq=16000)(
                            waveform
                        )
                    if waveform.shape[0] > 1:
                        waveform = torch.mean(waveform, dim=0, keepdim=True)

                # Averaging stereo/multi-channel to mono (ensure mono after load)
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
