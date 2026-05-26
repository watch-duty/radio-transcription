"""Hugging Face transformers × GCS-manifest ASR inference.

Iterates a JSONL manifest of GCS-hosted audio segments, downloads each into a
scoped temp dir, preprocesses (resample + mono), and runs the loaded HF model
in parallel-GPU batches with sequential single-item fallback. Distinct from
``public_dataset_evaluation`` (same HF model framework but a streaming HF
dataset as the data source). Requires the [hf] extra.
"""

import os
import logging
import tempfile
from typing import Any, Optional, Callable
from google.cloud import storage
from common.gcs_utils import download_to_scratch
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

    The public signature is kept stable so the evaluation notebooks that
    import this function keep working unchanged. Resample-and-mono conversion
    is delegated to ``preprocess_audio_for_model`` from ``common.audio_utils``.

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
        with tempfile.TemporaryDirectory(prefix="asr_dl_") as scratch_dir:
            batch_entries = []
            audios = []

            for entry in batch:
                audio_gcs_uri = entry["audio_filepath"]
                try:
                    local_path = download_to_scratch(
                        storage_client, audio_gcs_uri, scratch_dir
                    )

                    current_path = local_path
                    if preprocess_fn:
                        preprocessed_path = (
                            os.path.splitext(local_path)[0] + "_prep.wav"
                        )
                        if preprocess_fn(local_path, preprocessed_path):
                            current_path = preprocessed_path
                        else:
                            logger.warning(
                                f"Preprocessing failed for {local_path}, "
                                f"using original."
                            )

                    # Load audio waveform at 16kHz — uses preprocess_audio_for_model
                    # for resample + mono conversion.
                    resampled_path = (
                        os.path.splitext(local_path)[0] + "_resampled.wav"
                    )
                    if preprocess_audio_for_model(
                        current_path, resampled_path, target_sr=16000
                    ):
                        waveform, sr = torchaudio.load(resampled_path)
                    else:
                        # Fallback: load and resample inline if preprocessing failed
                        waveform, sr = torchaudio.load(current_path)
                        if sr != 16000:
                            waveform = T.Resample(orig_freq=sr, new_freq=16000)(
                                waveform
                            )

                    # Downmix stereo/multi-channel to mono. Load-bearing
                    # only on the fallback path (which resamples but does
                    # not downmix); a no-op on the preprocessed path,
                    # already mono from preprocess_audio_for_model.
                    if waveform.shape[0] > 1:
                        waveform = torch.mean(waveform, dim=0, keepdim=True)

                    audios.append(waveform.squeeze(0).numpy())
                    batch_entries.append(entry)
                except Exception as e:
                    logger.error(
                        f"Failed to download or preprocess {audio_gcs_uri}: {e}"
                    )
                    continue

            if not audios:
                continue

            # Parallel-GPU inference + sequential fallback: UNCHANGED from the
            # current code (both operate on the in-memory `audios` list). They
            # stay inside this `with` so scratch_dir lives until the batch ends.
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
            # scratch_dir removed here, every iteration, including on an exception.

    logger.info(f"Processed {len(results_list)} parallel results.")
    return results_list
