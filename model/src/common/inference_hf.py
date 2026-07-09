"""Hugging Face transformers x GCS-manifest ASR inference.

Iterates a JSONL manifest of GCS-hosted audio segments, downloads each into a
scoped temp dir, preprocesses (resample + mono), and runs the loaded HF model
in parallel-GPU batches with sequential single-item fallback. Distinct from
``public_dataset_evaluation`` (same HF model framework but a streaming HF
dataset as the data source). Requires the [hf] extra.
"""

import logging
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

from google.cloud import storage

from common.audio_utils import preprocess_audio_for_model
from common.gcs_utils import download_to_scratch

logger = logging.getLogger(__name__)

try:
    import torch
    import torchaudio
    from torchaudio import transforms
except ImportError as _e:
    _HF_MISSING = _e
else:
    _HF_MISSING = None


def _require_hf() -> None:
    """Raise a clear error if the [hf] extra is not installed."""
    if _HF_MISSING:
        msg = (
            "inference_hf requires the [hf] extra: "
            "pip install 'radio-transcription-model[hf]'"
        )
        raise ImportError(msg) from _HF_MISSING


def run_huggingface_inference_pipeline(
    model: Any,
    processor: Any,
    manifest_data: list[dict[str, Any]],
    storage_client: storage.Client,
    project_name: str,
    selected_model: str,
    batch_size: int = 4,
    limit: int | None = None,
    preprocess_fn: Callable[[str, str], bool] | None = None,
    max_new_tokens: int = 256,
    text_prompt: str = "transcribe",
    processor_kwargs: dict | None = None,
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
            batch_entries, audios = _prepare_hf_batch(
                batch=batch,
                storage_client=storage_client,
                scratch_dir=scratch_dir,
                preprocess_fn=preprocess_fn,
            )

            if not audios:
                continue

            results_list.extend(
                _infer_hf_batch(
                    model=model,
                    processor=processor,
                    audios=audios,
                    batch_entries=batch_entries,
                    selected_model=selected_model,
                    batch_number=i // batch_size + 1,
                    max_new_tokens=max_new_tokens,
                    text_prompt=text_prompt,
                    processor_kwargs=processor_kwargs,
                )
            )
            # scratch_dir removed here, every iteration, including on an exception.

    logger.info(f"Processed {len(results_list)} parallel results.")
    return results_list


def _prepare_hf_batch(
    *,
    batch: list[dict[str, Any]],
    storage_client: storage.Client,
    scratch_dir: str,
    preprocess_fn: Callable[[str, str], bool] | None,
) -> tuple[list[dict[str, Any]], list[Any]]:
    batch_entries = []
    audios = []
    for entry in batch:
        audio_gcs_uri = entry["audio_filepath"]
        try:
            waveform = _load_hf_waveform(
                storage_client=storage_client,
                audio_gcs_uri=audio_gcs_uri,
                scratch_dir=scratch_dir,
                preprocess_fn=preprocess_fn,
            )
        except Exception as e:
            logger.exception(
                f"Failed to download or preprocess {audio_gcs_uri}: {e}"
            )
            continue
        audios.append(waveform.squeeze(0).numpy())
        batch_entries.append(entry)
    return batch_entries, audios


def _load_hf_waveform(
    *,
    storage_client: storage.Client,
    audio_gcs_uri: str,
    scratch_dir: str,
    preprocess_fn: Callable[[str, str], bool] | None,
) -> Any:
    local_path = download_to_scratch(storage_client, audio_gcs_uri, scratch_dir)
    current_path = local_path
    if preprocess_fn:
        preprocessed_path = _derived_audio_path(local_path, "_prep.wav")
        if preprocess_fn(local_path, preprocessed_path):
            current_path = preprocessed_path
        else:
            logger.warning(
                f"Preprocessing failed for {local_path}, using original."
            )

    resampled_path = _derived_audio_path(local_path, "_resampled.wav")
    if preprocess_audio_for_model(
        current_path, resampled_path, target_sr=16000
    ):
        waveform, _ = torchaudio.load(resampled_path)
    else:
        waveform, sr = torchaudio.load(current_path)
        if sr != 16000:
            waveform = transforms.Resample(orig_freq=sr, new_freq=16000)(
                waveform
            )

    if waveform.shape[0] > 1:
        waveform = torch.mean(waveform, dim=0, keepdim=True)
    return waveform


def _infer_hf_batch(
    *,
    model: Any,
    processor: Any,
    audios: list[Any],
    batch_entries: list[dict[str, Any]],
    selected_model: str,
    batch_number: int,
    max_new_tokens: int,
    text_prompt: str,
    processor_kwargs: dict | None,
) -> list[dict[str, Any]]:
    try:
        return _infer_hf_batch_parallel(
            model=model,
            processor=processor,
            audios=audios,
            batch_entries=batch_entries,
            selected_model=selected_model,
            batch_number=batch_number,
            max_new_tokens=max_new_tokens,
            text_prompt=text_prompt,
            processor_kwargs=processor_kwargs,
        )
    except Exception as e:
        logger.warning(
            f"Batch parallel GPU execution failed: {e}. "
            f"Gracefully falling back to sequential GPU inference "
            f"(batch size: {len(batch_entries)}) to prevent batch crash."
        )
        return _infer_hf_batch_sequential(
            model=model,
            processor=processor,
            audios=audios,
            batch_entries=batch_entries,
            selected_model=selected_model,
            max_new_tokens=max_new_tokens,
            text_prompt=text_prompt,
            processor_kwargs=processor_kwargs,
        )


def _infer_hf_batch_parallel(
    *,
    model: Any,
    processor: Any,
    audios: list[Any],
    batch_entries: list[dict[str, Any]],
    selected_model: str,
    batch_number: int,
    max_new_tokens: int,
    text_prompt: str,
    processor_kwargs: dict | None,
) -> list[dict[str, Any]]:
    inputs = processor(
        audio=audios,
        sampling_rate=16000,
        text=[text_prompt] * len(audios),
        padding=True,
        return_tensors="pt",
        **(processor_kwargs or {}),
    )
    inputs.to(model.device, dtype=model.dtype)

    logger.info(f"Processing parallel GPU batch {batch_number}...")
    with torch.no_grad():
        outputs = model.generate(**inputs, max_new_tokens=max_new_tokens)

    if "input_ids" in inputs:
        new_tokens = outputs[:, inputs["input_ids"].shape[-1] :]
    else:
        new_tokens = outputs
    transcripts = processor.batch_decode(new_tokens, skip_special_tokens=True)
    return _prediction_rows(batch_entries, transcripts, selected_model)


def _infer_hf_batch_sequential(
    *,
    model: Any,
    processor: Any,
    audios: list[Any],
    batch_entries: list[dict[str, Any]],
    selected_model: str,
    max_new_tokens: int,
    text_prompt: str,
    processor_kwargs: dict | None,
) -> list[dict[str, Any]]:
    results = []
    for j, row in enumerate(batch_entries):
        try:
            pred = _infer_hf_single(
                model=model,
                processor=processor,
                audio=audios[j],
                max_new_tokens=max_new_tokens,
                text_prompt=text_prompt,
                processor_kwargs=processor_kwargs,
            )
        except Exception as ex:
            logger.exception(f"Sequential fallback failed for row {j}: {ex}")
            continue
        result_row = dict(row)
        result_row[f"pred_text_{selected_model}"] = pred.strip()
        results.append(result_row)
    return results


def _infer_hf_single(
    *,
    model: Any,
    processor: Any,
    audio: Any,
    max_new_tokens: int,
    text_prompt: str,
    processor_kwargs: dict | None,
) -> str:
    inputs = processor(
        audio=audio,
        sampling_rate=16000,
        text=text_prompt,
        return_tensors="pt",
        **(processor_kwargs or {}),
    )
    inputs.to(model.device, dtype=model.dtype)
    with torch.no_grad():
        out = model.generate(**inputs, max_new_tokens=max_new_tokens)
    if "input_ids" in inputs:
        new_tokens = out[0, inputs["input_ids"].shape[-1] :]
    else:
        new_tokens = out[0]
    return processor.decode(new_tokens, skip_special_tokens=True)


def _prediction_rows(
    batch_entries: list[dict[str, Any]],
    transcripts: list[str],
    selected_model: str,
) -> list[dict[str, Any]]:
    results = []
    for row, transcript in zip(batch_entries, transcripts, strict=True):
        result_row = dict(row)
        result_row[f"pred_text_{selected_model}"] = transcript.strip()
        results.append(result_row)
    return results


def _derived_audio_path(local_path: str, suffix: str) -> str:
    path = Path(local_path)
    return str(path.with_name(f"{path.stem}{suffix}"))
