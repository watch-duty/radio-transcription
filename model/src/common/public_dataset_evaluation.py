"""HuggingFace model x streaming public ASR dataset evaluation.

Streams a public ASR dataset (LibriSpeech etc.) via HuggingFace ``datasets``,
runs the loaded HF model in batches, and computes WER directly against the
dataset's labels — no GCS manifest. Distinct from ``inference_hf`` in its
data source (streaming HF dataset vs. segmented GCS audio), not its model.
Requires the [hf] extra.
"""

import logging
import re
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

try:
    import soundfile as sf
    import torch
    from datasets import load_dataset
    from evaluate import load
except ImportError as _e:
    _PUBLIC_DATASET_EVAL_MISSING = _e
else:
    _PUBLIC_DATASET_EVAL_MISSING = None


def _require_public_dataset_eval() -> None:
    if _PUBLIC_DATASET_EVAL_MISSING:
        msg = "public dataset evaluation requires the [hf] extra: pip install 'common[hf]'"
        raise ImportError(msg) from _PUBLIC_DATASET_EVAL_MISSING


def run_test_baseline_inference_evaluation(
    model: Any,
    prompt_fn: Callable[[dict | None, str], Any],
    inference_fn: Callable[[Any, list[Any]], list[Any]],
    decode_fn: Callable[[Any, Any], str],
    normalize_fn: Callable[[str], str] | None = None,
    dataset_name: str = "librispeech_asr",
    dataset_config: str = "clean",
    split: str = "test",
    num_examples: int = 20,
    batch_size: int = 4,
) -> tuple[float | None, list[str], list[str]]:
    """Run a baseline evaluation on a public dataset (e.g. LibriSpeech) using streaming and batching.

    Moved verbatim from ``inference_pipeline_runner.py`` lines 297-413. Signature is
    preserved exactly to avoid breaking notebook imports. Heavy imports
    (datasets, evaluate, soundfile) remain function-local as in the original — this
    keeps ``import common.public_dataset_evaluation`` light without the [hf] extra.

    Args:
        model: Loaded model instance.
        prompt_fn: Callable(entry, local_path) -> prompt structure.
        inference_fn: Callable(model, prompts) -> list of raw outputs.
        decode_fn: Callable(output, model) -> str (transcription).
        normalize_fn: Optional text normalization callable. Defaults to uppercase
            alpha-only normalization if None.
        dataset_name: Hugging Face dataset name. Defaults to 'librispeech_asr'.
        dataset_config: Dataset configuration. Defaults to 'clean'.
        split: Dataset split. Defaults to 'test'.
        num_examples: Number of examples to evaluate. Defaults to 20.
        batch_size: Inference batch size. Defaults to 4.

    Returns:
        Tuple of (wer_score, predictions, references) where wer_score is a float
        or None if no predictions were produced.
    """
    _require_public_dataset_eval()
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
    normalizer = normalize_fn or _default_normalize

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
            batch_predictions, batch_references = _process_public_dataset_batch(
                model=model,
                inference_fn=inference_fn,
                decode_fn=decode_fn,
                normalize_fn=normalizer,
                batch_prompts=batch_prompts,
                batch_refs=batch_refs,
                batch_temp_paths=batch_temp_paths,
            )
            predictions.extend(batch_predictions)
            references.extend(batch_references)
            batch_prompts = []
            batch_refs = []
            batch_temp_paths = []
            if i % (batch_size * 2) == 0:
                logger.info(f"Processed {i + 1} examples...")

    # Process any remaining items in the last batch
    batch_predictions, batch_references = _process_public_dataset_batch(
        model=model,
        inference_fn=inference_fn,
        decode_fn=decode_fn,
        normalize_fn=normalizer,
        batch_prompts=batch_prompts,
        batch_refs=batch_refs,
        batch_temp_paths=batch_temp_paths,
    )
    predictions.extend(batch_predictions)
    references.extend(batch_references)

    wer_score = None
    if predictions:
        wer_score = wer.compute(predictions=predictions, references=references)
        logger.info(f"WER on {len(predictions)} examples: {wer_score}")

    return wer_score, predictions, references


def _default_normalize(text: str) -> str:
    text = text.upper()
    text = re.sub(r"[^A-Z\s]", "", text)
    return re.sub(r"\s+", " ", text).strip()


def _process_public_dataset_batch(
    *,
    model: Any,
    inference_fn: Callable[[Any, list[Any]], list[Any]],
    decode_fn: Callable[[Any, Any], str],
    normalize_fn: Callable[[str], str],
    batch_prompts: list[Any],
    batch_refs: list[str],
    batch_temp_paths: list[str],
) -> tuple[list[str], list[str]]:
    predictions: list[str] = []
    references: list[str] = []
    if not batch_prompts:
        return predictions, references
    try:
        with torch.no_grad():
            outputs = inference_fn(model, batch_prompts)
        for j, out in enumerate(outputs):
            if out == "[ERROR]":
                logger.error("Error processing example in batch")
                continue
            pred = decode_fn(out, model)
            predictions.append(normalize_fn(pred))
            references.append(normalize_fn(batch_refs[j]))
    except Exception as e:
        logger.exception(f"Failed processing batch: {e}")
    finally:
        for temp_path in batch_temp_paths:
            Path(temp_path).unlink(missing_ok=True)
    return predictions, references
