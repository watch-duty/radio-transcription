"""Baseline evaluation on public ASR datasets (LibriSpeech etc). Requires [hf] extra."""

import logging
import os
import re

logger = logging.getLogger(__name__)


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
    """Run a baseline evaluation on a public dataset (e.g. LibriSpeech) using streaming and batching.

    Moved verbatim from ``inference_pipeline_runner.py`` lines 297-413. Signature is
    preserved exactly to avoid breaking notebook imports (Pitfall 7). Heavy imports
    (datasets, evaluate, soundfile) remain function-local as in the original — this
    keeps ``import common.baseline_eval`` light without the [hf] extra (Pitfall 8).

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
    from datasets import load_dataset
    from evaluate import load
    import tempfile
    import soundfile as sf
    import torch

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
                    logger.error("Error processing example in batch")

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
