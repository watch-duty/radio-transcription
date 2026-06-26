"""HTTP custom metric for Vertex AI Prompt Optimizer ASR scoring."""

from __future__ import annotations

import re
from functools import cache
from typing import Any

import functions_framework
import jiwer

METRIC_NAME = "asr_wer_score"
EMPTY_PENALTY = 0.5
WER_CAP = 1.5

try:
    from nemo_text_processing.inverse_text_normalization.inverse_normalize import (
        InverseNormalizer,
    )
except Exception:  # pragma: no cover - deployment fallback path
    InverseNormalizer = None


@functions_framework.http
def asr_wer_score(request: Any) -> tuple[dict[str, Any], int]:
    """Score one prompt-optimizer candidate response against a target transcript."""
    payload = request.get_json(silent=True) or {}
    response = _first_present(
        payload,
        "response",
        "model_response",
        "prediction",
        "output",
        "candidate",
    )
    reference = _first_present(
        payload,
        "reference",
        "target",
        "target_response",
        "ground_truth",
        "ground_truth_response",
    )
    if not reference:
        return {
            METRIC_NAME: 0.0,
            "explanation": "missing reference transcript",
        }, 200

    normalized_ref = _normalize(reference)
    normalized_hyp = _normalize(response)
    empty_response = not response.strip()
    try:
        wer = jiwer.wer(normalized_ref, normalized_hyp)
    except ValueError:
        wer = 1.0 if normalized_ref != normalized_hyp else 0.0
    score = 1.0 - min(WER_CAP, wer) / WER_CAP
    if empty_response:
        score -= EMPTY_PENALTY
    score = max(0.0, min(1.0, score))
    return {
        METRIC_NAME: round(score, 6),
        "explanation": (
            f"normalized_wer={wer:.4f}; empty_response={empty_response}"
        ),
    }, 200


def _first_present(payload: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, str):
            return value
    return ""


def _normalize(text: str) -> str:
    text = text.strip()
    text = re.sub(r"\[unintelligible\]", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\d{10,}", " [hallucination] ", text)
    text = re.sub(r"\b(uh|um|ah|er|uhh)\b", "", text, flags=re.IGNORECASE)
    if text:
        text = _inverse_normalize(text)
    text = _manual_small_numbers(text)
    text = re.sub(r"[:\-,\$]", " ", text)
    text = re.sub(r"(\d)", r" \1 ", text)
    text = text.lower()
    text = re.sub(r"[\n\r\t]+", " ", text)
    text = re.sub(r"[^\w\s]", "", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


@cache
def _nemo_normalizer() -> Any:
    if InverseNormalizer is None:
        return None
    return InverseNormalizer(lang="en")


def _inverse_normalize(text: str) -> str:
    normalizer = _nemo_normalizer()
    if normalizer is None:
        return text
    try:
        return normalizer.inverse_normalize(text, verbose=False)
    except Exception:
        return text


def _manual_small_numbers(text: str) -> str:
    mapping = {
        "one": "1",
        "two": "2",
        "three": "3",
        "four": "4",
        "five": "5",
        "six": "6",
        "seven": "7",
        "eight": "8",
        "nine": "9",
        "ten": "10",
    }
    for word, digit in mapping.items():
        text = re.sub(rf"\b{word}\b", digit, text, flags=re.IGNORECASE)
    return text
