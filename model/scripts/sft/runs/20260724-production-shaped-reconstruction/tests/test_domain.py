"""Tests for exact-frame domain primitives."""

from __future__ import annotations

import pathlib
import sys

import pytest


RUN_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(RUN_ROOT) not in sys.path:
    sys.path.insert(0, str(RUN_ROOT))

from dataset_run import domain  # noqa: E402


def test_nonempty_text_wins_over_pii_category() -> None:
    assert (
        domain.classify_annotation("PII/ADDRESS", " Engine 7 ")
        is domain.AtomClass.SPEECH
    )


def test_targetless_pii_is_excluded() -> None:
    assert (
        domain.classify_annotation("PII/ID", "  ")
        is domain.AtomClass.PII
    )


def test_other_blank_annotation_is_targetless_context() -> None:
    assert (
        domain.classify_annotation("UNINTELLIGIBLE", None)
        is domain.AtomClass.TARGETLESS
    )


def test_decimal_to_frame_uses_half_up_rounding() -> None:
    assert domain.decimal_to_frame("0.00003125", 16_000) == 1
    assert domain.decimal_to_frame("0.00003124", 16_000) == 0


def test_speech_atom_rejects_blank_target() -> None:
    with pytest.raises(ValueError, match="non-empty"):
        domain.Atom(
            atom_id="a",
            family="bcfy_feeds",
            source_uri="gs://bucket/source.wav",
            raw_manifest="feeds_train",
            raw_index=1,
            start_frame=0,
            end_frame=1,
            atom_class=domain.AtomClass.SPEECH,
            text=" ",
        )
