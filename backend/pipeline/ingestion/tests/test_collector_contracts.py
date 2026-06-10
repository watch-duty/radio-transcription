"""Verify every registered collector's type contract.

Iterates ``_COLLECTORS`` automatically — adding a new collector to the
registry includes it in these tests with zero extra boilerplate.
"""

from __future__ import annotations

import dataclasses
import datetime
import inspect
import unittest

from backend.pipeline.ingestion.models import CapturedChunk, SourceObservation
from backend.pipeline.ingestion.router import _COLLECTORS


class TestCollectorContracts(unittest.TestCase):
    def test_all_registered_collectors_are_callable(self) -> None:
        for source_type, (fn, url_base) in _COLLECTORS.items():
            with self.subTest(source_type=source_type.value):
                self.assertTrue(callable(fn))

    def test_all_registered_collectors_have_correct_return_annotation(
        self,
    ) -> None:
        """Each collector returns capture events or a chunk-only specialization."""
        for source_type, (fn, _url_base) in _COLLECTORS.items():
            with self.subTest(source_type=source_type.value):
                sig = inspect.signature(fn)
                ret = str(sig.return_annotation)
                self.assertTrue(
                    "CaptureEvent" in ret or "CapturedChunk" in ret,
                    f"{getattr(fn, '__name__', fn)} return annotation {ret!r} "
                    f"does not mention CaptureEvent or CapturedChunk",
                )


class TestCapturedChunkReceiptTime(unittest.TestCase):
    """RCPT-01: CapturedChunk has optional UTC receipt_time field."""

    def test_default_is_none(self) -> None:
        now = datetime.datetime.now(datetime.UTC)
        chunk = CapturedChunk(
            audio_bytes=b"x",
            chunk_start_time=now,
            chunk_end_time=now,
        )
        self.assertIsNone(chunk.receipt_time)

    def test_explicit_receipt_time_preserved(self) -> None:
        now = datetime.datetime.now(datetime.UTC)
        chunk = CapturedChunk(
            audio_bytes=b"x",
            chunk_start_time=now,
            chunk_end_time=now,
            receipt_time=now,
        )
        self.assertEqual(chunk.receipt_time, now)

    def test_is_frozen(self) -> None:
        now = datetime.datetime.now(datetime.UTC)
        chunk = CapturedChunk(
            audio_bytes=b"x",
            chunk_start_time=now,
            chunk_end_time=now,
        )
        with self.assertRaises(dataclasses.FrozenInstanceError):
            chunk.receipt_time = now  # type: ignore[misc]  # ty: ignore[invalid-assignment]


class TestSourceObservation(unittest.TestCase):
    """SourceObservation is a non-audio capture event."""

    def test_resume_position_defaults_to_none(self) -> None:
        observation = SourceObservation()

        self.assertIsNone(observation.resume_position)

    def test_is_frozen(self) -> None:
        now = datetime.datetime.now(datetime.UTC)
        observation = SourceObservation(resume_position=now)

        with self.assertRaises(dataclasses.FrozenInstanceError):
            observation.resume_position = None  # type: ignore[misc]  # ty: ignore[invalid-assignment]
