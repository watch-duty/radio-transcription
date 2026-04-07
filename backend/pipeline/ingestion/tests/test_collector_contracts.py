"""Verify every registered collector's type contract.

Iterates ``_COLLECTORS`` automatically — adding a new collector to the
registry includes it in these tests with zero extra boilerplate.
"""

from __future__ import annotations

import inspect
import unittest

from backend.pipeline.ingestion.router import _COLLECTORS


class TestCollectorContracts(unittest.TestCase):
    def test_all_registered_collectors_are_callable(self) -> None:
        for source_type, (fn, url_base) in _COLLECTORS.items():
            with self.subTest(source_type=source_type):
                self.assertTrue(callable(fn))

    def test_all_registered_collectors_have_correct_return_annotation(
        self,
    ) -> None:
        """Each collector's return annotation must mention CapturedChunk."""
        for source_type, (fn, _url_base) in _COLLECTORS.items():
            with self.subTest(source_type=source_type):
                sig = inspect.signature(fn)
                ret = str(sig.return_annotation)
                self.assertIn(
                    "CapturedChunk",
                    ret,
                    f"{getattr(fn, '__name__', fn)} return annotation {ret!r} "
                    f"does not mention CapturedChunk",
                )
