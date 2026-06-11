"""Grep-based lint tests that pin SLO emit-site invariants.

Two invariants protected here:

1. Exactly 3 ``# SLO: receipt_time stamp`` markers exist across
   ``backend/pipeline/ingestion/collectors/`` — one per collector module
   (Icecast, OpenMHZ, bcfy_calls). A PR that adds a 4th or removes one
   fails this test, forcing a conscious update to the SLO spec.

2. Exactly 1 call-download-failed SLO marker and exactly 1
   ``# SLO: chunk_ingested emit`` marker exist under
   ``backend/pipeline/ingestion/`` (excluding tests). The
   ``call_download_failed`` marker lives in the shared telemetry helper;
   zero emits are allowed in icecast or echo.
"""

from __future__ import annotations

import pathlib
import re
import unittest

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
_INGESTION_DIR = _REPO_ROOT / "backend" / "pipeline" / "ingestion"
_COLLECTORS_DIR = _INGESTION_DIR / "collectors"
_STAMP_MARKER_RE = re.compile(r"# SLO: receipt_time stamp")
_CALL_DL_FAILED_MARKER = "# SLO: " + "call_download_failed emit"
_CALL_DL_FAILED_EMIT_RE = re.compile(re.escape(_CALL_DL_FAILED_MARKER))
_CHUNK_INGESTED_EMIT_RE = re.compile(r"# SLO: chunk_ingested emit")
_EXPECTED_STAMP_COUNT = 3
_EXPECTED_CALL_DL_FAILED_EMIT_COUNT = 1
_EXPECTED_CHUNK_INGESTED_EMIT_COUNT = 1


class TestReceiptTimeStampMarkerCount(unittest.TestCase):
    """D-07: every collector stamp site is marked greppable."""

    def test_exactly_three_stamp_markers_in_collectors(self) -> None:
        """Exactly one stamp marker per collector (icecast, openmhz, bcfy_calls)."""
        count = 0
        found_files: list[str] = []
        for py_file in _COLLECTORS_DIR.rglob("*.py"):
            # Exclude test files — we only lint production code stamp sites.
            if "tests" in py_file.parts:
                continue
            text = py_file.read_text(encoding="utf-8")
            matches = _STAMP_MARKER_RE.findall(text)
            if matches:
                found_files.append(
                    f"{py_file.relative_to(_REPO_ROOT)}: {len(matches)}"
                )
                count += len(matches)
        self.assertEqual(
            count,
            _EXPECTED_STAMP_COUNT,
            msg=(
                f"Expected exactly {_EXPECTED_STAMP_COUNT} "
                f"'# SLO: receipt_time stamp' markers under "
                f"{_COLLECTORS_DIR.relative_to(_REPO_ROOT)}, "
                f"found {count}. Files with matches: {found_files}. "
                "If you added a collector, update _EXPECTED_STAMP_COUNT."
            ),
        )


class TestEmitMarkerCount(unittest.TestCase):
    """Phase 2: emit-site invariant (D-18 + chunk_ingested single site).

    Enforces exactly 1 call-download-failed SLO marker (in the
    shared telemetry helper) and exactly 1 `# SLO: chunk_ingested emit`
    marker (inline in collector_runtime._process_feed). A collector that
    needs call_download_failed telemetry must call the helper instead of
    adding a new emit marker.

    Also asserts ZERO call_download_failed markers in bcfy_feeds/icecast
    (SLO spec: no discrete download step) and echo (pre-existing Cloud Run
    collector path, not a Python caller site).
    """

    def _count_matches(
        self,
        pattern: re.Pattern[str],
        root: pathlib.Path,
    ) -> tuple[int, list[str]]:
        count = 0
        found: list[str] = []
        for py_file in root.rglob("*.py"):
            if "tests" in py_file.parts:
                continue
            text = py_file.read_text(encoding="utf-8")
            matches = pattern.findall(text)
            if matches:
                found.append(
                    f"{py_file.relative_to(_REPO_ROOT)}: {len(matches)}"
                )
                count += len(matches)
        return count, found

    def test_exactly_three_call_download_failed_emit_markers(self) -> None:
        count, found_files = self._count_matches(
            _CALL_DL_FAILED_EMIT_RE, _INGESTION_DIR
        )
        self.assertEqual(
            count,
            _EXPECTED_CALL_DL_FAILED_EMIT_COUNT,
            msg=(
                f"D-18: Expected exactly "
                f"{_EXPECTED_CALL_DL_FAILED_EMIT_COUNT} "
                f"call-download-failed SLO markers under "
                f"{_INGESTION_DIR.relative_to(_REPO_ROOT)} (excluding tests/), "
                f"found {count}. Files: {found_files}. "
                "If you added or removed a collector, update "
                "_EXPECTED_CALL_DL_FAILED_EMIT_COUNT and refresh D-18."
            ),
        )

    def test_exactly_one_chunk_ingested_emit_marker(self) -> None:
        count, found_files = self._count_matches(
            _CHUNK_INGESTED_EMIT_RE, _INGESTION_DIR
        )
        self.assertEqual(
            count,
            _EXPECTED_CHUNK_INGESTED_EMIT_COUNT,
            msg=(
                f"LOG-01: Expected exactly "
                f"{_EXPECTED_CHUNK_INGESTED_EMIT_COUNT} "
                f"'# SLO: chunk_ingested emit' marker under "
                f"{_INGESTION_DIR.relative_to(_REPO_ROOT)} (excluding tests/), "
                f"found {count}. Files: {found_files}. "
                "The emit lives in collector_runtime._process_feed strictly "
                "after the fenced bookmark and Pub/Sub publish both succeed."
            ),
        )

    def test_zero_call_download_failed_in_icecast(self) -> None:
        icecast_dir = _COLLECTORS_DIR / "icecast"
        if not icecast_dir.exists():
            self.skipTest(f"{icecast_dir} absent")
        count, _ = self._count_matches(_CALL_DL_FAILED_EMIT_RE, icecast_dir)
        self.assertEqual(
            count,
            0,
            msg=(
                "bcfy_feeds/icecast must NOT emit call_download_failed "
                "(no discrete download step per REQUIREMENTS.md LOG-02)."
            ),
        )

    def test_zero_call_download_failed_in_echo(self) -> None:
        echo_dir = _COLLECTORS_DIR / "echo"
        if not echo_dir.exists():
            self.skipTest(f"{echo_dir} absent")
        count, _ = self._count_matches(_CALL_DL_FAILED_EMIT_RE, echo_dir)
        self.assertEqual(
            count,
            0,
            msg=(
                "echo must NOT emit call_download_failed "
                "(pre-existing Cloud Run collector path, not a Python caller)."
            ),
        )


if __name__ == "__main__":
    unittest.main()
