"""Grep-based lint tests that pin Phase 1 invariants.

Two invariants protected here:

1. Exactly 3 `# SLO: receipt_time stamp` markers exist across
   `backend/pipeline/ingestion/collectors/` — one per collector module. A PR
   that adds a 4th or removes one will fail this test, forcing a conscious
   update to the SLO spec documentation.

2. The hand-extracted Terraform snapshot at `terraform-snapshots/slo_alerts.json`
   string-equals the constants exported from `slo_contract.py`. A rename of
   either side without the other will fail CI before a single alert is
   silently broken.

If the snapshot file is absent, the snapshot-match tests are SKIPPED (not
failed) with a warning — documented in ROADMAP.md Phase 4 VERIFY-01 as
"advisory-only pending snapshot". Phase 1 ships the snapshot so this is a
defensive fallback only.
"""

from __future__ import annotations

import json
import logging
import pathlib
import re
import unittest

from backend.pipeline.ingestion import slo_contract

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[4]
_INGESTION_DIR = _REPO_ROOT / "backend" / "pipeline" / "ingestion"
_COLLECTORS_DIR = _INGESTION_DIR / "collectors"
_SNAPSHOT_PATH = _REPO_ROOT / "terraform-snapshots" / "slo_alerts.json"
_STAMP_MARKER_RE = re.compile(r"# SLO: receipt_time stamp")
_CALL_DL_FAILED_EMIT_RE = re.compile(r"# SLO: call_download_failed emit")
_CHUNK_INGESTED_EMIT_RE = re.compile(r"# SLO: chunk_ingested emit")
_EXPECTED_STAMP_COUNT = 3
_EXPECTED_CALL_DL_FAILED_EMIT_COUNT = 2
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
                "If you added a collector, update _EXPECTED_STAMP_COUNT "
                "and refresh the Terraform snapshot."
            ),
        )


class TestEmitMarkerCount(unittest.TestCase):
    """Phase 2: emit-site invariant (D-18 + chunk_ingested single site).

    Enforces exactly 2 `# SLO: call_download_failed emit` markers (one in
    OpenMHZ, one in bcfy_calls) and exactly 1 `# SLO: chunk_ingested emit`
    marker (inline in normalizer_runtime._process_feed). A new collector that
    adds a 3rd call_download_failed emit must consciously update
    _EXPECTED_CALL_DL_FAILED_EMIT_COUNT or face this test.

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

    def test_exactly_two_call_download_failed_emit_markers(self) -> None:
        count, found_files = self._count_matches(
            _CALL_DL_FAILED_EMIT_RE, _INGESTION_DIR
        )
        self.assertEqual(
            count,
            _EXPECTED_CALL_DL_FAILED_EMIT_COUNT,
            msg=(
                f"D-18: Expected exactly "
                f"{_EXPECTED_CALL_DL_FAILED_EMIT_COUNT} "
                f"'# SLO: call_download_failed emit' markers under "
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
                "The emit lives in normalizer_runtime._process_feed strictly "
                "after retry_with_lease_check(update_feed_progress) returns "
                "ok=True (02-CONTEXT.md D-11 + LOG-01 SC#1)."
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


class TestTerraformSnapshotMatchesSloContract(unittest.TestCase):
    """D-09/D-10: the ops-team snapshot's strings equal slo_contract's values."""

    def setUp(self) -> None:
        if not _SNAPSHOT_PATH.exists():
            logging.getLogger(__name__).warning(
                "Snapshot file %s absent — skipping snapshot-match checks",
                _SNAPSHOT_PATH,
            )
            self.skipTest(f"snapshot file missing: {_SNAPSHOT_PATH}")
        with _SNAPSHOT_PATH.open(encoding="utf-8") as fh:
            self.snapshot = json.load(fh)

    def test_event_types_match(self) -> None:
        expected = {
            slo_contract.EVENT_TYPE_CHUNK_INGESTED,
            slo_contract.EVENT_TYPE_CALL_DOWNLOAD_FAILED,
            slo_contract.EVENT_TYPE_FEED_QUARANTINED,
        }
        self.assertEqual(set(self.snapshot["event_types"]), expected)

    def test_active_feed_count_metric_in_snapshot(self) -> None:
        self.assertIn(
            slo_contract.METRIC_TYPE_ACTIVE_FEED_COUNT,
            self.snapshot["metric_types"],
        )

    def test_quarantine_events_metric_in_snapshot(self) -> None:
        self.assertIn(
            slo_contract.METRIC_TYPE_QUARANTINE_EVENTS,
            self.snapshot["metric_types"],
        )

    def test_logger_path_matches(self) -> None:
        self.assertEqual(
            self.snapshot["logger_path"],
            slo_contract.INGESTION_LOGGER_PATH,
        )

    def test_metric_labels_match_allowlist(self) -> None:
        self.assertEqual(
            set(self.snapshot["metric_labels"]),
            slo_contract.METRIC_LABEL_ALLOWLIST,
        )


if __name__ == "__main__":
    unittest.main()
