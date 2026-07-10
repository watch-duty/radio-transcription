# Manifest Obsolete Cleanup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove transition-only manifest tests, fixtures, and wording while preserving every behavior used by current manifests, notebooks, and packaged flows.

**Architecture:** Treat compatibility defaults, identity-free merge matching, parser fidelity, and generic extension preservation as current public contracts. Make a test-only semantic cleanup in `test_manifest.py`; production code changes are out of scope unless the cleanup proves code unreachable.

**Tech Stack:** Python 3.13, `unittest`, pytest, Ruff, existing `common.manifest` APIs.

## Global Constraints

- Do not change production behavior.
- Keep `rows_from_manifest()` identity and offset defaults.
- Keep identity-free URI/offset prediction matching.
- Keep generic unknown-field and nested-extension coverage.
- Keep fail-loud loading, faithful parsed values, empty-hypothesis handling,
  single-consumption matching, and stale-prediction clearing.
- Do not stage or modify unrelated prompt-tuning worktree files.

---

### Task 1: Reframe Manifest Tests Around Current Contracts

**Files:**
- Modify: `model/tests/common/tests/test_manifest.py`
- Test: `model/tests/common/tests/test_manifest.py`

**Interfaces:**
- Consumes: Existing public functions imported from `common.manifest`.
- Produces: The same behavioral coverage without references to removed fields or
  historical implementation paths.

- [x] **Step 1: Establish the green baseline**

Run:

```bash
safe-run -- env PYTHONPATH=model/src python3 -m pytest \
  model/tests/common/tests/test_manifest.py -q
```

Expected: `59 passed, 59 subtests passed`.

- [x] **Step 2: Remove named obsolete-field assertions and unused fixture data**

Replace the removed-field enumeration with current generic extensibility
coverage:

```python
def test_prediction_and_unknown_fields_are_tolerated(self) -> None:
    for field in ("pred_text_whisper", "unknown_future_field"):
        with self.subTest(field=field):
            issues = validate_canonical_manifest(
                [_canonical_row(**{field: "not canonical"})]
            )
            self.assertEqual(issues, [])
```

Delete the unused argument from `test_invalid_rows_return_structured_issues`:

```python
audio_processing={"masked_categories": ["ok", "", 42]},
```

Expected: generic unknown-field behavior remains covered; no named removed
canonical fields remain in this test file.

- [x] **Step 3: Delete the transition-only silent-failure regression test**

Delete `test_does_not_return_empty_list_on_error` in full. Keep
`test_raises_on_malformed_prediction_offset`, which already proves the current
`ValueError` contract using the real merge API.

Expected: malformed-offset coverage remains once, without assertions about the
removed `return []` implementation.

- [x] **Step 4: Rewrite historical test framing as current behavior**

Apply these exact naming changes without altering method bodies:

```diff
-"""Tests for common.manifest.
-
-Covers:
-  - merge_predictions_to_manifest: fail-loud (re-raise) on unexpected error
-  - merge_predictions_to_manifest: happy-path offset-tolerant merge
-  - load_manifest: fail-loud JSON/JSONL parsing
-"""
+"""Tests for canonical manifest validation, conversion, loading, and merging."""

-class TestMergePredictionsToManifestFailLoud(unittest.TestCase):
-    """merge_predictions_to_manifest must raise on unexpected error, never return []."""
+class TestMergePredictionValidation(unittest.TestCase):
+    """Malformed prediction and ground-truth rows are rejected."""

     def test_raises_on_malformed_prediction_offset(self) -> None:
-        """A prediction whose offset cannot be cast to float raises ValueError.
-
-        The internal ``float(pred.get("offset", 0.0))`` call throws when the
-        offset is a non-numeric string.  The function must propagate the
-        exception rather than swallowing it and returning [].
-        """
+        """A prediction with a non-numeric offset raises ValueError."""

     def test_prediction_missing_audio_filepath_raises(self) -> None:
-        """A prediction without `audio_filepath` fails loud, not silently merges to ""."""
+        """A prediction requires a non-blank audio_filepath."""

     def test_prediction_missing_offset_raises(self) -> None:
-        """A prediction without `offset` fails loud, not silently defaults to 0.0."""
+        """A prediction requires an offset."""

     def test_raises_on_missing_ground_truth_offset(self) -> None:
-        """A GT row missing 'offset' raises — symmetric to the predictions side.
-
-        Silently defaulting to 0.0 would let a malformed manifest bind every
-        row missing an offset to whichever prediction sits at 0.0.
-        """
+        """A ground-truth row requires an offset."""

 class TestMergePredictionsHappyPath(unittest.TestCase):
-    """Sanity-check the normal merge path is unaffected by the re-raise change."""
+    """Valid predictions match by URI, identity, and offset."""

-    def test_prediction_without_identity_matches_exact_uri_and_offset(
+    def test_prediction_without_identity_matches_by_uri_and_offset(
         self,
     ) -> None:
-        """Older predictions without identity still match by exact URI."""
+        """Identity is optional when URI and offset identify the row."""

-    def test_unmatched_prediction_leaves_field_absent(self) -> None:
+    def test_offset_mismatch_raises_unmatched_prediction(self) -> None:

 class TestLoadManifestFailLoud(unittest.TestCase):
-    """load_manifest parses JSON/JSONL without compatibility coercion."""
+    """load_manifest parses faithfully and rejects malformed input."""

-    def test_non_string_text_is_not_coerced(self) -> None:
+    def test_non_string_text_value_is_preserved(self) -> None:

-class TestRowsFromManifestStrict(unittest.TestCase):
+class TestRowsFromManifestConversion(unittest.TestCase):
     """rows_from_manifest converts valid rows and fills conversion defaults."""

 class TestRowsFromManifestRequiredFields(unittest.TestCase):
-    """rows_from_manifest fails loudly for strict canonical fields."""
+    """rows_from_manifest rejects invalid required string fields."""

-    def test_non_string_text_raises_type_error_with_row_context(self) -> None:
+    def test_non_string_text_raises_value_error_with_row_context(self) -> None:
```

Delete the stale implementation comment immediately above `bad_predictions`
that describes a `float()` call inside a try block. In the identity-free merge
test, replace `legacy prediction`, the `legacy`
model key, and `pred_text_legacy` with neutral current values such as
`prediction`, `m`, and `pred_text_m`. Remove comments/docstrings that refer to
`re-raise`, `older`, `old silent-failure`, or silent compatibility behavior.

Expected: test names and prose describe only current contracts. Assertions and
production inputs remain behaviorally equivalent.

- [x] **Step 5: Verify obsolete references are absent**

Run:

```bash
rg -n -i \
  "audio_processing|sequence|original_audio_uri|source_group|dataset_name|\\blang\\b|legacy|older|re-raise|old silent|compatibility coercion" \
  model/tests/common/tests/test_manifest.py
```

Expected: no matches. References to active concepts such as stale predictions
or compatibility defaults are not part of this scan.

- [x] **Step 6: Run focused behavior verification**

Run:

```bash
safe-run -- env PYTHONPATH=model/src python3 -m pytest \
  model/tests/common/tests/test_manifest.py -q
```

Expected: all remaining tests and subtests pass; the test count decreases only
by the deleted transition-only test and removed subtest cases.

- [x] **Step 7: Run formatting and static checks**

Run:

```bash
safe-run -- uv run ruff format \
  model/tests/common/tests/test_manifest.py
safe-run -- uv run ruff check \
  model/src/common/manifest.py model/tests/common/tests/test_manifest.py
git diff --check -- \
  model/tests/common/tests/test_manifest.py \
  docs/superpowers/specs/2026-07-10-manifest-obsolete-cleanup-design.md \
  docs/superpowers/plans/2026-07-10-manifest-obsolete-cleanup.md
```

Expected: Ruff reports success and `git diff --check` emits no output.

- [x] **Step 8: Review the final scope**

Run:

```bash
git diff --stat HEAD -- \
  model/tests/common/tests/test_manifest.py \
  docs/superpowers/plans/2026-07-10-manifest-obsolete-cleanup.md
git status --short
```

Expected: implementation changes are limited to the manifest test file and this
plan; unrelated prompt-tuning files remain unstaged.

- [x] **Step 9: Commit the cleanup**

```bash
git add \
  model/tests/common/tests/test_manifest.py \
  docs/superpowers/plans/2026-07-10-manifest-obsolete-cleanup.md
git commit -m "test(model): remove obsolete manifest assertions"
```

Expected: one semantic test-cleanup commit with hooks enabled.
