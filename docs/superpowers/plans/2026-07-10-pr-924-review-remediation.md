# PR 924 Review Remediation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make PR #924 safe to resend by binding reusable outputs to actual
Gemini request payloads, enforcing fail-loud packaged manifest boundaries,
rejecting unstripped canonical audio URIs before inference, and clearing hard
Python standards and formatter failures.

**Architecture:** Request identities gain ordered SHA-256 digests derived from
the canonical request dictionaries returned by `vertex.build_request`; batch
reuse compares the complete identity and online reuse compares both URI and
digest prefixes. Exploratory manifest APIs remain lenient while new strict
local/GCS loaders are used exclusively by packaged prepare/eval boundaries.
Standards cleanup is mechanical and behavior-preserving after the correctness
changes are green.

**Tech Stack:** Python 3.11+ model package, Python 3.13 root CI, `unittest`,
`pytest`, `google-genai>=2.10,<3`, Ruff 0.15.12, `ty`, and Vulture.

## Global Constraints

- Keep existing inference result formats and GCS artifact locations unchanged.
- Reject old metadata sidecars without ordered request digests; reuse remains
  fail-closed.
- Preserve `parse_manifest_text`, `load_manifest`, and
  `download_jsonl_manifest` as lenient exploratory APIs.
- Packaged prepare/eval boundaries must fail before tuning or paid inference
  when JSONL syntax or canonical audio URIs are invalid.
- Follow `.github/instructions/PYTHON_STYLE.instructions.md`: modules-only
  imports, 80-character lines, public class attribute documentation, complete
  public/non-trivial function documentation, and module docstrings.
- Do not change polling defaults, dependency logger levels, artifact layouts,
  online worker-pool behavior, retry behavior, or snapshot behavior.
- Preserve all pre-existing untracked manifests, `model/research/`, and
  `results/`.

---

### Task 1: Bind reusable output identity to actual request payloads

**Files:**
- Modify: `model/src/common/gemini/request_identity.py:14-155`
- Modify: `model/src/common/gemini/batch.py:47-95`
- Modify: `model/src/gemini_sft/target_execution.py:198-255`
- Modify: `model/tests/sft_eval_fixtures.py:77-105`
- Test: `model/tests/gemini_sft/test_target_execution.py`
- Test: `model/tests/common/tests/test_gemini_batch.py`

**Interfaces:**
- Consumes: `common.gemini.vertex.build_request(...) -> dict[str, Any]` and
  aligned `audio_uris` / `histories` sequences.
- Produces:
  `build_gemini_eval_request_identity(..., histories=...) -> dict[str, Any]`
  with `schema_version == 2` and ordered `request_digests`; prefix validation
  checks both `audio_uris` and `request_digests`.

- [ ] **Step 1: Add failing request-content identity tests**

Add a `histories` parameter to the `_identity` test helper and add these tests
to `TestOnlineRequestIdentity`:

```python
def test_hash_changes_when_prior_transcript_changes(self) -> None:
    first = [[context.ContextTurn("gs://audio/prior.flac", "alpha")], []]
    second = [[context.ContextTurn("gs://audio/prior.flac", "bravo")], []]

    self.assertNotEqual(
        request_identity.request_identity_hash(_identity(histories=first)),
        request_identity.request_identity_hash(_identity(histories=second)),
    )

def test_prefix_identity_rejects_changed_existing_request(self) -> None:
    stored = _identity(
        audio_uris=["gs://audio/1.flac"],
        histories=[[]],
    )
    requested = _identity(
        audio_uris=["gs://audio/1.flac", "gs://audio/2.flac"],
        histories=[
            [context.ContextTurn("gs://audio/new-prior.flac", "changed")],
            [],
        ],
    )

    with self.assertRaisesRegex(ValueError, "request identity mismatch"):
        request_identity.validate_prefix_identity(
            stored,
            requested,
            "request identity mismatch",
        )
```

Update the existing true-prefix smoke test so stored/requested identities pass
aligned empty histories explicitly. In `test_gemini_batch.py`, add an existing
output/metadata test whose metadata uses `"old transcript"` and whose current
call uses `"new transcript"`; assert `run_batch_audio_inference` raises
`ValueError("batch prediction request identity mismatch")` before `submit_fn`.

- [ ] **Step 2: Run the new identity tests and verify RED**

Run:

```bash
cd model
safe-run -- uv run --extra dev --extra vertex pytest \
  tests/gemini_sft/test_target_execution.py::TestOnlineRequestIdentity \
  tests/common/tests/test_gemini_batch.py -q
```

Expected: the history-change assertions fail because current identities do not
contain request payload digests; any temporary signature errors must be fixed
in test setup until failures specifically demonstrate unchanged identity.

- [ ] **Step 3: Add ordered request digests to identity schema version 2**

Implement the following behavior in `request_identity.py`, using qualified
module imports:

```python
def _request_digest(request: collections.abc.Mapping[str, typing.Any]) -> str:
    payload = json.dumps(
        request,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _request_digests(
    *,
    audio_uris: collections.abc.Sequence[str],
    histories: collections.abc.Sequence[
        collections.abc.Sequence[context.ContextTurn]
    ],
    system_prompt: str,
    user_prompt: str,
    prior_context_mode: str,
) -> list[str]:
    if len(histories) != len(audio_uris):
        raise ValueError("histories length must match audio_uris length")
    return [
        _request_digest(
            vertex.build_request(
                audio_uri,
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                history=histories[index],
                history_mode=prior_context_mode,
            )
        )
        for index, audio_uri in enumerate(audio_uris)
    ]
```

Change `build_request_identity` to require `request_digests`, validate its
length against `audio_uris`, return `schema_version: 2`, and store both ordered
lists. Change `build_gemini_eval_request_identity` to require aligned
`histories`, call `_request_digests`, and pass them to the generic builder.

Change prefix validation to remove both sequence fields before comparing static
identity and then require both stored sequences to be exact prefixes:

```python
if len(existing_audio) != len(existing_digests):
    raise ValueError(error_message)
if request_audio[: len(existing_audio)] != existing_audio:
    raise ValueError(error_message)
if request_digests[: len(existing_digests)] != existing_digests:
    raise ValueError(error_message)
```

Update batch, online, and fixture call sites to pass aligned histories. Batch
must normalize `None` to one empty history per URI before identity creation;
online passes its already validated `history_list`.

- [ ] **Step 4: Run focused identity tests and verify GREEN**

Run the Step 2 command. Expected: all selected tests pass, including existing
exact reuse and true-prefix resume tests.

- [ ] **Step 5: Run the request-building regression set**

Run:

```bash
cd model
safe-run -- uv run --extra dev --extra vertex pytest \
  tests/common/tests/test_gemini_batch.py \
  tests/common/tests/test_gemini_vertex.py \
  tests/gemini_sft/test_target_execution.py \
  tests/gemini_sft/test_workflow.py -q
```

Expected: all tests and subtests pass.

- [ ] **Step 6: Commit the identity fix**

```bash
git add model/src/common/gemini/request_identity.py \
  model/src/common/gemini/batch.py \
  model/src/gemini_sft/target_execution.py \
  model/tests/sft_eval_fixtures.py \
  model/tests/common/tests/test_gemini_batch.py \
  model/tests/gemini_sft/test_target_execution.py
git commit -m "fix(gemini-sft): bind reuse to request payloads"
```

### Task 2: Add fail-loud packaged manifest parsing

**Files:**
- Modify: `model/src/common/manifest.py:532-614`
- Modify: `model/src/common/gcs_utils.py:184-196`
- Modify: `model/src/gemini_sft/artifacts.py:128-150`
- Modify: `model/src/gemini_sft/evaluate.py:140`
- Test: `model/tests/common/tests/test_manifest.py`
- Test: `model/tests/common/tests/test_gcs_utils.py`
- Test: `model/tests/gemini_sft/test_workflow.py`

**Interfaces:**
- Consumes: raw JSON array/JSONL text, local paths, or GCS object text.
- Produces: `parse_manifest_text_strict`, `load_manifest_strict`, and
  `download_jsonl_manifest_strict`; existing lenient APIs remain unchanged.

- [ ] **Step 1: Write strict-parser failure tests**

Add tests that preserve leniency and prove strict behavior:

```python
def test_strict_jsonl_parser_rejects_first_malformed_line(self) -> None:
    content = '{"audio_filepath":"gs://b/a.flac","text":"ok"}\n{bad json}'

    with self.assertRaisesRegex(
        ValueError,
        r"inline.jsonl: malformed JSON at line 2",
    ):
        manifest.parse_manifest_text_strict(content, source="inline.jsonl")

def test_strict_jsonl_parser_rejects_non_object_line(self) -> None:
    with self.assertRaisesRegex(
        ValueError,
        r"inline.jsonl: expected JSON object at line 1",
    ):
        manifest.parse_manifest_text_strict(
            '"not an object"\n',
            source="inline.jsonl",
        )
```

Use an unambiguous JSONL payload for the non-object test so it does not enter
the JSON-array branch. Add a temporary-file test proving
`load_manifest_strict` raises on a mixed valid/malformed file. Add a fake-GCS
test proving `download_jsonl_manifest_strict` raises on the same content while
the existing lenient downloader test remains green. Add a workflow test that
places mixed content in the durable eval manifest and asserts evaluation fails
before either batch submission or online client creation.

- [ ] **Step 2: Run strict-parser tests and verify RED**

Run:

```bash
cd model
safe-run -- uv run --extra dev --extra vertex pytest \
  tests/common/tests/test_manifest.py::TestParseManifestText \
  tests/common/tests/test_gcs_utils.py \
  tests/gemini_sft/test_workflow.py -q
```

Expected: new tests fail because strict parser/downloader entry points do not
exist or packaged eval still uses the lenient downloader.

- [ ] **Step 3: Implement strict text and local parsing without changing leniency**

Factor current text normalization into one private helper and add strict parsing:

```python
def _normalize_manifest_rows(
    rows: list[dict[str, typing.Any]],
) -> list[dict[str, typing.Any]]:
    for row in rows:
        if "text" in row:
            raw = row["text"]
            text = "" if raw is None else str(raw)
            row["text"] = text.replace("\n", " ").replace("\r", " ")
    return rows


def parse_manifest_text_strict(
    content: str,
    *,
    source: str = "manifest",
) -> list[dict[str, typing.Any]]:
    stripped = content.strip()
    if not stripped:
        return []
    if stripped.startswith("["):
        try:
            parsed = json.loads(stripped)
        except json.JSONDecodeError as exc:
            raise ValueError(f"{source}: malformed JSON array: {exc}") from exc
        if not isinstance(parsed, list) or not all(
            isinstance(row, dict) for row in parsed
        ):
            raise ValueError(f"{source}: expected JSON array of objects")
        return _normalize_manifest_rows(parsed)

    rows: list[dict[str, typing.Any]] = []
    for line_number, line in enumerate(stripped.splitlines(), start=1):
        if not line.strip():
            continue
        try:
            parsed_row = json.loads(line)
        except json.JSONDecodeError as exc:
            msg = f"{source}: malformed JSON at line {line_number}: {exc}"
            raise ValueError(msg) from exc
        if not isinstance(parsed_row, dict):
            msg = f"{source}: expected JSON object at line {line_number}"
            raise ValueError(msg)
        rows.append(parsed_row)
    return _normalize_manifest_rows(rows)
```

`load_manifest_strict(path)` must read UTF-8 text and propagate `OSError`, then
call `parse_manifest_text_strict(content, source=path)`.

- [ ] **Step 4: Implement strict GCS and packaged workflow boundaries**

Add `download_jsonl_manifest_strict(storage_client, gcs_manifest_uri)` beside
the lenient downloader. It downloads once, calls
`manifest.parse_manifest_text_strict`, logs the parsed count, and returns rows.
Change `artifacts.load_canonical_rows` to call
`manifest.load_manifest_strict`. Change `evaluate.evaluate_run` to call the
strict GCS downloader. Do not alter exploratory callers or their tests.

- [ ] **Step 5: Run strict-parser tests and verify GREEN**

Run the Step 2 command. Expected: all selected tests pass and the original
lenient parser/downloader tests still assert skipped malformed rows.

- [ ] **Step 6: Commit strict packaged parsing**

```bash
git add model/src/common/manifest.py model/src/common/gcs_utils.py \
  model/src/gemini_sft/artifacts.py model/src/gemini_sft/evaluate.py \
  model/tests/common/tests/test_manifest.py \
  model/tests/common/tests/test_gcs_utils.py \
  model/tests/gemini_sft/test_workflow.py
git commit -m "fix(gemini-sft): reject malformed packaged manifests"
```

### Task 3: Reject unstripped canonical audio URIs before inference

**Files:**
- Modify: `model/src/common/manifest.py:183-216`
- Test: `model/tests/common/tests/test_manifest.py`
- Test: `model/tests/gemini_sft/test_workflow.py`

**Interfaces:**
- Consumes: raw canonical row dictionaries.
- Produces: a `CanonicalManifestIssue` with code
  `unstripped_audio_filepath`, field `audio_filepath`, and a fail-loud packaged
  prepare/eval boundary.

- [ ] **Step 1: Write the failing canonical validation test**

Add a valid canonical row whose URI is `"  gs://bucket/audio.flac  "` and assert:

```python
issues = manifest.validate_canonical_manifest([row], expected_split="eval")

self.assertEqual(
    [(issue.code, issue.field) for issue in issues],
    [("unstripped_audio_filepath", "audio_filepath")],
)
with self.assertRaisesRegex(
    ValueError,
    "audio_filepath must not contain leading or trailing whitespace",
):
    manifest.require_canonical_manifest([row], expected_split="eval")
```

Add a workflow assertion that prepare/eval rejects the row before provider
submission. Reuse existing fake storage and paid-call sentinel helpers.

- [ ] **Step 2: Run the new tests and verify RED**

Run:

```bash
cd model
safe-run -- uv run --extra dev --extra vertex pytest \
  tests/common/tests/test_manifest.py \
  tests/gemini_sft/test_workflow.py -q
```

Expected: validation currently returns no whitespace issue, so the new tests
fail at the issue assertion or provider-call sentinel.

- [ ] **Step 3: Add the strict whitespace issue**

In `_validate_required_fields`, after required-field validation and before URI
shape validation, add:

```python
raw_audio_filepath = row.get("audio_filepath")
audio_filepath = _stripped_string(raw_audio_filepath)
if (
    isinstance(raw_audio_filepath, str)
    and audio_filepath is not None
    and raw_audio_filepath != audio_filepath
):
    _add_issue(
        issues,
        "unstripped_audio_filepath",
        "audio_filepath must not contain leading or trailing whitespace",
        row_index=row_index,
        field="audio_filepath",
    )
```

Keep the existing GCS/FLAC validation so one row can report both independent
problems where applicable.

- [ ] **Step 4: Run canonical/workflow tests and verify GREEN**

Run the Step 2 command. Expected: all selected tests pass and valid already
stripped URIs retain existing behavior.

- [ ] **Step 5: Commit early URI validation**

```bash
git add model/src/common/manifest.py \
  model/tests/common/tests/test_manifest.py \
  model/tests/gemini_sft/test_workflow.py
git commit -m "fix(manifest): reject unstripped canonical audio URIs"
```

### Task 4: Apply modules-only imports to PR-owned Python changes

**Files:**
- Modify: `model/src/common/gcs_utils.py`
- Modify: `model/src/common/gemini/batch.py`
- Modify: `model/src/common/gemini/context.py`
- Modify: `model/src/common/gemini/eval_artifacts.py`
- Modify: `model/src/common/gemini/request_identity.py`
- Modify: `model/src/common/gemini/tuning_data.py`
- Modify: `model/src/common/gemini/vertex.py`
- Modify: `model/src/gemini_sft/artifacts.py`
- Modify: `model/src/gemini_sft/config.py`
- Modify: `model/src/gemini_sft/evaluate.py`
- Modify: `model/src/gemini_sft/prepare.py`
- Modify: `model/src/gemini_sft/records.py`
- Modify: `model/src/gemini_sft/reporting.py`
- Modify: `model/src/gemini_sft/target_execution.py`
- Modify the affected tests listed in the Standards review.

**Interfaces:**
- Consumes: existing public APIs unchanged.
- Produces: behaviorally identical code using module-qualified symbols.

- [ ] **Step 1: Convert direct standard-library imports mechanically**

Apply these exact forms wherever the PR added their symbols:

```python
import collections
import collections.abc
import dataclasses
import pathlib
import typing
```

Then qualify usages as `collections.defaultdict`, `collections.abc.Sequence`,
`dataclasses.dataclass`, `dataclasses.field`, `pathlib.Path`,
`pathlib.PurePosixPath`, `typing.Any`, `typing.Final`, and
`typing.TYPE_CHECKING`. Preserve `from __future__ import annotations` as the
required future-import form.

- [ ] **Step 2: Convert direct local imports mechanically**

Use package-to-module imports and qualified calls. The production mapping is:

```python
from common import gcs_utils
from common import inference_manifest
from common import manifest
from common import scoring
from common.gemini import batch
from common.gemini import context
from common.gemini import eval_artifacts
from common.gemini import request_identity
from common.gemini import tuning_data
from common.gemini import vertex
from gemini_sft import artifacts
from gemini_sft import config
from gemini_sft import preflight
from gemini_sft import records
from gemini_sft import reporting
from gemini_sft import target_execution
```

Only import modules that each file actually uses. Replace every moved symbol
with its module-qualified form. Under `typing.TYPE_CHECKING`, import modules
such as `from google.cloud import storage` and `from common.gemini import
context`, then qualify annotations. Remove the function-local direct import in
`request_identity.py`; its top-level `vertex` module import is safe because
`vertex.py` does not import `request_identity.py`.

Apply the same rule to affected tests and `model/tests/sft_eval_fixtures.py`,
using module aliases such as `context`, `eval_artifacts`, `request_identity`,
`target_execution`, and `sft_eval_fixtures` rather than importing their public
symbols directly.

- [ ] **Step 3: Run focused tests after the import-only refactor**

Run:

```bash
cd model
safe-run -- uv run --extra dev --extra scoring --extra vertex pytest \
  tests/common/tests/test_gcs_utils.py \
  tests/common/tests/test_gemini_batch.py \
  tests/common/tests/test_gemini_context.py \
  tests/common/tests/test_gemini_eval_artifacts.py \
  tests/common/tests/test_gemini_vertex.py \
  tests/common/tests/test_manifest.py \
  tests/common/tests/test_scoring.py \
  tests/common/tests/test_tuning_data.py \
  tests/gemini_sft/test_config.py \
  tests/gemini_sft/test_reporting.py \
  tests/gemini_sft/test_target_execution.py \
  tests/gemini_sft/test_workflow.py -q
```

Expected: the full focused PR set passes with no import or annotation errors.

- [ ] **Step 4: Commit modules-only imports**

Stage only the Python files changed in this task and commit:

```bash
git commit -m "style(gemini-sft): follow modules-only imports"
```

### Task 5: Complete required Python documentation

**Files:**
- Modify: production/test files identified in Task 4.

**Interfaces:**
- Consumes/Produces: no runtime interface changes; documentation only.

- [ ] **Step 1: Add module docstrings to new test/helper modules**

Use these exact descriptions as the first statement in each module:

```python
"""Tests for Gemini prior-context construction."""
"""Tests for Gemini evaluation artifact paths."""
"""Tests for Gemini SFT evaluation reporting."""
"""Tests for Gemini SFT target execution."""
"""Shared fixtures for Gemini SFT evaluation tests."""
```

Apply them respectively to `test_gemini_context.py`,
`test_gemini_eval_artifacts.py`, `test_reporting.py`,
`test_target_execution.py`, and `sft_eval_fixtures.py`. Module docstrings must
precede `from __future__ import annotations`.

- [ ] **Step 2: Add `Attributes:` sections to public record classes**

Document every declared public field for `context.ContextTurn`,
`eval_artifacts.EvalTargetArtifactPaths`, `artifacts.EvalRowsWithHistory`,
`config.EvalModelTarget`, `config.EvalExecutionConfig`, `config.RunPaths`,
`config.RunConfig`, `reporting.ReportArtifacts`, `reporting.TargetMetrics`,
`reporting.EvalReport`, `target_execution.OnlineResumeState`, and
`target_execution.OnlinePredictionMap`. Use this concrete shape, replacing the
field lines with each class's declared names and semantics:

```python
"""One previous transcript and its source audio URI.

Attributes:
    audio_uri: Source URI retained for provenance.
    text: Prior transcript text supplied to Gemini.
"""
```

- [ ] **Step 3: Complete non-trivial public function docstrings**

For the public/non-trivial functions identified by the Standards review in
`batch.py`, `context.py`, `eval_artifacts.py`, `request_identity.py`,
`manifest.py`, `artifacts.py`, `config.py`, `evaluate.py`, `reporting.py`,
`target_execution.py`, and `vertex.py`, add all applicable sections in this
order. For example, `build_request_identity` must use this complete content:

```python
"""Build the deterministic identity for Gemini evaluation requests.

Args:
    target_label: Artifact label for the evaluated model target.
    model: Publisher model ID or Vertex model resource name.
    eval_manifest_uri: Durable URI of the evaluated canonical manifest.
    audio_uris: Ordered audio URI sequence represented by the identity.
    request_digests: Ordered digests of the corresponding request payloads.
    system_prompt: System instruction supplied to Gemini.
    user_prompt: Current-turn transcription prompt.
    prior_context_count: Maximum prior transcript turns per request.
    prior_context_mode: Request encoding used for prior transcripts.
    generation_config: Generation parameters supplied to Gemini.
    safety_settings: Ordered Gemini safety-setting dictionaries.

Returns:
    JSON-safe identity dictionary suitable for hashing and metadata storage.

Raises:
    ValueError: If audio URIs and request digests have different lengths.
"""
```

Omit `Returns:` only for functions that always return `None`; omit `Raises:`
only when the function has no explicit or intentionally propagated input
validation. Describe every parameter from the real signature and every
explicitly enforced invalid-input condition.

- [ ] **Step 4: Format and test documentation-only changes**

Run:

```bash
safe-run -- uv run ruff format model/src/common/gemini \
  model/src/gemini_sft model/tests/common/tests model/tests/gemini_sft \
  model/tests/sft_eval_fixtures.py
safe-run -- uv run ruff check model/src/common/gemini \
  model/src/gemini_sft model/tests/common/tests model/tests/gemini_sft \
  model/tests/sft_eval_fixtures.py
```

Expected: both commands exit 0.

- [ ] **Step 5: Commit documentation compliance**

Stage only Task 5 files and commit:

```bash
git commit -m "docs(gemini-sft): complete Python API documentation"
```

### Task 6: Final formatter and verification gate

**Files:**
- Modify: `model/tests/gemini_sft/test_target_execution.py` only if Ruff still
  reports the known lambda wrapping difference.
- Verify: every file changed since `origin/main`.

**Interfaces:**
- Consumes: completed Tasks 1-5.
- Produces: a locally clean, fully verified PR head ready to push and resend.

- [ ] **Step 1: Apply the root formatter**

Run:

```bash
safe-run -- uv run ruff format model/tests/gemini_sft/test_target_execution.py
```

Expected: the known `GenerateContentConfig.side_effect` lambda is rewritten to
the root Ruff 0.15.12 layout.

- [ ] **Step 2: Run the full focused PR test suite**

Run:

```bash
cd model
safe-run -- uv run --extra dev --extra scoring --extra vertex pytest \
  tests/common/tests/test_gcs_utils.py \
  tests/common/tests/test_gemini_context.py \
  tests/common/tests/test_gemini_batch.py \
  tests/common/tests/test_gemini_eval_artifacts.py \
  tests/common/tests/test_gemini_vertex.py \
  tests/common/tests/test_inference_manifest.py \
  tests/common/tests/test_manifest.py \
  tests/common/tests/test_scoring.py \
  tests/common/tests/test_tuning_data.py \
  tests/gemini_sft/test_config.py \
  tests/gemini_sft/test_reporting.py \
  tests/gemini_sft/test_target_execution.py \
  tests/gemini_sft/test_workflow.py -q
```

Expected: all tests and subtests pass.

- [ ] **Step 3: Run root quality gates**

Run each command and require exit 0:

```bash
safe-run -- uv run ruff check
safe-run -- uv run ruff format --check
safe-run -- uv run ty check
safe-run -- uv run vulture
git diff --check origin/main...HEAD
```

`ty` may print the existing unrelated unused-ignore warnings in backend
segmentation tests, but it must exit 0 and produce no new diagnostics in PR
files.

- [ ] **Step 4: Inspect final scope and preserve user files**

Run:

```bash
git status --short --branch
git diff --stat origin/main...HEAD
git diff origin/main...HEAD -- model/src model/tests
```

Expected: only intended PR/remediation files are tracked; all pre-existing
untracked inference manifests, `model/research/`, and `results/` remain
untouched.

- [ ] **Step 5: Commit any formatter-only remainder**

If Step 1 changed a file not already committed, stage that exact file and run:

```bash
git commit -m "style(gemini-sft): satisfy root formatter"
```

Do not push until the user explicitly requests it.
