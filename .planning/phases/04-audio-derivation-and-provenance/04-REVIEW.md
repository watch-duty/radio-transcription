---
phase: 04-audio-derivation-and-provenance
reviewed: 2026-05-28T04:44:35Z
depth: standard
files_reviewed: 14
files_reviewed_list:
  - model/scripts/sft/dataset_split/audio.py
  - model/scripts/sft/dataset_split/artifacts.py
  - model/scripts/sft/dataset_split/model_writers.py
  - model/scripts/sft/dataset_split/publisher.py
  - model/scripts/sft/dataset_split/leakage.py
  - model/scripts/sft/dataset_split/canonical.py
  - model/scripts/sft/dataset_split/reports.py
  - model/scripts/sft/tests/test_audio_derivation.py
  - model/scripts/sft/tests/test_dataset_artifacts.py
  - model/scripts/sft/tests/test_model_writers.py
  - model/scripts/sft/tests/test_dataset_publisher.py
  - model/scripts/sft/tests/test_dataset_split_leakage.py
  - model/scripts/sft/tests/test_dataset_canonical.py
  - model/scripts/sft/tests/test_dataset_reports.py
findings:
  blocker: 3
  critical: 3
  warning: 3
  info: 0
  total: 6
status: issues_found
---

# Phase 04: Code Review Report

**Reviewed:** 2026-05-28T04:44:35Z
**Depth:** standard
**Files Reviewed:** 14
**Status:** issues_found

## Summary

Reviewed the Phase 4 audio derivation, artifact publication, model writer, leakage, canonical, report, and targeted test files. The main risks are around immutable GCS publication failure modes, unchecked FFmpeg output correctness, and unsafe external audio download behavior. Tests cover the happy paths, but they miss several failure paths that can leave unretryable dataset versions or publish incorrect model-ready audio metadata.

## Blockers

### BL-01: Partial Audio Uploads Can Poison an Immutable Dataset Version

**File:** `model/scripts/sft/dataset_split/audio.py:359`
**Classification:** BLOCKER
**Issue:** `_prepare_audio_for_publication()` materializes each plan in sequence, and `_materialize_plan()` uploads the generated/copied audio immediately at lines 430-435. If segment N uploads successfully and segment N+1 later fails during FFmpeg, probe, copy, or upload, publication aborts after creating `sft/{dataset_version_id}/audio/...` objects but before final manifests. A rerun with the same `dataset_version_id` then fails the prefix-existence guard, leaving a permanently partial dataset version.
**Fix:** Split local materialization from GCS upload. Generate/copy/probe every non-reused audio file into scratch first and only start `upload_file_create_only()` after all local work succeeds.

```python
local_results = []
for plan in plans:
    enriched, local_output_path = _materialize_plan_locally(
        plan,
        scratch_dir=Path(scratch_dir),
        version_summary=version_summary,
        runner=runner,
    )
    local_results.append((plan, enriched, local_output_path))

for plan, _, local_output_path in local_results:
    if local_output_path is not None:
        _upload_audio_file(
            storage_client,
            uri=plan.destination_uri,
            local_path=local_output_path,
        )
```

### BL-02: FFmpeg Output Duration Is Recorded But Never Validated

**File:** `model/scripts/sft/dataset_split/audio.py:418`
**Classification:** BLOCKER
**Issue:** Derived and transcoded outputs are probed at lines 418 and 426, but the probed `output_duration` is only stored in metadata. The code uploads the file and returns manifest rows using `segment.duration` even if FFmpeg produced a shorter, longer, or otherwise wrong clip. This can happen when the source barely passes the tolerance check or FFmpeg truncates near EOF.
**Fix:** Validate every generated output duration against the requested row duration before upload, with a small tolerance for codec/probe precision. Fail with row context if the output does not match.

```python
def _validate_output_duration(segment: LabeledSegment, probe: AudioProbe) -> None:
    tolerance = _duration_tolerance(segment.duration)
    if abs(probe.duration - segment.duration) > tolerance:
        raise AudioDerivationError(
            "generated audio duration mismatch "
            f"output_duration={probe.duration} expected={segment.duration} "
            f"tolerance={tolerance} {_row_context(segment)}"
        )

# after probing derived/transcoded output and before upload
_validate_output_duration(plan.segment, output_probe)
```

### BL-03: External Audio Downloads Allow SSRF and Plain HTTP

**File:** `model/scripts/sft/dataset_split/audio.py:35`
**Classification:** BLOCKER
**Issue:** Manifest-controlled `audio_uri` values may use both `http` and `https` schemes, and `_download_external_source()` calls `requests.get()` with default redirect handling at lines 465-469. A malicious or compromised manifest row can make the offline job fetch internal services such as metadata endpoints, loopback, RFC1918 hosts, or an HTTPS URL that redirects to those targets. Plain HTTP also permits tampering before ffprobe/ffmpeg consumes the file.
**Fix:** Treat external downloads as an explicit trust boundary: require HTTPS, disable redirects or revalidate each redirected URL, reject loopback/link-local/private IPs after DNS resolution, and preferably use a configured allowlist for known source hosts.

```python
def _validate_external_url(source_uri: str) -> None:
    parsed = urlparse(source_uri)
    if parsed.scheme != "https":
        raise AudioDerivationError("external source audio must use https")
    if not parsed.hostname:
        raise AudioDerivationError("external source audio host is required")
    _reject_private_or_link_local_host(parsed.hostname)

_validate_external_url(source_uri)
with requests.get(
    source_uri,
    stream=True,
    timeout=EXTERNAL_DOWNLOAD_TIMEOUT,
    allow_redirects=False,
) as response:
    ...
```

## Warnings

### WR-01: FFprobe and FFmpeg Calls Can Hang Indefinitely

**File:** `model/scripts/sft/dataset_split/audio.py:89`
**Classification:** WARNING
**Issue:** `probe_audio()`, `_run_ffmpeg()`, and `_program_version()` call the runner without any subprocess timeout. A corrupt or adversarial audio file can hang ffprobe/ffmpeg and block the dataset publication job forever, despite the external download itself having timeouts.
**Fix:** Add bounded subprocess timeouts and map `subprocess.TimeoutExpired` into `AudioDerivationError` with row/program context.

```python
FFPROBE_TIMEOUT_SECONDS = 60
FFMPEG_TIMEOUT_SECONDS = 300

runner(command, capture_output=True, check=True, text=True, timeout=FFPROBE_TIMEOUT_SECONDS)
runner(command, capture_output=True, check=True, text=True, timeout=FFMPEG_TIMEOUT_SECONDS)
```

### WR-02: Non-Finite Segment Spans Reach FFmpeg

**File:** `model/scripts/sft/dataset_split/audio.py:610`
**Classification:** WARNING
**Issue:** `_validate_segment_span()` rejects `duration <= 0` and `offset < 0`, but `NaN` and positive/negative infinity pass those comparisons. Those values then flow into `_duration_tolerance()`, action selection, and FFmpeg arguments such as `-ss nan` or `-t inf`, producing late and inconsistent failures.
**Fix:** Require finite numeric `offset`, `duration`, and probed source duration before planning or deriving.

```python
import math

def _validate_segment_span(segment: LabeledSegment) -> None:
    if not math.isfinite(segment.duration) or segment.duration <= 0:
        raise AudioDerivationError(...)
    if not math.isfinite(segment.offset) or segment.offset < 0:
        raise AudioDerivationError(...)
```

### WR-03: Model-Ready Provenance Validation Does Not Check Field Consistency

**File:** `model/scripts/sft/dataset_split/leakage.py:123`
**Classification:** WARNING
**Issue:** `validate_model_ready_audio()` checks that transformation metadata has required keys and that `split` and `source_group` match the segment, but it does not verify `original_audio_uri`, `source_audio_uri`, `offset`, `duration`, or action-specific URI fields against the `LabeledSegment`. Canonical manifests and reports can therefore accept inconsistent or stale provenance if callers provide enriched segments directly or through a custom audio preparer.
**Fix:** Extend validation to compare provenance values against the segment and action semantics before canonical/report publication.

```python
expected = {
    "original_audio_uri": segment.original_audio_uri,
    "source_audio_uri": segment.audio_uri,
    "offset": segment.offset,
    "duration": segment.duration,
}
for key, expected_value in expected.items():
    if metadata[key] != expected_value:
        raise _model_ready_error(segment, action, f"transformation_metadata.{key}", "must match segment")
if action == "derived" and segment.derived_audio_uri != segment.model_ready_audio_uri:
    raise _model_ready_error(segment, action, "derived_audio_uri", "must equal model_ready_audio_uri")
```

---

_Reviewed: 2026-05-28T04:44:35Z_
_Reviewer: the agent (gsd-code-reviewer)_
_Depth: standard_
