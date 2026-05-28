# Phase 3: GCS Artifacts And Model Writers - Pattern Map

**Mapped:** 2026-05-27
**Files analyzed:** 12 new/modified files
**Analogs found:** 12 / 12

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|-------------------|------|-----------|----------------|---------------|
| `model/scripts/sft/dataset_split/artifacts.py` | service, utility | file-I/O, batch | `backend/pipeline/common/storage/gcs_uploader.py`; `model/scripts/sft/dataset_split/gcs_io.py`; `model/colabs/common/gcs_utils.py` | role-match |
| `model/scripts/sft/dataset_split/canonical.py` | utility | transform, batch | `model/scripts/sft/dataset_split/normalize.py`; `model/scripts/sft/dataset_split/types.py`; `model/scripts/sft/dataset_split/leakage.py` | exact |
| `model/scripts/sft/dataset_split/model_writers.py` | utility, service | transform, file-I/O | `model/scripts/sft/pipeline.py`; `model/colabs/common/sft.py`; `model/colabs/common/manifest.py` | role-match |
| `model/scripts/sft/dataset_split/publisher.py` | service, orchestrator | transform, file-I/O, batch | `model/scripts/sft/pipeline.py`; `model/scripts/sft/dataset_split/artifacts.py`; `backend/pipeline/common/storage/gcs_uploader.py` | role-match |
| `model/scripts/sft/dataset_split/reports.py` | utility | transform, batch | `model/scripts/sft/dataset_split/balance.py`; `model/scripts/sft/preflight.py` | role-match |
| `model/colabs/common/sft.py` | utility | transform, validation | existing `model/colabs/common/sft.py`; `model/colabs/common/vertex.py` | exact |
| `model/scripts/sft/tests/test_dataset_artifacts.py` | test | file-I/O, batch | `model/scripts/sft/tests/test_dataset_split_gcs_io.py`; `backend/pipeline/common/storage/tests/test_gcs_uploader.py` | exact |
| `model/scripts/sft/tests/test_dataset_canonical.py` | test | transform | `model/scripts/sft/tests/test_dataset_split_normalize.py`; `model/scripts/sft/tests/test_dataset_split_leakage.py` | exact |
| `model/scripts/sft/tests/test_dataset_publisher.py` | test | file-I/O, transform, batch | `model/scripts/sft/tests/test_pipeline_build.py`; `model/scripts/sft/tests/test_dataset_artifacts.py`; `backend/pipeline/common/storage/tests/test_gcs_uploader.py` | role-match |
| `model/scripts/sft/tests/test_dataset_reports.py` | test | transform, batch | `model/scripts/sft/tests/test_dataset_split_balance.py`; `model/scripts/sft/tests/test_pipeline_build.py` | role-match |
| `model/scripts/sft/tests/test_model_writers.py` | test | transform | `model/scripts/sft/tests/test_pipeline_build.py`; `model/colabs/common/tests/test_sft.py` | exact |
| `model/colabs/common/tests/test_sft.py` | test | transform, validation | existing `model/colabs/common/tests/test_sft.py` | exact |

## Pattern Assignments

### `model/scripts/sft/dataset_split/artifacts.py` (service/utility, file-I/O + batch)

**Primary analogs:** `backend/pipeline/common/storage/gcs_uploader.py`, `model/scripts/sft/dataset_split/gcs_io.py`, `model/colabs/common/gcs_utils.py`

**Imports pattern** from `gcs_io.py` (lines 1-4) and `gcs_uploader.py` (lines 3-9):

```python
from __future__ import annotations

import json
from typing import Any, Protocol
```

```python
import logging
from collections.abc import Callable

import numpy as np
from google.cloud import storage

logger = logging.getLogger(__name__)
```

For Phase 3, keep imports light and model-tooling local. Use `TYPE_CHECKING` or untyped `object` for fakeable GCS clients if that avoids requiring live GCS in tests.

**GCS URI validation pattern** from `dataset_split/gcs_io.py` (lines 7-18):

```python
class GcsInputError(ValueError):
    """Raised when configured GCS input cannot be read or parsed."""


class TextReader(Protocol):
    def read_text(self, uri: str) -> str:
        """Read the configured URI as text."""


def require_gs_uri(uri: str, *, label: str) -> None:
    if not uri.startswith("gs://"):
        raise GcsInputError(f"{label} must be a gs:// URI: {uri}")
```

Copy this style for `DatasetArtifactError` / `DatasetVersionExistsError`: typed `ValueError` subclasses with user-actionable messages. Keep fakeable protocols around GCS behavior instead of binding tests to real clients.

**GCS parse/existence helper pattern** from `common/gcs_utils.py` (lines 11-18, 24-45):

```python
def parse_gcs_uri(gcs_uri: str) -> tuple[str, str]:
    """Parses a GCS URI into bucket name and blob path."""
    if not gcs_uri.startswith("gs://"):
        raise ValueError("GCS URI must start with 'gs://'")
    parts = gcs_uri[len("gs://") :].split("/", 1)
    bucket_name = parts[0]
    blob_path = parts[1] if len(parts) > 1 else ""
    return bucket_name, blob_path
```

```python
def blob_exists(
    storage_client: storage.Client,
    gcs_uri: str,
    *,
    timeout: float | tuple[float, float] | None = None,
) -> bool:
    bucket_name, blob_path = parse_gcs_uri(gcs_uri)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    if timeout is None:
        return blob.exists(retry=DEFAULT_RETRY)
    return blob.exists(retry=DEFAULT_RETRY, timeout=timeout)
```

Use `parse_gcs_uri()` rather than adding a new parser. For Phase 3 prefix existence, adapt this to `client.list_blobs(bucket_name, prefix=prefix, max_results=1)` and fail if any object is returned.

**Create-only upload pattern** from `backend/pipeline/common/storage/gcs_uploader.py` (lines 23-57):

```python
def upload_bytes(
    self,
    data: bytes,
    bucket_name: str,
    destination_path: str,
    content_type: str = "application/octet-stream",
) -> str:
    try:
        bucket = self.gcs_client.bucket(bucket_name)
        blob = bucket.blob(destination_path)
        blob.upload_from_string(
            data, content_type=content_type, if_generation_match=0
        )
        uri = f"gs://{bucket_name}/{destination_path}"
        logger.debug("Uploaded artifact to %s", uri)
    except Exception:
        logger.exception(
            "Failed to upload artifact to gs://%s/%s",
            bucket_name,
            destination_path,
        )
        raise
    else:
        return uri
```

Phase 3 must keep `if_generation_match=0` but should convert precondition failures into a hard dataset-version-exists error, not idempotent success.

**Anti-pattern to avoid** from `backend/pipeline/common/gcp_helper.py` research: the async ingestion helper treats HTTP 412 as success for idempotent audio staging. Do not copy that behavior for immutable dataset-version publication.

**Testing pattern** from `backend/pipeline/common/storage/tests/test_gcs_uploader.py` (lines 10-32):

```python
mock_gcs = MagicMock()
mock_bucket = MagicMock()
mock_gcs.bucket.return_value = mock_bucket
mock_blob = MagicMock()
mock_bucket.blob.return_value = mock_blob

uploader = GCSAudioUploader(gcs_client=mock_gcs)

uri = uploader.upload_bytes(
    data=b"test-data",
    bucket_name="test-bucket",
    destination_path="path/to/obj.txt",
    content_type="text/plain",
)

self.assertEqual(uri, "gs://test-bucket/path/to/obj.txt")
mock_blob.upload_from_string.assert_called_once_with(
    b"test-data", content_type="text/plain", if_generation_match=0
)
```

Apply this fake-client pattern in `test_dataset_artifacts.py` for prefix checks and per-object precondition failures.

---

### `model/scripts/sft/dataset_split/publisher.py` (service/orchestrator, transform + file-I/O + batch)

**Primary analogs:** `model/scripts/sft/pipeline.py`, `dataset_split/artifacts.py`, `backend/pipeline/common/storage/gcs_uploader.py`

**Pipeline orchestration pattern** from `model/scripts/sft/pipeline.py`: build model-facing artifacts from canonical rows, validate target-specific shapes, stage outputs, and return/upload deterministic artifact URIs. For Phase 3, keep the same orchestration idea but replace legacy `round_id` and Gemini-only behavior with immutable dataset-version publication.

**Create-only publication pattern** from `dataset_split/artifacts.py` and `backend/pipeline/common/storage/gcs_uploader.py`: call the prefix guard before any upload, then route every object write through the helper that uses `if_generation_match=0`.

Use this sequence in `publish_dataset_version_artifacts()`:

```python
layout = DatasetArtifactLayout.for_dataset_version(
    dataset_version_id,
    root_prefix=root_prefix,
)
ensure_dataset_version_absent(storage_client, layout.root_uri)
canonical = canonical_manifests(segments)
per_dataset = per_dataset_manifests(segments)
nemo = build_nemo_inputs(...)
whisper = build_whisper_inputs(...)
gemini = build_gemini_inputs(...)
metadata = build_dataset_version_metadata(...)
report = build_dataset_version_report(...)
```

Then upload each serialized object with `upload_text_create_only(storage_client, uri, text, content_type=...)`. Do not call `blob.upload_from_string()` directly in `publisher.py`; that would bypass the Phase 3 overwrite safety contract.

**Artifact inventory pattern:** return a frozen result object with both structured `PublishedArtifact` entries and a plain URI inventory. Reports should receive the planned inventory so `reports/dataset_version_report.*` can list the final `config/`, `metadata/`, `manifests/`, `model_inputs/`, and `reports/` paths.

**Scope boundaries:** `publisher.py` is the final dataset-version writer, not a training runner. It must not instantiate `genai.Client`, submit tuning jobs, poll Vertex/Gemini jobs, derive clips, resample audio, delete prefixes, add force/resume cleanup, or mutate existing benchmark/eval manifests.

---

### `model/scripts/sft/dataset_split/canonical.py` (utility, transform + batch)

**Primary analogs:** `dataset_split/normalize.py`, `dataset_split/types.py`, `dataset_split/leakage.py`, `common/manifest.py`

**Imports pattern** from `dataset_split/normalize.py` (lines 1-12):

```python
from __future__ import annotations

from collections.abc import Iterable, Mapping

from dataset_split.config import InputDatasetConfig
from dataset_split.source_keys import resolve_source_group
from dataset_split.types import (
    ExcludedRow,
    LabeledSegment,
    NormalizationResult,
    RowValidationError,
)
```

For `canonical.py`, import `LabeledSegment` and `validate_split_integrity`; keep functions pure and pass segment tuples in.

**Internal source-of-truth model** from `dataset_split/types.py` (lines 14-33):

```python
@dataclass(frozen=True)
class LabeledSegment:
    dataset_name: str
    dataset_family: str
    source_strategy: str
    source_group: str
    audio_uri: str
    original_audio_uri: str
    text: str
    row_index: int
    offset: float = 0.0
    duration: float = 0.0
    timestamp: str | None = None
    example_id: str | None = None
    segment_id: str | None = None
    split: str | None = None
    model_ready_audio_uri: str | None = None
    derived_audio_uri: str | None = None
    transformation_metadata: dict[str, object] | None = None
    raw_row: dict[str, object] | None = None
```

Canonical rows should be built from this dataclass and must deliberately omit `raw_row`.

**Transform pattern** from `dataset_split/normalize.py` (lines 30-90):

```python
def normalize_manifest_rows(
    dataset: InputDatasetConfig,
    rows: Iterable[Mapping[str, object]],
    *,
    echo_registry: Mapping[str, set[str]] | None = None,
    source_map: Mapping[str, Mapping[str, str]] | None = None,
) -> NormalizationResult:
    segments: list[LabeledSegment] = []
    excluded: list[ExcludedRow] = []

    for row_index, row in enumerate(rows):
        audio_uri = _first_present_text(row, _AUDIO_URI_FIELDS)
        text = normalize_text(row.get("text"))

        if not text:
            excluded.append(...)
            continue

        if audio_uri is None:
            raise RowValidationError(
                f"dataset={dataset.name} row_index={row_index} "
                "missing audio URI"
            )
```

Copy the style: deterministic row iteration, explicit `row_index`, fail loud on structural errors, and separate soft exclusions/warnings from hard errors.

**Split integrity guard** from `dataset_split/leakage.py` (lines 59-62):

```python
def validate_split_integrity(segments: tuple[LabeledSegment, ...]) -> None:
    validate_split_leakage(segments)
    validate_no_duplicate_audio_spans(segments)
```

Call this before emitting canonical or per-dataset manifests so Phase 3 never publishes leaking artifacts.

**Existing eval row contract** from `common/manifest.py` (lines 22-36, 44-87):

```python
@dataclass(frozen=True)
class CanonicalRow:
    audio_filepath: str  # gs:// URI to the segment audio
    example_id: str
    segment_id: str
    offset: float
    duration: float
    text: str
```

```python
offset: float = float(entry.get("offset") or 0.0)
duration: float = float(entry.get("duration") or 0.0)
example_id: str = str(
    entry.get("example_id") or Path(audio_filepath).stem
)
segment_id: str = str(entry.get("segment_id", "001"))
```

Use the existing `audio_filepath`, `offset`, `duration`, `text`, `example_id`, and `segment_id` semantics in model-facing outputs. Canonical dataset-version manifests can be richer, but model writers must preserve these stable fields.

**Testing pattern** from `test_dataset_split_normalize.py` (lines 20-45, 87-104):

```python
def _dataset() -> InputDatasetConfig:
    return InputDatasetConfig(
        name="calls",
        family="bcfy_calls",
        manifest_uri="gs://bucket/calls.jsonl",
        source_strategy="bcfy_calls",
    )


class TestDatasetVersionNormalize(unittest.TestCase):
    def test_empty_text_is_excluded(self) -> None:
        result = normalize_manifest_rows(
            _dataset(), [{"audio_filepath": "gs://bucket/a.flac", "text": "  "}]
        )

        self.assertEqual(result.segments, ())
        self.assertEqual(len(result.excluded), 1)
        self.assertEqual(result.excluded[0].reason, "empty_text")
```

```python
def test_labeled_segment_contains_future_provenance_fields(self) -> None:
    result = normalize_manifest_rows(...)

    segment = result.segments[0]
    self.assertIsNone(segment.model_ready_audio_uri)
    self.assertIsNone(segment.derived_audio_uri)
    self.assertIsNone(segment.transformation_metadata)
    self.assertEqual(segment.raw_row["groupId"], "123")
```

Adapt this to assert canonical output includes enriched provenance fields but excludes `raw_row`.

---

### `model/scripts/sft/dataset_split/model_writers.py` (utility/service, transform + file-I/O)

**Primary analogs:** `model/scripts/sft/pipeline.py`, `model/colabs/common/sft.py`, `model/colabs/common/manifest.py`, `model/colabs/common/inference_hf.py`, `model/colabs/common/inference_nemo.py`, `model/colabs/common/vertex.py`

**Imports pattern** from `pipeline.py` (lines 16-31):

```python
from __future__ import annotations

import argparse
import json
import logging
import shutil
import sys
import tomllib
from pathlib import Path
from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:
    from collections.abc import Callable

logger = logging.getLogger(__name__)
```

For `model_writers.py`, prefer pure row/config builders over CLI parsing. Import `common.sft.build_example` inside Gemini-specific functions if preserving light import behavior matters.

**Existing Gemini JSONL build pattern** from `pipeline.py` (lines 153-183):

```python
from common.gcs_utils import parse_gcs_uri, upload_file_to_blob
from common.sft import build_example, validate_example

per_dataset_uris: dict[str, str] = {}
total_duration_seconds = 0.0

for ds_name in dataset_names:
    ds_cfg = registry["datasets"][ds_name]
    adapter = _make_adapter(
        ds_cfg, split=split, storage_client=storage_client
    )
    do_normalize = ds_cfg.get("normalize", False)

    examples: list[dict] = []
    for row in adapter.iter_rows():
        text = row.text
        if do_normalize:
            text = normalizer(text)
        ex = build_example(
            audio_uri=row.audio_filepath,
            gt_text=text,
            system_prompt=system_prompt,
            user_prompt=user_prompt,
        )
        if not validate_example(ex):
            logger.warning(
                f"[{ds_name}/{split}] skipping invalid example: {row.audio_filepath}"
            )
            continue
        examples.append(ex)
        total_duration_seconds += row.duration
```

Phase 3 should not silently skip structurally invalid Gemini rows if the target rejects them; return writer warnings for performance risks but hard-fail invalid shapes per D-20.

**JSONL serialization pattern** from `pipeline.py` (lines 185-199):

```python
out_path = staging_dir / f"{split}_{ds_name}.jsonl"
with open(out_path, "w") as f:
    for ex in examples:
        f.write(json.dumps(ex) + "\n")
logger.info(
    f"[{ds_name}/{split}] wrote {len(examples)} examples -> {out_path}"
)

gcs_uri = f"{GCS_SFT_PREFIX}/{round_id}/{split}_{ds_name}.jsonl"
bucket_name, blob_path = parse_gcs_uri(gcs_uri)
upload_file_to_blob(
    storage_client, bucket_name, blob_path, str(out_path)
)
```

For Phase 3 artifact modules, prefer in-memory JSONL strings or controlled staging, then pass bytes/text to the create-only uploader in `artifacts.py`. Do not use `upload_file_to_blob()` for final writes because it does not set `if_generation_match=0`.

**Gemini SFT nested shape** from `common/sft.py` (lines 17-63):

```python
def build_example(
    audio_uri: str,
    gt_text: str,
    system_prompt: str,
    user_prompt: str,
) -> dict[str, Any]:
    return {
        "systemInstruction": {
            "role": "system",
            "parts": [{"text": system_prompt}],
        },
        "contents": [
            {
                "role": "user",
                "parts": [
                    {
                        "fileData": {
                            "mimeType": "audio/flac",
                            "fileUri": audio_uri,
                        }
                    },
                    {"text": user_prompt},
                ],
            },
            {"role": "model", "parts": [{"text": gt_text}]},
        ],
    }
```

Update `build_example()` to accept a caller-supplied verified `mime_type` defaulting to `audio/flac`; keep this nested shape unchanged.

**Gemini validator pattern** from `common/sft.py` (lines 66-109):

```python
if "contents" not in example or "systemInstruction" not in example:
    return False
contents = example["contents"]
if not isinstance(contents, list) or len(contents) != 2:
    return False
...
file_uri = fd.get("fileUri", "")
if not isinstance(file_uri, str) or not file_uri.startswith("gs://"):
    return False
if fd.get("mimeType") != "audio/flac":
    return False
...
model_text = first_model_part.get("text") or ""
return bool(model_text.strip())
```

Extend the MIME check to allow verified Phase 3 MIME types, at minimum `audio/flac` and `audio/mpeg`.

**NeMo/HF model-facing row field pattern** from `common/inference_nemo.py` (lines 54-69, 87-124) and `common/inference_hf.py` (lines 65-79, 97-147):

```python
manifest_data: List of manifest entries. Assumes 'audio_filepath' points to a
GCS URI for an already-segmented audio file.
```

```python
for entry in batch:
    audio_gcs_uri = entry["audio_filepath"]
    try:
        local_path = download_to_scratch(
            storage_client, audio_gcs_uri, scratch_dir
        )
        ...
        result_row = dict(batch_entries[j])
        result_row[f"pred_text_{selected_model}"] = transcript.strip()
        results_list.append(result_row)
    except Exception as e:
        logger.error(f"Failed to process {audio_gcs_uri}: {e}")
        continue
```

Writers should emit model-input rows with `audio_filepath`, `text`, `duration`, and `offset` for NeMo compatibility. Whisper rows can preserve richer metadata, but keep the input URI/path obvious and warnings structured.

**Tuning config field pattern** from `common/vertex.py` (lines 143-203):

```python
def submit_tuning_job(
    *,
    train_uri: str,
    display_name: str,
    project: str,
    location: str,
    base_model: str = "gemini-2.5-flash",
    val_uri: "str | None" = None,
    epoch_count: int = 5,
    adapter_size: str = "ONE",
    lr_multiplier: float = 1.0,
    poll_interval: int = 30,
) -> str:
    ...
    cfg_kwargs: dict[str, Any] = {
        "tuned_model_display_name": display_name,
        "epoch_count": epoch_count,
        "adapter_size": _ADAPTER_ENUM[adapter_size],
        "learning_rate_multiplier": lr_multiplier,
    }
    if val_uri:
        cfg_kwargs["validation_dataset"] = types.TuningDataset(gcs_uri=val_uri)

    job = client.tunings.tune(
        base_model=base_model,
        training_dataset=types.TuningDataset(gcs_uri=train_uri),
        config=types.CreateTuningJobConfig(**cfg_kwargs),
    )
```

Phase 3 should emit config fragments only. Use `trainingDatasetUri` as required, `validationDatasetUri` as optional, and keep base model, region, adapter size, epochs, and learning-rate multiplier configurable.

**Testing pattern** from `test_pipeline_build.py` (lines 421-492):

```python
class TestBuildSplitJsonl(unittest.TestCase):
    """_build_split_jsonl writes per-dataset + combined JSONL and returns URIs + duration."""

    def test_train_split_builds_uploads_and_sums_duration(self) -> None:
        from types import SimpleNamespace

        import pipeline

        rows = [
            SimpleNamespace(
                audio_filepath="gs://b/a1.flac",
                text="engine 41 responding",
                duration=2.0,
            ),
            SimpleNamespace(
                audio_filepath="gs://b/a2.flac",
                text="copy that",
                duration=3.0,
            ),
        ]
        fake_adapter = unittest.mock.MagicMock()
        fake_adapter.iter_rows.return_value = iter(rows)
```

Use small in-memory segment fixtures and assert row shape, config fields, warning payloads, and line counts.

---

### `model/scripts/sft/dataset_split/reports.py` (utility, transform + batch)

**Primary analogs:** `dataset_split/balance.py`, `model/scripts/sft/preflight.py`

**Imports and dataclass serialization pattern** from `balance.py` (lines 1-8, 38-73):

```python
from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime

from dataset_split.types import LabeledSegment
```

```python
@dataclass(frozen=True)
class BalanceReport:
    weighted_score: float
    component_deltas: tuple[BalanceComponentDelta, ...]
    duration_buckets: dict[str, dict[str, float]]
    transcript_length_buckets: dict[str, dict[str, float]]
    time_distribution: dict[str, object]

    def to_dict(self) -> dict[str, object]:
        return {
            "weighted_score": self.weighted_score,
            "component_deltas": [
                component.to_dict() for component in self.component_deltas
            ],
            "duration_buckets": self.duration_buckets,
            "transcript_length_buckets": self.transcript_length_buckets,
            "time_distribution": self.time_distribution,
        }
```

Reports should use frozen dataclasses plus explicit `to_dict()` methods for stable JSON serialization.

**Aggregation pattern** from `balance.py` (lines 91-105, 217-229):

```python
def build_balance_report(
    segments: tuple[LabeledSegment, ...],
    *,
    train_ratio: float = 0.8,
    eval_ratio: float = 0.2,
    weights: Mapping[str, float] | None = None,
) -> BalanceReport:
    if abs((train_ratio + eval_ratio) - 1.0) > 1e-9:
        raise ValueError("train_ratio + eval_ratio must equal 1.0")
    for segment in segments:
        _require_split(segment)
```

```python
weighted_score = sum(
    _weight_for_component(component.name, effective_weights)
    * component.absolute_delta
    for component in component_deltas
)

return BalanceReport(
    weighted_score=weighted_score,
    component_deltas=tuple(component_deltas),
    duration_buckets=duration_bucket_report,
    transcript_length_buckets=transcript_bucket_report,
    time_distribution=_time_distribution(segments),
)
```

Copy the "validate first, compute deterministic dicts, return dataclass" structure.

**Report writing pattern** from `preflight.py` (lines 33-40, 281-293):

```python
@dataclass
class PreflightReport:
    failures: list[str] = field(default_factory=list)
    offending_ids: list[str] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return len(self.failures) == 0
```

```python
def _write_report(report: PreflightReport, report_path: Path) -> None:
    report_dict = {
        "passed": report.passed,
        "failures": report.failures,
        "offending_ids": report.offending_ids,
    }
    report_path.write_text(json.dumps(report_dict, indent=2))
    if report.passed:
        logger.info(f"Preflight passed. Report: {report_path}")
    else:
        logger.error(
            f"Preflight FAILED ({len(report.failures)} issues). "
            f"Report: {report_path}. Fix the data and re-run."
        )
```

Phase 3 report builders should return JSON-ready data and Markdown text. `artifacts.py` should own actual GCS upload, preserving one write path and artifact inventory collection.

**Testing pattern** from `test_dataset_split_balance.py` (lines 70-105, 107-139):

```python
def test_report_contains_weighted_score_and_component_deltas(self) -> None:
    report = build_balance_report((...))

    self.assertIsInstance(report, BalanceReport)
    self.assertGreaterEqual(report.weighted_score, 0.0)
    names = {component.name for component in report.component_deltas}
    self.assertTrue(any(name.startswith("dataset:") for name in names))
    self.assertTrue(any(name.startswith("family:") for name in names))
    self.assertTrue(any(name.startswith("global:") for name in names))
    self.assertIn("weighted_score", report.to_dict())
    self.assertIn("component_deltas", report.to_dict())
```

```python
def test_time_distribution_is_report_only(self) -> None:
    report = build_balance_report((...))

    self.assertTrue(report.time_distribution["report_only"])
    self.assertEqual(
        report.time_distribution["month"]["2026-01"],
        {"train": 1.0, "eval": 1.0},
    )
```

Use equivalent assertions for config copy/resolved config, leakage result, balance score/components, writer warnings, artifact inventory, and absence of SFT run metrics.

---

### `model/colabs/common/sft.py` (utility, transform + validation)

**Primary analog:** existing `model/colabs/common/sft.py`

**Module contract pattern** from `common/sft.py` (lines 1-9):

```python
"""Vertex AI audio-SFT JSONL builder and schema validator.

Provides ``build_example`` (LIB-04) — the current Vertex AI audio-SFT JSONL schema
with ``systemInstruction`` sibling of ``contents`` — and ``validate_example`` for local
schema-shape validation before submitting a paid tuning job (Pitfall 1).

No GCP project or bucket constants are defined in this module. All GCP identifiers are
caller-supplied parameters.
"""
```

Keep this module as a light pure builder/validator. Do not add GCS clients, Vertex clients, project IDs, or bucket constants.

**Change pattern:** extend the existing signature in place:

```python
def build_example(
    audio_uri: str,
    gt_text: str,
    system_prompt: str,
    user_prompt: str,
    mime_type: str = "audio/flac",
) -> dict[str, Any]:
```

Then use `mime_type` in `fileData`. Update `validate_example()` so verified MIME values pass and malformed/unsupported MIME values return `False`.

**Test analog** from `model/colabs/common/tests/test_sft.py` (lines 11-28, 76-117):

```python
class TestBuildExample(unittest.TestCase):
    def test_round_trips_audio_uri(self) -> None:
        from common.sft import build_example

        example = build_example(
            audio_uri="gs://bucket/seg001.flac",
            gt_text="Engine 41 copy",
            system_prompt="You are a transcriber.",
            user_prompt="Transcribe this.",
        )
        file_parts = [
            p for p in example["contents"][0]["parts"] if "fileData" in p
        ]
        self.assertEqual(
            file_parts[0]["fileData"]["fileUri"], "gs://bucket/seg001.flac"
        )
        self.assertEqual(file_parts[0]["fileData"]["mimeType"], "audio/flac")
```

```python
class TestValidateExample(unittest.TestCase):
    def test_accepts_well_formed_example(self) -> None:
        from common.sft import build_example, validate_example

        ex = build_example("gs://b/s.flac", "copy", "sys", "user")
        self.assertTrue(validate_example(ex))

    def test_rejects_wrong_mime_type(self) -> None:
        from common.sft import build_example, validate_example

        ex = build_example("gs://b/s.flac", "copy", "sys", "user")
        ex["contents"][0]["parts"][0]["fileData"]["mimeType"] = "audio/wav"
        self.assertFalse(validate_example(ex))
```

Add focused tests for default FLAC compatibility, explicit `audio/mpeg`, and unsupported MIME rejection.

---

### `model/scripts/sft/tests/test_dataset_artifacts.py` (test, file-I/O + batch)

**Primary analogs:** `test_dataset_split_gcs_io.py`, `backend/pipeline/common/storage/tests/test_gcs_uploader.py`

**Path setup/import pattern** from `test_dataset_split_gcs_io.py` (lines 1-20):

```python
from __future__ import annotations

import sys
import unittest
from pathlib import Path

_SFT_DIR = str(Path(__file__).resolve().parent.parent)
_COLABS_DIR = str(
    Path(__file__).resolve().parent.parent.parent.parent / "colabs"
)
if _SFT_DIR not in sys.path:
    sys.path.insert(0, _SFT_DIR)
if _COLABS_DIR not in sys.path:
    sys.path.insert(0, _COLABS_DIR)

from dataset_split.gcs_io import (  # noqa: E402
    GcsInputError,
    read_json_or_jsonl,
    require_gs_uri,
)
```

Use this exact path setup for new `model/scripts/sft/tests/*.py` files that import both `dataset_split.*` and `common.*`.

**Fake reader/client pattern** from `test_dataset_split_gcs_io.py` (lines 23-33):

```python
class FakeTextReader:
    def __init__(
        self, values: dict[str, str], errors: dict[str, Exception] | None = None
    ) -> None:
        self.values = values
        self.errors = errors or {}

    def read_text(self, uri: str) -> str:
        if uri in self.errors:
            raise self.errors[uri]
        return self.values[uri]
```

Use small fake classes for prefix existence, uploaded objects, and precondition failures. Keep live GCS out of unit tests.

**Create-only assertion pattern** from `test_gcs_uploader.py` (lines 27-32):

```python
self.assertEqual(uri, "gs://test-bucket/path/to/obj.txt")
mock_gcs.bucket.assert_called_with("test-bucket")
mock_bucket.blob.assert_called_with("path/to/obj.txt")
mock_blob.upload_from_string.assert_called_once_with(
    b"test-data", content_type="text/plain", if_generation_match=0
)
```

Add tests for:
- `test_layout_uses_dataset_version_root`
- `test_existing_prefix_fails`
- `test_create_only_precondition_failure_fails`
- `test_generation_targets_only_new_artifacts`

---

### `model/scripts/sft/tests/test_dataset_publisher.py` (test, file-I/O + transform + batch)

**Primary analogs:** `test_pipeline_build.py`, `test_dataset_artifacts.py`, `backend/pipeline/common/storage/tests/test_gcs_uploader.py`

**Pipeline fixture pattern:** follow `test_pipeline_build.py` by using small in-memory rows and asserting output payload shape, not by contacting external services. For publisher tests, construct split-populated `LabeledSegment` fixtures that cover train/eval, at least one per-dataset slice, one FLAC URI, and one MP3 URI.

**Fake GCS pattern:** reuse the fake-client shape from `test_dataset_artifacts.py` and the create-only assertion from `test_gcs_uploader.py`. The fake must support both:

```python
storage_client.list_blobs(bucket_name, prefix=prefix, max_results=1)
storage_client.bucket(bucket_name).blob(blob_path).upload_from_string(
    text,
    content_type=content_type,
    if_generation_match=0,
)
```

Keep all publisher tests no-network. Do not import real Google credentials, `google.genai`, NeMo, Hugging Face, audio decoders, or `gcloud`.

**Required publisher tests:**

- `test_publish_dataset_version_uploads_expected_uri_inventory`: asserts returned inventory contains `config/`, `metadata/`, `manifests/canonical/`, `manifests/per_dataset/`, `model_inputs/nemo/`, `model_inputs/whisper/`, `model_inputs/gemini/`, and `reports/` URIs under `gs://wd-transcription-data/sft/dv-001/`.
- `test_publish_dataset_version_checks_prefix_before_upload`: fake `list_blobs()` returns one existing object; `publish_dataset_version_artifacts()` raises `DatasetVersionExistsError` before any upload.
- `test_publish_dataset_version_precondition_failure_fails`: fake upload raises the same precondition failure handled by `upload_text_create_only()`; publisher surfaces the hard error instead of treating it as success.
- `test_publish_dataset_version_uses_create_only_helper_for_all_objects`: fake records each upload and asserts `if_generation_match=0` for every object.

**Assertions to include:** reports and inventory must not contain `raw_row`, credentials, tuned model IDs, tuning job names, `force`, `resume`, or cleanup/delete semantics.

---

### `model/scripts/sft/tests/test_dataset_canonical.py` (test, transform)

**Primary analogs:** `test_dataset_split_normalize.py`, `test_dataset_split_leakage.py`, `test_dataset_split_balance.py`

**Fixture pattern** from `test_dataset_split_leakage.py` (lines 20-45):

```python
def _segment(
    source_group: str,
    *,
    split: str | None = "train",
    original_audio_uri: str = "gs://bucket/audio.mp3",
    model_ready_audio_uri: str | None = None,
    text: str = "alpha bravo",
    offset: float = 0.0,
    duration: float = 10.0,
    row_index: int = 0,
) -> LabeledSegment:
    audio_uri = f"gs://bucket/{source_group}/{row_index}.mp3"
    return LabeledSegment(
        dataset_name="calls",
        dataset_family="bcfy_calls",
        source_strategy="bcfy_calls",
        source_group=source_group,
        audio_uri=audio_uri,
        original_audio_uri=original_audio_uri,
        text=text,
        row_index=row_index,
        offset=offset,
        duration=duration,
        split=split,
        model_ready_audio_uri=model_ready_audio_uri,
    )
```

Reuse this fixture shape and extend only fields needed for canonical row assertions.

**Validation assertion pattern** from `test_dataset_split_leakage.py` (lines 170-174):

```python
def test_missing_split_fails_with_row_index(self) -> None:
    with self.assertRaisesRegex(
        SplitLeakageError, "row_index=0 missing split"
    ):
        validate_split_integrity((_segment("feed-a", split=None),))
```

Canonical tests should assert that missing/invalid split fails before writing, and row errors identify `row_index`.

**Transform assertion pattern** from `test_dataset_split_normalize.py` (lines 87-104):

```python
segment = result.segments[0]
self.assertIsNone(segment.model_ready_audio_uri)
self.assertIsNone(segment.derived_audio_uri)
self.assertIsNone(segment.transformation_metadata)
self.assertEqual(segment.raw_row["groupId"], "123")
```

For Phase 3, invert the final part: assert generated canonical row does not contain `"raw_row"` while preserving source/split/provenance fields.

---

### `model/scripts/sft/tests/test_dataset_reports.py` (test, transform + batch)

**Primary analogs:** `test_dataset_split_balance.py`, `test_pipeline_build.py`

**Report fixture pattern** from `test_dataset_split_balance.py` (lines 20-44):

```python
def _segment(
    source_group: str,
    *,
    split: str = "train",
    dataset_name: str = "calls",
    dataset_family: str = "bcfy_calls",
    text: str = "alpha bravo",
    duration: float = 10.0,
    timestamp: str | None = None,
    row_index: int = 0,
) -> LabeledSegment:
    audio_uri = f"gs://bucket/{dataset_name}/{source_group}/{row_index}.mp3"
    return LabeledSegment(
        dataset_name=dataset_name,
        dataset_family=dataset_family,
        source_strategy=dataset_family,
        source_group=source_group,
        audio_uri=audio_uri,
        original_audio_uri=audio_uri,
        text=text,
        row_index=row_index,
        duration=duration,
        timestamp=timestamp,
        split=split,
    )
```

Use mixed dataset/split fixtures to verify counts, durations, and grouping.

**Report boundary assertions** from `test_dataset_split_balance.py` (lines 92-105):

```python
self.assertIsInstance(report, BalanceReport)
self.assertGreaterEqual(report.weighted_score, 0.0)
names = {component.name for component in report.component_deltas}
self.assertTrue(any(name.startswith("dataset:") for name in names))
self.assertTrue(any(name.startswith("family:") for name in names))
self.assertTrue(any(name.startswith("global:") for name in names))
self.assertTrue(
    any(name.startswith("duration_bucket:") for name in names)
)
self.assertTrue(
    any(name.startswith("transcript_words:") for name in names)
)
self.assertIn("weighted_score", report.to_dict())
self.assertIn("component_deltas", report.to_dict())
```

Phase 3 report tests should assert required fields exist and forbidden SFT run fields do not exist: no tuned model ID, endpoint, training metrics, post-run WER, or run comparison.

**File-write report pattern** from `test_pipeline_build.py` (lines 148-166):

```python
with tempfile.TemporaryDirectory() as tmp:
    train_path = Path(tmp) / "train.jsonl"
    report_path = Path(tmp) / "preflight_report.json"
    train_path.write_text(json.dumps(self._make_bad_example()) + "\n")

    run_preflight(
        train_jsonl_path=train_path,
        val_jsonl_path=None,
        storage_client=None,
        report_path=report_path,
    )

    self.assertTrue(report_path.exists())
    data = json.loads(report_path.read_text())
    self.assertFalse(data["passed"])
```

Use `tempfile.TemporaryDirectory()` only for report renderer tests that need local JSON/Markdown text before upload.

---

### `model/scripts/sft/tests/test_model_writers.py` (test, transform)

**Primary analogs:** `test_pipeline_build.py`, `model/colabs/common/tests/test_sft.py`

**Writer fixture pattern** from `test_pipeline_build.py` (lines 429-440):

```python
rows = [
    SimpleNamespace(
        audio_filepath="gs://b/a1.flac",
        text="engine 41 responding",
        duration=2.0,
    ),
    SimpleNamespace(
        audio_filepath="gs://b/a2.flac",
        text="copy that",
        duration=3.0,
    ),
]
```

For Phase 3, prefer `LabeledSegment` fixtures over `SimpleNamespace` when testing canonical-to-writer functions, but keep tests small and in-memory.

**JSONL/output assertions** from `test_pipeline_build.py` (lines 478-491):

```python
self.assertTrue((staging / "train_echo.jsonl").exists())
self.assertEqual(
    (staging / "train_echo.jsonl").read_text().count("\n"), 2
)
self.assertIn("echo", per_uris)
self.assertEqual(combined_uri, per_uris["echo"])
self.assertTrue(combined_uri.endswith("train_echo.jsonl"))
self.assertEqual(total, 5.0)
self.assertEqual(mock_upload.call_count, 1)
```

Adapt this to assert row counts and generated artifact inventory for NeMo, Whisper, and Gemini writers.

**Gemini shape assertions** from `test_sft.py` (lines 29-45, 47-73):

```python
example = build_example("gs://b/s.flac", "copy", "sys", "user")
self.assertIn("systemInstruction", example)
self.assertIn("contents", example)
for turn in example["contents"]:
    self.assertNotIn("systemInstruction", turn)
```

```python
example = build_example(
    audio_uri="gs://b/s.flac",
    gt_text="copy",
    system_prompt="sys",
    user_prompt="Transcribe this.",
)
text_parts = [p for p in example["contents"][0]["parts"] if "text" in p]
self.assertTrue(
    any(p["text"] == "Transcribe this." for p in text_parts),
    msg=f"Expected user_prompt text not found in user turn parts: {example['contents'][0]['parts']}",
)
```

Add writer tests for:
- NeMo rows include `audio_filepath`, `text`, `duration`, `offset`.
- NeMo config fragment points at train and eval manifests.
- Whisper rows preserve audio URI/path, transcript, duration, source metadata, split, and preprocessing recommendations.
- Whisper >30s examples produce structured warnings, not hard failures.
- Gemini rows use `common.sft.build_example()` shape, truthful MIME type, and config with required training and optional validation dataset URI.

---

### `model/colabs/common/tests/test_sft.py` (test, transform + validation)

**Primary analog:** existing `model/colabs/common/tests/test_sft.py`

**Import isolation pattern** from `test_sft.py` (lines 134-175):

```python
class TestImportIsolation(unittest.TestCase):
    """TEST-03: `import common.prompts` (the light core) must load no heavy deps."""

    def _run_isolated(self, code: str) -> subprocess.CompletedProcess:
        """Run code in a subprocess with model/colabs on PYTHONPATH."""
        env = {**os.environ, "PYTHONPATH": _COLABS_DIR}
        return subprocess.run(
            [sys.executable, "-c", code],
            capture_output=True,
            text=True,
            env=env,
        )

    def test_import_common_sft_loads_no_heavy_deps(self) -> None:
        code = (
            "import common.sft; "
            "import sys; "
            "forbidden = ['nemo_text_processing', 'torch', 'datasets']; "
            "found = [m for m in forbidden if m in sys.modules]; "
            "assert not found, f'Heavy deps leaked into the light core: {found}'"
        )
        result = self._run_isolated(code)
        self.assertEqual(
            result.returncode,
            0,
            msg=f"Import isolation failed (stdout={result.stdout!r} stderr={result.stderr!r})",
        )
```

Any `common.sft` MIME update must keep these import isolation tests green. Do not import storage, Vertex, Torch, datasets, or NeMo dependencies into `common.sft`.

## Shared Patterns

### Test Path Setup

**Source:** `model/scripts/sft/tests/test_dataset_split_gcs_io.py` lines 1-20

**Apply to:** all new `model/scripts/sft/tests/test_dataset_*.py` and `test_model_writers.py`

```python
_SFT_DIR = str(Path(__file__).resolve().parent.parent)
_COLABS_DIR = str(
    Path(__file__).resolve().parent.parent.parent.parent / "colabs"
)
if _SFT_DIR not in sys.path:
    sys.path.insert(0, _SFT_DIR)
if _COLABS_DIR not in sys.path:
    sys.path.insert(0, _COLABS_DIR)
```

### Error Handling

**Source:** `dataset_split/gcs_io.py` lines 7-18 and `dataset_split/config.py` lines 21-23

**Apply to:** artifact layout/upload, canonical serialization, writer validation, report generation

```python
class ConfigValidationError(ValueError):
    """Raised when a dataset-version TOML config is invalid."""
```

```python
def require_gs_uri(uri: str, *, label: str) -> None:
    if not uri.startswith("gs://"):
        raise GcsInputError(f"{label} must be a gs:// URI: {uri}")
```

Use typed `ValueError` subclasses for expected data/config failures; preserve underlying exceptions with `raise ... from exc` when wrapping I/O failures.

### GCS Create-Only Writes

**Source:** `backend/pipeline/common/storage/gcs_uploader.py` lines 41-57

**Apply to:** all dataset-version object uploads under `config/`, `metadata/`, `manifests/`, `model_inputs/`, and `reports/`

```python
blob.upload_from_string(
    data, content_type=content_type, if_generation_match=0
)
```

Pair this with a prefix preflight: `list_blobs(bucket_name, prefix=root_prefix, max_results=1)` must fail the generation if any object exists.

### Split Integrity Gate

**Source:** `model/scripts/sft/dataset_split/leakage.py` lines 17-62

**Apply to:** canonical manifest generation and full artifact bundle generation

```python
def validate_split_integrity(segments: tuple[LabeledSegment, ...]) -> None:
    validate_split_leakage(segments)
    validate_no_duplicate_audio_spans(segments)
```

Call before building rows. Do not recompute splits in Phase 3.

### JSONL Serialization

**Source:** `model/scripts/sft/pipeline.py` lines 185-188

**Apply to:** canonical manifests, per-dataset slices, NeMo manifests, Whisper manifests, Gemini manifests

```python
with open(out_path, "w") as f:
    for ex in examples:
        f.write(json.dumps(ex) + "\n")
```

For new code, return strings or bytes from builders when possible and leave the final upload to `artifacts.py`.

### Writer Warning Results

**Source:** `model/scripts/sft/preflight.py` lines 33-40 and research Pattern 3

**Apply to:** model writer functions, reports

```python
@dataclass
class PreflightReport:
    failures: list[str] = field(default_factory=list)
    offending_ids: list[str] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return len(self.failures) == 0
```

Use a `WriterResult`-style dataclass with rows/config plus structured warnings. Warnings are report data, not just log messages.

### Gemini SFT Shape

**Source:** `model/colabs/common/sft.py` lines 43-63

**Apply to:** Gemini writer output and `common.sft` MIME update

```python
return {
    "systemInstruction": {
        "role": "system",
        "parts": [{"text": system_prompt}],
    },
    "contents": [
        {
            "role": "user",
            "parts": [
                {
                    "fileData": {
                        "mimeType": "audio/flac",
                        "fileUri": audio_uri,
                    }
                },
                {"text": user_prompt},
            ],
        },
        {"role": "model", "parts": [{"text": gt_text}]},
    ],
}
```

Do not emit legacy flat `{prompt, response}` or `{input_text, output_text}` examples.

### Report Serialization

**Source:** `model/scripts/sft/dataset_split/balance.py` lines 64-73 and `model/scripts/sft/preflight.py` lines 281-287

**Apply to:** `reports.py`

```python
def to_dict(self) -> dict[str, object]:
    return {
        "weighted_score": self.weighted_score,
        "component_deltas": [
            component.to_dict() for component in self.component_deltas
        ],
        "duration_buckets": self.duration_buckets,
        "transcript_length_buckets": self.transcript_length_buckets,
        "time_distribution": self.time_distribution,
    }
```

```python
report_path.write_text(json.dumps(report_dict, indent=2))
```

Use deterministic JSON-ready structures and separate Markdown rendering from JSON data.

### Auth And Credentials

**Source:** `model/colabs/common/sft.py` lines 7-8; `model/colabs/common/vertex.py` lines 143-155

**Apply to:** all Phase 3 modules

```python
No GCP project or bucket constants are defined in this module. All GCP identifiers are
caller-supplied parameters.
```

```python
def submit_tuning_job(
    *,
    train_uri: str,
    display_name: str,
    project: str,
    location: str,
    ...
) -> str:
```

Phase 3 live GCS auth is ADC/IAM through the caller-supplied storage client. Do not embed credentials or query docs with sensitive values.

## No Analog Found

All planned new/modified files have usable local analogs. Prefix-level "dataset version exists if any object under prefix exists" has no exact local implementation; implement it by extending the existing GCS parse/create-only patterns and fake-client tests.

| File | Role | Data Flow | Reason |
|------|------|-----------|--------|
| None | - | - | All files mapped to at least one local analog |

## Metadata

**Analog search scope:** `model/scripts/sft/`, `model/colabs/common/`, `backend/pipeline/common/storage/`, `backend/pipeline/common/gcp_helper.py`, phase artifacts under `.planning/phases/03-gcs-artifacts-and-model-writers/`

**Project instructions loaded:** `AGENTS.md`; no local `.codex/skills/` or `.agents/skills/` directories were present in this worktree.

**Files scanned:** phase `CONTEXT.md`, `RESEARCH.md`, `VALIDATION.md`, project `AGENTS.md`, and relevant files under the analog search scope.

**Pattern extraction date:** 2026-05-27

**Planner notes:**
- Keep generated dataset artifacts in GCS or local staging only; do not add generated manifests/audio to Git.
- Do not modify existing benchmark/eval manifests.
- Do not use a force, overwrite, resume, or cleanup path in Phase 3.
- If implementation needs current Google API docs, use the repo-required `ctx7` CLI flow before coding.
