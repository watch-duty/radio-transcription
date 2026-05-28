# Phase 04: Audio Derivation And Provenance - Pattern Map

**Mapped:** 2026-05-27
**Files analyzed:** 14
**Analogs found:** 14 / 14

## Source Note

This pattern map applies to the active
`sft-dataset-versioning/radio-transcription` worktree. It maps Phase 4
planning artifacts to concrete project-relative source and test analogs that
should guide implementation.

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|---|---|---|---|---|
| `model/scripts/sft/dataset_split/audio.py` | utility/service | batch + file-I/O + transform | `backend/pipeline/common/audio.py`, `model/colabs/common/gcs_utils.py`, `model/scripts/sft/dataset_split/split.py` | partial |
| `model/scripts/sft/dataset_split/artifacts.py` | utility/config | file-I/O + request-response to GCS | `model/scripts/sft/dataset_split/artifacts.py` | exact |
| `model/scripts/sft/dataset_split/publisher.py` | service/publisher | batch + file-I/O | `model/scripts/sft/dataset_split/publisher.py` | exact |
| `model/scripts/sft/dataset_split/model_writers.py` | utility | batch + transform | `model/scripts/sft/dataset_split/model_writers.py` | exact |
| `model/scripts/sft/dataset_split/reports.py` | utility | batch + transform | `model/scripts/sft/dataset_split/reports.py` | exact |
| `model/scripts/sft/dataset_split/canonical.py` | utility | batch + transform | `model/scripts/sft/dataset_split/canonical.py` | exact |
| `model/scripts/sft/dataset_split/leakage.py` | utility/validation | batch + transform | `model/scripts/sft/dataset_split/leakage.py` | exact |
| `model/scripts/sft/tests/test_audio_derivation.py` | test | batch + file-I/O + subprocess | `backend/pipeline/common/tests/test_audio.py`, `model/scripts/sft/tests/test_dataset_artifacts.py` | role-match |
| `model/scripts/sft/tests/test_dataset_artifacts.py` | test | file-I/O + request-response to GCS | `model/scripts/sft/tests/test_dataset_artifacts.py` | exact |
| `model/scripts/sft/tests/test_dataset_publisher.py` | test | batch + file-I/O | `model/scripts/sft/tests/test_dataset_publisher.py` | exact |
| `model/scripts/sft/tests/test_model_writers.py` | test | batch + transform | `model/scripts/sft/tests/test_model_writers.py` | exact |
| `model/scripts/sft/tests/test_dataset_reports.py` | test | batch + transform | `model/scripts/sft/tests/test_dataset_reports.py` | exact |
| `model/scripts/sft/tests/test_dataset_canonical.py` | test | batch + transform | `model/scripts/sft/tests/test_dataset_canonical.py` | exact |
| `model/scripts/sft/tests/test_dataset_split_leakage.py` | test | batch + validation | `model/scripts/sft/tests/test_dataset_split_leakage.py` | exact |

## Pattern Assignments

### `model/scripts/sft/dataset_split/audio.py` (utility/service, batch + file-I/O + transform)

**Analogs:** `backend/pipeline/common/audio.py`,
`model/colabs/common/gcs_utils.py`, `model/scripts/sft/dataset_split/split.py`,
`backend/pipeline/common/storage/gcs_uploader.py`

**Imports and immutable row enrichment pattern**
(`model/scripts/sft/dataset_split/split.py` lines 1-15, 145-148):

```python
from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass, replace

from dataset_split.types import LabeledSegment

assigned_segments = tuple(
    replace(segment, split=source_group_splits[segment.source_group])
    for segment in segments
)
```

Copy this shape for audio enrichment: keep `LabeledSegment` frozen and return
new instances with `model_ready_audio_uri`, `derived_audio_uri`, and
`transformation_metadata` populated via `replace()`.

**Row type and error style** (`model/scripts/sft/dataset_split/types.py` lines 6-32):

```python
class RowValidationError(ValueError):
    """Raised when a manifest row is structurally invalid."""


@dataclass(frozen=True)
class LabeledSegment:
    audio_uri: str
    original_audio_uri: str
    offset: float = 0.0
    duration: float = 0.0
    split: str | None = None
    model_ready_audio_uri: str | None = None
    derived_audio_uri: str | None = None
    transformation_metadata: dict[str, object] | None = None
```

Use a local `AudioDerivationError(ValueError)` or reuse a similarly narrow
error class. Include `dataset_name`, `row_index`, `audio_uri`, `offset`, and
`duration` in failure messages.

**FFprobe subprocess pattern** (`backend/pipeline/common/audio.py` lines 25-47):

```python
result = subprocess.run(
    [
        "ffprobe",
        "-v",
        "error",
        "-show_entries",
        "format=duration",
        "-of",
        "default=noprint_wrappers=1:nokey=1",
        f.name,
    ],
    capture_output=True,
    check=True,
)
duration_sec = float(result.stdout.decode().strip())
```

Adapt this to the Phase 4 research JSON form for duration, codec, channels,
and sample rate. Keep `subprocess.run()` as an argv list, never `shell=True`.

**GCS staging/download pattern** (`model/colabs/common/gcs_utils.py` lines 123-153):

```python
def download_to_scratch(
    storage_client: storage.Client, gcs_uri: str, scratch_dir: str
) -> str:
    bucket_name, blob_path = parse_gcs_uri(gcs_uri)
    suffix = os.path.splitext(blob_path)[1] or ".audio"
    fd, local_path = tempfile.mkstemp(dir=scratch_dir, suffix=suffix)
    os.close(fd)
    download_blob_to_file(storage_client, bucket_name, blob_path, local_path)
    return local_path
```

Use this for `gs://` sources. For external sources, copy the timeout and
`raise_for_status()` style from `model/data_sources/broadcastify/bcfy_api.py`
lines 75-89; add streaming writes per research.

```python
response = requests.get(
    ALL_FEEDS_URL,
    headers={"Authorization": f"Bearer {jwt}"},
    params={"genreId": int(genre.value)},
    timeout=BROADCASTIFY_FETCH_TIMEOUT_SECONDS,
)
response.raise_for_status()
```

**Create-only binary upload pattern**
(`backend/pipeline/common/storage/gcs_uploader.py` lines 41-57):

```python
bucket = self.gcs_client.bucket(bucket_name)
blob = bucket.blob(destination_path)
blob.upload_from_string(
    data, content_type=content_type, if_generation_match=0
)
uri = f"gs://{bucket_name}/{destination_path}"
```

For Phase 4, prefer adding a dataset-split-specific helper in `artifacts.py`
that mirrors `upload_text_create_only()` error mapping, rather than importing
the production uploader with numpy dependencies.

**Testing pattern** (`backend/pipeline/common/tests/test_audio.py` lines 41-81):

```python
class TestAudioUtils(unittest.TestCase):
    @patch("subprocess.run")
    def test_get_audio_duration_success(self, mock_run: MagicMock) -> None:
        mock_result = MagicMock()
        mock_result.stdout = b"15.500000\n"
        mock_run.return_value = mock_result

        duration_ms = get_audio_duration(b"dummy audio")

        self.assertEqual(duration_ms, 15500)
        mock_run.assert_called_once()

    @unittest.skipIf(not _ffmpeg_available, "ffmpeg not available")
    def test_get_audio_duration_handles_headerless_lame_mp3(self) -> None:
        audio_bytes = _make_headerless_lame_mp3(duration_ms=7500)
        duration_ms = get_audio_duration(audio_bytes)
        self.assertGreaterEqual(duration_ms, 7200)
        self.assertLessEqual(duration_ms, 7800)
```

For `test_audio_derivation.py`, use mocked subprocess tests for command shape
and optional real `ffmpeg`/`ffprobe` fixture tests guarded by `skipIf`.

---

### `model/scripts/sft/dataset_split/artifacts.py` (utility/config, file-I/O)

**Analog:** `model/scripts/sft/dataset_split/artifacts.py`

**Imports and dependency boundary** (lines 1-12):

```python
from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any

from common.gcs_utils import parse_gcs_uri

try:
    from google.api_core.exceptions import PreconditionFailed
except ImportError:  # pragma: no cover - google-cloud-storage is a model dep.
    PreconditionFailed = None  # type: ignore[assignment]
```

Keep Google imports optional at import time and use `common.gcs_utils` for URI
parsing.

**Layout pattern and reserved audio prefix** (lines 15-38, 52-73):

```python
DATASET_VERSION_ROOT = "gs://wd-transcription-data/sft"
_SAFE_PATH_PART = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


@dataclass(frozen=True)
class DatasetArtifactLayout:
    dataset_version_id: str
    root_uri: str
    config_uri: str
    metadata_uri: str
    canonical_train_uri: str
    canonical_eval_uri: str
    reports_json_uri: str
    reports_markdown_uri: str
    audio_prefix_uri: str

root_uri = f"{normalized_root}/{normalized_id}/"
audio_prefix_uri=_join_uri(root_uri, "audio", trailing=True),
```

Add action-specific audio destination helpers by joining under
`layout.audio_prefix_uri`, e.g. `copied/`, `derived/`, `transcoded/`. Do not put
split names in generated audio paths.

**Create-only text upload and error mapping** (lines 137-157, 208-214):

```python
def upload_text_create_only(
    storage_client: object,
    uri: str,
    text: str,
    *,
    content_type: str,
) -> str:
    bucket_name, blob_path = parse_gcs_uri(uri)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    try:
        blob.upload_from_string(
            text, content_type=content_type, if_generation_match=0
        )
    except Exception as exc:
        if _is_precondition_failure(exc):
            raise DatasetVersionExistsError(
                f"dataset version object already exists at {uri}"
            ) from exc
        raise
    return uri
```

Copy this for `upload_file_create_only()` or `upload_bytes_create_only()` for
audio. Preserve `if_generation_match=0`.

**Validation helpers** (lines 160-199):

```python
def _join_uri(root_uri: str, *parts: str, trailing: bool = False) -> str:
    root = root_uri.rstrip("/")
    path = "/".join(part.strip("/") for part in parts)
    uri = f"{root}/{path}" if path else root
    if trailing:
        return f"{uri}/"
    return uri


def _clean_path_part(value: str, *, label: str) -> str:
    cleaned = value.strip()
    if not cleaned:
        raise DatasetArtifactError(f"{label} must not be empty")
    if cleaned in {".", ".."} or not _SAFE_PATH_PART.fullmatch(cleaned):
        raise DatasetArtifactError(
            f"{label} must contain only letters, numbers, '.', '_', or '-'"
        )
    return cleaned
```

Generated audio object names should use safe generated IDs, not raw source URI
parts.

**Test pattern** (`model/scripts/sft/tests/test_dataset_artifacts.py` lines 35-98, 184-201):

```python
class FakeBlob:
    _missing = object()

    def upload_from_string(
        self,
        text: str,
        *,
        content_type: str,
        if_generation_match: int | object = _missing,
    ) -> None:
        if if_generation_match is self._missing:
            raise AssertionError("if_generation_match=0 is required")
        self.uploads.append(
            {
                "text": text,
                "content_type": content_type,
                "if_generation_match": if_generation_match,
            }
        )

blob = client.fake_bucket.blobs[
    "sft/dv-001/config/resolved_config.json"
]
self.assertEqual(blob.uploads[0]["if_generation_match"], 0)
```

Extend fake blobs with `upload_from_filename()` or bytes capture for audio.

---

### `model/scripts/sft/dataset_split/publisher.py` (service/publisher, batch + file-I/O)

**Analog:** `model/scripts/sft/dataset_split/publisher.py`

**Imports and assembly boundary** (lines 7-28):

```python
from dataset_split.artifacts import (
    DATASET_VERSION_ROOT,
    DatasetArtifactLayout,
    ensure_dataset_version_absent,
    upload_text_create_only,
)
from dataset_split.canonical import canonical_manifests, per_dataset_manifests
from dataset_split.model_writers import (
    build_gemini_inputs,
    build_nemo_inputs,
    build_whisper_inputs,
    summarize_model_writer_result,
)
from dataset_split.reports import (
    build_dataset_version_metadata,
    build_dataset_version_report,
    render_dataset_version_markdown,
)
from dataset_split.types import LabeledSegment
```

Add `dataset_split.audio` imports here, keeping publisher as the orchestration
boundary and `audio.py` as the detailed planning/execution module.

**Current publish ordering to refactor** (lines 89-158):

```python
segment_tuple = tuple(segments)
_reject_sft_run_fields(resolved_config)
layout = DatasetArtifactLayout.for_dataset_version(
    dataset_version_id, root_prefix=root_prefix
)
ensure_dataset_version_absent(storage_client, layout.root_uri)

canonical = canonical_manifests(segment_tuple)
per_dataset = per_dataset_manifests(segment_tuple)
nemo = build_nemo_inputs(
    segment_tuple,
    train_manifest_uri=layout.model_input_uri("nemo", "train"),
    eval_manifest_uri=layout.model_input_uri("nemo", "eval"),
)

for artifact in planned:
    upload_text_create_only(
        storage_client,
        artifact.uri,
        artifact.text,
        content_type=artifact.content_type,
    )
```

Phase 4 must keep exactly one prefix absence check before any upload, then
materialize audio, then build canonical/model/report artifacts from enriched
segments. Do not call `ensure_dataset_version_absent()` again after audio
uploads because the audio objects make the root prefix exist.

**Artifact inventory pattern** (lines 176-220):

```python
return {
    "root": layout.root_uri,
    "model_inputs": {
        "nemo": {
            "train": layout.model_input_uri("nemo", "train"),
            "eval": layout.model_input_uri("nemo", "eval"),
            "config": layout.model_input_uri("nemo", "config", "json"),
        },
        "whisper": {
            "train": layout.model_input_uri("whisper", "train"),
            "eval": layout.model_input_uri("whisper", "eval"),
        },
        "gemini": {
            "train": layout.model_input_uri("gemini", "train"),
            "eval": layout.model_input_uri("gemini", "eval"),
            "tuning_config": layout.model_input_uri(
                "gemini", "tuning_config", "json"
            ),
        },
    },
    "audio_prefix": layout.audio_prefix_uri,
}
```

Extend inventory with concrete audio action prefixes and/or uploaded audio
objects from the audio materialization result.

**Planned artifact pattern** (lines 235-352):

```python
@dataclass(frozen=True)
class _PlannedArtifact:
    name: str
    uri: str
    text: str
    content_type: str


planned.extend(
    [
        _PlannedArtifact(
            "model_inputs/gemini/train",
            layout.model_input_uri("gemini", "train"),
            gemini_jsonl["train"],
            _JSONL,
        ),
        _PlannedArtifact(
            "reports/dataset_version_report",
            layout.reports_json_uri,
            _json_dump(report),
            _JSON,
        ),
    ]
)
```

If audio uploads need result metadata, introduce a separate frozen result type
rather than mixing binary payloads into `_PlannedArtifact`.

**Fail-fast config guard** (lines 359-374):

```python
def _reject_sft_run_fields(
    value: object, *, path: str = "resolved_config"
) -> None:
    if isinstance(value, Mapping):
        for key, nested in value.items():
            child_path = f"{path}.{key}"
            if str(key) in _FORBIDDEN_SFT_RUN_KEYS:
                raise DatasetPublicationError(
                    f"{child_path} belongs to an SFT run, not a dataset version"
                )
            _reject_sft_run_fields(nested, path=child_path)
```

Copy this fail-fast style for rejecting missing/non-`gs://`
`model_ready_audio_uri` after audio enrichment.

**Test pattern** (`model/scripts/sft/tests/test_dataset_publisher.py` lines 227-279, 309-329):

```python
result = _publish(client)

self.assertEqual(
    client.list_calls,
    [
        {
            "bucket_name": "wd-transcription-data",
            "prefix": "sft/dv-001/",
            "max_results": 1,
        }
    ],
)
self.assertEqual(
    {upload["if_generation_match"] for upload in client.uploads},
    {0},
)

with self.assertRaises(DatasetVersionExistsError):
    _publish(client)

self.assertEqual(client.uploads, [])
self.assertEqual(len(client.list_calls), 1)
```

Update this test to assert one root guard before both audio and text artifacts,
and that model writers receive enriched segments.

---

### `model/scripts/sft/dataset_split/model_writers.py` (utility, batch + transform)

**Analog:** `model/scripts/sft/dataset_split/model_writers.py`

**Imports and validation boundary** (lines 8-12):

```python
from common.sft import build_example, validate_example

from dataset_split.leakage import validate_split_integrity
from dataset_split.types import LabeledSegment
```

Keep writers pure: they validate already-enriched rows and emit model-specific
JSON rows, without deriving or uploading audio.

**Writer result/data shape** (lines 31-69):

```python
@dataclass(frozen=True)
class WriterWarning:
    writer: str
    code: str
    severity: str
    row_index: int
    message: str
    details: dict[str, object]


@dataclass(frozen=True)
class ModelWriterResult:
    rows_by_split: dict[str, tuple[dict[str, object], ...]]
    config: dict[str, object] | None
    warnings: tuple[WriterWarning, ...]
    summary_by_split: dict[str, dict[str, float | int]]
```

Add no new writer side effects. Keep performance issues as `WriterWarning`
unless a target hard-rejects them.

**Core writer pattern** (lines 89-155, 204-248):

```python
segment_tuple = tuple(segments)
validate_split_integrity(segment_tuple)
rows_by_split = _empty_rows_by_split()
for segment in segment_tuple:
    rows_by_split[_require_split(segment)].append(_nemo_row(segment))

for segment in segment_tuple:
    row = build_example(
        audio_uri=segment.audio_uri,
        gt_text=segment.text,
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        mime_type=_infer_audio_mime_type_for_segment(segment),
    )
    if not validate_example(row):
        raise ModelWriterError(
            f"row_index={segment.row_index} failed Gemini validation"
        )
```

Change all writer rows to use `_require_model_ready_audio_uri(segment)`.
Gemini MIME inference should use that model-ready URI, not `audio_uri`.

**Row helper and validation helper pattern** (lines 251-275, 310-320, 363-369):

```python
def _nemo_row(segment: LabeledSegment) -> dict[str, object]:
    return {
        "audio_filepath": segment.audio_uri,
        "text": segment.text,
        "duration": segment.duration,
        "offset": segment.offset,
        "example_id": segment.example_id,
        "segment_id": segment.segment_id,
    }


def _require_split(segment: LabeledSegment) -> str:
    if segment.split not in _SPLITS:
        raise ModelWriterError(f"row_index={segment.row_index} missing split")
    return segment.split


def _infer_audio_mime_type_for_segment(segment: LabeledSegment) -> str:
    try:
        return infer_audio_mime_type(segment.audio_uri)
    except ModelWriterError as exc:
        raise ModelWriterError(
            f"row_index={segment.row_index} unsupported audio_uri={segment.audio_uri}"
        ) from exc
```

Suggested helper shape to copy:

```python
def _require_model_ready_audio_uri(segment: LabeledSegment) -> str:
    uri = (segment.model_ready_audio_uri or "").strip()
    if not uri.startswith("gs://"):
        raise ModelWriterError(
            f"row_index={segment.row_index} missing model_ready_audio_uri"
        )
    return uri
```

**Test pattern** (`model/scripts/sft/tests/test_model_writers.py` lines 63-93, 118-156, 177-245, 280-298):

```python
result = build_nemo_inputs((segment,), train_manifest_uri=train_uri, eval_manifest_uri=eval_uri)
row = result.rows_by_split["train"][0]
self.assertEqual(row["audio_filepath"], segment.audio_uri)

result = build_whisper_inputs((segment,))
row = result.rows_by_split["train"][0]
self.assertEqual(row["audio_uri"], segment.audio_uri)

with self.assertRaisesRegex(ModelWriterError, "row_index=9"):
    build_gemini_inputs((segment,), system_prompt="sys", user_prompt="user", training_dataset_uri="gs://...")
```

Update `_segment()` to accept `model_ready_audio_uri`; assert NeMo, Whisper,
and Gemini use that URI and fail when it is missing or not `gs://`.

---

### `model/scripts/sft/dataset_split/reports.py` (utility, batch + transform)

**Analog:** `model/scripts/sft/dataset_split/reports.py`

**Imports and dataclass report pattern** (lines 1-9, 19-60):

```python
from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone

from dataset_split.leakage import validate_split_integrity
from dataset_split.types import LabeledSegment


@dataclass(frozen=True)
class DatasetVersionReport:
    dataset_version_id: str
    resolved_config: Mapping[str, object]
    split_counts: Mapping[str, int]
    duration_seconds: Mapping[str, float]
    dataset_summary: Mapping[str, object]
    model_writer_summary: Mapping[str, object]
    leakage_validation: Mapping[str, object]
    balance_report: Mapping[str, object]
    artifact_inventory: Mapping[str, object]
    writer_warnings: Mapping[str, Sequence[Mapping[str, object]]]
```

Add `audio_transformation_summary` or similar as a report field and include it
in `to_dict()`.

**Report build pattern** (lines 79-106):

```python
segment_tuple = tuple(segments)
validate_split_integrity(segment_tuple)
return DatasetVersionReport(
    dataset_version_id=_require_text(
        dataset_version_id, label="dataset_version_id"
    ),
    resolved_config=_json_ready(resolved_config),
    split_counts=_split_counts(segment_tuple),
    duration_seconds=_split_durations(segment_tuple),
    dataset_summary=_dataset_summary(segment_tuple),
    model_writer_summary=_validate_model_writer_summary(
        model_writer_summary
    ),
    leakage_validation=_json_ready(leakage_validation),
    balance_report=_json_ready(balance_report),
    artifact_inventory=_json_ready(artifact_inventory),
    writer_warnings=_writer_warnings(writer_warnings),
)
```

Compute audio counts from enriched rows inside this function so JSON and
markdown reports share one source of truth.

**Summary helper pattern** (lines 151-195):

```python
def _dataset_summary(
    segments: tuple[LabeledSegment, ...],
) -> dict[str, dict[str, object]]:
    summary: dict[str, dict[str, object]] = {}
    for dataset_name in sorted({segment.dataset_name for segment in segments}):
        dataset_segments = tuple(
            segment
            for segment in segments
            if segment.dataset_name == dataset_name
        )
        splits = {
            split: {"count": 0, "duration_seconds": 0.0}
            for split in MODEL_WRITER_SUMMARY_SPLITS
        }
```

Copy this style for action counts. Expected action keys from context:
`reused`, `copied`, `derived`, `transcoded`.

**JSON-safe scrub pattern** (lines 273-286):

```python
def _json_ready(value: object) -> object:
    to_dict = getattr(value, "to_dict", None)
    if callable(to_dict):
        return _json_ready(to_dict())
    blocked_keys = _sft_run_keys()
    if isinstance(value, Mapping):
        return {
            str(key): _json_ready(nested)
            for key, nested in value.items()
            if str(key) not in blocked_keys
        }
    if isinstance(value, tuple | list):
        return [_json_ready(nested) for nested in value]
    return value
```

Use this when embedding transformation metadata in reports.

**Test pattern** (`model/scripts/sft/tests/test_dataset_reports.py` lines 126-176):

```python
report = _report_dict()

for key in (
    "dataset_version_id",
    "resolved_config",
    "split_counts",
    "duration_seconds",
    "dataset_summary",
    "leakage_validation",
    "balance_report",
    "artifact_inventory",
    "writer_warnings",
):
    self.assertIn(key, report)
self.assertEqual(report["split_counts"], {"train": 2, "eval": 1})
```

Add assertions for `audio_transformation_summary` counts, per-action duration,
and markdown inclusion.

---

### `model/scripts/sft/dataset_split/canonical.py` (utility, batch + transform)

**Analog:** `model/scripts/sft/dataset_split/canonical.py`

**Canonical provenance preservation pattern** (lines 16-38):

```python
def canonical_row(segment: LabeledSegment) -> dict[str, object]:
    split = _require_split(
        segment.split, label=f"row_index={segment.row_index}"
    )
    return {
        "dataset_name": segment.dataset_name,
        "dataset_family": segment.dataset_family,
        "source_strategy": segment.source_strategy,
        "source_group": segment.source_group,
        "split": split,
        "audio_uri": segment.audio_uri,
        "original_audio_uri": segment.original_audio_uri,
        "text": segment.text,
        "offset": segment.offset,
        "duration": segment.duration,
        "model_ready_audio_uri": segment.model_ready_audio_uri,
        "derived_audio_uri": segment.derived_audio_uri,
        "transformation_metadata": segment.transformation_metadata,
    }
```

This already matches Phase 4 provenance preservation. If adding a post-Phase-4
hard boundary, keep raw rows out and preserve both source and model-ready URIs.

**Split and JSONL pattern** (lines 41-64, 90-98):

```python
def canonical_manifests(segments: tuple[LabeledSegment, ...]) -> dict[str, str]:
    validate_split_integrity(segments)
    return {
        split: serialize_jsonl(
            tuple(
                canonical_row(segment)
                for segment in segments
                if segment.split == split
            )
        )
        for split in _SPLITS
    }


def serialize_jsonl(rows: tuple[dict[str, object], ...]) -> str:
    if not rows:
        return ""
    return (
        "\n".join(
            json.dumps(row, sort_keys=True, allow_nan=False) for row in rows
        )
        + "\n"
    )
```

**Test pattern** (`model/scripts/sft/tests/test_dataset_canonical.py` lines 74-127):

```python
segment = _segment(
    "feed-a",
    raw_row={"secret": "x"},
    model_ready_audio_uri="gs://ready/feed-a.flac",
    derived_audio_uri="gs://derived/feed-a.flac",
    transformation_metadata={"reused": False},
)

row = canonical_row(segment)

self.assertEqual(row["transformation_metadata"], {"reused": False})
self.assertNotIn("raw_row", row)
self.assertNotIn("secret", json.dumps(row))
```

Update this to assert the Phase 4 action vocabulary and minimum metadata keys
if canonical validation is broadened.

---

### `model/scripts/sft/dataset_split/leakage.py` (utility/validation, batch)

**Analog:** `model/scripts/sft/dataset_split/leakage.py`

**Existing model-ready leakage gate** (lines 5-36):

```python
_SPLITS = {"train", "eval"}
_FIELD_LEAK_MESSAGES = {
    "source_group": "source_group appears in both splits",
    "original_audio_uri": "original_audio_uri appears in both splits",
    "model_ready_audio_uri": "model_ready_audio_uri appears in both splits",
}

def validate_split_leakage(segments: tuple[LabeledSegment, ...]) -> None:
    _validate_cross_split_overlap(
        segments,
        field_name="model_ready_audio_uri",
        value_for_segment=lambda segment: _normalized_uri(
            segment.model_ready_audio_uri
        ),
    )
```

Phase 4 should run this after audio enrichment. Empty model-ready fields are
ignored by this gate, so add a separate hard validator before publication.

**Duplicate span and split validation style** (lines 39-62, 90-93):

```python
def validate_no_duplicate_audio_spans(
    segments: tuple[LabeledSegment, ...]
) -> None:
    seen_by_split: dict[str, set[tuple[str | None, float, float]]] = {
        "train": set(),
        "eval": set(),
    }
    for segment in segments:
        split = _require_split(segment)
        uri = _normalized_uri(segment.original_audio_uri)
        key = (uri, segment.offset, segment.duration)
        if key in seen_by_split[split]:
            raise SplitLeakageError(
                f"duplicate audio span in {split}: "
                f"original_audio_uri={uri} "
                f"offset={segment.offset} duration={segment.duration}"
            )


def _require_split(segment: LabeledSegment) -> str:
    if segment.split not in _SPLITS:
        raise SplitLeakageError(f"row_index={segment.row_index} missing split")
    return segment.split
```

Use this style if adding `validate_model_ready_audio(segments)`: iterate rows,
raise with row context, require non-empty `gs://` URI.

**Test pattern** (`model/scripts/sft/tests/test_dataset_split_leakage.py` lines 76-117):

```python
with self.assertRaisesRegex(
    SplitLeakageError, "model_ready_audio_uri appears in both splits"
):
    validate_split_leakage(segments)

def test_empty_model_ready_audio_uri_is_ignored(self) -> None:
    segments = (
        _segment(..., model_ready_audio_uri=" "),
        _segment(..., model_ready_audio_uri=None),
    )

    validate_split_leakage(segments)
```

Add new tests for the hard post-Phase-4 requirement while preserving this
pre-Phase-4 leakage behavior if older phases still call it before enrichment.

---

## Shared Patterns

### Test Import Path Setup

**Source:** `model/scripts/sft/tests/test_model_writers.py` lines 8-16
**Apply to:** all new/modified `model/scripts/sft/tests/*` modules

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

Use this in `test_audio_derivation.py` if it imports both `dataset_split.*` and
`common.gcs_utils`.

### GCS Create-Only Semantics

**Source:** `model/scripts/sft/dataset_split/artifacts.py` lines 137-157 and
`backend/pipeline/common/storage/gcs_uploader.py` lines 41-57
**Apply to:** `artifacts.py`, `audio.py`, `publisher.py`, publisher/artifact tests

```python
blob.upload_from_string(
    text, content_type=content_type, if_generation_match=0
)
```

For local file uploads, use the same `if_generation_match=0` precondition and
map 412/precondition failures to `DatasetVersionExistsError`.

### Split Integrity After Enrichment

**Source:** `model/scripts/sft/dataset_split/model_writers.py` lines 95-99,
120-126, 217-219 and `leakage.py` lines 17-36
**Apply to:** `publisher.py`, `model_writers.py`, `reports.py`, `canonical.py`

```python
segment_tuple = tuple(segments)
validate_split_integrity(segment_tuple)
rows_by_split = _empty_rows_by_split()
for segment in segment_tuple:
    rows_by_split[_require_split(segment)].append(_nemo_row(segment))
```

Call integrity validation on enriched segments so `model_ready_audio_uri`
overlap is checked after Phase 4 populates it.

### JSONL Serialization

**Source:** `model/scripts/sft/dataset_split/canonical.py` lines 90-98 and
`model_writers.py` lines 372-380
**Apply to:** canonical/model writer artifacts and tests

```python
return (
    "\n".join(
        json.dumps(row, sort_keys=True, allow_nan=False) for row in rows
    )
    + "\n"
)
```

Preserve `allow_nan=False` to fail on invalid numeric metadata.

### Report JSON Safety And SFT Run Scrubbing

**Source:** `model/scripts/sft/dataset_split/reports.py` lines 273-296 and
`publisher.py` lines 359-374
**Apply to:** report additions and publisher resolved config validation

```python
if isinstance(value, Mapping):
    return {
        str(key): _json_ready(nested)
        for key, nested in value.items()
        if str(key) not in blocked_keys
    }
```

Use this for transformation metadata in reports; do not include generated job
run fields in dataset-version artifacts.

### FFmpeg/FFprobe Test Strategy

**Source:** `backend/pipeline/common/tests/test_audio.py` lines 1-8, 41-81
**Apply to:** `test_audio_derivation.py`

```python
_ffmpeg_available = shutil.which("ffmpeg") is not None

@patch("subprocess.run")
def test_get_audio_duration_success(self, mock_run: MagicMock) -> None:
    ...

@unittest.skipIf(not _ffmpeg_available, "ffmpeg not available")
def test_get_audio_duration_handles_headerless_lame_mp3(self) -> None:
    ...
```

Mock subprocesses for deterministic command assertions; add optional end-to-end
fixture tests only when binaries exist.

## No Analog Found

| File / Sub-capability | Role | Data Flow | Reason |
|---|---|---|---|
| External audio URL streaming helper inside `audio.py` | utility | file-I/O | Local code uses `requests.get(..., timeout=...)` and `raise_for_status()`, but no existing local helper streams arbitrary source audio to a scratch file. Use the research pattern with `stream=True` and chunked writes. |
| Full Phase 4 action planner (`reused`/`copied`/`derived`/`transcoded`) | service/utility | batch + transform | No existing dataset_split module performs audio action planning. Compose from `split.py` frozen row replacement, `artifacts.py` path validation, `backend/pipeline/common/audio.py` subprocess probing, and `model_writers.py` MIME support. |

## Metadata

**Analog search scope:** active `sft-dataset-versioning/radio-transcription`
worktree.

**Files scanned:** 40+ source/test files via `rg --files`, `find`, `rg`, `wc -l`,
and one-pass numbered reads for selected analogs.

**Strong analogs read:** `types.py`, `artifacts.py`, `publisher.py`,
`model_writers.py`, `reports.py`, `canonical.py`, `leakage.py`, `gcs_io.py`,
`normalize.py`, `split.py`, `backend/pipeline/common/audio.py`,
`model/colabs/common/gcs_utils.py`,
`backend/pipeline/common/storage/gcs_uploader.py`, and matching test modules.

**Pattern extraction date:** 2026-05-27
