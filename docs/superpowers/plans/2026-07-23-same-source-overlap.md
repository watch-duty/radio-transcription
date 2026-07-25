# Same-Source Overlap Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Allow partially overlapping same-source contextual segments while
rejecting equal, padded, and cropped duplicate spans before preparation or
provider execution.

**Architecture:** Keep `build_strict_causal_schedule()` as the single
transcript-free planning interface. Deepen its implementation with deterministic
containment validation and an `O(N log N + N * K)` per-source sweep, then make
training bind references to the same frozen dependency schedule that evaluation
uses. Compile all positive-context train, validation, and eval schedules before
writing model inputs or publishing durable state; the stacked execution PR
inherits the planner and adds only a defensive runtime regression test.

**Tech Stack:** Python 3.11+, dataclasses, standard-library interval/Fenwick
data structures, `unittest`, `pytest`, Ruff, `uv`, `safe-run`.

## Global Constraints

- Equal or contained intervals are invalid only within the same
  `(split, source_key)` contextual population.
- Partial overlap, contiguity, disjoint spans, other sources, and other splits
  remain valid.
- Containment uses the existing
  `_CAUSAL_BOUNDARY_TOLERANCE_SECONDS = 1e-7` symmetrically.
- `max_turns == 0` remains stateless: it does not require causal provenance and
  does not perform containment validation.
- Training and evaluation select the same structural K dependencies; unusable
  text is omitted after K selection without refill.
- Do not add `transmission_id`, `source_transmission_id`, `overlap_policy`, audio
  similarity, transcript similarity, or silent deduplication.
- Do not change anything under `model/colabs`.
- Tasks 1-5 belong to PR #1039
  (`refactor/gemini-eval-causal-contract`). Task 6 belongs to stacked PR #1003
  (`fix/gemini-predicted-history-eval`) after it is restacked onto #1039.
- Run only the targeted low-resource checks listed below; do not run local E2E,
  Docker, testcontainers, API, component, or unscoped test suites.

---

## File Structure

### PR #1039

- Modify `model/src/common/gemini/context.py`
  - Validate duplicate source spans.
  - Build dependencies with a bounded per-source sweep.
  - Resolve training references from a compiled schedule.
- Modify `model/src/gemini_sft/artifacts.py`
  - Normalize train, validation, and eval rows into the existing
    `EvaluationSegment` type through one adapter.
- Modify `model/src/gemini_sft/prepare.py`
  - Compile all positive-context schedules before writing Gemini JSONL or
    uploading durable artifacts.
  - Make `write_gemini_jsonl()` rendering-only.
- Modify `model/tests/common/tests/test_gemini_context.py`
  - Cover the span relationship matrix, deterministic diagnostics, schedule
    complexity behavior, and schedule-driven training binding.
- Modify `model/tests/common/tests/test_gemini_eval_artifacts.py`
  - Cover the shared provenance adapter.
- Modify `model/tests/gemini_sft/test_workflow.py`
  - Cover fail-fast training and eval preparation plus schedule-driven
    training rendering.
- Modify `model/scripts/sft/docs/configs.md`
  - Document containment rejection and allowed partial overlap.

### PR #1003 after restacking

- Modify `model/tests/gemini_sft/test_target_execution.py`
  - Prove containment failure precedes online wave execution and artifact
    publication.

---

### Task 1: Deepen the causal planner

**Files:**

- Modify: `model/src/common/gemini/context.py:5-9`
- Modify: `model/src/common/gemini/context.py:159-163`
- Modify: `model/src/common/gemini/context.py:463-535`
- Modify: `model/src/common/gemini/context.py:615-677`
- Test: `model/tests/common/tests/test_gemini_context.py:210-342`

**Interfaces:**

- Consumes:
  `build_strict_causal_schedule(segments, *, max_turns)`.
- Produces: the same public function and return type, with containment
  validation for positive K and `O(N log N + N * K)` planning.

- [ ] **Step 1: Write failing public-interface tests for span classification**

Add these tests to `TestStrictCausalSchedule`:

```python
def test_rejects_identical_same_source_intervals(self) -> None:
    segments = [
        self._segment("gs://a/one", start=10, end=20, index=0),
        self._segment("gs://a/two", start=10, end=20, index=1),
    ]

    with self.assertRaisesRegex(
        ValueError,
        "relationship=equality.*manifest_indices=\\(0, 1\\)",
    ):
        context.build_strict_causal_schedule(segments, max_turns=2)


def test_rejects_strict_containment_in_either_input_order(self) -> None:
    outer = self._segment("gs://a/outer", start=10, end=20, index=0)
    inner = self._segment("gs://a/inner", start=12, end=18, index=1)

    for segments in ([outer, inner], [inner, outer]):
        with (
            self.subTest(order=[row.audio_uri for row in segments]),
            self.assertRaisesRegex(
                ValueError,
                "relationship=containment.*total_invalid_pairs=1",
            ),
        ):
            context.build_strict_causal_schedule(segments, max_turns=2)


def test_rejects_shared_start_or_end_containment(self) -> None:
    cases = (
        (
            self._segment("gs://a/outer", start=10, end=20, index=0),
            self._segment("gs://a/inner", start=10, end=18, index=1),
        ),
        (
            self._segment("gs://a/outer", start=10, end=20, index=0),
            self._segment("gs://a/inner", start=12, end=20, index=1),
        ),
    )

    for outer, inner in cases:
        with (
            self.subTest(inner=(inner.start_seconds, inner.end_seconds)),
            self.assertRaisesRegex(ValueError, "relationship=containment"),
        ):
            context.build_strict_causal_schedule(
                [outer, inner],
                max_turns=2,
            )


def test_applies_containment_tolerance(self) -> None:
    tolerance = context._CAUSAL_BOUNDARY_TOLERANCE_SECONDS
    near_container = self._segment(
        "gs://a/near-container",
        start=10 + tolerance / 2,
        end=20,
        index=0,
    )
    contained = self._segment(
        "gs://a/contained",
        start=10,
        end=18,
        index=1,
    )

    with self.assertRaisesRegex(ValueError, "relationship=containment"):
        context.build_strict_causal_schedule(
            [near_container, contained],
            max_turns=2,
        )


def test_partial_overlaps_are_independent_then_both_feed_later(
    self,
) -> None:
    segments = [
        self._segment("gs://a/first", start=10, end=20, index=0),
        self._segment("gs://a/second", start=15, end=25, index=1),
        self._segment("gs://a/later", start=25, end=30, index=2),
    ]

    for values in (segments, list(reversed(segments))):
        schedule = context.build_strict_causal_schedule(
            values,
            max_turns=2,
        )

        self.assertEqual(schedule[0].dependency_audio_uris, ())
        self.assertEqual(schedule[1].dependency_audio_uris, ())
        self.assertEqual(
            schedule[2].dependency_audio_uris,
            ("gs://a/first", "gs://a/second"),
        )


def test_allows_equal_spans_in_other_contextual_populations(self) -> None:
    segments = [
        self._segment("gs://a/eval", start=10, end=20, index=0),
        self._segment(
            "gs://a/other-source",
            start=10,
            end=20,
            index=1,
            source="source-b",
        ),
        self._segment(
            "gs://a/other-split",
            start=10,
            end=20,
            index=2,
            split="train",
        ),
    ]

    schedule = context.build_strict_causal_schedule(segments, max_turns=2)

    self.assertEqual(
        [row.dependency_audio_uris for row in schedule],
        [(), (), ()],
    )


def test_zero_history_skips_contextual_duplicate_validation(self) -> None:
    segments = [
        self._segment("gs://a/outer", start=10, end=20, index=0),
        self._segment("gs://a/inner", start=12, end=18, index=1),
    ]

    schedule = context.build_strict_causal_schedule(segments, max_turns=0)

    self.assertEqual(
        [row.dependency_audio_uris for row in schedule],
        [(), ()],
    )
```

Replace the invalid same-end fixture in
`test_candidate_order_is_end_start_uri_then_last_k` with valid intervals:

```python
def test_candidate_order_retains_last_k_completed_segments(self) -> None:
    segments = [
        self._segment("gs://a/first", start=0, end=1, index=0),
        self._segment("gs://a/second", start=1, end=2, index=1),
        self._segment("gs://a/third", start=2, end=3, index=2),
        self._segment("gs://a/current", start=4, end=5, index=3),
    ]

    schedule = context.build_strict_causal_schedule(
        list(reversed(segments)),
        max_turns=2,
    )

    self.assertEqual(
        schedule[3].dependency_audio_uris,
        ("gs://a/second", "gs://a/third"),
    )
```

- [ ] **Step 2: Run the planner tests and verify the new cases fail**

Run from the repository root:

```bash
safe-run -- uv run --project model --extra dev pytest \
  model/tests/common/tests/test_gemini_context.py::TestStrictCausalSchedule \
  -q -n 0
```

Expected: the containment cases do not raise because the planner has not yet
implemented duplicate-span validation.

- [ ] **Step 3: Add deterministic duplicate-span data structures**

Add `bisect`, `heapq`, and `itertools` imports, then add these internal types
near the causal tolerance constant:

```python
_DUPLICATE_SPAN_SAMPLE_LIMIT: typing.Final = 5


@dataclasses.dataclass(frozen=True, slots=True)
class _DuplicateSpanPair:
    first: EvaluationSegment
    second: EvaluationSegment
    relationship: typing.Literal["equality", "containment"]


class _FenwickCounter:
    """Count compressed end coordinates with logarithmic updates and queries."""

    def __init__(self, size: int) -> None:
        self._tree = [0] * (size + 1)

    def add(self, index: int, delta: int) -> None:
        tree_index = index + 1
        while tree_index < len(self._tree):
            self._tree[tree_index] += delta
            tree_index += tree_index & -tree_index

    def prefix_count(self, stop: int) -> int:
        total = 0
        tree_index = stop
        while tree_index:
            total += self._tree[tree_index]
            tree_index -= tree_index & -tree_index
        return total
```

- [ ] **Step 4: Implement the exact containment scan and diagnostics**

Add these internal helpers:

```python
def _span_contains(
    container: EvaluationSegment,
    contained: EvaluationSegment,
) -> bool:
    tolerance = _CAUSAL_BOUNDARY_TOLERANCE_SECONDS
    return (
        container.start_seconds <= contained.start_seconds + tolerance
        and container.end_seconds >= contained.end_seconds - tolerance
    )


def _duplicate_span_pair(
    first: EvaluationSegment,
    second: EvaluationSegment,
) -> _DuplicateSpanPair:
    ordered = sorted(
        (first, second),
        key=lambda value: (value.manifest_index, value.audio_uri),
    )
    relationship: typing.Literal["equality", "containment"] = (
        "equality"
        if _span_contains(first, second)
        and _span_contains(second, first)
        else "containment"
    )
    return _DuplicateSpanPair(
        first=ordered[0],
        second=ordered[1],
        relationship=relationship,
    )


def _duplicate_pair_key(
    pair: _DuplicateSpanPair,
) -> tuple[int, str, int, str]:
    return (
        pair.first.manifest_index,
        pair.first.audio_uri,
        pair.second.manifest_index,
        pair.second.audio_uri,
    )


def _scan_duplicate_source_spans(
    source_segments: collections.abc.Sequence[EvaluationSegment],
    *,
    sample_limit: int,
) -> tuple[int, tuple[_DuplicateSpanPair, ...]]:
    ordered = sorted(
        source_segments,
        key=lambda value: (
            value.start_seconds,
            value.end_seconds,
            value.manifest_index,
            value.audio_uri,
        ),
    )
    end_values = sorted({value.end_seconds for value in ordered})
    end_indices = {value: index for index, value in enumerate(end_values)}
    all_prior_ends = _FenwickCounter(len(end_values))
    near_start_ends = _FenwickCounter(len(end_values))
    near_start_segments: collections.deque[EvaluationSegment] = (
        collections.deque()
    )
    near_active: set[tuple[int, str]] = set()
    max_end_heap: list[
        tuple[float, int, str, EvaluationSegment]
    ] = []
    near_min_end_heap: list[
        tuple[float, int, str, EvaluationSegment]
    ] = []
    samples: list[_DuplicateSpanPair] = []
    sample_keys: set[tuple[int, str, int, str]] = set()
    invalid_pair_count = 0
    tolerance = _CAUSAL_BOUNDARY_TOLERANCE_SECONDS

    def add_sample(
        first: EvaluationSegment,
        second: EvaluationSegment,
    ) -> None:
        if len(samples) >= sample_limit:
            return
        pair = _duplicate_span_pair(first, second)
        key = _duplicate_pair_key(pair)
        if key not in sample_keys:
            sample_keys.add(key)
            samples.append(pair)

    for prior_count, current in enumerate(ordered):
        while (
            near_start_segments
            and near_start_segments[0].start_seconds + tolerance
            < current.start_seconds
        ):
            expired = near_start_segments.popleft()
            near_start_ends.add(end_indices[expired.end_seconds], -1)
            near_active.remove(
                (expired.manifest_index, expired.audio_uri)
            )
        while near_min_end_heap:
            candidate = near_min_end_heap[0][3]
            candidate_key = (
                candidate.manifest_index,
                candidate.audio_uri,
            )
            if candidate_key in near_active:
                break
            heapq.heappop(near_min_end_heap)

        lower_end = current.end_seconds - tolerance
        upper_end = current.end_seconds + tolerance
        lower_index = bisect.bisect_left(end_values, lower_end)
        upper_index = bisect.bisect_right(end_values, upper_end)
        prior_contains_current = (
            prior_count - all_prior_ends.prefix_count(lower_index)
        )
        current_contains_prior = near_start_ends.prefix_count(
            upper_index
        )
        equality_count = (
            near_start_ends.prefix_count(upper_index)
            - near_start_ends.prefix_count(lower_index)
        )
        invalid_pair_count += (
            prior_contains_current
            + current_contains_prior
            - equality_count
        )

        if prior_contains_current:
            add_sample(max_end_heap[0][3], current)
        if current_contains_prior:
            add_sample(near_min_end_heap[0][3], current)

        end_index = end_indices[current.end_seconds]
        all_prior_ends.add(end_index, 1)
        near_start_ends.add(end_index, 1)
        near_start_segments.append(current)
        near_active.add((current.manifest_index, current.audio_uri))
        heapq.heappush(
            max_end_heap,
            (
                -current.end_seconds,
                current.manifest_index,
                current.audio_uri,
                current,
            ),
        )
        heapq.heappush(
            near_min_end_heap,
            (
                current.end_seconds,
                current.manifest_index,
                current.audio_uri,
                current,
            ),
        )

    return invalid_pair_count, tuple(samples)


def _format_duplicate_span_pair(pair: _DuplicateSpanPair) -> str:
    first = pair.first
    second = pair.second
    return (
        f"relationship={pair.relationship}, split={first.split!r}, "
        f"source_key={first.source_key!r}, "
        "manifest_indices="
        f"({first.manifest_index}, {second.manifest_index}), "
        f"audio_uris=({first.audio_uri!r}, {second.audio_uri!r}), "
        "intervals=("
        f"[{first.start_seconds}, {first.end_seconds}), "
        f"[{second.start_seconds}, {second.end_seconds}))"
    )


def _validate_no_duplicate_source_spans(
    grouped: collections.abc.Mapping[
        tuple[str, str],
        collections.abc.Sequence[EvaluationSegment],
    ],
) -> None:
    total_invalid_pairs = 0
    samples: list[_DuplicateSpanPair] = []
    for group_key in sorted(grouped):
        invalid_count, group_samples = _scan_duplicate_source_spans(
            grouped[group_key],
            sample_limit=_DUPLICATE_SPAN_SAMPLE_LIMIT - len(samples),
        )
        total_invalid_pairs += invalid_count
        samples.extend(group_samples)
    if total_invalid_pairs:
        sample_text = "; ".join(
            _format_duplicate_span_pair(pair) for pair in samples
        )
        msg = (
            "same-source duplicate spans detected: "
            f"total_invalid_pairs={total_invalid_pairs}; "
            f"sample=[{sample_text}]"
        )
        raise ValueError(msg)
```

- [ ] **Step 5: Replace quadratic dependency scans with a bounded sweep**

Add this helper:

```python
def _build_source_dependencies(
    source_segments: collections.abc.Sequence[EvaluationSegment],
    *,
    max_turns: int,
) -> tuple[
    dict[str, tuple[str, ...]],
    dict[str, int],
]:
    dependency_map: dict[str, tuple[str, ...]] = {}
    wave_by_audio_uri: dict[str, int] = {}
    ordered = sorted(source_segments, key=_execution_sort_key)
    if max_turns == 0:
        for segment in ordered:
            dependency_map[segment.audio_uri] = ()
            wave_by_audio_uri[segment.audio_uri] = 0
        return dependency_map, wave_by_audio_uri

    pending: list[
        tuple[float, float, str, EvaluationSegment]
    ] = []
    recent_completed: list[
        tuple[float, float, str, EvaluationSegment]
    ] = []
    activation_index = 0
    tolerance = _CAUSAL_BOUNDARY_TOLERANCE_SECONDS
    for current_start, batch_values in itertools.groupby(
        ordered,
        key=lambda value: value.start_seconds,
    ):
        batch = list(batch_values)
        while activation_index < len(ordered):
            candidate = ordered[activation_index]
            if candidate.start_seconds >= current_start - tolerance:
                break
            heapq.heappush(
                pending,
                (
                    candidate.end_seconds,
                    candidate.start_seconds,
                    candidate.audio_uri,
                    candidate,
                ),
            )
            activation_index += 1
        while pending and pending[0][0] <= current_start + tolerance:
            completed_item = heapq.heappop(pending)
            bisect.insort(recent_completed, completed_item)
            if len(recent_completed) > max_turns:
                del recent_completed[:-max_turns]

        dependencies = tuple(
            audio_uri for _, _, audio_uri, _ in recent_completed
        )
        wave = (
            0
            if not dependencies
            else 1
            + max(
                wave_by_audio_uri[audio_uri]
                for audio_uri in dependencies
            )
        )
        for current in batch:
            dependency_map[current.audio_uri] = dependencies
            wave_by_audio_uri[current.audio_uri] = wave
    return dependency_map, wave_by_audio_uri
```

Refactor the public scheduler body after grouping:

```python
if max_turns > 0:
    _validate_no_duplicate_source_spans(grouped)

dependency_map: dict[str, tuple[str, ...]] = {}
wave_by_audio_uri: dict[str, int] = {}
for group_key in sorted(grouped):
    source_dependencies, source_waves = _build_source_dependencies(
        grouped[group_key],
        max_turns=max_turns,
    )
    dependency_map.update(source_dependencies)
    wave_by_audio_uri.update(source_waves)
```

Remove the old per-current full-group `candidates` scan. Update only
`build_strict_causal_schedule()`'s `Raises:` text to state that equal and
contained same-population spans fail when `max_turns` is positive.
`_validate_evaluation_segments()` remains limited to scalar and identity
validation because it neither receives `max_turns` nor performs span
relationship checks.

- [ ] **Step 6: Add deterministic count and sample-bound tests**

Add:

```python
def test_duplicate_diagnostics_are_deterministic_and_bounded(
    self,
) -> None:
    outer = self._segment(
        "gs://a/outer",
        start=0,
        end=100,
        index=0,
    )
    inners = [
        self._segment(
            f"gs://a/inner-{index}",
            start=float(index * 10),
            end=float(index * 10 + 5),
            index=index,
        )
        for index in range(1, 7)
    ]

    messages = []
    for segments in ([outer, *inners], [*reversed(inners), outer]):
        with self.assertRaises(ValueError) as raised:
            context.build_strict_causal_schedule(
                segments,
                max_turns=2,
            )
        messages.append(str(raised.exception))

    self.assertEqual(messages[0], messages[1])
    self.assertIn("total_invalid_pairs=6", messages[0])
    self.assertEqual(messages[0].count("relationship="), 5)
    self.assertIn("split='eval'", messages[0])
    self.assertIn("source_key='source-a'", messages[0])
    self.assertIn("audio_uris=", messages[0])
    self.assertIn("intervals=", messages[0])
```

- [ ] **Step 7: Run the planner tests and targeted quality checks**

```bash
safe-run -- uv run --project model --extra dev pytest \
  model/tests/common/tests/test_gemini_context.py::TestStrictCausalSchedule \
  -q -n 0
uv run ruff check \
  model/src/common/gemini/context.py \
  model/tests/common/tests/test_gemini_context.py
uv run ruff format --check \
  model/src/common/gemini/context.py \
  model/tests/common/tests/test_gemini_context.py
git diff --check
```

Expected: all planner tests pass; Ruff and diff checks produce no findings.

- [ ] **Step 8: Commit the planner**

```bash
git add \
  model/src/common/gemini/context.py \
  model/tests/common/tests/test_gemini_context.py
git commit -m "fix(gemini-eval): reject duplicate source spans"
```

---

### Task 2: Resolve training references from the causal schedule

**Files:**

- Modify: `model/src/common/gemini/context.py:419-460`
- Modify: `model/tests/common/tests/test_gemini_context.py:12-145`

**Interfaces:**

- Consumes: `RollingHistoryScheduleRow` values produced by Task 1.
- Produces:

```python
def build_training_reference_histories(
    rows: collections.abc.Sequence[dict[str, typing.Any]],
    *,
    schedule: collections.abc.Sequence[RollingHistoryScheduleRow],
) -> list[list[TrainingReferenceTurn]]:
```

- [ ] **Step 1: Replace start-order training tests with schedule-binding tests**

Replace `TestTrainingReferenceHistories` with tests built from explicit
segments:

```python
class TestTrainingReferenceHistories(unittest.TestCase):
    def _segment(
        self,
        uri: str,
        *,
        start: float,
        end: float,
        index: int,
    ) -> context.EvaluationSegment:
        return context.EvaluationSegment(
            audio_uri=uri,
            split="train",
            source_key="source-a",
            start_seconds=start,
            end_seconds=end,
            manifest_index=index,
        )

    def test_resolves_references_from_frozen_dependencies(self) -> None:
        rows = [
            {"audio_filepath": "gs://a/one", "text": "one"},
            {"audio_filepath": "gs://a/two", "text": "two"},
            {"audio_filepath": "gs://a/three", "text": "three"},
        ]
        schedule = context.build_strict_causal_schedule(
            [
                self._segment("gs://a/one", start=0, end=1, index=0),
                self._segment("gs://a/two", start=1, end=2, index=1),
                self._segment("gs://a/three", start=2, end=3, index=2),
            ],
            max_turns=2,
        )

        histories = context.build_training_reference_histories(
            rows,
            schedule=schedule,
        )

        self.assertEqual(histories[0], [])
        self.assertEqual(
            histories[1],
            [context.TrainingReferenceTurn("one")],
        )
        self.assertEqual(
            histories[2],
            [
                context.TrainingReferenceTurn("one"),
                context.TrainingReferenceTurn("two"),
            ],
        )

    def test_omits_unusable_selected_reference_without_refill(
        self,
    ) -> None:
        rows = [
            {"audio_filepath": "gs://a/one", "text": "older usable"},
            {
                "audio_filepath": "gs://a/two",
                "text": "[Unintelligible]",
            },
            {"audio_filepath": "gs://a/three", "text": "current"},
        ]
        schedule = context.build_strict_causal_schedule(
            [
                self._segment("gs://a/one", start=0, end=1, index=0),
                self._segment("gs://a/two", start=1, end=2, index=1),
                self._segment("gs://a/three", start=2, end=3, index=2),
            ],
            max_turns=1,
        )

        histories = context.build_training_reference_histories(
            rows,
            schedule=schedule,
        )

        self.assertEqual(
            schedule[2].dependency_audio_uris,
            ("gs://a/two",),
        )
        self.assertEqual(histories[2], [])

    def test_rejects_schedule_and_row_alignment_drift(self) -> None:
        rows = [{"audio_filepath": "gs://a/wrong", "text": "one"}]
        schedule = context.build_strict_causal_schedule(
            [
                self._segment(
                    "gs://a/expected",
                    start=0,
                    end=1,
                    index=0,
                )
            ],
            max_turns=1,
        )

        with self.assertRaisesRegex(ValueError, "alignment"):
            context.build_training_reference_histories(
                rows,
                schedule=schedule,
            )
```

- [ ] **Step 2: Run the training resolver tests and verify failure**

```bash
safe-run -- uv run --project model --extra dev pytest \
  model/tests/common/tests/test_gemini_context.py::TestTrainingReferenceHistories \
  -q -n 0
```

Expected: calls fail because the existing function accepts `max_turns`, not
`schedule`.

- [ ] **Step 3: Implement schedule-driven training resolution**

Replace `build_training_reference_histories()` with:

```python
def build_training_reference_histories(
    rows: collections.abc.Sequence[dict[str, typing.Any]],
    *,
    schedule: collections.abc.Sequence[RollingHistoryScheduleRow],
) -> list[list[TrainingReferenceTurn]]:
    """Resolve training-only references for frozen causal dependencies."""
    row_values = tuple(rows)
    schedule_values = tuple(schedule)
    if len(row_values) != len(schedule_values):
        msg = "training rows and causal schedule must have equal lengths"
        raise ValueError(msg)

    text_by_audio_uri: dict[str, str] = {}
    audio_uri_by_index: list[str] = []
    for row in row_values:
        audio_uri_value = row.get("audio_filepath")
        if not isinstance(audio_uri_value, str) or not (
            audio_uri := audio_uri_value.strip()
        ):
            msg = "training row audio_filepath must be a non-empty string"
            raise ValueError(msg)
        if audio_uri in text_by_audio_uri:
            msg = "training row audio_filepath must be unique"
            raise ValueError(msg)
        text_by_audio_uri[audio_uri] = str(row.get("text") or "").strip()
        audio_uri_by_index.append(audio_uri)

    histories: list[list[TrainingReferenceTurn]] = [
        [] for _ in row_values
    ]
    seen_indices: set[int] = set()
    for schedule_row in schedule_values:
        index = schedule_row.segment.manifest_index
        if (
            index < 0
            or index >= len(row_values)
            or index in seen_indices
            or audio_uri_by_index[index]
            != schedule_row.segment.audio_uri
        ):
            msg = "training rows and causal schedule have invalid alignment"
            raise ValueError(msg)
        seen_indices.add(index)
        history: list[TrainingReferenceTurn] = []
        for dependency_audio_uri in schedule_row.dependency_audio_uris:
            if dependency_audio_uri not in text_by_audio_uri:
                msg = (
                    "training causal dependency is absent from rows: "
                    f"{dependency_audio_uri}"
                )
                raise ValueError(msg)
            text = text_by_audio_uri[dependency_audio_uri]
            if _usable_history_text(text):
                history.append(TrainingReferenceTurn(text=text))
        histories[index] = history
    if len(seen_indices) != len(row_values):
        msg = "training rows and causal schedule have invalid alignment"
        raise ValueError(msg)
    return histories
```

Retain `_episode_key()`, `_row_sort_key()`, `_numeric_value()`, and
`_int_value()` on PR #1039 because the temporary `ContextTurn` bridge still
uses them. PR #1003 removes that bridge after restacking.

- [ ] **Step 4: Run resolver and planner tests**

```bash
safe-run -- uv run --project model --extra dev pytest \
  model/tests/common/tests/test_gemini_context.py::TestTrainingReferenceHistories \
  model/tests/common/tests/test_gemini_context.py::TestStrictCausalSchedule \
  -q -n 0
uv run ruff check \
  model/src/common/gemini/context.py \
  model/tests/common/tests/test_gemini_context.py
git diff --check
```

Expected: all selected tests and static checks pass.

- [ ] **Step 5: Commit schedule-driven training binding**

```bash
git add \
  model/src/common/gemini/context.py \
  model/tests/common/tests/test_gemini_context.py
git commit -m "refactor(gemini-sft): bind training history to causal plan"
```

---

### Task 3: Share causal provenance normalization

**Files:**

- Modify: `model/src/gemini_sft/artifacts.py:242-465`
- Modify: `model/tests/common/tests/test_gemini_eval_artifacts.py:98-323`

**Interfaces:**

- Consumes: aligned raw Canonical Manifest dictionaries and
  `manifest.CanonicalRow` values.
- Produces:

```python
def causal_segments_from_rows(
    source_rows: collections.abc.Sequence[dict[str, typing.Any]],
    canonical_rows: collections.abc.Sequence[manifest.CanonicalRow],
    *,
    split: str,
) -> list[context.EvaluationSegment]:
```

- [ ] **Step 1: Add failing shared-adapter tests**

Add `import collections.abc` to `artifacts.py` in the implementation step. Add
these tests to `TestGeminiEvalArtifacts`:

```python
def test_causal_segments_normalize_training_and_eval_identically(
    self,
) -> None:
    source_rows_by_split = {
        split: [
            {
                **_eval_row(
                    f"gs://bucket/audio/{split}.flac",
                    "reference",
                    example_id=f"{split}-example",
                    segment_id="001",
                    offset=0.0,
                ),
                "split": split,
                "original_audio_uri": (
                    "gs://bucket/source/original.wav"
                ),
                "original_offset": 12.5,
            }
        ]
        for split in ("train", "eval")
    }
    segments: dict[str, context.EvaluationSegment] = {}
    for split, source_rows in source_rows_by_split.items():
        _, canonical_rows = sft_artifacts.canonical_rows_from_entries(
            source_rows,
            split=split,
            source="test",
        )
        (segments[split],) = sft_artifacts.causal_segments_from_rows(
            source_rows,
            canonical_rows,
            split=split,
        )

    self.assertEqual(segments["train"].split, "train")
    self.assertEqual(segments["eval"].split, "eval")
    self.assertEqual(
        (
            segments["train"].source_key,
            segments["train"].start_seconds,
            segments["train"].end_seconds,
            segments["train"].manifest_index,
        ),
        (
            segments["eval"].source_key,
            segments["eval"].start_seconds,
            segments["eval"].end_seconds,
            segments["eval"].manifest_index,
        ),
    )
    self.assertEqual(
        segments["train"].source_key,
        "gs://bucket/source/original.wav",
    )
    self.assertEqual(segments["train"].start_seconds, 12.5)
    self.assertEqual(segments["train"].end_seconds, 13.5)
    self.assertEqual(segments["train"].manifest_index, 0)
    for segment in segments.values():
        self.assertNotIn("text", dataclasses.asdict(segment))


def test_causal_segments_reject_alignment_drift(self) -> None:
    row = {
        **_eval_row(
            "gs://bucket/audio/001.flac",
            "reference",
            example_id="example",
            segment_id="001",
            offset=0.0,
        ),
        "original_audio_uri": "gs://bucket/source/original.wav",
        "original_offset": 0.0,
    }
    _, canonical_rows = sft_artifacts.canonical_rows_from_entries(
        [row],
        split="eval",
        source="test",
    )

    with self.assertRaisesRegex(ValueError, "equal lengths"):
        sft_artifacts.causal_segments_from_rows(
            [],
            canonical_rows,
            split="eval",
        )


def test_training_causal_segment_uses_contextual_diagnostic(self) -> None:
    row = {
        **_eval_row(
            "gs://bucket/audio/train.flac",
            "reference",
            example_id="train-example",
            segment_id="001",
            offset=0.0,
        ),
        "split": "train",
        "original_audio_uri": "gs://bucket/source/original.wav",
        "original_offset": True,
    }
    _, canonical_rows = sft_artifacts.canonical_rows_from_entries(
        [row],
        split="train",
        source="test",
    )

    with self.assertRaisesRegex(
        TypeError,
        "contextual row original_offset",
    ):
        sft_artifacts.causal_segments_from_rows(
            [row],
            canonical_rows,
            split="train",
        )
```

- [ ] **Step 2: Run adapter tests and verify the missing interface**

```bash
safe-run -- uv run --project model --extra dev pytest \
  model/tests/common/tests/test_gemini_eval_artifacts.py::TestGeminiEvalArtifacts \
  -q -n 0
```

Expected: `causal_segments_from_rows` is absent.

- [ ] **Step 3: Implement and route through the shared adapter**

Add:

```python
def causal_segments_from_rows(
    source_rows: collections.abc.Sequence[dict[str, typing.Any]],
    canonical_rows: collections.abc.Sequence[manifest.CanonicalRow],
    *,
    split: str,
) -> list[context.EvaluationSegment]:
    """Normalize aligned contextual rows into transcript-free segments."""
    source_values = tuple(source_rows)
    canonical_values = tuple(canonical_rows)
    if len(source_values) != len(canonical_values):
        msg = "source and canonical rows must have equal lengths"
        raise ValueError(msg)
    if not isinstance(split, str) or not split.strip():
        msg = "causal segment split must be a non-empty string"
        raise ValueError(msg)
    return [
        _causal_segment(
            source_row,
            canonical_row,
            manifest_index,
            split=split.strip(),
        )
        for manifest_index, (source_row, canonical_row) in enumerate(
            zip(source_values, canonical_values, strict=True)
        )
    ]
```

Rename `_causal_evaluation_segment()` to `_causal_segment()`, add a `split`
keyword, and construct the segment with:

```python
return context.EvaluationSegment(
    audio_uri=canonical_row.audio_filepath,
    split=canonical_row.split or split,
    source_key=source_key,
    start_seconds=start_seconds,
    end_seconds=end_seconds,
    manifest_index=manifest_index,
)
```

Rename `_evaluation_provenance()` to `_causal_provenance()` and change
the parameter name from `eval_row` to `canonical_row`. Generalize every
eval-specific docstring and error prefix in `_causal_segment()`,
`_causal_provenance()`, `_finite_nonnegative_number()`,
`_finite_positive_number()`, and `_finite_number()` from `eval row` or
`contextual eval row` to `contextual row`. Preserve the provenance helper's
atomic precedence:

1. Complete `original_audio_uri` plus `original_offset`, using canonical
   `duration`.
2. Otherwise complete `source_audio.audio_filepath`, `offset`, and `duration`.
3. No `example_id` fallback.

Refactor the positive-context block in
`eval_rows_for_inference_from_entries()` to:

```python
if prior_context_count > 0:
    segments = causal_segments_from_rows(
        source_rows,
        eval_rows,
        split="eval",
    )
else:
    segments = [
        _stateless_evaluation_segment(eval_row, manifest_index)
        for manifest_index, eval_row in enumerate(eval_rows)
    ]
```

- [ ] **Step 4: Run the adapter tests with the generalized diagnostics**

The current assertions match stable diagnostic substrings such as
`complete original provenance`; they should remain unchanged when the prefix
is generalized to `contextual row`. Run:

```bash
safe-run -- uv run --project model --extra dev pytest \
  model/tests/common/tests/test_gemini_eval_artifacts.py::TestGeminiEvalArtifacts \
  -q -n 0
uv run ruff check \
  model/src/gemini_sft/artifacts.py \
  model/tests/common/tests/test_gemini_eval_artifacts.py
git diff --check
```

Expected: all selected artifact tests pass.

- [ ] **Step 5: Commit shared causal normalization**

```bash
git add \
  model/src/gemini_sft/artifacts.py \
  model/tests/common/tests/test_gemini_eval_artifacts.py
git commit -m "refactor(gemini-sft): share causal row normalization"
```

---

### Task 4: Compile every positive-context plan before publication

**Files:**

- Modify: `model/src/gemini_sft/prepare.py:178-263`
- Modify: `model/src/gemini_sft/prepare.py:266-417`
- Modify: `model/tests/gemini_sft/test_workflow.py:29-105`
- Modify: `model/tests/gemini_sft/test_workflow.py:346-480`
- Modify: `model/tests/gemini_sft/test_workflow.py:935-1047`

**Interfaces:**

- Consumes: Task 1 scheduler, Task 2 training resolver, Task 3 causal adapter.
- Produces: fail-fast preparation and a rendering-only
  `write_gemini_jsonl(..., histories=...)` interface.

- [ ] **Step 1: Make valid positive-context workflow fixtures causal**

In `test_prepare_builds_same_source_prior_text_turn_context_examples`, set both
training clips to `duration=1.0`, retaining starts `0.0` and `1.0`. Give every
positive-context validation and eval row complete provenance:

```python
{
    **_row(
        "gs://audio/validation.flac",
        "validation transcript",
        duration=1.0,
        offset=5.0,
    ),
    "original_audio_uri": "gs://audio/validation-source.flac",
    "original_offset": 5.0,
}
```

```python
{
    **_row(
        "gs://audio/eval.flac",
        "eval transcript",
        duration=1.0,
        offset=6.0,
    ),
    "original_audio_uri": "gs://audio/eval-source.flac",
    "original_offset": 6.0,
}
```

Extend `_eval_only_config_text()` with
`prior_context_count: int | None = None` and pass it to `_config_text()`:

```python
def _eval_only_config_text(
    *,
    round_id: str = "round-a",
    eval_label: str = "base",
    eval_model: str = "gemini-3.1-flash-lite",
    prior_context_count: int | None = None,
) -> str:
    body = _config_text(
        round_id=round_id,
        prior_context_count=prior_context_count,
        eval_label=eval_label,
        eval_model=eval_model,
    )
    excluded = ("train_manifest_uri =", "validation_manifest_uri =")
    return "\n".join(
        line for line in body.splitlines() if not line.startswith(excluded)
    )
```

- [ ] **Step 2: Add failing preparation-boundary tests**

Add a helper:

```python
def _contained_context_rows(
    split: str,
) -> list[dict[str, typing.Any]]:
    source_uri = f"gs://audio/{split}-source.flac"
    return [
        {
            **_row(
                f"gs://audio/{split}-outer.flac",
                "outer",
                duration=10.0,
                example_id=f"{split}-example",
                segment_id="outer",
                offset=0.0,
                split=split,
            ),
            "original_audio_uri": source_uri,
            "original_offset": 0.0,
        },
        {
            **_row(
                f"gs://audio/{split}-inner.flac",
                "inner",
                duration=2.0,
                example_id=f"{split}-example",
                segment_id="inner",
                offset=2.0,
                split=split,
            ),
            "original_audio_uri": source_uri,
            "original_offset": 2.0,
        },
    ]
```

Add a training-round test that loops over train, validation, and eval:

```python
def test_positive_context_prepare_rejects_containment_before_upload(
    self,
) -> None:
    for invalid_split in ("train", "validation", "eval"):
        with (
            self.subTest(split=invalid_split),
            tempfile.TemporaryDirectory() as tmp_s,
        ):
            tmp = pathlib.Path(tmp_s)
            storage = fake_gcs.FakeStorageClient()
            manifests = {
                "train": _contained_context_rows("train")
                if invalid_split == "train"
                else [
                    {
                        **_row(
                            "gs://audio/train.flac",
                            "train",
                            duration=1.0,
                            split="train",
                        ),
                        "original_audio_uri": "gs://audio/train-source.flac",
                        "original_offset": 0.0,
                    }
                ],
                "validation": _contained_context_rows("validation")
                if invalid_split == "validation"
                else [
                    {
                        **_row(
                            "gs://audio/validation.flac",
                            "validation",
                            duration=1.0,
                            split="validation",
                        ),
                        "original_audio_uri": (
                            "gs://audio/validation-source.flac"
                        ),
                        "original_offset": 0.0,
                    }
                ],
                "eval": _contained_context_rows("eval")
                if invalid_split == "eval"
                else [
                    {
                        **_row(
                            "gs://audio/eval.flac",
                            "eval",
                            duration=1.0,
                            split="eval",
                        ),
                        "original_audio_uri": "gs://audio/eval-source.flac",
                        "original_offset": 0.0,
                    }
                ],
            }
            for split, rows in manifests.items():
                storage.put(
                    f"gs://source/manifests/{split}.jsonl",
                    _manifest(rows),
                )
            run_cfg = config_module.load_run_config(
                _write_config_file(tmp, prior_context_count=2)
            )

            with unittest.mock.patch.object(
                prepare.preflight,
                "run_preflight",
            ) as run_preflight:
                with self.assertRaisesRegex(
                    ValueError,
                    "same-source duplicate spans",
                ):
                    prepare.prepare_run(
                        run_cfg=run_cfg,
                        storage_client=storage,
                        results_dir=tmp / "results",
                    )

            run_preflight.assert_not_called()
            self.assertEqual(storage.uploads, [])
            _assert_no_prepared_outputs(self, tmp, storage)
```

Add an eval-only preparation test:

```python
def test_eval_only_positive_context_rejects_containment_before_upload(
    self,
) -> None:
    with tempfile.TemporaryDirectory() as tmp_s:
        tmp = pathlib.Path(tmp_s)
        storage = fake_gcs.FakeStorageClient()
        storage.put(
            "gs://source/manifests/eval.jsonl",
            _manifest(_contained_context_rows("eval")),
        )
        cfg_path = tmp / "run.toml"
        cfg_path.write_text(
            _eval_only_config_text(prior_context_count=2),
            encoding="utf-8",
        )
        run_cfg = config_module.load_prepare_run_config(cfg_path)

        with self.assertRaisesRegex(
            ValueError,
            "same-source duplicate spans",
        ):
            prepare.prepare_run(
                run_cfg=run_cfg,
                storage_client=storage,
                results_dir=tmp / "results",
            )

        self.assertEqual(storage.uploads, [])
        self.assertFalse(storage.has(run_cfg.paths.config_uri))
```

- [ ] **Step 3: Run preparation tests and verify containment reaches publication**

```bash
safe-run -- uv run --project model \
  --extra dev --extra scoring --extra vertex pytest \
  model/tests/gemini_sft/test_workflow.py::TestPrepareRun \
  -q -n 0
```

Expected: the new tests fail because preparation does not compile causal
schedules, and the existing training context fixture still uses start-only
history resolution.

- [ ] **Step 4: Add preparation helpers that compile before rendering**

Add these private helpers to `prepare.py` before `prepare_artifacts()`:

```python
def _training_reference_histories(
    source_rows: collections.abc.Sequence[dict[str, typing.Any]],
    canonical_rows: collections.abc.Sequence[manifest.CanonicalRow],
    *,
    split: str,
    max_turns: int,
) -> list[list[context.TrainingReferenceTurn]]:
    if max_turns == 0:
        return [[] for _ in source_rows]
    segments = artifacts_lib.causal_segments_from_rows(
        source_rows,
        canonical_rows,
        split=split,
    )
    schedule = context.build_strict_causal_schedule(
        segments,
        max_turns=max_turns,
    )
    return context.build_training_reference_histories(
        source_rows,
        schedule=schedule,
    )


def _validate_eval_context_plan(
    source_rows: collections.abc.Sequence[dict[str, typing.Any]],
    canonical_rows: collections.abc.Sequence[manifest.CanonicalRow],
    *,
    max_turns: int,
) -> None:
    if max_turns == 0:
        return
    segments = artifacts_lib.causal_segments_from_rows(
        source_rows,
        canonical_rows,
        split="eval",
    )
    context.build_strict_causal_schedule(
        segments,
        max_turns=max_turns,
    )
```

Add imports:

```python
import collections.abc

from common import manifest
```

- [ ] **Step 5: Compile all plans before writing either Gemini JSONL**

In `prepare_artifacts()`, retain `eval_entries`, calculate all histories, and
validate eval before either writer opens a file:

```python
eval_entries, eval_rows = artifacts_lib.load_canonical_rows(
    canonical_eval_path,
    "eval",
)

train_histories = _training_reference_histories(
    train_entries,
    train_rows,
    split="train",
    max_turns=run_cfg.prior_context_count,
)
validation_histories = _training_reference_histories(
    validation_entries,
    validation_rows,
    split="validation",
    max_turns=run_cfg.prior_context_count,
)
_validate_eval_context_plan(
    eval_entries,
    eval_rows,
    max_turns=run_cfg.prior_context_count,
)

write_gemini_jsonl(
    train_entries,
    gemini_train_path,
    histories=train_histories,
    system_prompt=run_cfg.system_prompt,
    user_prompt=run_cfg.user_prompt,
    prior_context_mode=run_cfg.prior_context_mode,
)
write_gemini_jsonl(
    validation_entries,
    gemini_validation_path,
    histories=validation_histories,
    system_prompt=run_cfg.system_prompt,
    user_prompt=run_cfg.user_prompt,
    prior_context_mode=run_cfg.prior_context_mode,
)
```

In `_prepare_eval_artifacts()`, retain `eval_entries` and call
`_validate_eval_context_plan()` before returning:

```python
eval_entries, eval_rows = artifacts_lib.load_canonical_rows(
    canonical_eval_path,
    "eval",
)
_validate_eval_context_plan(
    eval_entries,
    eval_rows,
    max_turns=run_cfg.prior_context_count,
)
```

Do not apply `eval.execution.limit` during preparation. Preparation validates
the complete durable manifest; runtime validates its effective limited view.

- [ ] **Step 6: Make `write_gemini_jsonl()` rendering-only**

Change its interface and body:

```python
def write_gemini_jsonl(
    rows: collections.abc.Sequence[dict[str, typing.Any]],
    path: pathlib.Path,
    *,
    histories: collections.abc.Sequence[
        collections.abc.Sequence[context.TrainingReferenceTurn]
    ],
    system_prompt: str,
    user_prompt: str,
    prior_context_mode: str = "text_turns",
) -> None:
    """Write Gemini audio-SFT JSONL from resolved training rows."""
    row_values = tuple(rows)
    history_values = tuple(histories)
    if len(row_values) != len(history_values):
        msg = "training rows and histories must have equal lengths"
        raise ValueError(msg)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row, history in zip(
            row_values,
            history_values,
            strict=True,
        ):
            audio_uri = str(row.get("audio_filepath") or "")
            example = tuning_data.build_audio_tuning_example(
                audio_uri=audio_uri,
                gt_text=str(row.get("text") or ""),
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                history=history,
                history_mode=prior_context_mode,
            )
            if not tuning_data.validate_audio_tuning_example(example):
                msg = f"invalid Gemini SFT example for {audio_uri}"
                raise ValueError(msg)
            handle.write(json.dumps(example) + "\n")
```

Remove `prior_context_count` from the writer interface. There are no direct
writer calls in the current tests; the eval-only `write_gemini_jsonl` mock
remains unchanged because that path must continue to assert the writer is never
called.

- [ ] **Step 7: Run preparation and common contract tests**

```bash
safe-run -- uv run --project model \
  --extra dev --extra scoring --extra vertex pytest \
  model/tests/common/tests/test_gemini_context.py \
  model/tests/common/tests/test_gemini_eval_artifacts.py \
  model/tests/gemini_sft/test_workflow.py::TestPrepareRun \
  -q -n 0
uv run ruff check \
  model/src/common/gemini/context.py \
  model/src/gemini_sft/artifacts.py \
  model/src/gemini_sft/prepare.py \
  model/tests/common/tests/test_gemini_context.py \
  model/tests/common/tests/test_gemini_eval_artifacts.py \
  model/tests/gemini_sft/test_workflow.py
git diff --check
```

Expected: all targeted tests and static checks pass. The K=0 eval-only test
continues to pass without source provenance.

- [ ] **Step 8: Commit preparation integration**

```bash
git add \
  model/src/gemini_sft/prepare.py \
  model/tests/gemini_sft/test_workflow.py
git commit -m "fix(gemini-sft): validate causal plans before publish"
```

---

### Task 5: Document and verify the base contract

**Files:**

- Modify: `model/scripts/sft/docs/configs.md:30-65`

**Interfaces:**

- Consumes: completed behavior from Tasks 1-4.
- Produces: operator documentation matching the executable contract.

- [ ] **Step 1: Document the exact overlap and K behavior**

Replace the rolling-evaluation paragraph in `configs.md` with:

```markdown
Training and rolling evaluation use the same transcript-free structural
schedule. Rows are grouped by split and source. Within floating-point boundary
tolerance, a dependency must start strictly before the current segment and
finish no later than the current start. Equal intervals and intervals where one
contains the other are rejected as duplicate contextual segments. Partial
overlap is allowed, but overlapping rows cannot become dependencies of each
other; both may become history for a later row after both have ended.

Contextual rows require one complete source-provenance tuple: either original
source URI plus original offset, or a complete `source_audio`
URI/offset/duration tuple. The configured K is applied to structural
dependencies before unusable references or predictions are omitted, without
refilling older rows. Every evaluation request contains transcript-only
predicted history and exactly one audio input: the current clip.
```

- [ ] **Step 2: Run the complete targeted base verification**

```bash
safe-run -- uv run --project model \
  --extra dev --extra scoring --extra vertex pytest \
  model/tests/common/tests/test_gemini_context.py \
  model/tests/common/tests/test_gemini_eval_artifacts.py \
  model/tests/gemini_sft/test_workflow.py::TestPrepareRun \
  -q -n 0
uv run ruff format --check \
  model/src/common/gemini/context.py \
  model/src/gemini_sft/artifacts.py \
  model/src/gemini_sft/prepare.py \
  model/tests/common/tests/test_gemini_context.py \
  model/tests/common/tests/test_gemini_eval_artifacts.py \
  model/tests/gemini_sft/test_workflow.py
uv run ruff check \
  model/src/common/gemini/context.py \
  model/src/gemini_sft/artifacts.py \
  model/src/gemini_sft/prepare.py \
  model/tests/common/tests/test_gemini_context.py \
  model/tests/common/tests/test_gemini_eval_artifacts.py \
  model/tests/gemini_sft/test_workflow.py
git diff --check
```

Expected: all targeted tests and checks pass.

- [ ] **Step 3: Commit documentation**

```bash
git add \
  model/scripts/sft/docs/configs.md
git commit -m "docs(gemini-eval): clarify duplicate span contract"
```

---

### Task 6: Restack and prove runtime validation on PR #1003

**Files:**

- Modify:
  `model/tests/gemini_sft/test_target_execution.py:1461-1595`

**Interfaces:**

- Consumes: `build_strict_causal_schedule()` from PR #1039 through the existing
  `_run_predicted_history_target_inference()` call.
- Produces: a regression test proving no online wave or rolling artifact starts
  before containment validation.

- [ ] **Step 1: Restack the execution branch**

From the existing `fix/gemini-predicted-history-eval` worktree after PR #1039
contains Tasks 1-5:

```bash
git rebase refactor/gemini-eval-causal-contract
```

Expected: PR #1003 contains the updated planner beneath its execution commits.
Resolve conflicts by preserving PR #1003's removal of the temporary
`ContextTurn` bridge and its existing call to
`build_strict_causal_schedule()`; do not reintroduce the legacy history loader.

- [ ] **Step 2: Add the defensive runtime test**

Add to `TestPredictedHistoryOnlineTargetInference`:

```python
def test_rejects_contained_segments_before_starting_online_wave(
    self,
) -> None:
    segments = (
        context.EvaluationSegment(
            audio_uri="gs://audio/outer.flac",
            split="eval",
            source_key="source-a",
            start_seconds=0.0,
            end_seconds=10.0,
            manifest_index=0,
        ),
        context.EvaluationSegment(
            audio_uri="gs://audio/inner.flac",
            split="eval",
            source_key="source-a",
            start_seconds=2.0,
            end_seconds=4.0,
            manifest_index=1,
        ),
    )

    with unittest.mock.patch.object(
        target_execution,
        "_run_online_wave",
        unittest.mock.AsyncMock(),
    ) as run_wave:
        with self.assertRaisesRegex(
            ValueError,
            "same-source duplicate spans",
        ):
            asyncio.run(
                target_execution.run_online_target_inference(
                    storage_client=self.storage,
                    run_gcs_prefix="gs://bucket/run",
                    project="project",
                    target_label="base",
                    target_model="gemini-3.1-flash-lite",
                    segments=segments,
                    system_prompt="system",
                    user_prompt="user",
                    prior_context_count=2,
                    prior_context_mode="text_turns",
                    eval_manifest_uri="gs://data/eval.jsonl",
                    local_dir=self.local_dir,
                    concurrency=2,
                    max_retries=1,
                )
            )

    run_wave.assert_not_awaited()
    self.assertEqual(self.storage.uploads, [])
```

Do not add another planner call to `evaluate.py` or `target_execution.py`.
`_run_predicted_history_target_inference()` already validates before
`_run_online_wave()`.

- [ ] **Step 3: Run stacked runtime and workflow checks**

```bash
safe-run -- uv run --project model \
  --extra dev --extra scoring --extra vertex pytest \
  model/tests/gemini_sft/test_target_execution.py::TestRunOnlineTargetInference \
  model/tests/gemini_sft/test_target_execution.py::TestPredictedHistoryOnlineTargetInference \
  model/tests/gemini_sft/test_workflow.py::TestPrepareRun \
  model/tests/gemini_sft/test_workflow.py::TestEvaluateRun \
  -q -n 0
uv run ruff check \
  model/tests/gemini_sft/test_target_execution.py
git diff --check
```

Expected: all selected stacked-PR tests pass; the new test proves the wave mock
was never awaited and GCS received no rolling artifacts.

- [ ] **Step 4: Commit the stacked regression test**

```bash
git add model/tests/gemini_sft/test_target_execution.py
git commit -m "test(gemini-eval): reject duplicate spans before inference"
```

---

## Final Review Checklist

- [ ] PR #1039 contains no changes under `model/colabs`.
- [ ] Positive-context equality and containment fail within the same
  `(split, source_key)`.
- [ ] Partial overlap remains valid and both rows can feed a later row.
- [ ] K=0 remains stateless and provenance-optional.
- [ ] Training and evaluation use identical structural dependency tuples.
- [ ] Unusable selected text is omitted without older-row refill.
- [ ] Training, validation, and full eval manifests validate before either
  Gemini JSONL file or any durable GCS artifact is published.
- [ ] PR #1003 adds no redundant planner call.
- [ ] Targeted tests, Ruff checks, and `git diff --check` are green.
