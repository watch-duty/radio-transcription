# Same-Source Overlap and Duplicate Segment Design

Status: Approved

Date: 2026-07-23

## Context

The Gemini prior-context workflow schedules model-ready audio segments from a
common source recording. Two segments from the same source may overlap without
being duplicates. For example, distinct transmissions or concurrent windows
may partially overlap and must remain valid evaluation targets.

The current causal evaluator prevents an overlapping segment from becoming
history for another overlapping segment. It does not, however, reject equal or
contained spans. Consequently, an original clip and a padded or cropped version
of that clip can both enter the manifest and later become separate history
turns.

Training and evaluation also construct histories differently today. The
training path orders rows by start time without considering end time, while the
causal evaluation path requires a prior segment to have ended before the
current segment starts.

## Goals

- Allow legitimate partial overlap between same-source segments.
- Reject exact, padded, and cropped duplicate spans.
- Keep overlapping segments causally independent from each other.
- Use the same structural history plan for training and evaluation.
- Fail before provider requests or durable run-state publication.
- Keep the interface small: no new identity field or overlap-policy setting.

## Non-Goals

- Detect semantic duplicates whose spans partially overlap but do not contain
  one another.
- Compare decoded audio, transcripts, embeddings, or perceptual similarity.
- Select a preferred row or silently remove duplicates.
- Infer conversation episodes or production lineage.
- Change the behavior of unrelated manifest validation.

## Span Model

Each segment has:

- a split;
- a normalized source identity;
- a half-open source-relative interval `[start, end)`;
- its authoritative manifest position and model-ready audio URI.

Span relationships are evaluated within the same contextual history population:
the same split and normalized source identity. Cross-split leakage validation is
a separate manifest concern and is not changed by this design.

Boundary comparisons use the causal scheduler's existing floating-point
tolerance. `A` contains `B` when
`A.start <= B.start + tolerance` and `A.end >= B.end - tolerance`.

For two same-source segments `A` and `B`:

| Relationship | Definition | Behavior |
| --- | --- | --- |
| Equal | Each span contains the other within tolerance | Reject as duplicate |
| Containment | `A` contains `B`, or `B` contains `A`, within tolerance | Reject as padded/cropped duplicate |
| Partial overlap | The spans intersect, but neither contains the other | Allow |
| Contiguous | One span ends where the other begins within tolerance | Allow |
| Disjoint | The spans do not intersect | Allow |

Containment includes a shared start with different ends and a shared end with
different starts.

Examples:

```text
A [10, 20), B [10, 20)  -> reject: equal
A [10, 20), B [ 9, 21)  -> reject: B contains A
A [10, 20), B [12, 18)  -> reject: A contains B
A [10, 20), B [15, 25)  -> allow: partial overlap
A [10, 20), B [20, 25)  -> allow: contiguous
```

This is an intentionally structural definition of duplicate. A shifted or
re-cut version that only partially overlaps remains valid because the available
metadata cannot prove semantic equivalence.

## Architecture

The seam remains the transcript-free causal planning module. Its existing
interface continues to accept normalized segment metadata and return an
immutable schedule. No `transmission_id`, `source_transmission_id`, or
`overlap_policy` is added.

The planning implementation performs these operations in order:

1. Validate segment fields and unique row identities.
2. Group segments by split and normalized source identity.
3. Reject equal or contained pairs with deterministic diagnostics.
4. Construct causal dependencies.
5. Return schedule rows in authoritative manifest order.

Partial overlap does not create a dependency in either direction. Once both
segments have ended, both may independently be eligible as history for a later
segment, subject to the configured K limit.

The containment check belongs in the shared planning implementation rather
than the training, evaluation, or provider adapters. This keeps duplicate-span
knowledge local and makes the planner interface the test surface.

## Training and Evaluation

Training and evaluation consume the same transcript-free structural plan:

- The training adapter resolves selected dependency rows to reference text.
- The evaluation adapter resolves those same rows to finalized model
  predictions.
- K is applied to structural dependencies before text-usability filtering.
- Missing, failed, blank, or `[UNINTELLIGIBLE]` selected turns are omitted
  without refilling from older rows.

An overlapping row never supplies history to another row whose audio overlaps
it. Distinct partially overlapping rows may both supply history to a later row
after both have completed.

For positive prior context, complete source provenance is already required, so
duplicate-span validation is mandatory. Stateless evaluation does not gain a
new provenance requirement.

## Failure Behavior

Duplicate spans fail preparation. The workflow does not choose a winner,
rewrite intervals, or delete a row.

Diagnostics include a bounded, deterministic sample containing:

- split;
- source identity;
- both manifest indices;
- both model-ready audio URIs;
- both half-open intervals;
- whether the relationship is equality or containment;
- the total number of invalid pairs.

Evaluation defensively revalidates the compiled structural plan before making
provider requests. No partial plan or scoring report is published after a
duplicate-span failure.

## Performance

The implementation should use a per-source interval sweep after deterministic
sorting. Validation should be `O(N log N)` overall rather than comparing every
pair in a source group. Dependency emission remains `O(N * K)`.

## Testing

Tests cross the causal planner interface and cover:

- identical intervals;
- strict containment in both directions;
- equal starts with different ends;
- equal ends with different starts;
- containment at floating-point tolerance;
- partial overlap in both directions;
- contiguous and disjoint intervals;
- overlap across different source identities;
- deterministic diagnostics under input permutation;
- both partially overlapping rows becoming dependencies of a later row;
- identical structural dependencies for training and evaluation;
- failure before provider calls and durable artifact publication.

The existing overlap test is retained but clarified to assert legitimate
partial-overlap behavior. New tests cover equality and containment rejection.

## Alternatives Rejected

### Reject all overlap

This is safe but rejects valid distinct segments and does not match the desired
dataset contract.

### Add a semantic transmission identifier

No current Canonical Manifest field reliably identifies one semantic
transmission. Production segment IDs are retry-stable emitted-segment IDs, and
dataset builders commonly assign row-oriented identifiers. Adding a new field
would create an underspecified producer contract without being necessary for
the agreed containment rule.

### Infer duplicates from similarity

Intersection-over-union thresholds, transcript equality, decoded-audio hashes,
or perceptual similarity introduce heuristic behavior, provider or media I/O,
and difficult cache/version semantics. They are unnecessary for the structural
duplicate definition.

## Migration

There is no backward-compatibility mode:

- contextual manifests containing equal or contained same-source spans become
  invalid;
- partially overlapping manifests remain valid;
- corrected manifests create new run and request identities;
- no compatibility flag or fallback identity is introduced.
