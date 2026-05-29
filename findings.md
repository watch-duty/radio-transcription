# Findings

## Public ATC Usefulness Hypothesis

The public ATC corpus should not improve Watch Duty terminology directly.
Its expected value is exposure to clear radio speech containing numbers,
identifiers, short commands, and phonetic alphabet words. Those features overlap
with Watch Duty incident IDs, addresses, unit identifiers, and clipped tactical
radio phrasing.

## Primary Risk

Training on too much ATC can bias the model toward aviation-procedure language.
The selector therefore excludes strong aviation terms and penalizes softer ATC
terms while keeping examples with target-like numeric and identifier structure.

## Measurement Plan

Use normalized WER as the primary metric. Use normalized CER and keyword metrics
as secondary metrics. Use hallucination rate, duration-bucket WER, and paired
bootstrap confidence intervals as diagnostics.

## Data Boundary

The Watch Duty eval split is used for Vertex validation and final scoring. It is
not added to training. Public ATC train, validation, and test splits are treated
as one training-only pool.

## Selection Audit Result

The refined public ATC slice is optimal for the current constrained selector:
the CP-SAT re-solve is `OPTIMAL`, objective delta is 0, all target deltas are 0,
and all 2,000 selected IDs match the resolved optimum.

The first selector version missed residual aviation phraseology such as
`direct`, `contact`, `radar`, `turn`, `proceed`, `decimal`, and common airline
callsigns. Those terms are now soft-penalized. The refined selection reduced
procedure-term rows from 620 to 110 and the sampled airline-token rows from 488
to 119 while preserving all numeric/phonetic targets.

## Remaining Assumptions Before Tuning

- Public ATC numeric and phonetic structures transfer to Watch Duty WER despite
  imperfect domain match.
- Original public word count is the right selector proxy for spoken complexity,
  even though normalized public training labels become shorter after digit-word
  grouping.
- Validation-backed Watch Duty eval is adequate for this one-off model
  selection, but it is not a clean held-out benchmark.
- Hugging Face metadata exposes no license field for `jacktol/ATC-ASR-Dataset`;
  confirm that this public dataset is acceptable for internal SFT before running
  paid jobs.
- The installed Google Gen AI SDK supports the tuning fields used by the
  pipeline, including validation datasets and checkpoint export.

## Preflight Result

The exact WD-only and WD+ATC train/validation pairs both pass the SFT preflight
gate before any tuning submission. This verifies JSONL parsing, SFT schema,
empty-target checks, approximate text-token caps, train/validation disjointness,
and GCS audio reachability for the configured train and validation examples.

Base-model batch evaluation was not run in this pass. It would submit a paid
Vertex batch inference job and does not change the data-readiness decision; the
same eval pipeline should be run once endpoints are available for final scoring.
