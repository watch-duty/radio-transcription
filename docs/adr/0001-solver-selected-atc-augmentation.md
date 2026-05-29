# ADR 0001: Solver-Selected Public ATC Augmentation for Gemini 3.1 Flash-Lite SFT

## Status

Accepted for the one-off Gemini 3.1 Flash-Lite SFT experiment.

## Context

We need two fine-tuning runs against Gemini 3.1 Flash-Lite:

1. Watch Duty internal data only.
2. Watch Duty internal data plus a useful slice of the public
   `jacktol/ATC-ASR-Dataset` corpus.

The public ATC corpus is not domain-equivalent to Watch Duty wildfire radio.
Aviation-procedure vocabulary can hurt the target task, while short radio
utterances with numbers, identifiers, and phonetic alphabet words may help the
model transcribe Watch Duty-style incident IDs, units, addresses, and tactical
radio fragments.

## Decision

Use an OR-Tools CP-SAT selector to choose exactly 2,000 public examples from the
combined ATC train, validation, and test splits. The selector treats all public
splits as one candidate pool because the public data is used only for training,
not for evaluation.

The selector optimizes for:

- short utterances matching the Watch Duty duration/word-count profile;
- numeric-heavy examples;
- examples with digit context rather than bare isolated number lists;
- some phonetic alphabet coverage;
- low exposure to aviation-procedure vocabulary. Strong aviation terms are hard
  exclusions; residual procedure terms and common airline/callsign tokens are
  soft penalties so the solver can avoid them when feasible without breaking the
  numeric/phonetic targets.

The selected public rows are appended to the Watch Duty Gemini training JSONL
and shuffled deterministically. The Watch Duty eval split is used as the Vertex
validation dataset and as the final validation-backed scoring set. Watch Duty
eval rows are never added to training.

## Consequences

This design tests whether public radio speech improves Watch Duty metrics
without letting ATC dominate the training distribution. It also gives us an
auditable selected manifest, feature distribution report, and deterministic
rebuild path.

The selected public rows are best only with respect to this locked selector
objective. A re-solve audit confirms the refined selected IDs are an optimal
solution with zero objective delta, but the WD-only versus WD+ATC tuning runs
remain necessary to test whether the objective improves Watch Duty WER.

The final score is validation-backed because the Watch Duty eval split is also
the Vertex validation split. That is acceptable for model selection here, but it
should not be reported as a clean held-out benchmark.

## Non-goals

- Training on all public ATC data.
- Evaluating against public ATC.
- Re-splitting the Watch Duty dataset for a new held-out test set.
- Running tuning jobs from the staging script.
