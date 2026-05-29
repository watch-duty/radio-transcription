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
