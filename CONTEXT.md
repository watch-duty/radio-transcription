# Context Glossary

## Watch Duty internal dataset

The full Watch Duty SFT dataset version `radio-transcription-sft-v20260528`.
It includes the `bcfy_calls`, `bcfy_feeds`, `echo`, and `fire_notifications`
families. It does not mean the Echo family alone.

## WD SFT v20260528

Short name for `radio-transcription-sft-v20260528`, the canonical Watch Duty
internal train/eval split used by this experiment.

## Public augmentation

Public ASR training examples added to the Watch Duty internal training set to
test whether external radio speech improves Watch Duty transcription metrics.

## Selected public ATC slice

The solver-selected subset of `jacktol/ATC-ASR-Dataset` used for public
augmentation. It is selected for short numeric radio utterances and identifier
coverage while avoiding aviation-procedure vocabulary.

## Validation-backed score

An evaluation score computed on a dataset that was also supplied to Vertex as a
validation dataset during tuning. It is useful for model comparison, but it is
not a clean held-out test score.

## Checkpoint evaluation

Scoring each available tuned-model epoch checkpoint instead of only scoring the
final tuned endpoint.
