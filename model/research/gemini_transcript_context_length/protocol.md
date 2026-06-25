# Gemini Transcript Context Length Protocol

## Objective

Find the best transcript-only prior-context window for Gemini ASR on the
`radio-transcription-sft-v20260528` eval split.

## Eval Set

- `bcfy_calls/eval.jsonl`
- `bcfy_feeds/eval.jsonl`
- `echo/eval.jsonl`
- `fire_notifications/eval.jsonl`

## Conditions

Run sequential self-feeding inference. Each arm uses only its own previous
Gemini predictions as text context. Prior audio is never included.

Count windows:

- 4 prior transcripts
- 6 prior transcripts
- 8 prior transcripts
- 10 prior transcripts

Duration windows:

- 5 seconds of retained prior active audio duration
- 10 seconds
- 15 seconds
- 20 seconds
- 30 seconds

Rows are grouped by `original_audio_uri` and sorted by `original_offset`, then
`row_index`. This avoids cross-recording leakage through broader
`source_group` labels.

## Gate

First run one count arm and one duration arm in parallel:

- count window 4
- duration window 15 seconds

Score both outputs. If both have complete row counts and scorer output is valid,
launch the remaining seven arms in parallel.

## Primary Metrics

- aggregate normalized WER using `common.scoring.build_normalizer`
- per-dataset normalized WER
- perfect match rate
- blank / `[UNINTELLIGIBLE]` rate
- API error count
- exact history-copy count
- history-substring hit count

## Selection Rule

Choose the smallest context window within 0.2 absolute WER points of the best
global WER, unless it has materially worse per-dataset WER or higher blank /
history-copy rates.
