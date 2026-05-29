# Protocol: Adapter Capacity Comparison

## Question

For Gemini 3.1 Flash-Lite SFT on Watch Duty transcription data, does adapter
size FOUR or EIGHT produce better validation-backed Watch Duty transcription
metrics?

## Runs

Prepare four tuning configurations:

| Variant | Training data | Adapter | Epochs | LR multiplier |
| --- | --- | --- | --- | --- |
| WD-A4 | Watch Duty internal | FOUR | 5 | 1.0 |
| WD-A8 | Watch Duty internal | EIGHT | 5 | 1.0 |
| WD+ATC-A4 | Watch Duty internal + selected public ATC | FOUR | 5 | 1.0 |
| WD+ATC-A8 | Watch Duty internal + selected public ATC | EIGHT | 5 | 1.0 |

## Checkpoints

Tune with checkpoint export enabled so each epoch checkpoint can be scored.
Compare checkpoint curves before choosing a final endpoint for follow-up work.

## Metrics

Primary:

- normalized WER

Secondary:

- normalized CER
- keyword metrics

Diagnostics:

- hallucination rate
- duration-bucket WER
- paired bootstrap interval against the WD-only baseline

## Stop Condition

This protocol prepares commands and configs only. Do not submit tuning jobs
until an operator explicitly starts them.
