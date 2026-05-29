# Protocol: Public ATC Augmentation

## Question

Does adding a solver-selected public ATC slice to the Watch Duty internal SFT
train set improve Watch Duty transcription quality for Gemini 3.1 Flash-Lite?

## Inputs

- Watch Duty train:
  `gs://wd-transcription-data/sft/dataset_versions/radio-transcription-sft-v20260528/model_inputs/gemini/train.jsonl`
- Watch Duty validation/eval:
  `gs://wd-transcription-data/sft/dataset_versions/radio-transcription-sft-v20260528/model_inputs/gemini/eval.jsonl`
- Watch Duty eval manifest:
  `gs://wd-transcription-data/sft/dataset_versions/radio-transcription-sft-v20260528/manifests/canonical/eval.jsonl`
- Public ATC:
  `jacktol/ATC-ASR-Dataset`, using train, validation, and test as one training
  candidate pool.

## Public Selection Requirements

Select exactly 2,000 public examples with OR-Tools CP-SAT.

Hard exclusions:

- runway
- taxi
- takeoff
- landing
- flight level
- squawk
- QNH
- ILS
- ATIS
- glideslope
- localizer

Targets:

- 350 examples with 0 to 3 words
- 1,150 examples with 4 to 10 words
- 400 examples with 11 to 14 words
- 100 examples with 15 to 25 words
- 0 examples with 26 or more words
- 1,600 examples containing numeric content
- 1,540 examples containing numeric content in phrase context
- 60 examples that are number-only
- 500 examples containing phonetic alphabet words
- 400 examples containing both numeric and phonetic content

Soft penalties:

- heading
- climb
- descend
- approach
- departure
- tower
- ground
- cleared
- knots
- altitude
- maintain
- direct
- contact
- radar
- turn
- proceed
- hold short
- line up
- wind
- degrees
- decimal
- lufthansa
- speedbird
- aeroflot
- turkish
- austrian
- shamrock
- ascot
- sunturk
- ethiopian
- csa

## Label Normalization

Normalize digit words, including ATC `niner`, to grouped digits. Keep phonetic
alphabet words as words. Reject rows where normalization would require extreme
rewrites beyond numeric normalization.

## Artifacts

- selected public manifest
- selection feature report
- selection audit report
- preflight reports for WD-only and WD+ATC train/validation pairs
- public Gemini JSONL
- public FLAC audio in GCS
- blended Gemini training JSONL in GCS
- four tuning config directories

## Selection Audit

The refined selected public slice is optimal for the locked constrained
objective. The audit re-solves the CP-SAT problem and confirms:

- 2,000 selected rows
- all numeric, phonetic, and word-count targets hit exactly
- objective delta versus the resolved optimum is 0
- 0 Watch Duty train/eval audio overlap

This proves the rows are best for the current selector objective. It does not
prove the selector objective is globally best for Watch Duty WER; that remains
the experiment hypothesis tested by the WD-only versus WD+ATC tuning runs.

The refined soft-penalty list reduces residual aviation-procedure rows, but some
remain because exact numeric/phonetic targets make them unavoidable. This is
acceptable because public ATC is a small fraction of the blended train set and
the comparison against WD-only isolates whether the transfer helps.

## Preflight Gate

Before tuning, run `oneoff_gemini31_flash_lite.py preflight-configs`. The
current WD-only and WD+ATC train/validation pairs both pass this gate with zero
failures. This verifies that paid tuning should not fail later because of
malformed SFT JSONL, empty targets, train/validation overlap, token-cap issues,
or unreachable GCS audio.

## Stop Condition

Do not run `pipeline.py tune` or submit a Vertex tuning job from this protocol.
