# Context Is an Interface: Utility and Failure Modes of Prior Transcripts for Emergency-Radio ASR

This directory contains the canonical venue-neutral Markdown paper,
**“Context Is an Interface: Utility and Failure Modes of Prior Transcripts for
Emergency-Radio ASR.”**

The paper reports a prediction-only development study of Gemini 3.1 Flash-Lite.
Every main-matrix nonzero-history request uses only the same target and
trajectory's own strictly earlier usable predictions; stress requests use only
authenticated predictions from the same target. Reference transcripts never
enter model requests, rolling schedules, or stress histories. They are used for
scoring and, in the reference-absent lexical stress only, a private offline
candidate-absence check.

The completed main matrix evaluates three historical targets in 18 cells on
4,108 clips; 4,098 normalized-nonempty rows form the primary ASR population.
For each target, P0 is the exact-P13 structural no-history control, R0 is the
distinct Option-D interface control, and R1/R2/R4/R8 use rolling predicted
history. A frozen 500-target stress frame adds unrelated, strictly older
same-source, shuffled, and reference-absent entity-bearing predicted histories;
498 normalized-nonempty rows support stress quality metrics.

The result is mixed and negative for the strong learned-contract thesis:

- Relative to exact-P13 P0, R8 improves WER by 1.85 points for no-context SFT
  (95% reduction interval [1.25, 2.42]) and 1.76 points for prior-context SFT
  ([1.05, 2.46]).
- No-context SFT and prior-context SFT reach 21.99% and 22.01% WER at R8; the
  0.02-point gap has a 95% CI of [−0.63, 0.70].
- Prior-context SFT has higher R8 lexical reference-proxy diagnostics: +1.44
  context-copy points among eligible-history rows and +1.11 prior-only points
  over all prediction tokens.
- Unrelated predicted context hurts both tuned targets, but the cross-SFT
  interaction is unsupported.
- In the reference-absent lexical stress, selected candidate phrases appear in
  0.8%, 0.4%, and 1.8% of base, no-context-SFT, and prior-context-SFT outputs;
  none of the three Holm-adjusted adoption contrasts is conclusive.

These findings do not establish semantic hallucination, acoustic support, or a
causal SFT effect. P0 now separates the P13 empty-history interface from
rolling history, but the SFT jobs remain historically unmatched, the frame is
development data, no held-out confirmation or human audio adjudication exists,
and the registered acoustic-overlap audit produced no admissible result.

## Canonical paper files

- [`main.md`](main.md): authoritative paper.
- [`appendix.md`](appendix.md): target lineage, prompts, prediction-only
  invariants, full metrics and contrasts, operational accounting, exclusions,
  reproduction commands, and submission gates.
- [`references.bib`](references.bib): programmatically verified primary-source
  bibliography; Markdown citations use these keys.
- [`claims_evidence_matrix.csv`](claims_evidence_matrix.csv): claim-level
  publication gate maintained by the broader research package.
- [`reviewer2.md`](reviewer2.md): publication-safe adversarial self-critique,
  including the post-outcome breakdown and submission gates.
- [`tables/predicted_history_extension_main.md`](tables/predicted_history_extension_main.md):
  generated 18-cell table with exact-P13 P0 controls and paired contrasts.
- [`tables/predicted_history_stress.md`](tables/predicted_history_stress.md):
  generated stress table.
- [`tables/predicted_history_reference_absent_entity.md`](tables/predicted_history_reference_absent_entity.md):
  generated reference-absent prediction lexical-stress table.
- [`tables/predicted_history_breakdown.md`](tables/predicted_history_breakdown.md):
  generated post-outcome duration, reference-length, and pseudonymized-family
  breakdown.
- [`figures/fig_task_contract.pdf`](figures/fig_task_contract.pdf) and
  [PNG](figures/fig_task_contract.png): prediction-only serving loop and
  scoring boundary.
- [`figures/fig_predicted_history_concepts_receipt.json`](figures/fig_predicted_history_concepts_receipt.json):
  concept-figure contract and output hashes.
- [`figures/fig_predicted_history_extension_exact_p13_v1.pdf`](figures/fig_predicted_history_extension_exact_p13_v1.pdf)
  and [PNG](figures/fig_predicted_history_extension_exact_p13_v1.png): generated
  exact-P13 P0/R1/R2/R4/R8 curves with Option-D R0 detached.
- [`figures/fig_predicted_history_extension_matrix_v1.pdf`](figures/fig_predicted_history_extension_matrix_v1.pdf)
  and [PNG](figures/fig_predicted_history_extension_matrix_v1.png): generated
  18-cell serving matrix with exact-P13 cells and detached Option-D controls.
- [`figures/fig_predicted_history_extension_tradeoff_v1.pdf`](figures/fig_predicted_history_extension_tradeoff_v1.pdf)
  and [PNG](figures/fig_predicted_history_extension_tradeoff_v1.png): generated
  extension WER/lexical-overlap trade-off.
- [`figures/fig_predicted_history_extension_v1_receipt.json`](figures/fig_predicted_history_extension_v1_receipt.json):
  aggregate-input, generator, data, and versioned output hashes.
- [`figures/gen_fig_predicted_history_extension_v1.py`](figures/gen_fig_predicted_history_extension_v1.py):
  aggregate-only versioned figure generator.
- [`figures/data/predicted_history_extension_v1.csv`](figures/data/predicted_history_extension_v1.csv)
  and [JSON](figures/data/predicted_history_extension_v1.json): validated
  aggregate figure data.

### Pre-extension prediction-only provenance

The following aggregate artifacts predate exact-P13 P0. They are retained for
reproduction history, but they are not the current main table or figures and
must not be used to place Option-D R0 at the origin of the P13 curve:

- [`tables/predicted_history_main.md`](tables/predicted_history_main.md):
  original 15-cell table;
- [`figures/fig_predicted_history_windows.pdf`](figures/fig_predicted_history_windows.pdf)
  and [PNG](figures/fig_predicted_history_windows.png): original window curves;
- [`figures/fig_predicted_history_tradeoff.pdf`](figures/fig_predicted_history_tradeoff.pdf)
  and [PNG](figures/fig_predicted_history_tradeoff.png): original trade-off;
- [`figures/fig_factorial_matrix.pdf`](figures/fig_factorial_matrix.pdf) and
  [PNG](figures/fig_factorial_matrix.png): original 3×5 serving matrix; and
- [`figures/fig_predicted_history_receipt.json`](figures/fig_predicted_history_receipt.json):
  aggregate-input and output-hash receipt for those pre-extension artifacts.

The earlier LaTeX paper, rendered PDFs, and their reference-history tables and
figures have been moved to the explicitly excluded
[`archive/`](archive/README.md). They are internal provenance only, are not
release candidates, and must not be submitted or cited as the current paper.
No conventionally named LaTeX or PDF paper is authoritative.

## Regenerating aggregate results

Run from the repository root in the locked model environment. The canonical
analyzer reauthenticates fixed private artifacts before joining scoring-only
references. It accepts no caller-provided predictions, histories, references,
models, or result bundle.

```bash
REPLAY_DIR="$(mktemp -d)"
PYTHONPATH=model/src python -m experiments.scripts.predicted_history_analysis \
  --json-output "$REPLAY_DIR/analysis.json" \
  --csv-output "$REPLAY_DIR/analysis.csv" \
  --main-table-output "$REPLAY_DIR/main.md" \
  --stress-table-output "$REPLAY_DIR/stress.md" \
  --window-data-output "$REPLAY_DIR/windows.csv" \
  --tradeoff-data-output "$REPLAY_DIR/tradeoff.csv"
cmp "$REPLAY_DIR/analysis.json" experiments/results/predicted_history_analysis.json
cmp "$REPLAY_DIR/analysis.csv" experiments/results/predicted_history_analysis.csv
cmp "$REPLAY_DIR/main.md" paper/tables/predicted_history_main.md
cmp "$REPLAY_DIR/stress.md" paper/tables/predicted_history_stress.md
cmp "$REPLAY_DIR/windows.csv" paper/figures/data/predicted_history_windows.csv
cmp "$REPLAY_DIR/tradeoff.csv" paper/figures/data/predicted_history_tradeoff.csv
```

The analyzer is create-only. This command writes to a fresh directory and
requires all six byte comparisons to succeed; it never targets the canonical
outputs.

Replay the additive P13-zero and lexical-stress analysis from its authenticated
owner-only assignment registry into a second fresh directory. Set
`OWNER_ONLY_ENTITY_REGISTRY` to that access-controlled file before running:

```bash
EXTENSION_REPLAY_DIR="$(mktemp -d)"
PYTHONPATH=.:model/src uv run --project model --frozen python -m \
  experiments.scripts.predicted_history_extension_analysis \
  --private-registry "$OWNER_ONLY_ENTITY_REGISTRY" \
  --json-output "$EXTENSION_REPLAY_DIR/analysis.json" \
  --csv-output "$EXTENSION_REPLAY_DIR/analysis.csv" \
  --main-table-output "$EXTENSION_REPLAY_DIR/main.md" \
  --entity-table-output "$EXTENSION_REPLAY_DIR/entity.md"
cmp "$EXTENSION_REPLAY_DIR/analysis.json" \
  experiments/results/predicted_history_extension_analysis.json
cmp "$EXTENSION_REPLAY_DIR/analysis.csv" \
  experiments/results/predicted_history_extension_analysis.csv
cmp "$EXTENSION_REPLAY_DIR/main.md" \
  paper/tables/predicted_history_extension_main.md
cmp "$EXTENSION_REPLAY_DIR/entity.md" \
  paper/tables/predicted_history_reference_absent_entity.md
```

The private registry contains row assignments and candidate phrases and must
remain owner-only. The analyzer emits aggregate-only, create-only outputs and
requires all four comparisons to match byte for byte.

Generate figures from the publication-safe aggregate CSVs:

```bash
uv run --script paper/figures/gen_fig_predicted_history_concepts.py
uv run --script paper/figures/gen_fig_predicted_history.py
uv run --script paper/figures/gen_fig_predicted_history_extension_v1.py
```

The existing generators validate the prediction-only task/matrix contract, the
original 15 main cells, and cross-file agreement. They write vector PDF,
300-DPI PNG, and hash receipts without opening raw audio, transcripts, or
predictions. The versioned extension generator reads only the checked aggregate
extension analysis and renders P0 as the P13 curve origin with R0 detached.
Its current generated data, three figures, and receipt passed a byte-identical
fresh-directory replay, hermetic tests, format/font checks, and visual
inspection.

The post-outcome breakdown performs no provider inference. Reproduction also
requires the same owner-only HMAC salt recorded by hash in the checked result:

```bash
mkdir -p experiments/results/reproductions paper/tables/reproductions
test ! -e experiments/results/reproductions/predicted_history_breakdown.json
test ! -e experiments/results/reproductions/predicted_history_breakdown.csv
test ! -e paper/tables/reproductions/predicted_history_breakdown.md
env PYTHONPATH=.:model/src uv run --project model --frozen python -m \
  experiments.scripts.predicted_history_breakdown \
  --protocol experiments/configs/predicted_history_breakdown_protocol.toml \
  --pseudonym-salt-env OWNER_ONLY_HMAC_SALT_FILE \
  --json-output experiments/results/reproductions/predicted_history_breakdown.json \
  --csv-output experiments/results/reproductions/predicted_history_breakdown.csv \
  --table-output paper/tables/reproductions/predicted_history_breakdown.md
cmp experiments/results/predicted_history_breakdown.json \
  experiments/results/reproductions/predicted_history_breakdown.json
cmp experiments/results/predicted_history_breakdown.csv \
  experiments/results/reproductions/predicted_history_breakdown.csv
cmp paper/tables/predicted_history_breakdown.md \
  paper/tables/reproductions/predicted_history_breakdown.md
```

## Evidence and operational accounting

The authenticated source of truth for the original program is
[`experiments/results/predicted_history_analysis.json`](../experiments/results/predicted_history_analysis.json),
with the tidy companion
[`predicted_history_analysis.csv`](../experiments/results/predicted_history_analysis.csv).
The additive source of truth is
[`predicted_history_extension_analysis.json`](../experiments/results/predicted_history_extension_analysis.json),
with its tidy
[`CSV`](../experiments/results/predicted_history_extension_analysis.csv).
Plans, execution receipts, operational telemetry, and stress receipts are
listed in the [appendix](appendix.md#i-artifacts-and-reproduction-commands).

The current program contains 73,944 requests in the 18-cell main matrix and
6,000 stress requests, for 79,944 registered requests. All 79,944 produced
successful terminal predictions, with no terminal provider errors. The two
extension studies recorded zero retries. In the earlier 66,120-request phase,
two additional transport attempts produced no terminal artifact after 900
seconds; they are excluded from scientific results and conservatively included
in whole-program cost estimates.

The common frozen request proxy estimates USD 25.605360872 for all 79,944
registered requests, or USD 25.606016939 with the two earlier orphan proxies.
Token-separated telemetry estimates USD 38.7240956521 for registered requests,
or USD 38.7247517191 with the orphan allowance. These are alternative
estimators, use an audio-token split proxy, and are not a reconciled provider
bill.

## Release boundaries and remaining blockers

No audio, reference, transcript, label, raw prediction, source locator, request
history, cloud resource identifier, signed URL, or private manifest is
release-authorized by this directory. The repository code license does not
establish redistribution rights for data, annotations, model outputs, or
third-party assets. Build any anonymous archive from an explicit governance
allowlist.

The draft's remaining submission gates are:

1. authorship and affiliation approval;
2. venue selection and current-format review; and
3. privacy, licensing, and release approval.

Held-out confirmation, repeated matched SFT jobs, and human audio adjudication
were not requested for this package. They remain explicit scientific
limitations: the paper must retain development-only wording, avoid causal SFT
claims, and avoid semantic-hallucination or acoustic-support claims.
