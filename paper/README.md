# Context Is an Interface: Utility and Failure Modes of Self-Predicted Transcript History for Emergency-Radio ASR

This directory contains the canonical venue-neutral Markdown paper,
**“Context Is an Interface: Utility and Failure Modes of Self-Predicted Transcript History for
Emergency-Radio ASR.”**

The paper reports a prediction-only development study of Gemini 3.1 Flash-Lite.
Every main-matrix nonzero-history request uses only the same model variant and
trajectory's own strictly earlier usable predictions; stress requests use only
authenticated predictions from the same model variant. Reference transcripts never
enter model requests, rolling schedules, or stress histories. They are used for
scoring and, in the reference-absent lexical stress only, a private offline
candidate-absence check.

The completed main matrix evaluates three frozen model variants in 18 cells on
4,108 clips; 4,098 normalized-nonempty rows form the primary ASR population.
For each model variant, K=0 is the fixed-interface empty-history condition; a
distinct no-history-interface prompt control is reported separately and is not
a K condition. K=1/K=2/K=4/K=8 use rolling predicted history. A frozen 500-row stress frame adds unrelated, strictly older
same-source, shuffled, and reference-absent entity-bearing predicted histories;
498 normalized-nonempty rows support stress quality metrics.

The result is mixed and negative for the strong learned-contract thesis:

- Relative to fixed-interface K=0, K=8 improves WER by 1.85 points for history-free SFT
  (95% reduction interval [1.25, 2.42]) and 1.76 points for prior-transcript SFT
  ([1.05, 2.46]).
- History-free SFT and prior-transcript SFT reach 21.99% and 22.01% WER at K=8; the
  0.02-point gap has a 95% CI of [−0.63, 0.70].
- Prior-transcript SFT has higher K=8 lexical reference-proxy diagnostics: +1.44
  context-copy points among eligible-history rows and +1.11 prior-only points
  over all prediction tokens.
- Unrelated predicted context hurts both tuned variants, but the cross-SFT
  interaction is unsupported.
- In the reference-absent lexical stress, selected candidate phrases appear in
  0.8%, 0.4%, and 1.8% of base, history-free-SFT, and prior-transcript-SFT outputs;
  none of the three Holm-adjusted adoption contrasts is conclusive.

These findings do not establish semantic hallucination, acoustic support, or a
causal SFT effect. K=0 now separates the fixed-interface empty-history
condition from rolling history, but the SFT jobs remain historically unmatched, the frame is
development data, no held-out confirmation or human audio adjudication exists,
and the registered acoustic-overlap audit produced no admissible result.

## Canonical paper files

- [`main.md`](main.md): authoritative paper.
- [`appendix.md`](appendix.md): model lineage, prompts, prediction-only
  invariants, full metrics and contrasts, operational accounting, exclusions,
  reproduction commands, and submission gates.
- [`references.bib`](references.bib): machine-readable copy of the
  programmatically verified primary-source bibliography.
- [`claims_evidence_matrix.csv`](claims_evidence_matrix.csv): claim-level
  publication gate maintained by the broader research package.
- [`reviewer2.md`](reviewer2.md): publication-safe adversarial self-critique,
  including the post-outcome breakdown and submission gates.
- [`tables/predicted_history_extension_main.md`](tables/predicted_history_extension_main.md):
  generated 18-cell table with fixed-interface K=0 conditions, detached prompt controls, and paired contrasts.
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
  fixed-interface K=0/K=1/K=2/K=4/K=8 curves with no-history-interface prompt control detached.
- [`figures/fig_predicted_history_extension_matrix_v1.pdf`](figures/fig_predicted_history_extension_matrix_v1.pdf)
  and [PNG](figures/fig_predicted_history_extension_matrix_v1.png): generated
  18-cell serving matrix with fixed-interface cells and detached no-history-interface controls.
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

Superseded pre-extension figures and tables remain in internal history only.
They are excluded from the sanitized reviewer package because their detached
prompt control can be mistaken for K=0. `main.md` is the canonical paper; no
LaTeX or PDF paper is authoritative.

## Reproducing publication tables and figures

The public aggregate JSON files are the scientific source of truth. The
reviewer-facing table renderer changes labels only: it reads those aggregates,
does not open row-level predictions or references, and writes create-only
outputs to a fresh directory.

```bash
TABLE_REPLAY_DIR="$(mktemp -d)"
python3 paper/tables/render_public_tables.py \
  --output-dir "$TABLE_REPLAY_DIR"
for name in \
  predicted_history_stress.md \
  predicted_history_extension_main.md \
  predicted_history_reference_absent_entity.md \
  predicted_history_breakdown.md
do
  cmp "$TABLE_REPLAY_DIR/$name" "paper/tables/$name"
done
```

The original aggregate analyzers retain their immutable run IDs internally so
that execution-plan and result hashes remain valid. Those IDs are translated
only at this presentation boundary. Owner-only reanalysis from authenticated
predictions requires the ignored private binding, assignment registry, and
pseudonym salt; the exact commands and boundaries are recorded in
[`appendix.md`](appendix.md#i-artifacts-and-reproduction-commands).

Replay the figures from publication-safe aggregate inputs into fresh
directories and compare the bytes:

```bash
CONCEPT_REPLAY_DIR="$(mktemp -d)"
EXTENSION_FIGURE_REPLAY_DIR="$(mktemp -d)"
uv run --script paper/figures/gen_fig_predicted_history_concepts.py \
  --output-dir "$CONCEPT_REPLAY_DIR"
uv run --script paper/figures/gen_fig_predicted_history_extension_v1.py \
  --output-dir "$EXTENSION_FIGURE_REPLAY_DIR"
cmp "$CONCEPT_REPLAY_DIR/fig_task_contract.pdf" \
  paper/figures/fig_task_contract.pdf
cmp "$CONCEPT_REPLAY_DIR/fig_task_contract.png" \
  paper/figures/fig_task_contract.png
cmp "$CONCEPT_REPLAY_DIR/fig_predicted_history_concepts_receipt.json" \
  paper/figures/fig_predicted_history_concepts_receipt.json
for name in \
  fig_predicted_history_extension_exact_p13_v1.pdf \
  fig_predicted_history_extension_exact_p13_v1.png \
  fig_predicted_history_extension_matrix_v1.pdf \
  fig_predicted_history_extension_matrix_v1.png \
  fig_predicted_history_extension_tradeoff_v1.pdf \
  fig_predicted_history_extension_tradeoff_v1.png \
  fig_predicted_history_extension_v1_receipt.json
do
  cmp "$EXTENSION_FIGURE_REPLAY_DIR/$name" "paper/figures/$name"
done
```

The figure generators validate the prediction-only task contract, the complete
18-cell matrix, and cross-file agreement. They write vector PDF, 300-DPI PNG,
and hash receipts without opening raw audio, transcripts, or predictions. The
current bytes passed fresh-directory replay, focused tests, format checks, and
visual inspection.

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

The sanitized aggregate paper package was explicitly authorized and published
on [`paper/context-interface-public`](https://github.com/watch-duty/radio-transcription/tree/paper/context-interface-public).
That authorization excludes audio, references, transcripts, labels, raw
predictions, source locators, request histories, cloud resource identifiers,
signed URLs, and private manifests. The repository code license does not
establish redistribution rights for those withheld artifacts.

The draft's remaining submission gates are:

1. authorship and affiliation approval;
2. venue selection and current-format review; and
3. final privacy and licensing sign-off for submission.

Held-out confirmation, repeated matched SFT jobs, and human audio adjudication
were not requested for this package. They remain explicit scientific
limitations: the paper must retain development-only wording, avoid causal SFT
claims, and avoid semantic-hallucination or acoustic-support claims.
