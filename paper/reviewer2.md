# Reviewer 2 self-critique

Date: 2026-07-13
Scope: publication-safe aggregate prediction-history evidence, including the exact-P13 zero-history extension, the reference-absent prediction lexical stress test, and the post-outcome descriptive breakdown

## Verdict

The study supports a useful but narrower paper than the motivating thesis. Under one fixed P13 interface, moving from zero prior turns (P0) to a maximum of eight prior *model predictions* (R8) lowers WER by 1.85 points for no-context SFT ([1.25, 2.42]; Holm-adjusted bootstrap tail fraction 0.00) and 1.76 points for prior-context SFT ([1.05, 2.46]; Holm 0.00). The difference between those improvements is +0.09 points ([−0.65, 0.85]), and the R8 systems themselves differ by only +0.02 points ([−0.63, 0.70]). The historically prior-context-targeted system therefore has no supported differential WER benefit. It does have more R8 lexical overlap: +1.44 context-copy points ([0.66, 2.25]) and +1.11 prior-only-token points ([0.64, 1.61]) relative to no-context SFT.

The reference-absent prediction intervention produces no supported WER change for any target. Candidate adoption rises by 0.80, 0.40, and 1.80 points for base, no-context SFT, and prior-context SFT, but the corresponding three-target Holm values are 0.45, 0.47, and 0.11. These are lexical diagnostics, not semantic findings. Overall, the evidence does not support a safer or more accurate learned context contract. It also does not prove that prior-context training is ineffective or semantically unsafe.

## Strongest alternative explanations

1. **Historical training confounding.** The two SFT jobs differ in corpus size, source mixture, labels, training prompt, serialization, temporal policy, and checkpoint selection. Their serving rows are paired, but training assignment is not controlled. A cross-target gap or interaction cannot identify the effect of prior-context SFT.
2. **Endogenous histories.** Every target/window cell rolls forward using its own predictions. Same-window target contrasts therefore compare complete systems receiving different text, not two checkpoints conditioned on an identical history. This is deployment-realistic but weakens mechanism claims.
3. **Development-set reuse.** The 4,108-row lineage informed historical prompt and checkpoint decisions. Confidence intervals quantify sampling variation within this development frame; they do not repair selection bias or provide held-out confirmation. The exact-P13 and lexical-stress extensions were frozen before their own outcomes but after the parent study outcomes were known, so they remain post-outcome exploratory development evidence.
4. **Unresolved acoustic overlap.** Object-level provenance does not establish absence of near-duplicate audio, speakers, incidents, or conversations across training and evaluation. The hardened audit has no admissible corpus result because its authenticated lineage and calibration gates remain incomplete.
5. **Automatic lexical attribution.** Longer histories create more opportunities for phrase matches. The donor-null analysis reduces but cannot eliminate this ambiguity, and neither the ordinary overlap metrics nor the selected entity-like candidate determines acoustic support.

The exact-P13 P0 extension resolves one earlier design defect: P0 and R1–R8 now share P13 and its wrapper. P0-to-Rk is therefore the primary history-policy contrast. Option-D R0 must remain detached as a prompt/interface control. P0−R0 WER is +0.02 points for base ([−0.44, 0.50]), +0.06 for no-context SFT ([−0.38, 0.48]), and −0.22 for prior-context SFT ([−0.76, 0.28]); these intervals do not prove equivalence.

## Leakage and safety audit

The core inference protocol does not serve reference transcripts. R1/R2/R4/R8 use only the same target and trajectory's strictly earlier usable predictions; P0 and R0 have no history. References remain scoring-only until a trajectory is final. Historical reference-history artifacts are excluded from the paper's deployment evidence.

The second stress test uses a private offline reference check only to select a prediction-derived entity-like candidate absent from the current reference. The reference itself never enters a serving request or prior-history block. This is compatible with the deployment constraint but creates a lexical stress diagnostic, not a deployment policy.

Context-copy, prior-only-token, insertion, donor-null, and candidate-adoption measures cannot decide whether repeated content was audible, semantically unsupported, continued beyond the target span, or came from another speaker. The candidate-adoption result is especially easy to overstate: even reference-absent text may be audible if the label omitted it. The paper must not report a semantic-hallucination, acoustic-support, continuation, or target-boundary-violation rate.

## Extension and multiplicity audit

The primary exact-P13 quality population is the common 4,098 normalized-nonempty rows; all 4,108 rows remain in operational and abstention censuses. The P0-to-R1/R2/R4/R8 comparisons use frozen four-member Holm families separately for each target and metric. The tuned-target gap and interaction are descriptive and have no Holm value in this extension analysis; the paper must not attach the parent analysis's different family correction to them.

The entity-like intervention uses 498 rows for WER, CER, insertion, and overlap quality metrics and all 500 assigned rows for intent-to-treat candidate adoption. Each metric has a fixed three-target Holm family. Every WER Holm value is 1.00. The prior-context-SFT adoption interval, +1.80 points [0.20, 3.90], excludes zero, but its Holm value is 0.11. The defensible wording is “low but nonzero point-estimate adoption, with no within-target adoption contrast surviving the registered three-target correction,” not “the prior-context model hallucinates entities.”

## Post-outcome breakdown audit

The duration, normalized reference-length, and HMAC-pseudonymized source-family breakdown was registered only after the primary outcomes were opened. It is properly labeled exploratory, launches no provider inference, and reaggregates exactly to the canonical R0/R1/R8 sufficient counts. Its 10,000-draw intervals are descriptive within-stratum intervals: there are no subgroup *p*-values, multiplicity decisions, or registered contrasts comparing one quartile's effect with another. A larger R8-minus-R1 point effect for the shortest no-context-SFT clips is therefore suggestive heterogeneity, not established effect modification.

The same limitation applies across the four pseudonymized source families. One prior-context-SFT family interval excludes zero, while three do not; this does not license family ranking, agency inference, or population generalization. The duration-Q3 quality–attribution pattern is a useful mixed result—lower WER alongside higher context-copy and prior-only-token proxies—but remains automatic and cannot be promoted to a semantic-safety conclusion.

## Claims that must remain weakened

- Say that self-predicted history improves WER for both tuned rolling systems *under P13*, not that prior-context SFT caused the gain.
- Make P0-to-Rk primary; keep Option-D R0 detached from the P13 window curve.
- Describe the two SFT targets as historically unmatched end-to-end systems.
- Say there is no detected differential WER benefit for the prior-context-targeted system; do not claim equivalence.
- Say the learned-contract thesis is unsupported, not falsified in general.
- Say the reference-absent intervention measures literal lexical adoption, not acoustic support or semantic hallucination.
- Say unrelated predicted context harms both tuned systems on an enriched stress frame; do not claim one target is more robust because the interaction interval crosses zero.
- Call keyword scoring occurrence-weighted recall, not entity accuracy.
- Make no state-of-the-art, external-baseline, public-dataset, or semantic-safety claim.

The defensible novelty is the prediction-only rolling protocol, exact-interface history ablation, authenticated evaluation workflow, context-specific lexical diagnostics, and counterfactual predicted-history stress design. It is evaluation and data-flow design, not architectural novelty.

## Remaining submission-readiness blockers

Within the agreed development-paper scope, held-out inference and human listening are limitations rather than requested next experiments. The remaining blockers are packaging and governance:

The aggregate analysis and figures were regenerated byte-for-byte; P0 is now the structural P13 zero and R0 is visually detached. The abstract, tables, appendix, claims matrix, reproduction commands, and candidate artifact registry were reconciled, and the final automated publication-safety scan passed. The remaining gates require project-owner judgment: authorship and affiliation approval, privacy and licensing approval, and explicit authorization of the public-release allowlist. Venue selection and current-format adaptation follow those decisions.

Held-out, source-separated replication would be required before generalizing beyond this development frame. Blinded listening would be required before making a semantic-support or boundary-error claim. Matched training jobs would be required for a causal claim about prior-context SFT. None is necessary to state the present narrower findings, and none should appear as the paper's requested next action.

## Exact next action

Obtain project-owner approval of authorship, privacy, licensing, and the aggregate-only release allowlist. Then freeze this Markdown development-study package and select a venue without broadening its causal or semantic claims.

The quantitative basis for this critique is [`predicted_history_extension_analysis.json`](../experiments/results/predicted_history_extension_analysis.json), the generated [exact-P13 table](tables/predicted_history_extension_main.md), the generated [reference-absent lexical-stress table](tables/predicted_history_reference_absent_entity.md), and the post-outcome [`predicted_history_breakdown.json`](../experiments/results/predicted_history_breakdown.json), with claim wording governed by [`claims_evidence_matrix.csv`](claims_evidence_matrix.csv).
