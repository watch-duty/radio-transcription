# Reviewer 2 self-critique

Date: 2026-07-13
Scope: publication-safe aggregate prediction-history evidence, including the fixed-interface K=0 extension, the reference-absent prediction lexical stress test, and the post-outcome descriptive breakdown

## Verdict

The study supports a useful but narrower paper than the motivating thesis. Under one fixed history interface, moving from zero prior turns (K=0) to a maximum of eight prior *model predictions* (K=8) lowers WER by 1.85 points for history-free SFT ([1.25, 2.42]; Holm-adjusted bootstrap tail fraction 0/10,000) and 1.76 points for prior-transcript SFT ([1.05, 2.46]; Holm 0/10,000). The difference between those improvements is +0.09 points ([−0.65, 0.85]), and the K=8 systems themselves differ by only +0.02 points ([−0.63, 0.70]). The historically prior-transcript-SFT system therefore has no supported differential WER benefit. It does have more K=8 lexical overlap: +1.44 context-copy points ([0.66, 2.25]) and +1.11 prior-only-token points ([0.64, 1.61]) relative to history-free SFT.

The reference-absent prediction intervention produces no supported WER change for any model variant. Candidate adoption rises by 0.80, 0.40, and 1.80 points for the untuned base model, history-free SFT, and prior-transcript SFT, but the corresponding three-variant Holm values are 0.45, 0.47, and 0.11. These are lexical diagnostics, not semantic findings. Overall, the evidence does not support a safer or more accurate learned context contract. It also does not prove that prior-transcript training is ineffective or semantically unsafe.

## Strongest alternative explanations

1. **Historical training confounding.** The two SFT jobs differ in corpus size, source mixture, labels, training prompt, serialization, temporal policy, and checkpoint selection. Their serving rows are paired, but training assignment is not controlled. A cross-variant gap or interaction cannot identify the effect of prior-transcript SFT.
2. **Endogenous histories.** Every model-variant/window cell rolls forward using its own predictions. Same-window model-variant contrasts therefore compare complete systems receiving different text, not two checkpoints conditioned on an identical history. This is deployment-realistic but weakens mechanism claims.
3. **Development-set reuse.** The 4,108-row lineage informed historical prompt and checkpoint decisions. Confidence intervals quantify sampling variation within this development frame; they do not repair selection bias or provide held-out confirmation. The fixed-interface and lexical-stress extensions were frozen before their own outcomes but after the parent study outcomes were known, so they remain post-outcome exploratory development evidence.
4. **Unresolved acoustic overlap.** Object-level provenance does not establish absence of near-duplicate audio, speakers, incidents, or conversations across training and evaluation. The hardened audit has no admissible corpus result because its authenticated lineage and calibration gates remain incomplete.
5. **Automatic lexical attribution.** Longer histories create more opportunities for phrase matches. The donor-null analysis reduces but cannot eliminate this ambiguity, and neither the ordinary overlap metrics nor the selected entity-like candidate determines acoustic support.

The K=0 extension resolves one earlier design defect: K=0 and K=1–K=8 now share the fixed history interface and its wrapper. K=0-to-K>0 is therefore the primary history-policy contrast. The no-history-interface prompt control must remain detached. K=0 minus the prompt control is +0.02 WER points for the untuned base model ([−0.44, 0.50]), +0.06 for history-free SFT ([−0.38, 0.48]), and −0.22 for prior-transcript SFT ([−0.76, 0.28]); these intervals do not prove equivalence.

## Leakage and safety audit

The core inference protocol does not serve reference transcripts. K=1/K=2/K=4/K=8 use only the same model variant and trajectory's strictly earlier usable predictions; K=0 and the no-history-interface prompt control have no history. References remain scoring-only until a trajectory is final. Historical reference-history artifacts are excluded from the paper's deployment evidence.

The second stress test uses a private offline reference check only to select a prediction-derived entity-like candidate absent from the current reference. The reference itself never enters a serving request or prior-history block. This is compatible with the deployment constraint but creates a lexical stress diagnostic, not a deployment policy.

Context-copy, prior-only-token, insertion, donor-null, and candidate-adoption measures cannot decide whether repeated content was audible, semantically unsupported, continued beyond the target span, or came from another speaker. The candidate-adoption result is especially easy to overstate: even reference-absent text may be audible if the label omitted it. The paper must not report a semantic-hallucination, acoustic-support, continuation, or target-boundary-violation rate.

## Extension and multiplicity audit

The primary fixed-interface quality population is the common 4,098 normalized-nonempty rows; all 4,108 rows remain in operational and abstention censuses. The K=0-to-K=1/K=2/K=4/K=8 comparisons use frozen four-member Holm families separately for each model variant and metric. The tuned-variant gap and interaction are descriptive and have no Holm value in this extension analysis; the paper must not attach the parent analysis's different family correction to them.

The entity-like intervention uses 498 rows for WER, CER, insertion, and overlap quality metrics and all 500 assigned rows for intent-to-treat candidate adoption. Each metric has a fixed three-variant Holm family. Every WER Holm value is 1.00. The prior-transcript-SFT adoption interval, +1.80 points [0.20, 3.90], excludes zero, but its Holm value is 0.11. The defensible wording is “low but nonzero point-estimate adoption, with no within-variant adoption contrast surviving the registered three-variant correction,” not “the prior-transcript model hallucinates entities.”

## Post-outcome breakdown audit

The duration, normalized reference-length, and HMAC-pseudonymized source-family breakdown was registered only after the primary outcomes were opened. It is properly labeled exploratory, launches no provider inference, and reaggregates exactly to the canonical no-history-interface prompt control/K=1/K=8 sufficient counts. Its 10,000-draw intervals are descriptive within-stratum intervals: there are no subgroup *p*-values, multiplicity decisions, or registered contrasts comparing one quartile's effect with another. A larger K=8-minus-K=1 point effect for the shortest history-free-SFT clips is therefore suggestive heterogeneity, not established effect modification.

The same limitation applies across the four pseudonymized source families. One prior-transcript-SFT family interval excludes zero, while three do not; this does not license family ranking, agency inference, or population generalization. The duration-Q3 quality–attribution pattern is a useful mixed result—lower WER alongside higher context-copy and prior-only-token proxies—but remains automatic and cannot be promoted to a semantic-safety conclusion.

## Claims that must remain weakened

- Say that self-predicted history improves WER for both tuned rolling systems *under fixed history interface*, not that prior-transcript SFT caused the gain.
- Make K=0-to-K>0 primary; keep no-history-interface prompt control detached from the fixed history interface window curve.
- Describe the two SFT model variants as historically unmatched end-to-end systems.
- Say there is no detected differential WER benefit for the prior-transcript-SFT system; do not claim equivalence.
- Say the learned-contract thesis is unsupported, not falsified in general.
- Say the reference-absent intervention measures literal lexical adoption, not acoustic support or semantic hallucination.
- Say unrelated predicted context harms both tuned systems on an enriched stress frame; do not claim one model variant is more robust because the interaction interval crosses zero.
- Call keyword scoring occurrence-weighted recall, not entity accuracy.
- Make no state-of-the-art, external-baseline, public-dataset, or semantic-safety claim.

The defensible novelty is the prediction-only rolling protocol, exact-interface history ablation, authenticated evaluation workflow, context-specific lexical diagnostics, and counterfactual predicted-history stress design. It is evaluation and data-flow design, not architectural novelty.

## Remaining submission-readiness blockers

Within the agreed development-paper scope, held-out inference and human listening are limitations rather than requested next experiments. The remaining blockers are packaging and governance:

The aggregate analysis and figures were regenerated from authenticated aggregate inputs; K=0 is the structural fixed-interface zero and the no-history-interface prompt control is visually detached. The abstract, tables, appendix, claims matrix, reproduction commands, and 30-cell evidence registry were reconciled. The sanitized aggregate package was explicitly authorized and published. Remaining gates require project-owner judgment on authorship, affiliation, and final submission privacy/licensing sign-off. Venue selection and current-format adaptation follow those decisions.

Held-out, source-separated replication would be required before generalizing beyond this development frame. Blinded listening would be required before making a semantic-support or boundary-error claim. Matched training jobs would be required for a causal claim about prior-transcript SFT. None is necessary to state the present narrower findings, and none should appear as the paper's requested next action.

## Exact next action

Obtain authorship, affiliation, and final submission privacy/licensing approval. Then freeze the public Markdown development-study package and select a venue without broadening its causal or semantic claims.

The quantitative basis for this critique is [`predicted_history_extension_analysis.json`](../experiments/results/predicted_history_extension_analysis.json), the generated [fixed-interface table](tables/predicted_history_extension_main.md), the generated [reference-absent lexical-stress table](tables/predicted_history_reference_absent_entity.md), and the post-outcome [`predicted_history_breakdown.json`](../experiments/results/predicted_history_breakdown.json), with claim wording governed by [`claims_evidence_matrix.csv`](claims_evidence_matrix.csv).
