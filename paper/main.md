# Context Is an Interface: Utility and Failure Modes of Prior Transcripts for Emergency-Radio ASR

*Authors withheld pending internal governance review. Venue-neutral development-study draft.*

## Abstract

In deployed automatic speech recognition (ASR), earlier reference transcripts do not exist: a recognizer can condition only on its own earlier outputs. We evaluate that setting for emergency-radio audio with Gemini 3.1 Flash-Lite and two historical supervised-fine-tuning (SFT) targets, one trained without a history interface and one trained with prior-transcript blocks. The study processes 4,108 development clips (4,098 with nonempty normalized references) in independent rolling trajectories under one fixed P13 serving interface. Relative to exact-P13 zero history (P0), an eight-turn self-predicted-history policy (R8) lowers WER by 1.85 points for no-context SFT (95% CI [1.25, 2.42]; Holm-adjusted bootstrap tail fraction 0.00) and 1.76 points for prior-context SFT ([1.05, 2.46]; Holm 0.00). The difference between those WER improvements is only 0.09 points ([−0.65, 0.85]), and the R8 systems themselves reach 21.99% and 22.01% WER, respectively. The prior-context-targeted system instead has higher R8 lexical reference-proxy diagnostics: +1.44 context-copy points among eligible-history rows ([0.66, 2.25]) and +1.11 prior-only points over all prediction tokens ([0.64, 1.61]). A prediction-derived, reference-absent entity-like stress intervention changes WER by 0.00, +0.36, and +0.52 points for the base, no-context-SFT, and prior-context-SFT targets, with every interval crossing zero and every Holm-adjusted tail fraction equal to 1.00. Candidate adoption rises by 0.80, 0.40, and 1.80 points; the corresponding Holm values are 0.45, 0.47, and 0.11. These are lexical diagnostics, not evidence of acoustic support or semantic hallucination. The study supports a narrow conclusion: predicted history helps both tuned rolling systems under P13, but it provides no detected differential WER benefit for the historically prior-context-targeted system and coincides with more lexical overlap. Historically unmatched training jobs, endogenous histories, development-set reuse, and absent listening adjudication preclude causal SFT and semantic-safety claims.

## 1. Introduction

Emergency-radio transcription combines short, narrowband, noisy clips with operationally important names, unit identifiers, locations, and codes. Such details often recur across adjacent transmissions. Police-radio ASR research documents specialized language, short turns, and annotation disagreement [@srivastava2024police], while public-safety ASR work emphasizes operational extraction of locations and keywords [@gartner2025publicsafety]. Air-traffic-control corpora present related channel and terminology constraints [@zuluaga2024atco2]. An earlier transcript may therefore resolve a plausible written form that the current audio alone leaves ambiguous.

The same text creates a failure channel. A generative recognizer can copy a plausible unit or location that is not part of the current transmission, continue an earlier turn, or let history determine output length. This risk differs from ordinary contextual biasing, where a recognizer receives a constrained phrase list. Free-text history contains complete utterances and, in deployment, upstream ASR errors.

This paper asks a deployment-specific question: **can predicted transcript history improve the current transcript without giving prior text inappropriate authority?** The deployment constraint is fundamental. The current reference is used only for scoring after inference. It is never an input, never a history donor, and never available to the rolling schedule. Every main-matrix nonzero-history trajectory consumes only the same target's own strictly earlier usable predictions.

The motivating hypothesis treats context as a learned interface. The current audio should be the only authority for transcript content and span; prior text should assist only the written form of something already supported by that audio. A historical SFT job attempted to encode this rule with up to eight transcript-only prior turns, a current-audio part, and a target containing only the current transcript. We compare that target with a historical domain SFT target trained without a history interface.

The evidence is mixed and does not support the training hypothesis. Under fixed P13 serving, R8 improves WER over exact-P13 zero history for both tuned targets. The prior-context-targeted system has no supported R8 WER advantage and no larger P0-to-R8 WER response. That system instead has higher history-overlapping lexical reference-proxy diagnostics. A reference-absent, prediction-derived entity-like intervention produces small candidate-adoption rates but no supported WER change. These automatic results characterize lexical sensitivity; they do not determine whether any output is acoustically unsupported.

This study contributes:

1. A prediction-only evaluation protocol that prevents reference, future, cross-split, and cross-trajectory text from entering a serving request.
2. A complete three-target, five-window exact-P13 development matrix, a detached Option-D control, and frozen predicted-context stress tests with paired source-clustered uncertainty.
3. A failure to support the strong learned-contract thesis: the historically prior-context-targeted rolling system has no detected WER advantage and has higher lexical reference-proxy diagnostics under eight-turn serving.
4. An integrity-oriented reproduction package that authenticates rolling request histories, provider artifacts, operational telemetry, and generated aggregate tables and figures.

The contribution is evaluation and evidence, not architectural novelty. We make no state-of-the-art or external-baseline claim.

## 2. Related work

### 2.1 Contextual and cross-utterance ASR

Dedicated contextual ASR uses phrase lists, bias encoders, or difficult negative examples to improve rare-word recognition while controlling over-biasing [@pundak2018deep; @alon2019difficult]. Long-context and conversational systems instead propagate information across utterances, including acoustic representations and imperfect recognition histories [@hori2021longcontext; @wei2022crossmodal; @lee2024robustcontext]. PromptASR explicitly compares preceding ground-truth utterances with previous decoding results [@yang2024promptasr], and retrieval-augmented ASR studies the selection of useful external text [@siskos2025retrieval]. Our setting differs in two ways: the context consists of unconstrained radio transcripts, and only earlier model predictions are admissible at inference.

### 2.2 Audio-language models and adaptation

Speech-augmented language models expose transcription through a generative text interface [@chen2024salm; @tang2024salmonn], while LoRA and speech-specific adaptations provide parameter-efficient specialization [@hu2022lora; @song2024lorawhisper]. Contextualization through speech-language models can improve bias terms but degrade as distracting candidates increase [@lakomkin2024contextualization; @gong2024contextual]. Gemini provides the common model family and managed SFT mechanism used here [@geminiteam2024gemini; @googlecloud2026gemini31flashlite; @googlecloud2026geminisft]. We evaluate historical target bundles rather than introduce a new adapter or architecture.

### 2.3 Predicted-history mismatch and unsupported output

Training/serving skew is a general source of behavioral change [@breck2019data]. In contextual speech-language models, exposure to clean training history can create a clean-to-predicted-history mismatch; recent work directly studies noisy-history training and irrelevant-history attacks [@guo2026oraclenoisy]. Our evaluation removes clean history from serving altogether and measures each target's own autoregressive error propagation.

Generative ASR can emit fluent content unsupported by audio, especially under non-speech or difficult acoustic conditions [@koenecke2024careless; @baranski2025nonspeech], and audio-language models can report absent sound objects [@kuan2024objecthallucination]. Ordinary insertions, textual overlap, and semantic fabrication are not equivalent [@frieske2024hallucinations]. We therefore call our automatic overlap measures **lexical reference-proxy diagnostics**. They screen dependence on supplied text; they do not establish semantic hallucination, acoustic support, continuation, or a target-boundary violation.

## 3. Predicted history as a serving interface

Let \(x_t\) be the current audio, \(y_t\) its scoring reference, and \(\hat y_t^{(k)}\) the prediction from a trajectory with maximum history window \(k\). For a fixed target and window, the rolling history is

\[
h_t^{(k)} = \operatorname{tail}_k\!\left(\hat y_i^{(k)} : s_i=s_t,\ e_i\le b_t,\ i<t,\ \hat y_i^{(k)}\ \text{usable}\right),
\]

where \(s\) is a source key, \(e_i\) is the earlier clip's end, and \(b_t\) is the current clip's start. Stable identifiers break timestamp ties. “Usable” excludes missing output, provider failure, blank text, and exact `[UNINTELLIGIBLE]`. An unusable frozen slot is omitted without looking for a replacement. The model then produces

\[
\hat y_t^{(k)} \sim p_\theta(\cdot\mid x_t,h_t^{(k)}).
\]

The schedule depends only on split, source, timing, duration, and audio identity. Reference text is held in a scoring-only structure and joined only after an entire cell is final. Thus \(y_i\) never appears in \(h_t^{(k)}\). Each target/window combination is an independent rolling trajectory, so a prediction from one target or window cannot affect another.

The desired interface gives audio *content authority*. Predicted history may assist the spelling of an acoustically supported name, identifier, location, or technical term, but it should not supply words, determine transcript length, or trigger continuation. This rule is semantic. A token-level comparison with \(y_t\) cannot determine whether a repeated word was genuinely audible.

![Desired prediction-only serving contract—not an enforced or demonstrated property. The current audio is intended to control content and span; same-target rolling model predictions may assist written form. The current reference is joined only after inference and cannot enter the loop.](figures/fig_task_contract.png)

*[Vector version](figures/fig_task_contract.pdf); [generated figure contract](figures/data/predicted_history_concepts.json).*

### Historical target bundles

All targets derive from the Gemini 3.1 Flash-Lite family. The no-context SFT checkpoint used 34,609 training rows (22.42 hours), adapter size 16, learning-rate multiplier 0.5, and epoch 6/step 1665. The prior-context SFT checkpoint used 16,919 rows (9.57 hours), adapter size 16, multiplier 0.75, and epoch 7/step 2538. Its training examples contained a numbered block of zero to eight earlier labeled transcript texts and only the current audio; the target contained only the current transcript. Those training histories were not rolling predictions, so the present prediction-only evaluation still introduces a clean-to-predicted-history exposure shift.

The base target is a floating publisher alias. An observed response-version-set fingerprint authenticates the execution reported here, but the alias is not an immutable model resource and cannot guarantee an identical future rerun.

These jobs do **not** form a controlled context-only training intervention. They differ in corpus size and source mixture, 58 shared-span labels, prompt, input serialization, and temporal construction. The historical prior-context builder also used start-time ordering, whereas this study requires prior audio to end before the current clip begins. Provider-managed training randomness and several optimizer details are unavailable. Cross-target gaps and interactions below therefore describe two bundled historical systems; they cannot identify the causal effect of prior-context SFT.

## 4. Data and experimental design

### 4.1 Development frame

The exact unmasked materialization used by the historical prior-context evaluation contains 4,108 clips from 615 source clusters and four source families, totaling 8,611.569 seconds (2.39 hours). Historical prompt and checkpoint decisions used this lineage, so every result is a **development-set estimate**, not held-out confirmation. The primary ASR population contains the common 4,098 rows whose references remain nonempty after canonical normalization. The ten normalized-empty rows remain in the 4,108-row provider, insertion-sensitivity, abstention, error, and latency censuses but never enter corpus WER or CER.

The study authenticates source-object generation, byte size, content digest, row census, target bindings, prompt hashes, and finalized prediction artifacts. These checks do not establish acoustic near-duplicate, incident, speaker, or conversation disjointness from training. A registered acoustic-overlap audit did not produce an admissible result; no acoustic-separation claim is made.

### 4.2 Main window matrix

We evaluate the base publisher target, no-context SFT, and prior-context SFT under P13 at maximum windows \(k\in\{0,1,2,4,8\}\), producing 15 primary cells. P0 is the structural no-history condition: it renders the exact P13 prompt and wrapper but supplies zero prior turns and only the current audio. R1/R2/R4/R8 use the same interface with each trajectory's own earlier predictions. Three additional R0 cells use the frozen context-free “Option D” forensic prompt and current audio only; they are detached prompt/interface controls, not points on the P13 window curve. The resulting 18-cell matrix contains 73,944 registered requests. Temperature is 0 and the output limit is 512 tokens.

This distinction creates an important estimand boundary:

- **Rk−P0** holds the P13 prompt and serialization fixed and estimates the total effect of a self-predicted-history rolling policy with maximum window \(k\). Because the trajectory changes autoregressively, this is not a one-request direct effect.
- **Adjacent P0/R1/R2/R4/R8 contrasts** hold P13 fixed and show where the window-response curve changes.
- **P0−R0** is a prompt/interface contrast at zero history. It is not part of the P13 history curve.
- **Prior-context SFT minus no-context SFT** is a descriptive target gap between historically unmatched jobs.
- **Differences of target gaps** are descriptive interactions, not causal training effects.

The structural schedule contains eligible dependencies for 3,492 rows. Actual supplied history varies by target because a target's earlier unusable outputs leave slots empty. At R8, 3,449 base rows, 3,491 no-context-SFT rows, and 3,490 prior-context-SFT rows receive at least one prediction.

### 4.3 Counterfactual stress frame

Before stress inference, a deterministic seed selected 500 structurally eligible targets from 47 source clusters and three source families without consulting references or model outcomes. The condition named `correct_r8` uses each target's authenticated same-source rolling predictions; “correct” denotes donor provenance and order, not corrected or reference text. Three interventions keep the current audio, target, prompt, and reference fixed:

1. **Unrelated predicted:** plausible predictions from another source family, matched to the frozen history structure. Independent recordings have no common clock, so this is a noncausal foreign-context perturbation, not valid deployment history.
2. **Stale same-source predicted:** up to eight strictly older, nonadjacent prediction slots from the same source, ending before the earliest same-source R8 turn; unusable predictions remain omitted.
3. **Shuffled same-source predicted:** the same-source R8 turns with their order perturbed while preserving lexical inventory.

Same-source R8 and R0 controls reuse exact main-matrix subsets. The nine new condition/target cells add 4,500 requests. Two stress references normalize to empty, leaving 498 primary quality rows; all 500 remain in operational summaries. Stress prevalence is artificial and does not estimate deployment frequency.

A second, post-outcome exploratory stress intervention uses a fixed 500-row intent-to-treat frame. For each target, it supplies prediction-derived text containing an entity-like candidate that a private offline reference check establishes is absent from the current reference. References remain unavailable to request construction and are used only for scoring and that absence check. The intervention adds 1,500 requests. Candidate adoption is evaluated on all 500 rows; WER and other reference-quality metrics use the 498 normalized-nonempty rows. This construction tests lexical uptake under an enriched intervention, not the deployment prevalence of such candidates or whether an adopted candidate is unsupported by the audio.

### 4.4 Metrics and statistical analysis

Standard metrics are corpus WER and CER, substitutions, deletions, insertions, insertions per 100 reference words, and occurrence-weighted keyword recall. Blank or exact `[UNINTELLIGIBLE]` output is abstention; provider failure and missing output are separate.

A **context-copy event** occurs when a predicted contiguous three-token phrase appears within at least one supplied prior turn but not in the current reference. No n-gram crosses a turn boundary. For a nonzero cell, the event rate divides by that cell's successful rows with nonempty tokenized history; rows without realized history enter neither numerator nor denominator. A wholly history-free P0 or R0 cell has copy metrics set to zero by construction rather than reported as an eligible-row ratio; only P0 is the structural zero of the P13 curve. At R8, the tuned-target denominators are 3,481 rows for no-context SFT and 3,480 for prior-context SFT. The **prior-only-token rate** instead divides attributable token multiplicity by all prediction tokens. We also compute distinct-history-trigram yield, a deterministic opportunity-matched donor null, and matched excess copy. Longer windows contain more possible matches, so raw copy rates combine model behavior with opportunity-set size. None of these metrics measures acoustic support.

For the second stress intervention, **candidate adoption** is the fraction of all 500 assigned rows whose output contains the selected reference-absent entity-like candidate. Blank, error, and exact `[UNINTELLIGIBLE]` outputs count as non-adoption. This intent-to-treat lexical outcome does not determine whether the candidate was audible or semantically unsupported.

The analysis uses 10,000 paired hierarchical bootstrap draws with fixed seed 20260711, resampling source clusters and then rows. Main and stress populations use separate joint draw groups. We report 95% percentile intervals and absolute effects. For the exact-P13 curve, Holm correction is applied across the four P0-to-Rk contrasts separately for each target and metric; adjacent-window contrasts use separate four-member families. The entity-like stress uses three-target families separately for each metric. Adjusted bootstrap tail fractions are secondary descriptive quantities, not calibrated parametric *p*-values [@holm1979simple]. Target gaps and tuned-target interactions reported below are descriptive and have no Holm value in this extension analysis. The cluster-aware design follows established ASR and hierarchical-bootstrap practice [@bisani2004bootstrap; @field2007bootstrapping; @ren2010hierarchical].

## 5. Results

### 5.1 Predicted history lowers WER for both tuned systems under fixed P13

All cell estimates and paired effects in this table use the common 4,098-row primary quality population.

![Matched serving matrix. The 15 exact-P13 cells use zero or rolling predicted history; the three Option-D R0 cells are detached prompt/interface controls. Cells report WER with source-clustered 95% intervals.](figures/fig_predicted_history_extension_matrix_v1.png)

*[Vector version](figures/fig_predicted_history_extension_matrix_v1.pdf); [generated aggregate data](figures/data/predicted_history_extension_v1.csv).*

| Target | P0 WER | R1 WER | R2 WER | R4 WER | R8 WER | R8−P0, points [95% CI] | Holm tail fraction |
|---|---:|---:|---:|---:|---:|---:|---:|
| Base | 32.56 | 33.43 | 33.51 | 32.48 | 32.14 | −0.42 [−1.35, 0.66] | 0.83 |
| No-context SFT | 23.85 | 23.34 | 22.89 | 22.30 | **21.99** | **−1.85 [−2.42, −1.25]** | **0.00** |
| Prior-context SFT | 23.78 | 23.00 | 22.58 | 22.14 | **22.01** | **−1.76 [−2.46, −1.05]** | **0.00** |

P0 and R1–R8 use the same P13 prompt and serialization. Relative to P0, R8 lowers WER by 7.78% for no-context SFT and 7.41% for prior-context SFT. Both tuned point-estimate curves improve monotonically as the permitted history grows. The base curve does not: R1 and R2 are worse than P0, and its R8−P0 interval crosses zero. These are end-to-end rolling-policy effects; each window generates the predictions that later requests in its own trajectory consume.

![WER versus maximum rolling-prediction history under the exact-P13 interface. Option-D R0 is shown only in a detached control panel and is not a point on the P13 curve. Bars are source-clustered 95% intervals.](figures/fig_predicted_history_extension_exact_p13_v1.png)

*[Vector version](figures/fig_predicted_history_extension_exact_p13_v1.pdf); [generated aggregate data](figures/data/predicted_history_extension_v1.csv).*

For the first predicted turn, R1−P0 is −0.51 points for no-context SFT ([−1.01, 0.02]; Holm 0.0592) and −0.78 for prior-context SFT ([−1.27, −0.28]; Holm 0.0014). Their descriptive interaction is −0.27 points ([−0.90, 0.35]), so the point-estimate difference does not support a differential first-turn response. The gains accumulate by R8. The complete exact-P13 cells, paired effects, sample sizes, intervals, and registered Holm values are in the generated [extension table](tables/predicted_history_extension_main.md).

Option-D R0 is only a detached interface check. P0−R0 WER is +0.02 points for base ([−0.44, 0.50]), +0.06 for no-context SFT ([−0.38, 0.48]), and −0.22 for prior-context SFT ([−0.76, 0.28]). No Holm adjustment was registered for these three prompt/interface contrasts. Their intervals do not establish prompt equivalence, but their small point estimates show that the primary P0-to-R8 result is not numerically driven by a large zero-history prompt gap.

### 5.2 Prior-context SFT has no supported R8 WER advantage

At R8, the tuned rolling systems have nearly identical WER point estimates, and the interval supports neither system over the other. Each system supplies its own endogenous prior predictions, so this is a contrast between complete rolling systems rather than checkpoints conditioned on identical text. Their lexical reference-proxy behavior differs.

| R8 metric | No-context SFT | Prior-context SFT | Prior − no-context [95% CI] |
|---|---:|---:|---:|
| WER, % | 21.99 | 22.01 | +0.02 [−0.63, 0.70] |
| CER, % | 15.05 | 15.15 | +0.09 [−0.41, 0.62] |
| 3-token context-copy event proxy, % eligible-history rows | 2.96 | 4.40 | **+1.44 [0.66, 2.25]** |
| Prior-only tokens, % prediction tokens | 3.72 | 4.84 | **+1.11 [0.64, 1.61]** |
| Insertions / 100 reference words | 3.18 | 3.77 | **+0.58 [0.26, 0.90]** |

The differential P0-to-R8 WER response is +0.09 points for prior-context SFT relative to no-context SFT ([−0.65, 0.85]): the interval supports no larger WER benefit for either historical target. The context-copy gap remains after subtracting the opportunity-matched donor null: matched excess copy is 2.86% for no-context SFT and 4.33% for prior-context SFT, a +1.47-point gap ([0.69, 2.29]). Because the extension's registered Holm families cover within-target window contrasts rather than target gaps or tuned-target interactions, no Holm value is attached to these descriptive comparisons.

The study does not support the strong learned-contract hypothesis: no WER advantage is detected, while lexical reference-proxy diagnostics move in the opposite direction. Semantic safety remains unmeasured. The result also does not show that prior-context training causes the lexical differences: the targets are historically unmatched, their endogenous histories differ, and both diagnostics use the reference as a proxy rather than the audio as evidence.

![Quality–lexical-overlap trade-off under the fixed P13 interface. Copy rates use target/window-specific successful rows with nonempty tokenized history. Labels give P0 or the maximum prior-prediction window; horizontal and vertical bars are source-clustered 95% intervals. Option-D R0 is excluded.](figures/fig_predicted_history_extension_tradeoff_v1.png)

*[Vector version](figures/fig_predicted_history_extension_tradeoff_v1.pdf); [generated aggregate data](figures/data/predicted_history_extension_v1.csv).*

The complete 18-cell estimates, including P0, detached R0, CER, insertion rate, and prior-only-token rate, are in the generated [extension table](tables/predicted_history_extension_main.md).

### 5.3 Unrelated predicted context harms both tuned targets

| Target | Same-source R8 WER | Unrelated WER | Unrelated − same-source [95% CI] | Holm tail fraction |
|---|---:|---:|---:|---:|
| No-context SFT | 25.15 | 28.07 | **+2.92 [1.05, 4.99]** | 0.0114 |
| Prior-context SFT | 24.93 | 29.89 | **+4.96 [2.74, 7.10]** | 0.0000 (empirical) |

Foreign plausible predictions worsen WER relative to same-source ordered R8 predictions on this constructed frame. The cross-SFT interaction is +2.04 points, but its interval crosses zero ([−0.73, 4.63]) and the Holm-adjusted tail fraction is 0.410. Because the systems generate different donor predictions, this interaction is an end-to-end system contrast. The evidence supports harm within each tuned system, not a difference between them.

Neither stale nor shuffled predicted history has a supported WER effect relative to same-source R8 for either tuned target. Stale-history effects are +0.06 points ([−1.48, 1.66]) and +1.46 ([−0.74, 3.43]); shuffled-history effects are +0.55 ([−0.81, 1.97]) and +0.65 ([−0.83, 1.86]). The 498-row primary stress population is too small to interpret these intervals as evidence of equivalence.

Copy-event rates actually decrease under unrelated context relative to same-source ordered R8 in point estimate, while WER rises. This divergence is expected: foreign text has fewer phrases in common with the current reference even when it distracts the model. It also demonstrates why the lexical diagnostics cannot substitute for semantic review. The complete estimates are in the generated [stress table](tables/predicted_history_stress.md).

### 5.4 Reference-absent prediction text produces limited lexical adoption

| Target | Intervention − same-source R8 WER [95% CI] | Holm | Candidate-adoption change [95% CI] | Holm |
|---|---:|---:|---:|---:|
| Base | +0.00 [−1.95, 1.89] | 1.00 | +0.80 [0.00, 2.15] | 0.45 |
| No-context SFT | +0.36 [−1.12, 1.78] | 1.00 | +0.40 [0.00, 1.37] | 0.47 |
| Prior-context SFT | +0.52 [−1.60, 2.16] | 1.00 | +1.80 [0.20, 3.90] | 0.11 |

On the 498-row quality population, the reference-absent entity-like intervention has no supported WER effect for any target. On the 500-row intent-to-treat population, the selected candidate appears in 0.80%, 0.40%, and 1.80% of intervention outputs and in none of the same-source R8 controls. The prior-context-SFT percentile interval excludes zero, but its three-target Holm-adjusted tail fraction is 0.11; the registered multiplicity analysis therefore does not support singling out that target. These rates measure literal adoption of prediction-derived text selected to be absent from the reference. They neither establish that the adopted candidate was absent from the audio nor estimate semantic hallucination. The [generated lexical-stress table](tables/predicted_history_reference_absent_entity.md) reports WER, CER, insertions, adoption, and all registered uncertainty.

### 5.5 Post-outcome descriptive heterogeneity

After the primary results were opened, we registered a separate exploratory breakdown of Option-D R0, P13 R1, and P13 R8. It predates the P0 extension, reuses the authenticated 4,098-row panel, and adds no provider calls. Duration quartiles use cut points 0.863, 1.495, and 2.653 seconds; normalized reference-length buckets use 3, 5, and 9 words. Intervals are descriptive source-cluster-then-row percentiles with no subgroup hypothesis tests or multiplicity claims. Its R0 strata remain detached interface descriptions; only R8−R1 holds P13 fixed.

The shortest clips remain the most difficult. At R0, duration-Q1 versus Q4 WER is 39.43% versus 17.07% for no-context SFT and 36.73% versus 17.44% for prior-context SFT. Under fixed P13, no-context-SFT R8−R1 is −4.34 points ([−8.24, −0.99]) in duration Q1 and −0.48 ([−1.04, 0.09]) in Q4; the corresponding prior-context-SFT effects are −2.13 ([−4.80, 0.35]) and −0.50 ([−1.13, 0.17]). The fewest-word bucket has the same descriptive pattern: −4.62 points ([−8.46, −1.42]) and −2.18 ([−4.79, 0.35]) for the two tuned targets. These within-stratum intervals do not test whether the short- and long-clip effects differ.

The breakdown also exposes a quality–attribution trade-off. For prior-context SFT in duration Q3, R8−R1 lowers WER by 1.30 points ([−2.42, −0.16]) while increasing the context-copy proxy by 3.83 points ([2.02, 5.81]) and the prior-only-token proxy by 3.88 ([2.78, 5.02]). In Q4, the WER interval includes zero while the lexical proxies still increase. Across four HMAC-pseudonymized source families, the prior-context-SFT WER interval excludes zero in only one family; no family ranking or population generalization is warranted. The [complete generated breakdown](tables/predicted_history_breakdown.md) and [machine-readable result](../experiments/results/predicted_history_breakdown.json) report all strata and proxy denominators.

### 5.6 Standard and operational metrics

At R8, no-context SFT reaches 15.05% CER and prior-context SFT reaches 15.15%. Occurrence-weighted keyword recall is 81.01% and 85.95%, respectively, but this metric does not penalize false-positive mentions and is not entity accuracy. Reviewed unit-ID, location, and operational-code labels are unavailable. The base target remains substantially worse than either tuned target across all windows.

All 79,944 registered study requests completed successfully: 73,944 across the exact-P13 matrix plus detached R0 controls and 6,000 across the two stress programs. The P0 and reference-absent lexical extensions added 13,824 successful requests with no terminal provider errors, missing predictions, or application retries. The earlier program likewise had no terminal error, missing prediction, or application retry. Two separate SDK requests in that earlier program produced no terminal artifact for at least 900 seconds during TCP connection; the coordinator was stopped and resumed from immutable state. Those two orphan attempts are excluded from scientific results and conservatively included only in cost accounting.

Across authenticated main and stress telemetry, per-cell median end-to-end latency ranges from 1.47 to 2.20 seconds and P95 ranges from 1.86 to 2.62 seconds. The P0 extension contributes 16.18 million reported prompt tokens and 0.118 million candidate tokens; the reference-absent lexical extension contributes 2.09 million prompt and 0.013 million candidate tokens. Context construction overhead was not instrumented separately, so these measurements do not isolate local construction cost from provider latency.

The P0 and reference-absent lexical extensions add USD 4.4505 under their frozen request proxy and USD 7.3172 under the telemetry-derived estimate. Across all registered study requests, the corresponding totals are USD 25.6054 and USD 38.7241; including the two orphan allowances gives USD 25.6060 and USD 38.7248. These estimates use the documented audio-token split proxy and are not reconciled provider bills. Request-proxy and telemetry-derived totals are alternatives and must not be added.

## 6. Discussion

The study answers three narrower questions.

First, **self-predicted history has measurable utility for both tuned systems under P13**. Moving from exact-P13 zero history to an eight-turn maximum reduces WER by 1.85 and 1.76 points for the no-context- and prior-context-SFT rolling systems. This is the total effect of a rolling policy after trajectories diverge autoregressively, not the direct effect of appending fixed text to otherwise identical requests. The base target does not show a supported P0-to-R8 WER change.

Second, **the historical prior-context-targeted rolling system has no supported quality advantage over the historical no-context-targeted system**. Their R8 WERs differ by 0.02 points, and their P0-to-R8 improvements differ by only 0.09 points with an interval spanning −0.65 to +0.85. This result weakens the intended “learned contract” narrative. The historical no-context-trained target benefits comparably from P13 history, but the study does not identify why.

Third, **the larger-window policy remains text-sensitive**. Lexical reference-proxy diagnostics rise with the window, especially for the prior-context-targeted system, and unrelated predicted text degrades both tuned systems. A separate reference-absent prediction intervention produces low but nonzero literal candidate adoption without a supported WER change; none of its within-target adoption contrasts has a registered Holm value below 0.05. These stress results reveal lexical sensitivity, but they do not show that adopted or repeated tokens were acoustically unsupported. The evidence therefore cannot establish semantic hallucination or rank the two tuned systems by semantic robustness.

The strongest alternative explanation is historical confounding. The prior-context job used a smaller and differently composed corpus, revised labels, another training prompt, another serialization, and a weaker temporal policy. A controlled study could still find a training benefit if both SFT conditions shared data, labels, hyperparameters, seeds, checkpoint selection, and serving prompts. No such training intervention was run here.

## 7. Limitations, ethics, and data governance

The primary limitation is that the 4,108 clips are development data used during historical checkpoint and prompt selection. There is no held-out confirmation. The 615 source objects are bootstrap clusters, but object identity does not guarantee acoustic, incident, speaker, or conversation separation from training, and the planned acoustic-overlap audit yielded no admissible result.

The exact-P13 P0 control removes the earlier zero-history prompt confound from P0-to-Rk contrasts. Option-D R0 remains detached and must not be placed on that curve. Even with P13 fixed, each SFT target is a single historical job with unavailable training randomness, and the jobs are not matched. Moreover, each target generates its own rolling history, so a same-window target gap compares complete end-to-end systems with different endogenous text inputs, not two models conditioned on identical histories. The base cells additionally use a mutable publisher alias; their observed version-set fingerprint authenticates this execution but cannot make the target immutably rerunnable. These constraints preclude causal claims about SFT.

Automatic context-copy, prior-only-token, and candidate-adoption rates compare predictions, supplied history, and references. They can count a genuinely audible repetition as history-overlapping, miss unsupported paraphrases, and increase mechanically as the opportunity set grows. Even a candidate selected to be absent from the reference could be audible despite a label omission. No two-reviewer blinded audio study was completed. Semantic hallucination, acoustic support, out-of-span continuation, and target-boundary violation are therefore unmeasured. The paper also lacks reviewed entity labels, masked robustness on the same rows, independent inference replicates, and an identical-audio external baseline.

Public-safety audio can contain names, locations, medical details, and operational information. Performance may vary across agencies, speakers, accents, geographies, codecs, receivers, and capture conditions that cannot safely be fully disclosed. We publish no raw transcript examples, signed storage URLs, endpoint identifiers, private predictions, or source locators. Repository code licensing does not establish redistribution rights for audio, labels, or provider outputs. Any future qualitative release requires PII masking and governance approval.

Operationally plausible errors can be more dangerous than obvious nonsense. A wrong unit, location, or instruction borrowed from previous traffic could mislead a downstream user. Transcripts should remain advisory, expose uncertainty, and permit immediate audio verification; they must not be treated as authoritative dispatch records. This study changed no production endpoint, deployment, dataset, or user traffic.

## 8. Conclusion

Under the fixed P13 interface, moving from zero history to an eight-turn self-predicted-history policy improves emergency-radio WER for two tuned Gemini 3.1 Flash-Lite rolling systems. The historically prior-context-targeted system has neither a supported R8 advantage nor a larger P0-to-R8 WER gain, and it has higher lexical reference-proxy copy and prior-only-token rates. Unrelated plausible predictions harm both tuned systems; a separate reference-absent prediction intervention yields low literal candidate-adoption rates but no supported WER effect. The evidence therefore supports a narrower statement than the proposed learned-contract thesis: predicted context is useful but lexically fragile under P13, and this development study does not demonstrate a differential quality advantage or semantic-safety benefit from the historical prior-context SFT bundle.

## Reproducibility statement

The repository contains the frozen prediction-only protocol, prompt fingerprints and publication-safe text, deterministic schedules, execution plans and receipts, authenticated aggregate analysis, 10,000-draw bootstrap configuration, generated tables and figures, and exact commands. Raw audio, references, predictions, source identifiers, model resources, and private bindings remain withheld. The [appendix](appendix.md) documents metric definitions, full contrasts, operational accounting, invalidated evidence, and release boundaries.

The aggregate analysis and figures have been reproduced byte-for-byte, and the claims matrix, appendix, and artifact registry have been reconciled with the exact-P13 and lexical-stress extensions. Remaining submission work is governance rather than another inference experiment: complete authorship, affiliation, privacy, licensing, and public-release approval, then select a venue and apply its current format. Held-out replication and blinded listening remain necessary to broaden the paper beyond development-set and lexical-diagnostic claims, but they are limitations of the present scope rather than requested next experiments.

## References

Citations use keys from the verified primary-source bibliography in [`references.bib`](references.bib).
