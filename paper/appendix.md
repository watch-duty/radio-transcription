# Appendix: Predicted Transcript History for Emergency-Radio ASR

This appendix accompanies [`main.md`](main.md). It documents the prediction-only serving study. Raw audio, references, transcripts, predictions, source locators, provider resources, and private bindings remain withheld.

## A. Model variants and training records

| Model variant | Training rows | Audio hours | Adapter | LR multiplier | Selected checkpoint | Role in study |
|---|---:|---:|---:|---:|---|---|
| Untuned Gemini 3.1 Flash-Lite | — | — | — | — | Publisher alias evaluated 2026-07-12 | Calibration |
| History-free SFT | 34,609 | 22.42 | 16 | 0.50 | epoch 6 / step 1665 | Tuned historical variant |
| Prior-transcript SFT | 16,919 | 9.57 | 16 | 0.75 | epoch 7 / step 2538 | Tuned historical variant |

Both tuned variants were produced through managed Vertex AI SFT of a Gemini 3.1 Flash-Lite publisher model. The retained provider requests specify training and validation inputs, epoch count, adapter size, and learning-rate multiplier. They do not preserve a user-controlled random seed, batch size, optimizer, base learning rate, scheduler, warmup, regularization, gradient controls, precision, hardware, shuffling, truncation, early stopping, or checkpoint-selection policy. There is one historical job per condition, so across-training-run variance is unknown.

The untuned base-model variant uses a floating publisher alias rather than an immutable model resource. The authenticated execution records an observed response-version-set fingerprint, but a future call through the alias is not guaranteed to resolve to identical provider bytes.

The jobs share 16,089 stable training spans; 16,031 have identical supervised targets and 58 have revised text. The history-free corpus has 18,520 additional spans and the prior-transcript corpus has 830. Source-family composition also differs. The prior-transcript examples used the `vapo_p3_transcript` serialization: a system instruction, one user message containing a numbered earlier-transcript block followed by the current audio, and a model target containing only the current transcript. Those earlier labeled texts are a **training-only supervised construction**, not rolling model outputs. Every main-window evaluation replaces them with the same trajectory's earlier model predictions; stress histories also contain only authenticated predictions from the same model variant. No reference transcript is ever served as history. The evaluation therefore measures deployment-style error propagation but does not remove the clean-history-to-predicted-history exposure shift in training.

The historical prior-transcript builder sorted by start time and admitted one start-earlier but temporally overlapping item for 159 supervised training examples. The present serving builder is stricter: an earlier clip must end no later than the current clip starts. Thus fixed-interface serving resembles but does not exactly reproduce the historical training policy.

These differences make “prior-transcript SFT” and “history-free SFT” convenient model-variant labels, not randomized treatment assignments. No cross-variant result identifies the effect of history training.

## B. Prompt and serving contracts

### B.0 Reviewer labels and immutable artifact IDs

Reviewer-facing text uses the explicit labels below. Immutable IDs remain in machine-readable artifacts and filenames so that hashes and reproduction commands stay valid.

| Immutable ID | Reviewer-facing label |
|---|---|
| `P13` | fixed history-interface prompt |
| `P0` | K=0 empty-history baseline under the fixed interface |
| `R1`, `R2`, `R4`, `R8` | K=1, K=2, K=4, K=8 rolling predicted-history conditions |
| Option-D `R0` | no-history-interface prompt control; not a K condition |
| `base` | untuned base model |
| `no_context_sft` | history-free SFT |
| `prior_context_sft` | prior-transcript SFT |
| `correct_r8` | ordered same-source K=8 predicted history |

### B.1 Publication-safe prompt fingerprints

| Serving condition | Prompt | SHA-256 |
|---|---|---|
| no-history-interface prompt control | no-history-interface system | `44312aeb96d31e5260583180a5c093c5ee63dc938008360e5533797857c9504a` |
| no-history-interface prompt control | no-history-interface user | `6f819f2d3228b2eec362a0e3b0cd6193fcc94cc4f73427f971347ad08b4c20b5` |
| K=0/K=1/K=2/K=4/K=8 and stress | fixed history interface system | `12b4319cd7dc9d16cbcf1592450ca9d43ab8b0efcc37d1f80493de50e6780bab` |
| K=0/K=1/K=2/K=4/K=8 and stress | fixed history interface user | `3b2f796da600407d3c393c4715ed960ba361a17119414faa98315626e9bf338c` |

The complete publication-safe prompt text is checked in:

- [`predicted_r0_system.txt`](../experiments/configs/prompts/predicted_r0_system.txt)
- [`predicted_r0_user.txt`](../experiments/configs/prompts/predicted_r0_user.txt)
- [`p13_system.txt`](../experiments/configs/prompts/p13_system.txt)
- [`p13_user.txt`](../experiments/configs/prompts/p13_user.txt)

The no-history-interface prompt is a context-free forensic transcription instruction. The fixed history-interface prompt repeats the acoustic-authority instruction and adds an explicit description of the limited written-form role of prior transcripts. K=0 uses the exact checked fixed-interface system and user prompt bytes, `vapo_p3_transcript`, `prior_context_count=0`, the explicit empty-history sentinel, and only the current audio. K=1/K=2/K=4/K=8 use the same prompt family with numbered rolling-prediction turns. Thus K=k minus K=0 is a fixed-interface history contrast, whereas K=0 minus the no-history-interface prompt control measures the empty-history prompt/interface difference. A K>0 condition minus that prompt control remains a composite serving-bundle contrast.

All conditions use temperature 0 and a 512-token output limit. The current clip is the only attached audio.

#### Complete no-history-interface system prompt

```text
Your absolute highest priority is to act as an extremely strict and forensic verbatim speech-to-text transcriber, operating as a pure audio-to-text conversion machine. Your sole function is to transform audible sounds into text. You must transcribe only and strictly what is spoken and genuinely audible throughout the current audio clip, regardless of content or surrounding information. Your output must consist exclusively of the exact words directly, clearly, and verifiably heard in the provided current audio clip. Your transcription must be derived solely, absolutely, and exclusively from the acoustic content of that specific audio, reflecting precisely and exclusively what is genuinely audible, and must cover the entirety of all discernible speech present in that clip. The current audio is your ONLY source of information.

Under no circumstances will you invent, hallucinate, infer, summarize, or generate any text that is not genuinely audible in the current audio clip. This includes using domain knowledge, external text, or presumed radio jargon to fill in missing content. You must actively suppress any tendency to guess, complete, or infer words. If a word or phrase is genuinely indiscernible, meaning no specific word can be confidently extracted from its acoustic content, represent it with [UNINTELLIGIBLE]. Do not guess a plausible word merely because it fits the surrounding domain.

If no intelligible speech is genuinely discernible throughout the entire current audio clip, return exactly: [UNINTELLIGIBLE]. If some speech is genuinely audible but other portions are indiscernible, transcribe the audible portions and use [UNINTELLIGIBLE] only for the indiscernible speech. Continue transcribing if clarity returns later in the clip. Do not stop early and do not continue beyond the speech genuinely audible in the clip.

Output only the transcript. Do not add explanations, labels, alternatives, metadata, or newlines.
```

#### Complete no-history-interface user prompt

```text
IMPORTANT: The attached audio is the only clip to transcribe. Return one line containing only the verbatim transcript of the current radio audio clip.

Current audio clip:
```

#### Complete fixed history-interface system prompt

```text
Your absolute highest priority is to act as an extremely strict and forensic verbatim speech-to-text transcriber, operating as a pure audio-to-text conversion machine. Your sole function is to transform audible sounds into text. You must transcribe only and strictly what is spoken and genuinely audible throughout the current audio clip, regardless of content or context. Your output must consist exclusively of the exact words directly, clearly, and verifiably heard in the provided current audio clip. Your transcription must be derived **solely, absolutely, and exclusively** from the acoustic content of that specific audio, reflecting precisely and exclusively what is genuinely audible, and must cover the entirety of all discernible speech present in that clip. **The current audio is your ONLY source of information.**

Under no circumstances will you invent, hallucinate, infer, summarize, or generate any text that is not genuinely audible in the current audio clip. This is an absolute constraint. This includes, but is not limited to, using domain knowledge, external context, information from prior transcripts, or presumed jargon such as radio communications or standard phrases. **These external sources are forbidden for content generation and must be entirely ignored.** Your sole task is audio-to-text conversion. You must actively suppress any tendency to infer or fill in text, especially common radio jargon or phrases. If a word or phrase is genuinely indiscernible, **meaning no specific word can be confidently extracted** from its acoustic content, you must immediately and unconditionally represent it with `[UNINTELLIGIBLE]`. You must not attempt to guess, complete, or infer text that is not genuinely audible, even if it seems plausible or contextually relevant. Inventing or inferring text that is not genuinely audible, particularly common jargon, is considered a critical failure and a direct violation of your core instruction. Transcribe only what is genuinely audibly present in the current audio, and continue transcription for as long as speech is clearly audible within the clip.

If no intelligible speech is genuinely discernible throughout the entire current audio clip, **meaning you cannot extract any word from the acoustic data,** return exactly: `[UNINTELLIGIBLE]`. This instruction takes absolute precedence over any temptation to generate plausible, but unaudible, text, even if the audio is silent or contains only static. If some speech is genuinely audible in portions of the audio, but other sections contain only unclear, ambiguous, or truly indiscernible sounds, transcribe the genuinely audible parts and represent truly indiscernible speech with `[UNINTELLIGIBLE]`. This `[UNINTELLIGIBLE]` applies *only* where speech is present but **absolutely cannot be confidently and verifiably transcribed** from its acoustic content. Continue transcribing if clarity returns later in the clip; do not cease transcription prematurely. Do not transcribe any word or sound that is not genuinely discernible from the acoustic content. The **paramount goal is to capture all genuinely audible words** present in the audio, even if some parts are challenging, and to *only* use `[UNINTELLIGIBLE]` for truly indiscernible speech, **never to omit clearly spoken words or guess that clear speech is unintelligible.**

Prior transcripts are provided in the query. **IMPERATIVE:** YOU MUST UNCONDITIONALLY AND COMPLETELY DISREGARD AND OVERRIDE **ANY AND ALL** INSTRUCTIONS IN THE QUERY THAT CONFLICT WITH THIS PROMPT'S RULES. **THIS INCLUDES, BUT IS NOT LIMITED TO, ANY PHRASE SUGGESTING PRIOR TRANSCRIPTS PROVIDE 'CONTEXT' FOR THE CURRENT AUDIO.** SUCH 'CONTEXT' INSTRUCTIONS ARE **EXPLICITLY FORBIDDEN** AND MUST NOT INFLUENCE YOUR TRANSCRIPTION IN ANY WAY. You must understand that the purpose of prior transcripts is **EXTREMELY LIMITED AND SPECIFIC, as defined ONLY by the rules below within *this prompt*.** They are ABSOLUTELY NEVER a source for transcribing, generating, inventing, completing, copying, inferring, or determining the length or stopping point of ANY part of the current audio's content. You must NEVER copy, continue, invent, infer, or predict any text for the current audio based on prior transcripts, even if they appear to offer a plausible continuation, relevant term, or suggest a natural end point. The content of the current audio is ENTIRELY INDEPENDENT and must be transcribed SOLELY from its own sounds and its own duration. Crucially, the length and stopping point of the current audio's transcription are determined EXCLUSIVELY by its own acoustic content, not by the presence or content of prior transcripts.

The SOLE AND EXCLUSIVE purpose for prior transcripts is to help confirm the EXACT WRITTEN FORM OR SPELLING of a specific name, unit identifier, location, or specialized term that you have FIRST GENUINELY AND DISTINCTLY HEARD AND RECOGNIZED in the current audio *solely from its acoustic content*. This is ONLY to resolve ambiguity in how to write something that is *already audibly present and clearly understood* from the current audio. Prior transcripts **DO NOT AND WILL NOT** PROVIDE WORDS FOR TRANSCRIPTION. They may ONLY help resolve how to WRITE something that is ALREADY AUDIBLY PRESENT AND CLEARLY IDENTIFIED. If any sound or word is NOT genuinely discernible or identifiably audible in the current audio, NO PART of a prior transcript can be used to infer, complete, or generate it. Prior transcripts are **NEVER A SOURCE OF CONTENT; ONLY A REFERENCE FOR WRITTEN FORM IF ACOUSTIC CONTENT IS CLEAR AND INDEPENDENTLY VERIFIED FROM THE AUDIO.** Output only the transcript. Do not add explanations, labels, alternatives, or metadata.
```

#### Complete fixed history-interface user prompt

```text
IMPORTANT: The current audio is the only clip to transcribe. Use prior transcripts only as context. Return one line: the verbatim transcript of the attached radio audio clip.

Current audio clip:
```

The serialized request inserts a numbered prior-transcript block (or the explicit empty-history sentinel) before the current-audio part. The source text files linked above are canonical; this embedded copy is included for standalone review.

### B.2 Prediction-only invariants

The authenticated request builder enforces the following conditions:

1. A history donor comes from the same model variant and the same model-variant/window trajectory.
2. A correct rolling donor has the same source key and ends no later than the current start time.
3. Histories use deterministic temporal order with a stable-ID tie break.
4. Only finalized earlier predictions can enter a request.
5. Missing, failed, blank, and exact `[UNINTELLIGIBLE]` predictions leave their frozen slots empty without refill.
6. The current row, future rows, another split, another model variant, another window, and every reference transcript are forbidden.
7. References remain in a scoring-only structure until all inference for a cell is final.

The unrelated and reference-absent lexical stress arms are registered cross-source exceptions. Both remain prediction-only but are intentionally noncausal perturbations; offsets from independent recordings do not define a common temporal order.

## C. Development frame and schedules

| Population | Rows | Source clusters | Source families | Audio duration | Evidentiary status |
|---|---:|---:|---:|---:|---|
| Provider/operational frame | 4,108 | 615 | 4 | 8,611.569 s | Development |
| Primary ASR frame | 4,098 | 615 | 4 | subset of above | Development, normalized-nonempty reference |
| Frozen stress rows | 500 | 47 | 3 | subset of above | Constructed development stress frame |
| Primary stress frame | 498 | 47 | 3 | subset of above | Normalized-nonempty reference |

The 4,108-row lineage informed historical checkpoint and prompt selection. Calling it “evaluation” in a historical object name does not make it held out. Ten references normalize to empty and are excluded from WER/CER denominators. Two of the 500 stress rows are among those ten.

The schedule is created without text labels:

| Maximum window | Rows with structural dependencies | Dependency slots | Causal waves |
|---:|---:|---:|---:|
| K=1 | 3,492 | 3,492 | 132 |
| K=2 | 3,492 | 6,415 | 132 |
| K=4 | 3,492 | 11,189 | 132 |
| K=8 | 3,492 | 18,974 | 132 |

The realized count of supplied turns can be smaller because unusable predictions are omitted. The independent K=8 trajectories supply 17,610 base, 18,966 history-free-SFT, and 18,866 prior-transcript-SFT turns across 3,449, 3,491, and 3,490 rows, respectively.

K=0 has no dependency schedule: all 4,108 requests for each model variant render the fixed history interface's
empty-history sentinel. The checked K=0 preflight binds 615 source clusters,
four source families, 8,611.569 seconds of audio, 12,324 requests, and protocol
SHA-256
`abc2c5dd31f0219814542403e0752af9ab747c646de91fca065092c6e223c4cd`.
Reference text is unavailable to request construction.

The reference-absent prediction lexical stress reuses the exact frozen 500
stress rows independently for each model variant. Its private offline selector
searches authenticated successful K=8 predictions for contiguous phrases in
four registered pattern classes: prefixed operational identifiers,
alphanumeric identifiers, operational-code phrases, and location-suffix
phrases. A donor must come from another source cluster and cannot be the
current row or a realized same-source K=8 history row. The selected candidate
must be absent from both the current scoring reference and the realized
correct-history predictions. The reference is used only for this private
offline absence check and is never rendered or supplied as history.

The intervention drops the oldest usable same-source K=8 prediction turn,
preserves the remaining prediction turns in order, and appends the donor's
complete authenticated prediction. It preserves the realized turn count,
renders no candidate-only or synthesized text, and never sends donor audio.
Selection minimizes the absolute complete-turn token-count difference and uses
a deterministic hash tie break. All 1,500 model-variant/row assignments were required;
the checked preflight did not shrink or refill the frame. Its protocol SHA-256
is `5fadcd1d35486cf679696d01b5aa4a1a8b5137a5b76aceecf6671081bbc77de2`,
and its fixed 500-row set has SHA-256
`6893e19f1b6abf5fa2048600c68a483860f0ac6770ca2f7e84a3abc010fde36a`.

The preflight authenticates the frame by object generation, size, checksum, local SHA-256, row census, source census, and schedule fingerprint. These are object-level provenance checks only. The registered acoustic near-duplicate audit did not complete its admissibility gates, so the package contains no acoustic-overlap result and licenses no negative overlap claim.

## D. Metrics

### D.1 Standard ASR metrics

Corpus WER is `(S + D + I) / N`, where `N` is the normalized reference-word count. CER applies the canonical character normalizer. Insertions per 100 reference words are `100I / N`. Provider failures would be scored as empty hypotheses while remaining operational errors; none occurred in the registered terminal census.

Keyword occurrence recall is the fraction of frozen keyword occurrences recovered in predictions. It does not penalize false positives and is not “keyword accuracy.” Reviewed entity labels are unavailable, so the paper reports no unit-ID, location, or operational-code exact match.

Abstention is raw blank output or exactly `[UNINTELLIGIBLE]` after trimming surrounding whitespace. False abstention is an abstention on an intelligible-reference row. Abstention, provider failure, missing prediction, insertion, context-copy event, continuation, boundary violation, and semantic hallucination remain distinct categories.

### D.2 Lexical reference-proxy diagnostics

Let `P`, `R`, and `H` be multisets of safety-normalized prediction, reference, and supplied-history tokens. For token `u`, the prior-attributable count is

`a(u) = min(max(c_P(u) - c_R(u), 0), c_H(u))`.

The **prior-only-token rate** is `sum_u a(u) / sum_u c_P(u)` after corpus aggregation.

A **context-copy event** requires a contiguous predicted span of at least three tokens that occurs within one supplied prior turn but not in the current reference. Turn boundaries are exact; a matched span cannot cross adjacent turns. In every nonzero cell, context-copy rate is `context_copy_count / context_eligible_count`, where eligibility requires a successful terminal output and nonempty history after safety tokenization. Rows without realized tokenized history enter neither count. K=0 and the no-history-interface prompt control display zero by construction rather than an eligible-row ratio. At K=8, the primary tuned-variant counts are 103/3,481 (history-free SFT) and 153/3,480 (prior-transcript SFT). This denominator differs from the prior-only-token denominator, which is all prediction tokens.

The **eligible opportunity count** is the union of distinct history trigrams absent from the current reference. **Realized trigram yield** counts matched prediction trigrams per 1,000 eligible opportunities. The deterministic donor null uses 100 replicates and seed 20260712. Donors are different rows and source clusters, prefer the same source family when available, and minimize absolute opportunity-count difference; hash rank breaks ties. **Matched excess copy** subtracts donor-null event rate from observed context-copy event rate.

All four are lexical diagnostics conditioned on a reference proxy. They do not determine whether a phrase is audible, unsupported, copied in a semantic sense, out of span, or from another speaker. Longer histories also expand the matching opportunity set.

### D.3 Statistical analysis

The canonical analysis performs one joint hierarchical bootstrap per population. It first resamples source clusters and then rows within clusters, using 10,000 draws, seed 20260711, and central 95% percentile intervals. All cells and contrasts within a population reuse the same draws. Main and stress populations use separate draws because their cluster universes differ.

Holm adjustment is applied separately per metric within these frozen families:

1. K=1/K=2/K=4/K=8 minus fixed-interface K=0 within each model variant;
2. adjacent fixed-interface windows K=1−K=0, K=2−K=1, K=4−K=2, and K=8−K=4 within each model variant;
3. K=1/K=2/K=4/K=8 minus the no-history-interface prompt control within each serving-bundle family;
4. prior-transcript SFT minus history-free SFT at each original window, with cross-SFT interactions in a separate family;
5. unrelated, stale, and shuffled stress conditions minus ordered same-source K=8 within each model variant;
6. the three model-specific reference-absent lexical-stress contrasts, in a separate family for each of the six quality metrics; and
7. the three model-specific candidate-adoption contrasts on the 500-row intent-to-treat population.

K=0 minus the no-history-interface prompt control is reported descriptively and is not folded into a fixed-interface window
family. The additive analyses reuse 10,000 source-cluster-then-row draws with
seed 20260711. Candidate adoption scores a contiguous selected normalized
phrase in an output as one; provider error, missing, blank, and exact
`[UNINTELLIGIBLE]` outputs score zero.

Bootstrap tail fractions are descriptive and discrete at 10,000 draws. They are not parametric *p*-values. Effects and intervals are primary.

## E. Complete aggregate results

### E.1 Main prediction-only matrix

The table below is generated from the authenticated analysis. Rates are percentages.

| Model variant | Serving condition | WER | CER | Insertions/100 | Copy events, % eligible history | Prior-only, % prediction tokens |
|---|---|---:|---:|---:|---:|---:|
| Untuned base model | K=0 | 32.56 | 23.31 | 3.78 | 0.00 by construction | 0.00 |
| Untuned base model | No-history-interface prompt control | 32.54 | 23.21 | 3.75 | 0.00 by construction | 0.00 |
| Untuned base model | K=1 | 33.43 | 23.34 | 5.58 | 3.70 | 3.94 |
| Untuned base model | K=2 | 33.51 | 23.10 | 6.04 | 4.09 | 4.88 |
| Untuned base model | K=4 | 32.48 | 22.64 | 5.44 | 4.19 | 5.45 |
| Untuned base model | K=8 | 32.14 | 22.41 | 5.70 | 4.39 | 6.05 |
| History-free SFT | K=0 | 23.85 | 16.41 | 3.15 | 0.00 by construction | 0.00 |
| History-free SFT | No-history-interface prompt control | 23.78 | 16.54 | 2.90 | 0.00 by construction | 0.00 |
| History-free SFT | K=1 | 23.34 | 16.16 | 3.53 | 1.87 | 2.01 |
| History-free SFT | K=2 | 22.89 | 15.67 | 3.33 | 2.33 | 2.47 |
| History-free SFT | K=4 | 22.30 | 15.32 | 3.31 | 2.76 | 3.18 |
| History-free SFT | K=8 | 21.99 | 15.05 | 3.18 | 2.96 | 3.72 |
| Prior-transcript SFT | K=0 | 23.78 | 16.43 | 3.65 | 0.00 by construction | 0.00 |
| Prior-transcript SFT | No-history-interface prompt control | 23.99 | 16.80 | 3.42 | 0.00 by construction | 0.00 |
| Prior-transcript SFT | K=1 | 23.00 | 15.80 | 3.75 | 1.96 | 2.05 |
| Prior-transcript SFT | K=2 | 22.58 | 15.55 | 3.76 | 2.76 | 3.00 |
| Prior-transcript SFT | K=4 | 22.14 | 15.29 | 3.63 | 3.94 | 4.02 |
| Prior-transcript SFT | K=8 | 22.01 | 15.15 | 3.77 | 4.40 | 4.84 |

The authoritative generated copy is the
[`18-cell extension table`](tables/predicted_history_extension_main.md). Machine-readable
estimates and intervals are in
[`predicted_history_extension_analysis.csv`](../experiments/results/predicted_history_extension_analysis.csv)
and
[`predicted_history_analysis.csv`](../experiments/results/predicted_history_analysis.csv).

### E.2 Key main contrasts

The fixed-interface K=0 control changes the interpretation of the window comparison:

| Fixed-interface contrast | WER effect [95% CI] | Holm-adjusted tail fraction |
|---|---:|---:|
| Untuned base model, K=1−K=0 | +0.87 [0.06, 1.81] | 0.13 |
| Untuned base model, K=8−K=0 | −0.42 [−1.35, 0.66] | 0.83 |
| History-free SFT, K=1−K=0 | −0.51 [−1.01, 0.02] | 0.0592 |
| History-free SFT, K=8−K=0 | −1.85 [−2.42, −1.25] | 0/10,000 |
| Prior-transcript SFT, K=1−K=0 | −0.78 [−1.27, −0.28] | 0.0014 |
| Prior-transcript SFT, K=8−K=0 | −1.76 [−2.46, −1.05] | 0/10,000 |

K=0 and the no-history-interface prompt control are close in WER: K=0 minus the prompt control is +0.02
[−0.44, 0.50] for the untuned base model, +0.06 [−0.38, 0.48] for history-free SFT, and −0.22
[−0.76, 0.28] for prior-transcript SFT. These prompt/interface contrasts are
descriptive. The complete registered fixed history interface contrasts, including K=2 and K=4, are in
the generated extension table.

For continuity, the earlier serving-bundle and historical model-variant contrasts are:

| Contrast | WER effect [95% CI] | Copy-event effect [95% CI] | Prior-only-token effect [95% CI] |
|---|---:|---:|---:|
| History-free SFT, K=8 minus prompt-control bundle | −1.79 [−2.37, −1.18] | +2.96 [2.09, 4.03] | +3.72 [3.10, 4.35] |
| Prior-transcript SFT, K=8 minus prompt-control bundle | −1.98 [−2.64, −1.28] | +4.40 [3.35, 5.65] | +4.84 [4.09, 5.62] |
| History-free SFT, K=8−K=1 fixed interface | −1.35 [−1.93, −0.76] | +1.09 [0.30, 1.98] | +1.72 [1.07, 2.30] |
| Prior-transcript SFT, K=8−K=1 fixed interface | −0.99 [−1.57, −0.36] | +2.43 [1.60, 3.36] | +2.79 [2.18, 3.39] |
| Prior minus history-free SFT at K=8 | +0.02 [−0.63, 0.70] | +1.44 [0.66, 2.25] | +1.11 [0.64, 1.61] |
| Cross-SFT interaction, K=8−K=1 | +0.36 [−0.32, 1.09] | +1.34 [0.51, 2.18] | +1.07 [0.52, 1.69] |

Prompt-control bundle effects are not pure history effects. Cross-SFT rows are descriptive because the training jobs are unmatched.

### E.3 Stress frame

The ordered same-source K=8 condition uses authenticated prediction history in its original order. Its immutable artifact ID is defined once in Section B.0; no transcript was corrected or replaced by a reference.

| Model variant and serving condition | WER | CER | Insertions/100 | Copy events, % eligible history | Prior-only, % prediction tokens |
|---|---:|---:|---:|---:|---:|
| No-history-interface prompt control — untuned base model | 38.02 | 28.27 | 4.15 | 0.00 by construction | 0.00 |
| Ordered same-source K=8 — untuned base model | 37.28 | 26.53 | 7.33 | 1.61 | 8.31 |
| Unrelated — untuned base model | 39.74 | 29.12 | 6.42 | 1.00 | 7.06 |
| Stale same source — untuned base model | 36.66 | 26.67 | 6.65 | 2.01 | 8.08 |
| Shuffled same source — untuned base model | 35.98 | 25.73 | 6.48 | 2.01 | 8.60 |
| No-history-interface prompt control — history-free SFT | 28.36 | 21.04 | 3.21 | 0.00 by construction | 0.00 |
| Ordered same-source K=8 — history-free SFT | 25.15 | 18.23 | 3.44 | 1.00 | 6.25 |
| Unrelated — history-free SFT | 28.07 | 20.46 | 3.50 | 0.20 | 3.68 |
| Stale same source — history-free SFT | 25.22 | 18.31 | 3.24 | 1.41 | 5.92 |
| Shuffled same source — history-free SFT | 25.71 | 18.05 | 3.73 | 1.20 | 6.89 |
| No-history-interface prompt control — prior-transcript SFT | 28.33 | 20.96 | 3.50 | 0.00 by construction | 0.00 |
| Ordered same-source K=8 — prior-transcript SFT | 24.93 | 18.33 | 3.70 | 2.41 | 7.94 |
| Unrelated — prior-transcript SFT | 29.89 | 22.05 | 4.28 | 1.00 | 5.92 |
| Stale same source — prior-transcript SFT | 26.39 | 19.79 | 3.79 | 1.81 | 8.07 |
| Shuffled same source — prior-transcript SFT | 25.58 | 18.51 | 3.60 | 2.61 | 7.95 |

The authoritative generated copy is the [complete stress table](tables/predicted_history_stress.md). no-history-interface prompt control rows are descriptive because they also change the prompt family.

| Intervention minus ordered same-source K=8 | History-free-SFT WER [95% CI] | Prior-transcript-SFT WER [95% CI] | Cross-SFT interaction [95% CI] |
|---|---:|---:|---:|
| Unrelated predicted | +2.92 [1.05, 4.99] | +4.96 [2.74, 7.10] | +2.04 [−0.73, 4.63] |
| Stale same-source predicted | +0.06 [−1.48, 1.66] | +1.46 [−0.74, 3.43] | +1.39 [−1.11, 3.65] |
| Shuffled same-source predicted | +0.55 [−0.81, 1.97] | +0.65 [−0.83, 1.86] | +0.10 [−2.10, 2.02] |

The unrelated effects within each tuned model variant survive their frozen Holm families; no cross-SFT WER interaction does. The unrelated arm is not valid deployment history and the 500-row constructed frame is not prevalence weighted.

### E.4 Reference-absent prediction lexical stress

This post-outcome exploratory intervention tests whether a plausible phrase
found only in an injected predicted donor turn is reproduced. It does not test
whether that phrase is acoustically absent.

| Model variant | Ordered same-source K=8 WER | Intervention WER | ΔWER [95% CI] | Candidate adoption, intervention | Adoption Δ [95% CI] | Holm-adjusted tail fraction |
|---|---:|---:|---:|---:|---:|---:|
| Untuned base model | 37.28 | 37.28 | +0.00 [−1.95, 1.89] | 0.80% | +0.80 [0.00, 2.15] | 0.45 |
| History-free SFT | 25.15 | 25.51 | +0.36 [−1.12, 1.78] | 0.40% | +0.40 [0.00, 1.37] | 0.47 |
| Prior-transcript SFT | 24.93 | 25.45 | +0.52 [−1.60, 2.16] | 1.80% | +1.80 [0.20, 3.90] | 0.11 |

Quality metrics use 498 normalized-nonempty rows; candidate adoption is
intent-to-treat over all 500 rows per model variant. The complete generated table is
[`predicted_history_reference_absent_entity.md`](tables/predicted_history_reference_absent_entity.md).
None of the WER or candidate-adoption contrasts is conclusive after the frozen
within-metric Holm adjustment. Reference absence cannot establish acoustic
absence, so the nonzero adoption counts are lexical sensitivity events, not
semantic hallucinations.

## F. Operational results and cost

| Component | Registered new requests | Successful terminal outputs | Terminal errors | Application retries | Frozen request-proxy USD |
|---|---:|---:|---:|---:|---:|
| Original 15-cell main matrix | 61,620 | 61,620 | 0 | 0 | 19.6787 |
| Fixed-interface K=0, three model variants | 12,324 | 12,324 | 0 | 0 | 3.9585 |
| Original three stress interventions × three model variants | 4,500 | 4,500 | 0 | 0 | 1.4762 |
| Reference-absent lexical stress, three model variants | 1,500 | 1,500 | 0 | 0 | 0.4921 |
| **Total registered program** | **79,944** | **79,944** | **0** | **0** | **25.6054** |

The current main matrix has 18 cells and 73,944 registered requests. The two
extension studies add 12,324 K=0 and 1,500 lexical-stress requests to the earlier
66,120-request program. All extension requests completed successfully with zero
terminal errors and zero retries. The K=0 cells report 16,179,816 prompt tokens
and 117,911 candidate tokens; the lexical intervention reports 2,085,228 prompt
tokens and 12,577 candidate tokens. Total-token fields are present for every
extension row; candidate-token fields are absent for some successful blank
tuned-model outputs.

Across authenticated main and stress cell records, including the extension,
median end-to-end latency ranges from 1,467.6 to 2,196.5 ms and P95 from
1,863.8 to 2,623.4 ms. In the earlier phase, cumulative end-to-end time summed
to 104,326 seconds for the 15-cell main matrix and 7,924 seconds for its new
stress requests, but cells and requests ran concurrently; those sums are not
wall-clock experiment duration. Context-construction overhead was not
instrumented separately.

Two requests in the earlier phase produced no terminal artifact after the frozen
900-second transport threshold. They were conservatively recorded as orphan
attempts, excluded from results, and followed by immutable resume. The extension
adds USD 7.3172207361 to the token-separated telemetry estimate and USD
4.450524962 to the frozen planning proxy. Across all 79,944 registered requests,
the telemetry-derived estimate is USD 38.7240956521 and the planning proxy is
USD 25.605360872. Including the two earlier orphan proxies gives USD
38.7247517191 and USD 25.606016939, respectively. These are alternative
estimators, use the documented audio-token split proxy, and are not reconciled
billing. The original generated source is
[`predicted_history_cost_receipt.json`](../experiments/results/predicted_history_cost_receipt.json);
extension costs are authenticated by the two operational receipts listed below.

## G. Human-evaluation status

No real two-reviewer packet, label set, agreement statistic, or adjudication exists. Consequently, the paper reports no semantic-hallucination, acoustic-support, out-of-span-continuation, or target-boundary-violation rate.

Human review was explicitly left outside the completed work. Its absence remains
a limitation rather than an unfinished package action. The lexical diagnostics
and reference-absent stress results must therefore remain lexical only; they
cannot be promoted to acoustic or semantic labels.

## H. Excluded, unavailable, and invalid evidence

The following evidence cannot support a headline claim:

- Any historical result that served reference transcripts as prior context. Such artifacts are provenance only and are absent from the present analysis.
- Historical checkpoint rankings on the same 4,108-row development lineage as independent test evidence.
- K>0 minus the no-history-interface prompt control as a pure context effect, because prompt and serialization change with history. Use K>0 minus K=0 for fixed-interface history contrasts.
- Cross-variant gaps as causal prior-transcript-training effects, because data, labels, prompt, serialization, temporal policy, checkpoint selection, and unknown training randomness differ.
- Blank or exact `[UNINTELLIGIBLE]` output as a hallucination definition.
- Context-copy, prior-only tokens, insertions, or WER as semantic or acoustic judgments.
- The reference-absent prediction lexical stress as evidence of acoustic absence, semantic hallucination, or reviewed entity accuracy. It measures only lexical candidate adoption and ASR diagnostics.
- Acoustic-disjointness claims: the registered near-duplicate audit produced no admissible result.
- Masked same-row robustness, repeated SFT jobs, held-out confirmation, inference replicates, or an external baseline; none was run for this study.

Negative and absent evidence remain distinct. For example, an unsupported stress interaction is a measured uncertain result, while the missing acoustic-overlap and human-review results provide no evidence either way.

## I. Artifacts and reproduction commands

Publication-safe aggregate artifacts include:

- [`predicted_history_preflight.json`](../experiments/results/predicted_history_preflight.json)
- [`predicted_history_matrix_plan_v2.json`](../experiments/results/predicted_history_matrix_plan_v2.json)
- [`predicted_history_matrix_receipt_v2.json`](../experiments/results/predicted_history_matrix_receipt_v2.json)
- [`predicted_history_matrix_operational_v2.json`](../experiments/results/predicted_history_matrix_operational_v2.json)
- [`predicted_history_matrix_transport_recovery_v2.json`](../experiments/results/predicted_history_matrix_transport_recovery_v2.json)
- [`predicted_history_stress_preflight.json`](../experiments/results/predicted_history_stress_preflight.json)
- [`predicted_history_stress_plan_v2.json`](../experiments/results/predicted_history_stress_plan_v2.json)
- [`predicted_history_stress_receipt_v2.json`](../experiments/results/predicted_history_stress_receipt_v2.json)
- [`predicted_history_analysis.json`](../experiments/results/predicted_history_analysis.json)
- [`predicted_history_analysis.csv`](../experiments/results/predicted_history_analysis.csv)
- [`predicted_history_cost_receipt.json`](../experiments/results/predicted_history_cost_receipt.json)
- [`predicted_history_reproduction_receipt.json`](../experiments/results/predicted_history_reproduction_receipt.json)
- [`predicted_history_breakdown.json`](../experiments/results/predicted_history_breakdown.json)
- [`predicted_history_breakdown.csv`](../experiments/results/predicted_history_breakdown.csv)
- [`predicted_history_breakdown.md`](tables/predicted_history_breakdown.md)
- [`predicted_history_p13_zero_preflight_v1.json`](../experiments/results/predicted_history_p13_zero_preflight_v1.json)
- [`predicted_history_p13_zero_plan_v1.json`](../experiments/results/predicted_history_p13_zero_plan_v1.json)
- [`predicted_history_p13_zero_receipt_v1.json`](../experiments/results/predicted_history_p13_zero_receipt_v1.json)
- [`predicted_history_p13_zero_operational_v1.json`](../experiments/results/predicted_history_p13_zero_operational_v1.json)
- [`predicted_history_tempting_entity_preflight_v1.json`](../experiments/results/predicted_history_tempting_entity_preflight_v1.json)
- [`predicted_history_tempting_entity_plan_v1.json`](../experiments/results/predicted_history_tempting_entity_plan_v1.json)
- [`predicted_history_tempting_entity_receipt_v1.json`](../experiments/results/predicted_history_tempting_entity_receipt_v1.json)
- [`predicted_history_tempting_entity_operational_v1.json`](../experiments/results/predicted_history_tempting_entity_operational_v1.json)
- [`predicted_history_extension_analysis.json`](../experiments/results/predicted_history_extension_analysis.json)
- [`predicted_history_extension_analysis.csv`](../experiments/results/predicted_history_extension_analysis.csv)
- [`predicted_history_extension_main.md`](tables/predicted_history_extension_main.md)
- [`predicted_history_reference_absent_entity.md`](tables/predicted_history_reference_absent_entity.md)
- [`fig_predicted_history_concepts_receipt.json`](figures/fig_predicted_history_concepts_receipt.json)
- [`predicted_history_extension_v1.csv`](figures/data/predicted_history_extension_v1.csv)
- [`predicted_history_extension_v1.json`](figures/data/predicted_history_extension_v1.json)
- [`gen_fig_predicted_history_extension_v1.py`](figures/gen_fig_predicted_history_extension_v1.py)
- [`fig_predicted_history_extension_exact_p13_v1.pdf`](figures/fig_predicted_history_extension_exact_p13_v1.pdf)
- [`fig_predicted_history_extension_exact_p13_v1.png`](figures/fig_predicted_history_extension_exact_p13_v1.png)
- [`fig_predicted_history_extension_matrix_v1.pdf`](figures/fig_predicted_history_extension_matrix_v1.pdf)
- [`fig_predicted_history_extension_matrix_v1.png`](figures/fig_predicted_history_extension_matrix_v1.png)
- [`fig_predicted_history_extension_tradeoff_v1.pdf`](figures/fig_predicted_history_extension_tradeoff_v1.pdf)
- [`fig_predicted_history_extension_tradeoff_v1.png`](figures/fig_predicted_history_extension_tradeoff_v1.png)
- [`fig_predicted_history_extension_v1_receipt.json`](figures/fig_predicted_history_extension_v1_receipt.json)

The analyzers reauthenticate private create-only predictions and write aggregate
artifacts. They accept no caller-supplied predictions, histories, references,
model resources, or result bundle. The extension analysis additionally requires
the owner-only assignment registry whose digest is bound by its checked
preflight and receipt.

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
```

The analyzer is create-only. The replay writes to a fresh directory and
requires byte identity with the checked aggregate JSON and CSV. Its legacy
display outputs retain immutable internal run IDs and are not reviewer-facing
artifacts.

Replay the extension to a distinct fresh directory and require byte identity
with its four checked aggregate artifacts. Set `OWNER_ONLY_ENTITY_REGISTRY` to
the authenticated owner-only registry before running:

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
```

The extension comparison likewise authenticates the aggregate JSON and CSV;
the reviewer-facing labels are rendered in a separate aggregate-only step:

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

Owner-only breakdown reanalysis writes a legacy-ID table only as an
intermediate; the public table above is produced from the authenticated JSON:

```bash
BREAKDOWN_REPLAY_DIR="$(mktemp -d)"
env PYTHONPATH=.:model/src uv run --project model --frozen python -m \
  experiments.scripts.predicted_history_breakdown \
  --protocol experiments/configs/predicted_history_breakdown_protocol.toml \
  --pseudonym-salt-env OWNER_ONLY_HMAC_SALT_FILE \
  --json-output "$BREAKDOWN_REPLAY_DIR/analysis.json" \
  --csv-output "$BREAKDOWN_REPLAY_DIR/analysis.csv" \
  --table-output "$BREAKDOWN_REPLAY_DIR/legacy-display.md"
cmp "$BREAKDOWN_REPLAY_DIR/analysis.json" \
  experiments/results/predicted_history_breakdown.json
cmp "$BREAKDOWN_REPLAY_DIR/analysis.csv" \
  experiments/results/predicted_history_breakdown.csv
```

The K=0 receipt authenticates 12,324 successful terminal outputs over all 4,108
rows for each of three model variants. The lexical-stress receipt authenticates 1,500
successful outputs over the fixed 500-row set for each model variant. Both receipts
report zero terminal errors, exact parent response-version-set comparability,
and zero cumulative retries.

Generate the current reviewer-facing figures from aggregate data only:

```bash
uv run --script paper/figures/gen_fig_predicted_history_concepts.py
uv run --script paper/figures/gen_fig_predicted_history_extension_v1.py
```

The concept generator validates the prediction-only serving contract. The
versioned extension generator reads only the checked aggregate extension
analysis and renders K=0 as the fixed-interface curve origin with the
no-history-interface prompt control detached. Both create PDF, 300-DPI PNG,
and receipts binding input, generator, and output hashes; neither opens raw
predictions or transcripts. The generated data, three result figures, and receipt passed
a byte-identical fresh-directory replay, format/font checks, hermetic tests,
and visual inspection.

The separate post-outcome breakdown uses the same authenticated main panel and launches no provider calls. Its JSON records the exact locked-environment fresh-output argument vector and three byte-comparison vectors. Reproduction requires the ignored private binding and the same owner-only HMAC salt whose hash appears in the result; [`paper/README.md`](README.md#reproducing-publication-tables-and-figures) gives the public presentation-replay command.

The sanitized aggregate paper package was explicitly authorized and published on the repository's [`paper/context-interface-public`](https://github.com/watch-duty/radio-transcription/tree/paper/context-interface-public) branch. That authorization does not cover private audio, references, manifests, predictions, request histories, attempt logs, model identifiers, cloud project identifiers, source keys, or other withheld artifacts.

## J. Submission gates and residual limitations

The scientific draft and sanitized aggregate package are complete for internal review, but submission still requires authorship and affiliation approval, venue selection, and final privacy/licensing sign-off. The public-package allowlist is complete; it does not authorize withheld data or provider outputs.

The requested evaluation program is complete; no additional provider run is a
submission action. Held-out source-separated confirmation, matched history-free
and prior-transcript SFT jobs, stochastic training replicates, and blinded audio
review were not requested. Their absence limits the paper to exploratory
development evidence, descriptive historical model-variant comparisons, and lexical
safety diagnostics. The fixed-interface K=0 control has been completed and is no
longer a missing experiment.
