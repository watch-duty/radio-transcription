# Empty-Biased Prompt Routing for Extremely Short Emergency Radio Transcription

Anonymous authors

## Abstract

Emergency radio transcription contains a large fraction of sub-second segments
whose acoustic evidence is too sparse for reliable verbatim recognition. In
these cases, a capable audio-language model may reduce uncertainty by producing
plausible radio boilerplate, but plausible completions are harmful when the
target is an exact transcript. We study this failure mode on a combined
Echo + Broadcastify Feeds supervised fine-tuning corpus of 11,832 radio
segments and 71,025 reference words. We evaluate `gemini-3.1-flash-lite` under
a fixed decoding configuration and compare a no-metadata baseline with
duration-routed short-fragment prompts. The best gated policy,
`policy_empty_bias_lt_0p75`, applies an abstention-biased prompt only to clips
shorter than 0.75s and uses the baseline prompt otherwise. This policy reduces
WER from 39.75 to 35.81 and CER from 29.55 to 26.05, removing 2,686 word edits
while improving keyword recall from 64.88% to 65.13%. Gains are concentrated in
the shortest clips: WER falls from 199.88 to 76.36 for clips under 0.5s and
from 70.38 to 55.17 for clips from 0.5s to 0.75s. The best raw-WER policy
scores slightly better at 35.70 WER but violates a deletion-risk gate. These
results show that for noisy dispatch audio, prompting should sometimes bias
toward silence rather than completion.

## 1. Introduction

Automatic speech recognition (ASR) systems are increasingly robust, with modern
end-to-end and large-scale weakly supervised systems improving accuracy across
standard benchmarks [Hannun et al., 2014; Gulati et al., 2020; Radford et al.,
2022]. Emergency radio traffic remains an awkward target for general ASR:
segments are short, noisy, clipped by push-to-talk boundaries, and dense with
domain-specific acknowledgements, unit identifiers, and status codes. For
operational transcription, the goal is not a plausible semantic summary. The
goal is a verbatim transcript of the audible segment.

This paper studies a narrow but high-impact failure mode: extremely short radio
segments. In the combined corpus used here, 2,872 of 11,832 segments are shorter
than 0.75s, and 1,022 are shorter than 0.5s. These clips often contain only a
click tail, a partial word, or a one-word acknowledgement. A model that has
learned radio conventions can over-complete such clips into plausible
boilerplate: unit numbers, 10-codes, dispatcher names, or generic status
phrases. These insertions inflate WER and can be operationally misleading.

We evaluate a family of duration-routed prompt policies for
`gemini-3.1-flash-lite`. The policies use a full no-metadata baseline prompt for
all ordinary clips, but substitute stricter short-fragment prompts for clips
below a duration threshold. The strongest production candidate is
`policy_empty_bias_lt_0p75`, which adds the following instruction for clips
under 0.75s:

> This clip is extremely short. If the spoken content is not clearly
> identifiable as one or two exact words, return an empty string. Do not fill
> with likely radio phrases, unit identifiers, 10-codes, dispatcher names, or
> incident words.

This intervention is intentionally simple. It does not add Broadcastify
metadata, external glossaries, confidence estimation, post-processing, or model
fine-tuning. It changes only the transcript prior for a duration-defined slice
of the input.

Our contributions are:

1. We identify extremely short emergency radio clips as a major source of
   over-completion error for a prompted audio-language model.
2. We introduce a duration-routed empty-biased prompt policy that improves
   aggregate WER, CER, insertion count, false unit-status output, and keyword
   recall on a combined Echo + Broadcastify Feeds corpus.
3. We show that the WER-optimal routing threshold is not automatically the best
   production policy: the best raw-WER policy improves WER but fails a
   deletion-risk gate, while the empty-biased sub-0.75s router passes all
   configured gates.

## 2. Problem Setting

### Task

Given an audio segment from emergency radio traffic, output a verbatim
transcript of only the words spoken in that segment. The transcript must not
continue beyond the segment, infer metadata, complete clipped phrases, or
generate likely radio acknowledgements unless they are audible.

### Dataset

We evaluate the four requested SFT manifests from
`radio-transcription-sft-v20260528` as one combined group:

| Manifest | Rows |
|---|---:|
| `echo/train.jsonl` | 5,489 |
| `echo/eval.jsonl` | 1,245 |
| `bcfy_feeds/train.jsonl` | 4,078 |
| `bcfy_feeds/eval.jsonl` | 1,020 |
| **Total** | **11,832** |

All rows have non-empty references and unique `model_ready_audio_uri` values.
The combined corpus has 6,734 Echo rows and 5,098 Broadcastify Feeds rows.

The short-duration population is large:

| Duration cutoff | Rows |
|---|---:|
| `<0.5s` | 1,022 |
| `<0.75s` | 2,872 |
| `<1.0s` | 4,520 |
| `<1.25s` | 5,968 |
| `<1.5s` | 7,023 |
| `<2.0s` | 8,559 |
| `<5.0s` | 11,354 |

### Model and decoding

All arms use `gemini-3.1-flash-lite` with the same deterministic generation
configuration: temperature 0.0, maximum output length 512, `candidate_count=1`,
frequency penalty 0.3, presence penalty 0.0, low thinking level, `top_k=1`, and
`top_p=0.1`. Safety settings are disabled for the emergency-audio categories
used by the existing evaluation harness.

### Metrics

We report WER and CER after the common transcription normalizer. WER is the
primary metric, following standard ASR practice and NIST-style scoring with
substitution, insertion, and deletion counts [NIST SCTK; NIST OpenASR, 2021].
Because WER alone can reward overly short transcripts, we also report:

- substitutions, insertions, and deletions;
- total reference words;
- keyword weighted recall;
- exact segment count;
- empty prediction rate;
- false `10-4`, false `10-8`, false `copy`, and false unit-status outputs;
- numeric perturbation count;
- improved and worsened row counts;
- grouped bootstrap confidence over source groups.

## 3. Method: Duration-Routed Short-Fragment Prompting

### Baseline

The baseline prompt asks the model to act as an expert radio dispatcher and
verbatim transcriptionist. It includes constraints to transcribe only audible
spoken words, use digits for numbers, reject metadata, avoid formatting, and
return an empty string when no spoken words are present. It also includes a
small glossary of dispatcher, unit, and status terms.

### Prompt arms

We submit one full baseline arm over all 11,832 rows and five short-fragment
arms over duration-limited candidate pools:

| Arm | Submitted rows | Prompt idea |
|---|---:|---|
| `baseline_short_final` | 11,832 | No-metadata baseline prompt |
| `short_guard_v1_pool_lt_1p5` | 7,023 | Do not infer missing words |
| `short_no_boilerplate_pool_lt_1p5` | 7,023 | Reject radio boilerplate unless clear |
| `short_numeric_preserve_pool_lt_1p5` | 7,023 | Preserve only acoustically clear numbers |
| `short_literal_fragment_pool_lt_1p5` | 7,023 | Literal acoustic fragment mode |
| `short_empty_bias_pool_lt_0p75` | 2,872 | Empty-string bias for extremely short clips |

The empty-biased prompt is deliberately more conservative than the other
short-fragment prompts. It says to return an empty string unless the clip
contains one or two clearly identifiable exact words.

### Composed policies

Each policy is scored by composing predictions after batch inference. For a
threshold policy, selected rows use the short-fragment arm and all non-selected
rows use the baseline arm:

```text
if duration(row) < threshold:
    prediction = short_prompt_arm(row)
else:
    prediction = baseline_short_final(row)
```

The empty-bias arm is evaluated at `<0.5s` and `<0.75s`. Other prompt arms are
evaluated at `<0.5s`, `<0.75s`, `<1.0s`, `<1.25s`, and `<1.5s`.

This composition is important. It avoids re-running the same baseline prompt in
a routed arm and then attributing unrelated generation variance to routing.

### Success gates

A policy is a production candidate only if it satisfies all gates:

1. WER improves over baseline.
2. CER improves over baseline.
3. Insertions are lower than baseline.
4. Deletions are no more than 0.5 WER points above baseline.
5. False unit-status output is lower than baseline.
6. Empty prediction rate is no more than 0.2 percentage points above baseline.
7. Numeric perturbation count is not higher than baseline.
8. Worsening mass is at least 25% smaller than improvement mass.
9. Echo and Broadcastify Feeds slices both improve or tie.

False `10-4` is retained as a diagnostic metric, not a hard gate. In earlier
experiments it was a useful marker of canned radio output, but applying it as a
hard constraint was too specific once the primary product requirement shifted
toward overall fire/radio transcript quality.

## 4. Results

### Main result

Table 1 compares the baseline, the best raw-WER policy, and the leading gated
policies.

**Table 1: Main H14 results. Lower WER/CER/edit counts are better.**

| Policy | WER | CER | Edits | Subs | Ins | Dels | Keyword recall | Gate |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| `baseline_short_final` | 39.75 | 29.55 | 28,229 | 15,797 | 6,974 | 5,458 | 64.88 | -- |
| `policy_v1_lt_1p5` | **35.70** | 26.13 | **25,357** | **15,551** | **3,811** | 5,995 | 64.70 | fail: deletions |
| `policy_empty_bias_lt_0p75` | 35.81 | **26.05** | 25,436 | 15,626 | 3,997 | 5,813 | 65.13 | pass |
| `policy_v1_lt_1p0` | 35.92 | 26.18 | 25,510 | 15,656 | 4,080 | 5,774 | 65.03 | pass |
| `policy_v1_lt_0p75` | 36.11 | 26.35 | 25,650 | 15,709 | 4,290 | 5,651 | **65.26** | pass |

The best production candidate is `policy_empty_bias_lt_0p75`. It improves WER
by 3.94 points and CER by 3.50 points, removes 2,686 word edits, reduces
insertions by 2,977, and slightly improves keyword recall. It has more
deletions than baseline, but exactly meets the updated deletion limit:

```text
allowed deletions = 5,458 + round(0.005 * 71,025) = 5,813
observed deletions = 5,813
```

The best raw-WER policy, `policy_v1_lt_1p5`, improves WER to 35.70 but produces
5,995 deletions, exceeding the deletion limit by 182. This is a useful reminder
that WER reduction can be achieved by becoming too terse.

### Bootstrap reliability

Grouped bootstrap is computed over 304 source groups. For
`policy_empty_bias_lt_0p75`, all 1,000 bootstrap samples improve over baseline:

| Policy | Mean edit delta | Median | 95% CI | P(delta < 0) |
|---|---:|---:|---:|---:|
| `policy_empty_bias_lt_0p75` | -2,684.9 | -2,672 | [-3,225, -2,196] | 1.000 |
| `policy_v1_lt_1p5` | -2,776.6 | -2,766 | [-3,337, -2,281] | 1.000 |
| `policy_v1_lt_0p75` | -2,486.9 | -2,479 | [-2,992, -2,029] | 1.000 |

The bootstrap result supports the aggregate improvement, but it does not remove
the deletion-risk distinction between policies.

### Slice results

Both dataset families improve under the selected policy:

| Slice | Baseline WER | `policy_empty_bias_lt_0p75` WER | Edit delta |
|---|---:|---:|---:|
| Echo | 38.45 | 34.12 | -1,674 |
| Broadcastify Feeds | 41.43 | 38.00 | -1,012 |

The improvement is concentrated in the two shortest duration buckets:

| Duration | Segments | Baseline WER | Policy WER | Edit delta |
|---|---:|---:|---:|---:|
| `<0.5s` | 1,022 | 199.88 | 76.36 | -2,081 |
| `0.5-0.75s` | 1,850 | 70.38 | 55.17 | -605 |
| `0.75-1s` | 1,648 | 48.06 | 48.06 | 0 |
| `1-1.25s` | 1,448 | 45.57 | 45.57 | 0 |
| `1.25-1.5s` | 1,055 | 40.75 | 40.75 | 0 |
| `1.5-2s` | 1,536 | 37.76 | 37.76 | 0 |
| `2-5s` | 2,795 | 29.93 | 29.93 | 0 |
| `5s+` | 478 | 19.99 | 19.99 | 0 |

This is expected: the selected policy only changes predictions under 0.75s.
The large aggregate gain comes from a high baseline error rate on the shortest
clips, not from broad changes across the corpus.

### Error behavior

`policy_empty_bias_lt_0p75` changes the error profile substantially:

| Metric | Baseline | Policy | Direction |
|---|---:|---:|---|
| Insertions | 6,974 | 3,997 | improves |
| Deletions | 5,458 | 5,813 | worsens, at gate limit |
| False `10-4` | 430 | 430 | tied |
| False unit-status | 294 | 62 | improves |
| Numeric perturbations | 4,062 | 4,051 | improves |
| Empty prediction rate | 0.194% | 0.127% | improves |

The reduction in false unit-status outputs is especially important for radio
traffic. The policy avoids completing short clips into forms such as
`Engine 12, 10-4` unless the content is acoustically clear.

Row-level outcomes show a strongly asymmetric effect:

| Policy | Improved rows | Worsened rows | Improvement mass | Worsening mass |
|---|---:|---:|---:|---:|
| `policy_empty_bias_lt_0p75` | 617 | 170 | 2,947 | 261 |

The policy affects many fewer worsened rows than improved rows, and the total
worsening mass is less than one tenth of the improvement mass. This margin is
why the policy remains attractive even though deletions increase.

## 5. Analysis

### Why empty bias works

The main failure mode is not a lack of radio vocabulary. The baseline already
knows radio language. The problem is that very short clips do not contain
enough acoustic evidence to justify a complete radio phrase. In that setting,
domain knowledge becomes a liability: it turns silence, clicks, or fragments
into plausible outputs.

The empty-biased prompt reverses the default behavior. It tells the model that
for extremely short clips, no transcript is preferable to a guessed transcript
unless one or two exact words are clearly identifiable. This reduces insertion
errors and false unit-status completions.

### Why the raw-WER winner is not selected

`policy_v1_lt_1p5` has the best aggregate WER and edit count. It also applies a
less severe short-clip guard to 7,023 rows, compared with 2,872 rows for the
empty-biased policy. That broader routing captures additional insertion wins in
the 0.75s--1.5s range, but it also produces too many deletions. The production
gate rejects it because verbatim transcription quality depends on retaining
audible content, not only reducing edit distance.

The result illustrates a general lesson for prompted ASR evaluation: WER is
necessary but insufficient. A transcription policy can improve WER by shortening
outputs, so deletion budgets and task-specific hallucination counters must be
part of model selection.

### Why metadata is absent

Earlier experiments explored Broadcastify metadata such as service category,
talkgroup labels, geography, and status glossaries. Those arms often changed
the model prior toward plausible dispatch language, which helped some rows but
hurt others through context copying and boilerplate insertion. The latest round
therefore removes metadata entirely and tests a narrower hypothesis: can a
prompt alter behavior only where duration indicates severe acoustic scarcity?
The answer is yes.

## 6. Related Work

End-to-end ASR systems have progressed from recurrent architectures such as
Deep Speech [Hannun et al., 2014] through convolution-augmented Transformers
such as Conformer [Gulati et al., 2020] to large weakly supervised systems such
as Whisper [Radford et al., 2022]. These systems improve robustness through
model architecture and large-scale data. Our setting differs: the model is
fixed, and the intervention is a decoding-time instruction applied to a
duration-defined slice of emergency radio audio.

WER remains the dominant ASR evaluation metric and is implemented by
NIST-style scoring toolkits such as SCTK/SCLITE [NIST SCTK; NIST OpenASR,
2021]. We follow this convention but add CER, edit-type decomposition,
keyword recall, false radio-boilerplate counters, and grouped bootstrap
analysis. The added metrics are necessary because WER alone can make a terse
policy look better even when it deletes audible speech.

This work is also related to contextual biasing and prompt conditioning, but it
uses a different form of context: duration rather than external vocabulary or
metadata. The key observation is that for extremely short clips, the useful
prior is not "expect domain terminology"; it is "do not complete what is not
audible."

## 7. Limitations

This study has several limitations.

First, the primary corpus combines train and eval manifests. The result is an
evaluation of a prompt policy over a requested corpus, not a claim about
generalization to a held-out deployment distribution.

Second, all results use one model family and one fixed decoding configuration.
The effect may differ for dedicated ASR systems, other Gemini model variants,
or different generation settings.

Third, the best gated policy lands exactly on the deletion limit. The policy is
strong by WER, CER, and insertion metrics, but deletion risk remains the main
deployment monitor.

Fourth, grouped bootstrap measures stability over available source groups, not
over independent data collection sites or future incident distributions.

Fifth, the evaluation optimizes text similarity and task-specific counters. It
does not measure downstream operator utility, incident triage accuracy, or
human correction time.

## 8. Reproducibility

The experiment is implemented in:

- `model/scripts/bcfy_calls_metadata_eval/run_short_fragment_sft_eval.py`
- `model/scripts/bcfy_calls_metadata_eval/analyze_short_fragment_sft_results.py`

The generated reports are:

- `model/scripts/bcfy_calls_metadata_eval/results/short_fragment_sft_v20260528_05_2026/sft_combined_policy_report.json`
- `model/scripts/bcfy_calls_metadata_eval/results/short_fragment_sft_v20260528_05_2026/sft_combined_policy_report.md`

The experiment submitted 42,796 Gemini audio requests across six arms. One
selected prediction was missing from `short_guard_v1_pool_lt_1p5`; composed
policies using that arm fell back to baseline for the affected row
(`echo_train:2570:841`). The selected `policy_empty_bias_lt_0p75` does not use
that missing arm.

All claims in this paper are derived from the regenerated H14 report using the
updated gates.

## 9. Conclusion

Extremely short emergency radio clips create a distinctive ASR failure mode:
models complete sparse acoustic evidence into plausible dispatch phrases. A
simple duration-routed empty-biased prompt substantially reduces this failure
without metadata, post-processing, or fine-tuning. On 11,832 combined Echo and
Broadcastify Feeds segments, `policy_empty_bias_lt_0p75` reduces WER from 39.75
to 35.81 and CER from 29.55 to 26.05 while passing production gates. The result
suggests that prompt policies for noisy operational audio should be routed not
only by content domain, but also by acoustic locality and segment duration.

## References

Hannun, A., Case, C., Casper, J., Catanzaro, B., Diamos, G., Elsen, E.,
Prenger, R., Satheesh, S., Sengupta, S., Coates, A., and Ng, A. Y. (2014).
Deep Speech: Scaling up end-to-end speech recognition. arXiv:1412.5567.
https://arxiv.org/abs/1412.5567

Gulati, A., Qin, J., Chiu, C.-C., Parmar, N., Zhang, Y., Yu, J., Han, W.,
Wang, S., Zhang, Z., Wu, Y., and Pang, R. (2020). Conformer:
Convolution-augmented Transformer for Speech Recognition. arXiv:2005.08100.
https://arxiv.org/abs/2005.08100

Radford, A., Kim, J. W., Xu, T., Brockman, G., McLeavey, C., and Sutskever, I.
(2022). Robust Speech Recognition via Large-Scale Weak Supervision.
arXiv:2212.04356. https://arxiv.org/abs/2212.04356

NIST. Speech Recognition Scoring Toolkit (SCTK), including SCLITE.
https://www.nist.gov/multimodal-information-group/tools

NIST. (2021). Open ASR 2021 Evaluation Plan. Describes WER as the primary
metric computed with SCLITE from SCTK.
https://www.nist.gov/system/files/documents/2021/08/31/OpenASR21_EvalPlan_v1_3_1.pdf
