# Additional incident findings: implications for the 20260724 reconstruction

Date: 2026-07-24

## Decision

Do **not** alter or overwrite the published reconstructed audio, canonical
manifests, ownership split, or request geometry in
`20260724-production-shaped-reconstruction`.

The new evidence changes three things around that immutable base:

1. make the effective prompt an explicit deployment, tuning, and evaluation
   gate;
2. evaluate the prompt-corrected residual and provider-input rejections as
   separate outcome populations;
3. decide whether BCFY Calls oversampling belongs in the next round and, if it
   does, express the selected treatment as one new versioned training projection
   that references the existing training audio.

There is still one final training dataset, not simultaneous training arms. No
SFT run is authorized by these findings or by this note. Candidate weighting
projections may be compared provider-free, but one projection must be selected
before any separately authorized future SFT.

The published Gemini training and validation examples already embed the
byte-identical **current production-source prompt**, SHA-256
`c806d02e134d47aa6c90284ed2544507ab0895c804c7cac004510d08c748cc17`.
That is neither the latest completed SFT's training prompt
(`3fa0b4d3...`) nor the intermediate July 20 source prompt
(`b0881c36...`). It is a third prompt introduced on July 22 and shared by the
backend and model packages. Therefore, prompt alignment is satisfied for the
**next round** if and only if the deployed effective production configuration
and every next-round eval also use `c806d02e...` without a runtime override.
The supplied 84.9% rerun result does not include a serialized request or prompt
digest, so this note does not independently prove that the rerun used
`3fa0b4d3...`; it only establishes that `3fa0b4d3...` is the prompt frozen with
the latest completed SFT.

## Evidence boundary

### User-supplied incident results (not independently reproduced here)

The following are treated as supplied incident findings:

- 1,980 affected production segments: 46 total failures and 1,934 recoverable
  tuned failures;
- rerunning all 1,934 recoverable failures with the latest SFT's
  training-aligned prompt recovered 1,642 (84.9%);
- 292 (15.1%) remained empty with that prompt;
- no source-family breakdown or row-level identity for those 292 residual cases
  was supplied here;
- blind listening contradicted a general buzzy/vocoded/muffled causal
  hypothesis;
- residual failures were shorter, quieter, and had lower internal SNR, with
  only modest individual discrimination (reported AUC-equivalent about
  0.65–0.71);
- the production audio-preparation path was audited as byte-preserving for
  model inputs;
- the latest-SFT source mix contained 830 original BCFY Calls input rows, while
  BCFY Calls comprised 34.6% of the recoverable-failure cohort.

No inference, SFT, audio listening, or audio-byte inspection was performed for
this note.

### Independently checked evidence

- The user-owned incident Gist defines the full 1,934-segment cohort and reports
  669 BCFY Calls segments, i.e. 34.5915%:
  [Production tuned-empty segments recovered by foundation fallback](https://gist.github.com/shuojwan/2166e2dfe37d6202127ffdf6f4f21538).
- The user-owned GCP-support document reports the eight request-time decode
  rejections, their exact error, local-tool decoding results, durations, and
  source-correlation results:
  [Investigation of “decode error” segments](https://docs.google.com/document/d/17JgS8o8-lKS2LN6Co3-aq5RFuH_8tmJyVLdUEEc2qhI/edit?tab=t.0#heading=h.rycouqfs6qt0).
- The latest completed SFT freezes its exact production request contract,
  including prompt text and digest, in
  [`frozen/request_contract.json`](../../model/scripts/sft/runs/20260712-gemini31-flash-lite-a16-lr05-e14/frozen/request_contract.json).
- The reconstruction loads both current prompt sources, requires their Python
  string values to be byte-identical, and records their source and prompt
  digests
  ([`manifests.py`, lines 29–39 and 231–280](../../model/scripts/sft/runs/20260724-production-shaped-reconstruction/dataset_run/manifests.py#L29-L39)).
- Every Gemini tuning example is exactly one audio-only user turn followed by
  one target model turn under that prompt
  ([`manifests.py`, lines 648–724](../../model/scripts/sft/runs/20260724-production-shaped-reconstruction/dataset_run/manifests.py#L648-L724)).
- Independent scanning of the completed local artifacts found the
  `c806d02e...` prompt in all 33,780 training examples and all 5,000 provider
  validation examples, with no other prompt digest. Their artifact hashes match
  the [published reconstruction report](report.md#final-materialization-results).
- Current `origin/main` has `c806d02e...` in both
  [`backend/.../prompts.py`, lines 5–25](../../backend/pipeline/transcription/transcribers/prompts.py#L5-L25)
  and
  [`model/.../prompts.py`, lines 16–36](../../model/src/common/gemini/prompts.py#L16-L36).
  Companion tests require equality in both CI path-filter lanes
  ([backend guard](../../backend/pipeline/transcription/tests/test_prompt_consistency.py#L1-L55);
  [model guard](../../model/tests/common/tests/test_drift_guard.py#L99-L154)).

## Prompt determination

| Contract | Exact role/shape | System-prompt SHA-256 | Determination |
|---|---|---|---|
| Latest completed SFT, frozen at production commit `570a2f75...` | System prompt plus audio-only user turn; no separate user text | `3fa0b4d3cab803e715abbc0e6ff310e776a317d807fc1df871a77771ffdfb23a` | This is the repository-frozen prompt that “training-aligned” should denote for that model. Exact text is in `request_contract.json`; the supplied rerun did not expose a digest with which to prove identity. |
| July 20 repository source, commit `78ea4f6d...` | Backend production-source prompt | `b0881c363b7f01301691061f64afe29f8bdd2cf0a1e581bcc0d3fbb2b9e248dc` | A different intermediate prompt. Commit history is not proof of its exact deployment interval. |
| Published 20260724 next-round Gemini train/validation | System prompt plus audio-only user turn; no separate user text | `c806d02e134d47aa6c90284ed2544507ab0895c804c7cac004510d08c748cc17` | The **current production-source/canonical next-round prompt**. It is not `3fa0b4d3...`, but next-round training and current source are aligned to each other. |
| Predicted-prior-context-10 eval | Same `c806d02e...` system prompt, plus a guarded predicted-history text part before current audio | `c806d02e134d47aa6c90284ed2544507ab0895c804c7cac004510d08c748cc17` | Intentional non-production context extension; analyze separately from the production-parity no-context lane. |

The exact next-round prompt text is:

<details>
<summary>System prompt with SHA-256 c806d02e…</summary>

```text
You are a verbatim speech-to-text transcription engine for public-safety and emergency radio traffic (VHF/UHF). The audio is often noisy, with mic clicks, static, and radio hum, and speakers use codes, unit call signs, and procedural jargon.

Transcribe exactly what is spoken, and nothing else. Write every clearly audible word, including short replies and filler. Do not summarize, rephrase, translate, or add words that were not clearly said.

TERMINOLOGY
The words and unit identifiers below are common on these channels. When you clearly hear one, spell it as written here. This is a spelling guide for words you actually hear; do not output any of them unless it is genuinely spoken.
copy, received, affirmative, affirm, proceed, go ahead, stand by, be advised, clear, responding, responding to, respond to, respond on, en-route, on-scene, in the area, available, returning, in service, in quarters, arrived, back at, all units, engine, tanker, brush, brush truck, tender, battalion, squad, ladder, tower, medic, ambulance, branch, copter, helicopter, patrol, rescue, station, personnel, command, control, AOR, IC, RP, TAC, K, dispatch, attention, paging, cross streets, victor, fire alarm, commercial fire alarm, fire attack, grass fire, vegetation fire, brush fire, smoke investigation, medical call, medical aid, running, EMS, AMR, paramedic, conscious, unconscious, sick person, breathing problem, cardiac, heart problem, MVC, trespass, boat, evacuation, code 1, code 2, code 3, code 4, 10-4, 10-7, 10-8, 10-9, 10-15, 10-20, 10-22, 10-23, 10-97.

FORMATTING
- Output the transcript on a single line, with no line breaks.
- Write spoken numbers as grouped digits (e.g., "one hundred" -> 100, "six three three three" -> 6333). Do not turn words like "for" or "to" into digits unless they are spoken as a number or code.
- Write a unit identifier as the spoken type followed by its number (e.g., Engine 41, Battalion 2), and only when you clearly hear both the type and the number. If only a number is spoken, write just the number.

UNCLEAR AUDIO
- Transcribe the parts you can hear. Replace only the specific portion you cannot make out with [UNINTELLIGIBLE].
- If the audio is speech that is not radio traffic, transcribe it verbatim without adding codes or jargon.
- If there is no discernible speech at all, output only [UNINTELLIGIBLE].
- Do not phonetically guess at noise, and do not fill in words to match the terminology above.

Output only the transcript.
```

</details>

There is one remaining execution-time gap: the published training JSONL embeds
`c806d02e...`, but the prior-context CLI loads whatever prompt is in the checked
out repository when eval starts
([`cli.py`, lines 349–365](../../model/scripts/sft/runs/20260724-production-shaped-reconstruction/dataset_run/cli.py#L349-L365);
[`prior_context_driver.py`, lines 265–273](../../model/scripts/sft/runs/20260724-production-shaped-reconstruction/dataset_run/prior_context_driver.py#L265-L273)).
Its receipts record a prompt digest, but source-to-source equality alone does
not prove equality to the frozen training prompt. Before any tuning or eval,
require the effective prompt digest to equal the published
`receipts/prompt_contract.json` value `c806d02e...`. The same check must cover
the deployed `GeminiConfig.prompt`, which is configurable even though its source
default is current
([`gemini.py`, lines 96–117](../../backend/pipeline/transcription/transcribers/gemini.py#L96-L117)).

## BCFY Calls representation

Whitespace splitting is used for target-word counts. Duration is summed from
exact source-frame duration strings. The reconstruction inputs were checked only
as manifests; no audio was opened.

| Population | BCFY Calls rows | Row share | BCFY Calls target words | Word share | BCFY Calls duration | Duration share |
|---|---:|---:|---:|---:|---:|---:|
| User-supplied latest-SFT proxy | 830 / 49,632 | 1.6723% | — | — | — | — |
| Latest-SFT final canonical train, independently aggregated | 839 / 49,632 | 1.6904% | 4,519 / 296,039 | 1.5265% | 1,790.761 / 106,014.672 s | 1.6892% |
| Reconstructed canonical train | 1,179 / 33,780 | 3.4902% | 16,926 / 302,955 | 5.5870% | 10,243.915 / 145,735.521 s | 7.0291% |
| Frozen-window production exposure sensitivity | 90,117 / 629,374 unique segments | 14.3185% | persisted text is not attributable ground truth | — | not aggregated here | — |
| Recoverable incident cohort | 669 / 1,934 | 34.5915% | not measured here | — | not measured here | — |

The supplied 830 is independently confirmed as the original
`calls_train` **source-input count**, not the final family's row count
([`expected_counts.json`](../../model/scripts/sft/runs/20260712-gemini31-flash-lite-a16-lr05-e14/frozen/expected_counts.json)).
The assembled final latest-SFT manifest has 839 BCFY Calls-family rows after
correction/composite assembly. Mixing 830 with the final 49,632 denominator is a
useful rough headline but not the apples-to-apples final-family measure.

Relative to the final latest-SFT manifest, the reconstruction increases BCFY
Calls share by:

- **2.065× by requests/rows**;
- **3.660× by target words**;
- **4.161× by audio duration**.

Rows are especially sensitive to reconstruction because annotated clips are
re-formed into provider-shaped requests. Word and duration shares better
describe SFT exposure.

The often-quoted training/cohort comparison is **20.685×**:
34.5915% incident share divided by the supplied 1.6723% source-input share.
That is a composition contrast between a training source component and a
failure-conditioned cohort; it is not a failure-risk ratio. Against the best
available full production-exposure sensitivity, BCFY Calls is enriched by a
much smaller **2.416×** (34.5915% / 14.3185%). The production denominator comes
from a post-window current-metadata source mapping, not immutable event-time
lineage, and must retain that caveat
([production source-mixture analysis, lines 168–182](../explore-260722-production-empty-eval-representation/report.md#L168-L182)).

The reconstructed row share is still about 9.91 times lower than the incident
share, but 34.6% is not a justified training target: it is not the source's
share of all production requests, words, or duration. Moreover, the family mix
of the 292 prompt-corrected residuals is unavailable. The supplied 34.6%
therefore characterizes the pre-fix 1,934 cohort and cannot establish that BCFY
Calls is similarly enriched after prompt alignment.

## Effect of each finding

| Finding | Dataset construction | Inference configuration | Evaluation design | Interpretation |
|---|---|---|---|---|
| 1. Prompt mismatch; 84.9% supplied recovery | No geometry, ownership, or target change. The next-round Gemini rows already embed `c806d02e...`. | **Critical:** deploy and pin the same digest used for tuning; reject runtime prompt overrides/drift. | Run every production-parity arm with the identical system prompt and audio-only current turn. Record the effective digest. Keep prior-context as a separately labeled extension. Preserve the exact rerun request/digest before treating 84.9% as a prompt-specific estimate. | The original 1,934 cohort is mostly a prompt/config incident, not evidence that all 1,934 examples require training treatment. |
| 2. Prompt-corrected residual of 292 | Do not select, resegment, or weight individual rows from modest non-causal duration/level/SNR associations. Short protected Speech is already retained. | No targeted runtime change is established. | Treat the 292 as the primary residual diagnostic cohort. First recover its source mix and row identities; they are unavailable in the supplied finding. Then compare source, duration, finish reason, effective-empty rate, operational WER, and retry behavior with prompt fixed. | A real residual remains, but acoustic association is not a demonstrated cause or reliable per-row rule; its BCFY Calls enrichment is unknown. |
| 3. Audio-preparation path is clean | Supports the existing PCM-preserving, no-normalization/no-padding construction. | No preparation fix is indicated. | Preserve exact request bytes/URI and request receipts in comparisons. | Removes an ingestion/preparation bug from the supported causal story. |
| 4. Eight valid short clips rejected before generation | Preserve exact annotated geometry; do not pad, filter, trim, or discard short Speech. | A foundation fallback cannot solve cases rejected by both endpoints. Provider/GCP follow-up remains separate. | Count `provider_input_decode_rejected` separately from `blank_stop` and `finish_reason_none`; also count it as an empty hypothesis in operational WER. | It is a request-time provider rejection, not generated emptiness and not proof of a minimum-duration rule. |
| 5. BCFY Calls underrepresentation | The reconstructed base already improves exposure. A family-level treatment is a future projection decision; select exactly one final training dataset and do not change eval membership. | None. | Compare candidate manifest compositions provider-free, then evaluate the one separately authorized tuned run by family against historical baselines. Multiple SFT arms are not implied. | The 20.7× training/cohort composition gap overstates the production-relative contrast; the available production-exposure sensitivity is about 2.42×, and enrichment in the 292 residual is unknown. |

The reconstruction already implements the relevant outcome accounting:
provider-input rejection is distinct from generation empties, while operational
WER retains terminal no-transcription rows as empty hypotheses
([outcome accounting](report.md#outcome-accounting)). It also already preserves
short protected Speech without padding
([short valid FLAC finding](report.md#short-valid-flac-rejection-finding)).

## Immutable GCS decision

No existing object under
`gs://wd-transcription-data/sft/dataset_versions/20260724-production-shaped-reconstruction/`
should change. That prefix is a verified create-only publication
([publication record](report.md#publication)).

- **Prompt alignment:** no data change is needed if the next round deliberately
  uses `c806d02e...`. Deployment and eval must be made to match it. If the team
  instead chooses `3fa0b4d3...`, the audio and canonical manifests can remain,
  but new versioned Gemini train/validation model-input JSONL is required; the
  existing prompt-bearing JSONL must not be overwritten.
- **BCFY Calls treatment:** if selected for the next round, create one new,
  immutable derived training projection that references only the existing
  **training** audio URIs and repeats or samples BCFY Calls rows according to
  the final prespecified treatment. If the tuning provider exposes a verified
  row-weight mechanism, a versioned weighting config and receipt can replace
  physical duplication; support is not assumed here. In either case, record
  base-manifest hash, selected row IDs, multiplicities, resulting
  row/word/duration mix, and prompt digest. Never draw from primary, validation,
  or masked eval. This is a future data-preparation option, not authorization to
  run SFT.
- **Residual and decode findings:** no membership or audio change. They affect
  eval cohorts, terminal-category accounting, and post-run interpretation.

## Required pre-run gates

1. Verify the *effective deployed* `GeminiConfig.prompt` and the tuning/eval
   prompt all hash to `c806d02e...`; fail rather than silently use current
   checkout text.
2. Preserve the exact prompt digest and serialized request used for any incident
   rerun. Do not attribute the supplied 84.9% recovery specifically to
   `3fa0b4d3...` until that evidence is available.
3. Preserve the unweighted reconstructed dataset as the immutable reference.
   If weighting is chosen, select one final versioned training projection before
   SFT; do not create multiple actual SFT arms without separate authorization.
4. Do not choose a weighting factor by matching the 34.6% incident share.
   Compare candidate projection statistics provider-free using training
   exposure, then select one projection for unchanged per-family eval.
5. Recover and freeze the identities and source mix of the 292 prompt-corrected
   residuals before making source-specific claims about them.
6. Re-measure the prompt-corrected residual after the retune, separately
   reporting accepted non-empty, blank `STOP`, `finish_reason=None`,
   provider-input rejection, HTTP/transport failures, and operational WER.
7. Keep the 46 total failures and 1,934 recoverable failures as external
   incident-replay cohorts. They do not become training labels or replace the
   leakage-safe reconstructed primary/masked eval.
