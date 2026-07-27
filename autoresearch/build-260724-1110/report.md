# Next-round SFT dataset reconstruction

Status: **canonical reconstruction and approved publication complete; final
model-facing projection specified but not materialized**. The full canonical
request/audio universe is materialized, the independent verifier reports
`COMPLETE`, the actual Gemini SFT loaders and local preflight pass, and the
create-only GCS publication has been independently verified. A later exact
production source/sample-rate census now also selects the recommended
model-facing weighting and media projection described below. No SFT job or
provider inference was run.

## Outcome

The build produces one reconstructed training dataset and three evaluation
views:

1. the full reconstructed primary eval without prior context;
2. that same primary eval with up to ten preceding **predicted** transcripts
   from the same checkpoint and valid context episode;
3. the four existing `masked_v2/eval.jsonl` datasets, unchanged at row level
   and refiltered against final training ownership.

There is no atomic-unmasked compatibility eval and no new validation split.
Provider tuning validation is a deterministic subset of the full primary eval;
the full primary eval remains available for scoring.

The published reconstruction remains the immutable owner-complete reference,
not a prefix to mutate in place. The current next-round design derives one
model-facing training projection from it:

- BCFY Calls uses **uniform total multiplicity 3** for every training row;
- the former 12x-at-8-kHz / 6x-at-other-rates proposal is rejected;
- BCFY Feeds model-input audio is serialized as production-aligned 16 kHz mono
  for the derived train, primary-eval, and predicted-context views;
- BCFY Calls, Echo, and Fire Notifications retain their native request sample
  rates;
- masked eval remains the already-published unchanged historical view.

This derived projection has not yet been materialized or submitted to SFT.

## Frozen source universe and ownership

The source universe is the original audio and raw annotations behind the latest
SFT run. It contains 8,709 source objects. A non-empty transcript defines a
protected Speech owner; `[UNINTELLIGIBLE]` is non-empty Speech.

The frozen ownership ledger contains 65,324 protected Speech owners:

- 50,586 training owners;
- 14,738 primary-eval owners.

Every complete physical recording group that intersects the latest training
split is training-owned. This moves 942 historical Fire Notifications eval
owners into training and produces zero final train/eval recording-group
overlap. The four masked eval inputs retain 2,043 of 2,238 original rows after
the same ownership exclusion.

## Reconstruction policies

### BCFY Feeds

BCFY Feeds is continuous audio. Requests may merge adjacent annotated clips and
retain intervening targetless audio. Boundaries are selected with unlimited
annotation lookahead, never cut a non-empty annotated clip, exclude PII-only
frames, and do not use transcript text, word count, WER, model output, or error
status.

The soft objective minimizes exact Wasserstein-1 distance between the
reconstructed split-wide request-duration distribution and the frozen
production BCFY-feed duration distribution. Thirty seconds is only a very soft
anchor; matching production request geometry has higher priority. This
objective matches request-duration geometry only. It does not establish
production source-family prevalence or model-input sample-rate parity.

### BCFY Calls and Fire Notifications

Each source URI is an authoritative provider item. Its maximal retained
components are the provider item minus PII-only intervals. Speech overlapping
PII is retained because Speech wins the overlap; PII-only remainder is absent.
Positive targetless gaps do not split an otherwise retained component.

### Echo

Historical Echo source URIs are hour archives, not provider-item identities.
There is no authoritative transmission sidecar. Echo therefore merges only
strictly overlapping protected-Speech intervals. Exactly touching clips and
all positive gaps remain separate so unrelated transmissions from the same
hour are never invented as one request.

Across all presegmented families, 50,116 Speech owners become 33,284 requests
while preserving every owner exactly once.

## Transcript reconciliation

Target reconciliation is deterministic:

1. an authoritative correction wins;
2. duplicate or contained alternatives appear once, using the longest
   authoritative form;
3. one clear suffix-prefix overlap is merged once;
4. otherwise both transcripts are concatenated in chronological order.

Transcript contents do not influence audio boundaries.

## Frozen base audio serialization

Every output in the published canonical base is a single continuous
source-frame slice. There is no synthetic silence, concatenation of disjoint
intervals, resampling, downmixing, normalization, padding, trimming, filtering,
crossfade, or time stretching. Source sample rate, channel count, and PCM
subtype are preserved, and decoded output PCM must exactly equal the selected
source PCM.

The frozen family-specific encoders are:

- BCFY Feeds: Python SoundFile 0.13.1 / libsndfile 1.2.2 with statically
  embedded libFLAC 1.4.3. The exact `soundfile.py`, `_soundfile.py`, and loaded
  bundled `libsndfile_x86_64.so` artifacts are hash-pinned, and the loaded
  native object is resolved independently rather than inferred from a package
  path;
- BCFY Calls, Echo, and Fire Notifications: the exact deployed FFmpeg 6.1.1
  binary from normalization revision `normalization-service-prod-00032-zmz`,
  FLAC compression level 5.

Each row records the encoded hash and size, decoded PCM hash, encoder profile
and version, command digest, frozen encoder-contract digest, and—where
applicable—the exact FFmpeg binary and deployed-image identities.

Those guarantees describe the immutable published reference. The derived
model-facing projection selected later in this report deliberately reserializes
BCFY Feeds as 16 kHz mono. For those derived rows, verification binds the
immutable source identity, selected interval, frozen conversion contract, and
decoded derived-media hash; source/output PCM equality is no longer the
applicable invariant. The derived media and manifests require a new projection
identity and must not overwrite the published base.

## Post-publication incident findings

The detailed evidence and decision record is
[`additional-findings-20260724.md`](additional-findings-20260724.md).
Its incident evidence remains relevant, but its older production source-mixture
estimate is superseded by the exact bounded census below.

The user subsequently supplied results from a complete rerun of the 1,934
production segments for which every observed tuned attempt had lacked an
accepted transcript and a configured foundation fallback had produced a
non-empty transcript. With the training-aligned prompt, 1,642/1,934 (84.9%)
produced a non-empty tuned response; 292/1,934 (15.1%) remained empty. This is
strong evidence that prompt skew was the dominant cause in that selected
incident cohort. It is evidence about whether a transcript was produced, not
about the transcript's WER.

Prompt parity is therefore a launch contract, not a data-treatment heuristic.
This build:

- loads the production backend prompt and the model/SFT prompt independently
  and aborts unless their text is exactly equal;
- uses that same text in every training and provider-validation example;
- freezes system-prompt SHA-256
  `c806d02e134d47aa6c90284ed2544507ab0895c804c7cac004510d08c748cc17`
  and audio-only request-schema SHA-256
  `df148bc8c710b2c5ea56e3093410f6b746980e937875377610a640623be9e856`.

The repository prompt-parity guard was merged in
[PR #1094](https://github.com/watch-duty/radio-transcription/pull/1094).
The latest completed SFT froze a different system prompt, SHA-256
`3fa0b4d3cab803e715abbc0e6ff310e776a317d807fc1df871a77771ffdfb23a`.
That is the repository-frozen meaning of “training-aligned” for the completed
model; the current next-round prompt is a third, later contract. The rerun's
exact prompt bytes or digest were not supplied with the incident summary, so
the 84.9% result must not be attributed specifically to either digest without
that final comparison. It demonstrates the importance of alignment, not the
performance of the new prompt. Repository/build parity also does not, by
itself, prove which revision is deployed. The deployment and every eval request
must bind the same prompt and request-contract digests before SFT results are
interpreted.

The user also reported that blind listening contradicted the proposed
digital-radio distortion signature: it was more common among rerun successes.
The 292 residual failures were statistically shorter, quieter, and lower in
internal SNR than successes, but the individual predictive discrimination was
only about 0.65–0.71 and causality was not established. Those associations
therefore do not justify trimming, padding, gain changes, membership changes,
or outcome-informed boundaries. They remain post-freeze evaluation strata.

### BCFY Calls representation

The previous run's frozen inventory contains 830 original `calls_train` input
rows. After corrections and composite assembly, its final canonical manifest
contains 839 BCFY Calls-family rows among 49,632 rows (1.690%), 4,519/296,039
target words (1.527%), and 1,790.761/106,014.672 seconds (1.689%). The
reconstructed unique training dataset contains:

| BCFY Calls measure | Count | Share of reconstructed training |
|---|---:|---:|
| Requests | 1,179 | 3.490% |
| Protected Speech owners | 2,914 | 5.760% |
| Target words | 16,926 | 5.587% |
| Audio duration | 10,243.915 s | 7.029% |

BCFY Calls supplied 669/1,934 (34.592%) of the pre-fix recovered-failure
cohort. Comparing 34.592% with the original-input proxy of 830/49,632 (1.672%)
yields the reported 20.7-fold composition gap. The apples-to-apples comparison
with the final 839-row family share is 20.46-fold. Neither is a calibrated
failure risk or a justified sampling multiplier: both compare training rows
with an outcome-selected cohort and include failures that the prompt
correction removed.

The exact bounded production census below assigns 84,277/555,645 unique
requests (15.167%), 431,821 seconds (18.312% of requested duration), and
85,014/559,261 invocations (15.201%) to BCFY Calls, with zero missing source
lineage. Against unique-request exposure, 669/1,934 is enriched by about
2.281-fold, not 20.7-fold. This is still not calibrated marginal risk: the
incident cohort is outcome-selected and mostly predates the prompt fix.

The evidence supports treating BCFY Calls exposure as a high-priority SFT
sampling decision, but it does not support making 34.592% the training target.
The exact production census below resolves the source/sample-rate exposure
question and selects a uniform total multiplicity of 3 for BCFY Calls. The
published unique owner census remains unchanged. The one actual SFT input must
be an explicit, separately hash-bound rendering of this same training dataset;
it must not silently alter the immutable published prefix or create a second
semantic dataset.

### Exact production source and sample-rate census

A later audit measured the exact production window
`[2026-07-24 08:03:00 PDT, 2026-07-27 13:45:00 PDT)`. The primary unit is one
unique exact segment/model-input audio URI observed in a `Transcribing` event.
Application-level invocation counts are retained separately so Pub/Sub
redelivery or reprocessing cannot silently change the source/rate mixture.

Two independent full Cloud Logging reads produced the same 559,261-row
invocation identity set and the same per-partition content chain. Exact URI
deduplication produced 555,645 unique requests and 3,616 repeat invocations.
A settled log-based attempts metric counted 559,270 starts, nine more than the
stable row evidence. Those nine metric-only events have no URI/rate evidence
and were not synthesized.

Every one of the 555,645 unique requests resolved to an immutable
`audio_segments.feed_id -> feeds.source_type` lineage and a sample rate:

- all non-BCFY-Feeds requests used generation-bearing 42-byte FLAC
  `STREAMINFO` reads, with no decode or sample inspection;
- production ingestion forces BCFY Feeds to 16 kHz mono PCM16; a deterministic
  10,000-object header sample found 10,000/10,000 at that format, giving a
  one-sided 95% upper bound of 0.030% on an unobserved violation.

No OpenMHz request appeared in the bounded window.

The exact unique-request source distribution is:

| Source | Requests | Request share | Audio duration | Duration share |
|---|---:|---:|---:|---:|
| BCFY Feeds | 424,131 | 76.331% | 1,578,693.990 s | 66.946% |
| BCFY Calls | 84,277 | 15.167% | 431,821.000 s | 18.312% |
| Echo | 25,633 | 4.613% | 163,914.843 s | 6.951% |
| Fire Notifications | 21,604 | 3.888% | 183,740.017 s | 7.792% |
| **Total** | **555,645** | **100%** | **2,358,169.850 s** | **100%** |

Invocation weighting produces nearly the same mixture: BCFY Feeds 76.271%,
BCFY Calls 15.201%, Echo 4.630%, and Fire Notifications 3.897%. Redelivery
therefore does not materially bias the source result.

The exact sample-rate distribution is:

| Sample rate | Unique requests | Request share | Duration share | Invocations | Invocation share |
|---|---:|---:|---:|---:|---:|
| 8 kHz | 67,549 | 12.157% | 21.273% | 68,152 | 12.186% |
| 16 kHz | 478,713 | 86.154% | 77.291% | 481,657 | 86.124% |
| 22.05 kHz | 8,912 | 1.604% | 1.292% | 8,975 | 1.605% |
| 44.1 kHz | 153 | 0.028% | 0.078% | 155 | 0.028% |
| 96 kHz | 318 | 0.057% | 0.066% | 322 | 0.058% |

Rate is strongly confounded with source:

| Source | Sample rate | Within-source request share | Within-source duration share |
|---|---:|---:|---:|
| BCFY Feeds | 16 kHz | 100.000% | 100.000% |
| BCFY Calls | 8 kHz | 28.644% | 45.019% |
| BCFY Calls | 16 kHz | 61.751% | 49.154% |
| BCFY Calls | 22.05 kHz | 9.046% | 5.041% |
| BCFY Calls | 44.1 + 96 kHz | 0.559% | 0.786% |
| Echo | 8 kHz | 100.000% | 100.000% |
| Fire Notifications | 8 kHz | 82.281% | 78.012% |
| Fire Notifications | 16 kHz | 11.757% | 17.250% |
| Fire Notifications | 22.05 kHz | 5.962% | 4.738% |

#### Training comparison and sample-rate decision

The hypothesis that 8 kHz is underrepresented is rejected:

| Rate | Canonical-train row share | Canonical-train duration share | Production request share | Production duration share |
|---|---:|---:|---:|---:|
| 8 kHz | 64.642% | 60.310% | 12.157% | 21.273% |
| 16 kHz | 13.085% | 14.439% | 86.154% | 77.291% |
| 22.05 kHz | 20.876% | 23.867% | 1.604% | 1.292% |
| Other | 1.397% | 1.383% | 0.085% | 0.144% |

This global contrast must not become a global inverse-frequency weight because
the source mixtures differ substantially:

| Source | Canonical-train row share | Canonical-train duration share | Production request share | Production duration share |
|---|---:|---:|---:|---:|
| BCFY Calls | 3.490% | 7.029% | 15.167% | 18.312% |
| BCFY Feeds | 29.467% | 25.030% | 76.331% | 66.946% |
| Echo | 47.016% | 20.794% | 4.613% | 6.951% |
| Fire Notifications | 20.027% | 47.147% | 3.888% | 7.792% |

Training should not literally duplicate rows until it matches traffic shares:
that would collapse effective sample size while adding no acoustic diversity,
and every protected owner must remain present at least once. Instead, source
and rate are treated jointly.

Within BCFY Calls, the major-rate duration geometry is already close:

| Rate | Train row share | Train duration share | Production request share | Production duration share |
|---|---:|---:|---:|---:|
| 8 kHz | 40.543% | 42.359% | 28.644% | 45.019% |
| 16 kHz | 52.417% | 48.867% | 61.751% | 49.154% |
| 22.05 kHz | 2.799% | 4.364% | 9.046% | 5.041% |
| 44.1 kHz | 4.241% | 4.411% | 0.182% | 0.425% |
| 96 kHz | 0% | 0% | 0.377% | 0.361% |

The 12x/6x proposal would move BCFY Calls to 57.695% 8 kHz rows and
59.510% 8 kHz duration, the wrong direction. It would also make BCFY Calls
23.369% of all training rows and 39.239% of all training duration.

The selected default is therefore **uniform total multiplicity 3**: every
BCFY Calls row occurs three times in the one final SFT manifest, independent of
sample rate. Relative to the immutable 33,780-row canonical train, that
projection has:

- 36,138 total rows;
- 3,537 BCFY Calls rows, 9.79% of requests;
- 50,778 BCFY Calls target words, 15.08% of target words;
- 30,731.745 BCFY Calls audio seconds, 18.49% of training duration.

The duration share closely matches production's 18.312% without inheriting
the incident cohort's outcome-selection bias or overemphasizing 8 kHz. Copies
must use unique model-input object URIs, deterministic identities, and
deterministic interleaving so sibling copies are not adjacent. This is total
multiplicity 3, not three extra copies.

This is not a claim that sample rate is irrelevant. The historical
87.5%-versus-44.9% BCFY Calls result came from an outcome-selected
failed-plus-matched-success study and cannot estimate marginal production
failure risks. If 8 kHz is deliberately hard-weighted in a future experiment,
that is an explicit hard-case intervention requiring a fixed-total-exposure
ablation, not correction of an exposure deficit.

#### Production-aligned BCFY Feeds representation

The published reconstruction correctly preserves source-native PCM and remains
the immutable owner-complete reference. It also reveals a model-input mismatch:
canonical-train BCFY Feeds is only 35.986% 16 kHz by row and 37.130% by
duration, while production model-input BCFY Feeds is 100% 16 kHz. Canonical
Feeds is predominantly 22.05 kHz: 56.108% of rows and 55.058% of duration.

For the derived model-facing projection, every reconstructed BCFY Feeds request
is therefore serialized as 16 kHz mono using one frozen deterministic
conversion contract. Request boundaries, transcripts, owners, and membership
do not change. The same derived media must be used by primary eval and its
predicted-context view. BCFY Calls, Echo, and Fire Notifications remain at
their native production request rates. The existing masked eval remains
unchanged for historical comparability.

Oversampling only the existing 16 kHz Feeds rows is rejected: it would discard
effective diversity and leave most annotated Feeds material in a representation
that production never sends to the model.

#### Evaluation and operational reporting

Every checkpoint and all three evaluation views—reconstructed primary,
reconstructed with prediction-only prior context, and retained masked—must
slice results by both source and the actual model-input sample rate, not just a
global 8-kHz/16-kHz split. Required views are source, exact rate, 8 kHz versus
non-8 kHz, 8 kHz versus 16 kHz, and source x exact rate. At minimum, exact-rate
tables report 8, 16, 22.05, 44.1, and 96 kHz where present. Every cell includes
row count, audio duration, reference-word support, and both row-weighted and
reference-word-weighted metrics. Sparse cells remain visible rather than being
silently folded into `other`; global rate results are never interpreted
without their source-conditioned counterpart.

Production request geometry in the census is:

| Duration | Request share | Audio-duration share |
|---|---:|---:|
| <2.3 s | 47.829% | 16.343% |
| 2.3–<4.36 s | 24.844% | 18.626% |
| 4.36–<7.89 s | 14.283% | 19.469% |
| 7.89–<11.46 s | 5.755% | 12.745% |
| 11.46–<17.47 s | 4.189% | 13.876% |
| 17.47–30 s | 2.311% | 12.007% |
| >30 s | 0.788% | 6.934% |

These are reporting strata, not bucket targets for reconstruction or
weighting.

The settled application metric recorded 10,254 `unintelligible` terminal
statuses (1.833%), which count as effective-empty under this project's broader
definition; 10,175 invocations entered fallback (an overlapping auxiliary
status). Of 559,270 metric starts, 545,271 ended as success, 3,618 as transient
error, 35 as permanent error, 40 as policy blocked, 50 as partial, and two were
cutoff-in-flight; zero ended in a separate literal `empty` status.

These are production benchmarks, not label quotas. End-to-end status does not
reveal the full-window internal tuned-attempt `finish_reason=None` versus
blank-`STOP` mixture. The reconstructed eval should expose provider and
transport outcomes naturally under production-equivalent requests, not inject
synthetic failures or force their frequencies to equal the bounded window. It
must report both attempt-level causes and terminal outcomes.

The exact aggregate receipt SHA-256 is
`6b735cd7310556627742357f5c02bf07e4fa10ea5f03d7f79281a9e18bafc19c`;
the 555,645-row membership artifact SHA-256 is
`4c45046de1ae1205495634605eacdb1df98cb70c9f3d4cecc4a416c4742f9b72`.
The first 679 logged invocations used the prior tuned endpoint before the
deployment transition; 558,582 used the current endpoint. That 0.12% boundary
slice does not materially change the source/rate conclusions.

## Short valid FLAC rejection finding

The user supplied an eight-case audit where tuned and foundation endpoints both
rejected the same locally valid FLAC before generation with HTTP 400
`INVALID_ARGUMENT` / gRPC 3:

> Failed to decode audio or visual data. Please make sure the audio or visual
> data is valid.

Seven clips contained speech-level signal and were 0.86–2.30 seconds long; one
0.14-second clip was near-silent. Six BCFY-feed clips matched their raw-source
offsets exactly. This rules out treating these selected cases as
`finish_reason=None` or blank `STOP`, but it does not establish a provider
minimum duration or isolate the provider-side cause.

The incident makes no membership, boundary, padding, merge, or weighting
change. Short protected Speech remains in the datasets exactly as annotated.
Local decoding and PCM equality prove local losslessness, not Gemini
acceptance.

This build did not reread those eight incident files or make diagnostic provider
calls. Their byte-level results are retained as user-supplied evidence, not as a
measured rate from the reconstructed dataset.

Post-freeze eval records provider input-media rejection separately from
generation empties. A terminal rejection remains in operational WER as an empty
hypothesis, so it creates reference-word deletions instead of disappearing from
the denominator.

## Predicted-prior-context evaluation

Prior context never contains reference transcription. Each checkpoint has
independent state. Episodes may run in parallel, but rows within an episode are
strictly sequential:

- BCFY Feeds: one episode per continuous source;
- BCFY Calls and Fire Notifications: one episode per authoritative provider
  item;
- Echo: one episode per reconstructed request.

Only an accepted non-empty prediction from that same checkpoint may enter the
next request, capped at ten entries. Empty, whitespace-only, marker-only,
provider-rejected, and failed attempts never enter context. A non-empty
`MAX_TOKENS` partial is scored and may enter later context.

Every logical application attempt and every terminal request is persisted.
Restart replays completed terminal rows to rebuild context and does not recall
the provider for the completed prefix.

## Outcome accounting

The evaluator keeps mutually exclusive attempt and terminal-request categories,
including:

- accepted non-empty;
- exact blank `STOP`;
- whitespace/marker effective-empty;
- `finish_reason=None`;
- non-empty and empty `MAX_TOKENS`;
- provider input-decode rejection;
- HTTP 499, HTTP 504, throttling, and other transport/provider errors;
- no candidate, safety/policy, and response-parse failures.

Metrics use two explicit denominators: all logical application attempts and all
scheduled terminal requests. Conditional successful-response WER and
operational all-row WER are both reported. Operational WER assigns every
terminal no-transcription outcome an empty hypothesis and never drops the row.

## Frozen base materialization results

The published frozen build contains 43,988 reconstructed requests/audio files:

| Family | Train requests | Eval requests | Train owners | Eval owners |
|---|---:|---:|---:|---:|
| BCFY Calls | 1,179 | 84 | 2,914 | 204 |
| BCFY Feeds | 9,954 | 750 | 14,238 | 970 |
| Echo | 15,882 | 6,484 | 15,909 | 6,490 |
| Fire Notifications | 6,765 | 2,890 | 17,525 | 7,074 |
| **Total** | **33,780** | **10,208** | **50,586** | **14,738** |

The training requests contain 302,955 whitespace-delimited target words across
145,735.521 seconds. Primary eval contains 86,453 words across 42,490.657
seconds. All 43,988 targets are non-empty.

Thirty seconds remained a soft anchor rather than a hard filter. There are 144
training requests and 41 primary-eval requests above 30 seconds. The longest
training request is a 72.9-second authoritative Fire Notifications provider
item; the longest BCFY-feed request is 58.416 seconds.

The frozen base BCFY-feed request-duration geometry is:

| Split | Sources | Requests | Duration W1 vs production request geometry | KS | Max duration |
|---|---:|---:|---:|---:|---:|
| Train | 369 | 9,954 | 45.468 ms | 0.007961 | 58.416 s |
| Eval | 21 | 750 | 99.103 ms | 0.008748 | 24.211 s |

The low W1 and KS values validate request-duration geometry only. They do not
show production sample-rate parity or source-share parity.

The exact `[UNINTELLIGIBLE]` target rate is 5/33,780 (0.0148%) in training and
0/10,208 in primary eval. Thirteen training requests (0.0385%) contain the
marker anywhere. The retained masked eval has no blank or
`[UNINTELLIGIBLE]` targets. These are dataset-label rates, not production model
empty-output rates. In particular, production's 1.833% terminal
`unintelligible` rate is an end-to-end model/application outcome, not a target
label rate; the two are not directly comparable and do not justify synthetic
empty targets.

Masked eval retains 2,043 unchanged raw rows:

| Family | Retained |
|---|---:|
| BCFY Calls | 92 |
| BCFY Feeds | 559 |
| Echo | 746 |
| Fire Notifications | 646 |

The historical selection proof is a normalized lineage representation with
SHA-256
`2bb7e826124864e965530c23a14d799d1e4256f5bfd33917890b73c22b97c601`.
It differs from the raw payload only by top-level `dataset` and `source_audio`
lineage fields. Removing exactly those fields reproduces the emitted raw
payload byte-for-byte, whose SHA-256 is
`85166828ce2aefa58f3185d0db9959e4e2c26dbe0296039fda0ff4d1f624f955`.

Key frozen base manifest identities:

| Artifact | Rows | SHA-256 |
|---|---:|---|
| Canonical train | 33,780 | `9888cbf20f2db7a4ad1b0338a1b8751e58c225c7904ea7d337665830b7a264ba` |
| Canonical primary eval | 10,208 | `760ac91e08275d865c9d2ce68df2c1b31e2c0954106004d7add9ec2190f300b6` |
| Canonical validation subset | 5,000 | `2e1c50847ccc107901ea1f506e1ce5e945daa32a5fc94aa2f300769d80dd08c8` |
| Gemini train input | 33,780 | `0a7a3bd4b51bad3cb92953199ee51dcb7defc805e2dfaa441b2cd0727479f745` |
| Gemini validation input | 5,000 | `41b6a01c319f841d12650cd12bd90e7152bb111438e1b207ae7bb2e17745fb46` |
| Predicted-context schedule | 10,208 | `78b39e5ba0b1205779aef6a6e13f66aed7a371c8de81d2bfb09199f4d3d79e68` |

## Production-census limitations and evidence quality

- The half-open census covers 3 days, 5 hours, and 42 minutes. It is exact for
  that window, not proof of a seasonal or long-term mixture.
- The primary denominator is 555,645 unique exact audio URIs; the secondary
  invocation denominator is 559,261. Nine additional metric-only starts lack
  URI/rate evidence and are not imputed.
- Source lineage missing is zero and sample rate missing is zero. All 131,514
  non-Feeds unique requests had their generation-bearing 42-byte FLAC
  `STREAMINFO` read. BCFY Feeds rate is code-contracted and independently
  checked on a deterministic 10,000-header sample rather than by reading all
  424,131 headers.
- Two independent full log reads produced the same 559,261-row identity set and
  per-partition content hashes.
- The first 679 invocations (0.12%) used revision 45 and the prior tuned
  endpoint; the remaining 558,582 used revisions 46–48 and the current
  endpoint.
- The census establishes exposure, not sample-rate causality or a complete
  success/failure-by-rate denominator. It inspected no transcript, prompt,
  annotation, model response, or decoded audio samples and ran no inference or
  SFT.

## Verification

Verification is complete:

- the reconstruction test suite passes: 282 tests;
- the stage completion marker covers 38 artifacts and 43,988 audio files;
- the optimizer-independent verifier prints `COMPLETE`;
- it proves all 65,324 Speech owners are conserved exactly once, train/eval
  physical groups are disjoint, PII-only intervals are absent, every output is
  one continuous in-bounds source slice, and source/output decoded PCM hashes
  match;
- the actual Gemini SFT canonical loaders accept 33,780 train, 10,208 eval, and
  5,000 validation rows;
- local Gemini SFT preflight passes with zero failures or offending IDs;
- the staged prior-context loader binds all 10,208 eval requests to 10,208
  frozen media receipts across 8,582 episodes, with cap ten, equal
  source/output PCM hashes, and no reference-transcript access;
- focused fake-provider tests prove same-episode serialization, cross-episode
  and cross-checkpoint parallel isolation, prediction-only context, and
  crash/resume without recalling completed requests.

The local completion marker SHA-256 is
`0391d662b9742e328156ce2eeec1c53482d13c74b479f8a813bf52d2f52d5ee8`.

## Publication

The approved build was published create-only at:

`gs://wd-transcription-data/sft/dataset_versions/20260724-production-shaped-reconstruction/`

The remote prefix contains exactly 44,027 declared objects totaling
3,230,841,881 bytes: 43,988 audio files, 38 non-marker artifacts, and the
completion marker. The independently recomputed publication aggregate SHA-256
is
`a1e8b895aa7620b84488aa07e13e56322d4f4d3a2221d81b88a3efbd2c302f86`.
The create-only publication receipt SHA-256 is
`81c62620b8b4be70b94a2157578d4c56152911b8443ae35569dc96b526c11a90`.

`_SUCCESS.json` was published last at GCS generation
`1784935243692024`. Its remote bytes equal the local completion marker exactly,
with SHA-256
`0391d662b9742e328156ce2eeec1c53482d13c74b479f8a813bf52d2f52d5ee8`.
The final audit compared the complete remote name/size/generation set with all
44,027 receipt entries and found exact equality. No object was overwritten:
every write used generation-zero preconditions, and an existing object was
accepted only after a generation-pinned exact-byte comparison.

No provider inference, tuning submission, or SFT run was performed as part of
publication.
