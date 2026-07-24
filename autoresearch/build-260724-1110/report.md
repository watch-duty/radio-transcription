# Next-round SFT dataset reconstruction

Status: **construction and approved publication complete**. The full
request/audio universe is materialized, the independent verifier reports
`COMPLETE`, the actual Gemini SFT loaders and local preflight pass, and the
create-only GCS publication has been independently verified. No SFT job or
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
anchor; matching production request geometry has higher priority.

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

## Audio serialization

Every output is a single continuous source-frame slice. There is no synthetic
silence, concatenation of disjoint intervals, resampling, downmixing,
normalization, padding, trimming, filtering, crossfade, or time stretching.
Source sample rate, channel count, and PCM subtype are preserved, and decoded
output PCM must exactly equal the selected source PCM.

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

## Post-publication incident findings

The detailed evidence and decision record is
[`additional-findings-20260724.md`](additional-findings-20260724.md).

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

The most complete frozen production sensitivity mapping instead assigns
90,117/629,374 candidate segments (14.319%) to BCFY Calls. Against that
exposure, the recovered cohort is enriched by about 2.42-fold, not 20.7-fold.
That mapping uses current metadata for 74,342 candidates whose event-time
source was unresolved, so it is sensitivity evidence rather than an
authoritative event-time mixture.

The evidence supports treating BCFY Calls exposure as a high-priority SFT
sampling decision, but it does not support making 34.592% the training target.
The source breakdown of the 292 post-prompt residuals is needed before using
that residual as a weighting target. The published unique owner census remains
unchanged. If repeated examples are used for the one actual SFT input, that
effective manifest must be an explicit, separately hash-bound rendering of
this same training dataset; it must not silently alter the immutable published
prefix or create a second semantic dataset.

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

## Final materialization results

The finished build contains 43,988 reconstructed requests/audio files:

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

The final BCFY-feed geometry is:

| Split | Sources | Requests | Duration W1 vs production | KS | Max duration |
|---|---:|---:|---:|---:|---:|
| Train | 369 | 9,954 | 45.468 ms | 0.007961 | 58.416 s |
| Eval | 21 | 750 | 99.103 ms | 0.008748 | 24.211 s |

The exact `[UNINTELLIGIBLE]` target rate is 5/33,780 (0.0148%) in training and
0/10,208 in primary eval. Thirteen training requests (0.0385%) contain the
marker anywhere. The retained masked eval has no blank or
`[UNINTELLIGIBLE]` targets. These are dataset-label rates, not production model
empty-output rates.

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

Key final manifest identities:

| Artifact | Rows | SHA-256 |
|---|---:|---|
| Canonical train | 33,780 | `9888cbf20f2db7a4ad1b0338a1b8751e58c225c7904ea7d337665830b7a264ba` |
| Canonical primary eval | 10,208 | `760ac91e08275d865c9d2ce68df2c1b31e2c0954106004d7add9ec2190f300b6` |
| Canonical validation subset | 5,000 | `2e1c50847ccc107901ea1f506e1ce5e945daa32a5fc94aa2f300769d80dd08c8` |
| Gemini train input | 33,780 | `0a7a3bd4b51bad3cb92953199ee51dcb7defc805e2dfaa441b2cd0727479f745` |
| Gemini validation input | 5,000 | `41b6a01c319f841d12650cd12bd90e7152bb111438e1b207ae7bb2e17745fb46` |
| Predicted-context schedule | 10,208 | `78b39e5ba0b1205779aef6a6e13f66aed7a371c8de81d2bfb09199f4d3d79e68` |

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
