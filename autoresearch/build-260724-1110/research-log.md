# Reconstruction build research log

## 2026-07-24 — H01 contract freeze

- Mode: orchestrator, build-feature archetype.
- Frozen the user-approved source, ownership, privacy-overlap, boundary, and
  evaluation-lane decisions before inspecting model outcomes.
- Atomic unmasked eval is intentionally absent. The retained lanes are the
  reconstructed primary eval/validation, its predicted-prior-context-10
  inference view, and leak-refiltered masked eval.
- Parallel audits are checking reusable code, source manifests, original-media
  availability, referenced evidence, and a deterministic construction solver.
- No SFT operation is authorized or implemented.

### H01 result

- Rejected the supplied causal claim that leading silence or noise is already
  proven to cause decoder collapse. The preserved evidence supports a duration
  association and outcome phenotypes, but not that waveform mechanism.
- Reconstructed an outcome-blind BCFY-feeds production target from all 94
  frozen telemetry shards. The exact target contains 485,142 logical requests,
  15,143 distinct millisecond durations, and 1,797,198.919 seconds total.
- This resolves the earlier 485,085 versus 485,142 discrepancy: 485,142 follows
  the frozen revision/trace/span logical-request definition and current source
  mapping used by this build.
- Froze the full histogram, rather than duration buckets or summary quantiles,
  for exact Wasserstein-1 evaluation.
- Began H02 with a generation-locked source inventory, then stopped the
  whole-source decode/hash pass when the user clarified that frozen source
  validity may be assumed and no concurrent writer exists. Source acquisition
  and header/bounds checks now occur lazily during required materialization.

## 2026-07-24 — H02 ownership and H03 source semantics

### H02 result

- Froze 65,324 protected non-empty Speech owners from the latest SFT source
  universe: 50,586 final training owners and 14,738 final eval owners.
- Applied training-wins ownership at the complete physical recording-group
  level. This moved 942 historical Fire Notifications eval owners into
  training and left zero final train/eval recording-group overlap.
- Refiltered the four supplied masked-v2 eval manifests against that final
  ownership. The retained immutable selection contains 2,043 raw rows; masked
  audio is not resegmented.
- Removed the redundant full-source decodability/checksum audit after the user
  confirmed frozen media validity and sole-writer execution. Required selected
  slices still fail immediately if they cannot be opened or are out of bounds.

### H03 result

- Rejected the initial assumption that every Echo `source_uri` identifies one
  provider item. All 509 frozen Echo sources are legacy hour archives, whereas
  production receives individual transmission objects.
- With no authoritative transmission sidecar and no manual reconciliation,
  froze the conservative maximum safe request: one connected component of
  strictly overlapping protected Speech intervals. Exactly touching clips and
  all positive targetless/unannotated gaps remain separate.
- This preserves 22,399 Echo owners exactly once in 22,366 requests, merges
  only 30 true overlap components, and limits the longest Echo request to
  27.32 seconds. The rejected source-wide rule produced artificial requests up
  to 600 seconds and bridged unknowable transmissions.
- Calls and Fire Notifications retain authoritative provider-item-minus-PII
  reconstruction. Across all presegmented families, 50,116 owners are conserved
  exactly once in 33,284 requests.
- Official Gemini 3.1 Flash-Lite tuning limits cap validation at 5,000 examples
  (and, above 1,000 examples, 30% of training size). The full reconstructed
  primary eval remains unchanged for scoring; canonical/provider validation is
  a deterministic subset of that same eval rather than a new split. The
  selector behavior-locks the latest SFT setup: proportional
  dataset-family × duration × word-count strata, integer largest-remainder
  quotas, and stable-hash source round-robin selection.
- Corrected the predicted-prior-context episode identity after the Echo
  discovery. BCFY feeds chain within a continuous source; Calls and Fire
  Notifications chain only within an authoritative provider item; each Echo
  strict-overlap request is its own episode. This prevents unrelated
  transmissions from one hour archive from sharing predicted context.

## 2026-07-24 — Short valid FLAC provider-rejection incident

- Recorded a user-supplied byte-level audit of eight production requests for
  which both tuned and foundation endpoints rejected the same audio before
  generation with `HTTP 400 INVALID_ARGUMENT` / gRPC 3 and the message
  `Failed to decode audio or visual data. Please make sure the audio or visual
  data is valid.` Seven were locally valid, speech-level clips of about
  0.86–2.30 seconds; one 0.14-second clip was near-silent. Six BCFY Feeds clips
  also matched their asserted source offsets exactly; the remaining two were
  authoritative, presegmented BCFY Calls items.
- Rejected a universal minimum-duration explanation. The frozen production
  target contains 46,876/485,142 requests below one second,
  211,884/485,142 below two seconds, and 247,622/485,142 at or below
  2.3 seconds. The selected eight cases have no successful-request denominator.
- Made no membership, boundary, padding, merging, or weighting change.
  Provider outcomes remain forbidden construction inputs, and the eight
  production clips are outside this build's frozen source universe.
- Classified this phenotype as a provider input-media rejection, not a model
  empty completion. Post-freeze reporting will separate it from blank `STOP`,
  `finish_reason=None`, marker/whitespace-only output, HTTP 499, and other
  transport/provider failures. Operational WER keeps terminal rejected rows as
  empty hypotheses rather than dropping them.
- Local PCM equality proves lossless materialization but not provider
  acceptance. Because every reconstructed request is newly encoded, the
  resulting eval can measure the decode-rejection rate under its own frozen
  serialization; it cannot claim in advance to reproduce the historical
  provider-rejection distribution exactly.
- Froze the serializer beneath its version strings so those future rates remain
  interpretable. Feed outputs bind to exact hashes for `soundfile.py`,
  `_soundfile.py`, and the actually loaded bundled `libsndfile_x86_64.so`
  containing libFLAC 1.4.3. Presegmented outputs bind to the exact deployed
  FFmpeg 6.1.1 binary and normalization image revision. This changes
  provenance, not request geometry or PCM.
- Current official Vertex AI reference/sample material retrieved through
  Context7 did not document a minimum audio duration for Gemini generation.

### User-supplied incident rows

| Segment prefix | Family | Duration | Peak / RMS dBFS | Local decode | Source-offset evidence |
|---|---|---:|---:|---|---|
| `0448abbf` | BCFY Feeds | 1.21 s | -0.93 / -25.4 | complete | exact |
| `11081b98` | BCFY Feeds | 1.94 s | -2.35 / -28.4 | complete | exact |
| `9b3dc8d4` | BCFY Feeds | 0.98 s | -8.86 / -30.7 | complete | exact |
| `aecaf646` | BCFY Feeds | 0.95 s | -6.39 / -29.3 | complete | exact |
| `ced89337` | BCFY Feeds | 0.86 s | -4.46 / -27.2 | complete | exact |
| `f1713075` | BCFY Feeds | 2.30 s | -8.37 / -30.2 | complete | exact |
| `719ac096` | BCFY Calls | 1.51 s actual; 1.00 s recorded | -4.26 / -27.3 | complete | authoritative item |
| `95a7f0b5` | BCFY Calls | 0.14 s | -63.7 / -82.0 | complete | authoritative item |

These are selected failures, not a rate denominator. The audit establishes
local container/PCM validity and, for the six feed rows, correct extraction
lineage. It does not identify the provider-side rejection stage or establish
that duration, silence, container bytes, MIME metadata, or another property is
causal.

## 2026-07-24 — H04 continuous reconstruction

- Completed exact split-wide BCFY-feed planning without model outcomes,
  transcript text, word counts, or duration buckets as construction inputs.
- Training uses 369 continuous sources, preserves 14,238 Speech owners in
  9,954 requests, and reaches 45.468 ms duration Wasserstein-1 and 0.007961 KS
  distance from the frozen production target.
- Eval uses 21 continuous sources, preserves 970 owners in 750 requests, and
  reaches 99.103 ms Wasserstein-1 and 0.008748 KS.
- Thirty seconds remained a soft anchor. Twelve feed training requests exceed
  it, with a maximum of 58.416 seconds; no feed eval request exceeds it.
- A receipt-reconciliation seam initially dropped correction/provenance
  metadata when reloading persisted continuous owners. Rebinding only the
  reconciliation receipts repaired the contract while preserving every
  request byte, request ID, boundary, owner list, and target text.

## 2026-07-24 — H05 full local materialization and verification

- Materialized 43,988 lossless FLAC requests: 33,780 train and 10,208 primary
  eval. Every selected output was decoded locally and matched its exact source
  slice PCM hash.
- Preserved all 65,324 protected Speech owners exactly once. Independent
  physical-group and transcript-provenance gates report zero train/eval
  leakage.
- Retained 2,043 masked rows unchanged. The initially failing verifier exposed
  a representation seam, not a selection difference: the historical proof
  added `dataset` and `source_audio`, whereas the required output is the raw
  row. All 2,043 identities, order, and common payload fields matched.
- Split the contract into the frozen normalized selection-proof hash
  `2bb7e826124864e965530c23a14d799d1e4256f5bfd33917890b73c22b97c601`
  and raw retained-payload hash
  `85166828ce2aefa58f3185d0db9959e4e2c26dbe0296039fda0ff4d1f624f955`.
  The verifier requires exact row equality after removing only those two
  lineage fields and separately recomputes the emitted raw payload.
- The complete reconstruction suite passes 282 tests. An independent targeted
  review found no High or Critical issue in the masked proof/payload repair.
- The stage completion marker covers 38 artifacts and all 43,988 audio files;
  a separate optimizer-independent invocation prints `COMPLETE`.
- The actual Gemini SFT loaders accept 33,780 train, 10,208 eval, and 5,000
  validation rows. Local Gemini SFT preflight reports zero failures and zero
  offending IDs.
- The predicted-context loader binds 10,208 scheduled requests to 10,208
  frozen media receipts across 8,582 episodes, verifies equal source/output PCM
  hashes, caps context at ten, and forbids reference-transcript access.
- At this local-build checkpoint, no provider inference, upload, publication,
  tuning submission, or SFT run had occurred.

## 2026-07-24 — H06 create-only publication

- After explicit approval, published the completed build create-only at
  `gs://wd-transcription-data/sft/dataset_versions/20260724-production-shaped-reconstruction/`.
- The first attempt stopped safely before `_SUCCESS.json` when concurrent
  transfer of the largest non-audio artifacts hit the provider request
  timeout. The partial prefix contained no extras or conflicts. A serial
  reproduction of the reported 58,351,225-byte manifest succeeded with the
  exact expected SHA-256, isolating transfer saturation rather than data drift.
- Hardened recovery without changing dataset bytes: audio remains concurrent,
  manifests and receipts are serialized, and create-only upload, metadata
  reload, and generation-pinned download use an explicit long timeout. The
  focused recovery tests, all 282 reconstruction tests, and an independent
  High/Critical review passed.
- Resumed idempotently. Every existing object was accepted only after exact
  byte and content-type verification, all missing artifacts were created, and
  `_SUCCESS.json` was published last.
- The final prefix contains exactly 44,027 declared objects totaling
  3,230,841,881 bytes. The independently recomputed aggregate SHA-256 is
  `a1e8b895aa7620b84488aa07e13e56322d4f4d3a2221d81b88a3efbd2c302f86`;
  the publication receipt SHA-256 is
  `81c62620b8b4be70b94a2157578d4c56152911b8443ae35569dc96b526c11a90`.
- The remote `_SUCCESS.json` is generation `1784935243692024`, exactly equals
  the local marker, and has SHA-256
  `0391d662b9742e328156ce2eeec1c53482d13c74b479f8a813bf52d2f52d5ee8`.
- No provider inference, tuning submission, or SFT run occurred.

## 2026-07-24 — H07 post-publication incident evidence

- Recorded the user-supplied full-cohort prompt rerun: 1,642/1,934 selected
  tuned-empty/foundation-nonempty segments produced a non-empty tuned response
  under the training-aligned prompt; 292 remained empty. This establishes a
  high-impact prompt-contract problem for that selected cohort, but does not
  establish transcript correctness.
- Verified that the current build independently loads the backend production
  prompt and model/SFT prompt, fails on any string difference, and writes the
  identical prompt into training and validation requests. The frozen prompt
  SHA-256 is
  `c806d02e134d47aa6c90284ed2544507ab0895c804c7cac004510d08c748cc17`;
  the audio-only request-schema SHA-256 is
  `df148bc8c710b2c5ea56e3093410f6b746980e937875377610a640623be9e856`.
- The exact prompt bytes/digest used in the 84.9% rerun were not included in
  the supplied summary. The latest completed SFT freezes a different prompt,
  SHA-256
  `3fa0b4d3cab803e715abbc0e6ff310e776a317d807fc1df871a77771ffdfb23a`.
  Kept the empirical result separate from both repository-verified prompt
  identities and from deployment state.
- Recorded the user-supplied residual characterization: blind listening
  contradicted the proposed buzzy/vocoded/muffled causal signature; residual
  failures were associated with shorter duration, lower level, and lower
  internal SNR, with only modest individual discrimination and no established
  causal intervention. Made no geometry, membership, or waveform-treatment
  change.
- Recomputed the final unique training mix from the emitted canonical
  manifest. BCFY Calls contributes 1,179/33,780 requests (3.490%),
  2,914/50,586 protected Speech owners (5.760%), 16,926/302,955 words
  (5.587%), and 10,243.915/145,735.521 seconds (7.029%).
- Recomputed the completed SFT's final canonical manifest rather than treating
  the 830 original `calls_train` inputs as its final family count. Assembly
  contains 839/49,632 BCFY Calls-family rows (1.690%), 4,519/296,039 words
  (1.527%), and 1,790.761/106,014.672 seconds (1.689%).
- Distinguished the reported 20.7-fold old-training-row/recovered-cohort
  composition gap from exposure-adjusted enrichment. The frozen
  current-metadata sensitivity mapping assigns 90,117/629,374 production
  candidates (14.319%) to BCFY Calls, while the recovered cohort contains
  669/1,934 (34.592%), a 2.42-fold composition enrichment. Neither ratio is an
  automatic SFT sampling multiplier.
- Kept the create-only GCS dataset immutable. A future repeated/weighted SFT
  rendering, if selected, must be a separate hash-bound manifest over the same
  unique training census. The family distribution of the 292 post-prompt
  residuals is still required before treating the residual as a weighting
  target.
