# Acceptance contract

The build is complete only when one deterministic command materializes the
datasets and a separate verifier prints `COMPLETE`.

The verifier must establish:

1. Frozen source media is accepted as valid without a separate whole-source
   decode/hash audit. Materialization must still fail immediately if a needed
   source cannot be opened or a requested exact-frame slice is out of bounds.
2. Every eligible non-empty annotated clip is owned by exactly one split and
   represented exactly once. Corrected recording groups and every component
   touching prior training remain training-owned.
3. Training and all evaluation lanes are disjoint by physical source interval
   and normalized transcript provenance.
4. Speech wins a Speech–PII overlap; all remaining PII-only audio is absent.
   Blank and unannotated spans may supply context but never standalone targets.
5. Continuous BCFY recordings may be reconstructed across adjacent annotations.
   BCFY Calls and Fire Notifications never merge across authoritative provider
   items. Historical Echo URIs are hour archives rather than provider-item
   identities, so Echo requests are limited to strictly-overlapping protected
   Speech components; touching clips and every positive gap remain separate.
6. Construction is independent of transcript text, word count, WER, model
   output, finish reason, HTTP status, or fallback behavior.
7. The only eval lanes are the full reconstructed primary eval,
   predicted-prior-context-10 derived from preceding predictions, and unchanged
   masked-v2 rows after final ownership filtering. Tuning validation is a
   deterministic provider-size-compliant subset of reconstructed primary eval,
   not a new ownership split or independent dataset.
8. Counts, duration geometry, source composition, topology, empty-marker rate,
   and post-freeze inference outcome distributions are reported without changing
   the frozen construction. Provider input-media rejection
   (`HTTP 400`/gRPC 3 decode error) is reported separately from model-generation
   empties, transport failures, and semantic empty/marker outputs. Both
   attempt-level and terminal-request rates retain every row in their
   denominators.
9. Every materialized row binds to a family-specific frozen encoder identity:
   the exact loaded SoundFile/CFFI/bundled-libsndfile artifacts for BCFY Feeds,
   or the exact deployed FFmpeg binary/image revision for presegmented
   families. The independent verifier rejects profile or artifact drift without
   rereading whole source files.

No SFT submission is part of this build.
