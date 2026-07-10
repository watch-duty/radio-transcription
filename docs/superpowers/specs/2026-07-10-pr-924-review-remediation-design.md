# PR 924 Review Remediation Design

## Goal

Make PR #924 safe to resend by fixing the confirmed reusable-output identity
bug, fail-loud canonical-manifest boundaries, late audio-URI normalization
failure, the active formatter failure, and documented hard Python standards
violations. Optional polling/logger scope changes and low-priority refactors
remain out of scope.

## Request identity

Reusable inference metadata will identify the actual request for every audio
row, not merely the audio URI and configuration fields. The shared request
identity builder will accept an ordered sequence of deterministic request
digests. Each digest will be computed from the same canonical request payload
used for inference, serialized with stable JSON ordering.

Batch reuse will continue to require exact identity equality. Online resume
will accept an existing identity only when both its audio URI sequence and its
request-digest sequence are prefixes of the new identity. The metadata schema
version will increase so old sidecars fail closed instead of being interpreted
as the stronger identity format.

Both batch and online execution will build request payloads through the shared
Gemini request builder and derive identity from those payloads. Operational
settings such as concurrency and retry limits remain excluded because they do
not change model output semantics.

## Canonical manifest boundaries

The existing lenient parser remains available for exploratory callers. New
strict local and GCS parsing entry points will reject malformed JSON, non-object
rows, and partially corrupt JSONL with source and line-number context. Packaged
Gemini SFT prepare and eval flows will exclusively use these strict entry
points before semantic canonical validation.

Strict canonical validation will reject `audio_filepath` values with leading
or trailing whitespace because the documented contract requires stripped
model-ready URIs. This makes invalid input fail before tuning or paid inference
and keeps preserved source rows aligned with normalized prediction keys.

## Standards and formatting

Added Python files and changed imports in the PR will follow the repository's
modules-only import convention. Public record classes will document attributes,
non-trivial public APIs will include the applicable `Args:`, `Returns:`, and
`Raises:` sections, and new test/helper modules will have module docstrings.
The root Ruff formatter will be applied to the failing test file and any files
changed by this remediation.

Judgment-call refactors such as introducing a broad request-spec object or
deduplicating every repeated test call remain out of scope unless required by
the modules-only import conversion.

## Error handling and compatibility

Existing sidecars without ordered request digests will be rejected as
non-reusable. This preserves the current fail-closed contract for unverified
artifacts. The inference result format and GCS artifact locations do not change.
Strict parsing changes only packaged workflow boundaries; existing documented
lenient parsing APIs retain their behavior.

## Testing

Development follows red-green-refactor cycles:

1. Prove online prefix reuse rejects changed histories while accepting a true
   request prefix.
2. Prove batch identity changes when request history changes.
3. Prove packaged local and GCS loaders reject a mixed valid/malformed JSONL
   manifest while the exploratory parser remains lenient.
4. Prove strict canonical validation rejects an unstripped audio URI before
   inference-manifest construction.
5. Run the focused Gemini SFT/common test suite, root Ruff check and format
   check, type checking, dead-code checking, and `git diff --check`.

