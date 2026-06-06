# ADR 0001: Model Package Boundaries

## Status

Accepted.

## Context

The model subtree originally grew from notebooks. Shared helpers lived under
`model/colabs/common`, while Gemini SFT orchestration lived under script files
in `model/scripts/sft`. That made notebook imports work from one working
directory, but it blurred ownership: cross-model helpers, Gemini-specific
helpers, and SFT workflow code all shared script/notebook conventions.

The next fine-tuned Gemini model needs to be reproducible from a normal package
entrypoint, and the maintained Gemini eval notebook must use the same prompt and
request helpers as SFT evaluation.

## Decision

The installable distribution is `radio-transcription-model`, rooted at
`radio-transcription/model/`.

Python source uses a normal `model/src/` layout:

- `common` remains the import package for cross-model ASR/model helpers.
- `common.gemini` owns Gemini-specific shared primitives.
- `gemini_sft` owns the packaged Gemini SFT workflow and exposes the
  `gemini-sft` console command.

The old script-as-entrypoint contract is removed from maintained docs and
tests. Compatibility wrappers are not required.

## Why Keep `common` as the Import Package?

Existing notebooks and tests already import shared helpers from `common`.
Keeping that import package avoids churn in cross-model notebooks while still
moving the source of truth out of `model/colabs/common`.

The distribution name changes to `radio-transcription-model` because package
installation should describe the model subtree as a whole, not just the shared
helper package.

## Alternatives Considered

1. Keep `model/colabs/common` as the package source.

   Rejected because notebooks would continue to own reusable library code and
   script workflows would keep depending on working-directory behavior.

2. Rename the import package from `common` to `radio_transcription_model`.

   Rejected for this milestone because it would add broad notebook churn without
   improving the SFT boundary.

3. Split `common.gemini.vertex` into many submodules immediately.

   Deferred. Moving the Gemini provider boundary is enough for this milestone;
   further splitting should wait for concrete complexity pressure.

4. Add a `whisper_sft` package at the same time.

   Deferred. Whisper SFT remains notebook-first until it becomes a repeatable
   workflow.

## Consequences

- Editable install from `model/` exposes `common`, `common.gemini`, and
  `gemini_sft`.
- Docker notebook startup installs the mounted model package in editable mode.
- Maintained Gemini notebook imports must use `common.gemini.prompts` and
  `common.gemini.vertex`.
- Gemini SFT operators use `gemini-sft prepare`, `gemini-sft tune`, and
  `gemini-sft eval`.
- Tests should target package imports and mock cloud boundaries; they should not
  depend on script paths or submit paid Vertex jobs.
