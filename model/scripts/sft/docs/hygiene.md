---
type: reference
title: Gemini SFT Artifact Hygiene
description: Pre-commit checks for local Gemini SFT experiment artifacts.
tags: [gemini-sft, git-hygiene]
---

# Gemini SFT Artifact Hygiene

## Never Commit By Default

Do not commit local experiment artifacts unless a maintainer explicitly asks
for a specific file. This includes:

- `.local.toml` run configs.
- Root `results/`.
- `model/scripts/sft/results/**/*.jsonl`.
- `model/scripts/sft/results/**/*.jsonl.gz`.
- `model/data/inference_manifests/*.jsonl`.
- `online_predictions.jsonl`.
- batch prediction JSONL.
- generated eval outputs.
- local research experiment outputs.

These files can contain source data, provider outputs, local-only state,
credentials, or misleading cache state.

## Pre-Commit Check

Start with the full status view, including ignored files:

```bash
git status --short --ignored
```

Then scan staged files for artifact classes that should not be committed by
default:

```bash
git diff --cached --name-only | rg '(^results/|^model/data/inference_manifests/|\.local\.toml$|^model/scripts/sft/results/.*\.jsonl(\.gz)?$|online_predictions\.jsonl$|batch_predictions.*\.jsonl$)'
```

This docs-level check is intentionally lightweight. `.gitignore` should cover
common local artifacts, but the staged-file check is the final source of truth
before committing.

## If A Check Matches

If the staged-file check prints a path, stop and inspect it before committing.
For normal local output, unstage it and keep it out of source control. For a
rare artifact that should be committed, document why it is intentional and make
sure it contains no credentials, private source data, generated provider output,
or local-only run state.

Do not treat a clean local `results/` mirror as proof that a run can be reused.
Durable reuse depends on GCS state and matching request-identity metadata.
