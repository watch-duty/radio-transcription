---
type: index
title: Gemini SFT Operator Documentation
description: Entry point for maintained Gemini SFT and evaluation guidance.
tags: [gemini-sft, operator-docs]
okf_version: "0.2"
---

# Gemini SFT Operator Documentation

The implementation modules linked from the
[SFT README](../README.md#code-ownership) are authoritative. These documents
retain only workflow, policy, and interpretation that helps an operator use the
code safely.

## Workflow

- [Operator runbook](runbook.md) — prepare, tune, evaluate, recover, and review.
- [Configuration guidance](configs.md) — lifecycle and non-obvious choices.
- [Evaluation methodology](evaluation-methodology.md) — comparison policy and
  reference-isolation requirements.

## Reference

- [Metric interpretation](metrics.md) — how to interpret execution and quality
  signals.
- [Artifact ownership](artifacts.md) — durable state versus local cache.
- [Artifact hygiene](hygiene.md) — files that should not be committed.

Historical runs, checkpoint inventories, and dataset investigations belong in
their immutable provider artifacts or Git history, not in maintained operator
documentation.
