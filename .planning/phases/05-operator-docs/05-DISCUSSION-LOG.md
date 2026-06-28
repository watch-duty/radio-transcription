# Phase 5: Operator Docs - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-06-28T22:30:00Z
**Phase:** 5-Operator Docs
**Areas discussed:** Operator Journey, Example Config Set, Metric Glossary, Artifact Hygiene Check

---

## Operator Journey

| Option | Description | Selected |
|--------|-------------|----------|
| README-only runbook | Put the whole operator workflow directly in `model/scripts/sft/README.md`. | |
| README entrypoint plus OKF runbook bundle | Keep README short and make the OKF runbook the canonical human-facing workflow. | ✓ |
| Split unrelated docs without a canonical runbook | Create separate docs but no single primary path. | |

**User's choice:** Use Open Knowledge Format for human-facing docs, with the
runbook itself as the OKF document.
**Notes:** README should be a thin entrypoint and should not duplicate the
runbook. The OKF runbook is canonical.

---

## Example Config Set

| Option | Description | Selected |
|--------|-------------|----------|
| Small example set | One normal full placeholder config plus one or two variants/snippets. | ✓ |
| One file per combination | Separate committed configs for base, tuned, checkpoint, masked, unmasked, batch, and online. | |
| Reference-only examples | No extra concrete examples beyond prose. | |

**User's choice:** One or two variants are more than enough.
**Notes:** Use the full existing placeholder config as the standard example.
Show endpoint/checkpoint and masked-eval differences as snippets or at most one
extra placeholder config.

---

## Metric Glossary

| Option | Description | Selected |
|--------|-------------|----------|
| Canonical report names only | Document the current shared report columns and explain legacy names only if needed. | ✓ |
| All historical names | Document old and new names equally. | |
| Minimal metric list | Only define WER/CER and leave the rest implicit. | |

**User's choice:** Proceed with the current canonical report names.
**Notes:** The docs must clearly distinguish exact empty model output from the
historical empty-or-unintelligible metric.

---

## Artifact Hygiene Check

| Option | Description | Selected |
|--------|-------------|----------|
| Runbook check plus lightweight guard | Include an explicit final check and add a small script/test only if it fits existing patterns. | ✓ |
| Documentation only | Warn operators but do not provide a concrete check. | |
| New heavy CI policy | Add a broad new repository-level enforcement system. | |

**User's choice:** Proceed with a practical hygiene check.
**Notes:** The check should catch accidental commits of `.local.toml`, local
`results/`, generated inference manifests, raw prediction JSONL, and similar
experiment artifacts.

---

## the agent's Discretion

- Exact OKF frontmatter fields and file names may be chosen during planning.
- Planning may choose whether masked eval is a second TOML file or a concise
  snippet in the config reference.
- Planning may choose whether artifact hygiene is docs-only plus manual command
  or backed by a small existing-style test/script.

## Deferred Ideas

- Large config gallery for every model/backend/eval variant.
- Dataset-breakdown docs before dataset-breakdown support exists.
- Internal multi-model orchestration.
- Linear comment automation, release notes, and promotion gates.
