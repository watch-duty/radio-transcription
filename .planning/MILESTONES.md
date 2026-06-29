# Milestones: Gemini SFT Workflow Onboarding

## v1.0 Gemini SFT Workflow Onboarding (Shipped: 2026-06-29)

**Delivered:** A config-driven Gemini SFT/eval operator workflow with shared
reports, single-target eval config, durable GCS artifacts, checkpoint scoring
parity, and OKF documentation.

**Phases completed:** 1-5, 17 plans total, 49 tasks.

**Key accomplishments:**

- Shared SFT eval reporting contract across console, JSON, Markdown, batch
  eval, and checkpoint scoring, including exact empty response metrics, raw
  edit counts, total reference words, missing predictions, and artifact URIs.
- Packaged eval target execution with shared prompt/request/prior-context
  helpers, deterministic batch/online routing, resumable online predictions,
  and request-identity metadata for reuse.
- Durable single-target eval contract using `[eval.model]` / `eval_model`,
  with legacy/plural config shapes rejected before paid Vertex work.
- Durable summary uploads and normalized inference manifests under GCS run
  prefixes, while local `results/` remains cache-only.
- OKF operator runbook, placeholder configs, metric glossary, artifact
  reference, hygiene checklist, and drift guards for report/docs alignment.

**Stats:**

- 5 phases, 17 plans, 49 tasks.
- 28/28 v1 requirements complete.
- Archive files: `.planning/milestones/v1.0-ROADMAP.md`,
  `.planning/milestones/v1.0-REQUIREMENTS.md`, and
  `.planning/milestones/v1.0-MILESTONE-AUDIT.md`.

**Known follow-ups:**

- Dataset breakdown reports.
- Promotion thresholds and pass/fail verdicts.
- Optional duration, prior-context-depth, and prompt/context report slices.
- Optional Linear, PR comment, or release-note automation.

---
