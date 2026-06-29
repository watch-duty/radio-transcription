# Retrospective

## Milestone: v1.0 - Gemini SFT Workflow Onboarding

**Shipped:** 2026-06-29
**Phases:** 5
**Plans:** 17
**Tasks:** 49

### What Was Built

- Shared SFT eval reporting across console, JSON, Markdown, batch eval, and
  checkpoint scoring.
- Explicit single-target eval config and durable `eval_model` state.
- Target execution through batch or online backends with shared Gemini request
  and prior-context helpers.
- Durable GCS summary artifacts, normalized inference manifests, and
  fail-closed prediction reuse metadata.
- OKF operator docs, placeholder configs, metric glossary, artifact reference,
  hygiene checklist, and drift guards.

### What Worked

- Keeping `REPORT_COLUMNS` canonical made docs, console output, and JSON output
  easier to verify.
- Moving prompt, request, safety, and context behavior into shared helpers
  reduced drift across notebooks, SFT generation, batch eval, and checkpoint
  scoring.
- The one-model `[eval.model]` decision simplified durable eval and made the
  operator workflow easier to explain.
- Drift guards were useful for tying docs and artifact hygiene back to code.

### What Was Inefficient

- The milestone originally carried older multi-target and dataset-breakdown
  requirements after the user narrowed scope, which required audit
  reconciliation at close.
- Phase 1 and Phase 3 lacked verification reports until milestone close,
  despite their tests and summaries being present.
- Some generated milestone summary text needed manual cleanup for readability.

### Patterns Established

- GCS `config.json` is authoritative for eval state.
- Local `results/` is a cache/mirror, not source of truth.
- Legacy or plural target config should fail loudly before paid work starts.
- Dataset breakdowns and promotion gates should be scoped as separate
  follow-up phases.

### Key Lessons

- Update requirements and roadmap wording immediately after user scope changes.
- Add phase verification reports before moving to the next phase.
- Keep paid Vertex work behind explicit commands and resumable durable state.
- Keep operator docs small at the entrypoint and put full runbooks in the docs
  bundle.

## Cross-Milestone Trends

| Trend | Observation |
|---|---|
| Scope control | Smaller single-model eval semantics were easier to verify than internal multi-target orchestration. |
| Documentation | OKF-style docs plus drift guards kept operator-facing docs aligned with code. |
| Verification | Missing verification files are easy to overlook if only plan summaries are checked. |
