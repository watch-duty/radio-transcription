---
phase: 05-operator-docs
plan: "01"
subsystem: docs
tags: [gemini-sft, operator-docs, okf, runbook, hygiene]
requires:
  - phase: 04-04
    provides: one-target durable eval docs and stable summary artifact paths
provides:
  - OKF operator docs index
  - canonical Gemini SFT operator runbook
  - thin SFT README entrypoint
affects: [gemini-sft-docs, operator-onboarding, artifact-hygiene]
tech-stack:
  added: []
  patterns:
    - OKF-compatible Markdown frontmatter for SFT operator docs
    - README as thin entrypoint into canonical docs
    - runbook ends with staged generated-artifact hygiene check
key-files:
  created:
    - model/scripts/sft/docs/index.md
    - model/scripts/sft/docs/runbook.md
  modified:
    - model/scripts/sft/README.md
key-decisions:
  - "The canonical workflow lives in docs/runbook.md; README only links and summarizes."
  - "Packaged eval docs preserve the one [eval.model] per config/run contract."
  - "Generated local artifacts must be checked in the staged set before commit."
patterns-established:
  - "Operator docs use lightweight type/title/description/tags frontmatter."
  - "Paid Vertex commands list expected GCS prefixes before invocation."
requirements-completed: [DOC-01, DOC-04, DOC-05]
duration: 5 min
completed: 2026-06-29
---

# Phase 05 Plan 01: OKF Runbook And README Entrypoint Summary

**Canonical Gemini SFT runbook with paid Vertex warnings, durable GCS artifact
inspection, and README routing**

## Performance

- **Duration:** 5 min
- **Started:** 2026-06-29T02:37:50Z
- **Completed:** 2026-06-29T02:42:51Z
- **Tasks:** 2 completed
- **Files modified:** 3

## Accomplishments

- Created `model/scripts/sft/docs/index.md` as the OKF navigation entrypoint.
- Created `model/scripts/sft/docs/runbook.md` as the canonical prepare, tune,
  eval, report, checkpoint sweep, masked/unmasked, and hygiene workflow.
- Replaced `model/scripts/sft/README.md` with a 42-line entrypoint linking to
  the OKF docs and preserving the no-paid-Vertex unit test boundary.

## Task Commits

1. **Task 1: Create OKF index and canonical runbook** - `3ec62a5a` (docs)
2. **Task 2: Make README a thin entrypoint** - `85eb8ac4` (docs)

## Files Created/Modified

- `model/scripts/sft/docs/index.md` - OKF index linking to the runbook and
  companion docs.
- `model/scripts/sft/docs/runbook.md` - Canonical operator path with required
  command sequence, paid-operation warnings, GCS prefixes, one-target eval
  contract, durable/local artifact distinction, and final staged-file hygiene
  check.
- `model/scripts/sft/README.md` - Thin operator entrypoint with runtime,
  doc links, command summary, and verification boundary.

## Decisions Made

- Kept the README as navigation plus safety context only to avoid drift with the
  canonical runbook.
- Documented checkpoint scoring as an optional paid ranking path, not the main
  packaged eval path.
- Linked companion docs expected from Plan 05-02 without duplicating their
  future config, metric, artifact, or hygiene details in this plan.

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

- An initial patch landed in the coordination directory rather than the nested
  `radio-transcription` repo; it was corrected before verification and no
  misplaced files were committed.

## Known Stubs

None. The stub scan found only intentional "placeholder" wording for operator
config examples required by the plan.

## Verification

- `test -f model/scripts/sft/docs/index.md && test -f model/scripts/sft/docs/runbook.md`
- `rg -n "^type: runbook$|^title: Gemini SFT Operator Runbook$|^## 8\\. Artifact Hygiene Before Commit$|gemini-sft tune --config /path/to/run.toml --confirm|git diff --cached --name-only|evals/wer_summary\\.\\{json,md\\}|docs/hygiene\\.md" model/scripts/sft/docs/runbook.md`
- `rg -n "\\[runbook\\.md\\]\\(runbook\\.md\\)|\\[configs\\.md\\]\\(configs\\.md\\)|\\[metrics\\.md\\]\\(metrics\\.md\\)|\\[artifacts\\.md\\]\\(artifacts\\.md\\)|\\[hygiene\\.md\\]\\(hygiene\\.md\\)" model/scripts/sft/docs/index.md`
- `rg -n "\\[Operator runbook\\]\\(docs/runbook\\.md\\)|\\[Config examples\\]\\(docs/configs\\.md\\)|\\[Metric glossary\\]\\(docs/metrics\\.md\\)|\\[Artifact reference\\]\\(docs/artifacts\\.md\\)|\\[Artifact hygiene\\]\\(docs/hygiene\\.md\\)" model/scripts/sft/README.md`
- `test "$(wc -l model/scripts/sft/README.md | awk '{print $1}')" -le 120`
- `rg -n "gemini-sft prepare --config /path/to/run.toml|gemini-sft tune --config /path/to/run.toml --confirm|gemini-sft eval --config /path/to/run.toml|must not submit paid Vertex" model/scripts/sft/README.md`
- `git diff --check -- model/scripts/sft/README.md model/scripts/sft/docs/index.md model/scripts/sft/docs/runbook.md`
- `rg -n "\\[eval\\.model\\]|external wrapper|results/|git diff --cached --name-only|paid Vertex" model/scripts/sft/docs/runbook.md model/scripts/sft/README.md`

## State Tracking

Per parallel executor instructions, `.planning/STATE.md` and
`.planning/ROADMAP.md` were not modified. The orchestrator owns shared tracking
writes after the wave completes.

## Self-Check: PASSED

- Found `model/scripts/sft/docs/index.md`.
- Found `model/scripts/sft/docs/runbook.md`.
- Found `model/scripts/sft/README.md`.
- Found `.planning/phases/05-operator-docs/05-01-SUMMARY.md`.
- Found task commits `3ec62a5a` and `85eb8ac4`.
- Confirmed `.planning/STATE.md` and `.planning/ROADMAP.md` are not modified.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

Plan 05-02 can fill in `configs.md`, `metrics.md`, `artifacts.md`, and
`hygiene.md` using the links and canonical workflow established here.

---
*Phase: 05-operator-docs*
*Completed: 2026-06-29*
