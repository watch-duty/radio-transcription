# Phase 3: Verification And Compatibility - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-06-15
**Phase:** 03-verification-and-compatibility
**Areas discussed:** Compatibility surfaces, Evidence organization

---

## Compatibility Surfaces

| Option | Description | Selected |
|--------|-------------|----------|
| Minimal tolerance | OpenAPI enum, shared TS type, status conversion allowlist, and existing UI tooltip label only. | |
| Operator polish | Also add explicit UI test coverage for the new label and wording. | |
| Backend-only contract | OpenAPI parity test is enough; skip frontend test additions unless TypeScript breaks. | ✓ |

**User's choice:** Backend-only contract.
**Notes:** Phase 3 should update frontend/API/shared status surfaces only as
needed for parity and compile/type compatibility. Do not add frontend tests
solely for `pipeline_publish_after_bookmark_failed`.

---

## Evidence Organization

| Option | Description | Selected |
|--------|-------------|----------|
| Requirement-indexed matrix | Map each pending `TEST-*` / `STAT-02` requirement to the exact test or compatibility file. | ✓ |
| Scenario-indexed matrix | Organize by behavior category like non-budgeted release, publish gap, feed quarantine, unknown telemetry gap. | ✓ |
| Test-file checklist | List test files and what each one covers. | |

**User's choice:** Both requirement-indexed and scenario-indexed evidence.
**Notes:** The authoritative checklist should be requirement-indexed, with
scenario grouping added for reviewer readability.

---

## Incident Taxonomy Traceability

| Option | Description | Selected |
|--------|-------------|----------|
| Pending requirements only | Include only scenarios needed for `STAT-02` and `TEST-01` through `TEST-08`. | |
| Include nearby regressions | Include already-touched cases like source-offline/auth/rate-limit/pipeline publish if they map cleanly to existing tests. | |
| Full incident taxonomy | Try to represent every original quarantine category in the Phase 3 evidence matrix. | ✓ |

**User's choice:** Full incident taxonomy.
**Notes:** The follow-up decision narrowed this: full taxonomy should be
traceability, not one dedicated test per historic incident label.

---

## Taxonomy Test Granularity

| Option | Description | Selected |
|--------|-------------|----------|
| Map each incident category to a covered policy scenario | Every incident class has an explainable v1 lane, but does not require a unique test. | ✓ |
| Add a dedicated test for each incident category | Bespoke test coverage for every original incident label. | |
| Document uncovered categories as future gaps | Treat categories as future gaps even when the v1 lane is covered. | |

**User's choice:** Map each incident category to a covered policy scenario.
**Notes:** Avoid duplicate test maintenance when several incident categories
collapse to the same policy lane.

---

## Taxonomy Location

| Option | Description | Selected |
|--------|-------------|----------|
| Phase 3 implementation summary only | Tests stay behavior-focused; summary proves incident-to-scenario traceability. | ✓ |
| Test comments/docstrings | Keeps traceability near code but can make tests noisy. | |
| Dedicated project doc | More durable, but likely too heavy for v1 compatibility/verification. | |

**User's choice:** Phase 3 implementation summary only.
**Notes:** Do not create a new durable project document for taxonomy mapping in
v1.

---

## the agent's Discretion

- Exact test names, assertion placement, and test-file grouping.
- Whether a requirement is proven by an existing test or a new test.
- Exact wording of implementation-summary evidence tables.

## Deferred Ideas

- Dedicated long-lived incident taxonomy document after v1 if needed.
- Rich operator UX for suppressed retry states.
