# Phase 6: Compatibility And Verification - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-06-15
**Phase:** 6-compatibility-and-verification
**Areas discussed:** Generated API sync, Type/UI test shape, Docs scope

---

## Generated API Sync

| Option | Description | Selected |
|--------|-------------|----------|
| Regenerate spec and routes | Run repo scripts for `generate-spec` and `generate-routes`, committing generated diffs only if they change. | |
| Spec only | Update/regenerate `openapi.yaml`, skip generated routes unless something breaks. | |
| Manual only | Edit compatibility files directly, no generation. | |
| Defer | Keep milestone backend-only and move API/generated compatibility to a follow-up. | ✓ |

**User's choice:** Defer.
**Notes:** User clarified the milestone only needs backend changes. Therefore OpenAPI and TSOA generated metadata should not be part of this milestone.

---

## Type/UI Test Shape

| Option | Description | Selected |
|--------|-------------|----------|
| Centralized TS reason list | Derive shared type and allowlist from one `as const` list, add focused tests for the new reasons. | |
| Current pattern plus tests | Keep the duplicated union/Set pattern and add focused tests for the new reasons. | |
| Minimal typecheck | Update values and rely mostly on typecheck. | |
| Defer | Keep milestone backend-only and move TS/UI compatibility to a follow-up. | ✓ |

**User's choice:** Defer.
**Notes:** Shared frontend types, UI labels, frontend tests, and status allowlists are explicitly deferred.

---

## Docs Scope

| Option | Description | Selected |
|--------|-------------|----------|
| Stale collector guide plus summaries | Update only the stale collector guide line/table plus Phase 6 summary/verification docs. | ✓ |
| Planning docs only | Leave repo docs for later and only update GSD artifacts. | |
| Broader doc sweep | Search and update wider repo documentation. | |

**User's choice:** Stale collector guide plus summaries.
**Notes:** The stale collector guide currently says `pipeline_publish_after_bookmark_failed` must not quarantine the feed. That conflicts with v1.1 and should be corrected.

---

## the agent's Discretion

- Choose exact collector guide wording.
- Choose exact focused backend verification commands.
- Decide whether Phase 6 planning should update roadmap/requirements to record the compatibility deferral.

## Deferred Ideas

- OpenAPI enum synchronization.
- TSOA generated route metadata synchronization.
- Shared frontend status reason type and allowlist synchronization.
- UI labels and frontend tests for the new split status reasons.
