# Phase 4: Strict Policy Table And Status Vocabulary - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-06-15
**Phase:** 4-Strict Policy Table And Status Vocabulary
**Areas discussed:** Compatibility boundary

---

## Compatibility Boundary

| Option | Description | Selected |
|--------|-------------|----------|
| Keep Phase 4 focused | Add backend enum/policy tests now; defer OpenAPI/frontend/generated metadata to follow-up compatibility work. | ✓ |
| Sync API enum early | Update backend enum and OpenAPI parity in Phase 4; leave generated frontend metadata and UI labels for later. | |
| Do all compatibility now | Move enum/API/UI compatibility into Phase 4 so the repo stays broadly green after the status enum change. | |

**User's choice:** Keep Phase 4 focused; frontend changes deferred as follow-up.

**Notes:** The phase context records that Phase 4 should not update
frontend/OpenAPI/generated/UI compatibility surfaces. Phase 4 tests should be
scoped around backend policy/status behavior; compatibility remains follow-up
work currently represented by Phase 6.

---

## the agent's Discretion

- Choose the exact policy-row data structure and test parametrization.
- Keep the implementation minimal and fail-closed.

## Deferred Ideas

- Frontend/OpenAPI/generated/UI compatibility changes are deferred from Phase 4.
