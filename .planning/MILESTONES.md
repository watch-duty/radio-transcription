# Milestones

## v1.1 Policy Merge (Shipped: 2026-06-15)

**Phases completed:** 3 phases, 7 plans, 19 tasks

**Key accomplishments:**

- Pure failure policy classification now uses explicit status/evidence rows with telemetry-gap fallback.
- Backend status reason splits now feed owner-scope mapping and explicit policy routes.
- Collector producers now emit precise runtime-config, credential-access, and source-payload status reasons for current root causes.
- Runtime pipeline failures now execute the same policy branch as collector failures, with Pub/Sub post-bookmark publish failures consuming thresholded feed budget.
- Non-budgeted runtime and storage paths now have focused guard tests, and the final Phase 5 backend verification slice passes.
- Collector authoring guide now describes final v1.1 quarantine routing for post-bookmark Pub/Sub publish gaps and split system status reasons
- Backend-only closeout evidence records passing focused verification and explicitly defers API/UI/generated compatibility

---
