# Phase 2: Cloud Logging and Pub/Sub Routing - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md - this log preserves the alternatives considered.

**Date:** 2026-06-26
**Phase:** 2-Cloud Logging and Pub/Sub Routing
**Areas discussed:** Terraform ownership boundary, Log sink filter strictness, Pub/Sub delivery posture, IAM identity shape, Relay dependency shape

---

## Terraform Ownership Boundary

| Option | Description | Selected |
|--------|-------------|----------|
| Public reusable module + deployment instantiation | Put reusable modules/contracts in public repo; concrete env instantiation in deployment repo. | ✓ |
| Deployment repo only | Keep all Terraform in deployment repo even if reusable. | |
| Public contract only | Public repo documents the contract; deployment repo owns all resources. | |

**User's choice:** Anything reusable should be placed in the public repo. Otherwise, place in the deployment repo.
**Notes:** This decision leaves room for planner/researcher to decide whether the Phase 2 route is truly reusable enough for a public module or is better as deployment-owned infrastructure with a public contract.

---

## Log Sink Filter Strictness

| Option | Description | Selected |
|--------|-------------|----------|
| Strict event + resource types | Filter by event contract and expected Cloud resource types. | |
| Event contract only | Filter by `jsonPayload.event_type` and `jsonPayload.schema_version` only. | ✓ |
| Event + service names | Filter by event contract plus known emitter service names. | |

**User's choice:** Event contract only.
**Notes:** The event contract is the routing boundary. Extra resource/service filters increase maintenance cost and can miss future valid emitters.

---

## Pub/Sub Delivery Posture

| Option | Description | Selected |
|--------|-------------|----------|
| Full route with relay inputs | Create topic, DLQ, sink, IAM, push subscription, OIDC, retry, and dead-letter policy with relay endpoint as input/contract. | ✓ |
| Sink/topic/DLQ only | Stop before creating the push subscription until relay exists. | |
| Temporary pull or placeholder | Create a temporary subscription or placeholder target for early testing. | |

**User's choice:** Full route with relay inputs.
**Notes:** Phase 2 should create concrete route infrastructure without implementing or faking the Phase 3 relay.

---

## IAM Identity Shape

| Option | Description | Selected |
|--------|-------------|----------|
| Dedicated route identities | Use a dedicated Pub/Sub push invoker service account and least-privilege IAM grants. | ✓ |
| Reuse existing runtime identities | Reuse a nearby app service account for push authentication. | |
| Let deployment decide | Leave service account selection entirely to env-specific configuration. | |

**User's choice:** Dedicated route identities.
**Notes:** The latest deployment repo already uses dedicated Pub/Sub push invoker identities in several service modules. Matching that pattern keeps caller identity narrow.

---

## Relay Dependency Shape

| Option | Description | Selected |
|--------|-------------|----------|
| Route module with relay inputs | Phase 2 defines route resources and accepts relay URL/service/name/identity inputs supplied by Phase 3. | ✓ |
| Move route creation into Phase 3 | Phase 2 documents the contract only; Phase 3 creates route and relay together. | |
| Placeholder relay target | Phase 2 creates a temporary Cloud Run target so the route can be fully applied before relay code exists. | |

**User's choice:** Route module with relay inputs.
**Notes:** This keeps Phase 2 concrete while avoiding throwaway placeholder services.

## the agent's Discretion

- Exact Terraform module name and variable/output names.
- Whether the route is best expressed as a small public reusable module, a deployment repo module, or a deployment app submodule, as long as the public/deployment ownership decision is respected.

## Deferred Ideas

- Relay implementation and WD webhook call behavior belong to Phase 3.
- Operational dashboards, DLQ runbooks, and end-to-end staging proof belong to Phase 4.
