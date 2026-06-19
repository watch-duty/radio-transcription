# Phase 3: Service and Compatibility Surface - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-06-19
**Phase:** 3-Service and Compatibility Surface
**Areas discussed:** Trusted Admin Actor Handoff, Missing Identity Policy, Diagnostic Detail API Shape, Frontend Compatibility Behavior

---

## Trusted Admin Actor Handoff

| Option | Description | Selected |
|--------|-------------|----------|
| BFF trusted header | BFF derives `actor_id` from authenticated `request.user` and forwards it to feeds-service through a trusted internal path. | ✓ |
| Feeds service only | Feeds-service keeps `service:feeds-service`, which is simple but loses human admin attribution. | |
| Request body | Client sends `actor_id` in JSON, which is easy to wire but spoofable. | |

**User's choice:** Use Option 1, but frame it as trusted identity propagation rather than a loose header.
**Notes:** We compared the proposal against GCP service-to-service auth, API Gateway/Endpoints userinfo headers, IAP signed headers, and OAuth token-exchange delegation. Final decision: BFF authenticates and authorizes the human admin, calls feeds-service as the BFF service account, and forwards the causal human actor only over the trusted service boundary. Persist one `actor_id`; do not add canonical separate actor columns in v1.

---

## Missing Identity Policy

| Option | Description | Selected |
|--------|-------------|----------|
| Use `user-email:<normalized_email>` | Preserve attribution when email exists but Google `sub` is missing. | |
| Reject the mutation | Fail closed when a Google admin identity lacks a valid `sub`. | ✓ |
| Use `unknown:unknown` | Keep the mutation flowing but weaken admin attribution. | |

**User's choice:** Reject missing or invalid Google `sub`.
**Notes:** The user clarified that all admins should have valid Google `sub`; missing or invalid `sub` should be treated as a security risk. `user-email` remains a future namespace, not the Phase 3 Google-admin fallback. Do not emit `user:null`, empty suffixes, or `unknown:unknown` for admin actions.

---

## Diagnostic Detail API Shape

| Option | Description | Selected |
|--------|-------------|----------|
| Return both fields | Return `status_reason_detail` and keep `quarantine_reason` as an equivalent legacy alias. | |
| Return only `status_reason_detail` | Public contract moves to the canonical field. | ✓ |
| Return only `quarantine_reason` | Most compatible with old callers but does not expose the canonical field. | |

**User's choice:** Use `status_reason_detail`; remove `quarantine_reason` from the public contract if flow is not broken.
**Notes:** Code search showed `quarantineReason` is optional display text only, not control flow. The user chose to stop treating `quarantine_reason` as a compatibility alias and asked to update docs/requirements. Phase 3 should migrate service/BFF/frontend contracts; runtime-owned write cleanup is Phase 4 unless the phase is explicitly expanded.

---

## Frontend Compatibility Behavior

| Option | Description | Selected |
|--------|-------------|----------|
| Same places as old `quarantineReason` | Existing status tooltip/search/table/header flows show `statusReasonDetail`. | ✓ |
| Types/API only, no display | Minimal UI change but loses the diagnostic detail text operators currently see. | |
| New visible UI sections | More discoverable but becomes a UI redesign outside Phase 3. | |

**User's choice:** Display `statusReasonDetail` in the same places as old `quarantineReason`.
**Notes:** The TypeScript/frontend name is `statusReasonDetail`, matching backend `status_reason_detail` and pairing with `statusReason`. No new UI section or redesign belongs in Phase 3.

---

## the agent's Discretion

- Exact internal actor-context header name and helper structure.
- Whether caller-service provenance is logged or added to audit metadata.
- Smallest frontend migration shape that preserves current status display.

## Deferred Ideas

- Signed actor-context JWT or HMAC if multiple trusted callers are introduced.
- Full OAuth token exchange / on-behalf-of flow if future architecture needs it.
- Runtime-owned `quarantine_reason` write cleanup in Phase 4.
- Separate actor columns remain out of scope for v1 core audit persistence.
