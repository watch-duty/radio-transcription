# Project Retrospective

*A living document updated after each milestone. Lessons feed forward into future planning.*

## Milestone: v1.0 - Feed Audit Events V1

**Shipped:** 2026-06-20
**Phases:** 5 | **Plans:** 17 | **Sessions:** multiple

### What Was Built

- Durable Feed Audit Event contract, glossary, and deletion-safe AlloyDB schema.
- Storage-owned transactional audit writes for feed create, update, deactivate, reset, and delete.
- Trusted admin actor propagation plus service/BFF/frontend diagnostic-detail compatibility.
- Runtime and Echo failure, quarantine, and recovery audit events with no default lease or heartbeat noise.
- 18-month retention procedure, AlloyDB pg_cron scheduler migration, and low-resource verification gates.

### What Worked

- Phased contract-first planning kept schema, storage, runtime, and UI compatibility decisions aligned.
- Static contract and low-resource gates caught drift without requiring Docker or external services on every run.
- Security and milestone audits exposed remaining UAT debt without blocking source-level closeout.

### What Was Inefficient

- Some verification reports remained `human_needed` after evidence had moved elsewhere, which required manual reconciliation.
- `.planning` being ignored meant archive files needed explicit force-add handling during closeout.
- The generated roadmap archive kept a stale Phase 5 unchecked item even though the progress table was complete.
- Resource-heavy Docker/Testcontainers and AlloyDB pg_cron lanes were deferred instead of promoted to a repeatable CI lane during the milestone.
- Stale integration test call signatures were caught late by milestone audit rather than during the phase that introduced the signature change.

### Patterns Established

- Storage boundaries own audit row creation; service, runtime, and Echo callers pass actor and prior-state context only.
- Runtime audit events require a persisted abnormal-state transition or recovery from abnormal state, not clean progress.
- Prepared-machine UAT debt is tracked explicitly in requirements, STATE.md, and milestone audit artifacts.
- Low-resource static verification can guard broad behavioral contracts while preserving heavier DB tests for prepared runners.

### Key Lessons

1. Prepared-machine lanes should become named CI jobs or explicit UAT artifacts before the milestone starts closing.
2. Integration tests need a fast signature-drift check whenever storage method contracts change.
3. Milestone archives should be reviewed before deleting root requirements because generated status can overstate pending UAT.

### Cost Observations

- Model mix: not measured.
- Sessions: multiple.
- Notable: quality mode produced strong design convergence, but milestone closeout still needed manual audit reconciliation.

---

## Cross-Milestone Trends

### Process Evolution

| Milestone | Sessions | Phases | Key Change |
|-----------|----------|--------|------------|
| v1.0 | multiple | 5 | Contract-first GSD phases with tracked UAT debt at milestone close. |

### Cumulative Quality

| Milestone | Tests | Coverage | Zero-Dep Additions |
|-----------|-------|----------|-------------------|
| v1.0 | Static, unit, service, BFF, runtime, Echo, and prepared DB lanes | Source complete with 3 pending UAT requirements | Contract and static verification gates |

### Top Lessons (Verified Across Milestones)

1. Pending resource-heavy verification needs first-class CI/UAT ownership.
2. Auditability work benefits from source-owned domain events rather than caller-constructed history.
