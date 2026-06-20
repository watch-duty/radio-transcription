---
phase: 05
slug: 05-retention-and-verification-hardening
status: verified
threats_open: 0
asvs_level: 1
created: 2026-06-20
---

# Phase 05 - Security

Per-phase security contract for retention and verification hardening. This
audit verifies only the threats declared in the Phase 05 plan threat models and
the threat flags reported by the Phase 05 summaries.

---

## Trust Boundaries

| Boundary | Description | Data Crossing |
|----------|-------------|---------------|
| AlloyDB scheduler -> audit tables | DB-owned pg_cron SQL deletes expired audit rows and sequence bookkeeping. | `feed_audit_events` rows and `feed_audit_event_sequences` rows. |
| Migration files -> local/CI schema helpers | `*pg_cron*` migrations are skipped outside AlloyDB, so executable retention SQL must remain extension-free. | SQL procedure and scheduler migrations. |
| Retention SQL -> audit evidence | Retention may remove historical evidence only under the approved 18-month policy. | Historical audit event payloads and sequence labels. |
| Documentation/tests -> future implementers | Contract text and static tests guard retention invariants against future drift. | Retention policy strings, forbidden tokens, expected test names. |
| Test gate -> behavioral test files | The v1 gate reads existing tests and fails if required coverage disappears. | Test names and audit action/status-detail tokens. |
| Backend service/BFF -> public feed consumers | Public compatibility depends on canonical `status_reason_detail` and no public `quarantine_reason` alias. | Public feed response fields. |
| Runtime/Echo -> storage audit insertion | Runtime and Echo paths must keep audit row construction inside storage boundaries. | Runtime actor values, state transitions, audit rows. |
| Local executor -> host resources | Verification must stay in low-resource local lanes by default. | Python/Vitest processes, Docker/Testcontainers only by checkpoint. |
| Testcontainers database -> retention SQL | Real DB execution validates retention procedure semantics in a prepared lane. | Seeded audit events and sequence rows. |
| Retention procedure -> sequence table | Cleanup may remove ordering metadata only after current feed and retained events are gone. | `feed_audit_event_sequences.next_sequence`. |
| CI/prepared-machine verification -> release confidence | Resource-heavy verification may happen outside the local execution turn. | Prepared-machine pytest and AlloyDB proof records. |
| Prepared AlloyDB/CI -> pg_cron scheduler metadata | Production-only scheduler migration must prove the expected pg_cron job. | `pg_extension`, `cron.job`, installed procedure body. |

---

## Threat Register

| Threat ID | Category | Component | Disposition | Evidence | Status |
|-----------|----------|-----------|-------------|----------|--------|
| T-05-01-01 | Tampering | `031_feed_audit_event_retention.sql` | mitigate | Procedure selects expired rows with `occurred_at < NOW() - INTERVAL '18 months'`, `LIMIT 10000`, and `FOR UPDATE SKIP LOCKED`, then deletes by selected ID only: `terraform/modules/alloydb/sql/ingestion/031_feed_audit_event_retention.sql:5-15`. Static tests require those tokens and reject synthetic inserts, `created_at <`, archive/tombstone/baseline, and sequence renumbering tokens: `backend/pipeline/storage/tests/test_feed_audit_contract.py:283-314`. | closed |
| T-05-01-02 | Repudiation | `feed_audit_events` retention | mitigate | Documentation states retained timelines start at the oldest non-expired event and `feed_sequence` gaps are expected/immutable: `documentation/feed-audit-events.md:197-199`. DB-backed test preserves retained sequence `[2]` and next sequence after expiry: `integration_tests/storage/test_feed_store_integration.py:1902-1953`. | closed |
| T-05-01-03 | Denial of Service | Daily retention cleanup | mitigate | Procedure uses bounded CTEs with `LIMIT 10000` and `FOR UPDATE SKIP LOCKED`: `031_feed_audit_event_retention.sql:5-15`, `031_feed_audit_event_retention.sql:17-36`. Scheduler runs one daily job at `15 3 * * *`: `032_feed_audit_events_pg_cron_retention.sql:7-10`. Static test requires `LIMIT 10000`: `test_feed_audit_contract.py:283-304`. | closed |
| T-05-01-04 | Information Disclosure | Diagnostic/audit payload retention | mitigate | Retention SQL contains only selected deletes and no payload rewrite/redaction path; targeted grep for `UPDATE`, `INSERT`, `archive`, `tombstone`, `baseline`, `redact`, `rewrite`, and `created_at <` in the two new SQL files found only the required `FOR UPDATE SKIP LOCKED` lock clauses. Documentation states retention does not archive, redact, rewrite, create tombstone/baseline events, or renumber retained events: `documentation/feed-audit-events.md:191-195`. Diagnostic sanitizer tests are gated by exact name: `backend/pipeline/storage/tests/test_feed_audit_v1_verification_gate.py:77-99`; actual sanitizer tests exist at `backend/pipeline/storage/tests/test_feed_lifecycle.py:61` and `backend/pipeline/storage/tests/test_feed_lifecycle.py:75`. | closed |
| T-05-01-05 | Elevation of Privilege | Public/service retention control | mitigate | Retention scheduling remains in AlloyDB pg_cron: `032_feed_audit_events_pg_cron_retention.sql:5-10`. Targeted grep for `prune_feed_audit_events_retention` and `feed-audit-events-retention` across non-test `backend` and `frontend` paths returned no matches, confirming no public API, service, runtime, or frontend retention entry point. | closed |
| T-05-01-06 | Tampering | `feed_audit_event_sequences` cleanup | mitigate | Sequence pruning is guarded by `NOT EXISTS` against both `public.feeds` and retained `public.feed_audit_events`: `031_feed_audit_event_retention.sql:17-36`. Static test requires both guards: `test_feed_audit_contract.py:283-295`. Integration tests cover live-feed preservation, deleted-feed-with-retained-history preservation, and orphan-after-expiry pruning: `integration_tests/storage/test_feed_store_integration.py:1956-2066`. | closed |
| T-05-02-01 | Repudiation | Audit event coverage | mitigate | V1 gate reads `test_feed_store.py` and requires exact tests/action tokens for create, update, deactivate, reset, delete, failure, quarantine, and recovery: `backend/pipeline/storage/tests/test_feed_audit_v1_verification_gate.py:17-55`. Targeted grep confirmed those behavior tests exist in `backend/pipeline/storage/tests/test_feed_store.py:1466`, `1540`, `1581`, `1615`, `1725`, `2606`, `2674`, `3035`, `3122`, `3155`, and `3233`. | closed |
| T-05-02-02 | Information Disclosure | Diagnostic detail tests | mitigate | Gate requires secret redaction and length-cap tests for `status_reason_detail`: `test_feed_audit_v1_verification_gate.py:77-99`. Targeted grep confirmed the tests exist at `backend/pipeline/storage/tests/test_feed_lifecycle.py:61` and `backend/pipeline/storage/tests/test_feed_lifecycle.py:75`. | closed |
| T-05-02-03 | Tampering | Public API compatibility | mitigate | Gate requires backend/BFF tests exposing `status_reason_detail`, `statusReasonDetail`, and rejecting public `quarantine_reason`: `test_feed_audit_v1_verification_gate.py:77-99`. Targeted grep confirmed the backend and BFF tests/tokens at `backend/services/feeds/tests/test_api.py:81`, `backend/services/feeds/tests/test_api.py:428`, `backend/services/feeds/tests/test_api.py:448`, and `frontend/api/src/feeds/feedsController.test.ts:260`, `273`. | closed |
| T-05-02-04 | Denial of Service | Test execution | mitigate | Gate is a pure file-read pytest and adds no Docker/Testcontainers execution: `test_feed_audit_v1_verification_gate.py:1-123`. Summary records targeted Python and targeted Vitest commands passed, and no broad E2E/API/component/Docker lanes were added: `.planning/phases/05-retention-and-verification-hardening/05-02-SUMMARY.md:67-78`. Repo guard requires targeted low-resource checks and explicit approval for broad local resource-heavy lanes: `AGENTS.md:6-16`. | closed |
| T-05-02-05 | Elevation of Privilege | Actor propagation tests | mitigate | Gate requires `test_admin_mutation_methods_require_keyword_only_actor_id`: `test_feed_audit_v1_verification_gate.py:77-99`. Targeted grep confirmed the service test exists at `backend/services/feeds/tests/test_service.py:37`. Actor contract states admin actors derive from trusted auth context, not request body: `documentation/feed-audit-events.md:100-103`. | closed |
| T-05-02-06 | Tampering | Runtime/Echo audit ownership | mitigate | Gate requires no-noise tests and `test_runtime_and_echo_sources_do_not_reference_audit_table`: `test_feed_audit_v1_verification_gate.py:102-123`. Targeted grep confirmed no-noise and runtime/Echo ownership tests at `backend/pipeline/storage/tests/test_sync_feed_store.py:591`, `607`, `641`, `backend/pipeline/storage/tests/test_feed_store.py:1762`, `1785`, `1820`, `1849`, and `backend/pipeline/storage/tests/test_feed_query_contracts.py:601`. | closed |
| T-05-03-01 | Tampering | Retention procedure behavior | mitigate | DB-backed retention test seeds 19-month expired and 17-month retained rows, calls `CALL public.prune_feed_audit_events_retention()`, and asserts only the retained row remains: `integration_tests/storage/test_feed_store_integration.py:1902-1953`. | closed |
| T-05-03-02 | Repudiation | Retained `feed_sequence` ordering | mitigate | Same DB-backed test asserts retained sequence `[2]` after expiry and preserves `next_sequence`: `integration_tests/storage/test_feed_store_integration.py:1945-1953`. Documentation says gaps are expected and labels are immutable: `documentation/feed-audit-events.md:197-199`. | closed |
| T-05-03-03 | Tampering | `feed_audit_event_sequences` pruning | mitigate | DB-backed tests cover live-feed sequence preservation, deleted-feed-with-retained-history preservation, and orphan sequence pruning after last audit expiry: `integration_tests/storage/test_feed_store_integration.py:1956-2066`. | closed |
| T-05-03-04 | Denial of Service | Local Testcontainers lane | mitigate | Local verification was kept to compile/static checks: `.planning/phases/05-retention-and-verification-hardening/05-03-SUMMARY.md:76-83`. Testcontainers lane is recorded as pending CI/prepared-machine UAT with exact command and "run only on an approved prepared machine or in CI": `05-03-SUMMARY.md:85-97`, `.planning/phases/05-retention-and-verification-hardening/05-HUMAN-UAT.md:15-24`. Verification notes explain why local execution is deferred: `05-VERIFICATION.md:7-13`. | closed |
| T-05-03-05 | Repudiation | Rollback/concurrency evidence | mitigate | Existing rollback and concurrent ordering tests are present: `integration_tests/storage/test_feed_store_integration.py:1796`, `1835`, `2072`; delete-survival test is present at `integration_tests/storage/test_feed_store_integration.py:2426`. Prepared-machine command includes those exact tests: `05-03-SUMMARY.md:91-97`, `05-HUMAN-UAT.md:20-24`. Deferred execution is tracked UAT, not an implementation security gap. | closed |
| T-05-03-06 | Tampering | AlloyDB pg_cron scheduler | mitigate | Scheduler migration installs pg_cron and registers `feed-audit-events-retention` with schedule `15 3 * * *` and command `CALL public.prune_feed_audit_events_retention()`: `032_feed_audit_events_pg_cron_retention.sql:5-10`. Prepared AlloyDB/CI checkpoint is documented with exact `pg_extension`, `cron.job`, and installed-procedure-body checks: `05-03-SUMMARY.md:99-109`, `05-HUMAN-UAT.md:26-35`. Deferred execution is tracked UAT, not an implementation security gap. | closed |

*Status: open/closed.*
*Disposition: mitigate (implementation required), accept (documented risk), transfer (third-party).*

---

## Accepted Risks Log

No accepted risks.

Deferred Testcontainers and prepared AlloyDB lanes are not accepted risks for
this security audit. They are tracked pending UAT because the implemented
mitigation is the explicit checkpoint plus UAT record and local execution guard.

---

## Unregistered Threat Flags

None. `05-03-SUMMARY.md` reports no new endpoint, auth path, file access path,
schema change, or trust-boundary production surface: `.planning/phases/05-retention-and-verification-hardening/05-03-SUMMARY.md:134-136`.
No `## Threat Flags` section is present in `05-01-SUMMARY.md` or
`05-02-SUMMARY.md`.

---

## Security Audit Trail

| Audit Date | Threats Total | Closed | Open | Run By |
|------------|---------------|--------|------|--------|
| 2026-06-20 | 18 | 18 | 0 | Codex security audit |

---

## Sign-Off

- [x] All threats have a disposition (mitigate / accept / transfer)
- [x] Accepted risks documented in Accepted Risks Log
- [x] `threats_open: 0` confirmed
- [x] `status: verified` set in frontmatter

**Approval:** verified 2026-06-20
