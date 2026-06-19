---
phase: 01-contract-and-schema-foundation
reviewed: 2026-06-19T05:26:39Z
depth: standard
files_reviewed: 5
files_reviewed_list:
  - documentation/feed-audit-events.md
  - CONTEXT.md
  - terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql
  - terraform/modules/alloydb/sql/ci/hot_protection_check.sql
  - backend/pipeline/storage/tests/test_feed_audit_contract.py
findings:
  critical: 2
  warning: 3
  info: 0
  total: 5
status: issues_found
---

# Phase 1: Code Review Report

**Reviewed:** 2026-06-19T05:26:39Z
**Depth:** standard
**Files Reviewed:** 5
**Status:** issues_found

## Summary

Reviewed the Feed Audit Events Phase 1 contract, glossary additions, schema
migration, HOT guard, and text-level contract tests. The submitted schema mostly
matches the intended table shape and delete-survival direction, but it leaves
important parts of the contract unenforced and documents a diagnostic-detail
security posture that conflicts with the repository's stated security
requirements.

## Critical Issues

### CR-01: BLOCKER - Raw Diagnostic Detail Contract Permits Secret Persistence

**File:** `documentation/feed-audit-events.md:132`
**Classification:** BLOCKER
**Issue:** Lines 132-139 define `status_reason_detail` as raw diagnostic text
with "no redaction or transformation beyond the 2048-character cap" and
explicitly accept persistence of sensitive upstream failure strings. The project
security requirement says persisted reason text must not contain secrets,
tokens, raw credential-bearing exception strings, or unbounded provider
responses. The migration only adds length checks on `feeds.status_reason_detail`
and `feed_audit_events.status_reason_detail`, so future writers following this
contract can durably store secret-bearing diagnostics in current state and audit
history for the 18-month retention window.
**Fix:**
```markdown
`status_reason_detail` is bounded diagnostic text after secret scrubbing. Writers
MUST redact credentials, tokens, signed URLs, service-account material, and raw
credential-bearing provider responses before persistence. The 2048-character cap
is a size bound, not the security boundary.
```
Add contract tests that assert the documentation requires scrubbing, and make
the future storage writer use one redaction helper before writing both
`feeds.status_reason_detail` and `feed_audit_events.status_reason_detail`.

### CR-02: BLOCKER - Audit JSON Columns Do Not Enforce Object Shape

**File:** `terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql:44`
**Classification:** BLOCKER
**Issue:** `before_values`, `after_values`, and `metadata` are `JSONB NOT NULL`
with object defaults, but PostgreSQL will still accept JSON arrays, strings,
numbers, booleans, and JSON `null`. The domain contract says before/after values
are allowlisted snapshots of meaningful domain values, which implies JSON
objects. Invalid JSON shapes can be inserted successfully and later break admin
timeline or delivery consumers that expect object snapshots. Existing repo SQL
already uses `jsonb_typeof(...)` checks for structured JSON in
`021_feed_properties_tags.sql`, so this migration is under-enforcing its own
contract.
**Fix:**
```sql
ALTER TABLE feed_audit_events
    ADD CONSTRAINT feed_audit_events_json_object_shape
    CHECK (
        jsonb_typeof(before_values) = 'object'
        AND jsonb_typeof(after_values) = 'object'
        AND jsonb_typeof(metadata) = 'object'
    );
```
Add a test that rejects non-object values for all three columns.

## Warnings

### WR-01: WARNING - Actor ID Constraint Allows Unbounded And Blank Suffixes

**File:** `terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql:83`
**Classification:** WARNING
**Issue:** The actor check only verifies a recognized prefix plus
`char_length(actor_id) > char_length(prefix)`. That accepts whitespace-only
suffixes such as `service:   ` and arbitrarily long actor IDs. Because
`actor_id` is also indexed at lines 164-165, a malformed or unexpectedly long
actor ID can bloat the audit index or fail inserts with PostgreSQL btree row-size
errors. It also does not enforce the "normalized_email" expectation for
`user-email:` actors.
**Fix:** Bound and validate the suffix, for example:
```sql
CHECK (
    char_length(actor_id) <= 512
    AND (
        actor_id = 'unknown:unknown'
        OR (actor_id LIKE 'service:%'
            AND btrim(substring(actor_id FROM char_length('service:') + 1)) <> '')
        -- Repeat for each namespace, with an email-shaped check for user-email.
    )
)
```
Keep the exact allowed character set aligned with the documented actor
vocabulary before runtime writers depend on it.

### WR-02: WARNING - Constraint Idempotency Checks Are Not Schema-Qualified

**File:** `terraform/modules/alloydb/sql/ingestion/029_feed_audit_events.sql:10`
**Classification:** WARNING
**Issue:** Every `information_schema.table_constraints` existence check in this
migration filters only by `table_name` and `constraint_name`. In a database with
another visible schema containing a same-named table and constraint, the
migration can incorrectly skip adding the constraint to the intended table. That
would silently leave production without the length, action, actor, sequence, or
uniqueness guardrails.
**Fix:**
```sql
SELECT 1
  FROM information_schema.table_constraints
 WHERE table_schema = current_schema()
   AND table_name = 'feed_audit_events'
   AND constraint_name = 'feed_audit_events_actor_id_check'
```
Apply the same schema qualification to each constraint guard, or query
`pg_constraint` through the target table's `regclass` OID.

### WR-03: WARNING - Contract Tests Overfit Text Instead Of Proving SQL Semantics

**File:** `backend/pipeline/storage/tests/test_feed_audit_contract.py:106`
**Classification:** WARNING
**Issue:** The tests mainly assert raw token presence. For example,
`test_migration_defines_actor_and_action_constraints` checks strings in the raw
file, so comments could satisfy the test, and line 166 only checks that
`'status_reason_detail'` appears anywhere in the HOT guard file, not that it is
inside the guarded `a.attname IN (...)` list. These tests can pass while the
actual CHECK constraints or HOT guard behavior regress.
**Fix:** Either add a targeted Postgres migration smoke test that inserts valid
and invalid rows, or make the text tests operate on comment-free SQL and assert
the relevant clauses directly. At minimum:
```python
guard_sql = _sql_without_comments(hot_guard)
assert re.search(
    r"a\.attname\s+IN\s*\([^)]*'status_reason_detail'",
    guard_sql,
    flags=re.IGNORECASE | re.DOTALL,
)
```
Add negative cases for invalid actions, empty/blank actor suffixes, and
non-object audit JSON values.

---

_Reviewed: 2026-06-19T05:26:39Z_
_Reviewer: the agent (gsd-code-reviewer)_
_Depth: standard_
