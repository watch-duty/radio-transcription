-- fillfactor=70: 30% slack on every page so HOT updates can rewrite in place.
-- autovacuum tuning: belt-and-suspenders, driven by scaling-plan §6. These
-- values apply per-table and override instance defaults.
-- Idempotent: ALTER TABLE ... SET is always safe to re-run with same values.
-- NOTE: VACUUM FULL feeds is intentionally not run here. Terraform's
-- schema_sql_hash trigger (main.tf:270-285) re-applies every migration when
-- any SQL file changes — embedding VACUUM FULL would take an ACCESS
-- EXCLUSIVE lock on feeds for every unrelated future schema edit. VACUUM
-- FULL is a one-shot psql command in the Phase 0 deploy runbook instead.
ALTER TABLE feeds SET (
    fillfactor = 70,
    autovacuum_vacuum_scale_factor = 0.05,
    autovacuum_vacuum_cost_delay = 10
);
