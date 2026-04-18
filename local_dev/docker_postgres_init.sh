#!/bin/sh
# Applies SQL migrations from /sql in lexical order, skipping pg_cron files.
# pg_cron is production-only (AlloyDB alloydb.enable_pg_cron flag); the
# postgres:15-alpine image used by docker-compose doesn't ship the extension,
# so CREATE EXTENSION pg_cron would fail and crash the container. Mirrors the
# *pg_cron* skip in .github/workflows/ci.yml's alloydb-hot-protection-check job
# and in the integration-test fixtures.
set -e
# Shell globs expand in alphabetical order (matches the ingestion fileset's
# behavior in main.tf:115 and the CI skip loop in .github/workflows/ci.yml).
for f in /sql/*.sql; do
    name=$(basename "$f")
    case "$name" in
        *pg_cron*)
            echo "SKIP $name (pg_cron extension not available in docker-compose postgres)"
            continue
            ;;
    esac
    echo "APPLY $name"
    psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f "$f"
done
