-- Establish the dormant ingestion group before application schema DDL.
-- Reconciliation after migrations owns all named-object grants.
BEGIN;

DO $bootstrap$
BEGIN
    PERFORM pg_catalog.set_config('search_path', 'pg_catalog', TRUE);

    IF NOT EXISTS (
        SELECT 1
          FROM pg_catalog.pg_roles
         WHERE rolname = 'app_ingestion_runtime'
    ) THEN
        CREATE ROLE app_ingestion_runtime
            NOLOGIN
            NOSUPERUSER
            NOCREATEDB
            NOCREATEROLE
            INHERIT
            NOREPLICATION
            NOBYPASSRLS;
    END IF;
END
$bootstrap$;

\ir 100_ingestion_runtime_hardening.sql

DO $bootstrap_database_grant$
BEGIN
    EXECUTE pg_catalog.format(
        'GRANT CONNECT ON DATABASE %I TO app_ingestion_runtime',
        pg_catalog.current_database()
    );
END
$bootstrap_database_grant$;

GRANT USAGE ON SCHEMA public TO app_ingestion_runtime;

COMMIT;
