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

ALTER ROLE app_ingestion_runtime
    NOLOGIN
    NOSUPERUSER
    NOCREATEDB
    NOCREATEROLE
    INHERIT
    NOREPLICATION
    NOBYPASSRLS
    CONNECTION LIMIT -1;
ALTER ROLE app_ingestion_runtime PASSWORD NULL;
ALTER ROLE app_ingestion_runtime RESET ALL;

-- The group must never inherit a broader role. Member logins are deliberately
-- not changed: they inherit this group, not the reverse.
DO $memberships$
DECLARE
    parent_role RECORD;
BEGIN
    FOR parent_role IN
        SELECT parent.rolname
          FROM pg_catalog.pg_auth_members AS membership
          JOIN pg_catalog.pg_roles AS parent
            ON parent.oid = membership.roleid
          JOIN pg_catalog.pg_roles AS member
            ON member.oid = membership.member
         WHERE member.rolname = 'app_ingestion_runtime'
    LOOP
        EXECUTE pg_catalog.format(
            'REVOKE %I FROM app_ingestion_runtime',
            parent_role.rolname
        );
    END LOOP;
END
$memberships$;

-- PUBLIC keeps the two allowed connection prerequisites (CONNECT and public
-- schema USAGE). Remove database/schema creation surfaces that would otherwise
-- become effective through PUBLIC, then grant the allowed rights explicitly.
DO $database_acl$
BEGIN
    EXECUTE pg_catalog.format(
        'REVOKE CREATE, TEMPORARY ON DATABASE %I FROM PUBLIC',
        pg_catalog.current_database()
    );
    EXECUTE pg_catalog.format(
        'REVOKE ALL PRIVILEGES ON DATABASE %I FROM app_ingestion_runtime',
        pg_catalog.current_database()
    );
    EXECUTE pg_catalog.format(
        'GRANT CONNECT ON DATABASE %I TO app_ingestion_runtime',
        pg_catalog.current_database()
    );
END
$database_acl$;

REVOKE CREATE ON SCHEMA public FROM PUBLIC;
REVOKE ALL PRIVILEGES ON SCHEMA public FROM app_ingestion_runtime;
GRANT USAGE ON SCHEMA public TO app_ingestion_runtime;

-- Migrations run as postgres. These are revocations only: future application
-- objects never acquire runtime DML, sequence, function, or unrelated type
-- rights from either a direct default ACL or PostgreSQL's PUBLIC defaults.
-- Global revokes remove built-in function/type PUBLIC defaults; scoped revokes
-- below also remove any explicit public-schema additions.
ALTER DEFAULT PRIVILEGES FOR ROLE postgres
    REVOKE ALL PRIVILEGES ON TABLES FROM app_ingestion_runtime;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres
    REVOKE ALL PRIVILEGES ON TABLES FROM PUBLIC;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres
    REVOKE ALL PRIVILEGES ON SEQUENCES FROM app_ingestion_runtime;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres
    REVOKE ALL PRIVILEGES ON SEQUENCES FROM PUBLIC;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres
    REVOKE ALL PRIVILEGES ON ROUTINES FROM app_ingestion_runtime;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres
    REVOKE EXECUTE ON ROUTINES FROM PUBLIC;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres
    REVOKE ALL PRIVILEGES ON TYPES FROM app_ingestion_runtime;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres
    REVOKE USAGE ON TYPES FROM PUBLIC;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public
    REVOKE ALL PRIVILEGES ON TABLES FROM app_ingestion_runtime;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public
    REVOKE ALL PRIVILEGES ON TABLES FROM PUBLIC;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public
    REVOKE ALL PRIVILEGES ON SEQUENCES FROM app_ingestion_runtime;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public
    REVOKE ALL PRIVILEGES ON SEQUENCES FROM PUBLIC;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public
    REVOKE ALL PRIVILEGES ON ROUTINES FROM app_ingestion_runtime;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public
    REVOKE EXECUTE ON ROUTINES FROM PUBLIC;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public
    REVOKE ALL PRIVILEGES ON TYPES FROM app_ingestion_runtime;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public
    REVOKE USAGE ON TYPES FROM PUBLIC;

DO $postcondition$
DECLARE
    role_state RECORD;
    parent_count BIGINT;
    owned_object_count BIGINT;
BEGIN
    SELECT
        rolcanlogin,
        rolsuper,
        rolinherit,
        rolcreaterole,
        rolcreatedb,
        rolreplication,
        rolbypassrls,
        rolconnlimit,
        rolconfig
      INTO role_state
      FROM pg_catalog.pg_roles
     WHERE rolname = 'app_ingestion_runtime';

    IF NOT FOUND
       OR role_state.rolcanlogin
       OR role_state.rolsuper
       OR NOT role_state.rolinherit
       OR role_state.rolcreaterole
       OR role_state.rolcreatedb
       OR role_state.rolreplication
       OR role_state.rolbypassrls
       OR role_state.rolconnlimit <> -1
       OR role_state.rolconfig IS NOT NULL THEN
        RAISE EXCEPTION
            'app_ingestion_runtime has unsafe role attributes';
    END IF;

    SELECT pg_catalog.count(*)
      INTO parent_count
      FROM pg_catalog.pg_auth_members AS membership
      JOIN pg_catalog.pg_roles AS member
        ON member.oid = membership.member
     WHERE member.rolname = 'app_ingestion_runtime';

    IF parent_count <> 0 THEN
        RAISE EXCEPTION
            'app_ingestion_runtime retains an inherited parent role';
    END IF;

    SELECT
        (SELECT pg_catalog.count(*)
           FROM pg_catalog.pg_database AS database
          WHERE database.datdba = role.oid)
      + (SELECT pg_catalog.count(*)
           FROM pg_catalog.pg_namespace AS namespace
          WHERE namespace.nspowner = role.oid)
      + (SELECT pg_catalog.count(*)
           FROM pg_catalog.pg_class AS relation
          WHERE relation.relowner = role.oid)
      + (SELECT pg_catalog.count(*)
           FROM pg_catalog.pg_proc AS procedure
          WHERE procedure.proowner = role.oid)
      + (SELECT pg_catalog.count(*)
           FROM pg_catalog.pg_type AS type
          WHERE type.typowner = role.oid)
      INTO owned_object_count
      FROM pg_catalog.pg_roles AS role
     WHERE role.rolname = 'app_ingestion_runtime';

    IF owned_object_count <> 0 THEN
        RAISE EXCEPTION
            'app_ingestion_runtime must not own database objects';
    END IF;
END
$postcondition$;

COMMIT;
