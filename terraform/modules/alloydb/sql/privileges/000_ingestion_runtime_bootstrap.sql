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

-- Parameter ACLs are cluster-shared. Remove every explicit privileged-GUC
-- grant from PUBLIC and the complete app member closure. Ordinary USERSET GUCs
-- are not represented by pg_parameter_acl and are deliberately unaffected.
DO $parameter_acl_revoke$
DECLARE
    parameter_state RECORD;
    runtime_grantee RECORD;
BEGIN
    FOR parameter_state IN
        SELECT parameter_acl.parname
          FROM pg_catalog.pg_parameter_acl AS parameter_acl
        UNION
        SELECT 'session_replication_role'::TEXT
        UNION
        SELECT 'lo_compat_privileges'::TEXT
         ORDER BY parname
    LOOP
        EXECUTE pg_catalog.format(
            'REVOKE SET, ALTER SYSTEM ON PARAMETER %I FROM PUBLIC CASCADE',
            parameter_state.parname
        );
        FOR runtime_grantee IN
            WITH RECURSIVE runtime_roles AS (
                SELECT role.oid, role.rolname
                  FROM pg_catalog.pg_roles AS role
                 WHERE role.rolname = 'app_ingestion_runtime'
                UNION
                SELECT member.oid, member.rolname
                  FROM runtime_roles AS parent
                  JOIN pg_catalog.pg_auth_members AS membership
                    ON membership.roleid = parent.oid
                  JOIN pg_catalog.pg_roles AS member
                    ON member.oid = membership.member
            )
            SELECT rolname FROM runtime_roles ORDER BY oid
        LOOP
            EXECUTE pg_catalog.format(
                'REVOKE SET, ALTER SYSTEM ON PARAMETER %I FROM %I CASCADE',
                parameter_state.parname,
                runtime_grantee.rolname
            );
        END LOOP;
    END LOOP;
END
$parameter_acl_revoke$;

-- A privileged administrator can pre-seed SUSET parameters at login without
-- granting SET. Remove unsafe global and current-database role defaults from
-- the group and every current member so a fresh session always starts safely.
DO $unsafe_role_settings$
DECLARE
    unsafe_setting RECORD;
BEGIN
    FOR unsafe_setting IN
        WITH RECURSIVE runtime_roles AS (
            SELECT role.oid, role.rolname
              FROM pg_catalog.pg_roles AS role
             WHERE role.rolname = 'app_ingestion_runtime'
            UNION
            SELECT member.oid, member.rolname
              FROM runtime_roles AS parent
              JOIN pg_catalog.pg_auth_members AS membership
                ON membership.roleid = parent.oid
              JOIN pg_catalog.pg_roles AS member
                ON member.oid = membership.member
        )
        SELECT
            runtime_role.rolname,
            role_setting.setdatabase,
            pg_catalog.split_part(configuration.setting, '=', 1)
                AS parameter_name,
            FALSE AS database_wide
          FROM runtime_roles AS runtime_role
          JOIN pg_catalog.pg_db_role_setting AS role_setting
            ON role_setting.setrole = runtime_role.oid
          CROSS JOIN LATERAL
            unnest(role_setting.setconfig) AS configuration(setting)
          JOIN pg_catalog.pg_settings AS parameter
            ON parameter.name =
               pg_catalog.split_part(configuration.setting, '=', 1)
         WHERE role_setting.setdatabase IN (
                   0::OID,
                   (
                       SELECT database.oid
                         FROM pg_catalog.pg_database AS database
                        WHERE database.datname = pg_catalog.current_database()
                   )
               )
           AND parameter.context IN ('superuser', 'superuser-backend')
        UNION ALL
        SELECT
            NULL::NAME AS rolname,
            role_setting.setdatabase,
            pg_catalog.split_part(configuration.setting, '=', 1)
                AS parameter_name,
            TRUE AS database_wide
          FROM pg_catalog.pg_db_role_setting AS role_setting
          CROSS JOIN LATERAL
            unnest(role_setting.setconfig) AS configuration(setting)
          JOIN pg_catalog.pg_settings AS parameter
            ON parameter.name =
               pg_catalog.split_part(configuration.setting, '=', 1)
         WHERE role_setting.setrole = 0::OID
           AND role_setting.setdatabase IN (
                   0::OID,
                   (
                       SELECT database.oid
                         FROM pg_catalog.pg_database AS database
                        WHERE database.datname = pg_catalog.current_database()
                   )
               )
           AND parameter.context IN ('superuser', 'superuser-backend')
         ORDER BY setdatabase, rolname NULLS FIRST, parameter_name
    LOOP
        IF unsafe_setting.database_wide
           AND unsafe_setting.setdatabase = 0::OID THEN
            EXECUTE pg_catalog.format(
                'ALTER ROLE ALL RESET %I',
                unsafe_setting.parameter_name
            );
        ELSIF unsafe_setting.database_wide THEN
            EXECUTE pg_catalog.format(
                'ALTER DATABASE %I RESET %I',
                pg_catalog.current_database(),
                unsafe_setting.parameter_name
            );
        ELSIF unsafe_setting.setdatabase = 0::OID THEN
            EXECUTE pg_catalog.format(
                'ALTER ROLE %I RESET %I',
                unsafe_setting.rolname,
                unsafe_setting.parameter_name
            );
        ELSE
            EXECUTE pg_catalog.format(
                'ALTER ROLE %I IN DATABASE %I RESET %I',
                unsafe_setting.rolname,
                pg_catalog.current_database(),
                unsafe_setting.parameter_name
            );
        END IF;
    END LOOP;
END
$unsafe_role_settings$;

-- PostgreSQL grants PUBLIC EXECUTE on these three built-in large-object
-- creators. Repository inventory found no legacy ingestion use; the postgres
-- owner/administrator retains EXECUTE while no non-admin role is re-granted it.
REVOKE EXECUTE ON FUNCTION
    pg_catalog.lo_create(oid),
    pg_catalog.lo_creat(integer),
    pg_catalog.lo_from_bytea(oid, bytea)
    FROM PUBLIC CASCADE;

DO $large_object_acl_revoke$
DECLARE
    runtime_grantee RECORD;
    large_object RECORD;
BEGIN
    FOR runtime_grantee IN
        WITH RECURSIVE runtime_roles AS (
            SELECT role.oid, role.rolname
              FROM pg_catalog.pg_roles AS role
             WHERE role.rolname = 'app_ingestion_runtime'
            UNION
            SELECT member.oid, member.rolname
              FROM runtime_roles AS parent
              JOIN pg_catalog.pg_auth_members AS membership
                ON membership.roleid = parent.oid
              JOIN pg_catalog.pg_roles AS member
                ON member.oid = membership.member
        )
        SELECT rolname FROM runtime_roles ORDER BY oid
    LOOP
        EXECUTE pg_catalog.format(
            'REVOKE EXECUTE ON FUNCTION '
            'pg_catalog.lo_create(oid), '
            'pg_catalog.lo_creat(integer), '
            'pg_catalog.lo_from_bytea(oid, bytea) '
            'FROM %I CASCADE',
            runtime_grantee.rolname
        );
    END LOOP;

    FOR large_object IN
        SELECT object.oid
          FROM pg_catalog.pg_largeobject_metadata AS object
         ORDER BY object.oid
    LOOP
        EXECUTE pg_catalog.format(
            'REVOKE SELECT, UPDATE ON LARGE OBJECT %s FROM PUBLIC CASCADE',
            large_object.oid
        );
        FOR runtime_grantee IN
            WITH RECURSIVE runtime_roles AS (
                SELECT role.oid, role.rolname
                  FROM pg_catalog.pg_roles AS role
                 WHERE role.rolname = 'app_ingestion_runtime'
                UNION
                SELECT member.oid, member.rolname
                  FROM runtime_roles AS parent
                  JOIN pg_catalog.pg_auth_members AS membership
                    ON membership.roleid = parent.oid
                  JOIN pg_catalog.pg_roles AS member
                    ON member.oid = membership.member
            )
            SELECT rolname FROM runtime_roles ORDER BY oid
        LOOP
            EXECUTE pg_catalog.format(
                'REVOKE SELECT, UPDATE ON LARGE OBJECT %s FROM %I CASCADE',
                large_object.oid,
                runtime_grantee.rolname
            );
        END LOOP;
    END LOOP;
END
$large_object_acl_revoke$;

-- Normalize defaults for every role that can effectively create in public.
-- Global revokes remove PostgreSQL's hard-wired routine/type PUBLIC defaults;
-- public-scoped revokes remove additive schema-local drift. This does not
-- alter existing pg_catalog objects or their built-in ACLs.
DO $creator_default_acl_revoke$
DECLARE
    creator_role RECORD;
    runtime_grantee RECORD;
BEGIN
    FOR creator_role IN
        SELECT role.oid, role.rolname
          FROM pg_catalog.pg_roles AS role
         WHERE pg_catalog.has_schema_privilege(
                   role.oid,
                   'public',
                   'CREATE'
               )
         ORDER BY role.oid
    LOOP
        EXECUTE pg_catalog.format(
            'ALTER DEFAULT PRIVILEGES FOR ROLE %I '
            'REVOKE ALL PRIVILEGES ON TABLES FROM PUBLIC CASCADE',
            creator_role.rolname
        );
        EXECUTE pg_catalog.format(
            'ALTER DEFAULT PRIVILEGES FOR ROLE %I '
            'REVOKE ALL PRIVILEGES ON SEQUENCES FROM PUBLIC CASCADE',
            creator_role.rolname
        );
        EXECUTE pg_catalog.format(
            'ALTER DEFAULT PRIVILEGES FOR ROLE %I '
            'REVOKE ALL PRIVILEGES ON ROUTINES FROM PUBLIC CASCADE',
            creator_role.rolname
        );
        EXECUTE pg_catalog.format(
            'ALTER DEFAULT PRIVILEGES FOR ROLE %I '
            'REVOKE ALL PRIVILEGES ON TYPES FROM PUBLIC CASCADE',
            creator_role.rolname
        );
        EXECUTE pg_catalog.format(
            'ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA public '
            'REVOKE ALL PRIVILEGES ON TABLES FROM PUBLIC CASCADE',
            creator_role.rolname
        );
        EXECUTE pg_catalog.format(
            'ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA public '
            'REVOKE ALL PRIVILEGES ON SEQUENCES FROM PUBLIC CASCADE',
            creator_role.rolname
        );
        EXECUTE pg_catalog.format(
            'ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA public '
            'REVOKE ALL PRIVILEGES ON ROUTINES FROM PUBLIC CASCADE',
            creator_role.rolname
        );
        EXECUTE pg_catalog.format(
            'ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA public '
            'REVOKE ALL PRIVILEGES ON TYPES FROM PUBLIC CASCADE',
            creator_role.rolname
        );

        FOR runtime_grantee IN
            WITH RECURSIVE runtime_roles AS (
                SELECT role.oid, role.rolname
                  FROM pg_catalog.pg_roles AS role
                 WHERE role.rolname = 'app_ingestion_runtime'
                UNION
                SELECT member.oid, member.rolname
                  FROM runtime_roles AS parent
                  JOIN pg_catalog.pg_auth_members AS membership
                    ON membership.roleid = parent.oid
                  JOIN pg_catalog.pg_roles AS member
                    ON member.oid = membership.member
            )
            SELECT rolname FROM runtime_roles ORDER BY oid
        LOOP
            EXECUTE pg_catalog.format(
                'ALTER DEFAULT PRIVILEGES FOR ROLE %I '
                'REVOKE ALL PRIVILEGES ON TABLES FROM %I CASCADE',
                creator_role.rolname,
                runtime_grantee.rolname
            );
            EXECUTE pg_catalog.format(
                'ALTER DEFAULT PRIVILEGES FOR ROLE %I '
                'REVOKE ALL PRIVILEGES ON SEQUENCES FROM %I CASCADE',
                creator_role.rolname,
                runtime_grantee.rolname
            );
            EXECUTE pg_catalog.format(
                'ALTER DEFAULT PRIVILEGES FOR ROLE %I '
                'REVOKE ALL PRIVILEGES ON ROUTINES FROM %I CASCADE',
                creator_role.rolname,
                runtime_grantee.rolname
            );
            EXECUTE pg_catalog.format(
                'ALTER DEFAULT PRIVILEGES FOR ROLE %I '
                'REVOKE ALL PRIVILEGES ON TYPES FROM %I CASCADE',
                creator_role.rolname,
                runtime_grantee.rolname
            );
            EXECUTE pg_catalog.format(
                'ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA public '
                'REVOKE ALL PRIVILEGES ON TABLES FROM %I CASCADE',
                creator_role.rolname,
                runtime_grantee.rolname
            );
            EXECUTE pg_catalog.format(
                'ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA public '
                'REVOKE ALL PRIVILEGES ON SEQUENCES FROM %I CASCADE',
                creator_role.rolname,
                runtime_grantee.rolname
            );
            EXECUTE pg_catalog.format(
                'ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA public '
                'REVOKE ALL PRIVILEGES ON ROUTINES FROM %I CASCADE',
                creator_role.rolname,
                runtime_grantee.rolname
            );
            EXECUTE pg_catalog.format(
                'ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA public '
                'REVOKE ALL PRIVILEGES ON TYPES FROM %I CASCADE',
                creator_role.rolname,
                runtime_grantee.rolname
            );
        END LOOP;
    END LOOP;
END
$creator_default_acl_revoke$;

DO $postcondition$
DECLARE
    role_state RECORD;
    parent_count BIGINT;
    settable_parameter_grant BOOLEAN;
    set_role_membership_mode TEXT;
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

    IF EXISTS (
        WITH RECURSIVE runtime_roles AS (
            SELECT role.oid
              FROM pg_catalog.pg_roles AS role
             WHERE role.rolname = 'app_ingestion_runtime'
            UNION
            SELECT member.oid
              FROM runtime_roles AS parent
              JOIN pg_catalog.pg_auth_members AS membership
                ON membership.roleid = parent.oid
              JOIN pg_catalog.pg_roles AS member
                ON member.oid = membership.member
        )
        SELECT 1
          FROM pg_catalog.pg_shdepend AS dependency
          JOIN runtime_roles AS owner
            ON owner.oid = dependency.refobjid
         WHERE dependency.refclassid =
                   'pg_catalog.pg_authid'::pg_catalog.regclass
           AND dependency.deptype = 'o'
    ) THEN
        RAISE EXCEPTION
            'app_ingestion_runtime must not own database objects';
    END IF;

    IF EXISTS (
        WITH RECURSIVE runtime_roles AS (
            SELECT role.oid
              FROM pg_catalog.pg_roles AS role
             WHERE role.rolname = 'app_ingestion_runtime'
            UNION
            SELECT member.oid
              FROM runtime_roles AS parent
              JOIN pg_catalog.pg_auth_members AS membership
                ON membership.roleid = parent.oid
              JOIN pg_catalog.pg_roles AS member
                ON member.oid = membership.member
        ),
        privileged_parameters AS (
            SELECT parameter_acl.parname
              FROM pg_catalog.pg_parameter_acl AS parameter_acl
            UNION
            SELECT 'session_replication_role'::TEXT
            UNION
            SELECT 'lo_compat_privileges'::TEXT
        )
        SELECT 1
          FROM runtime_roles AS runtime_role
          CROSS JOIN privileged_parameters AS parameter
          CROSS JOIN (VALUES ('SET'), ('ALTER SYSTEM')) AS access(privilege)
         WHERE pg_catalog.has_parameter_privilege(
                   runtime_role.oid,
                   parameter.parname,
                   access.privilege
               )
    ) THEN
        RAISE EXCEPTION
            'app ingestion member has a privileged parameter grant';
    END IF;

    IF pg_catalog.current_setting('server_version_num')::INTEGER >= 160000 THEN
        EXECUTE $parameter_set_query$
            WITH RECURSIVE runtime_roles AS (
                SELECT role.oid
                  FROM pg_catalog.pg_roles AS role
                 WHERE role.rolname = 'app_ingestion_runtime'
                UNION
                SELECT member.oid
                  FROM runtime_roles AS parent
                  JOIN pg_catalog.pg_auth_members AS membership
                    ON membership.roleid = parent.oid
                  JOIN pg_catalog.pg_roles AS member
                    ON member.oid = membership.member
            )
            SELECT EXISTS (
                SELECT 1
                  FROM runtime_roles AS runtime_role
                  CROSS JOIN pg_catalog.pg_parameter_acl AS parameter_acl
                  CROSS JOIN LATERAL
                    pg_catalog.aclexplode(parameter_acl.paracl) AS acl
                 WHERE acl.grantee <> 0::OID
                   AND pg_catalog.pg_has_role(
                       runtime_role.oid,
                       acl.grantee,
                       'SET'
                   )
            )
        $parameter_set_query$
        INTO settable_parameter_grant;
    ELSE
        WITH RECURSIVE runtime_roles AS (
            SELECT role.oid
              FROM pg_catalog.pg_roles AS role
             WHERE role.rolname = 'app_ingestion_runtime'
            UNION
            SELECT member.oid
              FROM runtime_roles AS parent
              JOIN pg_catalog.pg_auth_members AS membership
                ON membership.roleid = parent.oid
              JOIN pg_catalog.pg_roles AS member
                ON member.oid = membership.member
        )
        SELECT EXISTS (
            SELECT 1
              FROM runtime_roles AS runtime_role
              CROSS JOIN pg_catalog.pg_parameter_acl AS parameter_acl
              CROSS JOIN LATERAL
                pg_catalog.aclexplode(parameter_acl.paracl) AS acl
             WHERE acl.grantee <> 0::OID
               AND pg_catalog.pg_has_role(
                   runtime_role.oid,
                   acl.grantee,
                   'MEMBER'
               )
        )
        INTO settable_parameter_grant;
    END IF;
    IF settable_parameter_grant THEN
        RAISE EXCEPTION
            'app ingestion member can SET ROLE to a parameter grantee';
    END IF;

    IF EXISTS (
        WITH RECURSIVE runtime_roles AS (
            SELECT role.oid
              FROM pg_catalog.pg_roles AS role
             WHERE role.rolname = 'app_ingestion_runtime'
            UNION
            SELECT member.oid
              FROM runtime_roles AS parent
              JOIN pg_catalog.pg_auth_members AS membership
                ON membership.roleid = parent.oid
              JOIN pg_catalog.pg_roles AS member
                ON member.oid = membership.member
        )
        SELECT 1
          FROM pg_catalog.pg_db_role_setting AS role_setting
          CROSS JOIN LATERAL
            unnest(role_setting.setconfig) AS configuration(setting)
          JOIN pg_catalog.pg_settings AS parameter
            ON parameter.name =
               pg_catalog.split_part(configuration.setting, '=', 1)
         WHERE (
                   (
                       role_setting.setrole IN (
                           SELECT runtime_role.oid FROM runtime_roles AS runtime_role
                       )
                       AND role_setting.setdatabase IN (
                           0::OID,
                           (
                               SELECT database.oid
                                 FROM pg_catalog.pg_database AS database
                                WHERE database.datname =
                                      pg_catalog.current_database()
                           )
                       )
                   )
                   OR (
                       role_setting.setrole = 0::OID
                       AND role_setting.setdatabase IN (
                           0::OID,
                           (
                               SELECT database.oid
                                 FROM pg_catalog.pg_database AS database
                                WHERE database.datname =
                                      pg_catalog.current_database()
                           )
                       )
                   )
               )
           AND parameter.context IN ('superuser', 'superuser-backend')
    ) THEN
        RAISE EXCEPTION
            'app ingestion member has an unsafe role parameter default';
    END IF;

    set_role_membership_mode := CASE
        WHEN pg_catalog.current_setting('server_version_num')::INTEGER >= 160000
        THEN 'SET'
        ELSE 'MEMBER'
    END;

    IF EXISTS (
        WITH RECURSIVE runtime_roles AS (
            SELECT role.oid
              FROM pg_catalog.pg_roles AS role
             WHERE role.rolname = 'app_ingestion_runtime'
            UNION
            SELECT member.oid
              FROM runtime_roles AS parent
              JOIN pg_catalog.pg_auth_members AS membership
                ON membership.roleid = parent.oid
              JOIN pg_catalog.pg_roles AS member
                ON member.oid = membership.member
        )
        SELECT 1
          FROM pg_catalog.pg_proc AS procedure
          CROSS JOIN runtime_roles AS runtime_role
         WHERE procedure.oid IN (
                   'pg_catalog.lo_create(oid)'::pg_catalog.regprocedure,
                   'pg_catalog.lo_creat(integer)'::pg_catalog.regprocedure,
                   'pg_catalog.lo_from_bytea(oid, bytea)'::pg_catalog.regprocedure
               )
           AND (
               pg_catalog.has_function_privilege(
                   runtime_role.oid,
                   procedure.oid,
                   'EXECUTE'
               )
               OR pg_catalog.has_function_privilege(
                   runtime_role.oid,
                   procedure.oid,
                   'EXECUTE WITH GRANT OPTION'
               )
           )
        UNION ALL
        SELECT 1
          FROM pg_catalog.pg_proc AS procedure
          CROSS JOIN LATERAL pg_catalog.aclexplode(
              COALESCE(
                  procedure.proacl,
                  pg_catalog.acldefault('f'::"char", procedure.proowner)
              )
          ) AS acl
         WHERE procedure.oid IN (
                   'pg_catalog.lo_create(oid)'::pg_catalog.regprocedure,
                   'pg_catalog.lo_creat(integer)'::pg_catalog.regprocedure,
                   'pg_catalog.lo_from_bytea(oid, bytea)'::pg_catalog.regprocedure
               )
           AND acl.privilege_type = 'EXECUTE'
           AND (
               acl.grantee = 0::OID
               OR EXISTS (
                   SELECT 1
                     FROM runtime_roles AS runtime_role
                    WHERE acl.grantee = runtime_role.oid
                       OR (
                           acl.grantee <> 0::OID
                           AND (
                               pg_catalog.pg_has_role(
                                   runtime_role.oid,
                                   acl.grantee,
                                   'USAGE'
                               )
                               OR pg_catalog.pg_has_role(
                                   runtime_role.oid,
                                   acl.grantee,
                                   set_role_membership_mode
                               )
                           )
                       )
               )
           )
    ) THEN
        RAISE EXCEPTION
            'app ingestion member can execute a large-object creator';
    END IF;

    IF EXISTS (
        WITH RECURSIVE runtime_roles AS (
            SELECT role.oid
              FROM pg_catalog.pg_roles AS role
             WHERE role.rolname = 'app_ingestion_runtime'
            UNION
            SELECT member.oid
              FROM runtime_roles AS parent
              JOIN pg_catalog.pg_auth_members AS membership
                ON membership.roleid = parent.oid
              JOIN pg_catalog.pg_roles AS member
                ON member.oid = membership.member
        )
        SELECT 1
          FROM pg_catalog.pg_largeobject_metadata AS large_object
         WHERE EXISTS (
                   SELECT 1
                     FROM runtime_roles AS runtime_role
                    WHERE large_object.lomowner = runtime_role.oid
                       OR pg_catalog.pg_has_role(
                           runtime_role.oid,
                           large_object.lomowner,
                           'USAGE'
                       )
                       OR pg_catalog.pg_has_role(
                           runtime_role.oid,
                           large_object.lomowner,
                           set_role_membership_mode
                       )
               )
            OR EXISTS (
                   SELECT 1
                     FROM pg_catalog.aclexplode(
                         COALESCE(
                             large_object.lomacl,
                             pg_catalog.acldefault(
                                 'L'::"char",
                                 large_object.lomowner
                             )
                         )
                     ) AS acl
                    WHERE (
                              acl.privilege_type IN ('SELECT', 'UPDATE')
                              OR acl.is_grantable
                          )
                      AND (
                          acl.grantee = 0::OID
                          OR EXISTS (
                              SELECT 1
                                FROM runtime_roles AS runtime_role
                               WHERE acl.grantee = runtime_role.oid
                                  OR (
                                      acl.grantee <> 0::OID
                                      AND (
                                          pg_catalog.pg_has_role(
                                              runtime_role.oid,
                                              acl.grantee,
                                              'USAGE'
                                          )
                                          OR pg_catalog.pg_has_role(
                                              runtime_role.oid,
                                              acl.grantee,
                                              set_role_membership_mode
                                          )
                                      )
                                  )
                          )
                      )
               )
    ) THEN
        RAISE EXCEPTION
            'app ingestion member has a large-object capability';
    END IF;

    IF EXISTS (
        WITH RECURSIVE runtime_roles AS (
            SELECT role.oid
              FROM pg_catalog.pg_roles AS role
             WHERE role.rolname = 'app_ingestion_runtime'
            UNION
            SELECT member.oid
              FROM runtime_roles AS parent
              JOIN pg_catalog.pg_auth_members AS membership
                ON membership.roleid = parent.oid
              JOIN pg_catalog.pg_roles AS member
                ON member.oid = membership.member
        ),
        creators AS (
            SELECT role.oid
              FROM pg_catalog.pg_roles AS role
             WHERE pg_catalog.has_schema_privilege(
                       role.oid,
                       'public',
                       'CREATE'
                   )
        ),
        object_classes(defacl_type, acldefault_type) AS (
            VALUES
                ('r'::"char", 'r'::"char"),
                ('S'::"char", 's'::"char"),
                ('f'::"char", 'f'::"char"),
                ('T'::"char", 'T'::"char")
        ),
        future_acl AS (
            SELECT
                creator.oid AS creator_oid,
                object_class.defacl_type,
                COALESCE(
                    global_acl.defaclacl,
                    pg_catalog.acldefault(
                        object_class.acldefault_type,
                        creator.oid
                    )
                ) || COALESCE(
                    schema_acl.defaclacl,
                    '{}'::pg_catalog.aclitem[]
                ) AS acl
              FROM creators AS creator
              CROSS JOIN object_classes AS object_class
              LEFT JOIN pg_catalog.pg_default_acl AS global_acl
                ON global_acl.defaclrole = creator.oid
               AND global_acl.defaclnamespace = 0
               AND global_acl.defaclobjtype = object_class.defacl_type
              LEFT JOIN pg_catalog.pg_default_acl AS schema_acl
                ON schema_acl.defaclrole = creator.oid
               AND schema_acl.defaclnamespace =
                       'public'::pg_catalog.regnamespace
               AND schema_acl.defaclobjtype = object_class.defacl_type
        )
        SELECT 1
          FROM future_acl
          CROSS JOIN LATERAL pg_catalog.aclexplode(future_acl.acl) AS acl
         WHERE acl.grantee = 0::OID
            OR EXISTS (
                SELECT 1
                  FROM runtime_roles AS runtime_role
                 WHERE acl.grantee = runtime_role.oid
                    OR (
                        acl.grantee <> 0::OID
                        AND pg_catalog.pg_has_role(
                            runtime_role.oid,
                            acl.grantee,
                            'USAGE'
                        )
                    ) OR (
                        acl.grantee <> 0::OID
                        AND pg_catalog.pg_has_role(
                            runtime_role.oid,
                            acl.grantee,
                            set_role_membership_mode
                        )
                    )
            )
    ) THEN
        RAISE EXCEPTION
            'future public objects expose PUBLIC or an app ingestion member';
    END IF;
END
$postcondition$;

COMMIT;
