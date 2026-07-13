-- Lease identities are permanent, and fencing tokens may only move forward.
-- Recreate the guards transactionally so migration replay remains simple and
-- never exposes a partially guarded table.
BEGIN;

CREATE OR REPLACE FUNCTION public.guard_ingestion_lease_identity()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $guard$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            MESSAGE = 'ingestion Lease identities cannot be deleted';
    END IF;

    IF TG_OP = 'TRUNCATE' THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            MESSAGE = 'ingestion Lease identities cannot be truncated';
    END IF;

    IF TG_OP = 'UPDATE' THEN
        IF NEW.source_type IS DISTINCT FROM OLD.source_type THEN
            RAISE EXCEPTION USING
                ERRCODE = '23514',
                MESSAGE = 'ingestion Lease source identity cannot be changed';
        END IF;

        IF NEW.lease_key IS DISTINCT FROM OLD.lease_key THEN
            RAISE EXCEPTION USING
                ERRCODE = '23514',
                MESSAGE = 'ingestion Lease key cannot be changed';
        END IF;

        IF NEW.fencing_token < OLD.fencing_token THEN
            RAISE EXCEPTION USING
                ERRCODE = '23514',
                MESSAGE = 'ingestion Lease fencing token cannot regress';
        END IF;

        RETURN NEW;
    END IF;

    RAISE EXCEPTION USING
        ERRCODE = '23514',
        MESSAGE = 'unsupported ingestion Lease guard operation';
END;
$guard$;

DROP TRIGGER IF EXISTS trg_ingestion_leases_prevent_delete
    ON public.ingestion_leases;
CREATE TRIGGER trg_ingestion_leases_prevent_delete
    BEFORE DELETE ON public.ingestion_leases
    FOR EACH ROW
    EXECUTE FUNCTION public.guard_ingestion_lease_identity();

DROP TRIGGER IF EXISTS trg_ingestion_leases_prevent_truncate
    ON public.ingestion_leases;
CREATE TRIGGER trg_ingestion_leases_prevent_truncate
    BEFORE TRUNCATE ON public.ingestion_leases
    FOR EACH STATEMENT
    EXECUTE FUNCTION public.guard_ingestion_lease_identity();

DROP TRIGGER IF EXISTS trg_ingestion_leases_protect_identity_and_fence
    ON public.ingestion_leases;
CREATE TRIGGER trg_ingestion_leases_protect_identity_and_fence
    BEFORE UPDATE OF source_type, lease_key, fencing_token
    ON public.ingestion_leases
    FOR EACH ROW
    EXECUTE FUNCTION public.guard_ingestion_lease_identity();

ALTER TABLE public.ingestion_leases
    ENABLE ALWAYS TRIGGER trg_ingestion_leases_prevent_delete;
ALTER TABLE public.ingestion_leases
    ENABLE ALWAYS TRIGGER trg_ingestion_leases_prevent_truncate;
ALTER TABLE public.ingestion_leases
    ENABLE ALWAYS TRIGGER trg_ingestion_leases_protect_identity_and_fence;

COMMIT;
