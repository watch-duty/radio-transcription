-- Exercise durable Lease behavior without depending on PostgreSQL's internal
-- catalog representation. All fixtures are rolled back.
BEGIN;

DO $behavior$
DECLARE
    source_type_value TEXT;
BEGIN
    SELECT slug
      INTO source_type_value
      FROM public.source_types
     ORDER BY slug
     LIMIT 1;

    IF source_type_value IS NULL THEN
        RAISE EXCEPTION 'Lease behavior check requires a source type';
    END IF;

    INSERT INTO public.ingestion_leases (source_type, lease_key)
    VALUES (source_type_value, 'phase-1-behavior-check');

    BEGIN
        INSERT INTO public.ingestion_leases (
            source_type,
            lease_key,
            status
        ) VALUES (
            source_type_value,
            'phase-1-ownerless-active-check',
            'active'
        );
        RAISE EXCEPTION 'ownerless active Lease was accepted';
    EXCEPTION
        WHEN check_violation THEN NULL;
    END;

    BEGIN
        UPDATE public.ingestion_leases
           SET lease_key = 'phase-1-rekeyed'
         WHERE source_type = source_type_value
           AND lease_key = 'phase-1-behavior-check';
        RAISE EXCEPTION 'Lease identity change was accepted';
    EXCEPTION
        WHEN check_violation THEN NULL;
    END;

    UPDATE public.ingestion_leases
       SET fencing_token = 1
     WHERE source_type = source_type_value
       AND lease_key = 'phase-1-behavior-check';

    BEGIN
        UPDATE public.ingestion_leases
           SET fencing_token = 0
         WHERE source_type = source_type_value
           AND lease_key = 'phase-1-behavior-check';
        RAISE EXCEPTION 'Lease fencing token regression was accepted';
    EXCEPTION
        WHEN check_violation THEN NULL;
    END;

    BEGIN
        DELETE FROM public.ingestion_leases
         WHERE source_type = source_type_value
           AND lease_key = 'phase-1-behavior-check';
        RAISE EXCEPTION 'Lease deletion was accepted';
    EXCEPTION
        WHEN check_violation THEN NULL;
    END;
END
$behavior$;

ROLLBACK;
