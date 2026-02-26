CREATE TYPE feed_status AS ENUM (
    'unclaimed',
    'active',
    'failing',
    'quarantined',
    'deactivated'
);
