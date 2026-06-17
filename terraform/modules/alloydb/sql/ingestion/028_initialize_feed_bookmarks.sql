-- Set default value for last_bookmark_time to NOW() for new feed records.
ALTER TABLE feeds ALTER COLUMN last_bookmark_time SET DEFAULT NOW();
