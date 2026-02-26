CREATE TABLE feed_properties_icecast (
    feed_id    UUID PRIMARY KEY REFERENCES feeds(id) ON DELETE CASCADE,
    stream_url TEXT NOT NULL
);
