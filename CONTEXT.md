# Ingestion Context

This context describes the language used for feed ingestion, failure handling,
and quarantine decisions.

## Language

**Feed**:
A configured upstream audio source that the ingestion system may claim, poll,
stream, and process. A feed has one lifecycle status at a time.

**Leased Feed**:
A feed currently owned by one worker through a fencing token. A leased feed can
carry stale failure state from a previous failed processing episode.

**Captured Chunk**:
An audio payload emitted by a collector for runtime upload, publish, and
bookmarking. Avoid: call item, file listing, source response.

**Source Observation**:
A successful non-audio source check emitted by a collector when the source was
reachable but no audio payload should be processed. Avoid: empty chunk,
synthetic chunk.

**Observation Boundary**:
The source-specific scope used to decide whether item failures are isolated or
feed-level. For polling collectors this is usually one response page or file
listing.

**Collector-Local Failure Streak**:
An in-memory streak of failed poll, fetch, connection, or source operations
inside one collector task. It resets on successful source contact, even when no
audio is present.

**Feed Failure Episode**:
A terminal feed-level failure recorded in storage after a collector or runtime
decides the current feed cannot make progress. Consecutive feed failure
episodes drive quarantine.

**Status Reason**:
The current canonical abnormal-condition label for a feed. It says whether the
likely owner is the source/provider or the ingestion system.

**Quarantine**:
A lifecycle state that makes a feed ineligible for normal claiming after too
many consecutive feed failure episodes.

## Example Dialogue

Developer: "The Calls API returned an empty `calls` list."

Domain expert: "That is a source observation, not a captured chunk. Reset the
collector-local failure streak, and clear feed failure state only if the leased
feed was dirty."

Developer: "A page had three call items and all downloads failed."

Domain expert: "That observation boundary produced a feed failure episode. If
those episodes repeat, the feed can be quarantined."
