# Radio Transcription

Radio Transcription captures radio audio from source-specific collectors and
routes usable chunks through the transcription pipeline. This language describes
the ingestion failure-classification domain.

## Language

**Collector**:
A source-specific ingestion component that turns an upstream stream, API, or
file listing into `CapturedChunk` values or a typed feed-level failure.
_Avoid_: Capturer, scraper

**Feed-Level Failure**:
A classified failure for the feed as a whole, reported at the collector/runtime
boundary when the source or configuration cannot currently produce usable audio.
_Avoid_: Item failure, raw error

**Item Failure**:
A failure for one eligible discrete audio item, such as one call recording or
one file in a file-list poll. It is not feed-level unless all eligible items in
the relevant promotion context fail. HTTP failures for discrete item downloads
use the `item_http_*` raw-reason prefix.
_Avoid_: Feed failure, source outage

**Item Download Result**:
The outcome of attempting to download one discrete audio item. Broadcastify
Calls, OpenMHz, and Fire Notifications use this concept; Broadcastify
Feeds/Icecast does not because it captures a continuous stream. It can include
bounded download metadata such as content type when the collector needs that to
construct a chunk. It is either a success, a failure, or empty/no-result; it
must not be both success and failure.
_Avoid_: Fetch result, chunk result

**Observation Boundary**:
A natural source batch where all eligible item failures can imply a feed-level
problem. Broadcastify Calls uses one API page; Fire Notifications uses one
file-list poll.
_Avoid_: Failure streak

**Item Failure Window**:
A collector-defined run of consecutive eligible item failures used when a source
has no natural batch. OpenMHz uses call download failures since the last
successful yielded chunk.
_Avoid_: Observation boundary

**Stream Endpoint Failure**:
A continuous stream failure classified from the direct stream endpoint, ffmpeg
stderr, or same-endpoint probe evidence. Broadcastify Feeds/Icecast uses this
model instead of per-item download failure promotion.
_Avoid_: Item failure

## Example Dialogue

Developer: "OpenMHz had ten call downloads fail in a row. Is that an observation
boundary?"

Domain expert: "No. OpenMHz has no natural page or poll batch. Treat that as an
item failure window."

Developer: "Broadcastify Feeds is also long-running. Should it use the same item
failure window?"

Domain expert: "No. Broadcastify Feeds is an Icecast-style stream. Classify its
direct stream endpoint failure, not per-item downloads."
