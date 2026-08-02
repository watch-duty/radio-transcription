# Mock Audio Files Directory

The `local_dev/mock_audio` directory is mounted to the local `mock-audio-server` Docker container at `/data`.

When you run the system locally, the mock audio server will automatically serve audio files from subdirectories matching the feed's source type and source feed ID.

---

## 🔍 Discovering Feeds & IDs

To mock audio for a feed, you need to know its `<data_source>` (its source type) and its `<feed_id>` (its source feed ID).

1. **Pre-seeded Feeds:** During local development (`mise run dev`), the database is seeded using [local_dev/test_data.sql](../local_dev/test_data.sql). The pre-seeded feeds are:
   - **Broadcastify Calls:** Data source `broadcastify_calls` / Feed ID `2912` *(Pre-populated out-of-the-box)*
   - **Fire Notifications:** Data source `fire_notifications` / Feed ID `RECORDINGS/SAN-JOSE-DISP`
   - **Icecast Feed:** Data source `bcfy_feeds` / Feed ID `test-feed`
2. **Custom Feeds:** If you add a feed through the UI or directly in the DB, look up its `source_type` (e.g. `broadcastify_calls`) and `source_feed_id` (e.g. `2912` or similar unique string) in the `feeds` and `feed_properties` tables.

---

## 📥 Usage

The easiest way to add an audio file for testing is to use the provided `mise` task. This automatically creates the correct subdirectory structure:

```bash
# Usage:
mise run dev:add-audio <data_source> <feed_id> <path/to/your/audio.flac>

# Examples:
mise run dev:add-audio broadcastify_calls 2912 local_dev/mock_audio/test_bcfy.flac
mise run dev:add-audio fire_notifications RECORDINGS/SAN-JOSE-DISP local_dev/mock_audio/test_bcfy.flac
```

---

## ⚙️ Mock Server Behavior

- **Supported Audio Formats:** The mock server supports `.mp3`, `.wav`, `.flac`, `.m4a`, and `.ogg` files.
- **Sequential Playback:** If a feed directory contains multiple audio files, the mock server will sequentially cycle through them on each request to simulate new incoming transmissions.
- **Initial Setup:** The repository pre-populates `broadcastify_calls/2912/` with `test_bcfy.flac` as an initial sample file so that local development works immediately out of the box.

---

## 📂 Directory Structure (Under the Hood)

If you need to manage the files manually, organize them using the following path template:
`local_dev/mock_audio/<data_source>/<source_feed_id>/`
