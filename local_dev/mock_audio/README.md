# Mock Audio Files Directory

This directory is mounted to the local `mock-audio-server` Docker container at `/data`.

When you run the system locally (e.g., using `mise run dev:start`), the mock audio server will automatically serve audio files from subdirectories based on the requested data source and feed ID.

### Directory Structure
Audio files must be organized by data source and feed ID:
`local_dev/mock_audio/<data_source>/<source_feed_id>/`

### How to use:
The easiest way to add an audio file for testing is to use the provided mise task. This automatically creates the correct subdirectory structure:

```bash
mise run dev:add-audio <data_source> <feed_id> path/to/your/audio.flac
```

**Examples:**
- `mise run dev:add-audio broadcastify_calls 2912 local_dev/mock_audio/test_bcfy.flac`
- `mise run dev:add-audio fire_notifications RECORDINGS/SAN-JOSE-DISP local_dev/mock_audio/test_bcfy.flac`

Ensure the files have one of the supported audio extensions (`.mp3`, `.wav`, `.flac`, `.m4a`, `.ogg`). The mock server will sequentially cycle through the files on each request, simulating new incoming audio on that specific feed.

*Note: `test_bcfy.flac` is pre-populated in `broadcastify_calls/2912/` as an initial sample file so that it works out of the box.*
