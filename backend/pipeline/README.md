# Radio Transcription Pipeline Backend


## Protobuf Generation

We use Protocol Buffers (`.proto` files) to define schema types and gRPC boundaries in `protos/`. Because the generated Python code is ignored by git to keep version history clean, you must generate the Python bindings locally after cloning the repository or whenever you modify the `.proto` files.

To generate the protobufs:

### Recommendation:
The easiest way is to use the `mise` task from the repository root:
```bash
mise run generate:protos
```

### Manual Command:
If you don't use `mise`, you can run the following from the root:
```bash
uv run python -m grpc_tools.protoc -I protos --python_out=backend/pipeline/schema_types --grpc_python_out=backend/pipeline/schema_types --pyi_out=backend/pipeline/schema_types protos/*.proto
```

Once the wrappers are generated into `backend/pipeline/schema_types`, other local components can immediately import the generated schema modules (for example, `backend.pipeline.schema_types.<name>_pb2`).

## Ingestion Pipeline

The `backend/pipeline/ingestion/` package handles real-time audio capture from radio feeds, uploads to Google Cloud Storage, and event publication to Pub/Sub.

### GCP Helper (`gcp_helper.py`)

`gcp_helper` is the shared GCP client layer for the ingestion pipeline. It exposes three public async functions:

| Function | Description |
|---|---|
| `upload_audio(audio_chunk, feed, bucket, chunk_seq, sed_metadata=None)` | Upload a raw audio chunk to GCS. The object is stored at `{source_type}/{feed_id}/{timestamp}_{seq}.flac`. Optional `sed_metadata` (a `SedMetadata` protobuf) is serialized, base64-encoded, and attached as GCS object metadata. Returns the full `gs://bucket/object` URI. |
| `publish_audio_chunk(topic_path, feed_id, gcs_uri)` | Serialize an `AudioChunk` protobuf and publish it to a Pub/Sub topic with message ordering enabled. The `feed_id` is used as the `ordering_key` so chunks from the same feed are delivered in order. Returns the published message ID. |
| `close_client()` | Gracefully close all GCP connections (aiohttp session, GCS storage client, Pub/Sub publisher) held by the process-wide singleton, then evict the singleton from the cache. |

#### Client lifecycle

All clients (`aiohttp.ClientSession`, `gcloud.aio.storage.Storage`, `pubsub_v1.PublisherClient`) are owned by a single `GCPClients` instance that is created lazily on first use and cached for the lifetime of the process via `@functools.cache`. Call `close_client()` during application shutdown to release these resources cleanly.

```python
from backend.pipeline.ingestion.gcp_helper import upload_audio, publish_audio_chunk, close_client

# Upload an audio chunk
gcs_uri = await upload_audio(audio_bytes, feed, bucket="my-bucket", chunk_seq=0)

# Publish the URI to Pub/Sub
msg_id = await publish_audio_chunk(topic_path, feed_id=str(feed["id"]), gcs_uri=gcs_uri)

# On shutdown
await close_client()
```

#### Testing

Tests reset the singleton between cases using the cache-clear helper that `functools.cache` provides:

```python
import backend.pipeline.ingestion.gcp_helper as gcp_helper

def setUp(self):
  gcp_helper._get_default_clients.cache_clear()

def tearDown(self):
  gcp_helper._get_default_clients.cache_clear()
```

#### Migration from `gcs.py`

`gcp_helper.py` supersedes the old `gcs.py` module. The main changes are:

- **Renamed** from `gcs` → `gcp_helper` to reflect that Pub/Sub support (previously in `normalizer_runtime`) is now consolidated here.
- **`GCPClients` class** replaces the three module-level globals (`_session`, `_storage`, `_publisher`). The class owns all three clients and provides `get_storage()`, `get_publisher()`, and `async close()`.
- **Singleton via `@functools.cache`** on `_get_default_clients()` instead of `global` statements.
- **`publisher.stop()` is non-blocking** — it is now dispatched to a thread pool via `asyncio.to_thread()` so it cannot stall the event loop.
