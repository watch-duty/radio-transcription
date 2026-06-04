# Radio Transcription Pipeline Backend

## Ingestion Collector Contract

The collector/runtime boundary and feed failure-classification policy are
documented in `backend/pipeline/ingestion/collectors/README.md`. Read that
guide before adding a new audio source or changing `status_reason` behavior.

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

## Echo Recording Timestamps

Echo per-transmission MP3 files are produced by RTL-Airband with
`split_on_transmission = True`. For current Echo configs, RTL-Airband appends
`_YYYYMMDD_HHMMSS` when it opens a new recording file, so that filename suffix
is the UTC recording start time.

Do not treat GCS object metadata such as `timeCreated` as capture time. It is
the object finalization/upload time and can be delayed or batched. Echo ingestion
should use the filename timestamp as the primary recording time source and only
fall back to GCS object time when the filename cannot be parsed.
