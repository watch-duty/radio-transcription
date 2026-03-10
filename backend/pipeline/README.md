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
