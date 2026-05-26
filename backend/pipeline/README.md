# Radio Transcription Pipeline Backend

## Feed Health Vocabulary

This section defines the ingestion feed-health terms used by Fire Notifications, bcfy_calls, and OpenMHZ. Current feed failure handling is quarantine escalation, not true circuit-breaker semantics: quarantine is sticky and has no half-open automatic recovery path.

| Term | Definition |
|------|------------|
| Silent poll/page | A completed poll or page with no attempted new actionable items. |
| Unproductive poll/page | A completed poll or page with attempted > 0 and produced == 0. |
| Produced chunk | A chunk yielded back across the collector/runtime boundary. |
| source_unreachable | Reason for sustained poll, API, or transport reachability failure. |
| downloads_failing | Reason or evidence for attempted items producing zero chunks. |

| Source | Collector shape | Unproductive batch behavior | Quarantine policy in this milestone |
|--------|-----------------|-----------------------------|-------------------------------------|
| Fire Notifications | Bounded listing poll plus per-file downloads. | Completed polls emit `batch_unproductive` when attempted > 0 and produced == 0. | Sustained completed unproductive polls escalate through the existing runtime failure/quarantine path with reason `downloads_failing`. |
| bcfy_calls | Bounded API pages plus per-call downloads. | Completed all-download-failed pages emit `batch_unproductive` evidence. | Evidence only; they do not drive quarantine in this milestone because sticky quarantine behavior for all-download-failed pages is deferred. |
| OpenMHZ | WebSocket/event-driven calls plus per-call downloads. | Out of scope for bounded poll/page policy. | Out of scope for bounded poll/page policy in this milestone. |

Fire Notifications escalates only after sustained completed unproductive polls because raising on the first failed download can turn one bad object into head-of-line blocking and repeated poison-pill quarantine. The useful success boundary is a produced chunk yielded to the runtime, not a raw download.

`FIRE_NOTIFICATIONS_S3_BASE` is a global FN download base. If it is wrong, many FN feeds can quarantine at once; that consequence is accepted because every affected feed is genuinely broken and visible quarantine is preferable to silent zero output.

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
