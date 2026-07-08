# syntax=docker/dockerfile:1.9-labs
FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy

WORKDIR /app

ENV PYTHONPATH=/app

# ---------------------------------------------------------------------------
# HANDOFF: cloud-init prerequisites (must land in the infrastructure PR)
# ---------------------------------------------------------------------------
# The container cannot set these from inside; they must be applied at VM-level
# via cloud-init in the follow-up Terraform PR against radio-transcription-deployment.
# HANDOFF: net.ipv4.ip_local_port_range = "10000 65535"
# HANDOFF: net.ipv4.tcp_keepalive_time = 60
# HANDOFF: net.ipv4.tcp_keepalive_intvl = 10
# HANDOFF: net.ipv4.tcp_keepalive_probes = 3
# HANDOFF: container --stop-timeout >= 100 s (90 s graceful budget + ~10 s teardown
#          vs the GCE 120 s ACPI cap)
# ---------------------------------------------------------------------------

# 1. Install ffmpeg (audio processing) and libjemalloc2 (memory allocator).
# MEMORY-01: libjemalloc2's so-file lives at a multiarch path
# (/usr/lib/x86_64-linux-gnu/libjemalloc.so.2 on amd64,
# /usr/lib/aarch64-linux-gnu/libjemalloc.so.2 on arm64). Resolved at build
# time by the symlink below so LD_PRELOAD stays portable across
# architectures (lets local M-series Mac dev work without
# --platform linux/amd64). See https://packages.debian.org/bookworm/libjemalloc2.
COPY --from=mwader/static-ffmpeg:6.1.1 /ffmpeg /usr/local/bin/
COPY --from=mwader/static-ffmpeg:6.1.1 /ffprobe /usr/local/bin/
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        libjemalloc2 && \
    rm -rf /var/lib/apt/lists/*

# MEMORY-01: jemalloc allocator + glibc fallback. Build-time symlink
# resolves the multiarch jemalloc path (amd64 → x86_64-linux-gnu,
# arm64 → aarch64-linux-gnu) so LD_PRELOAD is the same on every arch.
# Production MIG hardware is amd64; this just makes local dev portable.
# MALLOC_ARENA_MAX=2 is a belt-and-suspenders fallback (only effective
# if jemalloc fails to load — under normal operation it is a no-op since
# jemalloc bypasses glibc malloc).
RUN JEMALLOC_PATH="$(find / -name "libjemalloc.so*" 2>/dev/null | head -1)" && \
    if [ -z "$JEMALLOC_PATH" ]; then \
        echo "ERROR: libjemalloc.so not found on filesystem — package install or path layout may have changed" >&2 && exit 1; \
    fi && \
    ln -sf "$JEMALLOC_PATH" /usr/local/lib/libjemalloc.so.2
ENV LD_PRELOAD=/usr/local/lib/libjemalloc.so.2
ENV MALLOC_ARENA_MAX=2

# MEMORY-01 (D-04): LD_PRELOAD propagates to ffmpeg children by default
# (asyncio.create_subprocess_exec uses env=None which inherits parent env).
# This is intentional — ffmpeg also benefits from jemalloc fragmentation
# reduction. If a future ffmpeg version exhibits issues under preloaded
# jemalloc, override per-spawn with:
#     env={**os.environ, "LD_PRELOAD": ""}
# passed into create_subprocess_exec. NOT applied today.

# 2. Install dependencies
COPY --from=ghcr.io/astral-sh/uv:0.11.13 /uv /uvx /bin/
COPY --parents uv.lock pyproject.toml backend/pipeline/*/pyproject.toml backend/services/*/pyproject.toml ./

ENV UV_PROJECT_ENVIRONMENT=/app/.venv
ENV PATH="/app/.venv/bin:$PATH"
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev && \
    uv export --group build --locked -o build-requirements.txt && \
    uv pip install -r build-requirements.txt

COPY backend/ ./backend/
COPY protos/ ./protos

# 3. Create the directory for the generated files
RUN mkdir -p ./backend/pipeline/schema_types
RUN touch ./backend/pipeline/schema_types/__init__.py
RUN python -m grpc_tools.protoc -I=protos --python_out=backend/pipeline/schema_types protos/*.proto

CMD ["python", "-m", "backend.pipeline.ingestion.main"]
