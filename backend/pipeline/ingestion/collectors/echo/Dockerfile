# syntax=docker/dockerfile:1.9-labs
FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy

WORKDIR /app

ENV PYTHONPATH=/app

# 1. Install system dependencies (ffmpeg/ffprobe required by get_audio_duration)
COPY --from=mwader/static-ffmpeg:6.1.1 /ffmpeg /usr/local/bin/
COPY --from=mwader/static-ffmpeg:6.1.1 /ffprobe /usr/local/bin/

# 2. Install Python dependencies
COPY --from=ghcr.io/astral-sh/uv:0.11.13 /uv /uvx /bin/
COPY --parents uv.lock pyproject.toml backend/pipeline/*/pyproject.toml backend/services/*/pyproject.toml ./
ENV UV_PROJECT_ENVIRONMENT=/app/.venv
ENV PATH="/app/.venv/bin:$PATH"
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-workspace && uv pip install grpcio-tools
COPY backend/ ./backend/

# 3. Compile proto definitions
COPY protos/ ./protos
RUN mkdir -p ./backend/pipeline/schema_types \
    && touch ./backend/pipeline/schema_types/__init__.py \
    && python -m grpc_tools.protoc \
        -I=protos \
        --python_out=backend/pipeline/schema_types \
        protos/*.proto

CMD ["functions-framework", \
     "--source=backend/pipeline/ingestion/collectors/echo/main.py", \
     "--target=handle_notification", \
     "--signature-type=cloudevent"]
