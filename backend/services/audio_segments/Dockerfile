FROM python:3.13-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV WORKDIR=/app
ENV PYTHONPATH=/app
ENV PORT=8080

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:0.7.12 /uv /uvx /bin/

COPY pyproject.toml uv.lock ./

ENV UV_PROJECT_ENVIRONMENT=/app/.venv
ENV PATH="/app/.venv/bin:$PATH"

COPY backend/pipeline/normalization/pyproject.toml ./backend/pipeline/normalization/
COPY backend/pipeline/transcription/pyproject.toml ./backend/pipeline/transcription/
RUN uv sync --frozen --no-dev && uv pip install grpcio-tools

COPY protos/ ./protos

# Create directory for generated files
RUN mkdir -p ./backend/pipeline/schema_types
RUN touch ./backend/pipeline/schema_types/__init__.py
RUN python -m grpc_tools.protoc -I=protos --python_out=backend/pipeline/schema_types protos/*.proto

COPY backend/pipeline/common/ ./backend/pipeline/common/
COPY backend/pipeline/storage/ ./backend/pipeline/storage/
COPY backend/services/audio_segments/ ./backend/services/audio_segments/

CMD exec uvicorn backend.services.audio_segments.main:app --host 0.0.0.0 --port $PORT
