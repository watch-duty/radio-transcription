FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

ENV PYTHONPATH=/app

# 1. Install dependencies
COPY --from=ghcr.io/astral-sh/uv:0.7.12 /uv /uvx /bin/
COPY pyproject.toml uv.lock ./
# We intentionally copy the entire backend/ directory before running uv sync.
# This ensures that ALL workspace sub-projects (e.g. transcription, ingestion) have
# their corresponding pyproject.toml files present. Without this, uv cannot resolve
# the local workspace dependencies defined in the root pyproject.toml.
COPY backend/ ./backend/
ENV UV_PROJECT_ENVIRONMENT=/app/.venv
ENV PATH="/app/.venv/bin:$PATH"
RUN uv sync --frozen --no-dev && uv pip install grpcio-tools
COPY protos/ ./protos

# 2. Create the directory for the generated files
RUN mkdir -p ./backend/pipeline/schema_types
RUN touch ./backend/pipeline/schema_types/__init__.py
RUN python -m grpc_tools.protoc -I=protos --python_out=backend/pipeline/schema_types protos/*.proto

CMD ["functions-framework", \
     "--source=backend/pipeline/evaluation/main.py", \
     "--target=evaluate_transcribed_audio_segment", \
     "--signature-type=cloudevent"]