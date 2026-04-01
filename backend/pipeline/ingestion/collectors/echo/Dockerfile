FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

ENV PYTHONPATH=/app

# 1. Install system dependencies (ffmpeg required by pydub for MP3→FLAC conversion)
RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/*

# 2. Install Python dependencies
COPY --from=ghcr.io/astral-sh/uv:0.7.12 /uv /uvx /bin/
COPY pyproject.toml uv.lock ./
# Copy the entire backend/ so uv can resolve workspace sub-projects.
COPY backend/ ./backend/
ENV UV_PROJECT_ENVIRONMENT=/app/.venv
ENV PATH="/app/.venv/bin:$PATH"
RUN uv sync --frozen --no-dev && uv pip install grpcio-tools

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
