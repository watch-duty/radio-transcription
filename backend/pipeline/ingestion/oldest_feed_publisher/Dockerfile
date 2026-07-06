# syntax=docker/dockerfile:1.9-labs
FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy

WORKDIR /app

ENV PYTHONPATH=/app

# 1. Install dependencies
COPY --from=ghcr.io/astral-sh/uv:0.11.13 /uv /uvx /bin/
COPY --parents uv.lock pyproject.toml backend/pipeline/*/pyproject.toml backend/services/*/pyproject.toml ./
ENV UV_PROJECT_ENVIRONMENT=/app/.venv
ENV PATH="/app/.venv/bin:$PATH"
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-workspace && \
    GRPCIO_TOOLS_VERSION=$(python3 -c "import tomllib; print(next(p['version'] for p in tomllib.load(open('uv.lock', 'rb'))['package'] if p['name'] == 'grpcio-tools'))") && \
    uv pip install "grpcio-tools==$GRPCIO_TOOLS_VERSION"
COPY backend/ ./backend/
COPY protos/ ./protos

# 2. Create the directory for the generated files
RUN mkdir -p ./backend/pipeline/schema_types
RUN touch ./backend/pipeline/schema_types/__init__.py
RUN python -m grpc_tools.protoc -I=protos --python_out=backend/pipeline/schema_types protos/*.proto

CMD ["functions-framework", \
     "--source=backend/pipeline/ingestion/oldest_feed_publisher/main.py", \
     "--target=oldest_feed_publisher", \
     "--signature-type=http"]
