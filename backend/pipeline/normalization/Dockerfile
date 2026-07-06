# syntax=docker/dockerfile:1.9-labs
FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install runtime OS dependencies (ffmpeg is required by AudioProcessor)
USER root
COPY --from=mwader/static-ffmpeg:6.1.1 /ffmpeg /usr/local/bin/
COPY --from=mwader/static-ffmpeg:6.1.1 /ffprobe /usr/local/bin/

WORKDIR /app

ENV PYTHONPATH=/app

# 1. Install locked, scoped dependencies
COPY --from=ghcr.io/astral-sh/uv:0.11.13 /uv /uvx /bin/
COPY --parents uv.lock pyproject.toml backend/pipeline/*/pyproject.toml backend/services/*/pyproject.toml ./

# Set environment variables for uv to use system python
ENV UV_PROJECT_ENVIRONMENT="/usr/local/"
ENV UV_SYSTEM_PYTHON=1
ENV UV_COMPILE_BYTECODE=1

# Install dependencies using uv sync directly into system python
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-workspace --package normalization-pipeline && \
    GRPCIO_TOOLS_VERSION=$(python3 -c "import tomllib; print(next(p['version'] for p in tomllib.load(open('uv.lock', 'rb'))['package'] if p['name'] == 'grpcio-tools'))") && \
    uv pip install "grpcio-tools==$GRPCIO_TOOLS_VERSION"

ENV PYTHONPATH="/app"
ENV PYTHONUNBUFFERED=1

# Copy full source
COPY backend/ ./backend/
COPY protos/ ./protos

# 2. Create the directory for the generated files
RUN mkdir -p ./backend/pipeline/schema_types
RUN touch ./backend/pipeline/schema_types/__init__.py
RUN python -m grpc_tools.protoc -I=protos --python_out=backend/pipeline/schema_types protos/*.proto

CMD ["functions-framework", \
     "--source=backend/pipeline/normalization/main.py", \
     "--target=normalize_claim_check", \
     "--signature-type=cloudevent"]
