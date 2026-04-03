FROM gcr.io/dataflow-templates-base/python313-template-launcher-base:latest AS launcher

FROM alpine:latest AS model-downloader
RUN apk add --no-cache curl
WORKDIR /models
RUN curl -sL https://github.com/k2-fsa/sherpa-onnx/releases/download/asr-models/silero_vad.onnx -o silero_vad.onnx && \
    curl -sL https://github.com/k2-fsa/sherpa-onnx/releases/download/speech-enhancement-models/gtcrn_simple.onnx -o gtcrn_simple.onnx

FROM apache/beam_python3.13_sdk:2.70.0

COPY --from=launcher /opt/google/dataflow/python_template_launcher /opt/google/dataflow/python_template_launcher

# Install OS dependencies: ffmpeg and libomp5
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ffmpeg \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies via uv
COPY --from=ghcr.io/astral-sh/uv:0.7.12 /uv /uvx /bin/
WORKDIR /app


ENV UV_PROJECT_ENVIRONMENT=/app/.venv
ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONPATH="/app"
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/app/backend/pipeline/transcription/main.py"

# Copy only the dependency manifests first so that uv sync is cached independently
# of source code changes. Only pyproject.toml/uv.lock changes bust this layer.
# The workspace member pyproject.toml must also be present for uv to resolve local deps.
COPY pyproject.toml uv.lock ./
COPY backend/pipeline/transcription/pyproject.toml ./backend/pipeline/transcription/pyproject.toml
RUN uv sync --frozen --no-dev && uv pip install grpcio-tools

COPY backend/ ./backend/

# Copy models from downloader stage
COPY --from=model-downloader /models/silero_vad.onnx ./backend/pipeline/transcription/resources/silero_vad.onnx
COPY --from=model-downloader /models/gtcrn_simple.onnx ./backend/pipeline/transcription/resources/gtcrn_simple.onnx


# Workaround for sherpa-onnx to find libonnxruntime.so without system packages
RUN ln -s $(find /app/.venv/lib/ -name "libonnxruntime.so.*" | head -n 1) $(find /app/.venv/lib/ -path "*/sherpa_onnx/lib" -type d | head -n 1)/libonnxruntime.so
COPY protos/ ./protos

# Generate protobuf bindings
RUN mkdir -p ./backend/pipeline/schema_types
RUN touch ./backend/pipeline/schema_types/__init__.py
RUN python -m grpc_tools.protoc -I=protos --python_out=backend/pipeline/schema_types protos/*.proto

# The python_template_launcher reads FLEX_TEMPLATE_PYTHON_PY_FILE and configures
# Dataflow workers to use this same container image. No wrapper script needed.
ENTRYPOINT ["/opt/google/dataflow/python_template_launcher"]

