FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

ENV PYTHONPATH=/app

# 1. Install locked, scoped dependencies
COPY --from=ghcr.io/astral-sh/uv:0.9.28 /uv /uvx /bin/
COPY pyproject.toml uv.lock ./
# Copy workspace pyproject.toml files to satisfy uv workspace requirement
COPY backend/pipeline/normalization/pyproject.toml ./backend/pipeline/normalization/
COPY backend/pipeline/transcription/pyproject.toml ./backend/pipeline/transcription/
COPY backend/pipeline/evaluation/pyproject.toml ./backend/pipeline/evaluation/
COPY backend/pipeline/notification/pyproject.toml ./backend/pipeline/notification/

RUN uv export --package evaluation-function --no-dev --locked --no-emit-project -o requirements.txt && \
    uv pip install --system -r requirements.txt && \
    uv pip install --system grpcio-tools

COPY backend/ ./backend/
COPY protos/ ./protos

# 2. Create the directory for the generated files
RUN mkdir -p ./backend/pipeline/schema_types
RUN touch ./backend/pipeline/schema_types/__init__.py
RUN python -m grpc_tools.protoc -I=protos --python_out=backend/pipeline/schema_types protos/*.proto

CMD ["functions-framework", \
     "--source=backend/pipeline/evaluation/main.py", \
     "--target=evaluate_transcribed_audio_segment", \
     "--signature-type=cloudevent"]