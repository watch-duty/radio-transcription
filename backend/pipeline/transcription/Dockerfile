FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# 1. Install the protobuf compiler
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        protobuf-compiler && \
    rm -rf /var/lib/apt/lists/*

ENV PYTHONPATH=/app

# 2. Install dependencies
COPY backend/pipeline/transcription/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY backend/__init__.py ./backend/__init__.py
COPY backend/pipeline/__init__.py ./backend/pipeline/__init__.py
COPY backend/pipeline/transcription/ ./backend/pipeline/transcription
COPY backend/pipeline/schema_types/ ./backend/pipeline/schema_types
COPY protos/ ./protos

RUN protoc --proto_path=protos --python_out=backend/pipeline/schema_types protos/*.proto

CMD ["functions-framework", \
     "--source=backend/pipeline/transcription/main.py", \
     "--target=transcribe_audio", \
     "--signature-type=cloudevent"]
