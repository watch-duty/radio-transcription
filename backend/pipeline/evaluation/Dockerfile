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

COPY backend/pipeline/evaluation/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY backend ./backend

RUN protoc --proto_path=protos --python_out=backend/pipeline/schema_types protos/*.proto

CMD ["functions-framework", \
     "--source=backend/pipeline/evaluation/evaluation_core.py", \
     "--target=evaluate_transcribed_audio_segment", \
     "--signature-type=cloudevent"]