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

COPY backend/pipeline/notification/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY backend/__init__.py ./backend/__init__.py
COPY backend/pipeline/__init__.py ./backend/pipeline/__init__.py
COPY backend/pipeline/notification/ ./backend/pipeline/notification
COPY protos/ ./protos

# 3. Create the directory for the generated files
RUN mkdir -p ./backend/pipeline/schema_types
RUN protoc --proto_path=protos --python_out=backend/pipeline/schema_types protos/*.proto

CMD ["functions-framework", \
     "--source=backend/pipeline/notification/send_notification.py", \
     "--target=send_notification", \
     "--signature-type=cloudevent", \
     "--port=8080"]
