FROM python:3.12-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy proto sources and compile. The generated _pb2.py files are not checked
# into the repository — they are produced by `mise run generate:protos`.
COPY protos/ protos/
RUN python -m grpc_tools.protoc \
        --proto_path=protos \
        --python_out=schema_types \
        --pyi_out=schema_types \
        protos/*.proto \
    && touch schema_types/__init__.py

COPY main.py .

CMD ["functions-framework", "--target=handle_notification", "--signature-type=cloudevent"]
