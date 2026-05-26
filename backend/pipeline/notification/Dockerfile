FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

ENV PYTHONPATH=/app

# 1. Install locked, scoped dependencies
COPY --from=ghcr.io/astral-sh/uv:0.7.12 /uv /uvx /bin/
COPY pyproject.toml uv.lock ./
COPY backend/ ./backend/
RUN uv export --package notification-function --no-dev --locked --no-emit-project -o requirements.txt && \
    uv pip install --system -r requirements.txt && \
    uv pip install --system grpcio-tools
COPY protos/ ./protos

# 2. Create the directory for the generated files
RUN mkdir -p ./backend/pipeline/schema_types
RUN touch ./backend/pipeline/schema_types/__init__.py
RUN python -m grpc_tools.protoc -I=protos --python_out=backend/pipeline/schema_types protos/*.proto

CMD ["functions-framework", \
     "--source=backend/pipeline/notification/send_notification.py", \
     "--target=send_notification", \
     "--signature-type=cloudevent"]
