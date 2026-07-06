# syntax=docker/dockerfile:1.9-labs
FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy

WORKDIR /app

ENV PYTHONPATH=/app

# 1. Install locked, scoped dependencies
COPY --from=ghcr.io/astral-sh/uv:0.11.13 /uv /uvx /bin/
COPY --parents uv.lock pyproject.toml backend/pipeline/*/pyproject.toml backend/services/*/pyproject.toml ./

RUN --mount=type=cache,target=/root/.cache/uv \
    uv export --package notification-function --no-dev --locked --no-emit-project -o requirements.txt && \
    uv pip install --system -r requirements.txt

COPY backend/ ./backend/


CMD ["functions-framework", \
     "--source=backend/pipeline/notification/send_notification.py", \
     "--target=send_notification", \
     "--signature-type=cloudevent"]
