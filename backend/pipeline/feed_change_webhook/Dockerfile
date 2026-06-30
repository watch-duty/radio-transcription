# syntax=docker/dockerfile:1.9-labs
FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy

WORKDIR /app

ENV PYTHONPATH=/app

COPY --from=ghcr.io/astral-sh/uv:0.11.13 /uv /uvx /bin/
COPY --parents uv.lock pyproject.toml backend/pipeline/*/pyproject.toml backend/services/*/pyproject.toml ./

RUN --mount=type=cache,target=/root/.cache/uv \
    uv export --package feed-change-webhook --no-dev --locked --no-emit-project -o requirements.txt && \
    uv pip install --system -r requirements.txt

COPY backend/ ./backend/

CMD ["uvicorn", "backend.pipeline.feed_change_webhook.main:app", "--host", "0.0.0.0", "--port", "8080", "--workers", "1"]
