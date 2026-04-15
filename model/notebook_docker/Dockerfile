# Use the official Jupyter Scipy notebook as base
FROM quay.io/jupyter/pytorch-notebook:latest

RUN apt-get update && \
    apt-get install -y --no-install-recommends ffmpeg && \
    rm -rf /var/lib/apt/lists/*

# Copy requirements and install them
COPY --chown=jovyan:users requirements.txt /tmp/requirements.txt
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -r /tmp/requirements.txt
