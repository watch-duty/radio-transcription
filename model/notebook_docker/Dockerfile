# Use the official Jupyter Scipy notebook as base
FROM jupyter/scipy-notebook:latest

# Using root because apparently non-root users can't install
# system level deps.
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends ffmpeg && \
    rm -rf /var/lib/apt/lists/*

USER ${NB_USER}

# Copy requirements and install them
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
