# Use NVIDIA's official PyTorch GPU container with Python 3.12 pre-installed
FROM nvcr.io/nvidia/pytorch:24.10-py3

# Copy requirements and install them
COPY requirements.txt /tmp/requirements.txt
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --no-cache-dir -r /tmp/requirements.txt