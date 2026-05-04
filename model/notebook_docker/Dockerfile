# Use Google Cloud's pre-built PyTorch GPU Deep Learning Container
FROM us-docker.pkg.dev/deeplearning-platform-release/gcr.io/pytorch-gpu:latest

# Copy requirements and install them
COPY requirements.txt /tmp/requirements.txt
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --no-cache-dir -r /tmp/requirements.txt --extra-index-url https://download.pytorch.org/whl/cu124