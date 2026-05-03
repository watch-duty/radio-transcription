# Use the official Jupyter Scipy notebook as base
FROM quay.io/jupyter/pytorch-notebook:latest

# Copy requirements and install them
COPY --chown=jovyan:users requirements.txt /tmp/requirements.txt
RUN --mount=type=cache,target=/root/.cache/pip \
    /opt/conda/bin/pip install torch torchvision torchaudio torchcodec --index-url https://download.pytorch.org/whl/cu128 --force-reinstall && \
    /opt/conda/bin/pip install -r /tmp/requirements.txt