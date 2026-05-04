# Use official PyTorch GPU container (comes with PyTorch & Torchaudio pre-installed)
FROM pytorch/pytorch:2.5.1-cuda12.4-cudnn9-runtime

# Copy requirements and install them along with Jupyter
COPY requirements.txt /tmp/requirements.txt
RUN --mount=type=cache,target=/root/.cache/pip \
    apt-get update && apt-get install -y git build-essential libsndfile1 && rm -rf /var/lib/apt/lists/* && \
    /opt/conda/bin/pip install --no-cache-dir jupyter jupyterlab && \
    /opt/conda/bin/pip install --no-cache-dir -r /tmp/requirements.txt