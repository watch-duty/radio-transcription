# Use official PyTorch GPU container (comes with PyTorch & Torchaudio pre-installed)
FROM pytorch/pytorch:2.5.1-cuda12.4-cudnn9-runtime

# Copy requirements and install them along with Jupyter
COPY notebook_docker/requirements.txt /tmp/requirements.txt
COPY notebook_docker/entrypoint.sh /usr/local/bin/asr-notebook-entrypoint
COPY --from=mwader/static-ffmpeg:6.1.1 /ffmpeg /usr/local/bin/
COPY --from=mwader/static-ffmpeg:6.1.1 /ffprobe /usr/local/bin/
RUN --mount=type=cache,target=/root/.cache/pip \
    apt-get update && apt-get install -y git build-essential libsndfile1 curl gnupg && rm -rf /var/lib/apt/lists/* && \
    /opt/conda/bin/pip install --no-cache-dir jupyter jupyterlab && \
    /opt/conda/bin/pip install --no-cache-dir -r /tmp/requirements.txt && \
    chmod +x /usr/local/bin/asr-notebook-entrypoint

# Install gcloud cli
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg && apt-get update -y && apt-get install google-cloud-cli -y

ENTRYPOINT ["/usr/local/bin/asr-notebook-entrypoint"]
