# Start from the official, fully-loaded NeMo container
FROM nvcr.io/nvidia/nemo:26.02.00

ARG NEMO_VERSION=v2.7.2

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# Set encoding to UTF-8 for handling special characters
ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8

# Install ZSH, Git, FFmpeg, and set up ZSH
COPY --from=mwader/static-ffmpeg:6.1.1 /ffmpeg /usr/local/bin/
COPY --from=mwader/static-ffmpeg:6.1.1 /ffprobe /usr/local/bin/
RUN apt-get update && \
    apt-get install -y --no-install-recommends zsh git wget && \
    wget -O- https://github.com/deluan/zsh-in-docker/releases/download/v1.1.5/zsh-in-docker.sh | sh -s -- -t robbyrussell && \
    rm -rf /var/lib/apt/lists/*

# Highlight the prompt so we know we are in a docker container.
RUN echo "export PROMPT=\"%F{yellow}(docker)%f \$PROMPT\"" >> ~/.zshrc

# Copy dedicated NeMo requirements
# This file only contain missing non-overlapping dependencies to preserve pre-installed optimized NeMo packages
COPY model/nemo_docker/requirements.txt /tmp/nemo_requirements.txt

# Copy the official pre-built uv binaries natively
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Install dependencies efficiently with cache mounts.
# DO NOT INSTALL any torch libraries manually as they will end up conflicting with the pre-installed
# NeMO pytorch libraries and you might end up with missing symbols and what not.
# Unfortunately its not possible to install torchaudio manually on top of the torch version
# that is coming with nemo either. Use the notebook_docker if you want to just use pytorch
# without NeMO
RUN --mount=type=cache,target=/root/.cache/uv \
    wget https://raw.githubusercontent.com/NVIDIA/NeMo/${NEMO_VERSION}/tools/speech_data_explorer/requirements.txt -O /tmp/speech_explorer_requirements.txt && \
    uv pip install --python /opt/venv/bin/python -r /tmp/speech_explorer_requirements.txt && \
    uv pip install --python /opt/venv/bin/python -r /tmp/nemo_requirements.txt


# Download ONLY the directories needed from NeMO github repo and discard heavy git metadata
WORKDIR /workspace/nemo
RUN git init && \
    git remote add origin https://github.com/NVIDIA/NeMo.git && \
    git config core.sparseCheckout true && \
    echo "examples/asr" >> .git/info/sparse-checkout && \
    echo "tools/speech_data_explorer" >> .git/info/sparse-checkout && \
    git fetch --depth 1 origin refs/tags/${NEMO_VERSION}:refs/tags/${NEMO_VERSION} && \
    git checkout tags/${NEMO_VERSION} && \
    rm -rf .git

# Install gcloud cli
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg && apt-get update -y && apt-get install google-cloud-cli -y

# Create a data directory for transcriptions (mount for local data at runtime)
RUN mkdir -p /workspace/nemo/data

# Launch into ZSH
ENTRYPOINT ["/bin/zsh"]
