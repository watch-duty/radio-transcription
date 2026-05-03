# Start from the official, fully-loaded NeMo container
FROM nvcr.io/nvidia/nemo:26.02.00

ARG NEMO_VERSION=v2.7.2

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# Set encoding to UTF-8 for handling special characters
ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8

# Install ZSH, Git, and set up ZSH
RUN apt-get update && \
    apt-get install -y --no-install-recommends zsh git wget && \
    wget -O- https://github.com/deluan/zsh-in-docker/releases/download/v1.1.5/zsh-in-docker.sh | sh -s -- -t robbyrussell && \
    rm -rf /var/lib/apt/lists/*

# Highlight the prompt so we know we are in a docker container.
RUN echo "export PROMPT=\"%F{yellow}(docker)%f \$PROMPT\"" >> ~/.zshrc

# Copy standalone requirements
COPY model/notebook_docker/requirements.txt /tmp/notebook_requirements.txt

# Copy the official pre-built uv binaries natively
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Download requirements.txt first to cache pip install
RUN --mount=type=cache,target=/root/.cache/uv \
    wget https://raw.githubusercontent.com/NVIDIA/NeMo/${NEMO_VERSION}/tools/speech_data_explorer/requirements.txt -O /tmp/requirements.txt && \
    uv pip install --python /opt/venv/bin/python -r /tmp/requirements.txt && \
    uv pip install --python /opt/venv/bin/python -r /tmp/notebook_requirements.txt && \
    uv pip install --python /opt/venv/bin/python --extra-index-url https://download.pytorch.org/whl/test/cu128 --no-deps torchaudio && \
    uv pip install --python /opt/venv/bin/python peft


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

# Create a data directory for transcriptions (mount for local data at runtime)
RUN mkdir -p /workspace/nemo/data

# Launch into ZSH
ENTRYPOINT ["/bin/zsh"]
